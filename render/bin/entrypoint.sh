#!/usr/bin/env bash
# The ingest service's entrypoint: check the environment, provision the
# schema, then run the pipeline.
set -euo pipefail

# Checked here rather than left to sqlflow. An unset template variable renders
# as an empty string, and the error for an empty DSN does not name the cause.
if [ -z "${SQLFLOW_POSTGRES_URI:-}" ]; then
  echo "SQLFLOW_POSTGRES_URI is not set" >&2
  exit 2
fi

# Unsigned mode is chosen, never reached by omission. Render lets a deploy
# leave the secret's prompt blank, and a blank secret under hmac would be a
# public write endpoint into this database.
#
# This script is the one place the mode is decided. It exports the mode it
# settled on, so pipeline.yml never reads a value this script did not check.
# The two once decided separately and disagreed about an empty string: this
# script read it as hmac and was satisfied by the secret, the template read it
# as not-hmac and rendered no signature block, and an unsigned request was
# answered 200 by a pipeline whose owner believed it was signed.
case "${SQLFLOW_WEBHOOK_AUTH:-hmac}" in
  hmac)
    SQLFLOW_WEBHOOK_AUTH=hmac
    if [ -z "${SQLFLOW_WEBHOOK_HMAC_SECRET:-}" ]; then
      echo "SQLFLOW_WEBHOOK_HMAC_SECRET is not set. Set it, or set SQLFLOW_WEBHOOK_AUTH=none to accept unsigned requests." >&2
      exit 2
    fi
    ;;
  none)
    SQLFLOW_WEBHOOK_AUTH=none
    echo "webhook accepts unsigned requests: SQLFLOW_WEBHOOK_AUTH=none" >&2
    ;;
  *)
    echo "SQLFLOW_WEBHOOK_AUTH is '${SQLFLOW_WEBHOOK_AUTH}'. Use hmac or none." >&2
    exit 2
    ;;
esac
export SQLFLOW_WEBHOOK_AUTH

# The prefix is rendered into the handler's SQL inside single quotes.
case "${SQLFLOW_METRIC_NAME_PREFIX:-}" in
  *[!A-Za-z0-9._-]*)
    echo "SQLFLOW_METRIC_NAME_PREFIX may hold only letters, digits, '.', '_' and '-'" >&2
    exit 2
    ;;
esac

# Names this process in every row it publishes, so that another instance
# publishing the same minute adds to it rather than replacing it. Made here
# and never read from the environment: a value someone set once would be
# shared by every instance, which is the defect it exists to prevent. New on
# every start, because a restarted process has lost its window and must add
# to what the last one published, not replace it.
SQLFLOW_WRITER_ID="$(od -An -N8 -tx1 /dev/urandom | tr -d ' \n')"
if [ "${#SQLFLOW_WRITER_ID}" != 16 ]; then
  echo "could not make a writer id from /dev/urandom" >&2
  exit 2
fi
export SQLFLOW_WRITER_ID

/app/bin/migrate.sh

# Two events per install, ever, to the sqlflow maintainers: that this was
# deployed, and that it received its first metric. SQLFLOW_TELEMETRY=off stops
# both. In the background and detached from this script's fate: it logs and
# swallows its own failures, and nothing it does can stop the pipeline below.
/app/bin/telemetry.sh &

# exec keeps sqlflow as PID 1, so the platform's SIGTERM reaches it and the
# graceful drain runs. Without exec, bash holds PID 1 and forwards nothing.
exec sqlflow run -c /app/pipeline.yml "$@"
