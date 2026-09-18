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
case "${SQLFLOW_WEBHOOK_AUTH:-hmac}" in
  hmac)
    if [ -z "${SQLFLOW_WEBHOOK_HMAC_SECRET:-}" ]; then
      echo "SQLFLOW_WEBHOOK_HMAC_SECRET is not set. Set it, or set SQLFLOW_WEBHOOK_AUTH=none to accept unsigned requests." >&2
      exit 2
    fi
    ;;
  none)
    echo "webhook accepts unsigned requests: SQLFLOW_WEBHOOK_AUTH=none" >&2
    ;;
  *)
    echo "SQLFLOW_WEBHOOK_AUTH is '${SQLFLOW_WEBHOOK_AUTH}'. Use hmac or none." >&2
    exit 2
    ;;
esac

# The prefix is rendered into the handler's SQL inside single quotes.
case "${SQLFLOW_METRIC_NAME_PREFIX:-}" in
  *[!A-Za-z0-9._-]*)
    echo "SQLFLOW_METRIC_NAME_PREFIX may hold only letters, digits, '.', '_' and '-'" >&2
    exit 2
    ;;
esac

/app/bin/migrate.sh

# exec keeps sqlflow as PID 1, so the platform's SIGTERM reaches it and the
# graceful drain runs. Without exec, bash holds PID 1 and forwards nothing.
exec sqlflow run -c /app/pipeline.yml "$@"
