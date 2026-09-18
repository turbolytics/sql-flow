#!/usr/bin/env bash
# The API service's entrypoint.
set -euo pipefail

if [ -z "${SQLFLOW_POSTGRES_URI:-}" ]; then
  echo "SQLFLOW_POSTGRES_URI is not set" >&2
  exit 2
fi

# The client id is the only thing between a reader and this database's
# metrics, so there is no default. It travels in a URL as ?client_id=, where
# '+' reads as a space and '/' and '=' need encoding: an id with any of them
# answers 401 to a caller who pasted it as it is.
if [ -z "${SQLFLOW_SERVE_CLIENT_ID:-}" ]; then
  echo "SQLFLOW_SERVE_CLIENT_ID is not set. Set it to a long random string, such as the output of: openssl rand -hex 16" >&2
  exit 2
fi
case "$SQLFLOW_SERVE_CLIENT_ID" in
  *[!A-Za-z0-9._~-]*)
    echo "SQLFLOW_SERVE_CLIENT_ID may hold only letters, digits, '.', '_', '~' and '-', so it can be pasted into a URL. Try: openssl rand -hex 16" >&2
    exit 2
    ;;
esac

# Both services migrate. Whichever starts first applies the schema, and the
# advisory lock in migrate.sh makes the other skip. Without it the API would
# fail its first deploy whenever it won the race: serve prepares every
# dataset at startup, against tables that do not exist yet.
/app/bin/migrate.sh

exec sqlflow serve -c /app/serve.yml "$@"
