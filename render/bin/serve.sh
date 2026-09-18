#!/usr/bin/env bash
# The API service's entrypoint.
set -euo pipefail

for var in SQLFLOW_POSTGRES_URI SQLFLOW_SERVE_CLIENT_ID; do
  if [ -z "${!var:-}" ]; then
    echo "$var is not set" >&2
    exit 2
  fi
done

# Both services migrate. Whichever starts first applies the schema, and the
# advisory lock in migrate.sh makes the other skip. Without it the API would
# fail its first deploy whenever it won the race: serve prepares every
# dataset at startup, against tables that do not exist yet.
/app/bin/migrate.sh

exec sqlflow serve -c /app/serve.yml "$@"
