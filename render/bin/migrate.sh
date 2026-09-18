#!/usr/bin/env bash
# Applies every migrations/*.sql exactly once, in filename order.
#
# Safe to run on every process start, which is how both services use it. Two
# instances starting during the same deploy cannot race: the applied check and
# the migration run in one transaction behind an advisory lock, so the second
# instance sees the first's commit and skips.
set -euo pipefail

if [ -z "${SQLFLOW_POSTGRES_URI:-}" ]; then
  echo "SQLFLOW_POSTGRES_URI is not set" >&2
  exit 2
fi

for tool in psql pg_isready; do
  if ! command -v "$tool" >/dev/null; then
    echo "$tool not found on PATH" >&2
    exit 2
  fi
done

# Wait for the database to accept connections before the first psql.
#
# Render resolves fromDatabase as soon as the database record exists, but a
# freshly provisioned instance is not listening yet. psql exits 2 on a refused
# connection and set -e turns that into a failed deploy, which is what a
# Blueprint's first deploy hit: "connection to server ... failed: Connection
# refused", exit 2, before a single migration ran.
#
# Nothing caught it earlier because every other way into this script gates on
# readiness first -- compose on service_healthy, CI on pg_isready -- and on a
# database that already exists there is nothing to wait for.
wait_seconds="${SQLFLOW_MIGRATE_WAIT_SECONDS:-180}"
if ! pg_isready -q -d "$SQLFLOW_POSTGRES_URI"; then
  echo "waiting up to ${wait_seconds}s for the database to accept connections"
  deadline=$((SECONDS + wait_seconds))
  until pg_isready -q -d "$SQLFLOW_POSTGRES_URI"; do
    if [ "$SECONDS" -ge "$deadline" ]; then
      echo "database did not accept connections within ${wait_seconds}s" >&2
      exit 2
    fi
    sleep 2
  done
  echo "database is accepting connections"
fi

cd "$(dirname "$0")/.."

# Under the same lock as the migrations: CREATE TABLE IF NOT EXISTS is not
# safe against itself, and two concurrent runs can fail on the system catalog's
# unique index. client_min_messages silences the "already exists, skipping"
# notice that every run after the first would otherwise print.
psql "$SQLFLOW_POSTGRES_URI" -v ON_ERROR_STOP=1 -q <<'SQL'
SET client_min_messages = warning;
BEGIN;
SELECT pg_advisory_xact_lock(hashtext('sqlflow_render_metrics_migrations')) AS locked \gset
CREATE TABLE IF NOT EXISTS schema_migrations (
  version    TEXT PRIMARY KEY,
  applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
COMMIT;
SQL

for file in migrations/*.sql; do
  version="$(basename "$file" .sql)"

  # Postgres DDL is transactional, so a migration that fails rolls back whole
  # and records no version. The next run retries it from a clean state.
  #
  # "AS locked \gset" consumes the lock's result row; without it psql prints a
  # table on every run. \echo interpolates :version only outside quotes.
  psql "$SQLFLOW_POSTGRES_URI" -v ON_ERROR_STOP=1 -q -v version="$version" <<SQL
BEGIN;
SELECT pg_advisory_xact_lock(hashtext('sqlflow_render_metrics_migrations')) AS locked \gset
SELECT count(*) = 0 AS pending FROM schema_migrations WHERE version = :'version' \gset
\if :pending
  \i $file
  INSERT INTO schema_migrations (version) VALUES (:'version');
  \echo 'applied ' :version
\else
  \echo 'skipped ' :version
\endif
COMMIT;
SQL
done
