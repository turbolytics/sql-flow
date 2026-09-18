#!/usr/bin/env bash
# Tells the sqlflow maintainers that this template was deployed, and that it
# received its first metric. Two events, each sent once per install, and
# nothing else, ever. render/README.md, "What this sends", prints both.
#
#   SQLFLOW_TELEMETRY=off   sends nothing. Also: false, 0, no.
#
# bin/entrypoint.sh starts this in the background after the migrations. It must
# never be the reason a pipeline does not start or stops: every failure here is
# logged and swallowed, a send is bounded at five seconds, and a collector that
# is down costs a retry on a later tick, not a deploy.
set -uo pipefail

log() { echo "telemetry: $*" >&2; }

case "$(printf '%s' "${SQLFLOW_TELEMETRY:-on}" | tr '[:upper:]' '[:lower:]')" in
  off|false|0|no)
    log "off (SQLFLOW_TELEMETRY=${SQLFLOW_TELEMETRY}). Nothing is sent."
    exit 0
    ;;
esac

# A hostname the maintainers own, not a platform's URL: a deployed copy never
# updates itself, so this is the address it calls for as long as it runs.
URL="${SQLFLOW_TELEMETRY_URL:-https://telemetry.turbolytics.io}"
POLL="${SQLFLOW_TELEMETRY_POLL_SECONDS:-30}"
# Where this template runs. Other templates report to the same collector, each
# with its own constant. Not a setting: a template knows where it runs.
SOURCE=render
TEMPLATE=render-metrics

sql() { psql "$SQLFLOW_POSTGRES_URI" -X -Atq -v ON_ERROR_STOP=1 -c "$1" 2>/dev/null; }

# The first line of `sqlflow --version` is "sqlflow v2026.09.18.1".
VERSION="$(sqlflow --version 2>/dev/null | awk 'NR==1 {print $2}')"
case "$VERSION" in ''|*[!A-Za-z0-9._-]*) VERSION=unknown ;; esac

# try <event name> <column prefix>
# Returns 0 once the event is sent, now or earlier. Returns 1 to try again.
try() {
  local name="$1" col="$2" id body

  [ "$(sql "SELECT ${col}_sent_at IS NOT NULL FROM install")" = t ] && return 0

  # The lease. One process gets the row back; the others get nothing.
  id="$(sql "UPDATE install SET ${col}_claimed_at = now()
             WHERE ${col}_sent_at IS NULL
               AND (${col}_claimed_at IS NULL OR ${col}_claimed_at < now() - interval '2 minutes')
             RETURNING install_id")"
  case "$id" in ''|*[!0-9a-f-]*) return 1 ;; esac

  body="{\"name\":\"$name\",\"type\":\"count\",\"value\":1,\"dimensions\":{\"install_id\":\"$id\",\"source\":\"$SOURCE\",\"template\":\"$TEMPLATE\",\"sqlflow_version\":\"$VERSION\"}}"

  # --fail: only a 2xx counts. A custom domain that is still propagating
  # answers 404, and an install recorded as sent on a 404 is never counted.
  if curl --fail --silent --show-error --max-time 5 -o /dev/null \
       -X POST "$URL/events" -H 'Content-Type: application/json' --data-binary "$body" 2>/dev/null; then
    sql "UPDATE install SET ${col}_sent_at = now() WHERE ${col}_sent_at IS NULL" >/dev/null
    log "sent $name to $URL: $body"
    log "this is one of two events this install ever sends. SQLFLOW_TELEMETRY=off stops them. See render/README.md."
    return 0
  fi

  sql "UPDATE install SET ${col}_claimed_at = NULL WHERE ${col}_sent_at IS NULL" >/dev/null
  return 1
}

tries=0
while :; do
  deployed=1; first=1
  try install.deployed deployed && deployed=0

  # The first metric, from anyone. Nothing about it is read: the question is
  # whether a row exists. install.* names do not count. They are telemetry, not
  # a deployer's metric, and a database that is itself a collector holds
  # nothing else.
  if [ "$(sql "SELECT EXISTS (SELECT 1 FROM metrics_1m WHERE name NOT LIKE 'install.%')")" = t ]; then
    try install.first_request first_request && first=0
  fi

  if [ "$deployed" = 0 ] && [ "$first" = 0 ]; then
    exit 0
  fi

  # Every POLL seconds for the first hour, then every five minutes: a deploy
  # nobody ever sends a metric to should not query its database twice a minute
  # for as long as it lives.
  tries=$((tries + 1))
  if [ "$tries" -gt $((3600 / POLL)) ] && [ "$POLL" -lt 300 ]; then
    sleep 300
  else
    sleep "$POLL"
  fi
done
