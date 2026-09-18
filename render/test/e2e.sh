#!/usr/bin/env bash
# Brings the template up, posts metrics, and reads them back at every grain.
# Run from render/: test/e2e.sh. Needs docker, curl, jq and openssl.
set -euo pipefail
cd "$(dirname "$0")/.."

INGEST="http://127.0.0.1:${INGEST_HOST_PORT:-10000}"
API="http://127.0.0.1:${API_HOST_PORT:-8080}"
SECRET=local-secret
GRAINS="1m 5m 15m 1h 6h 1d"

fail() { echo "FAIL: $*" >&2; docker compose logs --tail 40 ingest api >&2 || true; exit 1; }
cleanup() { docker compose down -v >/dev/null 2>&1 || true; }
trap cleanup EXIT

# status <expected> <signed|unsigned> <body>
status() {
  local want="$1" mode="$2" body="$3" got sig
  local args=(-s -o /dev/null -w '%{http_code}' -X POST "$INGEST/events" --data-binary "$body")
  if [ "$mode" = signed ]; then
    sig="$(printf '%s' "$body" | openssl dgst -sha256 -hmac "$SECRET" -hex | sed 's/^.* //')"
    args+=(-H "X-Signature-256: sha256=$sig")
  fi
  got="$(curl "${args[@]}")"
  [ "$got" = "$want" ] || fail "POST ($mode) $body: status $got, want $want"
}

# get <dataset> [curl --data-urlencode args...] -> response JSON
get() {
  local ds="$1"; shift
  local args=(-s -G "$API/v1/datasets/$ds" --data-urlencode client_id=local-dev)
  for kv in "$@"; do args+=(--data-urlencode "$kv"); done
  curl "${args[@]}"
}

# expect <description> <jq filter that must print true> <json>
expect() {
  [ "$(jq -r "$2" <<<"$3")" = true ] || fail "$1: $(jq -c . <<<"$3")"
  echo "ok   $1"
}

wait_for() {
  local what="$1" url="$2" i
  for i in $(seq 1 60); do
    if curl -s -o /dev/null "$url"; then return 0; fi
    sleep 1
  done
  fail "$what did not come up"
}

echo "== the entrypoint refuses a blank secret and an unknown auth mode"
cleanup
docker compose build -q
for env in "SQLFLOW_WEBHOOK_HMAC_SECRET=" "SQLFLOW_WEBHOOK_AUTH=open" "SQLFLOW_METRIC_NAME_PREFIX=a'b"; do
  code=0
  docker compose run --rm -T --no-deps -e "$env" ingest >/dev/null 2>&1 || code=$?
  [ "$code" = 2 ] || fail "ingest with $env exited $code, want 2"
  echo "ok   $env exits 2"
done

echo "== signed requests land"
docker compose up -d --wait postgres
docker compose up -d ingest api
wait_for ingest "$INGEST/events"
wait_for api "$API/healthz"

# One timestamp for every metric. Without it the posts could straddle a
# minute, and the 1m assertions below would see two buckets.
TS="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

status 400 unsigned '{"name":"unsigned","type":"count"}'
status 200 signed '{"name":"checkout","type":"count","timestamp":"'"$TS"'","dimensions":{"region":"us-east","plan":"pro"}}'
status 200 signed '{"name":"checkout","type":"count","value":2,"timestamp":"'"$TS"'","dimensions":{"plan":"pro","region":"us-east"}}'
status 200 signed '{"name":"checkout","type":"count","value":5,"timestamp":"'"$TS"'","dimensions":{"region":"us-east","plan":"free"}}'
status 200 signed '{"name":"checkout","type":"count","timestamp":"'"$TS"'","dimensions":{"region":"eu","plan":"pro"}}'
status 200 signed '{"name":"quote","type":"count","timestamp":"'"$TS"'","dimensions":{"q":"say \"hi\""}}'
status 200 signed '{"metrics":[{"name":"cpu","type":"gauge","value":0.25,"timestamp":"'"$TS"'"},{"name":"cpu","type":"gauge","value":0.5,"timestamp":"'"$TS"'"}]}'
# Each is answered 200 and stores nothing.
status 200 signed '{"name":"bad-type","type":"histogram"}'
status 200 signed '{"name":"bad-value","type":"count","value":"abc"}'
status 200 signed '{"name":"bad-dims","type":"count","dimensions":[1,2]}'
status 200 signed '{"name":"future","type":"count","timestamp":"2030-01-01T00:00:00Z"}'
status 200 signed '{"name":"bad-ts","type":"count","timestamp":"yesterday"}'
status 200 signed '[{"name":"bare-array","type":"count"}]'
status 200 signed 'not json'

echo "== waiting for the minute to close"
for i in $(seq 1 60); do
  n="$(get series | jq '.rows | length')"
  [ "$n" -ge 5 ] && break
  sleep 1
done

series="$(get series)"
expect "series holds the five that were valid, and no bad one" \
  '[.rows[].name] | sort == ["checkout","checkout","checkout","cpu","quote"]' "$series"
expect "a dimension value with a quote in it survives" \
  '[.rows[] | select(.name == "quote") | .dimensions | fromjson | .q] == ["say \"hi\""]' "$series"

# A pinned grain needs a range at least as wide as its bucket. serve snaps the
# range to bucket boundaries, and a bucket is in the range only when its start
# is, so the default range of one hour holds no whole 6h or 1d bucket and
# answers empty. Each grain is asked for a range just inside its max_range.
since_for() {
  local hours epoch
  case "$1" in 1m) hours=5 ;; 5m) hours=23 ;; 15m) hours=71 ;; 1h) hours=335 ;; 6h) hours=2159 ;; 1d) hours=8735 ;; esac
  epoch=$(( $(date -u +%s) - hours * 3600 ))
  # GNU date reads @epoch, BSD date reads -r epoch.
  date -u -d "@$epoch" +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || date -u -r "$epoch" +%Y-%m-%dT%H:%M:%SZ
}

for g in $GRAINS; do
  echo "== grain $g"
  since="since=$(since_for "$g")"
  all="$(get metric name=checkout grain="$g" "$since")"
  expect "$g: three checkout series" '.grain == "'"$g"'" and (.rows | length) == 3' "$all"
  expect "$g: two key orders were one series, summed" \
    '[.rows[] | select((.dimensions | fromjson) == {"plan":"pro","region":"us-east"}) | [.value_sum, .value_count, .value_min, .value_max]] == [[3,2,1,2]]' "$all"

  one="$(get metric name=checkout grain="$g" "$since" 'dimensions={"region":"us-east"}')"
  expect "$g: one pair returns the two series that contain it" '(.rows | length) == 2' "$one"
  both="$(get metric name=checkout grain="$g" "$since" 'dimensions={"region":"us-east","plan":"free"}')"
  expect "$g: both pairs return one series" '[.rows[].value_sum] == [5]' "$both"
  none="$(get metric name=checkout grain="$g" "$since" 'dimensions={"region":"us"}')"
  expect "$g: a value that is a prefix of another matches nothing" '(.rows | type) == "array" and (.rows | length) == 0' "$none"
  bad="$(get metric name=checkout grain="$g" "$since" 'dimensions={not json')"
  expect "$g: a filter that is not JSON matches nothing, and is not an error" '(.rows | type) == "array" and (.rows | length) == 0' "$bad"

  cpu="$(get metric name=cpu grain="$g" "$since")"
  expect "$g: a gauge keeps its fraction" \
    '[.rows[] | [.value_sum, .value_count, .value_min, .value_max]] == [[0.75,2,0.25,0.5]]' "$cpu"

  total="$(get metric_total name=checkout grain="$g" "$since")"
  expect "$g: the total sums every series" \
    '[.rows[] | [.value_sum, .value_count, .value_min, .value_max]] == [[9,4,1,5]]' "$total"
done

narrow="$(get metric name=checkout grain=1d)"
expect "a pinned 1d grain with the default hour is an empty range, not an error" \
  '(.range.since == .range.until) and (.rows | type) == "array" and (.rows | length) == 0' "$narrow"
auto="$(get metric name=checkout)"
expect "without a grain the API picks the finest that covers the range" '.grain == "1m" and (.rows | length) == 3' "$auto"

noname="$(get metric)"
expect "no name answers empty" '(.rows | type) == "array" and (.rows | length) == 0' "$noname"

echo "== a restart applies no migration twice"
# Both services migrate on every start, so a log line saying skipped proves
# nothing: whichever service lost the first race already printed one. The
# table is the record. A migration applied twice would fail on its CREATE
# TABLE and take the service down with it.
docker compose restart ingest >/dev/null
wait_for ingest "$INGEST/events"
applied="$(docker compose exec -T postgres psql -U metrics -d metrics -Atc 'SELECT count(*) FROM schema_migrations')"
[ "$applied" = 3 ] || fail "schema_migrations holds $applied rows after a restart, want 3"
[ -n "$(docker compose ps --status running -q ingest)" ] || fail "ingest is not running after a restart"
echo "ok   three migrations recorded, and ingest came back"

echo "== unsigned mode, and the name prefix"
docker compose stop ingest >/dev/null
SQLFLOW_WEBHOOK_AUTH=none SQLFLOW_WEBHOOK_HMAC_SECRET= SQLFLOW_METRIC_NAME_PREFIX=install. docker compose up -d ingest
wait_for ingest "$INGEST/events"
status 200 unsigned '{"name":"install.deployed","type":"count","dimensions":{"install_id":"abc"}}'
status 200 unsigned '{"name":"other.thing","type":"count"}'
for i in $(seq 1 60); do
  n="$(get series | jq '[.rows[] | select(.name == "install.deployed")] | length')"
  [ "$n" -ge 1 ] && break
  sleep 1
done
series="$(get series)"
expect "the prefixed name landed unsigned" '[.rows[] | select(.name == "install.deployed")] | length == 1' "$series"
expect "the name outside the prefix was dropped" '[.rows[] | select(.name == "other.thing")] | length == 0' "$series"

echo "PASS"
