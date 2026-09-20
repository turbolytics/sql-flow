#!/usr/bin/env bash
# Brings the template up, posts metrics, and reads them back at every grain.
# Run from render/: test/e2e.sh. Needs docker, curl, jq and openssl.
set -euo pipefail
cd "$(dirname "$0")/.."

INGEST="http://127.0.0.1:${INGEST_HOST_PORT:-10000}"
INGEST2="http://127.0.0.1:${INGEST2_HOST_PORT:-10001}"
API="http://127.0.0.1:${API_HOST_PORT:-8080}"
SECRET=local-secret
# The tag the image is built from, which an install reports as its version.
# make test passes it. Must match the Dockerfile's default.
: "${SQLFLOW_IMAGE:=turbolytics/sql-flow:v2026.09.19.1}"
export SQLFLOW_IMAGE
GRAINS="1m 5m 15m 1h 6h 1d"

fail() { echo "FAIL: $*" >&2; docker compose logs --tail 40 ingest api >&2 || true; exit 1; }
cleanup() { docker compose down -v >/dev/null 2>&1 || true; }
trap cleanup EXIT

# status <expected> <signed|unsigned> <body> [instance URL]
status() {
  local want="$1" mode="$2" body="$3" url="${4:-$INGEST}" got sig
  local args=(-s -o /dev/null -w '%{http_code}' -X POST "$url/events" --data-binary "$body")
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

echo "== an auth mode that is empty still requires a signature"
# The entrypoint and the template once disagreed about an empty mode, and an
# unsigned request was answered 200 by a pipeline with a secret set.
docker compose up -d --wait postgres
for env in "SQLFLOW_WEBHOOK_AUTH=" "SQLFLOW_WEBHOOK_AUTH=hmac"; do
  # Telemetry off: these pipelines are killed as soon as they are probed. One
  # killed between claiming install.deployed and giving the claim back leaves a
  # two-minute lease behind, and the pipelines started below cannot send the
  # event until it expires, which is longer than the test waits for it.
  docker compose run -d --rm --no-deps -p 127.0.0.1:10077:10000 -e "$env" -e SQLFLOW_TELEMETRY=off ingest >/dev/null
  wait_for "ingest with $env" "http://127.0.0.1:10077/events"
  got="$(curl -s -o /dev/null -w '%{http_code}' -X POST http://127.0.0.1:10077/events --data-binary '{"name":"x","type":"count"}')"
  docker ps -q --filter "publish=10077" | xargs -r docker rm -f >/dev/null
  [ "$got" = 400 ] || fail "with $env an unsigned request answered $got, want 400"
  echo "ok   $env refuses an unsigned request"
done

echo "== the API refuses a blank client id, and one a URL would mangle"
for env in "SQLFLOW_SERVE_CLIENT_ID=" "SQLFLOW_SERVE_CLIENT_ID=aA+SZt88/x="; do
  code=0
  docker compose run --rm -T --no-deps -e "$env" api >/dev/null 2>&1 || code=$?
  [ "$code" = 2 ] || fail "api with $env exited $code, want 2"
  echo "ok   $env exits 2"
done

echo "== signed requests land"
docker compose up -d --wait postgres
# Whatever ran before this, the pipelines below start with no claim held.
docker compose exec -T postgres psql -U metrics -d metrics -X -Atqc "UPDATE install SET deployed_claimed_at = NULL, first_request_claimed_at = NULL" >/dev/null 2>&1 || true

docker compose up -d collector ingest ingest2 api
wait_for ingest "$INGEST/events"
wait_for ingest2 "$INGEST2/events"
wait_for api "$API/healthz"

# install.deployed went out when the pipelines started. install.first_request
# goes out once the metrics below land, and under this stack's five-second idle
# bound the collector may already have closed the minute the first was sent
# in. Start in the next one, so the second is not dropped as late.
sleep $(( 61 - 10#$(date -u +%S) ))

# One timestamp for every metric. Without it the posts could straddle a
# minute, and the 1m assertions below would see two buckets.
TS="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

echo "== the pipeline answers a health check with no signature, and nothing else"
# render.yaml's healthCheckPath, and what an uptime monitor calls. Signatures
# are on here: the check must not need one, and must not open anything.
[ "$(curl -s "$INGEST/healthz")" = '{"status":"ok"}' ] || fail "GET /healthz did not answer {\"status\":\"ok\"}"
[ "$(curl -s -o /dev/null -w '%{http_code}' -I "$INGEST/healthz")" = 200 ] || fail "HEAD /healthz did not answer 200"
[ "$(curl -s -o /dev/null -w '%{http_code}' -X POST "$INGEST/healthz")" = 405 ] || fail "POST /healthz did not answer 405"
echo "ok   GET and HEAD /healthz answer 200 unsigned, and POST is refused"

status 400 unsigned '{"name":"unsigned","type":"count"}'
status 200 signed '{"name":"checkout","type":"count","timestamp":"'"$TS"'","dimensions":{"region":"us-east","plan":"pro"}}'
status 200 signed '{"name":"checkout","type":"count","value":2,"timestamp":"'"$TS"'","dimensions":{"plan":"pro","region":"us-east"}}'
status 200 signed '{"name":"checkout","type":"count","value":5,"timestamp":"'"$TS"'","dimensions":{"region":"us-east","plan":"free"}}'
status 200 signed '{"name":"checkout","type":"count","timestamp":"'"$TS"'","dimensions":{"region":"eu","plan":"pro"}}'
status 200 signed '{"name":"quote","type":"count","timestamp":"'"$TS"'","dimensions":{"q":"say \"hi\""}}'
status 200 signed '{"metrics":[{"name":"cpu","type":"gauge","value":0.25,"timestamp":"'"$TS"'"},{"name":"cpu","type":"gauge","value":0.5,"timestamp":"'"$TS"'"}]}'
# One minute of one series, split across two pipeline instances. Each holds
# its own window and publishes its own part. Keyed on the series alone, the
# second to publish replaced the first, and 7 and 5 were stored as one of
# them. The gauge's later reading goes to the second instance, so value_last
# must be chosen by event time across writers, not by who published last.
MIN="${TS%:*}"
status 200 signed '{"name":"split","type":"count","value":7,"timestamp":"'"$TS"'"}' "$INGEST"
status 200 signed '{"name":"split","type":"count","value":5,"timestamp":"'"$TS"'"}' "$INGEST2"
status 200 signed '{"name":"split.gauge","type":"gauge","value":9,"timestamp":"'"$MIN"':20Z"}' "$INGEST2"
status 200 signed '{"name":"split.gauge","type":"gauge","value":1,"timestamp":"'"$MIN"':10Z"}' "$INGEST"

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
  n="$(get series | jq '[.rows[] | select(.name | startswith("install.") | not)] | length')"
  [ "$n" -ge 7 ] && break
  sleep 1
done
# Both instances must have published before the split minute is read.
for i in $(seq 1 60); do
  w="$(docker compose exec -T postgres psql -U metrics -d metrics -Atc "SELECT count(DISTINCT writer) FROM metrics_1m_writers WHERE name = 'split'")"
  [ "$w" = 2 ] && break
  sleep 1
done
[ "$w" = 2 ] || fail "the split minute has $w writers, want 2: both instances must publish it"
echo "ok   two writers published the split minute"

series="$(get series)"
expect "series holds the seven that were valid, and no bad one" \
  '[.rows[].name | select(startswith("install.") | not)] | sort == ["checkout","checkout","checkout","cpu","quote","split","split.gauge"]' "$series"
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

  split="$(get metric name=split grain="$g" "$since")"
  expect "$g: a minute split across two instances is their sum, not the last to publish" \
    '[.rows[] | [.value_sum, .value_count, .value_min, .value_max]] == [[12,2,5,7]]' "$split"
  splitg="$(get metric name=split.gauge grain="$g" "$since")"
  expect "$g: across two instances value_last is the later reading" \
    '[.rows[] | [.value_last, .value_min, .value_max]] == [[9,1,9]]' "$splitg"

  total="$(get metric_total name=checkout grain="$g" "$since")"
  expect "$g: the total sums every series" \
    '[.rows[] | [.value_sum, .value_count, .value_min, .value_max]] == [[9,4,1,5]]' "$total"
done

narrow="$(get metric name=checkout grain=1d)"
expect "a pinned 1d grain with the default hour is an empty range, not an error" \
  '(.range.since == .range.until) and (.rows | type) == "array" and (.rows | length) == 0' "$narrow"
auto="$(get metric name=checkout)"
expect "without a grain the API picks the finest that covers the range" '.grain == "1m" and (.rows | length) == 3' "$auto"

# What a reader does with the id they chose: paste it into a URL as it is.
pasted="$(curl -s -o /dev/null -w '%{http_code}' "$API/v1/datasets/series?client_id=local-dev")"
[ "$pasted" = 200 ] || fail "a client id pasted into the URL answered $pasted"
echo "ok   the client id works pasted into a URL, unencoded"
wrong="$(curl -s -o /dev/null -w '%{http_code}' "$API/v1/datasets/series?client_id=not-the-id")"
[ "$wrong" = 401 ] || fail "a wrong client id answered $wrong, want 401"
echo "ok   a wrong client id is refused"

noname="$(get metric)"
expect "no name answers empty" '(.rows | type) == "array" and (.rows | length) == 0' "$noname"

echo "== telemetry: two events, once each, to the local collector and nowhere else"
psqlc() { docker compose exec -T postgres psql -U metrics -d metrics -X -Atc "$1"; }
install_id="$(psqlc 'SELECT install_id FROM install')"
for i in $(seq 1 60); do
  n="$(get series | jq '[.rows[] | select(.name == "install.deployed" or .name == "install.first_request")] | length')"
  [ "$n" -ge 2 ] && break
  sleep 1
done
series="$(get series)"
expect "one install.deployed and one install.first_request, from two pipeline instances" \
  '[.rows[] | select(.name | startswith("install.")) | .name] | sort == ["install.deployed","install.first_request"]' "$series"
expect "the event names this database and nothing about who deployed it" \
  '[.rows[] | select(.name == "install.deployed") | .dimensions | fromjson] == [{"install_id":"'"$install_id"'","source":"render","template":"render-metrics","sqlflow_version":"'"${SQLFLOW_IMAGE##*:}"'"}]' "$series"
for name in install.deployed install.first_request; do
  expect "$name was sent once, though two instances started together" \
    '[.rows[].value_sum] == [1]' "$(get metric_total name="$name")"
done
[ "$(psqlc 'SELECT deployed_sent_at IS NOT NULL AND first_request_sent_at IS NOT NULL FROM install')" = t ] \
  || fail "install does not record both events as sent"
echo "ok   both events are recorded as sent"

docker compose restart ingest ingest2 >/dev/null
wait_for ingest "$INGEST/events"
sleep 8
for name in install.deployed install.first_request; do
  expect "$name is still one after both instances restarted" \
    '[.rows[].value_sum] | add == 1' "$(get metric_total name="$name" "since=$(since_for 1h)" grain=1h)"
done

# What off and a dead collector do, each from a clean slate.
reset_install() { psqlc 'UPDATE install SET deployed_claimed_at = NULL, deployed_sent_at = NULL, first_request_claimed_at = NULL, first_request_sent_at = NULL' >/dev/null; }
run_probe() { # run_probe <label> <env...>: starts a pipeline on :10077 and leaves it running
  local label="$1"; shift
  local args=(); for e in "$@"; do args+=(-e "$e"); done
  docker compose run -d --rm --no-deps -p 127.0.0.1:10077:10000 "${args[@]}" ingest >/dev/null
  wait_for "$label" "http://127.0.0.1:10077/events"
}
stop_probe() { docker ps -q --filter "publish=10077" | xargs -r docker rm -f >/dev/null; }

reset_install
run_probe "ingest with telemetry off" SQLFLOW_TELEMETRY=off
sleep 6
[ "$(psqlc 'SELECT deployed_sent_at IS NULL AND deployed_claimed_at IS NULL FROM install')" = t ] \
  || fail "SQLFLOW_TELEMETRY=off still claimed or sent install.deployed"
stop_probe
echo "ok   SQLFLOW_TELEMETRY=off sends nothing and claims nothing"

reset_install
run_probe "ingest with a collector that is down" SQLFLOW_TELEMETRY_URL=http://127.0.0.1:9
sleep 6
[ "$(psqlc 'SELECT deployed_sent_at IS NULL FROM install')" = t ] \
  || fail "install.deployed is recorded as sent though the collector was down"
status_down="$(curl -s -o /dev/null -w '%{http_code}' -X POST http://127.0.0.1:10077/events --data-binary '{}')"
[ "$status_down" = 400 ] || fail "with the collector down the pipeline answered $status_down, want 400 for an unsigned request: it must be up"
stop_probe
echo "ok   a collector that is down leaves the event unsent and the pipeline up"

# Give the claim back so the next start, below, is the one that sends.
reset_install

echo "== a restart applies no migration twice"
# Both services migrate on every start, so a log line saying skipped proves
# nothing: whichever service lost the first race already printed one. The
# table is the record. A migration applied twice would fail on its CREATE
# TABLE and take the service down with it.
docker compose restart ingest >/dev/null
wait_for ingest "$INGEST/events"
applied="$(docker compose exec -T postgres psql -U metrics -d metrics -Atc 'SELECT count(*) FROM schema_migrations')"
want="$(ls migrations/*.sql | wc -l | tr -d ' ')"
[ "$applied" = "$want" ] || fail "schema_migrations holds $applied rows after a restart, want $want, one per file"
[ -n "$(docker compose ps --status running -q ingest)" ] || fail "ingest is not running after a restart"
echo "ok   $want migrations recorded, one per file, and ingest came back"

echo "== unsigned mode, and the name prefix"
docker compose stop ingest >/dev/null
SQLFLOW_WEBHOOK_AUTH=none SQLFLOW_WEBHOOK_HMAC_SECRET= SQLFLOW_METRIC_NAME_PREFIX=install. docker compose up -d ingest
wait_for ingest "$INGEST/events"
status 200 unsigned '{"name":"install.by.hand","type":"count","dimensions":{"install_id":"abc"}}'
status 200 unsigned '{"name":"other.thing","type":"count"}'
for i in $(seq 1 60); do
  n="$(get series | jq '[.rows[] | select(.name == "install.by.hand")] | length')"
  [ "$n" -ge 1 ] && break
  sleep 1
done
series="$(get series)"
expect "the prefixed name landed unsigned" '[.rows[] | select(.name == "install.by.hand")] | length == 1' "$series"
expect "the name outside the prefix was dropped" '[.rows[] | select(.name == "other.thing")] | length == 0' "$series"

echo "PASS"
