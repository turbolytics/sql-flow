#!/usr/bin/env bash
# Runs the MQTT POC scenarios against dev/mqtt.yml and checks each for loss.
#
#   make mqtt-poc SCENARIO=crash
#
# Scenarios: steady, ceiling, crash, broker, all. Each starts from an empty
# broker and an empty sink. The loss check reads the DuckDB file after
# SQLFlow stops, because DuckDB allows one writer.
set -euo pipefail

cd "$(dirname "$0")/.."
: "${SQLFLOW_IMAGE:?set SQLFLOW_IMAGE}"
COMPOSE=(docker compose -f dev/mqtt.yml)
DATA=dev/iot/data
DUCKDB_PY=$(sed 's/^v//' DUCKDB_VERSION)
SCENARIO=${1:-all}
# A failing scenario exits under set -e. The stack must not outlive it.
trap '"${COMPOSE[@]}" down -v >/dev/null 2>&1 || true' EXIT

reset() {
  "${COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
  rm -rf "$DATA" && mkdir -p "$DATA"
}

# Samples each service's memory every 2 s into $DATA/mem.csv until stopped.
# Only this stack's containers: docker stats with no IDs samples every
# container on the machine.
sample_mem() {
  while true; do
    local ids
    ids=$("${COMPOSE[@]}" ps -q 2>/dev/null)
    if [ -n "$ids" ]; then
      # shellcheck disable=SC2086
      docker stats --no-stream --format '{{.Name}},{{.MemUsage}}' $ids 2>/dev/null \
        | sed "s/^/$(date +%s),/" >> "$DATA/mem.csv" || true
    fi
    sleep 2
  done
}

sys_count() {
  "${COMPOSE[@]}" exec -T mosquitto mosquitto_sub -t "\$SYS/broker/$1" -C 1 -W 5 2>/dev/null || echo "?"
}

# Readings the broker holds that SQLFlow has not acknowledged. The store
# count includes retained messages, and Mosquitto's own $SYS topics are
# retained: an idle broker's count rose from 2 to 55 in 12 s. The retained
# count tracks that exactly, so the difference is the readings alone.
backlog() {
  local stored retained
  stored=$(sys_count "store/messages/count")
  retained=$(sys_count "retained messages/count")
  if [ "$stored" = "?" ] || [ "$retained" = "?" ]; then
    echo "?"
    return
  fi
  echo $((stored - retained))
}

# Waits until SQLFlow has acknowledged every reading, or 600 s pass. Stopping
# SQLFlow with a backlog would report readings still queued as missing, so a
# drain that never finishes fails the scenario rather than the loss check.
drain() {
  for _ in $(seq 300); do
    [ "$(backlog)" = "0" ] && return 0
    sleep 2
  done
  echo "backlog of $(backlog) readings after 600 s; not checking for loss" >&2
  return 1
}

finish() {
  local name=$1
  "${COMPOSE[@]}" stop collector
  drain
  "${COMPOSE[@]}" stop sqlflow
  local check ok=0
  check=$(uv run -q --with "duckdb==$DUCKDB_PY" python scripts/mqtt_poc_check.py "$DATA") || ok=$?
  local collector
  collector=$("${COMPOSE[@]}" logs --no-log-prefix collector | tail -1)
  local peak
  peak=$(uv run -q --with "duckdb==$DUCKDB_PY" python scripts/mqtt_poc_check.py --peak-mem "$DATA/mem.csv")
  echo "{\"scenario\":\"$name\",\"check\":$check,\"collector\":$collector,\"peak_mib\":$peak}" | tee "$DATA/../report-$name.json"
  return "$ok"
}

# Starts the stack in the order the delivery contract needs. MQTT drops a
# publish that matches no subscription, so the collector starts only after
# SQLFlow's session exists.
start_stack() {
  local rate=$1 duration=$2
  RATE=$rate DURATION=$duration "${COMPOSE[@]}" up -d --build mosquitto sqlflow >>"$DATA/compose.log" 2>&1
  local subscribed=""
  for _ in $(seq 60); do
    if "${COMPOSE[@]}" logs sqlflow 2>/dev/null | grep -q "mqtt subscribed"; then
      subscribed=1
      break
    fi
    sleep 1
  done
  if [ -z "$subscribed" ]; then
    echo "SQLFlow did not subscribe within 60 s; see $DATA/compose.log" >&2
    return 1
  fi
  RATE=$rate DURATION=$duration "${COMPOSE[@]}" up -d --no-deps collector >>"$DATA/compose.log" 2>&1
}

run_steady() {
  reset; sample_mem & local mp=$!
  start_stack "${RATE:-1000}" 600
  sleep 610
  kill $mp; wait $mp 2>/dev/null || true; finish steady
}

run_crash() {
  reset; sample_mem & local mp=$!
  start_stack "${RATE:-1000}" 120
  sleep 40
  "${COMPOSE[@]}" kill -s KILL sqlflow
  local q_before; q_before=$(backlog)
  sleep 20
  local q_after; q_after=$(backlog)
  echo "backlog while SQLFlow was down: $q_before -> $q_after" | tee "$DATA/queue.txt"
  "${COMPOSE[@]}" start sqlflow
  sleep 70
  kill $mp; wait $mp 2>/dev/null || true; finish crash
}

run_broker() {
  reset; sample_mem & local mp=$!
  start_stack "${RATE:-1000}" 120
  sleep 40
  "${COMPOSE[@]}" restart mosquitto
  sleep 90
  kill $mp; wait $mp 2>/dev/null || true; finish broker
}

# Steps the rate up and records, for each step, the collector's achieved rate
# and whether the broker queue stayed flat. The highest step whose queue
# stayed below one batch is the sustained ceiling.
run_ceiling() {
  : > "$DATA/../ceiling.txt"
  for rate in 1000 2000 5000 10000 20000; do
    reset
    start_stack "$rate" 60
    sleep 30; local q1; q1=$(backlog)
    sleep 30; local q2; q2=$(backlog)
    "${COMPOSE[@]}" stop collector
    local achieved
    achieved=$("${COMPOSE[@]}" logs --no-log-prefix collector | tail -1)
    echo "rate=$rate backlog_30s=$q1 backlog_60s=$q2 collector=$achieved" | tee -a "$DATA/../ceiling.txt"
  done
  reset
}

case "$SCENARIO" in
  steady) run_steady ;;
  crash) run_crash ;;
  broker) run_broker ;;
  ceiling) run_ceiling ;;
  all) run_crash; run_broker; run_steady; run_ceiling ;;
  *) echo "unknown scenario $SCENARIO" >&2; exit 2 ;;
esac
"${COMPOSE[@]}" down -v >/dev/null 2>&1 || true
