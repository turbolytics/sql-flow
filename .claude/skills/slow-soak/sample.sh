#!/usr/bin/env bash
# Sample a running slow-soak pipeline once a minute.
#
#   sample.sh <container-name> <out-dir> <minutes>
#
# Columns: t, phase, produced, landed, max_latency_s, lag, anon_mib,
# state_bytes, published_windows, healthz. One line per sample to stdout and
# to samples.csv; everything verbose stays in the container's own log.
set -uo pipefail
name=${1:?container}; out=${2:?out dir}; minutes=${3:?minutes}
mkdir -p "$out"
csv="$out/samples.csv"
echo "t,phase,produced,landed,max_latency_s,lag,anon_mib,state_bytes,published_windows,healthz" > "$csv"

# One sidecar per sample, not one per query. Five `docker run` calls a
# sample made an iteration take five minutes on a busy daemon, which
# silently destroyed the per-minute granularity every rule below depends on.
# This issues every query in one container and prints them tab separated.
#
# /debug answers [[v]] for a one-value query; /healthz answers a status code.
probe() {
  docker run --rm --network "container:$name" --entrypoint sh curlimages/curl:latest -c '
    q() { curl -s --max-time 10 --get --data-urlencode "sql=$1" http://127.0.0.1:5000/debug; }
    printf "%s\t" "$(q "SELECT coalesce((SELECT sum(count) FROM agg_slow),0) + coalesce((SELECT sum(count) FROM published),0)")"
    printf "%s\t" "$(q "SELECT round(greatest(coalesce((SELECT max(max_latency_s) FROM agg_slow),0), coalesce((SELECT max(max_latency_s) FROM published),0)),1)")"
    printf "%s\t" "$(q "SELECT coalesce(max(\"offset\"),-1) FROM sqlflow_offsets")"
    printf "%s\t" "$(q "SELECT count(*) FROM published")"
    printf "%s"   "$(curl -s -o /dev/null -w "%{http_code}" --max-time 5 http://127.0.0.1:8000/healthz)"
  ' 2>/dev/null
}

# scalar pulls the value out of [[v]], or empty when the query failed.
scalar() {
  python3 -c '
import json,sys
try:
    r = json.loads(sys.argv[1])
    print(r[0][0] if r and r[0] and r[0][0] is not None else "")
except Exception:
    print("")' "$1" 2>/dev/null
}

for ((i = 0; i < minutes; i++)); do
  t=$(date -u +%H:%M:%SZ)
  phase=$(grep -o 'phase=[a-z0-9]*' "$out/profile.log" 2>/dev/null | tail -1 | cut -d= -f2)
  produced=$(grep -o 'total=[0-9]*' "$out/profile.log" 2>/dev/null | tail -1 | cut -d= -f2)

  IFS=$'\t' read -r r_landed r_lat r_off r_pub r_hz <<< "$(probe)"
  landed=$(scalar "$r_landed")
  lat=$(scalar "$r_lat")
  committed=$(scalar "$r_off")
  pub=$(scalar "$r_pub")
  hz=$r_hz

  lag=$(( ${produced:-0} - 1 - ${committed:--1} ))
  [ "${produced:-0}" -eq 0 ] && lag=0

  anon=$(docker exec "$name" sh -c 'awk "/^anon /{print int(\$2/1048576)}" /sys/fs/cgroup/memory.stat 2>/dev/null' 2>/dev/null)
  state=$(docker exec "$name" sh -c 'stat -c %s /conf/state/slow.db 2>/dev/null' 2>/dev/null)

  echo "$t,${phase:-},${produced:-0},${landed:-},${lat:-},${lag},${anon:-},${state:-},${pub:-},${hz:-}" | tee -a "$csv"
  sleep 60
done
echo "SAMPLING_DONE" | tee -a "$csv"
