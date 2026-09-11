#!/usr/bin/env bash
# Run a pipeline under steady load and decompose its memory once a minute.
#
#   soak.sh <image> <config.yml> <minutes> <label>
#
# Writes soak-<label>/decomp.csv and one heap profile per minute. Feed the
# source yourself at a steady rate (see producer.sh); a burst that drains in
# seconds measures nothing.
#
# Environment:
#   SOAK_NETWORK   docker network the source lives on   (default dev_default)
#   SOAK_ENV       names of env vars to pass through     (default: every SQLFLOW_*;
#                  set to "" to pass nothing)
#   SOAK_PPROF     host port for pprof                   (default 6060)
#   SOAK_METRICS   host port for prometheus              (default 8000)
#   SOAK_FLAGS     extra run flags, e.g. "--turbostats"  (default none)
#   SOAK_POLL      path polled once a second for the whole run, e.g.
#                  "/turbostats/v1". A read path that allocates is a leak
#                  the sampler's own once-a-minute scrape would never find.
set -euo pipefail

image=${1:?image}; config=${2:?config.yml}; minutes=${3:?minutes}; label=${4:?label}
net=${SOAK_NETWORK:-dev_default}
pprof_port=${SOAK_PPROF:-6060}; metrics_port=${SOAK_METRICS:-8000}
name="sqlflow-soak-$label"
out="soak-$label"; mkdir -p "$out"

env_args=()
if [ -n "${SOAK_ENV:-}" ]; then
  for v in $SOAK_ENV; do env_args+=(-e "$v"); done
else
  while IFS= read -r v; do env_args+=(-e "$v"); done < <(env | cut -d= -f1 | grep '^SQLFLOW_' || true)
fi

docker rm -f "$name" >/dev/null 2>&1 || true
docker run -d --name "$name" --network "$net" \
  -p "$pprof_port:6060" -p "$metrics_port:8000" \
  ${env_args[@]+"${env_args[@]}"} \
  -v "$(cd "$(dirname "$config")" && pwd)":/conf \
  "$image" run "/conf/$(basename "$config")" --pprof --metrics=prometheus --with-http-debug \
  ${SOAK_FLAGS:-} >/dev/null
echo "started $name from $image; sampling for $minutes minutes into $out/"

# Poll a read path for the whole run, if one was named. A handler that
# allocates per request leaks in proportion to requests, and a sampler that
# scrapes once a minute would take hours to show it.
poll_pid=""
if [ -n "${SOAK_POLL:-}" ]; then
  (
    while true; do
      curl -s --max-time 5 -o /dev/null \
        "http://localhost:$metrics_port$SOAK_POLL" 2>/dev/null || true
      sleep 1
    done
  ) &
  poll_pid=$!
  trap 'kill $poll_pid 2>/dev/null || true' EXIT
  echo "polling $SOAK_POLL once a second"
fi

# The debug endpoint binds container-localhost, so query it from a sidecar
# that shares the network namespace.
ddb_query() {
  docker run --rm --network "container:$name" curlimages/curl:latest -s --max-time 8 \
    "http://127.0.0.1:5000/debug?sql=$1" 2>/dev/null
}

echo "min,rss_anon_mib,rss_file_mib,go_sys_mib,go_released_mib,go_retained_mib,native_mib,heap_alloc_mib,duckdb_mib,goroutines,msgs" > "$out/decomp.csv"
for m in $(seq 0 "$minutes"); do
  tag=$(printf %02d "$m")
  curl -s --max-time 20 "http://localhost:$pprof_port/debug/pprof/heap" > "$out/heap-$tag.pb.gz" 2>/dev/null || true
  st=$(docker exec "$name" sh -c 'grep -E "^(RssAnon|RssFile)" /proc/1/status' 2>/dev/null || true)
  ra=$(echo "$st" | awk '/RssAnon/{printf "%.1f", $2/1024}')
  rf=$(echo "$st" | awk '/RssFile/{printf "%.1f", $2/1024}')
  ms=$(curl -s --max-time 15 "http://localhost:$pprof_port/debug/pprof/heap?debug=1" 2>/dev/null | tail -40 || true)
  sys=$(echo "$ms" | awk '/^# Sys =/{printf "%.1f", $4/1048576}')
  rel=$(echo "$ms" | awk '/^# HeapReleased =/{printf "%.1f", $4/1048576}')
  ha=$(echo "$ms"  | awk '/^# HeapAlloc =/{printf "%.1f", $4/1048576}')
  ret=$(awk -v s="${sys:-0}" -v r="${rel:-0}" 'BEGIN{printf "%.1f", s-r}')
  nat=$(awk -v a="${ra:-0}" -v g="$ret" 'BEGIN{printf "%.1f", a-g}')
  ddb=$(ddb_query "SELECT%20round(sum(memory_usage_bytes)/1048576.0,2)%20FROM%20duckdb_memory()" | grep -oE '[0-9.]+' | head -1 || true)
  gor=$(curl -s --max-time 10 "http://localhost:$pprof_port/debug/pprof/goroutine?debug=1" 2>/dev/null | head -1 | grep -oE '[0-9]+' | head -1 || true)
  msgs=$(docker logs "$name" 2>&1 | grep -oE '"messages_consumed": [0-9]+' | tail -1 | grep -oE '[0-9]+' || true)
  echo "$m,${ra:-NA},${rf:-NA},${sys:-NA},${rel:-NA},$ret,$nat,${ha:-NA},${ddb:-NA},${gor:-NA},${msgs:-0}" >> "$out/decomp.csv"
  [ "$m" -lt "$minutes" ] && sleep 60
done
[ -n "$poll_pid" ] && kill "$poll_pid" 2>/dev/null || true
echo "done: $out/decomp.csv"
