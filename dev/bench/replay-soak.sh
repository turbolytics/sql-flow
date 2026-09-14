#!/usr/bin/env bash
# Run a pipeline container under Render's limits and decompose its memory
# once a minute.
#
#   dev/bench/replay-soak.sh <image> <config.yml> <minutes> <label>
#
# The sampling is scripts/soak.sh's: RssAnon, Go retained, native, and
# duckdb_memory(). What differs is the setting. This runs one pipeline
# against a source you started yourself, usually dev/bench/replay, with the
# container held to the plan the production worker runs on, because DuckDB
# sizes its buffer pool and threads from what the container can see.
#
# Writes bench-<label>/decomp.csv and bench-<label>/sqlflow.log.
#
# Environment:
#   BENCH_NETWORK   docker network                      (default dev_default)
#   BENCH_CPUS      container CPU limit                 (default 0.5)
#   BENCH_MEMORY    container memory limit              (default 512m)
#   BENCH_PPROF     host port for pprof                 (default 6060)
#   BENCH_PG        psql connection string; when set, each sample counts the
#                   rows in posts_per_minute_by_lang
#   BENCH_ENV       extra NAME=value pairs for the container, space separated,
#                   e.g. "MALLOC_ARENA_MAX=2"
#   Every SQLFLOW_* variable is passed to the container.
set -euo pipefail

image=${1:?image}; config=${2:?config.yml}; minutes=${3:?minutes}; label=${4:?label}
net=${BENCH_NETWORK:-dev_default}
cpus=${BENCH_CPUS:-0.5}; memory=${BENCH_MEMORY:-512m}
pprof_port=${BENCH_PPROF:-6060}
name="sqlflow-bench-$label"
out="bench-$label"; mkdir -p "$out"

env_args=()
while IFS= read -r v; do env_args+=(-e "$v"); done < <(env | cut -d= -f1 | grep '^SQLFLOW_' || true)
for v in ${BENCH_ENV:-}; do env_args+=(-e "$v"); done

docker rm -f "$name" >/dev/null 2>&1 || true
docker run -d --name "$name" --network "$net" \
  --cpus "$cpus" --memory "$memory" \
  --add-host host.docker.internal:host-gateway \
  -p "$pprof_port:6060" \
  ${env_args[@]+"${env_args[@]}"} \
  -v "$(cd "$(dirname "$config")" && pwd)":/conf \
  "$image" run "/conf/$(basename "$config")" --pprof --with-http-debug >/dev/null
echo "started $name from $image ($cpus CPU, $memory); sampling for $minutes minutes into $out/"

ddb_query() {
  docker run --rm --network "container:$name" curlimages/curl:latest -s --max-time 8 \
    "http://127.0.0.1:5000/debug?sql=$1" 2>/dev/null
}

echo "min,rss_anon_mib,rss_file_mib,cgroup_mib,go_retained_mib,native_mib,heap_alloc_mib,duckdb_mib,duckdb_tables_mib,goroutines,msgs,pg_rows" > "$out/decomp.csv"
for m in $(seq 0 "$minutes"); do
  if ! docker inspect -f '{{.State.Running}}' "$name" 2>/dev/null | grep -q true; then
    echo "container stopped at minute $m"; break
  fi
  st=$(docker exec "$name" sh -c 'grep -E "^(RssAnon|RssFile)" /proc/1/status; cat /sys/fs/cgroup/memory.current' 2>/dev/null || true)
  ra=$(echo "$st" | awk '/RssAnon/{printf "%.1f", $2/1024}')
  rf=$(echo "$st" | awk '/RssFile/{printf "%.1f", $2/1024}')
  cg=$(echo "$st" | awk '/^[0-9]+$/{printf "%.1f", $1/1048576}')
  ms=$(curl -s --max-time 15 "http://localhost:$pprof_port/debug/pprof/heap?debug=1" 2>/dev/null | tail -40 || true)
  sys=$(echo "$ms" | awk '/^# Sys =/{printf "%.1f", $4/1048576}')
  rel=$(echo "$ms" | awk '/^# HeapReleased =/{printf "%.1f", $4/1048576}')
  ha=$(echo "$ms"  | awk '/^# HeapAlloc =/{printf "%.1f", $4/1048576}')
  ret=$(awk -v s="${sys:-0}" -v r="${rel:-0}" 'BEGIN{printf "%.1f", s-r}')
  nat=$(awk -v a="${ra:-0}" -v g="$ret" 'BEGIN{printf "%.1f", a-g}')
  ddb=$(ddb_query "SELECT%20round(sum(memory_usage_bytes)/1048576.0,2)%20FROM%20duckdb_memory()" | grep -oE '[0-9.]+' | head -1 || true)
  ddt=$(ddb_query "SELECT%20round(coalesce(sum(memory_usage_bytes),0)/1048576.0,2)%20FROM%20duckdb_memory()%20WHERE%20tag%20IN%20('IN_MEMORY_TABLE','BASE_TABLE')" | grep -oE '[0-9.]+' | head -1 || true)
  gor=$(curl -s --max-time 10 "http://localhost:$pprof_port/debug/pprof/goroutine?debug=1" 2>/dev/null | head -1 | grep -oE '[0-9]+' | head -1 || true)
  msgs=$(docker logs "$name" 2>&1 | grep -oE '"messages_consumed": [0-9]+' | tail -1 | grep -oE '[0-9]+' || true)
  pg=""
  if [ -n "${BENCH_PG:-}" ]; then
    pg=$(psql "$BENCH_PG" -Atc "SELECT count(*) FROM posts_per_minute_by_lang" 2>/dev/null || true)
  fi
  echo "$m,${ra:-NA},${rf:-NA},${cg:-NA},$ret,$nat,${ha:-NA},${ddb:-NA},${ddt:-NA},${gor:-NA},${msgs:-0},${pg:-NA}" | tee -a "$out/decomp.csv"
  [ "$m" -lt "$minutes" ] && sleep 60
done
docker logs "$name" > "$out/sqlflow.log" 2>&1 || true
docker rm -f "$name" >/dev/null 2>&1 || true
echo "done: $out/decomp.csv"
