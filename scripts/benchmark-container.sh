#!/usr/bin/env bash
set -euo pipefail

# Benchmarks sqlflow from INSIDE the docker network. Docker Desktop's
# host->container port-forwarding caps Kafka fetches at ~10-15MB/s, which
# starves the pipeline and understates throughput ~10x. Running sqlflow on the
# same docker network as the broker measures the engine, not the NAT.

NUM_MESSAGES="${1:-300000}"
BATCH_SIZE="${2:-5000}"
# A fresh topic per run keeps runs hermetic. Reusing one accumulates a backlog
# past --max-msgs, and the consumer prefetches into it: throughput and peak
# memory then depend on how many benchmarks ran before this one.
TOPIC="${BENCH_TOPIC:-benchmark-$(date +%s)}"
# Third arg / CONFIG env selects the pipeline under test; the default is the
# structured handler, the fastest path. benchmark.inferred.mem.yml measures
# schema inference instead.
CONFIG="${3:-${CONFIG:-dev/config/examples/benchmark.structured.mem.yml}}"
NETWORK="dev_default"
GO_IMAGE="golang:1.25-bookworm"
RUN_IMAGE="debian:bookworm-slim"

echo "=== sqlflow benchmark (in-network) ==="
echo "Messages:   $NUM_MESSAGES"
echo "Batch size: $BATCH_SIZE"
echo "Config:     $CONFIG"
echo ""

# 1. Check Kafka is running
echo "--- Checking Kafka ---"
if ! docker ps --format '{{.Names}}' | grep -q '^kafka1$'; then
    echo "Kafka not running. Starting backing services..."
    make start-backing-services
    echo "Waiting for Kafka to be ready..."
    sleep 15
else
    echo "Kafka is running."
fi

# 2. Build sqlflow for linux inside a container (CGO for ADBC)
echo "--- Building sqlflow (linux) ---"
docker run --rm \
    -v "$PWD":/src \
    -v "$(go env GOMODCACHE)":/gomod \
    -e GOMODCACHE=/gomod -e CGO_ENABLED=1 -e GOFLAGS=-buildvcs=false \
    -e GOTOOLCHAIN=auto \
    -w /src "$GO_IMAGE" \
    go build -o bin/sqlflow-linux ./cmd/sqlflow/
echo "Built bin/sqlflow-linux"

# 3. Fetch libduckdb.so for linux (cached in bin/)
./scripts/install-libduckdb.sh bin libduckdb-linux.so

# 4. Publish test messages
echo "--- Publishing $NUM_MESSAGES messages to topic '$TOPIC' ---"
PYTHON="${SQLFLOW_PYTHON:-python3}"
"$PYTHON" cmd/publish-test-data.py --num-messages="$NUM_MESSAGES" --topic="$TOPIC"
echo "Publishing complete."

# 5. Run sqlflow in-network with a unique consumer group so re-runs start fresh
GROUP_ID="benchmark-$(date +%s)"
echo ""
echo "--- Running sqlflow in-network (batch_size=$BATCH_SIZE, group=$GROUP_ID) ---"
echo ""

docker run --rm --network "$NETWORK" \
    -v "$PWD/bin/sqlflow-linux":/sqlflow \
    -v "$PWD/bin/libduckdb-linux.so":/duckdb/libduckdb.so \
    -v "$PWD/dev":/dev-config \
    -e SQLFLOW_KAFKA_BROKERS=kafka1:19092 \
    -e SQLFLOW_DUCKDB_LIB=/duckdb/libduckdb.so \
    -e SQLFLOW_GROUP_ID="$GROUP_ID" \
    -e SQLFLOW_TOPIC="$TOPIC" \
    -e SQLFLOW_BATCH_SIZE="$BATCH_SIZE" \
    "$RUN_IMAGE" \
    sh -c '
        # Two memory numbers, sampled at 100ms because memory.peak needs
        # kernel 5.19+ (Docker Desktop LinuxKit is older):
        #
        #   peak_memory_bytes      cgroup memory.current -- everything the
        #                          container is charged for, page cache and
        #                          Go lazily-freed (MADV_FREE) pages
        #                          included. The provisioning ceiling.
        #   peak_anon_bytes        the anon line of memory.stat -- the
        #                          process working set, comparable to RSS.
        #                          The number that reflects the engine.
        #
        # Sampling is accurate for this workload: batch memory is a
        # sustained plateau, not a spike.
        cur=/sys/fs/cgroup/memory.current
        stat=/sys/fs/cgroup/memory.stat
        [ -r "$cur" ] || { cur=/sys/fs/cgroup/memory/memory.usage_in_bytes; stat=/sys/fs/cgroup/memory/memory.stat; }
        ( while [ -r "$cur" ]; do cat "$cur"; sleep 0.1; done ) > /tmp/mem.samples 2>/dev/null &
        s1=$!
        ( while [ -r "$stat" ]; do awk "/^(anon|rss) /{print \$2}" "$stat"; sleep 0.1; done ) > /tmp/anon.samples 2>/dev/null &
        s2=$!

        /sqlflow run -c "$1" --max-msgs="$2"
        ec=$?

        kill "$s1" "$s2" 2>/dev/null
        peak=""
        [ -r /sys/fs/cgroup/memory.peak ] && peak=$(cat /sys/fs/cgroup/memory.peak)
        [ -n "$peak" ] || peak=$(sort -n /tmp/mem.samples | tail -1)
        [ -n "$peak" ] && echo "peak_memory_bytes: $peak ($((peak / 1048576)) MiB)"
        anon=$(sort -n /tmp/anon.samples | tail -1)
        [ -n "$anon" ] && echo "peak_anon_bytes: $anon ($((anon / 1048576)) MiB)"
        exit $ec' \
    sh "/dev-config/${CONFIG#dev/}" "$NUM_MESSAGES" 2>&1 | tee /tmp/sqlflow-benchmark.log

echo ""
echo "=== Benchmark Complete ==="
echo ""
echo "Final throughput:"
grep "total_throughput_per_second" /tmp/sqlflow-benchmark.log | tail -1
echo "Peak memory:"
grep "peak_memory_bytes" /tmp/sqlflow-benchmark.log | tail -1
grep "peak_anon_bytes" /tmp/sqlflow-benchmark.log | tail -1
