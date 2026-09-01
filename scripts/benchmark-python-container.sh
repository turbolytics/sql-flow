#!/usr/bin/env bash
set -euo pipefail

# Benchmarks the LEGACY PYTHON ENGINE from inside the docker network, mirroring
# scripts/benchmark-container.sh (the Go engine) run-for-run: same broker, same
# network, same config, same fresh-topic/fresh-group hygiene, same memory
# sampling. Numbers from the two scripts are directly comparable.

NUM_MESSAGES="${1:-300000}"
BATCH_SIZE="${2:-5000}"
# A fresh topic per run keeps runs hermetic. Reusing one accumulates a backlog
# past --max-msgs-to-process, and the consumer prefetches into it.
TOPIC="${BENCH_TOPIC:-benchmark-$(date +%s)}"
CONFIG="${3:-${CONFIG:-dev/config/examples/benchmark.structured.mem.yml}}"
NETWORK="dev_default"
# Built for the HOST arch, unlike `make docker-image` which pins amd64 for
# publishing: under QEMU emulation the Python engine would measure the
# emulator, not the engine.
IMAGE="turbolytics/sql-flow:python-bench"

echo "=== sqlflow PYTHON benchmark (in-network) ==="
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

# 2. Build the python engine image (native arch)
echo "--- Building python engine image ---"
docker build -f Dockerfile.python -t "$IMAGE" .

# 3. Publish test messages
echo "--- Publishing $NUM_MESSAGES messages to topic '$TOPIC' ---"
PYTHON="${SQLFLOW_PYTHON:-python3}"
"$PYTHON" cmd/publish-test-data.py --num-messages="$NUM_MESSAGES" --topic="$TOPIC"
echo "Publishing complete."

# 4. Run the python engine in-network with a unique consumer group
GROUP_ID="benchmark-$(date +%s)"
echo ""
echo "--- Running python sqlflow in-network (batch_size=$BATCH_SIZE, group=$GROUP_ID) ---"
echo ""

docker run --rm --network "$NETWORK" \
    -v "$PWD/dev":/dev-config \
    -e SQLFLOW_KAFKA_BROKERS=kafka1:19092 \
    -e SQLFLOW_GROUP_ID="$GROUP_ID" \
    -e SQLFLOW_TOPIC="$TOPIC" \
    -e SQLFLOW_BATCH_SIZE="$BATCH_SIZE" \
    --entrypoint sh \
    "$IMAGE" \
    -c '
        # Same two memory numbers as the Go benchmark, sampled at 100ms:
        #   peak_memory_bytes  cgroup memory.current -- the provisioning ceiling
        #   peak_anon_bytes    anon from memory.stat -- the process working set
        cur=/sys/fs/cgroup/memory.current
        stat=/sys/fs/cgroup/memory.stat
        [ -r "$cur" ] || { cur=/sys/fs/cgroup/memory/memory.usage_in_bytes; stat=/sys/fs/cgroup/memory/memory.stat; }
        ( while [ -r "$cur" ]; do cat "$cur"; sleep 0.1; done ) > /tmp/mem.samples 2>/dev/null &
        s1=$!
        ( while [ -r "$stat" ]; do awk "/^(anon|rss) /{print \$2}" "$stat"; sleep 0.1; done ) > /tmp/anon.samples 2>/dev/null &
        s2=$!

        python cmd/sql-flow.py run "$1" --max-msgs-to-process="$2"
        ec=$?

        kill "$s1" "$s2" 2>/dev/null
        peak=""
        [ -r /sys/fs/cgroup/memory.peak ] && peak=$(cat /sys/fs/cgroup/memory.peak)
        [ -n "$peak" ] || peak=$(sort -n /tmp/mem.samples | tail -1)
        [ -n "$peak" ] && echo "peak_memory_bytes: $peak ($((peak / 1048576)) MiB)"
        anon=$(sort -n /tmp/anon.samples | tail -1)
        [ -n "$anon" ] && echo "peak_anon_bytes: $anon ($((anon / 1048576)) MiB)"
        exit $ec' \
    sh "/dev-config/${CONFIG#dev/}" "$NUM_MESSAGES" 2>&1 | tee /tmp/sqlflow-python-benchmark.log

echo ""
echo "=== Benchmark Complete ==="
echo ""
echo "Final throughput:"
grep "total messages / sec" /tmp/sqlflow-python-benchmark.log | tail -1
echo "Peak memory:"
grep "peak_memory_bytes" /tmp/sqlflow-python-benchmark.log | tail -1
grep "peak_anon_bytes" /tmp/sqlflow-python-benchmark.log | tail -1
