#!/usr/bin/env bash
set -euo pipefail

NUM_MESSAGES="${1:-100000}"
BATCH_SIZE="${2:-500}"
PPROF="${3:-}"
TOPIC="benchmark-input"
CONFIG="dev/config/examples/benchmark.structured.mem.yml"
BINARY="bin/turbine"

echo "=== Turbine Go Benchmark ==="
echo "Messages:   $NUM_MESSAGES"
echo "Batch size: $BATCH_SIZE"
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

# 2. Build turbine
echo "--- Building turbine ---"
go build -o "$BINARY" ./cmd/turbine/
echo "Built $BINARY"

# 3. Publish test messages
echo "--- Publishing $NUM_MESSAGES messages to topic '$TOPIC' ---"
PYTHON="${SQLFLOW_PYTHON:-python3}"
"$PYTHON" cmd/publish-test-data.py --num-messages="$NUM_MESSAGES" --topic="$TOPIC"
echo "Publishing complete."

# 4. Run turbine with a unique consumer group so re-runs start fresh
GROUP_ID="benchmark-$(date +%s)"
echo ""
echo "--- Running turbine (batch_size=$BATCH_SIZE, group=$GROUP_ID) ---"
echo ""

PPROF_FLAG=""
if [ -n "$PPROF" ]; then
    PPROF_FLAG="--pprof"
    echo "(pprof enabled on :6060)"
fi

SQLFLOW_BATCH_SIZE="$BATCH_SIZE" \
SQLFLOW_GROUP_ID="$GROUP_ID" \
SQLFLOW_TOPIC="$TOPIC" \
  "$BINARY" run -c "$CONFIG" --max-msgs="$NUM_MESSAGES" $PPROF_FLAG 2>&1 | tee /tmp/turbine-benchmark.log

echo ""
echo "=== Benchmark Complete ==="
echo ""
echo "Final throughput:"
grep "throughput" /tmp/turbine-benchmark.log | tail -1
