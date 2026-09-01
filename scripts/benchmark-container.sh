#!/usr/bin/env bash
set -euo pipefail

# Benchmarks sqlflow from INSIDE the docker network. Docker Desktop's
# host->container port-forwarding caps Kafka fetches at ~10-15MB/s, which
# starves the pipeline and understates throughput ~10x. Running sqlflow on the
# same docker network as the broker measures the engine, not the NAT.

NUM_MESSAGES="${1:-300000}"
BATCH_SIZE="${2:-5000}"
TOPIC="benchmark-input"
CONFIG="dev/config/examples/benchmark.structured.mem.yml"
NETWORK="dev_default"
GO_IMAGE="golang:1.25-bookworm"
RUN_IMAGE="debian:bookworm-slim"

echo "=== Turbine Go Benchmark (in-network) ==="
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
    /sqlflow run -c "/dev-config/${CONFIG#dev/}" --max-msgs="$NUM_MESSAGES" 2>&1 | tee /tmp/sqlflow-benchmark.log

echo ""
echo "=== Benchmark Complete ==="
echo ""
echo "Final throughput:"
grep "total_throughput_per_second" /tmp/sqlflow-benchmark.log | tail -1
