#!/usr/bin/env bash
# The memory gate a pull request has to pass.
#
#   scripts/soak.sh [minutes] [label]
#
# Runs the shipped image against a saturated Kafka topic, polls
# /turbostats/v1 once a second for the whole run, samples the memory
# decomposition once a minute, and prints a verdict.
#
# Why a soak and not a unit test. Go's heap profiler cannot see a buffer
# DuckDB allocated across the ADBC boundary, and duckdb_memory() does not
# track it either. Only the process can, and only over enough messages for a
# per-message leak to clear the noise. The leak this gate exists to catch was
# 44 bytes a message: invisible in a benchmark that finishes in a second, and
# 90 MiB over a day in production.
#
# Why high volume. A leak is linear in messages, not in wall clock. Ten
# minutes at 500 messages a second is 300k messages and proves little; the
# same ten minutes saturated is tens of millions.
#
# Why it polls the endpoint. /turbostats/v1 builds a bundle per request and
# walks the metric data to do it. A handler that allocates per request leaks
# in proportion to requests, which the once-a-minute sampler would take hours
# to show.
#
# Environment:
#   SQLFLOW_IMAGE   image under test        (default: git describe tag)
#   SOAK_NETWORK    docker network          (default dev_default)
#   SOAK_KAFKA      broker container name   (default kafka1)
#   SOAK_BROKER     broker from inside it   (default kafka1:19092)
#   SOAK_BYTES_PER_MSG  failing threshold   (default 1.0)
set -euo pipefail

minutes=${1:-10}
label=${2:-pr}
here=$(cd "$(dirname "$0")/.." && pwd)
skill="$here/.claude/skills/memory-soak"

image=${SQLFLOW_IMAGE:-turbolytics/sql-flow:$(git -C "$here" describe --tags --always --dirty)}
net=${SOAK_NETWORK:-dev_default}
kafka=${SOAK_KAFKA:-kafka1}
broker=${SOAK_BROKER:-kafka1:19092}
topic="soak-gate-$(date +%s)"

if ! docker image inspect "$image" >/dev/null 2>&1; then
  echo "image $image not present; run 'make sqlflow-image' first" >&2
  exit 1
fi

# Two samplers writing one decomp.csv interleave their rows, and the verdict
# then reads a file whose minutes do not ascend. Refuse rather than produce a
# result that looks real. Seen once, by running the gate twice with the same
# label after the first run's sampler survived its shell.
if [ -e "soak-$label/decomp.csv" ]; then
  echo "soak-$label/decomp.csv exists. Remove it, or pass another label." >&2
  exit 1
fi
if docker ps --format '{{.Names}}' | grep -qx "sqlflow-soak-$label"; then
  echo "sqlflow-soak-$label is already running. Stop it first." >&2
  exit 1
fi

echo "image:   $image"
echo "topic:   $topic"
echo "minutes: $minutes"
echo

# A saturated producer, restarted for the length of the run. --throughput -1
# means as fast as the broker will take it, so the pipeline is never waiting
# for input and the message count is the largest the hardware allows.
docker exec "$kafka" kafka-topics --bootstrap-server "$broker" \
  --create --topic "$topic" --partitions 1 --replication-factor 1 >/dev/null 2>&1 || true

# A payload file, not --record-size: the perf producer's own records are
# random bytes, and the pipeline rejects them as malformed JSON, which is
# correct of it. One line per record, cycled.
docker exec "$kafka" sh -c "
  awk 'BEGIN{
    srand(7);
    for (i = 0; i < 1000; i++)
      printf \"{\\\"sensor_id\\\":%d,\\\"ts\\\":\\\"2026-09-11T00:00:00\\\",\\\"value\\\":%.2f}\n\",
             int(rand()*5)+1, rand()*50-10
  }' > /tmp/soak-payload.json"

docker exec -d "$kafka" sh -c "
  for i in \$(seq 1 $((minutes + 2))); do
    kafka-producer-perf-test --topic $topic \
      --num-records 4000000 --throughput -1 \
      --payload-file /tmp/soak-payload.json --payload-delimiter '\n' \
      --producer-props bootstrap.servers=$broker acks=1 >/dev/null 2>&1
  done"
echo "producing to $topic at line rate"

config="$here/dev/config/soak/inferred.noop.yml"
if [ ! -f "$config" ]; then
  echo "missing $config" >&2
  exit 1
fi

# noop sink on purpose: this gate is about the engine. A sink's own buffers
# are its conformance suite's business, and a destination that falls behind
# would be indistinguishable from a leak here.
SQLFLOW_TOPIC="$topic" \
SQLFLOW_GROUP_ID="$topic" \
SQLFLOW_KAFKA_BROKERS="$broker" \
SQLFLOW_BATCH_SIZE=5000 \
SOAK_ENV="SQLFLOW_TOPIC SQLFLOW_GROUP_ID SQLFLOW_KAFKA_BROKERS SQLFLOW_BATCH_SIZE" \
SOAK_NETWORK="$net" \
SOAK_FLAGS="--turbostats" \
SOAK_POLL="/turbostats/v1" \
  bash "$skill/soak.sh" "$image" "$config" "$minutes" "$label"

echo
"$here/scripts/soak-verdict.py" "soak-$label/decomp.csv" \
  --max-bytes-per-msg "${SOAK_BYTES_PER_MSG:-1.0}"
