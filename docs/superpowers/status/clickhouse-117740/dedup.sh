#!/bin/bash
# dedup.sh: what a replayed batch does to each table engine, on self-hosted,
# non-replicated ClickHouse. A replay is the same two messages consumed twice
# by two consumer groups, so the sink sends the same block twice. IMG selects
# the image; DSN the server (its database must hold the tables). Each run
# writes a config of its own: a file rewritten within a second was once read
# stale through the Docker Desktop bind mount, and named a truncated table.
W="$(cd "$(dirname "$0")" && pwd)"
IMG="${IMG:-turbolytics/sql-flow:v2026.10.08}"
DSN="${DSN:-clickhouse://default@clickhouse:8123/probe}"
ch() { docker exec clickhouse clickhouse-client -q "$1" 2>&1; }
kt() { docker exec kafka1 kafka-topics --bootstrap-server localhost:9092 "$@" 2>&1 | tail -1; }

# run <table> <v-expression>: consume the two messages once, into <table>.
run() {
  local table="$1" vexpr="$2" g="dedup-$1-$RANDOM"
  n=$((n + 1)); f="d_${table}_$n"
  cat > "$W/$f.yml" <<YML
pipeline:
  batch_size: 2
  source:
    type: kafka
    kafka:
      brokers: [kafka1:19092]
      group_id: $g
      auto_offset_reset: earliest
      topics:
        - ch-dedup
  handler:
    type: 'handlers.InferredMemBatch'
    sql: |
      SELECT id::INTEGER AS id, $vexpr AS v FROM batch
  sink:
    type: clickhouse
    clickhouse:
      dsn: {{ SQLFLOW_CLICKHOUSE_DSN }}
      table: $table
YML
  docker run --rm --network dev_default -v "$W":/conf -e SQLFLOW_CLICKHOUSE_DSN="$DSN" \
    "$IMG" run /conf/$f.yml --max-msgs=2 > "$W/$f.log" 2>&1
  echo "  run into $table (v = $vexpr): exit=$?"
}
report() {
  local t="$1"
  echo "  count()        $(ch "SELECT count() FROM probe.$t")"
  [ "$2" = final ] && echo "  count() FINAL  $(ch "SELECT count() FROM probe.$t FINAL")"
  echo "  active parts   $(ch "SELECT count() FROM system.parts WHERE database = 'probe' AND table = '$t' AND active")"
  echo "  rows           $(ch "SELECT groupArray((id, v)) FROM (SELECT id, v FROM probe.$t ORDER BY id, v)")"
}

echo "image: $IMG"
ch "SELECT version()"
kt --delete --if-exists --topic ch-dedup; sleep 2
kt --create --topic ch-dedup --partitions 1 --replication-factor 1
printf '{"id":1,"v":"first"}\n{"id":2,"v":"first"}\n' | docker exec -i kafka1 kafka-console-producer --bootstrap-server localhost:9092 --topic ch-dedup

for t in rmt_distinct rmt_replay mt_replay mt_dedup_replay; do ch "DROP TABLE IF EXISTS probe.$t"; done
ch "CREATE TABLE probe.rmt_distinct    (id Int32, v String) ENGINE = ReplacingMergeTree ORDER BY id"
ch "CREATE TABLE probe.rmt_replay      (id Int32, v String) ENGINE = ReplacingMergeTree ORDER BY id"
ch "CREATE TABLE probe.mt_replay       (id Int32, v String) ENGINE = MergeTree ORDER BY id"
ch "CREATE TABLE probe.mt_dedup_replay (id Int32, v String) ENGINE = MergeTree ORDER BY id SETTINGS non_replicated_deduplication_window = 100"

echo
echo "=== ReplacingMergeTree, two distinct blocks with the same keys (a newer version of each row)"
run rmt_distinct "v"; run rmt_distinct "'second'"
report rmt_distinct final
ch "OPTIMIZE TABLE probe.rmt_distinct FINAL"
echo "  after OPTIMIZE ... FINAL: count() $(ch 'SELECT count() FROM probe.rmt_distinct'), rows $(ch "SELECT groupArray((id, v)) FROM (SELECT id, v FROM probe.rmt_distinct ORDER BY id)")"

echo
echo "=== ReplacingMergeTree, the identical block twice (a replay)"
run rmt_replay "v"; run rmt_replay "v"
report rmt_replay final

echo
echo "=== MergeTree, the identical block twice (a replay)"
run mt_replay "v"; run mt_replay "v"
report mt_replay

echo
echo "=== MergeTree SETTINGS non_replicated_deduplication_window = 100, the identical block twice"
run mt_dedup_replay "v"; run mt_dedup_replay "v"
report mt_dedup_replay
echo "  insert_deduplicate (session default): $(ch "SELECT value FROM system.settings WHERE name = 'insert_deduplicate'")"
