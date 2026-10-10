#!/bin/bash
# probe.sh <name> <create-table-body> <handler-select-sql> [dsn]
# Runs one message through sqlflow into probe.<name> and prints what landed.
# IMG picks the image; the default is the release the findings were measured on.
W="$(cd "$(dirname "$0")" && pwd)"; name="$1"; ddl="$2"; sql="$3"; dsn="${4:-clickhouse://default@clickhouse:8123/probe}"
IMG="${IMG:-turbolytics/sql-flow:v2026.10.08}"
docker exec clickhouse clickhouse-client -q "DROP TABLE IF EXISTS probe.$name"
docker exec clickhouse clickhouse-client -q "CREATE TABLE probe.$name ($ddl) ENGINE = MergeTree ORDER BY tuple()"
cat > "$W/p_$name.yml" <<YML
pipeline:
  batch_size: 1
${PIPELINE_EXTRA:-}
  source:
    type: kafka
    kafka:
      brokers: [kafka1:19092]
      group_id: probe-$name-$RANDOM
      auto_offset_reset: earliest
      topics:
        - ch-probe
  handler:
    type: 'handlers.InferredMemBatch'
    sql: |
      $sql
  sink:
    type: clickhouse
    clickhouse:
      dsn: {{ SQLFLOW_CLICKHOUSE_DSN }}
      table: $name
YML
s=$(date +%s)
docker run --rm --network dev_default -v "$W":/conf -e SQLFLOW_CLICKHOUSE_DSN="$dsn" ${PROBE_ENV:-} \
  "$IMG" run /conf/p_$name.yml --max-msgs=1 > "$W/p_$name.log" 2>&1
rc=$?; e=$(date +%s)
echo "exit=$rc wall=$((e-s))s"
/usr/bin/grep -i '"level":"error"\|error\|\[user\.\|\[system\.' "$W/p_$name.log" | tail -${PROBE_TAIL:-4} | cut -c1-${PROBE_CUT:-900}
docker exec clickhouse clickhouse-client -q "SELECT * FROM probe.$name FORMAT Vertical" 2>&1 | head -30
