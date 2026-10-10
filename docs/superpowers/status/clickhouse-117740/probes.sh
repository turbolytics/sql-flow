#!/bin/bash
# probes.sh: every single-message finding on the page, in one run.
# Output is the evidence; STATUS.md quotes it. IMG selects the image.
W="$(cd "$(dirname "$0")" && pwd)"
P="$W/probe.sh"
IMG="${IMG:-turbolytics/sql-flow:v2026.10.08}"; export IMG
ch() { docker exec clickhouse clickhouse-client -q "$1" 2>&1; }
hdr() { echo; echo "=== $1"; }

hdr "versions"
echo "image: $IMG"
ch "SELECT version(), timezone()"

hdr "enum_nozero: NULL into Enum8('a' = 1, 'b' = 2), not Nullable"
$P enum_nozero "e Enum8('a' = 1, 'b' = 2)" "SELECT CAST(NULL AS VARCHAR) AS e FROM batch"
echo "count():      $(ch 'SELECT count() FROM probe.enum_nozero')"
echo "SELECT e:     $(ch 'SELECT e FROM probe.enum_nozero' | head -1 | cut -c1-160)"
echo "CAST AS Int8: $(ch 'SELECT CAST(e AS Int8) FROM probe.enum_nozero')"

hdr "enum_zero: NULL into Enum8('' = 0, 'a' = 1, 'b' = 2)"
$P enum_zero "e Enum8('' = 0, 'a' = 1, 'b' = 2)" "SELECT CAST(NULL AS VARCHAR) AS e FROM batch"

hdr "enum_nullable: NULL into Nullable(Enum8('a' = 1, 'b' = 2))"
$P enum_nullable "e Nullable(Enum8('a' = 1, 'b' = 2))" "SELECT CAST(NULL AS VARCHAR) AS e FROM batch"

hdr "null_dt: NULL into DateTime DEFAULT now()"
$P null_dt "ts DateTime DEFAULT now()" "SELECT CAST(NULL AS TIMESTAMP) AS ts FROM batch"

hdr "null_str: NULL into String DEFAULT 'unset'"
$P null_str "s String DEFAULT 'unset'" "SELECT CAST(NULL AS VARCHAR) AS s FROM batch"

hdr "null_int: NULL into Int32 DEFAULT 7"
$P null_int "i Int32 DEFAULT 7" "SELECT CAST(NULL AS INTEGER) AS i FROM batch"

hdr "null_nullable: NULL into Nullable(String)"
$P null_nullable "s Nullable(String)" "SELECT CAST(NULL AS VARCHAR) AS s FROM batch"

hdr "omitted: a column the handler does not produce"
$P omitted "x Int32, s String DEFAULT 'omitted-default'" "SELECT 1::INTEGER AS x FROM batch"

hdr "ts_plain: '2026-09-01 12:00:00' into DateTime and DateTime('UTC')"
$P ts_plain "ts DateTime, tsu DateTime('UTC')" "SELECT '2026-09-01 12:00:00' AS ts, '2026-09-01 12:00:00' AS tsu FROM batch"

hdr "ts_offset: '2026-09-01 12:00:00 +09:00' into DateTime"
$P ts_offset "ts DateTime" "SELECT '2026-09-01 12:00:00 +09:00' AS ts FROM batch"

hdr "ts_iso: '2026-09-01T12:00:00Z' into DateTime (expect encode_failed, one attempt)"
$P ts_iso "ts DateTime" "SELECT '2026-09-01T12:00:00Z' AS ts FROM batch"
echo "attempts logged: $(/usr/bin/grep -c -i 'retry\|attempt' "$W/p_ts_iso.log")"

hdr "dec_str: CAST(12.34 AS VARCHAR) into Decimal(10, 2)"
$P dec_str "amount Decimal(10, 2)" "SELECT CAST(12.34 AS VARCHAR) AS amount FROM batch"

hdr "dec_dbl: DOUBLE into Decimal(10, 2) (expect encode_failed)"
$P dec_dbl "amount Decimal(10, 2)" "SELECT CAST(12.34 AS DOUBLE) AS amount FROM batch"

hdr "json_str: to_json({'k': 1}) into String"
$P json_str "s String" "SELECT to_json({'k': 1}) AS s FROM batch"

hdr "struct: STRUCT into String (expect type_unsupported)"
$P struct "s String" "SELECT {'k': 1} AS s FROM batch"

hdr "colmiss: a column the table lacks (expect write_failed)"
$P colmiss "x Int32" "SELECT 1::INTEGER AS x, 2::INTEGER AS extra FROM batch"

hdr "flush5: flush_interval_seconds: 5 validates and runs"
PIPELINE_EXTRA="  flush_interval_seconds: 5" $P flush5 "x Int32" "SELECT 1::INTEGER AS x FROM batch"
docker run --rm -v "$W":/conf -e SQLFLOW_CLICKHOUSE_DSN=clickhouse://default@clickhouse:8123/probe "$IMG" validate /conf/p_flush5.yml 2>&1 | tail -3
/usr/bin/grep -o 'flush_interval_seconds: 5' "$W/p_flush5.yml"

hdr "done"
