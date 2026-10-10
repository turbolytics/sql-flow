#!/bin/bash
# perf.sh <label> <batch_size> <dsn>: one throughput run of the taxi pipeline.
# IMG selects the image. A fresh consumer group every run, on purpose.
W="$(cd "$(dirname "$0")" && pwd)"; label="$1"; bs="$2"; dsn="$3"
IMG="${IMG:-turbolytics/sql-flow:v2026.10.08}"
g="perf-$label-$RANDOM"
sed -e "s/batch_size: 20000/batch_size: $bs/" -e "s/group_id: nyc-taxi/group_id: $g/" "$W/taxi.yml" > "$W/taxi_$label.yml"
docker exec clickhouse clickhouse-client -q "TRUNCATE TABLE nyc_taxi.trips_small" >/dev/null 2>&1
s=$(date +%s)
docker run --rm --name "perf_$label" --network dev_default -v "$W":/conf -e SQLFLOW_CLICKHOUSE_DSN="$dsn" -e SQLFLOW_KAFKA_BROKERS=kafka1:19092 \
  "$IMG" run /conf/taxi_$label.yml --max-msgs=1000000 > "$W/perf_$label.log" 2>&1
rc=$?; e=$(date +%s)
tp=$(/usr/bin/grep -o '"total_throughput_per_second": [0-9.]*' "$W/perf_$label.log" | tail -1 | /usr/bin/grep -o '[0-9.]*$')
rows=$(docker exec clickhouse clickhouse-client -q "SELECT count() FROM nyc_taxi.trips_small" 2>/dev/null)
errs=$(/usr/bin/grep -c 'ERROR' "$W/perf_$label.log")
echo "$label bs=$bs exit=$rc wall=$((e-s))s rows_per_s=$tp rows=$rows errors=$errs"
