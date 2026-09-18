#!/bin/bash
W="$(cd "$(dirname "$0")" && pwd)"; label="$1"; bs="$2"; dsn="$3"; H="$4"; U="$5"; PW="$6"
g="perf-$label-$RANDOM"
sed -e "s/batch_size: 20000/batch_size: $bs/" -e "s/group_id: nyc-taxi/group_id: $g/" "$W/taxi.yml" > "$W/taxi_$label.yml"
curl -s --max-time 60 "https://$H/" --user "$U:$PW" --data-binary "TRUNCATE TABLE nyc_taxi.trips_small" >/dev/null
s=$(date +%s)
docker run --rm --name "perf_$label" --network dev_default -v "$W":/conf -e SQLFLOW_CLICKHOUSE_DSN="$dsn" -e SQLFLOW_KAFKA_BROKERS=kafka1:19092 \
  turbolytics/sql-flow:v2026.09.17.1 run /conf/taxi_$label.yml --max-msgs=1000000 > "$W/perf_$label.log" 2>&1
rc=$?; e=$(date +%s)
tp=$(/usr/bin/grep -o '"total_throughput_per_second": [0-9.]*' "$W/perf_$label.log" | tail -1 | /usr/bin/grep -o '[0-9.]*$')
rows=$(curl -s --max-time 60 "https://$H/" --user "$U:$PW" --data-binary "SELECT count() FROM nyc_taxi.trips_small")
errs=$(/usr/bin/grep -c 'ERROR' "$W/perf_$label.log")
echo "$label bs=$bs exit=$rc wall=$((e-s))s rows_per_s=$tp rows=$rows errors=$errs"
