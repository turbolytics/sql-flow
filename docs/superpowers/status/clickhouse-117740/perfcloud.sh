#!/bin/bash
# perfcloud.sh <label> <batch_size>: one throughput run of the taxi pipeline
# against ClickHouse Cloud. CLICKHOUSE_CREDS names a file holding one line,
# clickhouse://user:password@host, and defaults to ~/.sqlflow-clickhouse. The
# password stays out of the argument list, where ps would show it. IMG selects
# the image. A fresh consumer group every run, on purpose.
W="$(cd "$(dirname "$0")" && pwd)"; label="$1"; bs="$2"
IMG="${IMG:-turbolytics/sql-flow:v2026.10.08}"
raw=$(cat "${CLICKHOUSE_CREDS:-$HOME/.sqlflow-clickhouse}"); rest=${raw#*://}
userpass=${rest%@*}; H=${rest#*@}; H=${H%%/*}; H=${H%%:*}
# clickhouse:// is plain HTTP, which Cloud refuses; Cloud needs TLS on 8443.
dsn="clickhouses://$userpass@$H:8443/nyc_taxi"
# Cloud serves reads from any replica, and one that has not yet fetched the
# newest part undercounts: a run of 1,000,000 once read 980,000 while every row
# was written. Sequential consistency makes the count wait for it.
q() { curl -s --max-time 60 "https://$H:8443/?select_sequential_consistency=1" --user "$userpass" --data-binary "$1"; }
g="perf-$label-$RANDOM"
sed -e "s/batch_size: 20000/batch_size: $bs/" -e "s/group_id: nyc-taxi/group_id: $g/" "$W/taxi.yml" > "$W/taxi_$label.yml"
q "TRUNCATE TABLE nyc_taxi.trips_small" >/dev/null
s=$(date +%s)
SQLFLOW_CLICKHOUSE_DSN="$dsn" docker run --rm --name "perf_$label" --network dev_default -v "$W":/conf \
  -e SQLFLOW_CLICKHOUSE_DSN -e SQLFLOW_KAFKA_BROKERS=kafka1:19092 \
  "$IMG" run /conf/taxi_$label.yml --max-msgs=1000000 > "$W/perf_$label.log" 2>&1
rc=$?; e=$(date +%s)
tp=$(/usr/bin/grep -o '"total_throughput_per_second": [0-9.]*' "$W/perf_$label.log" | tail -1 | /usr/bin/grep -o '[0-9.]*$')
rows=$(q "SELECT count() FROM nyc_taxi.trips_small")
errs=$(/usr/bin/grep -c 'ERROR' "$W/perf_$label.log")
echo "$label bs=$bs exit=$rc wall=$((e-s))s rows_per_s=$tp rows=$rows errors=$errs"
