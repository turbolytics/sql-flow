#!/bin/bash
# win.sh: the tumbling-window replay. 1,000 events in one past one-minute
# bucket, read in ten batches of 100, closed by idle_close_seconds. The page's
# claim is one row of 1,000. IMG selects the image.
W="$(cd "$(dirname "$0")" && pwd)"
IMG="${IMG:-turbolytics/sql-flow:v2026.10.08}"
ch() { docker exec clickhouse clickhouse-client -q "$1" 2>&1; }
kt() { docker exec kafka1 kafka-topics --bootstrap-server localhost:9092 "$@" 2>&1 | tail -1; }

echo "image: $IMG"
ch "DROP TABLE IF EXISTS probe.win"
ch "CREATE TABLE probe.win (bucket DateTime, n Int32) ENGINE = MergeTree ORDER BY bucket"
kt --delete --if-exists --topic ch-win; sleep 2
kt --create --topic ch-win --partitions 1 --replication-factor 1
for i in $(seq 0 999); do
  printf '{"ts":"2026-09-01T12:00:%02dZ","id":%d}\n' $((i % 60)) $i
done > "$W/win_events.jsonl"
head -3 "$W/win_events.jsonl" > "$W/win_events.sample.jsonl"
docker exec -i kafka1 kafka-console-producer --bootstrap-server localhost:9092 --topic ch-win < "$W/win_events.jsonl"
echo "events published: $(wc -l < "$W/win_events.jsonl" | tr -d ' ')"

echo "--- validate"
docker run --rm -v "$W":/conf -e SQLFLOW_CLICKHOUSE_DSN=clickhouse://default@clickhouse:8123/probe -e SQLFLOW_GROUP_ID=x \
  "$IMG" validate /conf/win.yml 2>&1 | tail -3

echo "--- run, no --max-msgs: the bucket closes on idle_close_seconds"
g="win-$RANDOM"
docker rm -f win_rig >/dev/null 2>&1
docker run -d --name win_rig --network dev_default -v "$W":/conf \
  -e SQLFLOW_CLICKHOUSE_DSN=clickhouse://default@clickhouse:8123/probe -e SQLFLOW_GROUP_ID="$g" \
  "$IMG" run /conf/win.yml >/dev/null
sleep "${WIN_WAIT:-40}"
docker stop -t 20 win_rig >/dev/null
docker logs win_rig > "$W/win.log" 2>&1
docker rm win_rig >/dev/null
/usr/bin/grep -i 'window\|watermark\|closed\|published' "$W/win.log" | /usr/bin/grep -v '"level":"debug"' | tail -${WIN_TAIL:-6} | cut -c1-400
echo "--- what landed"
ch "SELECT bucket, n FROM probe.win ORDER BY bucket FORMAT PrettyCompact"
echo "rows published: $(ch 'SELECT count() FROM probe.win')"

echo "--- validate: allowed_lateness_seconds with a ClickHouse window sink"
sed -e 's/idle_close_seconds: 15/idle_close_seconds: 15\n        allowed_lateness_seconds: 60/' "$W/win.yml" > "$W/win_late.yml"
docker run --rm -v "$W":/conf -e SQLFLOW_CLICKHOUSE_DSN=clickhouse://default@clickhouse:8123/probe -e SQLFLOW_GROUP_ID=x \
  "$IMG" validate /conf/win_late.yml 2>&1 | tail -4
