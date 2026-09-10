#!/usr/bin/env bash
# Feed a topic at a steady rate from inside the Kafka container, so nothing
# crosses the host boundary and a dropped pipe cannot end the run early.
#
#   producer.sh <kafka-container> <topic> <seconds> [msgs-per-second]
#
# The payload is a small sensor reading; edit the awk printf for another shape.
set -euo pipefail
kafka=${1:?kafka container}; topic=${2:?topic}; seconds=${3:?seconds}; rate=${4:-500}
docker exec "$kafka" kafka-topics --bootstrap-server "$kafka:19092" --create --topic "$topic" --partitions 1 --replication-factor 1 >/dev/null 2>&1 || true
docker exec -d "$kafka" sh -c "for t in \$(seq 1 $seconds); do
  awk -v s=\$t -v n=$rate 'BEGIN{srand(s); for(i=0;i<n;i++){printf \"{\\\"sensor_id\\\":%d,\\\"ts\\\":\\\"2026-09-11T00:00:00\\\",\\\"value\\\":%.2f}\n\", int(rand()*5)+1, rand()*50-10}}'
  sleep 1
done | kafka-console-producer --bootstrap-server $kafka:19092 --topic $topic"
echo "producing $rate msg/s to $topic for ${seconds}s inside $kafka"
