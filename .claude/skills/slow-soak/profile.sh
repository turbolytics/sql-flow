#!/usr/bin/env bash
# Feed a topic with a scripted traffic profile, from inside the Kafka
# container so nothing crosses the host boundary and a dropped pipe cannot
# end the run early.
#
#   profile.sh <kafka-container> <topic> [full|short]
#
# Phases:
#   surge     1,000 messages a second      full 5m    short 2m
#   silence   nothing                      full 60m   short 3m
#   trickle   one message every 90s, with  full 60m   short 3m
#             two gaps and one pair sent
#             in the same second
#   silence   nothing                      full 30m   short 2m
#
# Every message carries sent_at, which is how the sampler measures latency.
set -uo pipefail
kafka=${1:?kafka container}; topic=${2:?topic}; mode=${3:-full}

if [ "$mode" = short ]; then
  surge=120; s1=180; trickle=180; s2=120; tick=20
else
  surge=300; s1=3600; trickle=3600; s2=1800; tick=90
fi

id=0
send() {
  local n=$1 ts
  ts=$(date -u +%Y-%m-%dT%H:%M:%S)
  docker exec -i "$kafka" bash -c "awk -v s=$id -v n=$n -v ts=$ts 'BEGIN{for(i=0;i<n;i++) printf \"{\\\"id\\\":%d,\\\"sent_at\\\":\\\"%s\\\"}\n\", s+i, ts}' | kafka-console-producer --bootstrap-server localhost:19092 --topic $topic >/dev/null 2>&1"
  id=$((id + n))
  echo "$(date -u +%H:%M:%SZ) phase=$phase sent=$n total=$id"
}

phase=surge
echo "$(date -u +%H:%M:%SZ) phase=surge for ${surge}s"
end=$((SECONDS + surge))
while [ $SECONDS -lt $end ]; do send 1000; sleep 1; done

phase=silence1
echo "$(date -u +%H:%M:%SZ) phase=silence1 for ${s1}s total=$id"
sleep "$s1"

phase=trickle
echo "$(date -u +%H:%M:%SZ) phase=trickle for ${trickle}s, one every ${tick}s"
end=$((SECONDS + trickle))
gap_at=$((SECONDS + trickle / 3))
pair_at=$((SECONDS + trickle / 2))
while [ $SECONDS -lt $end ]; do
  # One deliberate long gap, several ticks wide, inside the trickle.
  if [ $SECONDS -ge $gap_at ] && [ $SECONDS -lt $((gap_at + tick * 3)) ]; then
    sleep 10; continue
  fi
  # One pair in the same second, so a batch holding two is exercised too.
  if [ $SECONDS -ge $pair_at ] && [ $SECONDS -lt $((pair_at + tick)) ]; then
    send 2; pair_at=$end
  else
    send 1
  fi
  sleep "$tick"
done

phase=silence2
echo "$(date -u +%H:%M:%SZ) phase=silence2 for ${s2}s total=$id"
sleep "$s2"

echo "$(date -u +%H:%M:%SZ) PROFILE_DONE total=$id"
