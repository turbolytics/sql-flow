#!/usr/bin/env bash
# Feed a topic with a scripted traffic profile, from inside the Kafka
# container so nothing crosses the host boundary and a dropped pipe cannot
# end the run early.
#
#   profile.sh <kafka-container> <topic> [medium|full|short]
#
# The shape that matters: traffic goes to zero, stays there longer than the
# window grace, and comes back. A window that only closes when newer data
# arrives would strand its last bucket in every one of those gaps, so each
# silence is five times the 60 second grace and each return proves the
# pipeline picks up where it left off.
#
#           medium (20m)          full (2h35m)        short (10m)
#   burst   2m at 1,000/s         5m                  2m
#   zero    5m                    60m                 3m
#   burst   1m at 1,000/s         -                   -
#   zero    5m                    -                   -
#   trickle 3m, one every 90s     60m, every 90s      3m, every 20s
#   zero    4m                    30m                 2m
#
# medium is the one to run. Every invariant here is observable within
# minutes: the window grace is 60 seconds, the flush interval 30, and an
# idle commit lands every interval. full exists only for something suspected
# of drifting slowly.
#
# Every message carries sent_at, which is how the sampler measures latency.
set -uo pipefail
kafka=${1:?kafka container}; topic=${2:?topic}; mode=${3:-full}

# Each phase is "kind:seconds"; kind is burst, zero or trickle.
case "$mode" in
  short)  phases="burst:120 zero:180 trickle:180 zero:120"; tick=20 ;;
  medium) phases="burst:120 zero:300 burst:60 zero:300 trickle:180 zero:240"; tick=90 ;;
  full)   phases="burst:300 zero:3600 trickle:3600 zero:1800"; tick=90 ;;
  *) echo "unknown mode: $mode (short|medium|full)" >&2; exit 2 ;;
esac

id=0
send() {
  local n=$1 ts
  ts=$(date -u +%Y-%m-%dT%H:%M:%S)
  docker exec -i "$kafka" bash -c "awk -v s=$id -v n=$n -v ts=$ts 'BEGIN{for(i=0;i<n;i++) printf \"{\\\"id\\\":%d,\\\"sent_at\\\":\\\"%s\\\"}\n\", s+i, ts}' | kafka-console-producer --bootstrap-server localhost:19092 --topic $topic >/dev/null 2>&1"
  id=$((id + n))
  echo "$(date -u +%H:%M:%SZ) phase=$phase sent=$n total=$id"
}

n=0
for spec in $phases; do
  kind=${spec%%:*}
  secs=${spec##*:}
  n=$((n + 1))
  phase="${kind}${n}"
  echo "$(date -u +%H:%M:%SZ) phase=$phase for ${secs}s total=$id"

  case "$kind" in
    burst)
      end=$((SECONDS + secs))
      while [ $SECONDS -lt $end ]; do send 1000; sleep 1; done
      ;;
    zero)
      sleep "$secs"
      ;;
    trickle)
      end=$((SECONDS + secs))
      pair_at=$((SECONDS + secs / 2))
      while [ $SECONDS -lt $end ]; do
        # One pair in the same second, so a batch holding two is exercised.
        if [ $SECONDS -ge $pair_at ] && [ $SECONDS -lt $((pair_at + tick)) ]; then
          send 2; pair_at=$end
        else
          send 1
        fi
        sleep "$tick"
      done
      ;;
  esac
done

echo "$(date -u +%H:%M:%SZ) PROFILE_DONE total=$id"
