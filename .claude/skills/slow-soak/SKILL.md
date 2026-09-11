---
name: slow-soak
description: Use when a pipeline must be shown to stay live and correct under slow, bursty, or absent traffic for hours - latency on a trickle, the tail of a surge, idle commits, window closes on silence, and health reporting. Use before publishing a version floor that third parties will depend on.
---

# Slow soak

Throughput proves nothing about a stream that trickles, stops, or never
starts, and that is most streams most of the time. This runs a scripted
profile against a real broker and judges seven rules once a minute. One line
per sample; everything verbose stays in files.

## The rule

A slow stream breaks different things than a fast one. Every failure below
has been real at least once:

- A batch that never fills has to leave on a timer.
- An offset has to commit after a batch of one.
- An idle pipeline has to keep committing, and empty commits must cost
  nothing on disk.
- A window whose rows have stopped arriving has to close anyway. The stream
  clock alone cannot close it, because a stopped stream never moves it.
- A consumer has to stay joined to a broker it has not heard from in an hour.
- Idle has to be distinguishable from stuck from outside the process.

## Run it

Copy the directory first. A checkout under a running soak has bitten before.

    cp -r .claude/skills/slow-soak /tmp/slow-soak && cd /tmp/slow-soak
    mkdir -p state soak
    make -C <repo> start-backing-services

    TOPIC=slow-$(date +%s)
    docker run -d --name sqlflow-slow --network dev_default \
      -v "$PWD":/conf -e SQLFLOW_TOPIC=$TOPIC \
      turbolytics/sql-flow:<tag> \
      run /conf/slow.yml --metrics=prometheus --with-http-debug

    ./profile.sh kafka1 $TOPIC medium > soak/profile.log 2>&1 &
    ./sample.sh sqlflow-slow soak 22 | tee soak/live.log
    python3 verdict.py soak/samples.csv 30

`medium` is twenty minutes and is the one to run: two bursts, each followed
by five minutes at zero, then a trickle and four more minutes at zero. What
it proves is the shape that breaks things, traffic going to zero for longer
than the window grace and then coming back.

`short` is ten minutes, for checking the harness itself; give `sample.sh`
13 minutes. `full` is two and a half hours and exists only for something
suspected of drifting slowly.

Do a short run first when you change anything here. It is how both harness
defects so far were found, one in the sampler's cadence and one in a
verdict rule that called a draining backlog a failure.

Do not map port 8000 or 6060 to the host unless you know they are free. The
sampler reaches both through a sidecar on the container's network namespace,
so no mapping is needed.

## What each failure means

| Verdict line | Where to look |
| --- | --- |
| worst latency above the interval | The flush ticker is not firing, or a flush is slow. `state_commit_latency_seconds` in /metrics. |
| offset lag after the surge | Offsets are not committing per flush. `state_commit_count_commits_total` against the sample count. |
| working set ended above the start | Something was held from the surge. Switch to the memory-soak skill, which decomposes it. |
| state file grew through a silence | Empty commits are not empty. Check what the idle tick writes. |
| windows still unpublished after the silence | The idleness branch of the window predicate did not fire. Read `sqlflow_progress` through /debug: a frozen `last_arrival` is the engine, a moving one is the predicate. |
| healthz not 200 | The commit clock stopped. That is a real stall, not a quiet stream. |
| landed below produced | Messages were lost, which no other rule here would catch on its own. |

## What it does not cover

Ordering, exactly-once, and late-arriving data. The engine is at-least-once
and the window predicate closes on silence, so a row that arrives after its
bucket closed publishes as a second part. That is by design and the docs say
so; this soak does not judge it.
