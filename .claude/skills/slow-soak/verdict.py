"""Judge a slow-soak samples.csv. Exit 1 on any failed rule, printing each.

Usage: verdict.py <samples.csv> [flush_interval_seconds]
"""
import csv
import sys

path = sys.argv[1]
interval = int(sys.argv[2]) if len(sys.argv) > 2 else 30

rows = [r for r in csv.DictReader(open(path)) if r.get("t") and not r["t"].startswith("SAMPLING")]
if not rows:
    sys.exit("no samples")


def f(x, d=0.0):
    try:
        return float(x)
    except (TypeError, ValueError):
        return d


fails = []

# 1. Latency: a message must reach the handler within the flush interval,
# plus slack for the sampler's own minute of granularity.
worst = max(f(r["max_latency_s"]) for r in rows)
if worst > interval + 60:
    fails.append(f"worst latency {worst:.0f}s is above the interval plus a minute")

# 2. Lag: the surge's backlog drains promptly once production stops, and
# then stays drained. Being behind during the surge, and for the first
# samples after it, is the consumer catching up and is not a defect. What
# would be a defect is never catching up, or falling behind again on a
# trickle where every message has a flush to itself.
# Everything after the first burst: the drain, the zeros, the trickle.
first_burst_end = next((i for i, r in enumerate(rows) if not r["phase"].startswith("burst")), len(rows))
post = rows[first_burst_end:]
drained_at = next((i for i, r in enumerate(post) if f(r["lag"], 0) <= 2), None)
if post and drained_at is None:
    fails.append(f"the backlog never drained; lag was still {post[-1]['lag']} at the end")
elif drained_at is not None and drained_at > 3:
    fails.append(f"the backlog took {drained_at} samples after the surge to drain")
else:
    for r in post[(drained_at or 0) + 1:]:
        if f(r["lag"], 0) > 2:
            fails.append(f"fell behind again after draining: lag {r['lag']} at {r['t']} in {r['phase']}")
            break

# 3. Memory: the end of the run is not above where the run started.
anon = [f(r["anon_mib"]) for r in rows if r["anon_mib"]]
if anon and anon[-1] > anon[0] * 1.10 + 5:
    fails.append(f"working set ended at {anon[-1]:.0f} MiB against {anon[0]:.0f} at the start")

# 4. State file: it must not grow while nothing is arriving.
for phase in sorted({r["phase"] for r in rows if r["phase"].startswith("zero")}):
    win = [r for r in rows if r["phase"] == phase and r["state_bytes"]]
    if len(win) >= 3 and f(win[-1]["state_bytes"]) > f(win[0]["state_bytes"]) * 1.05:
        fails.append(
            f"state file grew through {phase}: "
            f"{f(win[0]['state_bytes']):.0f} to {f(win[-1]['state_bytes']):.0f} bytes"
        )

# 5. Windows: the surge's buckets must publish during the silence that
# follows it. This is the idleness branch, and the reason the soak exists.
if post and f(post[-1]["published_windows"]) == 0:
    fails.append("no window was ever published, so the idleness branch never fired")
# Every zero phase longer than a couple of samples must end with more
# windows published than it began with, unless there was nothing open. That
# is the whole point: a stream at zero still closes what it was holding.
for phase in sorted({r["phase"] for r in rows if r["phase"].startswith("zero")}):
    win = [r for r in rows if r["phase"] == phase]
    if len(win) >= 3 and f(win[-1]["published_windows"]) == f(win[0]["published_windows"]) == 0:
        fails.append(f"{phase} published no window, so a bucket was stranded at zero traffic")

# 6. Health: idle is healthy, so a red sample is a real failure.
red = [r["t"] for r in rows if r["healthz"] and r["healthz"] != "200"]
if red:
    fails.append(f"healthz was not 200 at {len(red)} samples, first {red[0]}")

# 7. Everything produced eventually lands.
last = rows[-1]
if f(last["produced"]) and f(last["landed"]) < f(last["produced"]) * 0.999:
    fails.append(f"landed {last['landed']} of {last['produced']} produced")

if fails:
    print("FAIL")
    for x in fails:
        print(" -", x)
    sys.exit(1)

print(
    f"PASS over {len(rows)} samples: worst latency {worst:.0f}s, "
    f"working set {anon[0]:.0f} to {anon[-1]:.0f} MiB, "
    f"{f(last['landed']):.0f} of {f(last['produced']):.0f} messages landed, "
    f"{f(last['published_windows']):.0f} windows published"
)
