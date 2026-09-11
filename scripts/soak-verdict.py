#!/usr/bin/env python3
"""Pass or fail a memory soak, and say why.

    scripts/soak-verdict.py soak-pr/decomp.csv [--max-bytes-per-msg 1.0]
                            [--warmup 3]

The number that decides it is bytes retained per message, not megabytes per
minute. A leak is linear in messages, so per-message growth is comparable
between a saturated run and a trickle, and between a fast machine and a slow
one. Minutes per megabyte is none of those things.

Native is what is judged: Go retained is reported beside it, because a Go-side
leak should also fail, but the leak this gate exists to catch was a DuckDB
buffer that Go's heap profiler could not see.

Warm-up is discarded. Allocator arenas are bought once and reused, so the
first minutes grow and then stop; judging from minute zero reads that as a
slow leak.
"""
import argparse
import csv
import sys


def load(path):
    rows = []
    with open(path) as fh:
        for raw in csv.DictReader(fh):
            row = {}
            for k, v in raw.items():
                try:
                    row[k] = float(v)
                except (TypeError, ValueError):
                    row[k] = None
            if row.get("min") is not None:
                rows.append(row)
    return rows


def slope(points):
    """MiB per minute, least squares. None when there is too little to fit."""
    n = len(points)
    if n < 3:
        return None
    sx = sum(x for x, _ in points)
    sy = sum(y for _, y in points)
    sxx = sum(x * x for x, _ in points)
    sxy = sum(x * y for x, y in points)
    d = n * sxx - sx * sx
    return (n * sxy - sx * sy) / d if d else None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("csv")
    ap.add_argument("--max-bytes-per-msg", type=float, default=1.0)
    ap.add_argument("--warmup", type=int, default=3,
                    help="minutes discarded before judging")
    args = ap.parse_args()

    allrows = load(args.csv)

    # Minutes must ascend. Two samplers writing one file interleave their rows
    # and the arithmetic below would run happily over the mixture, reporting a
    # window and a message count that belong to neither run.
    minutes_seen = [r["min"] for r in allrows]
    if minutes_seen != sorted(minutes_seen) or len(set(minutes_seen)) != len(minutes_seen):
        print(f"FAIL  {args.csv}: samples are not in ascending minute order, "
              "so more than one sampler wrote this file. Rerun it alone.")
        return 1

    rows = [r for r in allrows if r["min"] >= args.warmup]
    if len(rows) < 3:
        print(f"FAIL  {args.csv}: {len(rows)} samples after warm-up, need 3")
        return 1

    first, last = rows[0], rows[-1]
    minutes = last["min"] - first["min"]
    messages = (last.get("msgs") or 0) - (first.get("msgs") or 0)

    native = [(r["min"], r["native_mib"]) for r in rows
              if r.get("native_mib") is not None]
    go = [(r["min"], r["go_retained_mib"]) for r in rows
          if r.get("go_retained_mib") is not None]

    native_slope = slope(native)
    go_slope = slope(go)
    growth_mib = last["native_mib"] - first["native_mib"]

    print(f"window        minutes {args.warmup}-{int(last['min'])}, "
          f"{int(messages):,} messages")
    print(f"native        {first['native_mib']:.1f} -> {last['native_mib']:.1f} MiB")
    print(f"go retained   {first['go_retained_mib']:.1f} -> "
          f"{last['go_retained_mib']:.1f} MiB")
    if native_slope is not None:
        print(f"native slope  {native_slope:+.3f} MiB/min")
    if go_slope is not None:
        print(f"go slope      {go_slope:+.3f} MiB/min")

    failures = []

    # The volume gate. Without enough messages the per-message number is
    # noise, and a pass would mean nothing. A saturated ten minutes clears
    # this by two orders of magnitude; 500 msg/s for ten minutes does not.
    if messages < 1_000_000:
        failures.append(
            f"only {int(messages):,} messages in the window; a per-message "
            "leak needs volume to clear the noise, so this run proves nothing")

    if messages > 0:
        per_msg = growth_mib * 1024 * 1024 / messages
        print(f"per message   {per_msg:+.3f} B/msg "
              f"(threshold {args.max_bytes_per_msg})")
        if per_msg > args.max_bytes_per_msg:
            failures.append(
                f"native grew {per_msg:.3f} B/msg, over the "
                f"{args.max_bytes_per_msg} threshold")

    # Go's side is judged more loosely: it is bounded by GC rather than by
    # arithmetic, so a slow upward drift there is usually the heap settling.
    if go_slope is not None and go_slope > 1.0:
        failures.append(f"Go retained grew {go_slope:+.3f} MiB/min")

    print()
    if failures:
        print("FAIL")
        for f in failures:
            print(f"  {f}")
        return 1
    print(f"PASS  memory is flat over {int(messages):,} messages")
    return 0


if __name__ == "__main__":
    sys.exit(main())
