#!/usr/bin/env python3
"""Compare memory soaks: native slope after warm-up, slope over the final
third, bytes retained per message, and whether each run plateaued.

    compare.py soak-baseline/decomp.csv soak-candidate/decomp.csv [--warmup 8]

Native is the number that matters; Go retained is printed so a Go-side leak
shows up too. Two slopes are reported because a run that is still settling
looks like a slow leak over the whole window and flat over its last third.
"plateau" means the final-third slope is under 0.25 MiB/min and the last five
samples span under 3 MiB, the noise floor seen on a 500 msg/s pipeline.
"""
import csv
import os
import sys


def load(path):
    rows = []
    with open(path) as f:
        for r in csv.DictReader(f):
            row = {}
            for k, v in r.items():
                try:
                    row[k] = float(v)
                except (TypeError, ValueError):
                    pass
            if "min" in row:
                rows.append(row)
    return rows


def fit(points):
    n = len(points)
    if n < 3:
        return None
    sx = sum(x for x, _ in points); sy = sum(y for _, y in points)
    sxx = sum(x * x for x, _ in points); sxy = sum(x * y for x, y in points)
    d = n * sxx - sx * sx
    return (n * sxy - sx * sy) / d if d else None


def series(rows, key, start):
    return [(r["min"], r[key]) for r in rows if key in r and r["min"] >= start]


def per_message(rows, key):
    a = [r for r in rows if key in r and r.get("msgs", 0) > 0]
    if len(a) < 2 or a[-1]["msgs"] <= a[0]["msgs"]:
        return None
    return (a[-1][key] - a[0][key]) * 1048576 / (a[-1]["msgs"] - a[0]["msgs"])


def fmt(v, spec):
    return format(v, spec) if v is not None else "n/a"


def main():
    argv = sys.argv[1:]
    warmup = 8
    if "--warmup" in argv:
        i = argv.index("--warmup")
        warmup = int(argv[i + 1])
        del argv[i:i + 2]
    if not argv:
        print(__doc__); sys.exit(2)

    hdr = f"{'soak':<22} {'n':>3} {'native first':>12} {'native last':>11} {'slope':>7} {'last-third':>10} {'B/msg':>6} {'go slope':>8} {'plateau':>7}"
    print(hdr)
    print("-" * len(hdr))
    for path in argv:
        rows = load(path)
        name = os.path.basename(os.path.dirname(path)) or os.path.basename(path)
        nat = series(rows, "native_mib", 0)
        if not nat:
            print(f"{name:<22} no native_mib column"); continue
        last_min = nat[-1][0]
        overall = fit(series(rows, "native_mib", warmup))
        tail = fit(series(rows, "native_mib", max(warmup, last_min * 2 / 3)))
        go = fit(series(rows, "go_retained_mib", warmup))
        last5 = [y for _, y in nat[-5:]]
        plateau = tail is not None and len(last5) == 5 and tail < 0.25 and (max(last5) - min(last5)) < 3.0
        print(f"{name:<22} {len(rows):>3} {nat[0][1]:>12.1f} {nat[-1][1]:>11.1f} "
              f"{fmt(overall, '.2f'):>7} {fmt(tail, '.2f'):>10} "
              f"{fmt(per_message(rows, 'native_mib'), '.0f'):>6} {fmt(go, '.2f'):>8} "
              f"{'yes' if plateau else 'no':>7}")
    print("\nslopes are native MiB per minute; B/msg needs a msgs column")


if __name__ == "__main__":
    main()
