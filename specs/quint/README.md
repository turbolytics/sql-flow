# Formal models (Quint)

Formal models of SQLFlow's multi-worker window-commit protocol, in
[Quint](https://quint-lang.org). Tracked by #418; the first model targets the
watermark/assignment limit in #417.

These model the **protocol**, not the Go. Their value is that a model checker
explores interleavings exhaustively where tests only sample — every real defect
in the durable-counts work (#414) was an "exists an interleaving" bug.

## window_watermark.qnt — issue #417

Models the windowed-Kafka watermark against the invariant
**`noOpenBucketRefused`**: a bucket a partition still legitimately holds (within
`allowed_lateness` of that partition's own event-time progress) is never
refused as late.

A `FIX` flag selects the design:

- `FIX = false` — today's design: one watermark per window per worker, the
  minimum over the worker's *active* partitions. A partition still being
  replayed after a reassignment is excluded from that minimum, so a faster
  partition drags the watermark up and the replayed partition's own open
  buckets are refused. **This is #417.**
- `FIX = true` — the proposed fix: the watermark is judged per partition,
  against that partition's own progress.

### Result

The randomized simulator **reproduces #417** with the current design and finds
no violation of the same invariant with the per-partition fix:

```
# current design: finds the counterexample (partition reassigned to a worker
# whose other partition has run ahead; its own open bucket is refused)
quint run window_watermark.qnt --invariant=noOpenBucketRefused

# the fix: no violation in 50,000 samples
sed 's/FIX = false/FIX = true/' window_watermark.qnt > /tmp/fixed.qnt
quint run /tmp/fixed.qnt --invariant=noOpenBucketRefused --max-samples=50000
```

The counterexample (seed `0x1c3`): worker 0 owns partition 0 (progress 5) and
is handed partition 1 (progress 1); while partition 1 replays it is excluded
from worker 0's minimum, so the watermark sits at 5 and partition 1's bucket 0
— which it still holds, being within lateness of its own progress 1 — is
refused.

## Running

Quint needs Node; its randomized simulator additionally downloads a Rust
evaluator on first run.

```
npm install -g @informalsystems/quint   # or: npx @informalsystems/quint ...
cd specs/quint
quint typecheck window_watermark.qnt
quint run window_watermark.qnt --invariant=noOpenBucketRefused
```

## Scope and limits

- The randomized simulator **samples** executions — it finds bugs, it does not
  prove their absence. The `FIX = true` result is strong evidence, not a proof.
- An exhaustive proof needs `quint verify` (the Apalache backend, which needs a
  JVM) or a bounded model check. That is the next step for this model.
- Next properties to model (from #418): a worker commits offsets only for
  partitions it owns; committed offset ≤ lowest retained offset; one writer per
  `(bucket, partition)`; every event counted exactly once.
