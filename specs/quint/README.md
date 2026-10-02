# Formal models (Quint)

Formal models of SQLFlow's multi-worker window-commit protocol, in
[Quint](https://quint-lang.org). Tracked by #418.

These model the **protocol**, not the Go: a model checker explores interleavings
exhaustively where tests only sample, and every real defect in the
durable-counts work (#414) was an "exists an interleaving" bug.

Two ways to check each model:
- `quint run` — a randomized simulator. Finds bugs fast; samples, so it cannot
  prove absence. Needs only Node.
- `quint verify` — exhaustive symbolic model checking (the Apalache backend,
  needs a JVM). Proves the invariant holds across **all** interleavings up to a
  step bound, or returns a counterexample.

Each model carries a `FIX` flag: `false` is the shipped design, `true` is the
proposed fix. Running each both ways shows the bug and that the fix removes it.

## window_watermark.qnt — issue #417

Invariant `noOpenBucketRefused`: a bucket a partition still legitimately holds
(within `allowed_lateness` of that partition's own progress) is never refused
as late.

- `FIX = false` — one watermark per window per worker, the minimum over its
  *active* partitions; a partition being replayed after a reassignment is
  excluded, so a faster partition drags the watermark up and the replayed
  partition's own open buckets are refused. **This is #417.**
- `FIX = true` — the watermark is judged per partition.

| Design | `quint run` (sampled) | `quint verify` (exhaustive ≤12 steps) |
|---|---|---|
| `FIX = false` (today) | counterexample | **counterexample** |
| `FIX = true` (per-partition) | no violation, 50k samples | **NoError (proved)** |

## single_writer.qnt — partition_owned single-writer guarantee

Invariant `noNonOwnerPublish`: a worker never publishes a bucket for a
partition it does not currently own. This is the property that keeps one
process writing each `(bucket, partition)` key across a rebalance.

- `FIX = false` — a plain windowed pipeline: the old owner keeps its
  unpublished rows after a handoff, so an in-flight pass can still publish them
  for a partition it no longer owns, racing the new owner.
- `FIX = true` — `partition_owned`: the handoff drops the old owner's
  unpublished rows atomically (under the pass lock), so only the new owner
  publishes.

| Design | `quint run` (sampled) | `quint verify` (exhaustive ≤10 steps) |
|---|---|---|
| `FIX = false` | counterexample | **counterexample** |
| `FIX = true` (partition_owned) | no violation, 50k samples | **NoError (proved)** |

## commit_offsets.qnt — the durable offset commit (#414 Finding 1 + low watermark)

Two invariants on the windowed-Kafka offset commit:

- `commitsOnlyOwned` — a worker commits offsets only for a partition it
  currently owns. `FIX = false` keeps a revoked partition's uncommitted work,
  so the old owner commits a partition it lost (the adversarial review's
  Finding 1); `FIX = true` forgets it on the handoff.
- `committedBelowRetained` — the committed offset is at or below the lowest
  offset a retained bucket still needs, so a restart replays what the open
  windows hold. `FIX = false` commits the processed offset (ahead of the
  retained rows); `FIX = true` commits the low watermark.

| Invariant | design | `quint run` | `quint verify` (≤10 steps) |
|---|---|---|---|
| `commitsOnlyOwned` | `FIX = false` | counterexample | **counterexample** |
| `commitsOnlyOwned` | `FIX = true` | no violation | **NoError — proved** |
| `committedBelowRetained` | `FIX = false` | counterexample | **counterexample** |
| `committedBelowRetained` | `FIX = true` | no violation | **NoError — proved** |

## exactly_once.qnt — no double count under retry/replay

Invariant `exactlyOnce`: the published count equals the number of distinct ids
delivered, however many times a retry or a crash-replay redelivers an id. This
is the no-double-count half of exactly-once; the no-loss half is
`committedBelowRetained` above.

- `FIX = false` — no dedup: the window counts every delivery, so a replayed id
  is counted twice.
- `FIX = true` — dedup by id (a set), idempotent under redelivery.

| Design | `quint run` | `quint verify` (≤10 steps) |
|---|---|---|
| `FIX = false` | counterexample | **counterexample** |
| `FIX = true` (dedup) | no violation | **NoError — proved** |

## Running

```
# randomized simulator (Node only)
npx @informalsystems/quint run specs/quint/window_watermark.qnt --invariant=noOpenBucketRefused

# exhaustive verify (needs a JVM for Apalache)
npx @informalsystems/quint verify specs/quint/window_watermark.qnt --invariant=noOpenBucketRefused --max-steps=12

# both models, both designs, both backends:
specs/quint/run.sh          # simulator
specs/quint/run.sh verify   # + exhaustive verify if java is on PATH
```

## Scope and limits

- The models verify the **protocol, not the implementation** (the Go still has
  to refine them; the first code review's Finding 1 was exactly a case where the
  code gated a rule the design did not).
- `quint verify` is exhaustive up to a **step bound**, not unbounded; it is a
  bounded proof, far stronger than sampling but not an inductive one. Raising
  `--max-steps` widens it.
- The five core invariants from #418 are now modeled and verified. Next:
  raise the step bounds, and pursue inductive (unbounded) invariants so the
  proofs no longer depend on a bound.
