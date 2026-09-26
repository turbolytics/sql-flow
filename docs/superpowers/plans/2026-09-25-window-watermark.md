# The asserted watermark: implementation plan

**Spec:** `docs/superpowers/specs/2026-09-24-window-watermark-design.md`
**Goal:** the engine asserts one watermark per window, inside the batch's
commit; the manager closes on it and on nothing else; the idle fact, the
quiet clock and the manager's clock are deleted.

**Architecture.** A `core.Watermarks` value tracks, per source partition,
the newest event time this process has placed and when it last did, and
which partitions it holds; on every commit it computes each window's
watermark as the minimum over the partitions in the minimum and writes it to
`sqlflow_watermarks` when it moved. The manager reads that row and its own
`sqlflow_windows` row (closed up to) and does one thing: publish every bucket
whose end is at or before the asserted watermark and after the closed one.

No compatibility to preserve: nothing outside this repository windows.

## Global constraints

- No clock but event time reaches a close decision. The manager has no
  clock at all after this; the engine's monotonic clock decides only which
  partitions are *in the minimum*, never where the watermark is.
- The watermark is written by the engine only, on the pipeline's connection,
  inside the state transaction where there is one. The manager never writes
  it.
- `max(stored, W)`: monotonic by construction.
- Config schema unchanged. `grace_seconds` = out-of-orderness delay,
  `idle_close_seconds` = partition idleness, `late_rows` = lateness policy.
- Every simulator scenario carries its table diagram.

## Decisions the spec left open (rulings)

1. **Two rows, two owners.** The engine's assertion lives in a new engine
   table `sqlflow_watermarks(name, watermark)`; the manager's "closed up to"
   stays in `sqlflow_windows(name, watermark, closed_at)`. One row written by
   two connections would make every batch a write-write conflict with every
   poll. The manager needs its own row to tell a late bucket from a due one.

2. **Revoked leaves the minimum; lost holds it.** The spec says "a revoked
   partition holds the minimum". Taken literally, a stable scale-out (a
   partition moved to another member for good) freezes every window on the
   old member forever: the revoked partition's candidate never moves, it is
   never idle, and the all-idle close never fires. That is a liveness bug
   the spec did not foresee. Kafka already distinguishes the two facts
   (`internal/kafka/partitions.go`): *revoked* means another member holds
   the partition now — its future rows are that member's, and holding for
   them is holding for data that will never come here; *lost* means this
   session failed and the partition may come back with a backlog. So:
   revoked → out of the minimum at once; lost → holds at its last position
   (or −∞ if it never delivered) until reassigned. The two simulator
   scenarios that pin "a consumer holding nothing does not close" describe
   the *lost* case (a rejoin after a crash) and move to a `Lose` step; a
   new scenario pins the scale-out case. A source with no partition events
   (websocket, webhook, MQTT) is one partition that is *lost* while
   `Delivering()` is false.

3. **The all-idle close uses the table as a floor.** After a restart the
   process has seen nothing, and "W := max(seen)" would be empty. The
   engine reads each window's newest bucket at start and keeps it as the
   floor the all-idle close is measured from, so a quiet stream's last
   buckets still close after a restart. And "max(seen)" is the newest
   *bucket's end*, `trunc(max seen, size) + size`, or the newest row's own
   bucket would stay open — the same value `close.idle` uses today.

## The computation

```
for each held or lost partition p:
  seen[p]     newest placed event time (nanos), 0 = nothing this process
  lastRow[p]  monotonic instant of that row
  heldSince[p]monotonic instant of the assignment

candidate(p, window):
  lost, seen == 0            -inf
  lost, seen > 0             seen - grace            (holds at its last position)
  held, seen == 0            -inf
  held, idle(p)              (not in the minimum)
  held, otherwise            seen - grace

idle(p) = idle_close > 0 && now - max(lastRow[p], heldSince[p]) >= idle_close

W(window) = min over candidates in the minimum
          = if nothing is in the minimum and idle_close > 0:
                trunc(max(seen..., floor), size) + size     (the idle close)
            if nothing is in the minimum and idle_close == 0:
                unchanged                                    (shapes A and B)
stored := max(stored, W); written only when it moved
```

Only a record the engine placed (`CanPlace`) updates `seen`; an
unplaceable one advances nothing (#358). A record with `EventAtNanos == 0`
— a source that stamps nothing — updates nothing; such a pipeline closes
by idleness alone.

## Tasks

### Task 1 — `internal/core/watermarks.go`: the tracker and the store
- `type WindowSpec struct{Name string; Size, Grace, IdleClose time.Duration}`
- `type Watermarks struct` with `NewWatermarks(specs []WindowSpec, now func() time.Time)`,
  `Assigned/Released/Lost(map[string][]int32)`, `SetDelivering(bool)` for
  partition-less sources, `Observe(topic string, partition int32, atNanos int64)`,
  `Restore(name string, newestBucketStart time.Time)`, `Advance() map[string]time.Time`
  (each window's new watermark if it moved).
- `WatermarkStore` on a connection: `Init`, `Load(name)`, `Save(name, w)`; table
  `sqlflow_watermarks`, added to `EngineTables`.
- Unit tests, no engine: every line of the computation above, monotonicity,
  a revoked partition leaving, a lost one holding, the restart floor.

### Task 2 — the turbine asserts it
- `WithWindows(*Watermarks, WatermarkSaver)` option. In the message loop,
  after `canPlace`, `Observe`. In `commitState`, after `recordProgress`,
  `Advance` and save each moved window (inside the state tx where there is
  one). On the idle tick the same. Subscribe to `PartitionOwner` when the
  source is one; otherwise `SetDelivering` from `Deliverer` before each commit.
- Delete `quietSince`, `holdQuietWhileNotDelivering`, `WithQuietConfirmation`,
  `notDeliveringSince`; keep `sqlflow_progress` as liveness for /stats only.
- `progress_test.go` loses the quiet-confirmation tests; new tests: the
  watermark rides the state transaction, an unplaceable record does not
  move it, an idle tick moves it only when a partition went idle.

### Task 3 — the manager reads it
- `Poll`: load closed (own), load asserted; no asserted row → hold. Late =
  end ≤ closed; due = closed < end ≤ asserted. Save closed := asserted.
- Delete `nextWatermark`, `confirmedQuietSQL`, `Idle`, `StateOf`'s quiet,
  `WithClock`, `defaultPollInterval` stays. `decide.go`: watermark table on
  one fact `asserted ∈ {none, behind, ahead}` → `hold.unasserted`,
  `hold.behind`, `close`. Bucket table unchanged. `docs/windows/decisions.md`
  re-rendered.
- `close_lag` = asserted − closed, event time. `NewestStart` gauge kept from
  the table, observability only.
- Rewrite `watermark_test.go`, `conformance_test.go`, `helpers_test.go`
  (helpers write `sqlflow_watermarks` by hand instead of the progress row).

### Task 4 — the model
- `model.go`: events `Produce{Partition, At}`, `Elapse`, `Lose`, `Assign`,
  `Revoke`, `Restart`, `Poll`; the engine half is `core.Watermarks` itself
  (the model drives the real computation, no copy); the manager half is the
  two-fact rule. Enumerate per shape A–D; property: after the quiescing tail
  every produced row is published once or dropped by policy; must-reach per
  shape from body decisions; the minimum property: no bucket closes while a
  held, non-idle partition's `seen − grace` is below its end.

### Task 5 — the simulator
- `source` relays `OnPartitions`; `Revoke` → released, new `Lose` → lost,
  `Assign` → assigned. `RunWindowed` wires `WithWindows`.
- `AFastPartitionClosesASlowOnesBuckets` inverts (every row accounted for).
  `ASourceThatCannotDeliverStopsTheIdleClose` and `TheResumptionBoundsTheQuiet`
  use `Lose`. New: `ARevokedPartitionLeavesTheMinimum` (scale-out closes by
  the remaining partition), `ARestartStillClosesByIdleness` (the floor).
  README diagrams updated.

### Task 6 — wiring, docs, ledger
- `internal/cli/run`: build `core.Watermarks` from the config's windows,
  `WatermarkStore.Init` beside the windows store, `WithWindows`; drop
  `progressOptions`' quiet flag.
- `docs/windows/decisions.md` (rendered), the config reference's window
  section, `internal/simulate/README.md`, CHANGELOG, `docs/coverage/invariants.yml`:
  `window.asserted_in_the_commit`, `window.minimum_over_partitions`,
  `window.never_backwards` (moved), `pipeline.window.counts_every_row` kept.
