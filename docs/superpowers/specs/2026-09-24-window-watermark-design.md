# The window as an asserted watermark

**Status:** proposal, for discussion alongside #369.
**Relates to:** #369 (the correctness framework), #374 (a burst's rows dropped as late), #183 (a bucket split across workers).

## The problem this solves

A window manager needs one thing: how far the stream has got. Today it is never
told. It infers it, from three facts written asynchronously by someone else:

| fact | read from | what it is really asking |
|---|---|---|
| `data` | the window table's newest bucket | how far has event time reached? |
| `idle` | `last_commit - last_arrival` in `sqlflow_progress` | has anything arrived lately? |
| `source` | `delivering_for_us` in `sqlflow_progress` | could anything have arrived? |

All three exist to answer one question, and every window defect this project has
had is one of the three being wrong, or two of them disagreeing:

- `idle` alone could not tell a quiet stream from a consumer between
  assignments, so `source` was added to disambiguate it — the whole of #369.
- `idle` is on the engine clock and `data` is on event time, so a rule that
  compares them is a bug by construction, and the clock-domain rule exists to
  forbid it.
- The window rows and the progress row are written in separate transactions, so
  a reader can pair a burst's first rows with the silence they ended: #374.
- The same reading is corrected in two places, `StateOf`'s clamp and the
  table's `hold.not_delivering` row, which then disagreed about a ripe bucket.
- The truth table needs 24 states and seven rules, almost all of which exist to
  reconcile the three.

Seven rules to answer one question is the symptom. The cause is that nobody
asserts the answer.

## What Flink does

Flink's primitive is that **the watermark is in the stream**. `Watermark(T)` is
an element travelling in order with the records, meaning "no further record
will have an event time below T". Everything else follows from it:

- **It is emitted at the source**, where the event times and the partition
  assignments actually are, not inferred downstream.
- **It is per partition, combined by minimum.** An operator's watermark is the
  lowest of its inputs, so a lagging partition holds the window open and a fast
  one cannot close it early.
- **Idleness is explicit.** A source that knows it has nothing marks itself
  idle, and an idle input is excluded from the minimum instead of pinning it
  forever.
- **Out-of-orderness is a bounded subtraction**: `W = maxEventTimeSeen - delay`.
- **Windows are timers on the watermark**, not polls. A bucket fires when `W`
  passes its end. Lateness is then a simple comparison, not a race.

The property that matters here is not the elegance. It is that a watermark is
**asserted by the party that knows**, and travels with the data it describes,
so no reader ever has to reconstruct it from side effects.

## The observation that makes this cheap

**The current config schema already describes this model.** Nothing in it needs
to change:

| config key | what it is in Flink's vocabulary |
|---|---|
| `time_column` | the event-time attribute |
| `size_seconds` | tumbling window size |
| `grace_seconds` | bounded out-of-orderness, the watermark delay |
| `idle_close_seconds` | source idleness timeout |
| `late_rows: drop` | allowed lateness zero, late records dropped |
| `late_rows: reemit` | late records emitted as an update to the fired pane |
| `emit_sql` | the window function over a fired pane |
| `poll_interval_seconds` | an implementation bound, not semantics |

`grace_seconds` is already documented as "how far past a bucket's end the
stream's own clock must reach", which is exactly `maxEventTime - delay`. The
engine already computes that value — in the manager's poll, as
`newest.Add(-decl.Grace).After(previous)`. The schema has been describing
watermarks all along. Only the plumbing disagrees.

So this is not a new feature or a config migration. It is moving one
computation from the reader to the writer.

## The proposal

**The engine asserts the watermark. The manager becomes a function of it.**

### One asserted fact

A new durable value per windowed table, written **inside the batch's
transaction**, beside the rows it describes:

```
sqlflow_window_watermark(table_name VARCHAR, watermark TIMESTAMPTZ, ...)
```

Its meaning is a promise, not a measurement: *as of this commit, every row this
pipeline will ever write for this table has `time_column >= watermark`.*

The manager's whole decision becomes:

```sql
-- close every bucket whose end is at or before the asserted watermark
WHERE bucket + size <= watermark
```

No clocks. No silence. No source liveness. One monotonic timestamp, compared
against event time — the same domain — which makes the clock-domain rule
unnecessary rather than merely enforced.

### Where the value comes from

The engine computes it where the facts are, per source partition:

```
  seen[p]        = max(time_column) among rows this pipeline wrote for partition p
  candidate[p]   = seen[p] - grace_seconds
  W              = min over ACTIVE partitions of candidate[p]
```

and advances the stored watermark to `max(stored, W)` — monotonic by
construction rather than by a checked invariant.

A partition is **idle** when the engine has seen nothing from it for
`idle_close_seconds`, measured on the engine's monotonic clock, and an idle
partition leaves the minimum. This is the one place a wall-clock duration is
still consulted, and it now lives in the engine — the only party that has both
a coherent clock and the source object that knows whether it holds the
partition at all. `delivering_for_us` stops being a column a remote reader
interprets and becomes a local `if` where the source already is.

When **every** partition is idle, `W := max(seen)` with no grace subtracted,
which closes every bucket holding data. That is exactly what
`idle_close_seconds` means today, preserved precisely.

### Why the transaction matters

The watermark is written in the same transaction as the rows whose arrival
justified it. That is what makes #374 unrepresentable rather than fixed: there
is no instant at which a reader can see rows without the watermark that
accounts for them, or a watermark without its rows. The workaround on #376 —
announcing a waking before the handler runs — exists only because those two
facts are written separately, and it can be deleted.

## What gets deleted

- The `idle` fact, `confirmedQuietSQL`, and the `last_commit - last_arrival`
  subtraction.
- The `source` fact and the `delivering_for_us` column, along with its NULL /
  negative / duration tri-state and the WAL it costs on every commit.
- `announceArrival` and the one-second threshold coupled to
  `idle_close_seconds` being whole seconds (#376).
- Five of the seven rows of the watermark truth table. What remains is
  `hold` and `close`, on one fact.
- `FactClock`, and with it the clock-domain rule — not enforced, but absent,
  because only one clock reaches a decision.
- The model's two-clock handling (`mono`/`skew`) and the `SourceLost` /
  `SourceBack` events, which exist to model a fact that no longer crosses a
  boundary.

## What becomes impossible rather than fixed

| defect | today | proposed |
|---|---|---|
| #374, rows dropped on a burst | fixed by a pre-handler write | no separate facts to desynchronise |
| the clamp / table contradiction | fixed by moving the rule | one fact, one rule, nowhere to disagree |
| a close deciding on a stale reading | narrowed to microseconds | the reading is the commit |
| event time compared to engine clock | forbidden by a documented rule | absent: one domain |
| a window closing during a rebalance | `source` fact, read remotely | the partition leaves the minimum, locally |
| watermark moving backwards | checked over random readings | `max(stored, W)`, monotonic by construction |

## What it does not solve, stated plainly

- **#183 is only half solved.** Per-partition watermarks with a minimum fix the
  *watermark* half — no worker closes a bucket another worker is still filling.
  The *state* half is untouched: two workers holding rows for the same bucket
  still publish two partial values, and that needs partition-keyed state or an
  additive sink contract. This design makes that work possible, not done.
- **Durability is orthogonal.** A windowed pipeline with no `state.path` still
  loses every open bucket and its watermark on restart. The watermark table is
  a durable fact only where the state is durable.
- **A stalled event-time clock still stalls the window.** If a device's clock
  freezes, `seen` stops advancing and nothing closes except by the idleness
  rule. That is the correct answer for event-time semantics, and it is now at
  least explicit rather than emergent.
- **Sink-side exactly-once is unchanged**: at-least-once plus an idempotent
  keyed sink, as the ledger says.

## Migration

1. Add the watermark table and write it in the batch transaction, alongside the
   existing facts. Nothing reads it yet.
2. Add a conformance test asserting the asserted watermark and the inferred one
   agree, over the simulator's existing scenarios. Any disagreement is a bug in
   one of them, and finding out which is the point.
3. Switch the manager to read the asserted watermark. Keep the old path behind
   nothing — delete it in the same commit, so there is one source of truth.
4. Delete the `idle` and `source` facts, `delivering_for_us`, and the rules that
   served them.
5. Add per-partition watermarks and the minimum, which is the step #183 needs.

Steps 1 and 2 are additive and safe to land independently; the design is worth
little until step 3.

## Cost and risk

- **Per batch:** one `max(time_column)` over rows already in memory, and one
  UPDATE in a transaction the batch already has. Against today: one fewer
  column written per commit, and the `idle`/`source` reads disappear from every
  poll.
- **The risk is concentrated in step 3**, where a wrong watermark silently
  closes buckets early — the same failure #374 produced. Step 2 exists to spend
  that risk before the switch, and #369's simulator is the instrument for it.
- **The framework in #369 is what makes this tractable**, and its gaps matter
  more under this design, not less: the model must be able to place a poll
  inside a batch, and the counting property must be able to fail on a bucket
  that never closes. Both are prerequisites rather than follow-ups.

## Why this is the right shape for IoT

The facts that disappear are the expensive ones. `delivering_for_us` is
rewritten on every commit and every idle tick because its value always changes,
which is roughly 154 bytes of WAL per write on a state path; the watermark
changes only when event time advances, so an idle pipeline writes nothing. The
idle tick stops being load-bearing for correctness, so a quiet gateway can
write less rather than more. And the decision no longer depends on a wall clock
at all, on a class of device whose wall clock boots at 1970 and then jumps by
decades.
