# The window as an asserted watermark

**Status:** implemented in #393, having been revised after an adversarial
review of #369. Three things the implementation decided that this document
left open or got wrong, recorded here so the spec and the code agree:

1. **Revoked leaves the minimum; lost holds it.** This document says "a
   revoked partition is not idle, it holds the minimum". That is right for a
   partition whose session failed and wrong for one a rebalance moved: the
   latter never delivers here again and never goes idle, so the minimum pins
   at its last position for the life of the process, no window on that worker
   closes again, and its table grows without bound. Kafka reports the two
   facts separately and the engine follows it. The argument is in
   `internal/core/watermarks.go`.
2. **Two rows, two owners.** The engine asserts into `sqlflow_watermarks`;
   the manager keeps what it has closed in `sqlflow_windows`. One row with two
   writers on two connections would make every batch a write-write conflict
   with every poll.
3. **The watermark's write is paced**, at one second, and forced by the drain.
   This document's cost section assumed the write was free because an idle
   pipeline makes none; a busy one advances event time on every batch, and one
   UPDATE is about 126 microseconds. Pacing leaves the watermark older than
   the rows it describes, which delays a close and can never bring one
   forward -- the same argument the progress write's pace rests on.
**Relates to:** #369 (the correctness framework), #374 (a burst's rows dropped as late), #183 (a bucket split across workers).

No windowed pipeline outside this repository depends on the current design, so
this is a redesign rather than a migration: the old facts are deleted, not
deprecated, and #374's workaround was reverted (#377) rather than shipped ahead
of it.

Two claims in the first draft were wrong and are corrected below: that the
`source` fact is deleted (it moves), and that the simulator's scenarios
transfer unchanged (three do; two depend on a rule this draft now states; and
the simulator cannot yet produce the one thing a watermark is about).

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
- The same reading was corrected in two places, `StateOf`'s clamp and the
  table's `hold.not_delivering` row, which then disagreed about a ripe bucket.
- The truth table needs 24 states and seven rules, almost all of which exist to
  reconcile the three.

Seven rules to answer one question is the symptom. The cause is that nobody
asserts the answer.

## What Flink does, and which of it we take

Flink's primitive is that **the watermark is in the stream**. `Watermark(T)` is
an element travelling in order with the records, meaning "no further record
will have an event time below T". It is asserted by the party that knows —
the source — and travels with the data it describes, so no reader ever has to
reconstruct it from side effects.

Its primitives divide into two sets, and the line between them is whether a
cluster coordinator is needed.

**Taken — these need no coordinator and are a straight simplification:**

| primitive | what it is |
|---|---|
| timestamp assigner | event time extracted per record, at the source |
| `forBoundedOutOfOrderness(delay)` | `W = maxSeen − delay` |
| per-partition watermarks, combined by minimum | a lagging partition holds the window open; a fast one cannot close it early |
| `withIdleness(duration)` | a quiet partition leaves the minimum instead of pinning it |
| a new partition holds the minimum at −∞ until it emits | do not close windows while an input that just arrived may still deliver older data |
| `EventTimeTrigger` | fire every window whose end the watermark has passed |
| allowed lateness, side output | what happens to a row for a window that already fired |

**Deliberately not taken — these need a JobManager and we have none:**

- `keyBy`, the network shuffle that guarantees one owner per key.
- Checkpoint barriers and coordinated rewind: a snapshot that advances state
  and source offsets together, job-wide, and rolls both back together on
  failure.
- Key-group rescaling.

The first set is exactly this proposal. The second set is exactly what
multiple consumers need, which is why that section below is a deployment
contract rather than an engine feature.

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
stream's own clock must reach", which is exactly `maxEventTime − delay`. The
engine already computes that value — in the manager's poll, as
`newest.Add(-decl.Grace).After(previous)`. The schema has been describing
watermarks all along. Only the plumbing disagrees.

One thing to say plainly so nobody reads it backwards: `grace_seconds` is
Flink's **out-of-orderness delay**, not its **allowed lateness**. Flink has two
knobs; we have one, and ours is the first. The lateness policy is `late_rows`.

## Where event time comes from

A watermark is only as true as the timestamps it is computed from, so the
timestamp assigner has to be explicit per source:

| source | event time | in Flink's terms |
|---|---|---|
| Kafka | the record timestamp: create time, or log-append time where the topic sets `message.timestamp.type` | the connector's assigner |
| HTTP / webhook | arrival, stamped by the engine | ingestion time |
| websocket (Bluesky) | **a field of the payload, named in the source's configuration** | a custom assigner |

The engine already extracts this, per record per source, for
`event_lag_seconds`, and names the clock in `event_lag_basis`
(`kafka_create_time`, `kafka_log_append_time`, `arrival`). The watermark is
computed from **that value and no other**. Growing a second notion of event
time would give the pipeline two clocks that disagree, and one of them would be
lying. The websocket source gains a configuration key naming the payload field,
and its basis becomes the field's name.

Two rules follow, and both are correctness rules rather than conventions.

**The watermark and `time_column` are the same clock.** The watermark is
`max(assigned event time) − grace`; buckets come from `time_column`, which the
handler's SQL computes. If a pipeline assigns event time from Kafka's record
timestamp and buckets on a payload field, the watermark bounds nothing: it
closes buckets on evidence about a different clock, which is the same error as
comparing event time to the engine clock, one level up. So the assigned
timestamp is exposed to the handler's SQL as a column, and `time_column` must
be derived from it. Validation checks the derivation where it can; the
conformance harness proves the two agree where it cannot.

**A poison timestamp must not close every window.** Under `W = max(seen) −
grace`, one record claiming the year 2099 advances the watermark past every
open bucket, publishes them all, and makes everything that follows late — one
device with a wrong clock, or one malformed field, and the stream is silently
dropped. Flink has the identical hazard. The event-lag work already refuses a
timestamp before 2020 (a device that booted at 1970) and one ahead of the
engine's own clock (a producer running fast); as **watermark** inputs those
rules become load-bearing, and "ignore for the metric" is not a policy for a
row that still has to go somewhere. The policy is: a row whose event time is
outside those bounds **does not advance the watermark** and is routed by the
pipeline's error policy — dropped and counted, or to the DLQ — never bucketed.
The bounds are the engine's, not the operator's, and they are the same bounds
the lag reporting uses.

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
  seen[p]        = max(event time) among rows this pipeline wrote for partition p
  candidate[p]   = seen[p] − grace_seconds
  W              = min over partitions in the minimum of candidate[p]
```

and advances the stored watermark to `max(stored, W)` — monotonic by
construction rather than by a checked invariant.

### The partition lifecycle

Which partitions are "in the minimum" is the whole of what `source` used to
say, now as a local rule in the engine — the only party with a coherent clock
and the source object that knows what it holds:

```
  assigned, nothing seen yet     candidate = −∞      holds the minimum
  delivering                     candidate = seen − grace
  silent for idle_close_seconds  leaves the minimum
  revoked                        holds the minimum  (see below)
```

A partition is **idle** when the engine has seen nothing from it for
`idle_close_seconds`, measured on the engine's monotonic clock from the later
of its last row and its assignment. When **every** partition is idle,
`W := max(seen)` with no grace subtracted, which closes every bucket holding
data — exactly what `idle_close_seconds` means today.

**A revoked partition is not idle. It holds the minimum.** This is where we
depart from Flink deliberately, and the first draft got it wrong by not
deciding. In Flink a split's state travels with it; here the window rows a
revoked partition already contributed stay in this process's table, and the
partition may come back. Treating revocation as idleness would advance the
watermark over those rows and publish them, and every row the partition
delivers on reassignment for the same buckets would be late — #374's shape,
produced by design. So the engine holds until the partition is reassigned or
the consumer leaves the group. That is what
`TestSimulate_ASourceThatCannotDeliverStopsTheIdleClose` and
`TestSimulate_TheResumptionBoundsTheQuiet` pin today, and they pin it
correctly; what changes is that the rule lives in the watermark's computation
rather than in a column a remote reader interprets.

So `source` is **moved, not deleted**: from a serialised tri-state column to
one term of a local minimum. The column, its NULL/negative/duration encoding,
its WAL cost and the table rows that interpret it go. The knowledge stays where
the source is.

### Why the transaction matters

The watermark is written in the same transaction as the rows whose arrival
justified it. That is what makes #374 unrepresentable rather than fixed: there
is no instant at which a reader can see rows without the watermark that
accounts for them, or a watermark without its rows.

## Multiple consumers

Four partitions, two consumers. Each consumer owns two partitions, has its own
DuckDB, its own window table, its own watermark and its own sink. Two problems
follow, and they need different answers; #183 has been hard to close because
one issue number covers both.

### Split state

A key `K` at bucket `T` receives rows from partitions owned by both consumers.
Each holds a partial and publishes it.

Flink prevents this with `keyBy`. We have no shuffle, so the two coherent
answers are:

- **Co-partition.** The window's key is the Kafka message key, so a key never
  spans partitions and never splits. The shuffle becomes the producer's job.
- **Merge at the destination.** Each process publishes its partial keyed on
  `(bucket, key, process)` and the destination combines across processes.
  The ledger already carries this as `pipeline.writers.merge_exactly`, proven
  against the Render template's schema. It works for associative aggregates —
  `count`, `sum`, `min`, `max` — and not for `count(distinct)`, `avg` without
  carrying counts, or percentiles.

Neither is currently stated as the rule. The 12% loss reproduced under #183 was
against a schema keyed on `(bucket, key)` that upserts, which is neither. The
configuration declares which one a pipeline is in, validation refuses an
aggregate the merge cannot combine, and the invariant
`pipeline.window.counts_every_row` is understood at the grain the rule implies:
per process under co-partitioning, per merged destination under merge.

### Stranded state

A consumer dies holding rows it ingested but had not published. Its offsets
were committed when those rows entered its window table — correctly,
atomically. The survivor takes the partitions and resumes from the committed
offset, which is past those rows. They exist only in the dead machine's state
file. Nobody will publish them, and no destination schema can help, because the
partial was never written.

Precisely: **offsets and window state commit atomically per process, but
recovery happens per group.** Flink makes those one unit — a checkpoint
advances both and a failure rewinds both. Our rewind unit is the group's
committed offset; our state unit is one machine's disk.

Three coherent answers, all real trade-offs:

1. **Offsets do not advance past an unpublished window.** Correct and simple to
   state; bounds consumer lag by `size + grace`.
2. **Shared window state**, so the survivor inherits. Changes the deployment
   shape and reintroduces concurrent writers to one table.
3. **Declare it**: a process that dies loses its unpublished window rows, and
   the docs and ledger say so rather than reading as a promise.

This proposal does not choose. It records that the choice exists, that it is
independent of the watermark, and that #183 should be split into one issue per
problem so each can be closed on its own terms.

### What the watermark does here

Per-partition watermarks with the minimum fix the **rebalance** case cleanly:
a partition just assigned holds the minimum at −∞ until it delivers, so the
survivor cannot close buckets its newly acquired partitions still have data
for. That is real. It fixes neither problem above, and "Flink-style
watermarks" should not be read as solving scale-out — it solves the ordering
half only.

## Working backwards from the configuration

Three keys have behavioural consequences: `grace_seconds` (0 or more),
`idle_close_seconds` (absent or set) and `late_rows`. Four stream shapes, each
a use case an operator would recognise, and what each one does:

| | `grace` | `idle_close` | the stream this is for | what closes a bucket | the failure mode to document |
|---|---|---|---|---|---|
| **A** | 0 | absent | an ordered firehose that never stops | only event time moving on | **a stream that stops never publishes its last bucket** |
| **B** | >0 | absent | a reordered firehose | event time, less the tolerance | the same, and the tail is `grace` further behind |
| **C** | 0 | set | ordered, intermittent | event time *or* silence | `idle_close` shorter than the gap *within* a burst closes mid-burst; the rest is late |
| **D** | >0 | set | bursty and reordered — the IoT default | either | both of the above |

Each crossed with `drop` (a straggler is discarded and counted) or `reemit` (a
straggler is republished as an update, which a replacing sink turns into
loss).

A and B are the liveness gap expressed as a configuration: with
`idle_close_seconds` absent, a stream that stops is *documented* to leave its
last bucket open. Today that is a sentence in a Go doc comment. It belongs on
the rendered page, beside the decision it produces.

Under the watermark design the decision table has two rules on one fact, so
"which rules can fire under my configuration" collapses to one question: can
the idleness rule fire at all. The rendered page carries that as a column.

## What gets deleted

- The `idle` fact, `confirmedQuietSQL`, and the `last_commit − last_arrival`
  subtraction.
- The `delivering_for_us` column, its NULL/negative/duration tri-state, the
  `ALTER TABLE` that migrates it in, and the WAL it costs on every commit.
- The `source` fact as a *fact the manager reads* — see above: the knowledge
  moves into the engine's minimum.
- Five of the seven rows of the watermark truth table. What remains is `hold`
  and `close`, on one fact.
- `FactClock`, and with it the clock-domain rule — not enforced, but absent,
  because only one clock reaches a decision.
- The model's two-clock handling (`mono`/`skew`) and its `SourceLost` /
  `SourceBack` events as *row* events. They survive as partition events, which
  is what they always were.

## What becomes impossible rather than fixed

| defect | today | proposed |
|---|---|---|
| #374, rows dropped on a burst | reverted; live on `main` | no separate facts to desynchronise |
| the clamp / table contradiction | fixed by moving the rule | one fact, one rule, nowhere to disagree |
| a close deciding on a stale reading | possible for up to a second | the reading is the commit |
| event time compared to engine clock | forbidden by a documented rule | absent: one domain |
| a window closing during a rebalance | `source` fact, read remotely | a partition at −∞ holds the minimum, locally |
| watermark moving backwards | checked over random readings | `max(stored, W)`, monotonic by construction |
| a poison timestamp closing every window | possible today | refused at the assigner; never advances W |

## What it does not solve, stated plainly

- **#183's two halves** are a deployment contract and a recovery-model choice,
  above. The watermark makes them stateable, not solved.
- **Durability is orthogonal.** A windowed pipeline with no `state.path` still
  loses every open bucket and its watermark on restart.
- **A stalled event-time clock still stalls the window.** If a device's clock
  freezes, `seen` stops advancing and nothing closes except by idleness. That
  is the correct answer for event-time semantics, and it is now explicit rather
  than emergent.
- **Sink-side exactly-once is unchanged**: at-least-once plus an idempotent
  keyed sink, as the ledger says.

## How the framework verifies this

The redesign has no dual-write to cross-check against — two sources of truth
for a window's progress is the condition it exists to end. So the framework in
#369 is the only instrument that says whether the watermark is right, and it
has to be able to see what a watermark is about before the watermark is
written. Three of its properties already can; two of its gaps are
prerequisites; and one of its claims in the first draft was false.

**What already carries over.** The model's counting property, able to fail on
a bucket that never closes; its ability to place a poll inside a batch; its
check that a close on idleness rests on silence the source could have filled,
recomputed from the model's own instants rather than taken from the row; the
clock and trigger seams (`core.WithClock`,
`WithFlushTrigger`, `managers.WithPollTrigger`); the simulator's runner, which
drives a real loop and a real manager over one DuckDB; the ledger entry; the
table checker and the rendered page. These are design-independent, and they
have killed real mutants.

**What the simulator cannot do yet, and must.** Its event time *is* the engine
clock: `bucket := h.clock().UTC().Truncate(h.size)`, and `Elapse` moves both.
It cannot produce one out-of-order row, one late row for a closed bucket, one
lagging partition, or one stalled device clock — which is every question a
`max(seen) − grace` watermark can get wrong. So, before the watermark is
written:

- `Produce{Partition, Rows, At}` — rows carry an event time the script
  chooses, independent of the simulated engine clock;
- a partition dimension on the windowed run, so two partitions can lag each
  other and the minimum has something to take;
- an out-of-range row, so the poison-timestamp policy is a scenario and not a
  paragraph.

**Which scenarios transfer.** Of the five windowed scenarios, three are pinned
on behaviour the redesign preserves and transfer unchanged:
`AWindowedRunAccountsForEveryRow`, `AnIdleCloseNeedsTheLoopToConfirmTheQuiet`,
`TheIdleCloseResumesWhenTheSourceIsBack`. Two —
`ASourceThatCannotDeliverStopsTheIdleClose` and `TheResumptionBoundsTheQuiet` —
encode the revoked-partition rule, and transfer only because this draft keeps
it. Had the first draft's "revoked is idle" rule stood, both would fail, and
they would have been right to.

**The properties the watermark must satisfy**, each as a scenario or an
enumeration, before the old facts are deleted:

1. Every row produced is published exactly once or dropped under the declared
   policy, after the stream quiesces — the existing property, per
   configuration shape A–D rather than for one declaration.
2. No bucket closes while a partition in the minimum could still deliver a row
   for it: a lagging partition holds the window; a newly assigned one holds it
   at −∞.
3. A revoked partition holds; a reassigned one resumes; an idle one leaves.
4. A row whose event time is out of range advances nothing and is routed, not
   bucketed.
5. The watermark never moves backwards, by construction — checked anyway,
   once, so a refactor that breaks the construction is caught.
6. The row and the rows are visible together or not at all: `Insert` followed
   by a poll closes nothing early, because under this design there is no
   `Insert` without its watermark. The test that asserts #374 today inverts.

The enumeration runs once per configuration shape, not once. Its must-reach
list is checked against the run's own decisions, not the quiescing tail, so a shape under which the idleness rule cannot fire fails
loudly rather than passing by construction.

## How this lands

No windowed pipeline outside this repository depends on any of it, so there is
no compatibility to preserve and no reason to stage the change. One change:
write the watermark, read the watermark, delete the facts it replaces, in the
same commit.

In order:

1. The framework prerequisites above — event-time `Produce`, partitions, the
   out-of-range row — landed first, on #369 or on top of it, with the six
   properties written as scenarios against the *current* engine. Some will
   fail against it (#374's, the poison timestamp's); those are characterisation
   tests until the switch and the evidence the switch was worth making.
2. The timestamp assigner made explicit per source, with the websocket
   configuration key, and `time_column` tied to it.
3. The watermark: table, computation, partition lifecycle, manager reading it
   and nothing else, old facts deleted, in one commit, against those scenarios.
4. #183 split into its two issues, and the split-state rule chosen and
   validated.

`poll_interval_seconds` becomes vestigial: with a watermark asserted at commit,
the poll is only a bound on how soon a closed bucket is noticed. The key stays
in the schema and stops mattering.

## Cost and risk

- **Per batch:** one `max(event time)` per partition over rows already in
  memory, and one UPDATE in a transaction the batch already has. Against today:
  one fewer column written per commit, and the `idle`/`source` reads disappear
  from every poll.
- **The risk is a wrong watermark silently closing buckets early** — the same
  failure #374 produced, and the reason this is worth care even with no users
  to break. What absorbs it is the framework, which is why its prerequisites
  come first rather than after.
- **The single-connection case is most of the value and none of the hard
  part.** A websocket source is one partition; the minimum is trivial. Landing
  it first, then Kafka's partitions, keeps the risky step small.

## Why this is the right shape for IoT

The facts that disappear are the expensive ones. `delivering_for_us` is
rewritten on every commit and every idle tick because its value always changes,
roughly 154 bytes of WAL per write on a state path; the watermark changes only
when event time advances, so an idle pipeline writes nothing. The idle tick
stops being load-bearing for correctness, so a quiet gateway can write less
rather than more. The decision no longer depends on a wall clock at all, on a
class of device whose wall clock boots at 1970 and then jumps by decades. And a
device whose clock is simply wrong can no longer take the pipeline's other
devices with it.
