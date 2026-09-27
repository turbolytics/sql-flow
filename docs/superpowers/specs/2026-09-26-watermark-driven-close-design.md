# The close is driven by the watermark, and lateness is decided at arrival

**Status:** proposal, approved in discussion on 2026-09-26.
**Builds on:** #393 (the engine asserts the watermark), #385 (`event_time` exposed to handler SQL), #389 (every source can be told where its event time is).
**Removes:** `poll_interval_seconds`, `late_rows`, the manager's poll loop, and the manager's late-row sweep.
**Adds:** `allowed_lateness_seconds`.

## Why this is the last step of the watermark work

#393 made the window's decision a function of one fact the engine asserts: a
bucket closes when its end is at or before the asserted watermark. Two
leftovers of the old design survived it, and they are the two things a person
reading the code still has to hold in their head that Flink's model does not
ask of them:

1. **The manager still polls.** Every `poll_interval_seconds` it wakes, reads
   the watermark, and acts. The poll never moved a watermark; it only noticed
   one that had. After #393 the engine knows the exact moment the watermark
   moves, because it is the party that moves it, and it says nothing.
2. **Late rows are swept, not refused.** A row for a bucket that already closed
   is written into the table by the handler and found later by the poll, which
   then drops it or republishes it. That sweep is the only reason the poll
   cannot simply disappear: a late row arrives without moving the watermark, so
   nothing would ever collect it.

Both leftovers exist because, before #393, the engine did not know where the
watermark was. Now it does, and the design that follows is what falls out:
the engine tells the manager when the watermark moved, and decides at arrival
whether a record is late, exactly as Flink's window operator does. What is
left of the manager is small enough to describe in two sentences, and
`poll_interval_seconds` goes because nothing remains for it to pace.

## What Flink does

Take a one-minute bucket `[12:00, 12:01)`, `count(*)`, and `allowedLateness`
of five minutes. Everything is measured by the watermark, never a wall clock:

```
watermark    what happens to bucket 12:00
──────────────────────────────────────────────────────────────────────────
< 12:01      open. Rows land in it.
= 12:01      FIRES: count(*) over its rows → emits 100.  State kept.
12:01–12:06  a row stamped 12:00:30 arrives → late but allowed:
             added to state, FIRES AGAIN → emits 101.  The whole value.
             another → emits 102.
= 12:06      end + lateness reached → state PURGED.
> 12:06      a row stamped 12:00:30 arrives → dropped (or side output).
```

Two primitives, and neither has a clock in it:

**`allowedLateness(d)`** is one number that does three things: how long a
fired window's state is kept, how long late rows are still accepted, and when
the purge happens. The default is `0`: fire once, purge at once, everything
after is dropped.

**The re-fire is a recompute, not a delta.** Flink runs the window function
over the *whole* retained state and emits the full updated value -- 101, not
1. Downstream treats it as a replacement for `(window, key)`. Flink's window
operator never emits a delta.

The mechanism underneath, in the window operator:

```
record arrives
  ├─ assign window: [floor(ts/size)·size, +size)
  ├─ late?  window.end + allowedLateness <= currentWatermark
  │     yes → dropped (or side output). Never enters state.
  └─ no  → added to state; trigger.onElement:
             window.end <= watermark ?  → FIRE now
             otherwise                  → register an event-time timer at window.end

watermark advances past a registered timer
  ├─ fire: run the window function over the state, emit
  └─ at window.end + allowedLateness: purge the state
```

Lateness is decided **at arrival**. Firing is caused by the **watermark
advancing**. Re-firing for a late-but-allowed record is caused by **that
record's arrival**. The purge is another watermark-driven timer. The one
wall-clock element in Flink's event-time story is `withIdleness` at the
**source**, which stops an idle partition holding the watermark back; we have
exactly that in the engine's idle tick, and it is the source's clock advancing
the watermark, not a window flush.

## Mapped to our configuration

| ours | Flink | what it means |
|---|---|---|
| `grace_seconds` | `forBoundedOutOfOrderness(d)` | watermark = newest seen − d |
| `idle_close_seconds` | `withIdleness(d)` | a silent partition stops holding the watermark back |
| **`allowed_lateness_seconds`** (new) | `allowedLateness(d)` | a closed bucket's rows are kept, and late rows accepted, until the watermark reaches end + d |
| `late_rows: drop` (removed) | `allowedLateness(0)` | -- |
| `late_rows: reemit` (removed) | **no equivalent** | emits a delta; Flink never does |
| recompute (the behaviour within lateness) | the default re-fire | re-run `emit_sql` over the whole bucket, publish the full value |

`drop` and `recompute` are not alternatives in Flink; they are the two sides of
one number. Beyond `end + lateness` every window drops; within it every window
recomputes. With lateness `0` there is no "within", so it is pure drop. So
**`late_rows` goes entirely**, and `allowed_lateness_seconds`, default `0`, is
the single knob. Today's `drop` is the default; today's `reemit` has no
successor, because what it did -- publish the late rows alone -- is a delta,
and the whole point of this change is that the sink always receives the exact
value.

## The proposal

### The engine, per record

```
record arrives (placed)
  bucket   = trunc(event_time, size)
  end      = bucket + size
  ├─ end + lateness <= W   → LATE: refused, counted. Never inserted.
  ├─ end            <= W   → late but allowed: inserted, signal(recompute, bucket)
  └─ otherwise             → handler as now
commit
  └─ W moved               → signal(watermark)
```

`W` is the engine's **own** asserted watermark, from `core.Watermarks`. No
read of another connection, no read of the manager's state. The check sits
beside the placement rule (#383), which already refuses a record whose event
time this engine cannot place: the two together are the timestamp assigner and
the lateness check of Flink's operator, in the one place that has the record,
its event time and the watermark at once.

Under `allowed_lateness_seconds: 0` the middle branch never fires: a bucket at
or below the watermark is closed, and a row for it is refused. Under a positive
lateness a row for a closed bucket that is still within lateness is written by
the handler as any row is, and the engine remembers the bucket so the manager
can republish it.

**This repairs a promise #393 made and did not fully keep.** The watermark's
meaning is: *as of this commit, every row this pipeline will ever write for
this window has `time_column` at or after the watermark*. Today late rows break
that at the source -- the handler writes them below the watermark -- and the
manager's sweep cleans up after. With lateness decided at arrival the promise
is literally true for lateness `0`, and precisely `>= W − lateness` otherwise.
A row below the watermark can only be in the table because the engine allowed
it there, so the manager's late-row branch is deleted rather than moved.

### The signal

One per window, owned by `core.Watermarks`, handed to the manager at build:

```go
// WindowSignal is what the engine tells one window's manager.
type WindowSignal struct {
	// kick is closed-over by the manager's select. Capacity one, sent to
	// without blocking: two kicks before the manager wakes are one kick,
	// because a pass reads the current state, not the event.
	kick chan struct{}
	// recompute is the buckets a late-but-allowed row landed in since the
	// manager last drained it. A set, so a burst of late rows for one bucket
	// is one recompute.
	mu        sync.Mutex
	recompute map[time.Time]struct{}
}
```

Both signals are sent **after the commit** that makes their rows visible,
from the same place the tracker records the assertion (`commitState`, after
`windows.Commit(moved)`). A manager woken by a kick reads committed state,
which is the property #393 arranged for the progress snapshot and which
holds here for the same reason. Recompute buckets are accumulated during the
batch and published to the set at commit; a batch that rolls back publishes
none.

### The manager

Two signals, no clock, no sweep, no configuration:

```
on start        one pass -- what a restart left: due buckets, retained buckets
on kick         one pass
on drain        one pass, on the drain budget

one pass:
  load asserted W (engine's) and closed C (own)
  publish every bucket with C < end <= W          (the close, as today)
  republish every bucket in the recompute set     (full value, emit_sql over the whole bucket)
  delete every bucket with end + lateness <= W    (the purge)
  save C := W
```

What is deleted from `internal/managers/watermark.go`: the poll ticker and
`Start`'s select on it; `poll` and `defaultPollInterval`; the late-row branch
of `Poll` (`countClosedSQL(closed)` for late rows, `DropLate`, `ReemitLate`,
`lateToReemit`, `dropped`, `lateCounted`); `LatePolicy`, `ParseLatePolicy`,
`Declaration.Late`. The bucket table in `decide.go` loses its two `late.*`
rows and its `policy` fact: a bucket is `open`, `due`, or `retained` (closed,
within lateness, kept for recompute), and the table says what each is.

What is renamed: `Poll` becomes `Pass`, because nothing polls. `WithPollTrigger`
becomes the wiring of the signal, because what was a test seam is the
production mechanism.

The pass on start is what makes a restart safe without the manager remembering
anything: due buckets are published, and every retained bucket is republished.
The recompute set is in memory, so a late-but-allowed row committed just before
a crash would otherwise never be republished; republishing every retained
bucket at start is bounded by `lateness / size` buckets per key and is
idempotent, because the value published is the whole bucket.

### Configuration

```yaml
window:
  time_column: bucket
  size_seconds: 60
  grace_seconds: 60                 # unchanged
  idle_close_seconds: 10            # unchanged
  allowed_lateness_seconds: 300     # new; absent or 0 means late rows are refused
  emit_sql: ...
  sink: ...
```

Removed: `poll_interval_seconds`, `late_rows`. The window block is
`additionalProperties: false`, so a configuration that still sets either
**fails validation** with a message naming the replacement. That is the right
outcome: the key's meaning is gone, and silently ignoring it would let a
pipeline run with a different lateness policy than its author wrote down. No
pipeline outside this repository windows, so there is nothing to migrate and
no deprecation period.

Everywhere the keys appear today, all updated in the same change:

| where | today | after |
|---|---|---|
| `internal/config/config.go` | `PollIntervalSecs`, `LateRows` | `AllowedLatenessSecs` |
| `internal/validate/schemas/config.json` | both keys | regenerated |
| `render/pipeline.yml` | `poll_interval_seconds: {{ SQLFLOW_WINDOW_POLL_SECONDS\|default('10') }}`, `late_rows` | removed; `SQLFLOW_WINDOW_POLL_SECONDS` is removed from `render/docker-compose.yml`, which sets it to 1 twice |
| `dev/config/examples/*.yml` (six windowed) | `late_rows: drop`, one `poll_interval_seconds: 3600` | removed |
| `dev/bench/bluesky/*.yml` (three) | `poll_interval_seconds: 1` | removed; the comment that says the bench sets it to 1 goes with it |
| `.claude/skills/slow-soak/slow.yml` | both | removed |
| `README.md` | the window section's `poll_interval_seconds` line and the late-rows paragraph | rewritten for `allowed_lateness_seconds` |

`validate` rules, replacing the two `late_rows` ones:

- `allowed_lateness_seconds > 0` **requires a sink that replaces by key** --
  today the postgres upsert. A republished bucket is the whole value, and an
  append-only sink would hold it twice. This inverts today's
  `ReemitOverwrites` rule, which refuses `reemit` *with* an upsert because a
  delta would overwrite the bucket; a recompute is the value the upsert
  wants. Flink's contract is the same: a downstream of a window with allowed
  lateness must handle updates.
- `allowed_lateness_seconds` cannot be negative, and is refused without
  `event_time` in the handler's SQL, for the reason in the next section.

### The engine's bucket must be the handler's bucket

The engine computes `trunc(event_time, size)`; the handler's SQL computes
`time_column`. If the two disagree, lateness is decided against the wrong
bucket: a row is refused that the handler would have put in an open bucket,
or admitted into one that closed. So the #385 promotion is a **prerequisite,
and it gets sharper**:

- `validate` **refuses** (no longer warns) a windowing pipeline whose
  `time_column` is not `time_bucket(INTERVAL '<size_seconds> seconds', event_time)`
  -- the form every shipped example takes today with the payload field, which
  #385 lets them take with `event_time` instead. The five shipped windowed
  examples are updated in this change.
- A test in `internal/core` proves the engine's Go truncation agrees with
  DuckDB's `time_bucket` for every `size_seconds` the configuration accepts,
  over a grid of instants including bucket boundaries and the microsecond
  either side of them. If a size disagrees, that size is refused by validate
  rather than silently mis-bucketed.

Flink has no such requirement because its `WindowAssigner` assigns the window
and the function only reads it. We could get the same guarantee by
construction later, by having the engine expose the bucket as a column the
handler must use; this design does not, because the check-and-prove route is
smaller and every shipped example already has the required form.

### What windows do when nothing arrives, or what arrives is wrong

Unchanged by this design, and stated so nobody reads the removal of the poll as
a change in liveness:

| situation | window stays open? | why |
|---|---|---|
| stream stops, `idle_close_seconds` set | no | the engine's flush tick sees every partition silent for the bound and asserts `max(seen) + size`; the kick that follows closes everything held |
| stream stops, no `idle_close_seconds` | yes | shapes A and B, documented: a stream that stops leaves its last bucket open. Flink is identical |
| data flows, event time frozen | yes | the partition is delivering, so it is not idle; its event time never advances, so the watermark never moves. Visible as `event_lag_seconds` climbing while `messages_consumed` moves. Flink is identical. Not fixed by a heuristic here; an opt-in knob if it is a material problem in the field |
| data flows, event time goes backwards | no | those rows are late: refused, or recomputed within lateness |
| data flows, event time jumps ahead | no | refused by placement before the handler (#383) |

The poll never moved a watermark, so removing it changes when a close is
noticed and never whether it happens. The `idle_close_seconds` row is the one
wall clock in the whole design, and it is the source's, as in Flink.

## What gets deleted

- `poll_interval_seconds` and `late_rows`, everywhere the table above lists.
- The manager's ticker, `defaultPollInterval`, `WithPollTrigger` as a test-only
  seam.
- The manager's late-row sweep: the count, the delete, the reemit collect, and
  the counter recorded from inside a poll.
- `LatePolicy`, `ParseLatePolicy`, `Declaration.Late`, the bucket table's
  `policy` fact and its two `late.*` rows.
- The `reemit` semantics and `ReemitOverwrites`, the rule that refused
  pairing it with a replacing sink.
- `SQLFLOW_WINDOW_POLL_SECONDS` from the Render template.

## What changes in the ledger

- `manager.publish.eventually` today reads *"the loop polls on its own, and a
  window that closes is published."* It becomes: *a closed window reaches the
  sink on the manager's start pass or on the kick that follows the commit which
  closed it, without anything else happening.* The claim is the same
  liveness; the mechanism named is the real one.
- `manager.late.policy_holds` and `manager.late.counted_once` are about the
  sweep and go with it. Their replacement is engine-side:
  `window.lateness_decided_at_arrival` -- *a record whose bucket ended at or
  before the watermark less the allowed lateness is refused before the handler
  and counted; one within lateness is written and republished as the bucket's
  whole value; the window table never holds a row the engine did not admit.*
  Verified by the conformance harness's pipeline subject, which this change
  gives a windowed shape (the follow-up #393 recorded), and by the model and
  simulator below.
- `window_late_rows_total` is counted at the engine, on the record path, with
  `outcome="refused"` or `outcome="recomputed"`. It shares the placement
  counter's caveat: a batch that fails and replays counts its refused rows
  again. The manager's copy of the counter goes.

## How the framework verifies it

**The exact value is the property.** For every bucket, the *last* value the
sink holds equals the rows produced for it less the rows refused as beyond
lateness -- never a delta, never a stale first publish. That is what "recompute"
means and it is what `reemit` could not satisfy, so it is the assertion every
new scenario ends on. The simulator's window sink keeps the last value per
bucket (replace semantics) rather than summing everything it was ever sent.

**Model** (`internal/managers/model.go`): the alphabet gains
`Produce{Late}` (a row for a bucket at or below the watermark, within
lateness) and `Produce{VeryLate}` (beyond it); the shapes A--D are crossed
with `allowed_lateness` of `0` and of one bucket. Properties, each per shape:
every row produced is in the last published value of its bucket or was refused
beyond lateness; a refused row is never in the table; a recompute publishes the
whole bucket; nothing below `W − lateness` is ever in the table after a pass.

**Simulator** (`internal/simulate`): `Poll{}` becomes the manager's `Pass`,
called synchronously as today, so scripts keep their shape. New scenarios,
each with its table:

- `ALateRowRecomputesTheBucket` -- the sink's last value for the bucket is the
  exact count after the late row, not the late row alone.
- `ALateRowBeyondLatenessIsRefused` -- never in the table, counted as refused.
- `ABurstOfLateRowsIsOneRecompute` -- three late rows for one bucket, one
  republish, the exact value.
- `ARecomputeSurvivesARestart` -- a late row committed, a restart before the
  manager passes, the start pass republishes the exact value.
- `TheKickFollowsTheCommit` -- a pass driven by the signal rather than by
  `Pass{}` sees the rows the commit wrote.

`ARowForAClosedBucketIsDropped` is rewritten: the six late rows are refused at
the engine and never reach the table, and `LateDropped` is measured from the
engine's counter rather than as a residual.

**Conformance** (`internal/conformance`): `ManagerSubject.New` loses its poll
argument. `checkPublishEventually` passes on the start pass alone, which is
the claim's new wording. `checkLatePolicyHolds` and `checkLateCountedOnce`
are removed with the sweep; the pipeline subject gains a windowed shape and
checks `window.lateness_decided_at_arrival` through the real engine.

**Unit** (`internal/core`): the bucket agreement test above; the late check
against a grid of `(event_time, W, lateness)` including the boundaries; the
signal is sent after the commit and not before; a batch that rolls back
publishes no recompute buckets.

## Cost

Closed buckets are retained for `lateness` instead of deleted at close:
`lateness / size` extra buckets per key in the window table, deleted on the
watermark move that passes `end + lateness`. With the default of `0` nothing
changes. A refused late row is never inserted, which removes a write-then-
delete on the window table -- the pattern #268 says DuckDB never frees under
an index -- for the case that used to produce it most.

The manager does no work between signals. An idle pipeline's manager wakes
zero times, against 8,640 a day at the old default.

## What it does not solve

- **#183's two halves** are untouched. A recompute republishes *this* worker's
  whole value for a bucket, which under co-partitioning is the bucket's value
  and under a split is one worker's share; the destination contract is the
  same question it was.
- **A frozen event-time clock** still stalls the window, as above.
- **Multiple windows on one pipeline** each get their own signal, their own
  lateness and their own manager, as they have their own declaration today.

## How this lands

One change, because the pieces do not stand on their own: a manager without
a poll needs the signal, the signal without engine-side lateness leaves late
rows uncollected, engine-side lateness without the bucket agreement decides
against the wrong bucket, and none of it makes sense with `late_rows` still
in the schema. In order within it:

1. The #385 promotion: `validate` refuses a windowing pipeline whose
   `time_column` is not `time_bucket` over `event_time`; the shipped examples
   updated; the bucket agreement test.
2. `allowed_lateness_seconds` in the config, `late_rows` and
   `poll_interval_seconds` out, schema and goldens regenerated, the Render
   template and every shipped configuration updated.
3. The engine's per-record lateness check and the two signals.
4. The manager: one pass, three occasions, the sweep and the ticker deleted.
5. The model, the simulator, the conformance subject, the ledger.

`poll_interval_seconds` is gone rather than deprecated, and a configuration
that sets it fails validation naming this document.
