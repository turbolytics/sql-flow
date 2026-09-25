# The window simulator

A scripted run of the **real** consume loop and the **real** window manager
over one **real** DuckDB, with a fake Kafka coordinator underneath. You write
a sequence of events; it tells you how many rows were produced, published,
still open, and lost.

```go
r := RunWindowed(t, []int32{0}, windowDecl(), []Step{
        Produce{Partition: 0, Rows: 5, At: base.Add(30 * time.Second)},
        Produce{Partition: 0, Rows: 4, At: base.Add(5 * time.Minute)},
        Poll{},
})
// r.Produced, r.Published, r.StillOpen, r.LateDropped, r.Republished
```

## Why it exists

Three things are checked in three different places, and this is the only one
of them that runs the whole interaction:

| where | what it can see | what it cannot |
|---|---|---|
| `internal/managers` unit tests | one decision against hand-written rows | whether the engine ever writes those rows |
| `internal/managers/model.go` | every sequence of events, exhaustively | a real database, real SQL, real concurrency |
| **`internal/simulate`** | **the loop and the manager together, over one DuckDB** | more than one worker (see the end) |

The model proves the rules are consistent. This proves the engine and the
manager agree about what actually happened.

## Two clocks, and keeping them apart is the point

```
engine clock    advances one second per step, plus whatever Elapse adds.
                No window decision is made on it.
event time      what a row says about itself, set by Produce{At}.
                This is what buckets are cut on.
```

A simulator whose event time *is* its own clock cannot produce a row that
arrives out of order, a row for a bucket that already closed, a partition
lagging another, or a device whose clock is wrong — which is every question
an event-time window can get wrong. Keeping them independent is what makes
the scenarios below expressible.

## How to read the tables

Every scenario carries a table in its doc comment. The columns are what the
manager could see if it polled at that moment:

```
step           what the script does
event time     the row's own clock
bucket         event time truncated to the window size
window table   what the handler has written, and what a poll would read
watermark      what the manager has decided
```

Worked example — `OutOfOrderRowsInsideTheGraceAreNotLate`, under
`windowDecl()`: one-minute buckets, one minute of grace, a ten-second idle
close, late rows dropped.

```
step         event time   bucket   window table          watermark
-----------------------------------------------------------------------
Produce 3    12:01:30     12:01    12:01: 3              -
Produce 2    12:01:05     12:01    12:01: 5              -
               ^ older than the row before it, same bucket, not late:
                 nothing has closed, so there is no watermark to be behind
Produce 4    12:05:00     12:05    12:01: 5, 12:05: 4    -
Poll         -            -        12:05: 4              12:04
               ^ first close, by grace: newest 12:05 - 1m grace = 12:04.
                 12:01 ends at 12:02, at or before 12:04, so it publishes
                 all 5 rows. 12:05 ends at 12:06 and stays open.
```

## The four rules that explain every outcome

1. **Grace close.** The stream moving past a bucket closes it, and the
   watermark lands at `newest bucket − grace`.
2. **Idle close.** The engine confirming the stream quiet for
   `idle_close_seconds` closes everything, and the watermark lands at
   `newest bucket + size`. Note this is *further* than a grace close.
3. **The hold.** While the source can deliver nothing — a consumer between
   assignments, a websocket reconnecting — the loop resets its quiet clock on
   every idle commit, so that silence never accrues. Time the stream was
   never asked about is not evidence the stream is quiet. When the source
   comes back, the quiet it may confirm is bounded by how long it has been
   back.
4. **Late collection happens on a close, not on a poll.** The manager
   collects late rows only on a poll that moves the watermark. A run that
   ends without a close reports whatever the timing gave, which is why every
   scenario asserting a loss ends with `Elapse` / `IdleTick` / `Poll`.

Rule 4 is a property of the implementation rather than of the design, and it
made three scenarios flaky before it was understood: one read six drops
plainly and four under `-race`.

## The scenarios

Each test's doc comment carries its own table. This is the index.

### Delivery, with no window — `single_worker_test.go`

| scenario | what it proves |
|---|---|
| `ASingleWorkerPublishesEveryRow` | a restart, elapsed time and an idle tick in one run lose nothing |
| `ARestartLosesNothing` | two partitions, two restarts, nothing missing; duplicates permitted, losses not |
| `ARevokedPartitionStopsArriving` | a partition this worker does not own produces nothing for it, and that is not a loss |
| `AWorkerHoldingNothingReportsItCannotDeliver` | a worker between assignments reports that it holds nothing |

### Windows — `window_test.go`

| scenario | what it proves |
|---|---|
| `AWindowedRunAccountsForEveryRow` | rule 1: the stream moving on closes a bucket, and nothing evaporates |
| `AnIdleCloseNeedsTheLoopToConfirmTheQuiet` | rule 2: time passing is not enough; the loop has to commit before the manager may act |
| `ASourceThatCannotDeliverStopsTheIdleClose` | rule 3: five minutes of wall time, and not one second counts as the stream being quiet |
| `TheResumptionBoundsTheQuiet` | rule 3's bound, and the only scenario where the bound is what decides: the row would say five minutes, the source has been back five seconds, five seconds is the honest number |
| `TheIdleCloseResumesWhenTheSourceIsBack` | the same silence does close once the source has had a minute in which it could have delivered and did not |

### Event time — `event_time_test.go`

| scenario | what it proves |
|---|---|
| `OutOfOrderRowsInsideTheGraceAreNotLate` | the case `grace_seconds` exists for: records shuffled by less than the tolerance are not late |
| `ARowForAClosedBucketIsDropped` | the contract `late_rows: drop` promises |
| `AFastPartitionClosesASlowOnesBuckets` | **a defect** — see below |
| `APoisonTimestampClosesEveryBucket` | **a defect** — see below |
| `ReplayingEveryRowPublishesTheSameTotals` | **a defect** — see below |

### Not yet — `multi_worker_test.go`

Two scenarios whose scripts are written and whose runner is not: it drives
one worker. Both are tracked by [#183], and their doc comments carry the
diagrams that argue #183 is really two issues — a destination-schema
contract, and a recovery-model gap.

## The three defects this suite documents

These assert the engine's current behaviour, which is wrong. Each says in its
failure message how to invert it, so the day the fix lands the suite tells you
to turn it into an assertion of correctness.

**A fast partition closes a slow one's buckets.** The manager takes the newest
bucket in the table, whoever wrote it. So a partition racing ahead in event
time closes buckets another is still filling, and the slow partition's share
is dropped as late.

```
per-partition watermark, combined by minimum, would decide:
  p0 = 12:05 - 1m = 12:04       p1 = 12:00 - 1m = 11:59
  W  = min(...)   = 11:59   ->  12:00 stays open until p1 moves on
```

**A poison timestamp closes every bucket.** One row claiming the year 2099
advances the stream past every open bucket and lands the watermark 73 years
ahead. Every record that follows is late on arrival. One device with a wrong
clock, or one malformed field, silently drops the stream. The fix is to refuse
an event time outside the engine's bounds so it advances nothing.

**A replayed row doubles the bucket.** At-least-once means a restart re-reads
whatever the offsets did not cover. The engine has no record identity, so a
replayed row is simply a new row. Dedupe on an observation id, plus publishing
a whole value on a deterministic key, is what would make replay safe — and is
what lets a destination skip deduplicating at read time.

## What is pinned, and what is not

The loss counts are stable: five rows to the fast partition, seven to the 2099
timestamp, six to the closed bucket, across five plain runs and three under
`-race`.

What is **not** stable is how the *surviving* rows split between published and
still open, because the closing idle tick races the run's own stop. No
scenario asserts that split. If you need it, force the close and pin it
deliberately rather than relying on it.

## Adding a scenario

1. Write the script. `Produce{Partition, Rows, At}`, `IdleTick`, `Elapse`,
   `Revoke`, `Assign`, `Restart`, `Poll`.
2. If it asserts a loss, end with `Elapse` / `IdleTick` / `Poll` — rule 4.
3. Run it and *look at the numbers* before writing the assertion. Several of
   these behaved differently from how their author expected, and the
   difference was the interesting part.
4. Write the table in the doc comment. If you cannot fill in the watermark
   column, you do not yet know why your scenario passes.
5. Assert what is stable. Run `-count=5` and `-race` before believing it.

[#183]: https://github.com/turbolytics/sql-flow/issues/183
