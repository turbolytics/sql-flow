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
                It decides only whether a partition has gone idle.
event time      what a row says about itself, set by Produce{At}.
                This is what buckets are cut on, and what the watermark is
                computed from.
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
asserted       the engine's watermark after the step's commit: the minimum
               over the partitions that could still deliver, each at its
               newest event time less the grace. A bucket closes when its
               end is at or before it.
```

The engine asserts that value in the commit that makes the rows visible
(`internal/core/watermarks.go`); the manager reads it and nothing else — no
clock of its own, no progress row, no reading of the table's newest bucket.

Worked example — `OutOfOrderRowsInsideTheGraceAreNotLate`, under
`windowDecl()`: one-minute buckets, one minute of grace, a ten-second idle
close, late rows dropped.

```
step         event time   bucket   window table          asserted
-----------------------------------------------------------------------
Produce 3    12:01:30     12:01    12:01: 3              12:00:30
Produce 2    12:01:05     12:01    12:01: 5              12:00:30
               ^ older than the row before it, same bucket, not late: the
                 newest event time is still 12:01:30, so nothing moved
Produce 4    12:05:00     12:05    12:01: 5, 12:05: 4    12:04
Poll         -            -        12:05: 4              12:04
               ^ 12:01 ends at 12:02, at or before 12:04, so it publishes
                 all 5 rows. 12:05 ends at 12:06 and stays open.
```

## The four rules that explain every outcome

1. **While partitions deliver**, the assertion is the minimum over them of
   `newest event time − grace`. A partition racing ahead cannot close the
   buckets a slower one is still filling; one that has delivered nothing
   holds the minimum at −∞.
2. **When every partition has been silent for `idle_close_seconds`**, the
   stream is done with what it has, and the assertion is `newest event time
   + size`, which closes the newest bucket too. Note this is *further* than
   rule 1 ever reaches.
3. **Lost holds, revoked leaves.** A partition whose session failed may come
   back with a backlog for the buckets this worker holds, so it stays in the
   minimum at its last position and is never idle. A partition another worker
   now holds is gone from the minimum: its future rows are that worker's, and
   holding for them would freeze this worker's windows for good. Idleness is
   measured from the later of a partition's last row and its assignment, so
   an outage counts for nothing and the clock restarts on the return.
4. **Late collection happens on a close, not on a poll.** The manager
   collects late rows only on a poll that has something to do. A run that
   ends without a close reports whatever the timing gave, which is why every
   scenario asserting a loss ends by forcing one.

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
| `AnIdleCloseNeedsTheLoopToAssertIt` | rule 2: time passing is not enough; a commit has to find the partition idle before anything closes |
| `ALostPartitionHoldsTheWindow` | rule 3: five minutes with the session gone, and nothing closes |
| `TheAssignmentBoundsTheIdleness` | rule 3's bound, and the only scenario where the bound is what decides: five minutes of outage, back for five seconds, and five seconds is the honest number |
| `TheIdleCloseResumesWhenThePartitionIsBack` | the same silence does close once the partition has had a minute in which it could have delivered and did not |
| `ARevokedPartitionLeavesTheMinimum` | rule 3's other half: a scale-out closes by the remaining partition rather than freezing |
| `ARevokedPartitionsRowsStillCloseOnIdleness` | the liveness that rule depends on: the rows a revoked partition left behind close even after every remaining partition goes quiet, because the all-idle close is measured over everything this process ever placed |
| `AWorkerHoldingNothingClosesWhatItHas` | a worker scaled down to no partitions publishes its table rather than stranding it |
| `ARestartStillClosesByIdleness` | a new process has seen no rows, so the idle close is measured from what its table holds |

### Event time — `event_time_test.go`

| scenario | what it proves |
|---|---|
| `OutOfOrderRowsInsideTheGraceAreNotLate` | the case `grace_seconds` exists for: records shuffled by less than the tolerance are not late |
| `ARowForAClosedBucketIsDropped` | the contract `late_rows: drop` promises |
| `AFastPartitionHoldsForTheSlowOne` | rule 1's minimum: the lagging partition holds the bucket both were filling, and its rows are never late |
| `APoisonTimestampClosesEveryBucket` | one wrong clock costs the pipeline exactly the record that carried it |
| `ReplayingEveryRowPublishesTheSameTotals` | **a defect** — see below |

### Not yet — `multi_worker_test.go`

The per-partition watermark fixes the ordering half of scale-out and neither
half of the two below.

Two scenarios whose scripts are written and whose runner is not: it drives
one worker. Both are tracked by [#183], and their doc comments carry the
diagrams that argue #183 is really two issues — a destination-schema
contract, and a recovery-model gap.

## The two defects this suite closed, and the one it still documents

**A fast partition closed a slow one's buckets.** The manager used to take the
newest bucket in the table, whoever wrote it, so a partition racing ahead in
event time closed buckets another was still filling and the slow partition's
share was dropped as late. `AFastPartitionHoldsForTheSlowOne` pinned that loss
at five rows; it now asserts that nothing is lost, because the watermark is
the minimum:

```
  p0 = 12:05 - 1m = 12:04       p1 = 12:00:45 - 1m = 11:59:45
  W  = min(...)   = 11:59:45  ->  12:00 stays open until p1 moves on
```

**A poison timestamp closed every bucket** — [#358]. One row claiming the year
2099 advanced the stream past every open bucket and landed the watermark 73
years ahead; every record that followed was late on arrival. One device with a
wrong clock, or one malformed field, silently dropped the stream. The fleets
most exposed were the ones least able to prevent it: gateways with no
battery-backed clock, which boot to a fixed epoch and drift until NTP syncs.
The engine now refuses an event time it cannot place, before the handler, so
it reaches neither the table nor the watermark.

**A replayed row doubles the bucket** — still open. At-least-once means a
restart re-reads whatever the offsets did not cover. The engine has no record
identity, so a replayed row is simply a new row. Dedupe on an observation id,
plus publishing a whole value on a deterministic key, is what would make
replay safe — and is what lets a destination skip deduplicating at read time.

## What is pinned, and what is not

The published and still-open counts are pinned in the windowed scenarios now,
because the close no longer races the run's own stop: a bucket closes when
the engine has asserted past its end, and the assertion is committed before
the commit that carries it is visible. A scenario that ends without a poll
still leaves whatever the last poll did not close.

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
[#358]: https://github.com/turbolytics/sql-flow/issues/358
