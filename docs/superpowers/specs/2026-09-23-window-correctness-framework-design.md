# The window's correctness framework

Every fact the window decides on states where it came from, which clock
measured it, and what it was measured against. One table decides over those
facts. A simulator replays sequences of events against the pair and asserts
one end-to-end property. Nothing here changes when a window closes.

The decision was never the problem. `decide.go` made the rules total and
inspectable, and nine review passes over #352 found no wrong row in it. Every
defect those passes found lived one layer down, in a fact handed to the table:
an arrival stamped before the sink write rather than after it, a wall clock
where a monotonic reading was needed, a restart inheriting the previous
process's arrival, a drain forcing a write across a rebalance, a source that
could not deliver counted as a quiet stream. A total decision over a fact that
lies is still an early close.

## Why now

Correctness here has been established bottom-up: a defect is found, a fact is
corrected, a test pins that correction. That works and it has worked, but the
next fact nobody modelled is always one deploy away. Two things in the current
design show the pattern rather than the cure.

The newest correction sits outside the table. `holdQuietWhileNotDelivering`
rewrites `quietSince` before the manager computes anything, so "can the source
deliver" — a genuine input to the close decision — is a mutation applied to a
different fact instead of a fact of its own.

No invariant owns the window's output. A local reproduction of #183 published
12% fewer records than were produced, across six of twenty-one buckets, and
the ledger's 52 invariants recorded nothing, because each one owns a step and
none owns the result.

## Decisions

**Three facts, one table.** `Source` joins `Data` and `Idle` as a dimension,
so the hold becomes a row rather than a correction applied beforehand.

**The row carries the facts.** `sqlflow_progress` gains `delivering_since`,
and the manager derives both `Idle` and `Source` from one read of one row.

**Every fact carries its provenance.** What was measured, the limit it was
measured against, which clock produced it, and which component wrote it.

**One property, declared.** A bucket's published value counts exactly the rows
produced for it, once, with `late_rows: drop` as an explicit counted
exception.

**Sequences, not only combinations.** `checkTables` proves every state selects
one rule. It cannot prove that a run of events ends with a correct
publication, and that is where the defects were.

**No behavior change.** Any difference in when a window closes is a bug this
work found, and lands as its own commit with its own test.

## The clock domains

Two clocks decide a close, and nothing may compare across them.

*Event time* comes from the data: the newest bucket the stream has reached and
the committed watermark. It decides the grace path, where a bucket closes once
the stream has moved past its end by the grace.

*The engine's clock* comes from the progress row: `last_arrival`,
`last_commit`, and now `delivering_since`. It decides the idle path. All three
are written from one reading, and the manager only ever subtracts one from
another, so a wall-clock step moves none of them relative to the others.

The manager's own `now()` belongs to neither. Comparing it against
`last_arrival` read a sparse stream as stalled and a skewed clock as dead; #352
removed the last such comparison. The framework's job is to make the next one
visible in review rather than in a chart.

## The facts

| Fact | Values | Computed from | Clock |
|---|---|---|---|
| `Data` | `none`, `behind`, `open`, `ripe` | the newest bucket against the committed watermark | event time |
| `Idle` | `off`, `unconfirmed`, `confirmed` | `last_commit − last_arrival`, bounded by `last_commit − delivering_since` | engine |
| `Source` | `delivering`, `not_delivering` | `delivering_since` present or empty | engine |

`delivering_since` is the instant from which the source has been continuously
able to deliver: for Kafka, when the group last assigned it a partition; for a
websocket, when the connection was dialled; empty while it holds nothing or is
disconnected. The engine writes it as `now − deliveringFor`, from the monotonic
duration `core.Deliverer` already returns, so it lands on the same clock as the
other two columns.

Bounding the quiet by the resumption is what the hold does today, stated as a
fact instead of a rewrite. A websocket that dropped at 11:59:00 and reconnected
at 12:00:30, with `idle_close_seconds: 60`, has a row at 12:01:00 reading
`last_arrival 11:58:58, last_commit 12:01:00, delivering_since 12:00:30`. The
subtraction alone says two minutes of quiet and closes everything. The bound
says the source has been able to deliver for thirty seconds, and the window
holds. Same outcome as today; the difference is that the row now explains it.

## Provenance

Each fact is accompanied by the measurement that produced it:

```go
// Fact is one input to a decision, with the evidence for its value.
type Fact struct {
    Name     string        // "idle"
    Value    string        // "confirmed"
    Measured time.Duration // 62s
    Limit    time.Duration // 60s, the declared idle_close
    Clock    Clock         // EventTime, EngineClock, NoClock
    From     string        // "sqlflow_progress"
}
```

A close logs the rule it matched and the facts that selected it, each with its
clock. The generated `docs/windows/decisions.md` renders the same, so the page
states not only what the engine decides but what it decides on. `Clock` exists
so that a comparison mixing domains is a type-level smell in review, and so the
simulator can assert that no fact mixes them.

## The property

> A bucket's published value counts exactly the rows produced for it, once.

Two exceptions, both explicit. Rows dropped under `late_rows: drop` are
excluded and counted in `window_late_rows_total`, so the property holds over
rows the engine accepted. Under `late_rows: reemit` the published value is
replaced by the late rows alone, which is a different contract and is out of
scope here.

It enters `docs/coverage/invariants.yml` as
`pipeline.window.counts_every_row`, `class: safety`, `verified_by: harness`,
`tracked_by` the new multi-worker issue, because a rebalance breaks it today
and this PR does not fix that.

## The simulator

A deterministic replay drives a real turbine and a real window manager against
a scripted sequence of events, then asserts the property over what the sink
received. The events are the ones that have produced defects: a batch arrives,
an idle tick fires, the process restarts, partitions are revoked, the wall
clock steps forward or back, a sink write stalls, the progress store fails.

Single-worker sequences are in scope and should pass on today's code. Anything
involving a second worker is declared and skipped against the multi-worker
issue, so the ledger records the gap rather than implying coverage.

The simulator asserts three things: the property above, that no fact mixes
clock domains, and that a watermark never moves backwards.

## Out of scope

The drain-and-flush on revoke (#183) is a separate change, and the repro in
that ticket shows it does not by itself fix the windowed loss. Partition-keyed
window state and additive publication — the two candidate fixes for a bucket
split across workers — need the multi-worker issue and its own spec. Carrying
a generic origin (source, shard, position) on every message, so window state
can be keyed by it, belongs with that work.

## Delivery

This PR is the spec. The implementation follows in one PR: the `Source`
dimension and `delivering_since`, provenance on the facts, the declared
property, and the simulator with its single-worker sequences.

## Evidence

The reproduction behind this spec is in
[#183](https://github.com/turbolytics/sql-flow/issues/183#issuecomment-5793390961):
a two-partition topic, two workers in one group, ~400 msg/s, a 10-second
tumbling window with a Postgres upsert sink. On `93553f2` it published 31,600
of 36,000 records, short in six of twenty-one buckets, each short bucket
missing exactly one worker's partition share. No invariant failed while that
was happening.
