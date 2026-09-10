# Slow-moving data: the same confidence as throughput

Status: design, 2026-09-10.

## The problem

Every proof the engine carries today was taken under load. The throughput
matrix consumes ten million messages in seconds, the memory soak runs a
thousand messages a second for an hour, and the conformance harness drives
its sources as fast as they will go. Nothing proves what happens when a
stream is slow, stops, or never starts, and that is most streams most of the
time.

Slow data breaks different things than fast data. A batch that never fills
has to leave on a timer. An offset has to commit even though the batch was
one row. A state transaction has to close while nothing arrives. A window
whose rows have stopped coming has to close anyway. A consumer has to stay
joined to a broker it has not heard from in an hour. And an operator has to
be able to tell a pipeline that is idle from one that is stuck, which today
look identical from outside.

One of these is a known defect. The tumbling window examples shipped in #234
close a window against the stream's own clock, `max(bucket) - grace`, so a
replay no longer fragments windows. The cost is that a stream which goes
quiet never moves its clock, and its last window stays open until data
arrives that is a full grace period newer. A surge that stops leaves a full
bucket unpublished indefinitely.

## Three shapes, not one rate

Confidence means proving every invariant under each of these, because they
fail differently:

- **Trickle.** One message every few minutes, sometimes two in a second,
  sometimes a gap of an hour. Every message pays a full flush. The question
  is latency and liveness, not throughput.
- **Surge, then silence.** Minutes at benchmark rates, then nothing for an
  hour. The surge looks like the throughput matrix; everything interesting
  happens after it stops: the last partial batch, the last offset, the last
  window, and the memory that should come back down.
- **Nothing.** A pipeline that starts and receives no message for hours.
  Idle commits, a state file that does not grow, and a health check that
  still says healthy, because idle is not stuck.

## The invariants

| Invariant | Today | Proof |
| --- | --- | --- |
| A message reaches the sink within `flush_interval_seconds` of arriving, at any rate and after any gap | `pipeline.flush.eventually` is a harness cell driven at full speed | Cell C1 at a trickle, cell C2 after a surge, and the soak's latency sample |
| Offsets commit after every flush, so lag never exceeds one interval and a restart replays at most one interval | Unit tested | C1 and C2 check marks; the soak samples committed offset against produced |
| An idle stateful pipeline keeps committing, and empty commits do not grow the state file | Unit tested for one tick | C3 over many ticks; the soak samples file size through an hour of silence |
| A window whose rows have stopped closes within grace plus one poll, and a replay still publishes each window once | Fails for the shipped stream-clock examples | C4, both halves, on the new predicate |
| A consumer survives idle longer than its session timeout, and a WebSocket source survives a quiet server and reconnects after a dropped one | Untested | C5 against a real broker and a scripted server |
| A configuration that cannot make progress does not run quietly | `flush_interval_seconds: 0` already becomes 30 in `cli/run/root.go:346`; the invariant says otherwise and is marked unenforced | C6 pins the default; the registry entry is corrected and marked enforced |
| An operator can tell idle from stuck | `/stats` reports state only | Progress on `/stats`, a `/healthz` that goes red, and C7 |

## Engine changes

Three, all small.

### A progress table the SQL can see

The consume loop maintains one engine table, `sqlflow_progress`, beside
`sqlflow_offsets`, with one row:

```sql
CREATE TABLE IF NOT EXISTS sqlflow_progress (
  last_arrival TIMESTAMP,   -- wall clock when the newest batch was written
  last_commit  TIMESTAMP,   -- wall clock of the newest state commit
  messages     BIGINT       -- consumed since start
);
```

`processBatch` sets `last_arrival`, `last_commit` and `messages` inside the
state transaction it already commits, so they are durable with the offsets.
The idle tick sets `last_commit` only. A pipeline without a state file gets
the same table in its in-memory database. It is engine bookkeeping like
`sqlflow_offsets`, excluded from the state stats the same way.

The window manager runs its collect SQL on its own connection to the same
database (`internal/managers/tumbling.go:133`), so the predicate can read it
with no new plumbing.

### The window predicate: stream clock, or idleness

The shipped examples change from

```sql
WHERE bucket < (SELECT max(bucket) FROM agg) - INTERVAL '60' SECOND
```

to

```sql
WHERE bucket < (SELECT max(bucket) FROM agg) - INTERVAL '60' SECOND
   OR (SELECT now() - last_arrival FROM sqlflow_progress) > INTERVAL '60' SECOND
```

Under a replay, arrivals are continuous, the second branch stays false, and
the first closes each window once as the stream passes it, which is the
property #234 bought. When the stream goes quiet for the grace period, the
second branch closes every open window, including the newest. `now()` in the
manager's own statement is the statement's start time, so it is fresh on
every poll; the consume loop's idle commit is what keeps `last_arrival`
honest rather than frozen.

Rows that arrive for a bucket after it has closed on idleness form a new
partial for that bucket and publish as a second part. That is the engine's
existing at-least-once behaviour for late rows and the docs already describe
it; nothing here changes it.

### Progress on `/stats`, and `/healthz`

`/stats` gains a `progress` object: `last_arrival`, `last_commit`,
`messages`, and the two ages in seconds. `/healthz` returns 200 while
`last_commit` is younger than three flush intervals and 503 with the age in
the body otherwise. Idle is healthy; the commit keeps happening. Stuck is not.
The threshold is three intervals so one slow sink flush does not flap it.

## Conformance cells

Each is a Go test in the existing harness style, first line
`coverage.Covers(t, "<feature>")`, minutes at most, using the Turbine's
flush interval directly, which has no floor outside the config path.

- **C1, trickle latency.** A recording source emits one message every two
  seconds for twenty seconds, batch size 1000, flush interval one second.
  Every message reaches the sink within the interval plus a scheduling
  allowance, and the committed mark advances after each flush.
- **C2, surge then silence.** Five thousand messages at full speed, then
  nothing for five intervals. The final partial batch reaches the sink
  within one interval of the last arrival, the mark equals the last offset,
  and no further sink write occurs during the silence.
- **C3, idle commits stay flat.** A stateful pipeline with a state file,
  twenty idle ticks. `last_commit` advances on every tick, `last_arrival`
  does not, and the state file's size after tick twenty is within one
  checkpoint of its size after tick one.
- **C4, windows close on silence and stay whole on replay.** The example
  predicate, a one-minute window, a one-second poll, a five-second grace for
  the test. Half one: rows into one bucket, then silence; the bucket
  publishes once within grace plus a poll. Half two: rows spanning three
  buckets delivered in one burst; each bucket publishes exactly once. Half
  two is the #234 property and must keep passing.
- **C5, idle survival.** Kafka, against a real broker: consume one message,
  stay idle past a session timeout set to six seconds for the test, produce
  one, consume it. WebSocket, against a scripted server: a server that says
  nothing for ten seconds then sends a frame, delivered; a server that closes
  the connection, reconnected and the next frame delivered.
- **C6, no silent stall.** `flush_interval_seconds` absent, zero and
  negative all yield thirty seconds through the config path. The invariant
  registry entry is rewritten to say so and marked enforced.
- **C7, idle is healthy, stuck is not.** With a fake clock: `/healthz` is
  200 through idle ticks, and 503 with the age once no commit has happened
  for three intervals.

Cells C1 to C4 and C7 run in the unit pass. C5 is an integration cell.

## The soak

A skill beside `memory-soak`, `slow-soak`, with the same shape: a producer,
a sampler, a verdict, one line per sample, everything else in files.

The producer runs a scripted profile rather than a rate:

| Phase | Duration | Traffic |
| --- | --- | --- |
| surge | 5 minutes | 1,000 messages a second |
| silence | 60 minutes | none |
| trickle | 60 minutes | one message every 90 seconds, with two five-minute gaps and one pair sent in the same second |
| silence | 30 minutes | none |

Every message carries its send time. The sampler, once a minute, records:
messages produced and messages at the sink, the newest sink row's arrival
latency, the committed offset against the last produced offset, resident
anonymous memory, the state file's size, the count of published window rows,
and `/healthz`. The verdict fails on any of: a latency above the flush
interval plus thirty seconds, an offset lag above one interval at any sample
after the surge, a working set at the end of the second silence more than
ten percent above the working set before the surge, a state file that grew
through a silence, a window unpublished a grace and a poll after its last
row, or a red `/healthz` at any sample.

Default duration is the profile above, about two and a half hours. A short
profile of five minutes per phase exists for a dry run.

## Docs

- The tumbling window tutorials and the FAQ describe the predicate's two
  branches and the one sentence a reader needs: a quiet stream closes its
  open windows a grace period after its last row.
- The configuration page documents `sqlflow_progress`, `/stats` progress,
  and `/healthz`, beside the existing `flush_interval_seconds` text.
- The benchmarks page gains a "Slow data" section reporting the soak the
  way the memory section reports its hour.

## Out of scope

- An event-time watermark and a late-data policy, which is #203. The
  idleness bound closes windows; it does not reorder them.
- `max_memory` and `max_state_size`, the other half of #162.
- Sources other than Kafka and WebSocket in C5. The webhook source has no
  connection to keep alive.
