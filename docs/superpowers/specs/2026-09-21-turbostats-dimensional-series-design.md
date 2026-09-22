# TurboStats v1 amendment: dimensional series

This spec amends `2026-09-19-turbostats-contract-amendment-design.md`. It adds
ten fields to the `pipeline` section, summaries of series the engine records
under attributes, and three gauges the bundle needs to read them: when lag
was observed, and each window's close lag and newest bucket. It changes no existing field, so it
is additive and stays v1.

It also retires the 1 KiB bundle bound as a design constraint. Measurement
below shows the bound protected nothing it was believed to protect.

Where this spec and the earlier ones disagree, this spec wins.

## Why now

The bundle cannot answer whether a pipeline is keeping up.

It reports 31,966 messages consumed. It does not report that the pipeline is
400,000 messages behind, because `consumer_lag` carries `topic` and
`partition` attributes and `Collect` reads only attribute-free points. Lag is
the first question an operator asks about a streaming pipeline, and the
control plane cannot show it.

Four more signals are invisible for the same reason:

| Instrument | Attributes | Recorded at |
|---|---|---|
| `consumer_lag` | `topic`, `partition` | `core/turbine.go:1003` |
| `window_late_rows` | `window`, `policy` | `managers/watermark.go:273` |
| `window_closed` | `window` | `managers/watermark.go:325` |
| `window_watermark_seconds` | `window` | `managers/watermark.go` |
| `sink_retry_count` | `sink` | `sinks/metrics.go:45` |

`window_late_rows` under `policy=drop` counts rows the engine deleted. That is
silent data loss, and today nothing outside a Prometheus scrape can see it.

The earlier spec put "per-dataset numbers and per-table gauges" out of scope.
That was right when no receiver existed. The control plane now draws these
pages, so the deferral has a cost that can be named.

## The rule this turns on

The earlier spec reads every number from a dimensionless series, and argues
the case well: `sink_flush_count` carries `result=ok` and `result=error`, and
summing them reports a number of flushes that is true of nothing.

That argument is about what the attribute *means*, not about attributes. It
divides into two kinds:

- An **outcome** attribute partitions points that measure different things.
  `result`, `policy`, `outcome`, `code`. Collapsing one destroys the fact.
- A **shard** attribute partitions the same measurement across parts of one
  system. `topic`, `partition`, `window`, `sink`. Collapsing one is arithmetic
  that stays true.

So the amended rule:

> A bundle field may summarize an attributed series when it collapses only
> shard attributes, and when its name states both the aggregation and the
> attribute it collapsed. Outcome attributes are never collapsed; each value
> gets its own field or none does.

`late_rows_dropped` and `late_rows_reemitted` are separate fields under this
rule, because `policy` is an outcome. That is the rule catching the exact
mistake the original argument warned about.

## Decisions

| Decision | Choice | Rejected |
|---|---|---|
| Shape | Fixed scalar fields, one per aggregate. | A map keyed by partition. Its cardinality is the broker's to choose, so the document's width would be set by someone outside this system. A heartbeat is a fixed-shape record; a variable-width one is a metrics payload wearing its clothes. |
| Grain | Summaries only. | Per-partition detail at a 10s cadence duplicates Prometheus, which already does it better and is already exposed. The heartbeat answers "which instance is behind"; `/metrics` answers "which partition". |
| Which aggregate | `max` and `sum` for lag, named in the field. | One of them: `max` alone hides a uniform backlog, `sum` alone hides one stuck partition. |
| Where it is computed | `Collect`, from the points the reader already returns. | The recording site: the per-message path is 22 ns/op and a second instrument is a cost the operator never asked for. |
| Lag as a gauge | Allowed, and the one exception stated. | Deriving it from counters: the high watermark is the broker's, and the engine holds no counter for it. |
| Bundle size | A realistic run bundle stays under 1 KiB, tested, because a constrained link pays for it every interval. A 4 KiB ceiling on every field at its widest guards the shape. | A 4 KiB ceiling alone: a bundle could grow fourfold on a metered link and pass CI. The old 1 KiB bound's storage rationale was wrong -- see below -- but the link rationale is right. |
| Histograms | Still out, and for their own reasons. | Folding them in here: distributions are a different problem from dimensions, and mixing them makes one amendment nobody can review. |

## New fields

All ten are in `pipeline`. Presence is decided per group, not per field,
because for these numbers zero is a reading and absence is a different fact.
The rule the review of #354 held every field to: **no field may show its
healthiest value when the pipeline is in trouble.**

- **Lag** is present once lag has been measured. A pipeline that has caught
  up reports `0`; a pipeline with no Kafka source reports nothing. The fields
  are pointers in the Go type for that reason: `omitempty` on a plain integer
  would render both states as an absent field. An instance whose partitions
  all moved elsewhere, or one in a group with more consumers than partitions,
  reports zero partitions and zero lag rather than absence.
- **`lag_observed_at`** dates the lag, and a receiver must read the two
  together. Lag is measured when a message is processed, so a consumer that
  stops receiving -- cut off from its brokers, fenced out of its group, stuck
  in a rebalance -- keeps its last reading, usually zero, while the backlog
  grows.
- **Lag covers the partitions this instance holds or last held.** Three
  events take a partition away, and they are different facts:
  - A **rebalance revocation** means another member holds it. It leaves the
    lag table: a synchronous gauge never forgets an attribute set, so it used
    to stay at its last lag here, and fleet sums counted it on two instances.
  - **Closing** leaves the group, and leaving revokes every partition. That is
    this consumer stopping, not another taking over, so the lag stays for the
    exit bundle. Treated as a rebalance, it emptied the table first, and every
    exit reported lag 0 over 0 partitions.
  - A **loss** is this process's session failing: a broker outage, a fence.
    Who holds the partition is unknown, so its lag stays, frozen at the last
    reading and dated by `lag_observed_at`, until the next assignment says
    whether it came back. Treated as a rebalance, an instance cut off from its
    broker reported the same document as an idle standby.

- **The three window counters travel together, from startup.** The window
  manager records a zero at construction, so a windowed pipeline reports them
  before its first close. `late_rows_dropped` counts data the engine deleted,
  and "no rows were dropped" has to be distinguishable from "nothing here
  drops rows". It counts only after the close that dropped the rows commits;
  counted before, a close that lost a write conflict rolled the delete back,
  kept the count, and counted the same rows again next poll.
- **Each window time is present once measured.** `window_lag_seconds` after a
  window's first poll, including the first after a restart;
  `window_newest_bucket_at` once any window has held a row.
- **`sink_retry_count` is always sent by an engine that has it, and absent
  from one that does not.** A pointer, so an older engine in a mixed fleet
  reads as unknown rather than decoding to zero retries.

| Field | Type | From | Aggregate |
|---|---|---|---|
| `lag_max_messages` | int64 | `consumer_lag` | max over held `topic`+`partition` |
| `lag_total_messages` | int64 | `consumer_lag` | sum over held `topic`+`partition` |
| `lag_partitions` | int | `consumer_lag` | count of held partitions |
| `lag_observed_at` | time | `consumer_lag_observed_timestamp` | when lag was last measured |
| `sink_retry_count` | int64 | `sink_retry_count` | sum over `sink` |
| `late_rows_dropped` | int64 | `window_late_rows{policy=drop}` | sum over `window` |
| `late_rows_reemitted` | int64 | `window_late_rows{policy=reemit}` | sum over `window` |
| `window_closed_count` | int64 | `window_closed` | sum over `window` |
| `window_lag_seconds` | int64 | `window_close_lag_seconds` | max over windows |
| `window_newest_bucket_at` | time | `window_newest_bucket_start_seconds` | latest over windows, as event time |

A policy with no field of its own leaves both late counts absent. Adding it to
neither would report the known counts as complete while some rows went
uncounted.

`lag_partitions` states how many points the other two summarize. Without it,
`lag_max_messages` equal to `lag_total_messages` could mean one partition or
one stuck partition among many.

### The window times

`window_lag_seconds` is how far the most behind window's closes trail the data
it holds, in event seconds. Each poll computes where the watermark should be,
given the rows the window holds; the manager records the candidate minus the
watermark that actually committed as `window_close_lag_seconds`. That is zero
whenever a close commits, zero after an idle close has closed everything, and
grows while rows arrive and closes fail. After a restart the first poll
measures from the stored watermark, and a window that has never closed
measures from the oldest bucket's end, where its first close was due.

No clock enters it. Three readings were tried and rejected, each by a review,
and the reasons are the design:

- **The watermark's age.** It trails the newest bucket by size and grace by
  design, so a healthy hourly window read an hour or two behind while fully
  caught up, and a one-minute window stuck for half an hour hid behind it.
- **The newest bucket's end against the wall clock.** Zero while rows arrive
  -- including while closes have stopped committing, because rows keep
  arriving. The window looked healthy while nothing it held was published.
- **Wall time past the next due close.** Right for a stalled close, wrong for
  a sparse stream: after an idle close it grew for as long as the stream was
  quiet, so a store-and-forward device looked stalled all the time. It was
  only recorded on a commit, so a process restarted into a stall never
  reported one. And it subtracted event time from this host's clock, which on
  a gateway with no real-time clock is the thing most likely to be wrong.

A stream going quiet is the source's to report, as `last_message_at` does. The
window reports whether it keeps up with the data it has.

`window_newest_bucket_at` is the start of the newest bucket any window holds,
as a timestamp from the data. Ahead of a trusted clock means rows are stamped
in the future: one device with a clock a day fast moves the watermark past
every correctly-timed row, and under `late_rows: drop` each is then late and
deleted. The engine does not compute the age itself, because this host's clock
is not the one to trust on the fleets that need this most. A receiver compares
it with its own clock at receipt. The gauge is recorded on every poll that
finds rows, even one whose close fails, and it keeps its last value after an
idle close empties the table, so a future bucket that closed stays visible.

`lag_*` and `window_lag_seconds` are levels, not counters. The high
watermark and the window's own data belong to something other than a counter
this process keeps, so no counter derives them. Every other number in the bundle stays a counter the receiver
subtracts.

## The document

```jsonc
{
  "pipeline": {
    "message_count": 31966,
    "handler_rows_read": 31891,
    // ... existing fields unchanged ...

    // Absent without a Kafka source. Read with lag_observed_at.
    "lag_max_messages": 412,
    "lag_total_messages": 1902,
    "lag_partitions": 6,
    "lag_observed_at": "2026-09-21T09:00:00Z",

    // Sent by every engine that has it, zero included.
    "sink_retry_count": 3,

    // Present from startup with any window, absent with none.
    "late_rows_dropped": 0,
    "late_rows_reemitted": 14,
    "window_closed_count": 288,

    // Present once measured. The newest bucket is event time.
    "window_lag_seconds": 0,
    "window_newest_bucket_at": "2026-09-21T09:00:00Z"
  }
}
```

## Size, and what actually costs

The old bound was 1 KiB. It was chosen to protect the control plane's
storage. Measured against the running control plane, it does not:

| Measure | Bytes |
|---|---|
| `serve` bundle, raw JSON on the wire | 474 |
| `run` bundle, websocket source, no windows | 721 |
| Average raw JSON across 3,576 stored bundles | 485 |
| Average **stored** size of the same `doc` column | **588** |
| `run` bundle with these fields: 32 partitions, windows, a retrying sink | 921 |
| Every field of both sections at its widest value | 1,853 |

The last two are measured by tests, not estimated. The 32-partition bundle is
no wider than a one-partition bundle, which is the shape invariant doing its
job.

Storage is 21% *larger* than the wire form, not smaller. `doc` is `jsonb` with
EXTENDED storage, and Postgres compresses a value only above roughly 2 KB, so
every bundle is stored uncompressed in a binary form that repeats its keys. A
bundle small enough to satisfy the old bound is a bundle guaranteed not to be
compressed.

The real cost is rows times retention, not bytes per row. All-in with indexes
the table holds about 888 B per heartbeat, so at a 10s interval one instance
costs 7.7 MB a day, and 1,000 instances cost 7.7 GB a day. Adding ten fields
moves that by roughly a quarter. Retention moves it by orders of magnitude,
and retention is already configurable.

Three consequences, none of which belong in this contract:

- The control plane should promote the new fields to columns, as it already
  does for `rss_bytes` and `goroutines`. Queries then never parse `doc`.
- `doc` is kept for forensics. It should have its own short retention,
  separate from the extracted columns.
- If `doc` is worth compressing, the lever is the column, not the contract:
  `SET STORAGE MAIN` with a lowered TOAST threshold, or `bytea` holding
  compressed text. A request `Content-Encoding` compresses the wire and
  changes nothing about the row.

There is one reader for whom bytes per heartbeat is the real cost, and it is
not the control plane: an instance on a metered or constrained link. At 921
bytes the default 60s interval costs about 1.3 MB a day and a 10s interval
about 8.0 MB, before HTTP and TLS overhead of the same order. That is the
strongest argument for the shape invariant below, stronger than storage ever
was: a map keyed by partition would have put the broker's partition count on
that link every interval. It also makes compressing the *request* worthwhile
where compressing the row was not, since a JSON document repeats its keys.
The interval remains the lever that matters most.

So the realistic run bundle is held under 1 KiB by a test, and growing past
it is a decision rather than a drift found on a bill. A 4 KiB ceiling on every
field at its widest guards the shape. The
invariant that matters is not a byte count but a shape: **no field's presence
or repetition depends on data cardinality.** A reviewer can check that by
reading the struct, which a byte count cannot.

## Bytes read

`message_payload_bytes` is the bytes of every message value received, added
for instances on constrained links, where volume is the cost. It is not
dimensional, so it sits outside this amendment's rule, but it ships into the
same v1.

| Field | Type | From |
|---|---|---|
| `message_payload_bytes` | int64 | `message_payload_bytes`, flat |

It counts the same messages `message_count` counts, at the same point --
received, before `--max-msgs` or a rejected message can drop one -- so the two
divide to a true average message size. It is payload and named so: keys,
headers, framing and TLS are excluded, and under Kafka compression the wire
carries fewer bytes. Wire bytes are what a metered link pays, only some
clients expose them, and they would be a different field.

Its cost on the consume loop is one length read and one add per message,
measured in isolation at 0.38 ns and no allocation. The end-to-end write-path
benchmark cannot resolve that: its run-to-run noise on a laptop is several
percent of a 160 ns message. It is a pointer in the Go type, so an engine that
predates it reads as unknown rather than as a pipeline that received nothing.

## Testing

- A unit test per aggregate, built from synthetic `metricdata` with several
  attribute sets, asserting the collapsed value and that an outcome attribute
  never collapses.
- A test that the bundle struct has no map, slice, or repeated field whose
  length depends on topic, partition, window, or sink count. This is the
  invariant; the byte ceiling is the smoke alarm.
- A test that a realistic run bundle stays under 1 KiB, and one that every
  field at its widest stays under 4 KiB.
- A test that a pipeline with no Kafka source omits all three `lag_*` fields,
  because absent lag and zero lag are different facts.
- A mutation check on the rule: change `max` to `sum` in the lag aggregate and
  confirm a test fails. An aggregate nothing pins is an aggregate that drifts.
- A test per presence rule, and one per trouble case that used to read
  healthy: a future-dated row, a stalled small window beside a healthy large
  one, a close that stops while rows keep arriving, a partition that moves
  away, and a rolled-back close.
- A reproduction of the rebalance against a real broker: two consumers in one
  group, and the first must stop reporting what the second took.
- Reproductions against a real broker: a rebalance, where the first consumer
  must stop reporting what the second took; a close, which must keep the lag;
  and an outage, made by pausing the broker past a short session timeout,
  which must keep it too. The close and the outage both failed before the fix.
- A release test that sends SIGTERM to a Kafka pipeline and reads its exit
  bundle. Against the image before the fix it reported 0 partitions after
  consuming 200 messages.
- A mutation per rule. Twenty-three, and twenty-two fail the test written for
  them. The survivor is equivalent: it removed a guard that `unixTime` already
  enforced, so the guard went instead.
- The memory soak, because this touches the collect path of a process that
  runs for weeks. It ran, candidate beside baseline, and both plateau in the
  same band. A soak sees about a thousand collects and cannot resolve a slow
  per-collect leak against its own noise, so a volume test runs fifty
  thousand collects in CI and reads the live heap, which counts a leaked Go
  object exactly.

`Collect` must not grow a second pass over the points. It walks every data
point once in `walk`; the aggregates accumulate in that walk.

## What breaks if this is wrong

- A collapsed outcome attribute reports a number true of nothing, which is the
  defect the flat-scalar rule existed to prevent. The rule above is narrower
  than the old one only for shard attributes, and the tests pin which is which.
- A wrong aggregate reads as healthy. `sum` where `max` belongs turns one
  stuck partition into a number that looks like normal backlog.
- Omitting a field where zero belongs, or the reverse, makes a pipeline with
  no windows indistinguishable from one whose windows never close.
- A field that reads healthy while the pipeline is in trouble is the worst
  failure this contract can have, because it is the one nobody investigates.
  Every trouble case above has a test that fails if its field reads healthy.
- A map sneaking in later makes bundle width a function of the broker's
  partition count, which is the failure the shape invariant exists to catch.

## Out of scope

- **Histograms and latency.** Six histograms exist and none reach the bundle.
  The correct shape is bucket counts, not a mean: buckets let the receiver
  compute any quantile and merge across instances, and a mean hides the tail
  it would be added to find. That is a distribution problem rather than a
  dimensional one, and it deserves its own amendment.
- Per-partition, per-window, or per-sink detail. `/metrics` carries it.
- **Lag measured on a timer.** `lag_observed_at` makes a stale reading
  visible; it cannot say the backlog behind it is growing. From inside the
  consume loop an idle topic and a lost broker look the same -- the source
  blocks in `PollFetches` until records arrive -- so both age the reading
  alike. Measuring lag while nothing arrives means asking the broker for each
  held partition's high watermark on a timer, a round trip this change does
  not add. Until then a receiver should show old lag as old, not as current.
- **Bytes written.** The sink interface takes an Arrow table, so the core
  never sees bytes, and each sink encodes differently -- Kafka JSON, a
  Postgres COPY stream, ClickHouse's own format. It needs an optional sink
  interface and a definition of "bytes" per sink before any code.
- **A freshness probe.** Every number in the bundle is the engine attesting
  about itself. None of them can say the output arrived: a sink that
  acknowledges and loses, or a destination nobody can read, looks healthy from
  in here. Proving freshness means reading the destination, which is a
  different component with its own credentials and its own failure modes.
- Error detail beyond `error_count`. An operator still goes to the logs to
  learn what failed. `exit.reason` shows a taxonomy code is possible in a
  bundle, so this is worth its own amendment rather than a field bolted here.
- Any command verb or command status.
