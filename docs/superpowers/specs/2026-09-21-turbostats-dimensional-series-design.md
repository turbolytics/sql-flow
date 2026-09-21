# TurboStats v1 amendment: dimensional series

This spec amends `2026-09-19-turbostats-contract-amendment-design.md`. It adds
ten fields to the `pipeline` section, summaries of series the engine records
under attributes, and two window gauges the bundle needs to read them. It changes no existing field, so it
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
| Bundle size | A 4 KiB ceiling, as a smoke alarm against accidental bloat. | The old 1 KiB bound. It was believed to protect storage. It does not — see below. A bound that shapes the contract should be measured first. |
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
- **Lag covers only the partitions this instance holds.** The source reports
  each rebalance, and a partition that moves away leaves the lag table. A
  synchronous gauge never forgets an attribute set, so the partition used to
  stay at the lag it had when it left, and a fleet summing lag counted it on
  two instances.
- **The three window counters travel together, from startup.** The window
  manager records a zero at construction, so a windowed pipeline reports them
  before its first close. `late_rows_dropped` counts data the engine deleted,
  and "no rows were dropped" has to be distinguishable from "nothing here
  drops rows". It counts only after the close that dropped the rows commits;
  counted before, a close that lost a write conflict rolled the delete back,
  kept the count, and counted the same rows again next poll.
- **The two window times travel together.** Both are present once any window
  has both closed and held rows.
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
| `window_lag_seconds` | int64 | `window_close_due_seconds` | max over windows of `sent_at − due`, floored at 0 |
| `window_ahead_seconds` | int64 | `window_newest_bucket_start_seconds` | max over windows of `newest − sent_at`, floored at 0 |

A policy with no field of its own leaves both late counts absent. Adding it to
neither would report the known counts as complete while some rows went
uncounted.

`lag_partitions` states how many points the other two summarize. Without it,
`lag_max_messages` equal to `lag_total_messages` could mean one partition or
one stuck partition among many.

### The window times

`window_lag_seconds` is how overdue the most overdue window's next close is.
The manager records `window_close_due_seconds` as `watermark + size + grace`
whenever a close commits: the bucket after the watermark closes once rows
arrive a grace period past its end, so at the wall clock's pace that is when
the next close is due. The bundle subtracts it from `sent_at`.

Two readings were tried and rejected, and the reasons are the design:

- **The watermark's age**, as the first version of this spec had it. A
  watermark trails the newest bucket by size and grace by design, so a healthy
  hourly window read 3,600 to 7,200 seconds behind while fully caught up, and
  a one-minute window stuck for half an hour hid behind it.
- **The newest bucket's end.** Zero while rows arrive, whatever the size --
  but also zero while closes have stopped committing, because rows keep
  arriving. The window looked healthy while nothing it held was published.

Measured from the close that is due, the lag is zero while closes keep up and
grows when either the stream or the close stops.

`window_ahead_seconds` is how far the newest bucket starts beyond now. It is
recorded on every poll that finds rows, even one whose close fails, and it is
not clamped as a clock artefact. A watermark is event time from the data: one
device with a clock a day fast moves it a day ahead, and every correctly-timed
row after that is late and, under `late_rows: drop`, deleted.

`lag_*` and the window times are levels, not counters. The high watermark and
the wall clock belong to something other than this process, so no counter
derives them. Every other number in the bundle stays a counter the receiver
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

    // Present together once a window has closed and held rows.
    "window_lag_seconds": 0,
    "window_ahead_seconds": 0
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
| `run` bundle with these fields: 32 partitions, windows, a retrying sink | 868 |
| Every field of both sections at its widest value | 1,803 |

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
not the control plane: an instance on a metered or constrained link. At 868
bytes the default 60s interval costs about 1.2 MB a day and a 10s interval
about 7.5 MB, before HTTP and TLS overhead of the same order. That is the
strongest argument for the shape invariant below, stronger than storage ever
was: a map keyed by partition would have put the broker's partition count on
that link every interval. It also makes compressing the *request* worthwhile
where compressing the row was not, since a JSON document repeats its keys.
The interval remains the lever that matters most.

So the contract keeps a ceiling only to catch accidents. 4 KiB, tested. The
invariant that matters is not a byte count but a shape: **no field's presence
or repetition depends on data cardinality.** A reviewer can check that by
reading the struct, which a byte count cannot.

## Testing

- A unit test per aggregate, built from synthetic `metricdata` with several
  attribute sets, asserting the collapsed value and that an outcome attribute
  never collapses.
- A test that the bundle struct has no map, slice, or repeated field whose
  length depends on topic, partition, window, or sink count. This is the
  invariant; the byte ceiling is the smoke alarm.
- A test that a full bundle with every new field stays under 4 KiB.
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
- A mutation per rule. Seventeen mutations, each failing the test written for
  it, including the three the review found surviving: the clamp on a
  future-dated watermark, late rows counting as a window, and the guard that
  keeps an unset reading from subtracting the epoch.
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
- **Byte volume.** `bytes_read` and `bytes_written` will be added, most of all
  for an instance on a constrained link, and they are not here because nothing
  measures them: unlike every field above, this is new instrumentation on the
  consume loop rather than a series the bundle dropped, so it needs the
  write-path benchmark as a gate. The definition has to be settled first.
  Payload bytes are uniform across sources and nearly free, but exclude
  framing and TLS, and under Kafka compression the wire can be smaller than
  the payload. Wire bytes are what a metered link pays, and only some clients
  expose them. Whichever ships must be named for what it is.
- **A freshness probe.** Every number in the bundle is the engine attesting
  about itself. None of them can say the output arrived: a sink that
  acknowledges and loses, or a destination nobody can read, looks healthy from
  in here. Proving freshness means reading the destination, which is a
  different component with its own credentials and its own failure modes.
- Error detail beyond `error_count`. An operator still goes to the logs to
  learn what failed. `exit.reason` shows a taxonomy code is possible in a
  bundle, so this is worth its own amendment rather than a field bolted here.
- Any command verb or command status.
