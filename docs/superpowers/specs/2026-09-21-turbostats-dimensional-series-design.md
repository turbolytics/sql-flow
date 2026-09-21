# TurboStats v1 amendment: dimensional series

This spec amends `2026-09-19-turbostats-contract-amendment-design.md`. It adds
eight fields to the `pipeline` section, all of them summaries of series the
engine already records under attributes. It changes no existing field, so it
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
| `window_watermark_seconds` | `window` | `managers/watermark.go:322` |
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

All eight are in `pipeline`. Presence is decided per group, not per field,
because for these numbers zero is a reading and absence is a different fact:

- **Lag** is present when the pipeline has a Kafka source. A pipeline that has
  caught up reports `0`; a pipeline with no Kafka source reports nothing. The
  three fields are pointers in the Go type for that reason: `omitempty` on a
  plain integer would render both states as an absent field, and the
  healthiest state a pipeline has would read the same as the unknown one.
- **The window fields travel together.** All four are present when the
  pipeline runs any window, and all four are absent when it runs none.
  `late_rows_dropped` counts data the engine deleted, so "no rows were
  dropped" has to be distinguishable from "nothing here drops rows".
- **`sink_retry_count` is always present.** A pipeline always has a sink, so
  no retries is a reading rather than a silence.

| Field | Type | From | Aggregate |
|---|---|---|---|
| `lag_max_messages` | int64 | `consumer_lag` | max over `topic`+`partition` |
| `lag_total_messages` | int64 | `consumer_lag` | sum over `topic`+`partition` |
| `lag_partitions` | int | `consumer_lag` | count of points |
| `sink_retry_count` | int64 | `sink_retry_count` | sum over `sink` |
| `late_rows_dropped` | int64 | `window_late_rows{policy=drop}` | sum over `window` |
| `late_rows_reemitted` | int64 | `window_late_rows{policy=reemit}` | sum over `window` |
| `window_closed_count` | int64 | `window_closed` | sum over `window` |
| `watermark_lag_seconds` | int64 | `window_watermark_seconds` | max of `sent_at − watermark` |

`lag_partitions` states how many points the other two summarize. Without it,
`lag_max_messages` equal to `lag_total_messages` could mean one partition or
one stuck partition among many, which are different operational facts.

`watermark_lag_seconds` is a derived age, not the raw watermark. The raw value
is a Unix second that means nothing without the read time, and both clocks are
the instance's, so the subtraction carries no skew.

`lag_*` are levels, not counters. This is the one place the bundle reports a
gauge that no counter derives, because the high watermark belongs to the
broker. Every other number in the bundle stays a counter the receiver
subtracts.

## The document

```jsonc
{
  "pipeline": {
    "message_count": 31966,
    "handler_rows_read": 31891,
    // ... existing fields unchanged ...

    // Absent without a Kafka source.
    "lag_max_messages": 412,
    "lag_total_messages": 1902,
    "lag_partitions": 6,

    // Always present: every pipeline has a sink.
    "sink_retry_count": 3,

    // All four present with any window, all four absent with none.
    "late_rows_dropped": 0,
    "late_rows_reemitted": 14,
    "window_closed_count": 288,
    "watermark_lag_seconds": 65
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
| `run` bundle with these fields: 32 partitions, windows, a retrying sink | 807 |
| Every field of both sections at its widest value | 1,715 |

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
costs 7.7 MB a day, and 1,000 instances cost 7.7 GB a day. Adding eight fields
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
not the control plane: an instance on a metered or constrained link. At 807
bytes the default 60s interval costs about 1.1 MB a day and a 10s interval
about 7 MB, before HTTP and TLS overhead of the same order. That is the
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
- The memory soak, because this touches the collect path of a process that
  runs for weeks.

`Collect` must not grow a second pass over the points. It already walks every
data point once in `scalars`; the aggregates accumulate in that walk.

## What breaks if this is wrong

- A collapsed outcome attribute reports a number true of nothing, which is the
  defect the flat-scalar rule existed to prevent. The rule above is narrower
  than the old one only for shard attributes, and the tests pin which is which.
- A wrong aggregate reads as healthy. `sum` where `max` belongs turns one
  stuck partition into a number that looks like normal backlog.
- Omitting a field where zero belongs, or the reverse, makes a pipeline with
  no windows indistinguishable from one whose windows never close.
- A map sneaking in later makes bundle width a function of the broker's
  partition count, which is the failure the shape invariant exists to catch.

## Out of scope

- **Histograms and latency.** Six histograms exist and none reach the bundle.
  The correct shape is bucket counts, not a mean: buckets let the receiver
  compute any quantile and merge across instances, and a mean hides the tail
  it would be added to find. That is a distribution problem rather than a
  dimensional one, and it deserves its own amendment.
- Per-partition, per-window, or per-sink detail. `/metrics` carries it.
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
