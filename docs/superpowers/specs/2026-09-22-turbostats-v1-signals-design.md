# TurboStats v1: the signals a receiver needs

The bundle says how much work a pipeline did and whether it is still doing
any. It does not say how long the work takes, what kind of failure it is,
what the pipeline is made of, or how far behind the stream it runs. This
amendment adds those four, and says plainly what it leaves out.

Every field here is additive. The document stays `v1`.

## The rule this amendment is built on

**A process's field set is fixed from its first report to its last.**

Nothing is keyed by what the process discovers at runtime: no per-topic,
per-partition, per-dataset or per-error-code maps. Which sections a process
sends, `pipeline` under `run` and `serve` under `serve`, is decided at
startup and never changes. An operator's labels come from the config and
nothing writes them while the process runs.

A receiver stores a series per field. A field set that grows with the data
turns one instance into an unbounded number of series, and every chart,
comparison and retention policy built on it breaks quietly. So the contract
refuses that shape, and a breakdown that needs runtime keys waits for a
design that bounds its cardinality.

**v1 is the coarsest grain that is useful.** Every field below earns its
place by answering a question an operator asks about a fleet. Finer grain
follows a customer asking for it, not a guess.

## What the four signals need

An operator asks five questions about a running pipeline. Four of them the
bundle already answers, and one it does not.

| Question | Today | This amendment |
| --- | --- | --- |
| Is it doing anything? | `last_message_at`, `last_request_at`, `idle_seconds` | unchanged |
| How much work? | `message_count`, `message_payload_bytes`, `handler_rows_read`, `sink_rows_written`, `request_count` | unchanged |
| How long does the work take? | nothing | `duration` per phase |
| Did it succeed? | one `error_count` | per-phase counters, the DLQ, the last error's code |
| What is queued or waiting? | Kafka offset lag, `sessions_in_use` | `recv_wait_seconds` |
| How far behind the stream? | offset lag, in messages, Kafka only | `event_lag_seconds`, in time, any source with an event time |

## Durations

The engine keeps the latency histograms already. Three of them reach the
bundle:

- `pipeline.duration.batch`: one batch, from receive to processed.
- `pipeline.duration.sink_flush`: one flush, where a slow destination shows.
- `serve.duration.request`: one request, end to end.

Source read, state commit, query time and session wait stay in the Prometheus
endpoint, for whoever is diagnosing one instance. They reach the bundle when
a customer asks for them.

Each phase carries the same five fields:

```json
"duration": {
  "batch": {
    "count": 41203,
    "sum_seconds": 831.4,
    "min_seconds": 0.0008,
    "max_seconds": 41.2,
    "buckets": [120, 30188, 9400, 1300, 170, 20, 4, 1, 0]
  }
}
```

- `count`, `sum_seconds` and every entry of `buckets` are counters since the
  process started. A receiver subtracts two reports for the interval's
  distribution, reads any percentile from it, and sums buckets across a
  fleet.
- `min_seconds` and `max_seconds` are since the process started, and never
  reset. Two readers exist, the reporter and `GET /turbostats/v1`, and a
  value reset on read would hide events from whichever reader did not see
  it. Per-interval movement comes from the buckets: the last bucket rising
  says the tail moved.
- `buckets` is nine counts against eight fixed boundaries, in seconds:
  `0.001, 0.01, 0.05, 0.25, 1, 5, 30, 60`, and everything above 60 in the
  ninth. The boundaries are in this contract, not in the bundle, so nine
  numbers carry them. The engine's own histograms keep their 16 boundaries.
- Changing a boundary is a contract change, not a config option. Buckets
  that differ per instance cannot be summed across a fleet.

**Why min and max as well as buckets.** Nobody knows a customer's workload.
One whose flushes all take 90 seconds puts every sample in the overflow
bucket, and the distribution says nothing at all. Count, sum, min and max
stay true whatever the workload, and they are four numbers.

### The successor: an exponential histogram

Fixed boundaries cannot be re-scaled after the fact, and a percentile across
a fleet needs a form that merges. The successor is the base-2 exponential
histogram, which the OTel SDK already implements
(`AggregationBase2ExponentialHistogram`), so it needs no new dependency:

```json
"batch": {
  "count": 41203, "sum_seconds": 831.4,
  "min_seconds": 0.0008, "max_seconds": 41.2,
  "hist": { "scale": 3, "zero_count": 0, "offset": -84, "counts": [2, 19, 240, ...] }
}
```

`scale` sets the relative error, `offset` is the index of the first count,
and `counts` runs from there. It is one field with a fixed shape: an array,
never a map keyed by bucket, so the shape rule holds. Two sketches at one
scale merge exactly, and a sketch at a finer scale reduces to a coarser one,
which is what makes a fleet-wide percentile possible.

It is not in v1 because the cost lands in the receiver: storage, merging and
quantile math before anyone sees a number, for a question no customer has
asked. It also multiplies the report's size by two to four, on links where
that is the constraint.

When it arrives, `buckets` is derived from `hist` rather than measured
separately, so a receiver keeps one path and nothing has to read both.

## Errors

The `pipeline` section gains counters, one per phase the engine already
distinguishes, and the last error:

- `source_error_count`, `handler_error_count`, `sink_error_count`,
  `state_error_count`.
- `dlq_rows`: rows diverted to the dead-letter queue rather than dropped.
- `last_error_code`: the engine's code, such as `system.sink.unreachable`.
- `last_error_at`.

`error_count` stays, as the total, so a receiver that reads only it keeps
working.

**No error message, ever.** A message carries the row that failed, a
connection string, a customer's data. The code is the taxonomy's, bounded
and safe to store, and an operator with the code and the timestamp can find
the message in their own logs.

**No count per code.** That map grows as new codes occur, which the rule
above refuses.

`serve` keeps its single `request_error_count`, which counts 5xx only: a 4xx
is the caller's failure, and counting it would let one bad client paint a
server unhealthy.

## Queue state

- **Kafka**, unchanged: `lag_max_messages`, `lag_total_messages`,
  `lag_partitions`, `lag_observed_at`. No per-partition detail in v1.
- **Any source:** `recv_wait_seconds`, a counter of the time the consume
  loop spent waiting for input. The engine times this and the bundle drops
  it. It separates two states that look identical from outside: a pipeline
  waiting on a quiet source, and one saturated by its own work. Wait time
  near the wall clock means the source is quiet; near zero means the engine
  is the bottleneck.
- **Serve**, unchanged: `sessions_in_use`, `sessions_total`.
- **Sinks:** nothing. A buffered-row count needs a new reading from every
  sink, and a sink falling behind already shows as flush duration and rising
  retries.

## Lag, in time

Four fields in `pipeline`, measured once per batch:

- `event_lag_seconds`: at each batch, now minus the event time of the newest
  event in it. A pipeline that is behind is handling old events, so its
  newest event is old, and this is how far behind the stream it runs.
- `event_lag_observed_at`: when that reading was taken. A consumer cut off
  from its brokers keeps its last reading, and the age of the reading is
  what catches that.
- `event_lag_max_seconds`: the worst reading since the process started.
- `event_lag_basis`: where the event time came from, and it travels with the
  number so nobody compares two of them that mean different things.
  - `kafka_timestamp`: the record's timestamp. Broker to processing.
  - `arrival`: the webhook and websocket sources stamp each message as it
    arrives. Queueing inside the process, not transport.

A source with no event time sends none of the four.

**Why the newest event and not the oldest.** The oldest event in a batch
adds the batch's own span to the number, which says as much about
`batch_size` as about the stream. The newest is the boundary the pipeline
has reached.

**Why per batch.** One timestamp read per batch costs nothing. Per event, it
is a read and a compare per message, on hardware where that budget is the
product.

## Metadata

The `instance` section gains what the pipeline is, read from the config at
startup:

- `source_type`: `kafka`, `webhook`, `websocket`.
- `sink_type`: `postgres`, `kafka`, `clickhouse`, `iceberg`, `parquet`,
  `console`, `sqlcommand`, `noop`.
- `handler_type`: `structured`, `inferred_mem`, `inferred_disk`.

This is what makes "every stream that reads from Kafka" a query rather than
a grep. The DLQ's sink type, whether the pipeline windows, and whether it
keeps state stay out: each is one more field and none has been asked for.

And what the operator says about it:

```yaml
pipeline:
  turbostats:
    id: gateway-0142
    labels:
      region: eu-west
      tenant: acme
      env: prod
```

- At most 10 labels. A key is at most 32 characters and matches
  `[a-z][a-z0-9_]*`, so a receiver can use it as a column or a filter name
  without escaping. A value is at most 64 characters.
- These names are refused, because a label that shadows a contract field
  makes two different things share a name: `id`, `name`, `version`,
  `commit`, `arch`, `config_hash`, `source_type`, `sink_type`,
  `handler_type`.
- `sqlflow validate` enforces every rule, so a bad label set fails before
  the pipeline runs rather than silently once a minute.
- Labels are fixed for the process's life. Changing one is a config change,
  which is a new `config_hash` and, in practice, a restart.

A fleet whose instances carry different label sets is fine: the rule is per
process, not per fleet.

## What this leaves out, and why

- **Freshness.** The age of the data a pipeline has landed, measured where a
  reader sees it: the last successful sink write, and the event time of the
  newest row written. The engine reports neither, and neither follows from a
  counter it keeps. Its own amendment.
- **Completeness.** A ledger per boundary: rows in, rows late, rows out,
  never across the SQL. Not derivable from what exists.
- **SLOs, and the word degraded.** Both need an expectation the instance
  declares. Nothing declares one, so a receiver says idle, which is what it
  can see. Labels and a declared rate would be the start of it.
- **Per-partition, per-dataset, per-code breakdowns.** Refused by the shape
  rule until a design bounds their cardinality.

## Size, and compatibility

The bundle grows from about 700 bytes to about 1.5 KB. The receiver's limit
is 16 KiB, and a reporter sends once a minute by default.

Every field is additive and the document stays `v1`:

- A receiver that does not know a field ignores it, which the contract
  already requires.
- Absent stays absent rather than zero, which the contract already requires:
  a pipeline with no event time sends no lag, and that is different from a
  lag of zero.
- Control reads each new field when it is there and works without it, the
  way it already does for `uptime_seconds` and `idle_seconds`.

## Invariants this adds

- A process's field set does not change between its first report and its
  last.
- `count`, `sum_seconds` and every bucket count only rise within a process.
- `min_seconds` is at most `max_seconds`, and every duration is at least
  zero.
- The sum of a phase's buckets equals its `count`.
- `error_count` is at least the sum of the per-phase error counters. The
  total is the older field and stays authoritative for the total.
- `event_lag_seconds` is present only with `event_lag_basis`, and both are
  absent for a source with no event time.
- A label key never collides with a field this contract defines.
