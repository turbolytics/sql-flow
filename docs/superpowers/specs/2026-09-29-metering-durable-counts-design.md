# Durable counts: a 200 is in Kafka, and a window survives its worker

**Status:** design, awaiting review.
**Branch:** `metering/durable-counts`, one PR.
**Builds on:** #399 (the close is driven by the watermark, lateness decided at
arrival), the offset marks (`core.Marks`, `Source.CommitMarks`,
`Source.SeekTo`), and the partition relay (`kafka.PartitionEvents`).
**For:** usage metering (turbolytics.io issue #20, the `usage-metering`
reference repo). A billing count that is silently low or silently doubled is
the one failure a metering customer cannot accept.

## The stack this has to hold for

```
app ─SDK─▶ ingest (webhook → Kafka, keyed by customer)
                       │
            Kafka topic usage.events, N partitions
                       │
     count × W workers, one consumer group
     window: 1 minute, sum(quantity) over distinct md5_number(id)
             per (minute, customer, meter, partition)
                       │
     Postgres minute table, upsert keyed (minute, customer, meter, kafka_partition)
```

Every guarantee below is stated against this stack and tested against it.

## The problem

Four places on `main` where an acknowledged or consumed event can be lost or
counted twice. Each was found by reading the code; none has a test that would
catch it.

1. **The webhook answers 200 before the event is anywhere durable.**
   `receiveEvents` puts the body on a channel with a buffer of one and answers
   `{"status":"received"}` as soon as the send succeeds
   (`internal/webhook/source.go:168, 364-366`). The turbine receives it later
   and flushes it later still (`internal/core/turbine.go:1055`,
   `processBatch`). This holds at `batch_size: 1`: the batch size only decides
   when the *next* request waits. A crash between the 200 and the flush loses
   an event the sender was told it had delivered, and the sender will not
   retry it.

2. **The Kafka sink writes records with no key.** `KafkaSink.Flush` produces
   `&kgo.Record{Topic, Value}` (`internal/sinks/kafka.go:148`), and
   `config.KafkaSink` has no key field. A retried event lands on whatever
   partition the partitioner picks. Per-partition dedupe in a downstream
   window then misses the duplicate whenever the retry lands elsewhere. The
   metering stack keys by customer: a retry has the same customer, so it
   lands on its original's partition, and each customer writes one row per
   meter per minute rather than one per partition.

3. **The committed offset passes rows that live only in an open window.**
   `commitSource` commits the positions the pipeline has processed
   (`internal/core/turbine.go:1564-1579`): the rows are in the window table,
   not emitted. With a state path, a restart on the same disk is correct,
   because the window rows and the marks commit together and `SeekTo` resumes
   from the marks. A worker whose disk is gone, or a new owner after a
   rebalance, starts from the group's committed offset instead. It never sees
   the rows that were only in the lost window, and the minute undercounts with
   no error and no lag.

4. **A revoked partition's open window can still emit.** After a rebalance the
   old owner holds rows for a partition it no longer owns. If it emits them,
   its partial count and the new owner's count race for the same key, and the
   last write wins.

## Decisions

### 1. `ack: after_flush` on the webhook source

```yaml
source:
  type: webhook
  webhook:
    ack: after_flush     # default: on_receive, which is today's behavior
```

- The handler sends its message with a completion channel and waits on it.
  The turbine fires the completion for every message in a batch after the
  sink flush for that batch returns nil. A failed flush fires it with the
  error, and the handler answers 503 so the sender retries.
- The wait is bounded by the request context. A sender that hangs up gets
  nothing, and the event may or may not be in Kafka. That is the normal
  at-least-once case, and the event id makes it safe downstream.
- `validate` refuses `after_flush` on a pipeline that windows. A window can
  hold a row for minutes, and a request cannot wait that long. The ingest
  pipeline does not window.
- `on_receive` stays the default. Every config that exists today behaves as
  before.

### 2. `key` on the Kafka sink

```yaml
sink:
  type: kafka
  kafka:
    topic: usage.events
    key: customer      # a column of the handler's output
```

- The record key is the column's value, as UTF-8 bytes of its text form. The
  row is still written whole, key column included.
- A row whose key is null fails with a user-class error under the pipeline's
  existing error policy. An unkeyed event would defeat the reason for the key.
- `validate` checks the column exists in the handler's declared output where
  it can.

### 3. Commit the low watermark of every partition

The committed offset for a partition becomes the lowest offset that still
feeds a bucket the window has not finished with: open, or closed but inside
`allowed_lateness_seconds`. Kafka becomes the durable copy of window state. A
worker that starts without the rows replays from there and rebuilds each
bucket whole.

- The engine records, per window table and bucket, the lowest offset per
  (topic, partition) of the batches that wrote rows into it, in a state table
  beside the watermarks, in the same transaction as the rows. When the output
  carries `kafka_partition`, the attribution is per partition. When it does
  not, every partition in the batch is attributed to every bucket the batch
  touched. That replays more than it needs to, never less.
- A bucket that leaves retention drops its record. The commit for a partition
  is the minimum over the buckets still retained, or the processed position
  when none remain.
- Local state still wins on the same disk. At start, `SeekTo` uses the marks
  in the state database for the partitions it has rows for. The group's
  committed offset applies only to a partition with no local state. Replaying
  over rows already held would count them twice.
- Replay re-emits buckets that already closed. That is safe because the emit
  is a whole value written by key (#399 already requires a replacing sink
  under `allowed_lateness_seconds`).
- A pipeline with no window commits as today.

Cost: after a lost disk or a rebalance, the new owner re-reads at most window
+ grace + allowed lateness of each partition it picks up. For the metering
stack's 60 + 60 + 60 seconds, that is three minutes of events. At the sizing
scenario's 50,000 events a second over 6 partitions, that is about 1.5
million events per partition picked up.

### 4. A revoked partition's rows are dropped, not emitted

```yaml
tables:
  sql:
    - name: calls_per_minute
      window:
        partition_owned: true
```

- `partition_owned: true` declares that every row of this window belongs to
  one Kafka partition. `validate` requires `kafka_partition` in the handler's
  output and in the sink's key, and a Kafka source.
- On `released` or `lost` from the partition relay, the engine deletes that
  partition's rows and offset records from the window table and the state,
  without emitting, in one transaction. The new owner's full recount is then
  the only write for that key.
- Without `partition_owned`, a window keeps today's behavior. A window whose
  rows mix partitions cannot be split by partition, and turning this on
  implicitly would change what existing configs emit.

## Tests, written first

Each scenario runs real Kafka and real Postgres, starting from the existing
harness: `internal/kafka/broker_test.go`, the partition-event tests in
`internal/kafka/partitions_test.go`, and `startPostgres` in the sink
conformance tests. Each feeds a fixed event set whose correct answer is known,
and asserts the exact count per (minute, customer, meter) in Postgres, summed
over partitions. A count that is off by one fails.

Topology: one topic with 6 partitions, 3 count workers in one group, the
window above with `partition_owned: true`, and the Postgres sink keyed by
(minute, customer, meter, kafka_partition).

| # | Scenario | Expected on `main` |
|---|----------|--------------------|
| 1 | Steady state, all workers up | passes |
| 2 | A worker joins mid-minute, forcing a rebalance | fails: undercount or overwrite |
| 3 | A worker leaves mid-minute | fails: undercount or overwrite |
| 4 | `kill -9` a worker, state volume kept | passes |
| 5 | `kill -9` a worker, state volume lost | fails: undercount |
| 6 | Duplicate ids, retried through the webhook and keyed sink | fails: a retry on another partition doubles |
| 7 | Late events within `allowed_lateness_seconds` | passes |
| 8 | Webhook `after_flush`: kill between receive and flush | fails: an event answered 200 is missing from Kafka |
| 9 | Webhook `after_flush`: a failed flush answers 503 | new |
| 10 | Rebalance under load, one hot partition at a high rate: exact totals, and the replay time recorded | fails with 2 on `main`; the time is new |
| 11 | A late event refused, then the worker's disk lost with a later minute open: the fresh worker replays past it | fails on `main`: the open minute undercounts; fails with decision 3 alone: the late event's minute is overwritten; passes with decision 5 |

The "expected on `main`" column is a prediction. The first commit after this
spec adds the tests, and the PR records what each actually did before the
fixes. A prediction that turns out wrong is reported, not quietly adjusted.

Unit tests beside the integration ones: the offset-record arithmetic (min over
retained buckets, attribution with and without `kafka_partition`), the
completion fan-out on flush success and failure, the key encoding, and every
new `validate` rule with a config that should fail.

## Scope

In:

- The four decisions above, with their config, validation, docs and schema
  entries.
- The eleven scenarios above, and a coverage matrix entry for each new
  invariant.

Out, each for its own issue:

- Exactly-once from the webhook to Kafka. With `after_flush` the path is
  at-least-once, and the event id carries the rest.
- Dedupe across a horizon longer than the window's retention. A retry after
  window + grace + lateness counts twice. The SDK's retry budget stays inside
  it. OpenMeter dedupes for 32 days in Redis; a long-horizon store is its own
  design.
- Probabilistic dedupe. A Bloom filter silently drops real events and depends
  on insertion order, so a replay can disagree with the original. Billing
  totals must be exact and reproducible. The metering stack hashes ids with
  `md5_number` in SQL instead, which needs nothing from the engine.
- A remote state store. Kafka replay is the recovery path.
- `after_flush` for sinks other than Kafka. The flush contract is the same,
  but only the Kafka sink is tested here.

## Revisions from planning (2026-09-30)

Reading the code for the plan changed six things. Each is stated here so the
plan and the spec agree.

1. **Attribution is per record, and always exact.** The engine already
   computes each record's bucket from its event time, with the same function
   the handler's `time_bucket` uses (`core.BucketStart`). The offset record is
   keyed by that bucket and the record's own partition. The rule that
   attributed every partition to every bucket when the output lacked
   `kafka_partition` is gone.
2. **Offset records expire on the manager's closed watermark, not the
   engine's.** The engine's watermark runs ahead of what the manager has
   published. With lateness 0, a bucket is due and unpublished at the moment
   the engine's watermark passes it. Expiring its record then would commit
   past a bucket no sink has seen. The manager reports its closed watermark
   on the window's signal after each commit.
3. **Decision 5: the commit carries each window's closed watermark as Kafka
   commit metadata.** A worker that replays from the low watermark with no
   state of its own has no watermark. A late event the original worker
   refused, sitting after the low watermark, would be admitted into an
   expired minute and published alone, replacing the full count. The new
   owner reads the metadata when it is assigned the partition and refuses a
   record whose bucket ended at or before `closed - lateness`. franz-go
   supports both halves: `kgo.PreCommitFnContext` and `kgo.OnOffsetsFetched`.
   Scenario 11 tests it.
4. **Revoked partitions are forgotten.** The offset seeker kept the marks it
   loaded at startup and reapplied them whenever a partition came back, so a
   partition revoked and later reassigned rewound to a stale position. The
   seeker now forgets a released or lost partition. A windowed pipeline also
   drops the partition's marks, so it stops committing a partition another
   member owns.
5. **A drop waits for the window manager.** A manager pass in flight when
   the partition is revoked could publish the partition's partial rows after
   the callback returned. The drop takes the window's pass lock, so no pass
   runs while the rows are deleted. Records the consumer had prefetched from
   the partition are skipped when they arrive.
6. **A null key stops the pipeline.** A sink write error is fatal whatever
   the error policy says, so a row with a null key fails with
   `user.sink.encode_failed` and the pipeline stops. The ingest handler
   filters rows with no customer before the sink.

The scenario table changes too:

- Scenarios 4 and 5 run one worker. With three, the killed worker's
  partitions move to the others after the 45-second session timeout, and the
  scenario becomes scenario 3.
- Scenario 9 runs in process: a real webhook source and turbine with a sink
  whose flush fails. A failed flush against a real broker needs a fault
  proxy.
- Scenario 11 is new: a worker refuses a late event and loses its disk
  while a later minute is still open, and a fresh worker replays past the
  late event. Expected on `main`: fails, because the open minute's rows are
  lost, as in scenario 5. With decision 3 alone it fails differently: the
  late event's minute is published with that event alone. With decision 5
  it passes.

## Risks

- **Replay cost on a busy partition.** Three minutes of a hot partition is
  re-read on every rebalance. Scenario 10 measures it and the PR states the
  number.
- **Offset records grow with buckets × partitions.** One row per bucket per
  partition, dropped at retention: 180 seconds of one-minute buckets across 6
  partitions is about 18 rows per window table. Small, and bounded.
- **`after_flush` latency.** A request waits up to one flush interval. The SDK
  sends batches, so this is paid per batch, not per event. Measure p50 and p99
  at the demo's rate.
