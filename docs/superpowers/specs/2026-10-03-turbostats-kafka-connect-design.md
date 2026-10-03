# TurboStats v1 amendment: pipelines beyond SQLFlow, starting with Kafka Connect

This spec makes the TurboStats contract describe any pipeline runtime, not
only SQLFlow. Kafka Connect is the first runtime it adds, and Debezium CDC is
the first workload. A jar inside the Connect worker reports one bundle per
connector task.

Every change is additive, so the document stays v1. No field in this spec
names Debezium or Kafka Connect. Each one has a meaning on SQLFlow too, and
the mapping from Connect to the contract lives in its own section.

Where this spec and the earlier ones disagree, this spec wins.

## Why now

A Debezium connector fails in ways today's contract reports as healthy:

- **A task fails while the worker stays up.** The worker keeps sending
  heartbeats, so a receiver reads the connector as `up`. This is the most
  common way CDC breaks silently.
- **A task restarts in a loop inside one JVM.** `process.started_at` never
  moves, so uptime can't detect the loop.
- **The connector loses its database.** Debezium retries, the task stays
  running, and nothing in the bundle changes.
- **A snapshot stalls.** The contract has no idea of bounded work inside a
  stream.

The contract also assumes one process runs one pipeline. A Connect worker
runs many connectors, and a rebalance moves them between workers.

## Identity

**`instance.id` names one stream of reports, and the reporter guarantees it is
unique within its org.** Two streams under one id are a reporter bug. The
reporter owns identity because only the reporter knows what makes it unique.
A receiver never namespaces ids. If it detects two streams interleaved under
one id, by `process.id`, it flags a collision rather than repairing it.

Three identities describe a report, and none is specific to a runtime:

| Field | Means | Kafka Connect | SQLFlow |
|---|---|---|---|
| `instance.id` | One stream of reports, unique per org | `<cluster>/<connector>/<task>` | The configured `id` |
| `instance.name` | The logical pipeline. Instances that share a name are parts of one pipeline. | `<cluster>/<connector>` | `pipeline.name` |
| `process.host` (new) | Where the stream runs now | The worker ID, `host:port` | The hostname |

`process.id` keeps its meaning: 16 random bytes drawn when a process starts.

**The unit of a Connect report is a task, not a connector.** Each task
restarts on its own and keeps its own counters, so a sum across tasks has no
single epoch and two reports can't be subtracted. A task number survives a
rebalance, so `instance.id` and its history survive one too. A single-task
connector, such as Debezium Postgres or MySQL, reports as `/0`.

**`<cluster>` defaults to the worker's `group.id`.** Many installs keep a stock
value such as `connect-cluster`, which collides across clusters in one org.
The reporter takes a `turbostats.cluster` setting and logs a warning when it
falls back to a stock value.

Combining a pipeline's instances into one view, by `instance.name`, is a later
receiver feature. It needs no field beyond this spec.

SQLFlow violates the uniqueness rule today: replicas that share one config
share one `id`. That is #430, and it is out of this spec's scope.

### What the instance runs on

| Field | Type | Means | Kafka Connect | SQLFlow |
|---|---|---|---|---|
| `instance.runtime` | string | The engine that runs the pipeline | `kafka-connect` | `sqlflow` |
| `instance.runtime_version` | string | That engine's version | Kafka's version | absent: `version` already says it |
| `instance.reporter_version` | string | The reporter's own version, when it is not the engine | The jar's version | absent |

On Connect, `instance.version` is the connector plugin's version, such as
`3.2.0.Final`, and `instance.commit` is empty. `instance.config_hash` hashes
the connector's config. The config's values never cross the wire, and many
of them are secrets.

`source_type` and `sink_type` keep their open vocabulary. A Debezium Postgres
connector sends `postgres` and `kafka`. A JDBC sink connector sends `kafka`
and `jdbc`. `handler_type` is absent on Connect: a chain of single message
transforms is not one of SQLFlow's handlers.

The new instance field names join the reserved label names: `runtime`,
`runtime_version` and `reporter_version`.

## The pipeline's lifetime

A pipeline can start, fail and restart while its process keeps running. Three
fields in `pipeline` describe that:

- **`state`** is `starting`, `running`, `paused`, `stopped` or `failed`. A
  receiver reads a value it doesn't know as unknown, never as running.
  SQLFlow sends `running` for as long as it reports.
- **`started_at`** is the epoch for every pipeline counter. A receiver
  subtracts two reports only when their `started_at` agrees. On SQLFlow it
  equals `process.started_at`. On Connect, a task restart moves it, and
  Debezium resets its own counters at the same moment.
- **`restart_count`** counts pipeline starts after the first, since the
  process started. It is a counter, because a task can restart several times
  between two reports and `started_at` shows only the last one.

`state` is the field this spec exists for. It is the only one that can say a
pipeline has failed while its process is healthy. Without it, the contract
breaks the rule from #354: no field may show its healthiest value while the
pipeline is in trouble.

**Stopping is not moving.** A Connect task moved by a rebalance sends nothing
more from its old worker, and its new worker starts reporting under the same
`instance.id`. A connector that the operator stops or deletes sends a final
bundle with `exit`, the way a clean SQLFlow shutdown does. A receiver then
says `exited`, not `unreachable`. A connector that no worker holds sends
nothing, and `unreachable` is the true reading.

## Backfill

A backfill is bounded work inside an unbounded pipeline. Debezium's snapshot
is one. Other examples include a SQLFlow topic replay from the earliest
offset, a rebuild of window state, and a historical sync. A backfill is not a
pipeline: it has no source, sink or identity of its own.

`pipeline.backfill` is present for a source that can run a backfill, from the
first report to the last. It carries `state: none` until one runs. It is
absent for a source that can't, such as a Connect sink connector, or SQLFlow
today. This keeps the rule that a process's field set never changes: a
Debezium incremental snapshot can start at any point in a connector's life.

| Field | Type | Means |
|---|---|---|
| `state` | string | `none`, `running`, `paused`, `completed` or `aborted` |
| `blocks_stream` | bool | Whether the stream waits for the backfill. True for an initial or blocking snapshot, false for an incremental one. |
| `elapsed_seconds` | int64 | How long the current or last backfill has run, paused time included |
| `unit` | string | What `units_*` counts: `table`, `partition` |
| `units_total` | int | The units the backfill covers |
| `units_left` | int | The units it has yet to finish |
| `rows_read` | int64 | Rows read, summed over units |

- **A completed backfill keeps its final values until the next one starts.**
  Debezium's metrics behave the same way.
- **Backfill rows count toward the pipeline's totals.** `message_count` stays
  the pipeline's total work, and `rows_read` reports the backfill's share of
  it. Phase is a shard: it splits one measurement, and the sum stays true.
- **Backfill rows never enter event lag.** A snapshot row's age says when the
  row was last written, not how far behind the stream the pipeline runs.
- **`units_left` follows the rollup daemon's `tables_left`.** Both count what
  remains, so a receiver reads progress the same way in both places.
- **`rows_read` moves in steps.** Debezium updates it every 10,000 rows and at
  the end of each table. A receiver's stall threshold has to exceed the time
  one step takes on the slowest table.

## Freshness: input and output

The contract already records when work arrives: `pipeline.last_message_at`,
and `idle_seconds` on a monotonic clock. It does not record when work leaves.

**`pipeline.last_sink_write_at` is the time of the last write the destination
acknowledged.** A pipeline that reads but can't write shows a fresh
`last_message_at` and a stale `last_sink_write_at`. That gap is the failure a
receiver pages on. It is absent until the first acknowledged write.

On SQLFlow it is set when `Flush` returns nil. The Connect mapping is in its
own section, because Connect's own counter of written records counts on send,
not on acknowledgment.

This field is a pipeline's own report. The `freshness` section, which reads a
store's tables from outside, is a different measurement and stays separate.

## Completeness

Row accounting (#240) defined four stages, and they hold for any runtime:
`message_count`, `handler_rows_read`, `sink_rows_accepted` and
`sink_rows_written`. Each adjacent ratio isolates one kind of loss.

One outcome has no field: rows discarded under a tolerant error policy.
**`pipeline.error_rows_dropped`** counts them. On Connect that is
`errors.tolerance=all` without a dead letter queue. On SQLFlow that is the
`IGNORE` policy dropping a batch. It is silent data loss, like
`late_rows_dropped`, so it has its own field and is never summed with another
outcome.

Every ratio is a floor, not an equality. Both runtimes are at-least-once, so a
ratio above 1 after a restart is normal. A receiver alerts on a low ratio,
never on a ratio that isn't exactly 1.

The engine attests to its own work. Proving that a database and a topic hold
the same rows needs a probe that reads both, which stays out of scope.

## Bytes

`message_payload_bytes` measures payload: what the pipeline handles. The
dimensional series spec said wire bytes would be different fields. They are:

| Field | Type | Means |
|---|---|---|
| `pipeline.source_wire_bytes` | int64 | Bytes the source read off the network, framing and compression included |
| `pipeline.sink_wire_bytes` | int64 | Bytes the sink wrote to the network, framing and compression included |

Each is a counter, and absent when the client can't report it. Payload bytes
and wire bytes never share a field.

## Memory

The contract has Go-specific fields: `go_retained_bytes` and `go_heap_bytes`.
They stay, because removing a field is v2. A JVM has the same two quantities
under different names. **`process.memory`** carries them for any runtime with
a managed heap:

| Field | Type | Means | Go | JVM |
|---|---|---|---|---|
| `runtime` | string | Which runtime's memory this is | `go` | `jvm` |
| `retained_bytes` | int64 | Memory the runtime holds from the operating system | `go_retained_bytes` | Heap and non-heap committed |
| `live_bytes` | int64 | What the last collection found live | `go_heap_bytes` | Old generation usage after collection |
| `heap_limit_bytes` | int64 | The runtime's heap ceiling; absent when unlimited | `GOMEMLIMIT` | `-Xmx` |
| `gc_count` | int64 | Collections since the process started | `/gc/cycles/total:gc-cycles` | Sum over the collector beans |

On Go, `retained_bytes` and `live_bytes` are copies of the Go fields, the way
`last_activity_at` copies its section timestamps. The Go fields stay the
source.

- **`live_bytes` is the leak signal.** Heap in use rises and falls with every
  collection. Live bytes rising across hours is a leak.
- **`rss_bytes` less `retained_bytes` is native memory.** That covers DuckDB
  and Arrow on SQLFlow, and direct buffers on the JVM. The Go field's comment
  already defines the split this way.
- **`gc_count` is compared only within one process.** Collectors count cycles
  differently, so its rate says something and its value across runtimes says
  nothing.

**`process.memory_limit_bytes` is the container's limit,** from the cgroup. It
is absent when no limit is set or the process can't read it. `rss_bytes`
approaching it predicts a kill. `live_bytes` approaching `heap_limit_bytes`
predicts an out-of-memory error.

**GC time stays out.** The JVM reports wall-clock pause time and Go reports
CPU time. One field would mean two different things.

**`process.goroutines` becomes omittable.** It is required today, so a JVM
would have to send `0`, which is false rather than unknown. A live Go process
always has at least one goroutine, so SQLFlow keeps sending it. `rss_bytes`
set this precedent. Loosening a field is not removing it, so the document
stays v1. A receiver decodes it into a pointer.

## Source connection

**`pipeline.source_connected`** says whether the source holds its connection
now. It is absent when the source can't tell. It separates a quiet source
from a lost one, which `last_message_at` alone cannot: both stop delivering.

## Event lag

The `event_lag_basis` vocabulary gains `source_commit_time`: the source
database's commit time of the change, to processing. Debezium's
`MilliSecondsBehindSource` measures that span. The vocabulary is open, so a
receiver that doesn't know the name keeps the reading and declines to compare
it.

## Kafka Connect and Debezium

This section maps Connect and Debezium onto the contract. It is the
reporter's design, not the contract's.

### How the reporter collects

- **A REST extension** runs inside each worker (`rest.extension.classes`). It
  reads cluster state, task placement and connector config, and it sends the
  bundles.
- **JMX** supplies Connect's metrics, Debezium's metrics and the JVM's memory,
  from the worker's platform MBean server.
- **A producer interceptor** (`producer.interceptor.classes`) counts
  acknowledged records per task. Connect's `source-record-write-total` counts
  when the worker sends a record, before the broker acknowledges it
  (`AbstractWorkerSourceTask`). The interceptor's `onAcknowledgement` is the
  only per-record acknowledgment the worker exposes. The interceptor publishes
  its counts as an MBean, so the extension reads them through JMX whatever
  classloader each one runs in.

### Source connectors

| Contract | Debezium source connector | Other source connectors |
|---|---|---|
| `state` | Task status | Task status |
| `message_count` | `TotalNumberOfEventsSeen`, snapshot plus streaming | `source-record-poll-total` |
| `handler_rows_read` | `source-record-poll-total` | `source-record-poll-total` |
| `sink_rows_accepted` | `source-record-write-total` | `source-record-write-total` |
| `sink_rows_written` | Interceptor acknowledgments | Interceptor acknowledgments |
| `last_sink_write_at` | Interceptor's last acknowledgment | Interceptor's last acknowledgment |
| `last_message_at` | Newer of the two contexts' `MilliSecondsSinceLastEvent` | When `source-record-poll-total` last rose |
| `error_count` | `total-record-errors` | `total-record-errors` |
| `error_rows_dropped` | `total-records-skipped` | `total-records-skipped` |
| `dlq_rows` | `deadletterqueue-produce-requests` less `deadletterqueue-produce-failures` | The same |
| `last_error_at` | `last-error-timestamp` | `last-error-timestamp` |
| `sink_wire_bytes` | Producer `outgoing-byte-total` | Producer `outgoing-byte-total` |
| `source_connected` | Streaming `Connected` | absent |
| `event_lag_*` | `MilliSecondsBehindSource`, its max, basis `source_commit_time` | absent |
| `backfill` | Snapshot context | absent |

`backfill.state` comes from `SnapshotRunning`, `SnapshotPaused`,
`SnapshotCompleted` and `SnapshotAborted`. A skipped snapshot is `none`.
`units_total` is `TotalTableCount`, `units_left` is `RemainingTableCount`,
`rows_read` is `RowsScanned` summed over tables, and `elapsed_seconds` is
`SnapshotDurationInSeconds`. A non-empty `ChunkId` marks an incremental
snapshot, which sets `blocks_stream` to false.

`source_connected` is absent while a backfill blocks the stream. During an
initial snapshot the streaming connection isn't open yet, and `false` would
read as a lost database.

### Sink connectors

| Contract | Sink connector |
|---|---|
| `message_count`, `handler_rows_read` | `sink-record-read-total` |
| `sink_rows_accepted` | `sink-record-send-total` |
| `last_sink_write_at` | When `offset-commit-completion-total` last rose |
| `lag_*` | Consumer `records-lag`, collapsed by the dimensional series rule |
| `source_wire_bytes` | Consumer `bytes-consumed-total` |

`sink_rows_written` is absent. Connect commits a sink task's offsets only
after the task has flushed, so a commit dates a write, but no metric counts
the rows behind it.

### Restarts

`started_at` is when the task's metrics group registered. `restart_count`
counts registrations after the first, observed through the MBean server's
registration notifications. Sampling would miss a task that restarts between
two samples.

## The document

A Debezium Postgres connector during an incremental snapshot:

```jsonc
{
  "instance": {
    "id": "prod-connect/inventory-cdc/0",
    "name": "prod-connect/inventory-cdc",
    "runtime": "kafka-connect",
    "runtime_version": "3.8.0",
    "reporter_version": "0.1.0",
    "version": "3.2.0.Final",
    "commit": "",
    "config_hash": "…",
    "source_type": "postgres",
    "sink_type": "kafka"
  },
  "process": {
    "id": "9f2c…",
    "host": "10.0.3.7:8083",
    "started_at": "…",
    "rss_bytes": 912000000,
    "memory_limit_bytes": 2147483648,
    "memory": {
      "runtime": "jvm",
      "retained_bytes": 780000000,
      "live_bytes": 240000000,
      "heap_limit_bytes": 1073741824,
      "gc_count": 18233
    }
  },
  "pipeline": {
    "state": "running",
    "started_at": "…",
    "restart_count": 0,
    "source_connected": true,
    "message_count": 1840221,
    "handler_rows_read": 1840221,
    "sink_rows_accepted": 1840221,
    "sink_rows_written": 1840190,
    "last_sink_write_at": "…",
    "error_rows_dropped": 0,
    "sink_wire_bytes": 412000000,
    "event_lag_seconds": 2,
    "event_lag_basis": "source_commit_time",
    "backfill": {
      "state": "running",
      "blocks_stream": false,
      "elapsed_seconds": 1260,
      "unit": "table",
      "units_total": 42,
      "units_left": 25,
      "rows_read": 1203554
    }
  }
}
```

## JSON Schema

A reporter that isn't written in Go needs the contract in a form it can
check. The Kafka Connect reporter is the first. The contract is therefore
published as JSON Schema, reflected from the Go types in `turbostats/wire`.

**The Go types are the contract, and the schema is an artifact of them.**
Config schemas already work this way: `internal/schema` reflects them with
`invopop/jsonschema`, a golden test fails when the committed file is stale,
and `make schema` regenerates it. A hand-written schema would be a second
statement of the contract with nothing holding the two together.

- `internal/schema` gains a generator that reflects `wire.Bundle` and
  `wire.Response`. Descriptions come from the doc comments in
  `turbostats/wire`, so the schema carries the contract's prose.
- The output is committed as `turbostats/wire/schema/bundle.json` and
  `response.json`. The `turbostats/wire/schema` package embeds both with
  `go:embed`, so `wire` and its subpackages still import only the standard
  library.
- `make schema` regenerates them, and a golden test fails when they are
  stale.

### Four rules the config schemas don't follow

The contract's own rules make this schema differ from the config schemas:

1. **Unknown fields are allowed everywhere.** Every reader ignores unknown
   sections and fields, and a new field is additive. A test asserts that
   `"additionalProperties": false` appears nowhere.
2. **No enums.** A new value of `state` or `backfill.state` is an additive v1
   change, and an enum would make an old validator reject a new engine. Each
   field's description lists the values known today.
3. **`required` follows the json tags.** A field without `omitempty` is
   required. `goroutines` stops being required in this change.
4. **`buckets` holds exactly `len(DurationBounds)+1` items.** The generator
   reads the count from the code, the way the config generator reads its
   enums from the registries.

### Where it is published

The control plane serves both documents:

| Document | URL |
|---|---|
| Bundle | `https://control.turbolytics.io/v1/turbostats/bundle.schema.json` |
| Response | `https://control.turbolytics.io/v1/turbostats/response.schema.json` |

Each URL is the document's `$id`. The `v1` prefix matches the media type and
the ingest route, `POST /v1/turbostats`. A v2 contract gets new URLs, and v1's
stay as they are.

The control plane embeds the files from the `wire` module it already builds
against. It serves the schema of exactly the types it accepts, and no copy
step can fall behind.

### What validates against it

- The contract's Go tests validate `testdata/vectors.json` and a freshly
  collected bundle, with `santhosh-tekuri/jsonschema`, which is already a
  dependency.
- The Kafka Connect reporter's CI validates every bundle it builds against the
  same file. This is the guard across languages: the Java reporter can't drift
  from the Go types without failing CI. Whether it also generates its Java
  types from the schema is the reporter plan's decision.

### What the schema can't say

The schema checks shape. The presence rules are about how a process's reports
relate over time:

- the window fields travel together;
- `backfill` is present from the first report;
- a process's field set never changes;
- counters only rise within one `pipeline.started_at`.

Those stay in this spec's prose and in the invariant tests.

## What a receiver does with these

This section guides the control plane. It is not part of the contract.

The fields fall into the first two levels of data operational maturity:

| Level | Question | Fields |
|---|---|---|
| 1. Mechanism | Is it running, succeeding, queuing? Resources? | `state`, `restart_count`, errors, durations, `recv_wait_seconds`, memory |
| 2. Consistency | Volume, freshness, progress | The ledger, bytes, `last_sink_write_at`, event lag, `backfill` |
| 3. Accuracy | Is the output correct? | None. That needs a probe. |

- **A level 1 verdict outranks every level 2 verdict.** A failed task also
  makes output stale and stops the ledger. The verdict says "failed", with
  staleness as evidence, not three problems.
- **Page on symptoms, notify on causes.** Stale output, a failed task and lag
  past its threshold page. A crash loop, a lost source, a stalled backfill and
  memory growth notify, and escalate when output goes stale. Error counts
  notify on a rate over budget, never per error.
- **A freshness threshold is a business tolerance, so the operator declares
  it** in the receiver's alert settings, per pipeline. It never enters the
  contract.

## Decisions

| Decision | Choice | Rejected |
|---|---|---|
| What a snapshot is | Bounded work inside a pipeline: `backfill` | A pipeline of its own. It has no source, sink or identity, and it can run concurrently with the stream it belongs to. |
| The unit of a Connect report | One task | One connector: tasks restart separately, so a sum has no epoch. One worker with a list of pipelines: a connector's history would jump between workers, and verdicts would split between levels. |
| Who makes ids unique | The reporter | The receiver: it can't know what is unique inside a runtime it doesn't understand. |
| Grouping tasks into a connector | `instance.name`, later, in the receiver | A `member` field: it names a Connect concept, and `name` already carries the grouping. |
| Two document types, process and pipeline | Not now | The cleanest model, but a v2-sized change before any customer asked for it. |
| Acknowledged rows on Connect | A producer interceptor | `source-record-write-total`: it counts on send. |
| The schema's source | Reflected from the Go types | Hand-written: a second statement of the contract that drifts silently. |
| Where the schema is published | The control plane, under `/v1/turbostats` | `turbolytics.io`: a copy step that can fall behind. Raw GitHub at a tag: the URL changes with every release, and `$id` must not. |
| GC time | Out | One field meaning wall time on the JVM and CPU time on Go. |
| Heap in use, garbage included | Out | It rises and falls every collection and says nothing `live_bytes` doesn't. |

## Testing

### The contract

- A round-trip test for every new field, and a test that an engine predating
  them decodes as unknown, not zero.
- The shape invariant extended: `backfill` and `memory` are fixed-shape
  objects, and no new field repeats with data cardinality.
- A test that `backfill` is present with `state: none` before any backfill,
  and in every report after.
- A test that a bundle without `goroutines` decodes, and that SQLFlow still
  sends it.
- `pytest tests/release`, because the bundle's shape has a Python guard as
  well as the Go type walk.
- The schema's golden test, the test that no object forbids unknown fields,
  and validation of `vectors.json` and a collected bundle against the schema.
- A mutation check: adding `"additionalProperties": false` to one object, or
  an enum to `state`, fails a test.

### SQLFlow

- It sends `runtime: sqlflow`, `state: running`, and a `pipeline.started_at`
  equal to `process.started_at`.
- `memory.retained_bytes` and `memory.live_bytes` equal the Go fields in the
  same bundle.
- `memory_limit_bytes` matches the limit of a container started with
  `--memory`, and is absent without one.
- `last_sink_write_at` moves on a flush that returns nil, and doesn't move on
  one that fails.
- `error_rows_dropped` counts a batch the `IGNORE` policy drops.

### The reporter, against a real Connect worker and Postgres

Each trouble case has a test that fails if its field reads healthy:

- An initial snapshot: `backfill.state` running, `blocks_stream` true,
  `source_connected` absent.
- Streaming after the snapshot: `backfill.state` completed, event lag present.
- An incremental snapshot by signal: `blocks_stream` false, and event lag
  still reported.
- Postgres stopped: `source_connected` false while `state` stays running.
- A failed task: `state` failed while the worker keeps reporting.
- A task restarted three times inside one interval: `restart_count` rises by
  three.
- The broker paused: `sink_rows_accepted` rises and `last_sink_write_at`
  stops.
- Two workers and a rebalance: the moved task's `instance.id` stays the same,
  `process.host` changes, and no `exit` is sent.
- A connector stopped: a final bundle with `exit`.
- A sink connector with three tasks: three distinct `instance.id` values under
  one `instance.name`.
- A stock `group.id` without `turbostats.cluster`: the warning is logged.
- `live_bytes` under G1 and under ZGC, against a deliberate leak.

## What breaks if this is wrong

- **Without `state`, a failed connector reads as up.** That is the failure
  this spec exists to prevent, and the one nobody investigates.
- **Without `pipeline.started_at`, a receiver subtracts counters across a
  task restart.** Debezium's counters reset, so the rate goes negative or
  jumps.
- **If `last_sink_write_at` counts on send, a broker outage reads as fresh.**
  The paging verdict then never fires.
- **If ids collide, two tasks' counts interleave under one instance.** Every
  rate is wrong and still looks plausible.
- **If `source_connected` reports false during an initial snapshot,** every
  new connector pages as a lost database.
- **If `backfill` appears only when a snapshot starts,** the field set changes
  mid-process and a receiver's series split.

## Verify before building

These rest on documentation or reading source, not on execution. The plan
proves each one first:

- The producer interceptor sees the client ID Connect assigns per task, so it
  can attribute an acknowledgment to a connector and task.
- The interceptor's MBean is readable from the REST extension across plugin
  classloaders.
- `ChunkId` is non-empty during an incremental snapshot, and `Connected` is
  false before streaming starts.
- Old-generation usage after collection tracks a leak under G1, ZGC and
  Parallel.
- Task metrics groups register on every task start, including a restart
  through the REST API.
- `ConnectClusterState` exposes a connector's config and its tasks' workers to
  a REST extension.
- A sink connector's consumer `records-lag` keeps its last value when the
  consumer stops fetching. If it does, `lag_observed_at` needs a time the
  consumer recorded, not the reporter's sample time.

## Out of scope

- **Grouping a pipeline's instances** by `instance.name` in the receiver.
- **SQLFlow replicas that share an id.** #430.
- **Lineage:** which tables feed which topics. The shape rule refuses a
  per-table list in a heartbeat. Lineage comes from config, so it could
  travel as a separate document sent when `config_hash` changes.
- **A SQLFlow backfill.** The object exists for it, and SQLFlow sends none
  until it has one.
- **Postgres replication-slot growth** on a quiet database.
- **Durations on Connect.** Connect keeps only a max and an average for poll
  time, which can't fill the buckets.
- **A freshness probe** that reads the destination.
- **Commands,** such as restarting a failed task from the receiver.
