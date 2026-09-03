# SQLFlow: DuckDB for Streaming Data.

[Quickstart](#quick-start-getting-started-in-5-minutes) | [Tutorials](https://sql-flow.com/docs/category/tutorials) | ![Docker Pulls](https://img.shields.io/docker/pulls/turbolytics/sql-flow) | [Documentation](https://sql-flow.com)

SQLFlow is a stream processing engine that lets you define pipelines with just SQL. It consumes a stream, runs DuckDB SQL over each batch, and writes the result out. Think of it as a lightweight, single-binary Flink.

- Sources: [Kafka](https://kafka.apache.org/), WebSockets, webhooks.
- Sinks: Kafka, ClickHouse, Iceberg, the console, or anything DuckDB can `COPY` to (PostgreSQL, S3, parquet, MotherDuck, DuckLake).
- Built on [DuckDB](https://duckdb.org/) and [Apache Arrow](https://arrow.apache.org/): ~900k messages/sec on a laptop, in about a quarter GiB of memory.
- One Go binary, one Docker image, one YAML file per pipeline. No cluster.

# Quick Start (Getting Started in 5 Minutes)

1. Build a binary (see [Installation](#installation) for prebuilt binaries and Docker):

```
make sqlflow
```

2. Validate a pipeline against test data, without a broker. `dev invoke` runs the
   config's handler over a JSONL fixture and prints the result:

```
./bin/sqlflow dev invoke dev/config/examples/basic.agg.mem.yml dev/fixtures/simple.json

{"city":"New York","city_count":28672}
{"city":"Baltimore","city_count":28672}
```

3. Start Kafka locally:

```
docker-compose -f dev/kafka-single.yml up -d
```

4. Publish test messages. The publisher is a Python script, so install its
   dependencies once:

```
pip install -r requirements.txt
python3 cmd/publish-test-data.py --num-messages=10000 --topic="input-simple-agg-mem"
```

5. Start a Kafka consumer to watch the output:

```
docker exec -it kafka1 kafka-console-consumer --bootstrap-server=kafka1:9092 --topic=output-simple-agg-mem
```

6. Run the pipeline:

```
./bin/sqlflow run -c dev/config/examples/basic.agg.mem.yml --max-msgs=10000
```

The consumer prints one row per city:

```
{"city":"San Fransisco","city_count":177}
{"city":"New York","city_count":236}
{"city":"Miami","city_count":203}
{"city":"Baltimore","city_count":180}
```

# Installation

sqlflow reaches DuckDB through the Arrow ADBC driver manager, which **dlopens
`libduckdb` at runtime**. The binary is not standalone: wherever you run it,
that shared library has to be present. `SQLFLOW_DUCKDB_LIB` points at it;
without that variable sqlflow looks in `/opt/homebrew/lib/libduckdb.dylib` on
macOS and `/usr/local/lib/libduckdb.so` on Linux.

The pinned DuckDB version lives in one place, the `DUCKDB_VERSION` file.

### Docker (no libduckdb setup)

The published image bakes in a matching `libduckdb.so` and sets
`SQLFLOW_DUCKDB_LIB`, so nothing else is needed. Images are multi-arch
(`linux/amd64`, `linux/arm64`):

```
docker run --rm \
  -v $(pwd)/dev:/tmp/conf \
  turbolytics/sql-flow:latest \
  dev invoke /tmp/conf/config/examples/basic.agg.mem.yml /tmp/conf/fixtures/simple.json
```

To build the image from the repo, `make sqlflow-image` — it prints the tag it
built, derived from `git describe`.

### Prebuilt binary

Release binaries are published for linux and macOS on amd64 and arm64. Download
the one matching your platform, then install a matching `libduckdb`:

```
chmod +x sqlflow_<version>_<os>_<arch>
./scripts/install-libduckdb.sh /usr/local/lib
export SQLFLOW_DUCKDB_LIB=/usr/local/lib/libduckdb.so
./sqlflow_<version>_<os>_<arch> version
```

`scripts/install-libduckdb.sh` **always fetches the linux `libduckdb.so`**, for
the architecture it detects. It is for linux hosts and containers only. On
macOS, `brew install duckdb` puts the library at the default path and no
environment variable is needed.

### From source

Requires Go 1.25+, a C toolchain (cgo is mandatory), and libduckdb.

```
# macOS
brew install duckdb
make sqlflow

# linux
./scripts/install-libduckdb.sh /usr/local/lib
export SQLFLOW_DUCKDB_LIB=/usr/local/lib/libduckdb.so
make sqlflow
```

The binary lands at `bin/sqlflow`.

# How SQLFlow Works

A pipeline has three parts:

<img width="1189" alt="SQLFlow architecture" src="https://github.com/user-attachments/assets/1295e7eb-a0b8-4087-8aa4-cad75a0c8cfa" />

**Source** — Kafka, a WebSocket, or webhooks, modelled as a stream of messages.

**Handler** — DuckDB SQL executed over a batch of that stream: filter,
aggregate, enrich, or drop data.

**Sink** — where the SQL result goes: Kafka, ClickHouse, Iceberg, the console,
or — through the `sqlcommand` sink — anywhere DuckDB can write.

A config file names all three, plus optional `commands` run before the pipeline
starts (attaching databases, creating tables) and optional `tables` the engine
manages across the pipeline's lifetime:

<img width="1256" alt="Example config" src="https://github.com/user-attachments/assets/3d7b8434-4f73-4a66-800b-5c1392c97d52" />

## SQLFlow Use-Cases

- **Streaming Data Transformations**: Clean data and types and publish the new data ([example config](dev/config/examples/basic.agg.mem.yml)).
- **Stream Enrichment**: Add data to an input stream and publish the new data ([example config](dev/config/examples/enrich.yml)).
- **Data aggregation**: Aggregate input data batches to decrease data volume ([example config](dev/config/examples/basic.agg.mem.yml)).
- **Tumbling Window Aggregation**: Bucket data into arbitrary time windows (such as "hour" or "10 minutes") ([example config](dev/config/examples/tumbling.window.yml)).
- **Run SQL against the Bluesky Firehose**: Execute SQL against any websocket source, such as the [Bluesky firehose](https://docs.bsky.app/docs/advanced-guides/firehose) ([example config](dev/config/examples/bluesky/bluesky.kafka.raw.yml)).
- **Stream Data to Iceberg**: Stream writes to an Iceberg catalog.
- **Stream Data to ClickHouse**: Insert stream processing outputs into ClickHouse ([example config](dev/config/examples/kafka.clickhouse.yml)).
- **Enrich Streams with Postgres Data**: Query postgres during stream processing to enrich stream data.
- **Sink Kafka to Postgres**: Insert stream processing outputs into postgres.

# CLI Reference

```
sqlflow [command]
```

| Command | Purpose |
|---|---|
| `run` | Run a pipeline against a live source |
| `dev invoke` | Run a pipeline's handler against a static file |
| `config validate` | Validate a config against the JSON Schema |
| `config example` | Print a commented example configuration |
| `tail` | Print every message from a config's source |
| `version` | Print version, commit and Go version |

### `sqlflow run`

Runs the pipeline: consume, batch, execute SQL, sink, commit offsets.

```
sqlflow run <config> [flags]
sqlflow run -c <config> [flags]
```

| Flag | Default | Description |
|---|---|---|
| `-c`, `--config` | *(required)* | Path to the config file, unless given positionally |
| `--max-msgs` | `0` | Stop after N messages; `0` is unlimited. `--max-msgs-to-process` is an alias |
| `--metrics` | *(off)* | Metrics exporter. Only `prometheus` is supported; serves `/metrics` on `:8000` |
| `--stats-json` | *(off)* | Write final run stats as JSON to this path |
| `--pprof` | `false` | Serve pprof on `:6060`, and enable block/mutex profiling |

`--stats-json` writes a small object, useful for CI assertions:

```
$ sqlflow run -c dev/config/examples/benchmark.structured.mem.yml \
    --max-msgs=2000 --stats-json=/tmp/stats.json
...
{"messages_consumed":2000,"num_errors":0}
```

### `sqlflow dev invoke`

Runs the config's `commands`, `tables` and handler over a JSONL fixture and
prints the resulting rows to stdout, one JSON object per line. The sink is
deliberately **not** exercised, so this is safe to run against a production
config. This is the fastest way to iterate on SQL.

```
sqlflow dev invoke <config> <fixture>
```

### `sqlflow config validate`

Renders the config's Jinja2 template, then validates the result against the
JSON Schema.

```
$ sqlflow config validate dev/config/examples/basic.agg.mem.yml
dev/config/examples/basic.agg.mem.yml: valid
```

### `sqlflow config example`

Prints a fully commented YAML skeleton generated from the schema — every key,
its description, and the accepted enum values.

### `sqlflow tail`

Connects a config's source and prints every message to stdout, with no handler
or sink. Useful for confirming a source is configured correctly.

```
sqlflow tail -c <config>
```

### `sqlflow version`

```
$ sqlflow version
sqlflow v1.0.4
commit: 55c3129
go:     go1.25.5
```

# Configuration

A config is a YAML file rendered as a **Jinja2 template** before it is parsed.
Every `SQLFLOW_*` environment variable is injected into the template context
under its own name, which is how configs stay portable across environments:

```yaml
brokers: [{{ SQLFLOW_KAFKA_BROKERS|default('localhost:9092') }}]
```

Two extra template variables are always defined: `STATIC_ROOT` (from
`SQLFLOW_STATIC_ROOT`, default `/tmp/sqlflow/static`) and
`SQL_RESULTS_CACHE_DIR` (from `SQLFLOW_SQL_RESULTS_CACHE_DIR`, default
`/tmp/sqlflow/resultscache`).

Parsing is **strict**: an unknown key is an error rather than a silently
ignored setting.

The top-level shape:

```yaml
commands:   # optional: SQL run once, before the pipeline starts
tables:     # optional: tables created at startup, optionally window-managed
pipeline:   # required
  name:
  description:
  batch_size:              # required, >= 1
  flush_interval_seconds:  # optional; unset means only batch_size triggers a batch
  state:                   # optional; makes state durable, see Durable state
  on_error:                # optional
  source:                  # required
  handler:                 # required
  sink:                    # required
```

`batch_size` is how many messages accumulate before the handler runs.
`flush_interval_seconds` bounds the wait: once the interval elapses a partial
batch is executed anyway, so a low-traffic topic still makes progress. When
`--max-msgs` ends a run, the final partial batch is executed on exit.

## Sources

### Kafka

```yaml
source:
  type: kafka
  kafka:
    brokers: [localhost:9092]
    group_id: my-consumer-group
    auto_offset_reset: earliest   # or latest
    topics:
      - input-topic
```

Offsets are committed after the batch has been handled and the sink has
flushed, and only up to the last message the pipeline actually processed, not
to wherever the consumer has read ahead to. See
[Delivery guarantees](#delivery-guarantees).

**SASL / TLS.** Set `security_protocol` to one of `PLAINTEXT`, `SSL`,
`SASL_PLAINTEXT`, `SASL_SSL`:

```yaml
source:
  type: kafka
  kafka:
    brokers: [localhost:9093]
    group_id: test
    auto_offset_reset: earliest
    security_protocol: SASL_SSL
    ssl:
      ca_location: /certs/ca-cert.pem
      certificate_location: /certs/client-cert.pem
      key_location: /certs/client-key.pem
      key_password: testpass
      endpoint_identification_algorithm: 'none'   # disables hostname verification
    sasl:
      mechanism: PLAIN            # or SCRAM-SHA-256, SCRAM-SHA-512
      username: user
      password: bitnami
    topics:
      - input-sasl-tls-1
```

The same `security_protocol` / `ssl` / `sasl` block works on the Kafka **sink**.
See [`kafka.sasl-tls.yml`](dev/config/examples/kafka.sasl-tls.yml).

Two limits, both of which fail loudly rather than silently: `GSSAPI` is
rejected, and an encrypted PEM key is rejected with instructions to convert it
(`openssl pkcs8 -topk8 -nocrypt`). `key_password` only covers unencrypted PEMs.

**Kafka metadata.** A Kafka source exposes `kafka_topic`, `kafka_partition` and
`kafka_offset` to `InferredMemBatch` handler SQL, if the SQL selects them.

### WebSocket

```yaml
source:
  type: websocket
  websocket:
    uri: wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post
```

Reconnects with backoff. See the [bluesky examples](dev/config/examples/bluesky/).

### Webhook

Listens for `POST /events` on `0.0.0.0:8001` (not configurable) and optionally
validates an HMAC-SHA256 signature:

```yaml
source:
  type: webhook
  webhook:
    signature_type: hmac
    hmac:
      header: 'X-Hub-Signature-256'
      sig_key: 'sha256'
      secret: "{{ SQLFLOW_GITHUB_WEBHOOK_SECRET }}"
```

Responds 200 on accept, 400 for a missing signature, 403 for an invalid one.

> **Known gotcha:** the JSON Schema only enumerates `kafka` and `websocket`, so
> `sqlflow config validate` **rejects** a webhook config that `sqlflow run`
> accepts.

## Handlers

All three handlers take `sql`. The batch is exposed to that SQL as a table.

| `type` | Batch table | Notes |
|---|---|---|
| `handlers.InferredMemBatch` | `batch` | Schema inferred from the JSON, in memory |
| `handlers.InferredDiskBatch` | `batch` | Same, but buffered through disk via `read_json_auto` |
| `handlers.StructuredBatch` | the table named by `table` | Schema declared up front; fastest |

**Inferred** handlers derive the Arrow schema from the messages themselves:

- The **column set** comes from the first message in the batch. A top-level key
  that first appears in a later message is not a column.
- **Nested struct fields** are unioned across the whole batch: a key inside an
  object that appears only in a later message still becomes a field, null in
  the rows that lack it. Without this, whether a nested field existed would
  depend on which message happened to arrive first.
- **Types** are promoted across the batch (`int` widens to `double`), JSON
  arrays become lists (of scalars, structs, or further lists), and JSON string
  escapes are decoded. A value that cannot be promoted fails the batch.

```yaml
handler:
  type: 'handlers.InferredMemBatch'
  sql: |
    SELECT properties.city as city, count(*) as city_count
    FROM batch
    GROUP BY city
```

`InferredDiskBatch` additionally accepts `sql_results_cache_dir` (default
`/tmp/sqlflow/resultscache`). It stages fixed filenames there, so two pipelines
must not share a cache directory.

**StructuredBatch** takes a table you declared in `commands`, and parses
directly into that schema. This is the fastest handler — no inference, and a
zero-copy Arrow ingest. The table is truncated at the start of every batch.

```yaml
commands:
  - name: create source buffer table
    sql: |
      CREATE TABLE source (
        event STRING,
        properties STRUCT(city TEXT)
      );

pipeline:
  handler:
    type: "handlers.StructuredBatch"
    table: source
    sql: |
      SELECT properties.city as city, COUNT(*) as count
      FROM source
      GROUP BY properties.city
```

## Sinks

`sink.type` is one of `console`, `kafka`, `sqlcommand`, `iceberg`,
`clickhouse`, `noop`. An omitted type falls back to `console`.

```yaml
# console — one JSON object per row on stdout
sink:
  type: console

# noop — discard (used for benchmarking)
sink:
  type: noop

# kafka — one JSON message per row; Flush blocks on broker acks
#         before offsets are committed
sink:
  type: kafka
  kafka:
    brokers: [localhost:9092]
    topic: output-topic
    # security_protocol / ssl / sasl as per the Kafka source

# clickhouse — the table must exist; columns are matched by name
sink:
  type: clickhouse
  clickhouse:
    dsn: clickhouse://default:@localhost:8123/default          # self-hosted, HTTP
    # dsn: clickhouses://default:<pw>@<host>.clickhouse.cloud:8443/default   # Cloud, TLS
    table: events

# iceberg — catalog resolved from .pyiceberg.yaml, exactly as pyiceberg does
sink:
  type: iceberg
  iceberg:
    catalog_name: default
    table_name: default.city_events
```

**ClickHouse.** The DSN scheme picks the protocol: `clickhouse://` is HTTP on
8123, `clickhouses://` is HTTP over TLS on 8443 (ClickHouse Cloud), `tcp://` /
`natives://` the native protocol on 9000 / 9440. Scalars, `DateTime`,
`Date`, `Enum`, `LowCardinality`, `Nullable` and `Array(T)` columns all map;
`UUID`, `IPv4` and `Decimal` accept their textual form as a string. `Map` and
`Tuple` are not supported. Measured throughput is ~50k rows/sec into a local
ClickHouse from one process.

**Iceberg.** SQL-backed catalogs only (`sqlite://`); a REST catalog is an
error.

**`sqlcommand`** is the general escape hatch: the batch is exposed to your SQL
as the table `sqlflow_sink_batch`, and DuckDB does the writing. This is how to
reach PostgreSQL, S3, local parquet, MotherDuck and DuckLake:

```yaml
sink:
  type: sqlcommand
  sqlcommand:
    substitutions:
      - var: $sqlflow_uuid
        type: uuid4        # the only supported substitution type
    sql: |
      COPY sqlflow_sink_batch
        TO '/tmp/sqlflow/out/$sqlflow_uuid.parquet'
      (FORMAT 'parquet');
```

`sink.format.type: parquet` is parsed and ignored.

## Error policies

`pipeline.on_error.policy` is `RAISE` (default), `IGNORE`, or `DLQ`. It is
applied at both the `handler.write` and `handler.invoke` phases.

```yaml
pipeline:
  on_error:
    policy: DLQ
    dlq:                 # a full sink definition, any sink type
      type: kafka
      kafka:
        brokers: [localhost:9092]
        topic: dlq-topic
```

`DLQ` without a `dlq` block is a startup error. DLQ records carry four string
columns: `error`, `message`, `phase` (`handler.write` or `handler.invoke`) and
`timestamp`. See [`kafka.dlq.yml`](dev/config/examples/kafka.dlq.yml).
`source.error.policy` is parsed but unused; use `pipeline.on_error`.

## Durable state

A pipeline that aggregates needs its state to survive a restart. Give it a file:

```yaml
pipeline:
  state:
    path: /var/lib/sqlflow/state.db
```

DuckDB then runs on that file instead of in memory, and a `sqlflow_offsets`
table lives there beside the tables your handler writes. Every batch commits
the handler's writes and the Kafka offsets that produced them in one
transaction. On startup the pipeline reads those offsets and resumes from them.

Without a state path, DuckDB runs in memory. A crash mid-window loses that
window's aggregate while its offsets are already committed, so the consumer
group reports no lag and a restart replays nothing. Set a state path for any
pipeline with a `tables` block.

Two consequences worth knowing:

- The state file is the source of truth. When it disagrees with the consumer
  group, the pipeline resumes from the file's offsets. It applies them while
  joining the group, so a restart works even while the previous process is
  still a member, which it is for the session timeout after a crash.
- Window close predicates are evaluated at most one `flush_interval_seconds`
  behind the wall clock. A stateful pipeline holds one transaction open per
  batch and DuckDB's `now()` is the transaction's start time, so the pipeline
  commits on the flush tick even when idle to keep that clock moving.
- DuckDB locks the file exclusively. One state file belongs to one running
  pipeline, and no other process can read it, not even read-only. Use the
  `/stats` endpoint to inspect a running pipeline.

Durable state costs throughput. See
[What durable state costs](#what-durable-state-costs) for the numbers and the
batch size to use.

## Delivery guarantees

SQLFlow gives two guarantees. Which one applies depends on where the data
lands.

**Pipeline state is exactly-once relative to offsets.** With a state path set,
the tables your handler writes and the offsets that produced them commit in a
single transaction. A crash replays exactly the batches whose state did not
commit, so a windowed aggregate is neither short nor double-counted across a
restart.

**External sinks are at-least-once.** Kafka, ClickHouse, Iceberg and
`sqlcommand` are flushed before that transaction commits. A crash in between
replays the batch and the sink sees those rows twice. Committing first would
move offsets past rows the sink never received, which loses them silently, so
the duplicate is the deliberate choice. Use a sink that absorbs it:
`ReplacingMergeTree` in ClickHouse, an upsert in `sqlcommand`, or a downstream
dedupe on a key.

**Window sinks are at-least-once for the same reason.** A tumbling window is
published before its rows are deleted, and that delete commits with the
pipeline's next batch. A crash in between republishes the window.

Without a state path there is no state guarantee at all: handler state does not
survive the process.

## Tumbling windows

A table declared under `tables.sql` can carry a `manager`, which polls the table
on an interval, publishes the closed windows to its own sink, and then deletes
them. The handler SQL keeps the window table up to date with an upsert:

```yaml
tables:
  sql:
    - name: agg_cities_count
      sql: |
        CREATE TABLE agg_cities_count (
          bucket TIMESTAMPTZ, city VARCHAR, count INT
        );
        CREATE UNIQUE INDEX daily_cities_count_idx ON agg_cities_count (bucket, city);
      manager:
        tumbling_window:
          poll_interval_seconds: 10      # optional, default 10
          collect_closed_windows_sql: |
            SELECT ... FROM agg_cities_count
            WHERE bucket < (now()::timestamptz - INTERVAL '60' SECOND)
          delete_closed_windows_sql: |
            DELETE FROM agg_cities_count
            WHERE bucket < (now()::timestamptz - INTERVAL '60' SECOND)
        sink:
          type: kafka
          kafka:
            brokers: [localhost:9092]
            topic: output-tumbling-window-1
```

Collect, write and flush happen before the delete, so a sink failure retries
rather than dropping a window. A retry re-sends rows the sink already received,
so give a window sink a key it can deduplicate on. One final poll runs on
shutdown, against a current clock and with its delete committed, so a window
that closes during shutdown is published once and not republished on the next
start.

Windows close on wall-clock time, as written in your
`collect_closed_windows_sql`. There is no event-time watermarking and no
late-arrival policy: a message that arrives after its window closed lands in
whichever window its own SQL puts it in.

State in a managed table is lost on a crash unless the pipeline sets
[`state.path`](#durable-state).

`tumbling_window` is currently the only manager type. See
[`tumbling.window.yml`](dev/config/examples/tumbling.window.yml) and
[`kafka.stateful.window.yml`](dev/config/examples/kafka.stateful.window.yml).

## Metrics

`--metrics prometheus` serves `/metrics` on `:8000`. Twelve instruments are
exported under the meter name `sqlflow`:

| Instrument | Type | Unit |
|---|---|---|
| `message_count` | counter | messages |
| `error_count` | counter (attr: `phase`) | count |
| `source_read_latency` | histogram | seconds |
| `batch_processing_latency` | histogram | seconds |
| `sink_flush_latency` | histogram | seconds |
| `sink_flush_count` | counter | flushes |
| `sink_flush_num_rows` | gauge | rows |
| `consumer_lag` | gauge (attrs: `topic`, `partition`) | messages |
| `state_commit_count` | counter | commits |
| `state_commit_latency` | histogram | seconds |
| `state_db_size_bytes` | gauge | bytes |
| `state_table_rows` | gauge (attr: `table`) | rows |

The last four appear only when the pipeline declares a state path. An absent
series and an empty state are different facts, so a pipeline with state in
memory reports nothing rather than zero.

```
$ sqlflow run -c <config> --metrics=prometheus &
$ curl -s localhost:8000/metrics | grep message_count
message_count_messages_total{otel_scope_name="sqlflow",...} 154635
```

`consumer_lag` is the one to alert on. It is the broker's high watermark minus
the offset the pipeline has finished with, so it measures the work the
pipeline still owes rather than what its consumer group has been told.

### Inspecting durable state

`--metrics prometheus` also serves `/stats` on `:8000`, which reports what is
on disk right now:

```
$ curl -s localhost:8000/stats
{"state":{"path":"/var/lib/sqlflow/state.db","size_bytes":2109440,
          "tables":[{"table":"agg_city_count","rows":1440}],
          "offsets":[{"topic":"events","partition":0,"offset":98213}]}}
```

Reads come from a second connection, so a scrape never blocks a batch and
never reports rows a rollback then erased. A pipeline with no state path
serves no `/stats`.

## Environment variables

| Variable | Purpose |
|---|---|
| `SQLFLOW_DUCKDB_LIB` | Path to `libduckdb`. Defaults per-OS as described in [Installation](#installation) |
| `SQLFLOW_LOG_LEVEL` | Log level, default `INFO` (`DEBUG`, `INFO`, `WARN`/`WARNING`, `ERROR`) |
| `SQLFLOW_SQL_RESULTS_CACHE_DIR` | Staging dir for `InferredDiskBatch`, default `/tmp/sqlflow/resultscache` |
| `SQLFLOW_STATIC_ROOT` | `STATIC_ROOT` template variable, default `/tmp/sqlflow/static` |
| `PYICEBERG_HOME`, `PYICEBERG_CATALOG__*` | Iceberg catalog resolution, same as pyiceberg |
| `SQLFLOW_*` | Anything else is injected into the config template context under its own name |

## Behaviour notes

- **An empty batch produces no output rather than an error.** A batch is
  legitimately empty when a fixture is empty or every message in it was
  rejected; the handlers return no table and the sink is not called.
- **A missing field and an explicit `null` are the same value.** Both read as
  SQL `NULL`, so they aggregate into one group.
- **`StructuredBatch`** truncates its table at the start of every batch.
- **DuckDB version.** The engine loads whatever `libduckdb` you install;
  `DUCKDB_VERSION` pins what the image and benchmarks use.
- **Templating** is gonja (Jinja2 for Go). Every example config renders under
  it, asserted by a test.

# Benchmarks

Measured with `make benchmark-container`: Apple M1 Pro (10 cores, 32 GB),
Docker 20.10.13, DuckDB v1.5.2, Go 1.25.5, single-partition Kafka
(`confluentinc/cp-kafka:7.3.2`), 300,000 JSON messages aggregated into DuckDB.
Every run uses a fresh topic and consumer group, so runs are hermetic.

| Handler | `batch_size` | Throughput | Peak memory (container) | Peak working set |
|---|---|---|---|---|
| `handlers.StructuredBatch` | 500 | ~305k msgs/sec | 254 MiB | 167 MiB |
| `handlers.StructuredBatch` | 2000 | ~685k msgs/sec | 255 MiB | 172 MiB |
| `handlers.StructuredBatch` | 5000 | **~927k msgs/sec** | 240 MiB | 155 MiB |
| `handlers.InferredMemBatch` | 500 | ~159k msgs/sec | 256 MiB | 171 MiB |
| `handlers.InferredMemBatch` | 2000 | ~229k msgs/sec | 264 MiB | 181 MiB |
| `handlers.InferredMemBatch` | 5000 | **~256k msgs/sec** | 255 MiB | 171 MiB |

Two memory figures, both sampled from the container's cgroup by the benchmark
script: **peak memory** is everything the container is charged for (page cache
and lazily-freed pages included) — the provisioning ceiling; **peak working
set** is anonymous memory, comparable to RSS — what the engine actually holds.
Memory is flat across handlers and batch sizes at roughly a quarter GiB, so
throughput scales with batch size without buying it with memory.

## What durable state costs

Setting `pipeline.state.path` puts every batch in a DuckDB transaction. That
transaction is one fsync per batch, so its cost per message falls as batches
grow. Same pipeline, same 300,000 messages, state in memory against state on
disk:

| `batch_size` | State in memory | Durable state | Cost |
|---|---|---|---|
| 500 | ~95,600 msgs/sec | ~51,100 msgs/sec | 47% |
| 2000 | ~184,600 msgs/sec | ~130,600 msgs/sec | 29% |
| 5000 | ~201,600 msgs/sec | ~171,500 msgs/sec | 15% |

Use a batch of at least 5000 for a stateful pipeline. Below 2000 the commit
dominates and you pay for durability on every message instead of amortising it
across a batch.

Memory is unchanged: peak working set stayed within 191-218 MiB across every
run above, with and without state. Durability costs throughput, not memory.

`state_commit_latency` reports the per-batch commit time on the metrics
endpoint, so you can see this cost on your own hardware rather than inferring
it from the table.

Reproduce both arms:

```
make benchmark-container NUM_MESSAGES=300000 BATCH_SIZE=5000 \
    CONFIG=dev/config/examples/benchmark.stateful.mem.yml
STATE_PATH=/tmp/bench-state.db make benchmark-container NUM_MESSAGES=300000 BATCH_SIZE=5000 \
    CONFIG=dev/config/examples/benchmark.stateful.mem.yml
```

Delete the state file between runs. A second run resumes from the first run's
offsets and consumes nothing, which reports a meaningless number rather than
failing.

To reproduce:

```
make start-backing-services
make benchmark-container NUM_MESSAGES=300000 BATCH_SIZE=5000
make benchmark-container NUM_MESSAGES=300000 BATCH_SIZE=5000 \
    CONFIG=dev/config/examples/benchmark.inferred.mem.yml
```

## Benchmarks must run inside the docker network

**Docker Desktop's host→container port-forwarding caps Kafka fetches at roughly
10-15 MB/s.** That starves the pipeline and understates throughput by about
**10x** — you will measure the NAT, not the engine. `make benchmark-container`
builds a linux sqlflow and runs it on the same docker network as the broker,
which is the only way to get a number that reflects the engine.

`make benchmark` runs the same workload from the host. It is fine for a quick
smoke test, but do not quote its numbers.

# Building release binaries

```
make release-binaries        # artifacts land in dist/
```

sqlflow **cannot be cross-compiled the usual way**, and it is worth
understanding why before you try. The ADBC driver manager is a cgo package:

- `CGO_ENABLED=0` does not merely produce a degraded binary, it **fails to
  compile**: `internal/duckdb/open.go:36:20: undefined: drivermgr.Driver`.
- `CGO_ENABLED=1 GOOS=linux go build` on a mac hands the C files to the host
  clang, which cannot target linux, and the build dies in `runtime/cgo`.

So each target needs a C toolchain for that target, and the matrix is built
three different ways:

| Target | How it is built | Host requirement |
|---|---|---|
| `linux/amd64` | `docker run --platform linux/amd64` | docker (+ binfmt/qemu if the host is arm64) |
| `linux/arm64` | `docker run --platform linux/arm64` | docker (+ binfmt/qemu if the host is amd64) |
| `darwin/arm64` | native `go build` | macOS + Xcode command line tools |
| `darwin/amd64` | `go build` with `-arch x86_64` | macOS + Xcode command line tools |

**A macOS host with docker produces all four.** A **linux host produces only the
two linux targets** — darwin binaries would need a macOS SDK and an
osxcross-style toolchain, which this repo deliberately does not ship. Targets
that cannot be built on the current host are reported as skipped, not faked.
Windows is not a target.

The resulting binaries are dynamically linked and dlopen libduckdb; they are not
standalone. See [Installation](#installation).

# Publishing the release image

`make sqlflow-image` builds for the host architecture only, which is fine for
local testing and wrong for publishing. Releases go out through
`make release-image`, which builds `linux/amd64` and `linux/arm64` and pushes
both under one manifest:

```
git tag -a v1.0.4 -m "..." && git push origin v1.0.4
make test-image        # functional tests against the image
make release-image     # multi-arch build + push, tags latest too
make release-image-verify
```

Tag first: `VERSION` comes from `git describe`, so an untagged `main` yields
`v1.0.3-1-gabc1234` rather than a release version. The target refuses to run on
a dirty tree or an untagged `HEAD` for that reason.

| Variable | Default | Purpose |
|---|---|---|
| `RELEASE_PLATFORMS` | `linux/amd64,linux/arm64` | Architectures to build |
| `RELEASE_LATEST` | `1` | Also tag `latest`; set `0` when re-publishing an older tag |
| `RELEASE_OUTPUT` | `--push` | Set `--output=type=cacheonly` for a dry run that publishes nothing |
| `SQLFLOW_IMAGE` | `turbolytics/sql-flow:$(VERSION)` | Full image reference |

`make release-image-verify` reads the registry back and runs the published
image on each architecture: it fails unless the version tag carries both
architectures, `latest` resolves to the identical manifest digest, and
`sqlflow version` on each platform reports the tag. A single-arch publish, a
`latest` left on an older release, or an emulated build that never actually
ran all look fine locally and are only visible from outside.

The foreign architecture builds under QEMU emulation, so expect the `amd64`
`go build` to take several minutes on an arm64 host — `CGO_ENABLED=1` is
required for the ADBC driver manager, which rules out cross-compiling. Each
platform fetches its own libduckdb, because `scripts/install-libduckdb.sh`
branches on `uname -m` and sees the target architecture.

Publishing is a manual step from a workstation; CI builds and tests the image
on every push but does not push to the registry.

> **Why this target exists:** `v1.0.0` was published by hand from a mac with a
> plain `docker build`, so it went out **arm64-only** and did not run on amd64
> at all. `docker tag` + `docker push` has the same failure mode — it flattens a
> manifest list down to one architecture. To point an existing tag at another
> release, copy the manifest instead:
> `docker buildx imagetools create -t turbolytics/sql-flow:latest turbolytics/sql-flow:v1.0.4`.

# Examples

Additional examples are available in the wiki: [Tutorials](https://github.com/turbolytics/sql-flow/wiki/Tutorials).
Every example config lives in [`dev/config/examples/`](dev/config/examples/).

### Consume Bluesky Firehose

Running SQL against the [Bluesky firehose](https://docs.bsky.app/docs/advanced-guides/firehose) is a single configuration file:

<img width="1280" alt="bluesky firehose config" src="https://github.com/user-attachments/assets/86a46875-3cfa-46d3-ab08-1457c29115d9" />

The following command starts a bluesky consumer and prints every post to stdout:

```
./bin/sqlflow run -c dev/config/examples/bluesky/bluesky.raw.stdout.yml
```

![output](https://github.com/user-attachments/assets/185c6453-debc-439a-a2b9-ed20fdc82851)

[Checkout the configuration files here](dev/config/examples/bluesky)

### Stream Kafka to Iceberg

The following configuration writes to an Iceberg table using a local SQLite catalog:

- Initialize the SQLite iceberg catalog and test table. `PYICEBERG_HOME` points
  at the directory holding `.pyiceberg.yaml`, which defines the `sqlflow_test`
  catalog the example config expects:
```
PYICEBERG_HOME=$(pwd)/dev/config/iceberg python3 cmd/setup-iceberg-local.py setup
created default.city_events
created default.bluesky_post_events
Catalog setup complete.
```

- Start Kafka Locally
```
docker-compose -f dev/kafka-single.yml up -d
```

- Publish Test Messages to Kafka
```
python3 cmd/publish-test-data.py --num-messages=5000 --topic="input-kafka-mem-iceberg"
```

- Run sqlflow, which reads from Kafka and writes to the iceberg table locally
```
PYICEBERG_HOME=$(pwd)/dev/config/iceberg \
  ./bin/sqlflow run -c dev/config/examples/kafka.mem.iceberg.yml --max-msgs=5000
```

- Verify iceberg data was written by querying it with duckdb
```
$ duckdb -c "select count(*) from '/tmp/sqlflow/warehouse/default.db/city_events/data/*.parquet';"
┌──────────────┐
│ count_star() │
│    int64     │
├──────────────┤
│         5000 │
└──────────────┘
```

# Migrating from the Python engine

SQLFlow began as a Python engine. It is **deprecated and no longer
maintained**; v1 is the Go engine described above, and it reads the same
configuration files. Move an existing pipeline by swapping the image or binary
— the config, the templating and the DuckDB SQL carry over. What changes:

- **UDFs are not supported.** The Python engine took Python UDFs via a `udfs:`
  block. Define the function in DuckDB instead — a macro, an extension, or an
  `ATTACH`ed database that provides it. A `udfs:` block is a hard error naming
  the functions, rather than a silent skip that would later surface as an
  opaque binder error.
- **The command line is accepted as it was.** `run pipeline.yml
  --max-msgs-to-process=N` works unchanged; `-c` and `--max-msgs` are the
  native spellings.
- **Console output** is one JSON object per line rather than a Python list of
  dicts. Same rows, different rendering.
- **An empty batch** produces no output; the Python engine raised.
- **`StructuredBatch`** truncates its table every batch; the Python engine did
  not.
- **Log format** is zap's console format rather than Python logging's, and
  `SQLFLOW_LOG_LEVEL` accepts Python's level names too.
- **DuckDB** is whatever `libduckdb` you install (the image pins 1.5.x); the
  Python engine pinned 1.3.1. "Same SQL, same result" is not guaranteed across
  that gap.

The Python source stays in the repository under `sqlflow/` for reference, with
`Dockerfile.python` and `make docker-image` (tagged `python-<sha>`) to reproduce
an old image. Do not publish a bare version tag from it. The Python test suite
(`make test-unit`, `make test-integration`) is frozen with it.

# Development

```
make sqlflow        # build bin/sqlflow
make test-go        # build, vet, gofmt check, unit tests
make test-image     # build the image and run tests/release against it
```

`make test-go` and `make test-image` are what CI runs on every push.
Kafka-backed integration tests are deliberately excluded from `test-go`; they
run from the dev stack. Backing services for local development:

```
make start-backing-services
make stop-backing-services
```

# Contact Us

Like SQLFlow? Use SQLFlow? Feature Requests? Please let us know! danny@turbolytics.io
