# SQLFlow: DuckDB for Streaming Data.

[Quickstart](#quick-start-getting-started-in-5-minutes) | [Tutorials](https://sql-flow.com/docs/category/tutorials) | ![Docker Pulls](https://img.shields.io/docker/pulls/turbolytics/sql-flow) | [Documentation](https://sql-flow.com)

SQLFlow is a high-performance stream processing engine that simplifies building data pipelines by enabling you to define them using just SQL. Think of SQLFlow as a lightweight, modern Flink.

Key Features:
- Process data from [Kafka](https://kafka.apache.org/), WebSockets, and webhooks.
- Write outputs to Kafka topics, ClickHouse, Iceberg, or anything DuckDB can `COPY` to (PostgreSQL, S3, parquet, MotherDuck, DuckLake).
- Built on [DuckDB](https://duckdb.org/) and [Apache Arrow](https://arrow.apache.org/) for high-speed processing.

## Two engines, one config spec

This repository ships **two** implementations of SQLFlow:

| | `turbine` (Go) | `sqlflow` (Python) |
|---|---|---|
| Entry point | `turbine` binary (`cmd/turbine/`) | `python cmd/sql-flow.py` |
| Source tree | `internal/` | `sqlflow/` |
| Docker image | built from `Dockerfile.turbine` | `turbolytics/sql-flow` |
| Throughput | ~793k msgs/sec | low tens of thousands msgs/sec |
| Status | **v1, the engine to use for new pipelines** | maintained, feature-complete |

**turbine is a Go rewrite of the Python engine, and it reads the same configuration files.**
A `sqlflow.yml` written for the Python engine is intended to run unmodified on turbine
— same YAML spec, same Jinja2 templating, same JSON Schema, same DuckDB SQL.
The [Differences from the Python engine](#differences-from-the-python-engine)
section below lists every place that is not yet true.

The Python engine is still here and still documented — see
[The Python engine](#the-python-engine) at the end of this README.

Why the rewrite:

- Raw performance: ~20-60x the throughput of the Python engine on the same pipeline.
- The ability to ship a single binary to edge / IoT hardware.
- Ergonomics of background processing in Go vs Python.

# Quick Start (Getting Started in 5 Minutes)

1. Get a turbine binary. The fastest path from a clone is to build one
   (see [Installation](#installation) for prebuilt binaries and Docker):

```
make turbine
```

2. Set up the local development environment:

```
make setup-dev
```

3. Validate your pipeline against test data, without a broker. `dev invoke` runs
   the config's handler over a JSONL fixture and prints the result:

```
./bin/turbine dev invoke dev/config/examples/basic.agg.mem.yml dev/fixtures/simple.json

{"city":"New York","city_count":28672}
{"city":"Baltimore","city_count":28672}
```

4. Start Kafka locally using docker:

```
docker-compose -f dev/kafka-single.yml up -d
```

5. Publish test messages to Kafka:

```
python3 cmd/publish-test-data.py --num-messages=10000 --topic="input-simple-agg-mem"
```

6. Start a Kafka consumer from inside the docker-compose container, to verify SQLFlow output:

```
docker exec -it kafka1 kafka-console-consumer --bootstrap-server=kafka1:9092 --topic=output-simple-agg-mem
```

7. Run turbine against the stream:

```
./bin/turbine run -c dev/config/examples/basic.agg.mem.yml --max-msgs=10000
```

- Verify output in the Kafka consumer:

```
{"city":"San Fransisco","city_count":177}
{"city":"New York","city_count":236}
{"city":"Miami","city_count":203}
{"city":"Baltimore","city_count":180}
```

You just ran SQLFlow against a stream of Kafka data!

> **Note the flag.** turbine takes its config as `-c/--config`, not as a
> positional argument, and the message cap is `--max-msgs` (the Python engine
> uses `run <config> --max-msgs-to-process`). See
> [CLI differences](#cli-differences).

# Installation

turbine reaches DuckDB through the Arrow ADBC driver manager, which **dlopens
`libduckdb` at runtime**. The binary is therefore not standalone: wherever you
run it, that shared library has to be present. `SQLFLOW_DUCKDB_LIB` points at
it; without that variable turbine looks in
`/opt/homebrew/lib/libduckdb.dylib` on macOS and `/usr/local/lib/libduckdb.so`
on Linux.

The pinned DuckDB version lives in one place, the `DUCKDB_VERSION` file.

### Docker (no libduckdb setup)

The image bakes in a matching `libduckdb.so` and sets `SQLFLOW_DUCKDB_LIB`, so
nothing else is needed. Build it from the repo:

```
make turbine-image
```

Then run a pipeline with your config and cache mounted in:

```
docker run \
  -v $(pwd)/dev:/tmp/conf \
  -v /tmp/sqlflow:/tmp/sqlflow \
  turbolytics/turbine:<tag> \
  dev invoke /tmp/conf/config/examples/basic.agg.mem.yml /tmp/conf/fixtures/simple.json
```

`make turbine-image` prints the tag it built; it is derived from `git describe`.

### Prebuilt binary

Release binaries are published for linux and macOS on amd64 and arm64. Download
the one matching your platform, then install a matching `libduckdb`:

```
chmod +x turbine_<version>_<os>_<arch>
./scripts/install-libduckdb.sh /usr/local/lib
export SQLFLOW_DUCKDB_LIB=/usr/local/lib/libduckdb.so
./turbine_<version>_<os>_<arch> version
```

`scripts/install-libduckdb.sh` **always fetches the linux `libduckdb.so`**, for
the architecture it detects. It is for linux hosts and containers only — running
it on a mac gets you a linux library that will not load.

On macOS, `brew install duckdb` puts the library at the default path
(`/opt/homebrew/lib/libduckdb.dylib`) and no environment variable is needed.

To produce the release binaries yourself:

```
make release-binaries
```

This is **not** a plain `GOOS=... GOARCH=... go build`, and it cannot be —
see [Building release binaries](#building-release-binaries).

### From source

Requires Go 1.25+, a C toolchain (cgo is mandatory), and libduckdb.

```
# macOS
brew install duckdb
make turbine

# linux
./scripts/install-libduckdb.sh /usr/local/lib
export SQLFLOW_DUCKDB_LIB=/usr/local/lib/libduckdb.so
make turbine
```

The binary lands at `bin/turbine`.

# How SQLFlow Works

SQLFlow embeds DuckDB and Apache Arrow for high performance. A pipeline has
three parts:

<img width="1189" alt="SQLFlow architecture" src="https://github.com/user-attachments/assets/1295e7eb-a0b8-4087-8aa4-cad75a0c8cfa" />

**Input Source**

SQLFlow ingests data from Kafka, WebSockets, and webhooks, modelling the input
as a stream of messages.

**Handler**

SQLFlow uses DuckDB and Apache Arrow to execute SQL against a batch of that
stream. Handlers contain the stream processing logic: filter, aggregate, enrich
or drop data.

**Output Sink**

SQLFlow writes the results of the SQL to Kafka, ClickHouse, Iceberg, the
console, or — through the `sqlcommand` sink — anywhere DuckDB can write:
PostgreSQL, S3, local parquet, MotherDuck, DuckLake.

The following image shows an example SQLFlow configuration file:

<img width="1256" alt="Example config" src="https://github.com/user-attachments/assets/3d7b8434-4f73-4a66-800b-5c1392c97d52" />

The file explicitly contains a `pipeline` configuration with a `source`, `handler` and `sink` section. This configuration file also contains commands to be executed prior to the pipeline running. These commands support things like attaching databases to the pipeline execution context.

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
turbine [command]
```

| Command | Purpose |
|---|---|
| `run` | Run a pipeline against a live source |
| `dev invoke` | Run a pipeline's handler against a static file |
| `config validate` | Validate a config against the JSON Schema |
| `config example` | Print a commented example configuration |
| `tail` | Print every message from a config's source |
| `version` | Print version, commit and Go version |

### `turbine run`

Runs the pipeline: consume, batch, execute SQL, sink, commit offsets.

```
turbine run -c <config> [flags]
```

| Flag | Default | Description |
|---|---|---|
| `-c`, `--config` | *(required)* | Path to the config file |
| `--max-msgs` | `0` | Stop after N messages; `0` is unlimited |
| `--metrics` | *(off)* | Metrics exporter. Only `prometheus` is supported; serves `/metrics` on `:8000` |
| `--stats-json` | *(off)* | Write final run stats as JSON to this path |
| `--pprof` | `false` | Serve pprof on `:6060`, and enable block/mutex profiling |

`--stats-json` writes a small object, useful for CI assertions:

```
$ turbine run -c dev/config/examples/benchmark.structured.mem.yml \
    --max-msgs=2000 --stats-json=/tmp/stats.json
...
{"messages_consumed":2000,"num_errors":0}
```

### `turbine dev invoke`

Runs the config's `commands`, `tables` and handler over a JSONL fixture and
prints the resulting rows to stdout. The sink is deliberately **not** exercised,
so this is safe to run against a production config. This is the fastest way to
iterate on SQL.

```
turbine dev invoke <config> <fixture>
```

### `turbine config validate`

Renders the config's Jinja2 template, then validates the result against the
same JSON Schema the Python engine uses.

```
$ turbine config validate dev/config/examples/basic.agg.mem.yml
dev/config/examples/basic.agg.mem.yml: valid
```

### `turbine config example`

Prints a fully commented YAML skeleton generated from the schema — every key,
its description, and the accepted enum values.

```
turbine config example
```

### `turbine tail`

Connects a config's source and prints every message to stdout, with no handler
or sink. Useful for confirming a source is configured correctly.

```
turbine tail -c <config>
```

### `turbine version`

```
$ turbine version
turbine v1.0.0
commit: f36d970
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
  flush_interval_seconds:  # optional, default 30
  on_error:                # optional
  source:                  # required
  handler:                 # required
  sink:                    # required
```

`batch_size` is how many messages accumulate before the handler runs.
`flush_interval_seconds` bounds the wait: a partial batch is flushed anyway once
the interval elapses, so a low-traffic topic still makes progress.

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

Offsets are committed manually, after the batch has been handled and the sink
has flushed.

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

Two limits, both of which fail loudly rather than silently:
`GSSAPI` is rejected, and an encrypted PEM key is rejected with instructions to
convert it (`openssl pkcs8 -topk8 -nocrypt`). `key_password` only covers
unencrypted PEMs.

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

> **Known gotcha:** the JSON Schema shipped with both engines only enumerates
> `kafka` and `websocket`, so `turbine config validate` **rejects** a webhook
> config that `turbine run` accepts. This affects the Python engine identically.

## Handlers

All three handlers take `sql`. The batch is exposed to that SQL as a table.

| `type` | Batch table | Notes |
|---|---|---|
| `handlers.InferredMemBatch` | `batch` | Schema inferred from the JSON, in memory |
| `handlers.InferredDiskBatch` | `batch` | Same, but buffered through disk via `read_json_auto` |
| `handlers.StructuredBatch` | the table named by `table` | Schema declared up front; fastest |

**Inferred** handlers derive the Arrow schema from the messages themselves:
columns come from the first message and types are promoted across the batch. A
value that cannot be promoted fails the batch.

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
zero-copy Arrow ingest:

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

# clickhouse
sink:
  type: clickhouse
  clickhouse:
    dsn: clickhouse://default:@localhost:9000/default
    table: events

# iceberg — catalog resolved from .pyiceberg.yaml, exactly as pyiceberg does
sink:
  type: iceberg
  iceberg:
    catalog_name: default
    table_name: default.city_events
```

**`sqlcommand`** is the general escape hatch, and it is how the Python engine
implements its Postgres, S3, parquet, MotherDuck and DuckLake sinks. The batch
is exposed to your SQL as the table `sqlflow_sink_batch`:

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
rather than dropping a window. One final poll runs on shutdown, so a window that
closes during shutdown is not stranded. `tumbling_window` is currently the only
manager type. See [`tumbling.window.yml`](dev/config/examples/tumbling.window.yml).

## Metrics

`--metrics prometheus` serves `/metrics` on `:8000` (the same port the Python
engine uses, so scrape configs and dashboards carry over). Seven instruments are
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

```
$ turbine run -c <config> --metrics=prometheus &
$ curl -s localhost:8000/metrics | grep message_count
message_count_messages_total{otel_scope_name="sqlflow",...} 154635
```

## Environment variables

| Variable | Purpose |
|---|---|
| `SQLFLOW_DUCKDB_LIB` | Path to `libduckdb`. Defaults per-OS as described in [Installation](#installation) |
| `SQLFLOW_SQL_RESULTS_CACHE_DIR` | Staging dir for `InferredDiskBatch`, default `/tmp/sqlflow/resultscache` |
| `SQLFLOW_STATIC_ROOT` | `STATIC_ROOT` template variable, default `/tmp/sqlflow/static` |
| `PYICEBERG_HOME`, `PYICEBERG_CATALOG__*` | Iceberg catalog resolution, same as pyiceberg |
| `SQLFLOW_*` | Anything else is injected into the config template context under its own name |

# Benchmarks

Measured on an M-series Mac, single Kafka partition, 300,000 messages of JSON
aggregated into DuckDB, `batch_size=5000`:

| Handler | Throughput | Peak RSS |
|---|---|---|
| `handlers.StructuredBatch` | **~793,000 msgs/sec** | ~316 MB |
| `handlers.InferredMemBatch` | **~261,000 msgs/sec** | ~316 MB |

Throughput scales with batch size — at `batch_size=500` StructuredBatch does
~269k msgs/sec, at `2000` ~551k msgs/sec.

For comparison, the Python engine's published numbers on similar pipelines are
in the tens of thousands of msgs/sec (see
[The Python engine](#the-python-engine)).

## Benchmarks must run inside the docker network

This matters more than any tuning flag:

```
make benchmark-container NUM_MESSAGES=300000 BATCH_SIZE=5000
```

**Docker Desktop's host→container port-forwarding caps Kafka fetches at roughly
10-15 MB/s.** That starves the pipeline and understates throughput by about
**10x** — you will measure the NAT, not the engine. `make benchmark-container`
builds a linux turbine and runs it on the same docker network as the broker,
which is the only way to get a number that reflects the engine.

`make benchmark` runs the same workload from the host. It is fine for a quick
smoke test, but do not quote its numbers.

# Differences from the Python engine

turbine targets drop-in compatibility, and the whole example config suite
renders and parses under both engines. These are the places the two genuinely
differ today.

### UDFs are not supported

The Python engine supports Python UDFs via a `udfs:` block. **turbine drops
them by design.** They belong to DuckDB — write a macro, load an extension, or
`ATTACH` a database that provides the function. A `udfs:` block is a hard error
naming the functions, rather than a silent skip that would later surface as an
opaque binder error:

```
$ turbine dev invoke dev/config/examples/udf.yml dev/fixtures/udf.jsonl
Error: udfs are not supported: parse_domain. Define them in DuckDB instead
(a macro or extension, or ATTACH a database that provides them)
```

### Sink and source gaps

- **ClickHouse sink**: nested types, decimals and intervals are rejected with an
  explicit unsupported-type error. Flat scalar schemas work.
- **Webhook source**: the fragmented-message framing differs from the Python
  implementation, and the webhook's own request metrics
  (`webhook_requests_total`, `webhook_request_duration_seconds`) are not ported.
  The seven pipeline instruments listed above are.
- **Iceberg sink**: SQL-backed catalogs (`sqlite://`) only; REST catalogs error.
- **`sink.format.type: parquet`** is parsed and ignored — on **both** engines.

### CLI differences

| | Python | turbine |
|---|---|---|
| Run | `run <config>` (positional) | `run -c <config>` (flag) |
| Message cap | `--max-msgs-to-process` | `--max-msgs` |
| HTTP SQL debug | `--with-http-debug` | not implemented |
| Tail a source | — | `tail -c <config>` |
| Version | — | `version` |
| Run stats | — | `--stats-json` |
| Profiling | — | `--pprof` |

### Behavioural notes

- **`StructuredBatch`** `TRUNCATE`s its table every batch and errors on an empty
  batch; the Python engine does neither.
- **DuckDB version**: the Python engine pins DuckDB 1.3.1; turbine loads
  whatever `libduckdb` you install (`DUCKDB_VERSION` pins 1.5.x for the image
  and benchmarks). "Same SQL, same result" is not guaranteed across that gap
  yet.
- **`dev invoke` output format**: turbine's console sink emits one JSON object
  per line; the Python engine prints a Python list of dicts. Same rows, different
  rendering.
- **`source.error.policy`** is parsed but unused. Use `pipeline.on_error`.
- **Templating** uses gonja v2 (Jinja2 for Go) rather than Jinja2 itself.
  All 24 example configs render identically, asserted by a test.
- **Log level** is not configurable yet.

# Building release binaries

```
make release-binaries        # artifacts land in dist/
```

turbine **cannot be cross-compiled the usual way**, and it is worth
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

# The Python engine

The original Python implementation still lives in this repository under
`sqlflow/`, is still maintained, and is still the engine behind the published
tutorials at [sql-flow.com](https://sql-flow.com).

- Docker image: `turbolytics/sql-flow` (built from the top-level `Dockerfile`)
- Entry point: `python cmd/sql-flow.py`
- Tutorials: [sql-flow.com/docs/category/tutorials](https://sql-flow.com/docs/category/tutorials)

Install and run:

```
pip install -r requirements.txt
pip install -r requirements.dev.txt

$ python3 cmd/sql-flow.py dev invoke dev/config/examples/basic.agg.mem.yml dev/fixtures/simple.json
[{'city': 'New York', 'city_count': 28672}, {'city': 'Baltimore', 'city_count': 28672}]

$ python3 cmd/sql-flow.py run dev/config/examples/basic.agg.mem.yml --max-msgs-to-process=10000
```

Or via Docker:

```
docker run -v $(pwd)/dev:/tmp/conf -v /tmp/sqlflow:/tmp/sqlflow \
  turbolytics/sql-flow:latest \
  dev invoke /tmp/conf/config/examples/basic.agg.mem.yml /tmp/conf/fixtures/simple.json
```

### Python engine benchmarks

[More information about benchmarks are available in the wiki](https://github.com/turbolytics/sql-flow/wiki/Benchmarks).

| Name                      | Throughput        | Max RSS Memory | Peak Memory Usage |
|---------------------------|-------------------|----------------|-------------------|
| Simple Aggregation Memory | 45,000 msgs / sec | 230 MiB        | 130 MiB           |
| Simple Aggregation Disk   | 36,000 msgs / sec | 256 MiB        | 102 MiB           |
| Enrichment                | 13,000 msgs /sec  | 368 MiB        | 124 MiB           |
| CSV Disk Join             | 11,500 msgs /sec  | 312 MiB        | 152 MiB           |
| CSV Memory Join           | 33,200 msgs / sec | 300 MiB        | 107 MiB           |
| In Memory Tumbling Window | 44,000 msgs / sec | 198 MiB        |  96 MiB           |

### Python engine UDFs

UDFs are a Python-engine-only feature; see
[Differences from the Python engine](#differences-from-the-python-engine).

# Examples

Additional examples are available in the wiki: [Tutorials](https://github.com/turbolytics/sql-flow/wiki/Tutorials).
Every example config lives in [`dev/config/examples/`](dev/config/examples/).

### Consume Bluesky Firehose

SQLFlow supports DuckDB over websocket. Running SQL against the [Bluesky firehose](https://docs.bsky.app/docs/advanced-guides/firehose) is a simple configuration file:

<img width="1280" alt="bluesky firehose config" src="https://github.com/user-attachments/assets/86a46875-3cfa-46d3-ab08-1457c29115d9" />

The following command starts a bluesky consumer and prints every post to stdout:

```
./bin/turbine run -c dev/config/examples/bluesky/bluesky.raw.stdout.yml
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

- Run turbine, which reads from Kafka and writes to the iceberg table locally
```
PYICEBERG_HOME=$(pwd)/dev/config/iceberg \
  ./bin/turbine run -c dev/config/examples/kafka.mem.iceberg.yml --max-msgs=5000
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

# Development

```
make turbine        # build bin/turbine
make test-go        # build, vet, gofmt check, unit tests (Go engine)
make test-unit      # Python engine unit tests
make test-integration
```

`make test-go` is what CI runs against the Go engine. Kafka-backed integration
tests are deliberately excluded from it.

Backing services for local development:

```
make start-backing-services
make stop-backing-services
```

# Contact Us

Like SQLFlow? Use SQLFlow? Feature Requests? Please let us know! danny@turbolytics.io
