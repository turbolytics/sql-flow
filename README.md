# SQLFlow: DuckDB for Streaming Data.

[Quickstart](#quick-start-getting-started-in-5-minutes) | [Tutorials](https://sql-flow.com/docs/category/tutorials) | ![Docker Pulls](https://img.shields.io/docker/pulls/turbolytics/sql-flow) | [Documentation](https://sql-flow.com)

SQLFlow is a stream processing engine that lets you define pipelines with just SQL. It consumes a stream, runs DuckDB SQL over each batch, and writes the result out. Think of it as a lightweight, single-binary Flink.

- Sources: [Kafka](https://kafka.apache.org/), WebSockets, webhooks.
- Sinks: Kafka, ClickHouse, Iceberg, the console, or anything DuckDB can `COPY` to (PostgreSQL, S3, parquet, MotherDuck, DuckLake).
- Built on [DuckDB](https://duckdb.org/) and [Apache Arrow](https://arrow.apache.org/): ~900k messages/sec on a laptop, in about a quarter GiB of memory.
- One Go binary, one Docker image, one YAML file per pipeline. No cluster.
- [Coverage and invariant matrix](docs/coverage/matrix.md): what is tested, and
  separately, what is *proven* — regenerated from the suites on every push.

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

4. Publish test messages. The publisher is a Python script. [uv][uv] reads
   `uv.lock` and builds the environment on first run:

```
uv run python cmd/publish-test-data.py --num-messages=10000 --topic="input-simple-agg-mem"
```

[uv]: https://docs.astral.sh/uv/

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
{"city":"San Francisco","city_count":177}
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
| `serve` | Serve a config's named SQL datasets over HTTP |
| `validate` | Check a pipeline offline and report every fault at once |
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

### `sqlflow serve`

Serves named SQL over HTTP. A serve config declares datasets, each a name and
a fixed SQL statement with typed parameters. `serve` attaches the data through
the config's `commands`, then answers each request by binding its parameters
into that statement. Nothing in a request becomes SQL text.

```
sqlflow serve <config> [flags]
sqlflow serve -c <config> [flags]
```

| Flag | Default | Description |
|---|---|---|
| `-c`, `--config` | *(required)* | Path to the serve config, unless given positionally |
| `--pprof` | `false` | Serve pprof on `:6060` |

A serve config is its own file, with `commands` and `serve` and no
`pipeline`. `sqlflow validate` checks it against the serve schema, and
`sqlflow config example --serve` prints every key. This one serves a Postgres
view:

```yaml
commands:
  - name: pin the session timezone
    sql: SET TimeZone='UTC';
  - name: load postgres
    sql: |
      INSTALL postgres;
      LOAD postgres;
  - name: attach postgres read-only
    sql: ATTACH '{{ SQLFLOW_POSTGRES_URI }}' AS pg (TYPE POSTGRES, READ_ONLY);

serve:
  http:
    addr: "0.0.0.0:8080"
    cors:
      allowed_origins: [https://example.com]
  clients:
    - name: demo-page
      id: "{{ SQLFLOW_SERVE_CLIENT_ID }}"
  limits:
    max_rows: 10000
    timeout_seconds: 10
  pool:
    # Requests answered at once. Each session is a backend session; a
    # concurrent query costs a few MiB. Omit for the default, 4.
    size: 4
  metrics:
    # Serve GET /metrics on this listener, without a client id. Off by default:
    # the listener is public and the labels name every dataset.
    enabled: false
  datasets:
    - name: posts_by_lang
      description: Posts per bucket per language.
      params:
        - {name: since, type: timestamp}
        - {name: lang, type: string}
      grains:
        1h:
          sql: |
            SELECT bucket, lang, posts FROM pg.posts_per_hour_by_lang
            WHERE bucket >= coalesce($since, now() - INTERVAL '7 days')
              AND lang = coalesce($lang, lang)
            ORDER BY bucket, lang
```

Parameters are `string`, `integer`, or `timestamp`. A timestamp is RFC 3339
with an offset; encode a `+` as `%2B` in a URL. An absent parameter binds
`NULL`, so default it in SQL with `coalesce`. Every statement must use every
declared parameter. A dataset has either `sql` or `grains`, and a request
names a grain with `?grain=`.

An `integer` param may declare `min`, `max`, or both. A request outside them
is `400 invalid_param`, and the message names the bounds. Nothing is clamped:
a clamp would answer `200` with less than the caller asked for. `/v1/datasets`
lists the bounds with the param.

```yaml
      params:
        - {name: top, type: integer, min: 1, max: 20}
```

A dataset with grains can declare a time range instead, and let the server
choose the grain:

```yaml
    - name: posts_by_lang
      params:
        - {name: since, type: timestamp}
        - {name: until, type: timestamp}
        - {name: lang, type: string}
      range: {since: since, until: until, default: 24h}
      grains:
        5m:
          max_range: 1d
          sql: |
            SELECT bucket, lang, posts FROM pg.posts_per_5m_by_lang
            WHERE bucket >= $since AND bucket < $until
              AND lang = coalesce($lang, lang)
        1h:
          max_range: 14d
          sql: ...
        1d:
          max_range: 365d
          sql: ...
```

`range` names the two `timestamp` params that bound the range, and `default`
is the width a request gets without `since`. Each grain's `max_range` is the
widest range it serves. For each request:

1. `until` defaults to the time the request arrived, and `since` to `until`
   minus `default`.
2. Without `grain`, the server picks the grain with the narrowest
   `max_range` that covers `until − since`. Six hours gets `5m`, three days
   `1h`, ninety days `1d`. A named grain answers when its `max_range` covers
   the range.
3. The server binds the resolved `since` and `until`, never `NULL`, so the
   SQL filters on them directly with no `coalesce`.
4. The response carries the grain it chose and the range it resolved:
   `"grain": "1h", "range": {"since": "…", "until": "…"}`.

A range wider than the named grain's `max_range`, or wider than every
grain's, is `400 range_too_wide`, and the message names the grains that fit.
Nothing is cut from the left of a chart without saying so. Durations are a
whole number and one unit: `s`, `m`, `h` or `d`.

#### Caching

Every request runs its query unless the dataset opts in:

```yaml
serve:
  cache:
    max_mb: 16              # optional. Bounds the cache; enables nothing
  datasets:
    - name: pipeline_status
      cache: {ttl_seconds: 15}
      sql: ...
    - name: posts_by_lang
      cache: {ttl_seconds: 30}
      range: {since: since, until: until, default: 24h}
      grains:
        5m:
          bucket: 5m        # how wide one bucket of this grain is
          max_range: 1d
          sql: ... WHERE bucket >= $since AND bucket < $until
        1d:
          bucket: 1d
          cache: {ttl_seconds: 600}   # optional: this grain's own TTL
          max_range: 365d
          sql: ...
```

A cached answer is the encoded result, served for at most `ttl_seconds` after
the query that produced it **started**. Nothing is invalidated, because
`serve` is never told that the backend changed: after a backfill, wait one TTL
or restart. `max_mb` is what every cached dataset shares, 16 by default. The
least recently used answer goes first, after every expired one, and an answer
over a quarter of the bound is returned and not kept.

A grain may carry a `cache` block of its own, and its `ttl_seconds` replaces
the dataset's for that grain. Use it on the coarse grains. A year of days
changes by one open bucket, and its rounded `until` only moves at midnight, so
the TTL is the only thing that re-runs the widest query there is: at the
dataset's 30 seconds, twice a minute, for a change no chart can show. The
dataset's block is still the opt-in, and a grain's block without it is
refused.

A dataset with a range declares `bucket` on every grain. `since` and `until`
are rounded **up** to it, the statement binds the rounded values, and `range`
echoes them, so every request inside one bucket-wide window is one question
with one answer. A chart polled by a hundred tabs runs its query once per
window, not once per tab.

**The rounding returns the same rows only for SQL like the above**: `since`
and `until` compared with a column whose values sit on bucket boundaries,
half-open, `>= $since AND < $until`. Two ways to break it, and `serve` cannot
detect either, which is why caching is the author's claim and off by default:

- `bucket <= $until` admits the bucket that starts at the rounded `until`.
- Filtering a raw timestamp, `event_at >= $since`, drops the events between
  `since` and the boundary it rounds up to.

`sqlflow rollup serve` writes `bucket` on every grain and SQL of the right
shape. `bucket` must divide one day, so a week is refused: weeks start on a
Monday and the epoch was a Thursday.

Concurrent requests for one question run one query. That query finishes even
if every caller gives up, because the engine would have finished it anyway,
and its answer is the next caller's hit.

A cached dataset's response says what happened:

```
"cache":"hit","age_ms":12400,"queued_ms":0,"elapsed_ms":0
```

`cache` is `miss` when this request ran the query, `hit` when the answer came
from memory, and `shared` when it waited on a query another request had
started. `age_ms` is how long ago that query started. `queued_ms` and
`elapsed_ms` are this request's own, so a hit reports zero and not the
original query's time. The request log carries `cache` too, and
`/v1/datasets` lists each dataset's `ttl_seconds` and each grain's `bucket`.

Responses stay `Cache-Control: no-store`: the cache is inside `serve`, and
nothing between it and the caller holds an answer. Each instance holds its
own, so two behind a load balancer may answer up to one TTL apart.

With `serve.metrics.enabled`, a server with a cached dataset adds four
metrics: `sqlflow_serve_cache_requests_total` by `dataset` and `outcome`,
`sqlflow_serve_cache_evictions_total` by `reason` (`size` or `expired`), and
the gauges `sqlflow_serve_cache_bytes` and `sqlflow_serve_cache_entries`.
Expired answers are reclaimed when the next one is stored, so after a quiet
spell the gauges still count them. `sqlflow_serve_query_duration_seconds`
counts only requests that ran a query; a hit shows in
`sqlflow_serve_request_duration_seconds`.

`dev/config/serve/local.cached.yml` caches one dataset and says why it does
not cache the other.

Routes, all `GET`:

| Route | `client_id` | Returns |
|---|---|---|
| `/healthz` | none | `200` `{"status":"ok"}` when a session answers `SELECT 1`; `503` `{"status":"busy"}` when none is free, `{"status":"unavailable"}` when the query fails. `HEAD` is answered too, for monitors |
| `/metrics` | none | Prometheus text, only when `serve.metrics.enabled` is set |
| `/v1/datasets` | required | Every dataset: its params, and its SQL as written |
| `/v1/datasets/{name}` | required | Rows |

```
$ curl 'localhost:8080/v1/datasets/posts_by_lang?client_id=<id>&grain=1h&lang=en'
{"dataset":"posts_by_lang","grain":"1h",
 "columns":[{"name":"bucket","type":"TIMESTAMP WITH TIME ZONE"},...],
 "rows":[{"bucket":"2026-09-10T00:00:00Z","lang":"en","posts":102340}],
 "row_count":1,"truncated":false,"queued_ms":0,"elapsed_ms":41}
```

A zoned timestamp is UTC. A decimal is a string of its exact digits. `NaN`
and infinities are strings. `truncated: true` means `max_rows` cut the result.

`elapsed_ms` is the query. `queued_ms` is how long the request waited for a
session, which is what rises when the pool is too small for the load.

Every route is `GET`, and `/healthz` also answers `HEAD`. A `HEAD` of a
dataset would run its query, borrow a session and discard the rows, so it is
refused with `405`.

A client id identifies a caller. It does not authenticate one: a browser page
ships it in plain sight, as an analytics snippet ships its site key. It names
the caller in the request log, and deleting it cuts the caller off. Do not
serve through it anything that the public may not read.

The id travels as `?client_id=<id>` and not in an `Authorization` header, for
two reasons. A header named for authorization reads as a leaked credential to
everyone who opens the page's source. A `GET` with no custom header is also a
CORS simple request, so a browser sends it without a preflight. `client_id`
is no dataset's param: it never reaches the SQL or the cache key, and a
dataset cannot declare a param of that name.

Deprecated: this release still answers `Authorization: Bearer <id>` when a
request has no `client_id`, and still reads `serve.auth.tokens`, each token as
a client whose id is the token. Both log a warning. The next release refuses
both.

Every refusal is `{"error": {"code", "message"}}`:

| Status | Code |
|---|---|
| `400` | `unknown_param`, `invalid_param`, `missing_grain`, `unknown_grain`, `range_too_wide` |
| `401` | `unauthorized` |
| `404` | `unknown_dataset`, `not_found` |
| `405` | `method_not_allowed` |
| `500` | `query_failed`. DuckDB's error goes to the server log, with passwords redacted, never to the caller |
| `504` | `query_timeout` |

At start, `serve` prepares every statement, so a missing table or a syntax
error stops the process with `user.sql.invalid` rather than failing the first
request. `rate_limit` is reserved: the config accepts the key, and a non-zero
value stops the process with `user.config.serve_reserved`.

What to know before you deploy it:

- **Attach `READ_ONLY`.** `serve` runs the dataset SQL as written. A write in
  it runs too, unless the attachment refuses.
- **Aggregate in the backend.** DuckDB pushes filters and projections into an
  attached Postgres and runs `GROUP BY` itself, so a rollup over raw rows
  copies every row to DuckDB first. `sqlflow rollup` keeps pre-aggregated
  tables in Postgres and generates the datasets that read them. A Postgres
  view with the `GROUP BY` also pushes the filter in, but re-aggregates the
  range on every request.
- **Bound Postgres connections.** An attachment opens up to
  `pg_connection_limit` connections, 64 by default. Set it low for a small
  database, as a command. The pool does not multiply it: with eight sessions
  scanning at once and a limit of four, the measured peak was four, so the
  connections are shared across sessions rather than opened per session.
- **A timeout stops reading, not always the query.** At the deadline the
  caller gets `504`, and the reader stops at the next batch and is released,
  which is ADBC's equivalent of cancelling. An operator that runs long before
  yielding a batch still runs to the end, holding its session. Bound the part
  that is usually slow in the backend instead: a libpq connection string takes
  `options='-c statement_timeout=30000'`, so an attached Postgres enforces its
  own ceiling.
- **A pool serves requests.** `serve.pool.size` sessions answer at once, four
  by default. A request waits for a free session, and that wait counts toward
  the dataset's timeout, so an exhausted pool answers `504 query_timeout`.
  `queued_ms` in the response and `sqlflow_serve_session_wait_seconds` in the
  metrics say whether the pool is the limit.

### `sqlflow rollup`

Generates rollup tables in Postgres, the triggers that keep them current, and
the `serve` datasets that read them, from one declaration. The pipeline keeps
writing its finest grain; the database keeps every coarser grain as each write
commits, and `serve` reads pre-aggregated rows.

```
sqlflow rollup ddl   -c rollups.yml [--backend postgres]
sqlflow rollup serve -c rollups.yml [--dataset NAME]
sqlflow rollup check -c rollups.yml --migration FILE --serve FILE
```

| Command | Does |
|---|---|
| `ddl` | Prints a migration: one table per dimension set and grain, the functions and triggers that keep them current, and a backfill. Connects to nothing. |
| `serve` | Prints the `serve` datasets, to paste into a serve file's `datasets`. |
| `check` | Exits `10` when the migration or the serve file differs from what the declaration generates, or when a dataset could answer more rows than its `max_rows`. |

`sqlflow rollup` writes and checks the files; applying the migration is your
migration runner's job. [`dev/config/rollups/bluesky.yml`](dev/config/rollups/bluesky.yml)
is a complete declaration, and `sqlflow validate` checks a rollups file
against its schema and rules.

Each grain is re-merged from the grain below it. When a statement writes the
finer table, a statement-level trigger locks the coarse buckets it touched and
recomputes them, in the writer's transaction. A minute written twice replaces
its count at every grain instead of adding to it.

Measures are `sum`, `min`, `max` and `count_buckets`, which counts the source
buckets present, such as minutes observed. `avg`, `gauge` and `histogram` are
reserved.

What to know before you deploy it:

- **Postgres 15 or later**, and a writer in `READ COMMITTED`, the default. The
  trigger refuses any other isolation level, because its lock only works when
  each statement reads a new snapshot.
- **The migration blocks the source's writers** until it commits, so no write
  lands between the triggers existing and the backfill reading. The backfill
  reads the whole source table.
- **A trigger error fails the writer's transaction.** A pipeline writing the
  source stops with it.
- **Deletes do not propagate.** Rollups outlive a retention job on the source.
- **Changes are additive.** Adding a grain or a dimension set is a new
  migration holding the regenerated script. Removing, renaming, or changing a
  measure's type needs a migration written by hand.
- **A served dataset has at most one dimension**, and folds it to its top
  values. `check` proves `max_buckets × (top.max + 1) <= max_rows`, so
  `truncated` never happens.
- **A served dataset can be cached.** `cache_ttl_seconds: 30` on a serve
  dataset generates its `cache` block, and `cache_ttl_by_grain: {1h: 120,
  1d: 600}` generates a longer one on the grains it names. Every generated grain carries its
  `bucket`, which is its name, and the generated SQL compares the bucket
  column half-open, the shape [the cache](#caching) needs. `check` holds the
  serve file to both.

### `sqlflow validate`

Checks a pipeline without running it. `validate` reaches no broker and no
sink, executes nothing from the `commands` block, and reports every fault it
finds in one pass rather than stopping at the first.

```
sqlflow validate <config> [--json]
```

It renders the config's template, then checks the result against the config
schema. Both halves of the template's variable use are reported, which is what
makes a misspelled name obvious:

```
$ sqlflow validate pipeline.yml
pipeline.yml:8:49: error: [user.config.template_undefined] template variable
  SQLFLOW_AZURE_CONNECTION_STRING is not defined and renders as an empty
  string, but a similar name is supplied and never read
    did you mean: [SQLFLOW_AZURE_STORAGE_CONNECTION_STRING]
supplied but never read: [SQLFLOW_AZURE_STORAGE_CONNECTION_STRING]
```

A variable the config reads with no `default` filter is a required input. When
it is unset, `validate` warns rather than failing, so a config still checks in
CI where no secret is set. A missing name that closely resembles a supplied one
is a different matter: that resemblance is evidence of a typo, and it fails.

`--json` emits the same report as a document, with a `checks` array beside the
diagnostics. A check that could not run reports `skipped` with a reason and
never reports `pass`, so a consumer can tell "checked and fine" from "not
checked".

Exit codes follow the error taxonomy: `0` when the config is sound, `10` for a
fault the user has to fix.

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
its description, and the accepted enum values. `--serve` prints the serve
config's skeleton instead.

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
    max_body_bytes: 26214400 # optional, default 25 MiB
    max_connections: 64 # optional, default 64
```

To send a signed event, compute an HMAC-SHA256 of the exact bytes of the body
with the shared secret, hex-encode it, and put it in the configured header
with the `sha256=` prefix:

```bash
export SQLFLOW_GITHUB_WEBHOOK_SECRET=shhh
body='{"action":"opened","number":1}'
sig="sha256=$(printf '%s' "$body" \
  | openssl dgst -sha256 -hmac "$SQLFLOW_GITHUB_WEBHOOK_SECRET" \
  | awk '{print $NF}')"

curl -s -X POST http://localhost:8001/events \
  -H 'Content-Type: application/json' \
  -H "X-Hub-Signature-256: $sig" \
  --data-binary "$body"

{"status":"received"}
```

The signature covers the raw bytes, so the body sent must be the body signed:
`--data-binary` and `printf '%s'` keep it byte for byte, where `-d` and `echo`
can alter whitespace or add a newline. GitHub signs the same way, so a
repository webhook with the same secret is accepted as is.

Responds 200 on accept, 400 for a missing signature, 403 for an invalid one,
413 for a body over `max_body_bytes`, and 408 for a body that stalls. The
body bound applies before the body is read and before the signature is
checked, so an unsigned oversized request never holds memory past it. The
default is 25 MiB, GitHub's payload ceiling.

The listener holds at most `max_connections` open connections; past that, a
new connection waits in the kernel backlog until one closes. Three fixed
timeouts release a slot: 10s for a connection to send its request headers,
60s to send the body, and 60s of idle keep-alive. A delivery that is in and
waiting on the pipeline is under no timeout.

`sqlflow config validate` checks the webhook block, including `max_body_bytes`
and `max_connections`, against the same JSON Schema `sqlflow run` loads.

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

### Sink retries

ClickHouse, Iceberg and Postgres flushes retry when the destination is not answering.
Omit the block to accept the defaults. Set `max_attempts: 1` to turn retrying
off. The Kafka sink ignores this block: franz-go already retries a produce
with its own backoff.

```yaml
sink:
  type: clickhouse
  retry:
    max_attempts: 4           # total attempts, including the first
    initial_backoff_ms: 100   # doubles each attempt
    max_backoff_ms: 2000      # ceiling on the backoff
    deadline_seconds: 10      # bounds the whole ladder, not one attempt
```

The values shown are the defaults.

Keep `deadline_seconds` below `pipeline.flush_interval_seconds`. The retry
runs inside the open state transaction, and a ladder that outlives the flush
interval freezes the window clock.

The ladder retries only what another attempt could change. The error code
decides:

| Error | Retried | Why |
| --- | --- | --- |
| Any `user.*` code, including `user.sink.encode_failed` and `user.sink.type_unsupported` | No | The config, SQL or a value is wrong. It fails identically every time. |
| `system.sink.write_failed` | No | The destination answered and refused the write. |
| `system.sink.unreachable` | Yes | The destination may come back. |
| An error with no code | Yes | A driver's timeout or reset arrives unclassified. The deadline bounds the cost. |
| Any other `system.*` code | Yes | |

A failure that is not retried keeps its own code and exit code. A retried
failure that outlasts the ladder is reported as `system.sink.unreachable`,
exit 12, and `sink_retry_count_total` counts each attempt after the first.

`user.sink.encode_failed` is the sink's client refusing a value before
anything reaches the network, such as a timestamp string the ClickHouse
driver's `DateTime` layout cannot parse. Cast or format the column in the
handler SQL. The message names the column and quotes the value.

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

## Graceful shutdown

`SIGTERM` and `SIGINT` drain the pipeline. The drain runs in order:

1. Stop consuming.
2. Write the batch it had buffered.
3. Run each window's final poll.
4. Commit state and offsets.
5. Exit 0.

A supervisor that stops a pipeline this way loses nothing. Skipping the drain
loses no data either, because the buffered batch replays from the last
committed offset. It costs that duplicate work, and it republishes any window
that closed during shutdown.

The drain has no deadline. A sink that blocks holds the process open until the
supervisor escalates to `SIGKILL`.

## Tumbling windows

A table declared under `tables.sql` can carry a `window`. The handler appends
each batch's counts to the table, keyed by a bucket start, and the engine does
the rest: it keeps a watermark, publishes every bucket the watermark has passed
to the window's sink, and deletes it.

```yaml
tables:
  sql:
    - name: agg_cities_count
      sql: |
        CREATE TABLE agg_cities_count (
          bucket TIMESTAMPTZ, city VARCHAR, count INT
        );
      window:
        time_column: bucket
        size_seconds: 3600
        grace_seconds: 0
        idle_close_seconds: 60
        late_rows: drop              # or reemit; required
        poll_interval_seconds: 10    # optional
        emit_sql: |                  # optional; default SELECT * FROM closed
          SELECT bucket, city, sum(count)::INT AS count
          FROM closed
          GROUP BY ALL
        sink:
          type: kafka
          kafka:
            brokers: [localhost:9092]
            topic: output-tumbling-window-1

pipeline:
  handler:
    type: handlers.InferredMemBatch
    sql: |
      INSERT INTO agg_cities_count BY NAME
      SELECT
        date_trunc('hour', CAST(timestamp AS TIMESTAMPTZ)) AS bucket,
        properties.city AS city,
        count(*) AS count
      FROM batch
      GROUP BY bucket, city
```

**The watermark.** One instant per window, in event time, persisted in
`sqlflow_windows`, never moving backwards. A bucket is closed when its end,
`time_column + size_seconds`, is at or before the watermark. While data
arrives the watermark is the newest bucket start the table holds, less
`grace_seconds`: a bucket closes once the stream has moved past it, so a
replay and a live run produce the same rows. The newest bucket start is what
the table holds, so a grace shorter than one bucket rounds up to one. After
`idle_close_seconds` with nothing arriving, the watermark moves past the newest
bucket and every open bucket closes. Wall clock appears nowhere in the close.

**Late rows.** A row for a bucket below the watermark arrived after that
bucket was published. `late_rows` is required, because the two policies are
different promises to the sink. `drop` deletes the row and counts it in
`window_late_rows_total`, so the sink sees each bucket once, as it closed.
`reemit` runs `emit_sql` over the late rows alone and publishes the result,
because the bucket's other rows were deleted when it closed. The sink has to
add that result to the bucket it holds. An upsert that replaces on the
bucket's key overwrites the bucket's count with the late rows' count, so pair
a replacing upsert with `drop`. An upsert that adds counts a republished close
twice; see **Guarantees**. `sqlflow validate` warns when `reemit` is paired
with the Iceberg or Kafka sink. A rising drop count means the grace is too
short for the stream.

The watermark is one value for the whole table, which makes it the fastest
partition's clock. A topic whose partitions run at uneven rates has a slow
partition whose rows arrive late, and the grace is the allowance for them.
Size it from `window_late_rows_total`.

**emit_sql** shapes the rows before the sink. It reads one relation, `closed`,
holding every row of every bucket that just closed. Cast a `sum` back to the
column's type, because DuckDB widens integers to `HUGEINT`, and use
`GROUP BY ALL`, which groups by the output columns.

**Do not put a `UNIQUE INDEX` or `PRIMARY KEY` on a window table.** DuckDB
never frees rows deleted from an indexed table. The engine deletes every
bucket it publishes, so an index grows memory without bound, with or without a
state path. See [#268](https://github.com/turbolytics/sql-flow/issues/268). A
keyed table with no window, such as a running total, can keep its index.

**Guarantees.** The window runs on a connection of its own and reads committed
rows only, so a batch that rolls back was never published. Collect, write and
flush happen before the delete, and the delete and the watermark commit
together, so a sink failure leaves the bucket in the table and stops the
process rather than dropping the bucket. A crash between the flush and that
commit republishes the bucket on the next start, so give a window sink a key
it can deduplicate on. One final poll runs on shutdown, inside the drain
deadline. With a state path the watermark survives a restart; without one the
window table and its watermark are lost together.

`sqlflow validate` checks the declaration: the time column must be declared
`TIMESTAMPTZ` in the table's `CREATE`, `emit_sql` must read `closed`, and the
old `manager` block is refused with the keys that replace it. That block and
its two predicates are gone, and a file that carries them does not run; the
declaration cannot be derived from two arbitrary predicates, so the change
is a major version with the migration in the changelog.

See [`tumbling.window.yml`](dev/config/examples/tumbling.window.yml) and
[`kafka.stateful.window.yml`](dev/config/examples/kafka.stateful.window.yml).

## Metrics

`--metrics prometheus` serves `/metrics` on `:8000`. Twenty-four instruments
are exported, all under the meter name `sqlflow` except the two webhook ones.

The instrument name and the Prometheus series name differ: the exporter appends
the unit, then `_total` for counters, and skips the unit when the name already
contains it. Only the series name is queryable, so both are listed. A test
asserts this table against the running exporter.

**Row accounting.** Four counts follow a row through the pipeline:

| Instrument | Series | Type | Attrs |
|---|---|---|---|
| `message_count` | `message_count_messages_total` | counter | — |
| `handler_rows_read` | `handler_rows_read_total` | counter | — |
| `sink_rows_accepted` | `sink_rows_accepted_total` | counter | `sink`, `role` |
| `sink_rows_written` | `sink_rows_written_total` | counter | `sink`, `role` |
| `sink_flush_num_rows` | `sink_flush_num_rows` | gauge | — |

**Sink health:**

| Instrument | Series | Type | Attrs |
|---|---|---|---|
| `sink_flush_count` | `sink_flush_count_flushes_total` | counter | `result` |
| `sink_flush_latency` | `sink_flush_latency_seconds` | histogram | — |
| `sink_retry_count` | `sink_retry_count_total` | counter | `sink` |

**Pipeline:**

| Instrument | Series | Type | Attrs |
|---|---|---|---|
| `batch_processing_latency` | `batch_processing_latency_seconds` | histogram | — |
| `error_count` | `error_count_total` | counter | `class`, `domain`, `code`, `phase` |
| `phase_duration` | `phase_duration_seconds` | histogram | `phase` |
| `handler_checkpoints_skipped` | `handler_checkpoints_skipped_total` | counter | — |

`phase_duration` decomposes batch time. It carries the same six phases
`error_count` does — `handler.write`, `handler.invoke`, `sink.write`,
`sink.flush`, `state.commit`, `handler.init` — so one query says where the time
went:

```promql
sum(rate(phase_duration_seconds_sum[5m])) by (phase)
```

Read it with `error_count` to tell slow from broken. `phase_duration` records
whether or not the phase succeeded, which no other latency here does:
`sink_flush_latency` and `state_commit_latency` record after their error
returns, so a sink that takes thirty seconds to fail leaves them flat.

`handler.write` is measured around the whole message loop rather than each
write, so it also carries the loop's own bookkeeping. Timing each write cost
4.1x on that path; see `BenchmarkConsumeLoopWritePath`.

Every latency histogram shares one set of bucket boundaries, from 100
microseconds to 60 seconds. The OTel SDK's defaults are millisecond-shaped and
these instruments record seconds, so before this every `histogram_quantile`
over them returned a number under five seconds and meant nothing.

`handler_checkpoints_skipped` counts batches whose checkpoint the structured
handler skipped. That handler checkpoints after each batch to reclaim the rows
it truncated. DuckDB refuses while another connection holds an uncommitted
update or DDL, such as a window saving its watermark, and the next batch
reclaims them instead. A count that rises with every batch means a write stays
open for longer than a batch, and memory grows until it closes.

**Source:**

| Instrument | Series | Type | Attrs |
|---|---|---|---|
| `source_read_latency` | `source_read_latency_seconds` | histogram | — |
| `consumer_lag` | `consumer_lag_messages` | gauge | `topic`, `partition` |

**Reference tables**, recorded once at startup for each table the handler SQL
joins, with or without a state path:

| Instrument | Series | Type | Attrs |
|---|---|---|---|
| `reference_table_rows` | `reference_table_rows` | gauge | `table` |

**Windows**, one set per declared window:

| Instrument | Series | Type | Attrs |
|---|---|---|---|
| `window_watermark_seconds` | `window_watermark_seconds` | gauge | `window` |
| `window_closed` | `window_closed_total` | counter | `window` |
| `window_late_rows` | `window_late_rows_total` | counter | `window`, `policy` |

Wall time minus `window_watermark_seconds` is how far the stream's clock
trails, which is the number to size `grace_seconds` from.

**Durable state**, present only when the pipeline declares a state path. An
absent series and an empty state are different facts, so a pipeline with state
in memory reports nothing rather than zero:

| Instrument | Series | Type | Attrs |
|---|---|---|---|
| `state_commit_count` | `state_commit_count_commits_total` | counter | `result` |
| `state_commit_latency` | `state_commit_latency_seconds` | histogram | — |
| `state_db_size_bytes` | `state_db_size_bytes` | gauge | — |
| `state_table_rows` | `state_table_rows` | gauge | `table` |

**Webhook source**, under the meter `sqlflow.sources.http` and present only with
a webhook source:

| Instrument | Series | Type | Attrs |
|---|---|---|---|
| `webhook_requests_total` | `webhook_requests_total` | counter | `status_code` |
| `webhook_request_duration_seconds` | `webhook_request_duration_seconds` | histogram | `status_code` |

```
$ sqlflow run -c <config> --metrics=prometheus &
$ curl -s localhost:8000/metrics | grep message_count
message_count_messages_total{otel_scope_name="sqlflow",...} 154635
```

`consumer_lag` is the one to alert on. It is the broker's high watermark minus
the offset the pipeline has finished with, so it measures the work the
pipeline still owes rather than what its consumer group has been told.

### Reading the row counts

Each adjacent ratio isolates one kind of loss:

- `handler_rows_read` over `message_count` — parse and DLQ loss. A message the
  handler rejected never reaches the SQL.
- `sink_rows_accepted` over `handler_rows_read` — whatever the SQL does. A join
  that drops, a `WHERE`, a `GROUP BY`. Its meaning depends on the pipeline,
  which is why sqlflow reports the numbers and leaves the threshold to you.
- `sink_rows_written` over `sink_rows_accepted` — delivery loss. A ratio that
  stays below 1 is a sink that is not draining.

The `role` attribute separates the pipeline sink from the DLQ and from a window
manager's sink. Sum across roles and a rejected record counts as a delivered
one.

**Every ratio is a floor, not an equality.** sqlflow is at-least-once: a crash
between the flush and the offset commit replays the batch, and the sink writes
those rows again, so a ratio can exceed 1 after a normal recovery. Alert on a
ratio that is low. Never alert on one that is not exactly 1, or a healthy
restart pages somebody.

The sink's buffer depth is `sink_rows_accepted_total - sink_rows_written_total`.
There is no gauge for it: a counter difference gives the depth and a rate,
and it cannot misreport itself the way a sink's own count can.

An enrichment pipeline joining a dimension table that never loaded shows up
here as `sink_rows_accepted` collapsing against `handler_rows_read` — and at
startup, as a warning naming the empty table.

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
PYICEBERG_HOME=$(pwd)/dev/config/iceberg uv run python cmd/setup-iceberg-local.py setup
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
uv run python cmd/publish-test-data.py --num-messages=5000 --topic="input-kafka-mem-iceberg"
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

# Upgrading from a pre-v1 release

SQLFlow was written in Python before v1. That implementation has been removed.
v1 reads the same configuration files, so moving a pipeline means swapping the
image or binary: the config, the templating and the DuckDB SQL carry over.

Seven things changed:

- **UDFs are not supported.** Pre-v1 took Python UDFs through a `udfs:` block.
  Define the function in DuckDB instead — a macro, an extension, or an
  `ATTACH`ed database that provides it. A `udfs:` block is now a hard error
  naming the functions, rather than a silent skip that surfaces later as an
  opaque binder error.
- **The command line is accepted as it was.** `run pipeline.yml
  --max-msgs-to-process=N` works unchanged. `-c` and `--max-msgs` are the
  native spellings.
- **Console output** is one JSON object per line rather than a Python list of
  dicts. Same rows, different rendering.
- **An empty batch** produces no output. It used to raise.
- **`StructuredBatch`** truncates its table every batch. It used to keep it.
- **Log format** is zap's console format. `SQLFLOW_LOG_LEVEL` still accepts
  Python's level names.
- **DuckDB** is whatever `libduckdb` you install, and the image pins 1.5.x.
  Pre-v1 pinned 1.3.1, so "same SQL, same result" is not guaranteed across that
  gap.

Published `python-<sha>` tags remain on Docker Hub. Reproducing one means
checking out a commit from before the removal.

The `sqlflow/` directory still exists, and holds test harness rather than an
engine: `settings.py`, `kafka.py`, `fixtures/` and `logging.py`. The image
tests under `tests/release` and the dev scripts in `cmd/` import them.

# Development

```
make sqlflow        # build bin/sqlflow
make test-go        # build, vet, gofmt check, unit tests
make test-image     # build the image and run tests/release against it
```

`tests/release` and the coverage matrix are Python. Both run through [uv][uv],
which builds the environment from `uv.lock` on first use. There is no
`pip install` step, and no dependency is resolved at install time.

The matrix generator is `scripts/coverage_matrix/`, one module per concern
with a test file each under `tests/tooling/`. Its own docstring lists them.

[**Coverage and invariant matrix**](docs/coverage/matrix.md) — what is tested,
and separately, what is proven. It is generated from the suites on every push
and gates the merge.

What is committed is one status per feature and per invariant, under
`docs/coverage/status/`. Adding a test inside a feature that is already
covered changes none of it. The page is rendered from those files and the
registries alone, so `make coverage-page` regenerates it in a second, with no
Docker and no test run. Test names and counts are in the coverage report CI
publishes on every run.

[**Contributing**](CONTRIBUTING.md) — the verification a pull request carries,
including the ten-minute memory soak (`make soak`) required of any change that
allocates per message or per request.

`make test-go` and `make test-image` are what CI runs on every push.
Kafka-backed integration tests are deliberately excluded from `test-go`; they
run from the dev stack. Backing services for local development:

```
make start-backing-services
make stop-backing-services
```

# Contact Us

Like SQLFlow? Use SQLFlow? Feature Requests? Please let us know! danny@turbolytics.io
