# Changelog

## Unreleased

### Changed

- `sqlflow serve` takes the caller's id as `?client_id=<id>`, and the config
  declares callers under `serve.clients`, each with a `name` and an `id`. The
  id was an `Authorization: Bearer` token under `serve.auth.tokens`. It never
  authenticated anyone: a browser page ships it in plain sight. Sent as a
  bearer token, it read as a leaked credential to anyone who opened the page's
  source. A request with no `Authorization` header is also a CORS simple
  request, so a browser no longer sends a preflight before it. `client_id`
  reaches neither the SQL nor the cache key, and a dataset cannot declare a
  param of that name. The request log's `token` field is now `client`.

### Deprecated

- `Authorization: Bearer <id>` and `serve.auth.tokens`. This release answers
  the header when a request has no `client_id`, reads each token as a client,
  and logs a warning for each. The next release refuses both. To move: deploy
  this release, switch every caller to `?client_id=`, then rename
  `serve.auth.tokens[].token` to `serve.clients[].id`.

### Fixed

- `sqlflow serve` redacted a password from a database error before logging it,
  but not a token or a key. A MotherDuck attach carries its token in the URL
  or as an `ATTACH ... (TOKEN '…')` option, and DuckDB's secrets carry keys
  the same way, so an attach that failed wrote the credential into the log —
  on a hosted deployment, into a dashboard an operator pastes from. Redaction
  now covers `token`, `secret`, `api_key`, `access_key` and `credential`,
  including compound names like `motherduck_token` and
  `s3_secret_access_key`, in both the `=`/`:` and the DuckDB option form.
  `KEY_ID` is left alone: it identifies a secret rather than authenticating
  it, and a reader needs it to tell two apart.

### Added

- `sqlflow serve` can answer a dataset from memory. It is off unless the
  dataset says `cache: {ttl_seconds: N}`, and a dataset without the block is
  served byte for byte as before. An answer is served for at most
  `ttl_seconds` after the query that produced it started; nothing is
  invalidated, because serve is never told the backend changed.
  `serve.cache.max_mb`, 16 by default, bounds what every cached dataset
  shares. A dataset with a range declares `bucket` on each grain, and `since`
  and `until` are rounded up to it, in what the statement binds and in the
  `range` the response echoes, so every request inside one bucket-wide window
  is one question. A grain may carry its own `cache: {ttl_seconds}`, which
  replaces the dataset's for that grain: a year of days changes by one open
  bucket and its key only rolls over at midnight, so the TTL alone decides
  how often the widest query runs. Concurrent requests for one question run one query, which
  finishes and is kept even if every caller gives up. Measured locally on the
  demo's data at 16 concurrent: two hundred requests with two hundred
  different ranges inside one five-minute window went from 200 queries at a
  951 ms median to one query at 15 ms. **The rounding returns the same rows
  only when the SQL compares `since` and `until`, half-open, with a column
  whose values sit on bucket boundaries**: `bucket >= $since AND bucket <
  $until`. `bucket <= $until`, or a filter on a raw timestamp, returns
  different rows, and serve cannot tell, which is why caching is the author's
  claim. A cached dataset's response carries `cache` (`miss`, `hit` or
  `shared`) and `age_ms`, the request log carries `cache`, and four
  `sqlflow_serve_cache_*` metrics join `/metrics`.
  `sqlflow_serve_query_duration_seconds` now counts only requests that ran a
  query, so a hit does not pull it toward zero.
- `sqlflow rollup serve` writes `bucket` on every generated grain, a `cache`
  block when the serve dataset sets `cache_ttl_seconds`, and a grain's own
  block for each grain `cache_ttl_by_grain` names. `rollup
  check` holds a serve file to both, so **a serve file generated before this
  release fails `check` until it is regenerated**: run `sqlflow rollup serve`
  and paste the datasets again.
- `sqlflow serve` answers requests from a pool of sessions rather than one
  connection. `serve.pool.size` sets how many run at once, four by default,
  measured to peak at 72 MiB resident on Linux against a 256 MB box. Every
  session is pinned to UTC, and the config's `commands` run once, on a
  connection of their own, because `ATTACH` is database-wide while
  `SET TimeZone` is not. A response carries `queued_ms` beside `elapsed_ms`,
  so a busy pool is no longer reported as a slow query. `/healthz` answers
  `busy` rather than `unavailable` when no session is free, and answers
  `HEAD` for monitors. With `serve.metrics.enabled`, `GET /metrics` serves
  six instruments, including the session wait that sizes the pool. A request
  that gives up now stops reading at the next batch and releases its reader,
  which is ADBC's documented equivalent of cancelling.

- `sqlflow serve`: an `integer` param may declare `min` and `max`. A request
  outside them is `400 invalid_param` naming the bounds, and `/v1/datasets`
  lists them.
- `sqlflow rollup ddl | serve | check`: generate rollup tables in Postgres,
  the triggers that keep them current as the pipeline writes, and the `serve`
  datasets that read them, from one `rollups.yml`. `check` fails CI when a
  committed migration or serve file drifts from the declaration, or a dataset
  could answer more rows than `max_rows`. `sqlflow validate` checks rollups
  files. Error codes `user.config.rollup` and `user.config.rollup_drift`.

### Fixed

- `bluesky.postgres.windowed.yml` paired `late_rows: reemit` with an upsert
  that replaces `posts`. `reemit` publishes `emit_sql` over the late rows
  alone, so one late post replaced a closed minute's count with its own. The
  example declares `drop`. The README, the config schema, the example
  comments and `validate`'s warning said `reemit` publishes the bucket again
  for a sink that upserts. They now say it publishes the late rows, for a
  sink that adds them to the bucket it holds.
- A windowed pipeline on `handlers.StructuredBatch` stopped with `checkpoint
  after truncate: Cannot CHECKPOINT: there are other write transactions
  active`. The handler checkpoints after every batch, and DuckDB refuses
  while another connection holds an uncommitted update or DDL. Since
  v2026.09.14 a window closes and publishes on connections of its own: its
  watermark save is an update, and the `sqlcommand` sink drops and creates
  its batch table. A batch that re-initialised during either failed, and the
  process exited. The handler skips a refused checkpoint, and the next batch
  reclaims what it left.
- The reference-table check at startup parsed memory DuckDB had already
  freed. It read the handler SQL's AST as a string that pointed into the
  query result, and released the result before parsing it. When DuckDB
  reused that memory, the check warned `parse serialized sql: invalid
  character` or counted another query's tables. It copies the AST first.

### Added

- `handler_checkpoints_skipped_total` counts those skips. A count that rises
  with every batch means a write stays open longer than a batch.
- The enforced invariant `pipeline.batch.independent_of_window_io`: a
  window's sink write or close, however long, never fails the consume loop.
  The watermark manager's conformance subject proves it with the structured
  handler.
- A `postgres` sink. `type: postgres` with `dsn`, `table`, `mode: upsert |
  append` and, for upsert, `key`. Each batch is a `COPY` into a session
  staging table and a server-side `INSERT ... ON CONFLICT`, in its own
  transaction, so a flush costs the batch rather than the table. Two rows
  with one key in a batch: the last one wins. A column the batch omits takes
  its default on insert and keeps its value on update. The probe checks the
  table exists and that a unique index or constraint covers exactly the key;
  a partial index does not count. Failures classify from SQLSTATE: a refused
  or lost connection exits 12 and retries, and a refused value, a missing
  column or a constraint violation exits 10. It writes over pgx and touches
  no DuckDB connection.
- `sqlflow validate` refuses `late_rows: reemit` with a postgres sink in
  upsert mode, warns on it in append mode, and warns on any `sqlcommand` sink
  whose SQL carries `ON CONFLICT` while a command attaches a Postgres. The
  DuckDB postgres extension runs that upsert by copying every row's key from
  the target table into DuckDB on every flush.
- The invariant `sink.flush.idempotent_on_key`: delivering the same batch
  twice leaves a keyed sink's destination holding it once. The postgres sink
  proves it; every other sink is exempt with a proof that it names no key.
- The image sets `MALLOC_ARENA_MAX=2`. In a loop of 3,600 upserts through the
  DuckDB postgres extension it cut native memory growth from 55 KB to 20 KB a
  flush.

### Changed

- `bluesky.postgres.windowed.yml` and `kafka.postgres.sink.yml` write through
  the `postgres` sink, and neither attaches Postgres through DuckDB.

## v2026.09.14

The first date-tagged release. Upgrade from v1.2.0. The pipeline file
changes: the tumbling window `manager` block is gone, and a file that
carries one does not start until it declares a `window`. The full notes are
in the tag message.

### Added

- `window`, on a table under `tables.sql`, replaces the `manager` block. The
  user declares `time_column`, `size_seconds`, `grace_seconds`,
  `idle_close_seconds`, `late_rows` and an optional `emit_sql` over `closed`,
  and the engine closes the window: it keeps a persisted event-time
  watermark in `sqlflow_windows`, publishes every bucket the watermark has
  passed, and deletes it. `now()` appears nowhere in a close, so an
  in-memory and a stateful pipeline behave the same.
- `late_rows: drop | reemit`, required. A row for a bucket that already
  closed is discarded and counted, or published again for a sink that
  upserts on the bucket's key. `validate` warns when `reemit` is paired with
  a sink that appends.
- Each window runs on a DuckDB connection of its own. It reads committed
  rows only, its delete and watermark commit together, and it takes no part
  in the pipeline's lock or transaction.
- `window_watermark_seconds`, `window_closed_total` and
  `window_late_rows_total`.
- `sqlflow validate` checks a window declaration: the time column is
  `TIMESTAMPTZ`, `emit_sql` reads `closed`, and a `manager` block is refused
  with the keys that replace it.
- `manager.watermark.never_regresses`, `manager.close.committed_rows_only`
  and `manager.late.policy_holds` join the enforced manager invariants.

- `pipeline.drain_deadline_seconds` bounds the whole shutdown after SIGTERM.
  The final batch, the managers' final poll and the state syncs share one
  deadline, 30 seconds by default. When it passes the process exits 15 with
  `system.lifecycle.drain_incomplete`. Nothing unwritten was committed, and
  the next start replays it.
- `/healthz` reports `starting`, `healthy`, `degraded` or `failed`, with a
  `reason`. `degraded` means a sink's retry ladder is running, or the
  pipeline recorded an error inside the last flush interval. `failed` answers
  503 and the other three answer 200. The previous `ok` and `stuck` bodies
  are now `healthy` and `failed`, and their HTTP codes did not change.
- `sqlflow validate` warns when a sink's `retry.deadline_seconds` is longer
  than the drain deadline.
- `/stats` carries `errors` and `last_error` under `progress`.
- `sqlflow serve` serves named SQL over HTTP. A serve config declares
  datasets, each a fixed statement with typed parameters, and grains that
  select one statement per aggregation level. Requests authenticate with a
  bearer token that names the caller. Responses are JSON rows with DuckDB
  type names. A row cap, a query timeout and CORS are configured in the
  file. See the README's `sqlflow serve` section.
- `sqlflow validate` and `sqlflow config validate` check a serve config
  against its own schema, `serve.json`, and report every rule with its line.
- `sqlflow config example --serve` prints the serve config skeleton.
- A `sqlflow serve` dataset can declare `range: {since, until, default}`
  and a `max_range` per grain. A request without `grain` gets the finest
  grain whose `max_range` covers its range, the server binds the resolved
  `since` and `until`, and the response echoes the grain and range. A range
  no grain serves is `400 range_too_wide`.
- Error codes `user.config.serve_reserved` and `user.config.serve_dataset`.

### Removed

- The `manager` block, `collect_closed_windows_sql` and
  `delete_closed_windows_sql`. This is a breaking change to the pipeline
  file: a config that carries them stops validating and stops running, and
  there is no translation, because the declaration cannot be derived from
  two arbitrary predicates. Every shipped example carries a `window`
  declaration instead. To migrate: `collect_closed_windows_sql` becomes
  `emit_sql` over `closed` with its WHERE dropped; `delete_closed_windows_sql`
  goes; the predicate's grace, idleness clause and bucket length become
  `grace_seconds`, `idle_close_seconds` and `size_seconds`; the bucket column
  becomes `time_column`; `poll_interval_seconds` and `sink` keep their names;
  and `late_rows` is new and required.

### Fixed

- A batch the sink refused had its offsets committed on the way out.
  Positions advanced when a message reached the handler, and the shutdown's
  state syncs made them durable, so a restart resumed past rows nothing had
  written. The loop resets to the last committed positions on any error,
  proven by the enforced invariant `pipeline.shutdown.commits_only_delivered`.
- A window poll the sink refused was retried every tick while the container
  reported healthy. It stops the process with the sink's error after one
  attempt.
- A SIGTERM that arrived while a batch was being flushed aborted the flush,
  and the process exited with the sink's error instead of draining. The
  batch now finishes inside the drain deadline.
- A cancel during a window's regular poll skipped its final poll, and
  a final poll that failed was logged while the process exited 0. The final
  poll always runs, and its failure is the exit code.
- A value the sink's driver cannot encode was retried the full ladder and
  reported as unreachable. It fails once, as `user.sink.encode_failed`,
  exit 10.
- The handler reset, the progress write and the sqlcommand sink ran on the
  pipeline's DuckDB connection without its lock, so a `--with-http-debug`
  query could close a result the loop had in flight. Every statement on that
  connection holds the lock.
- One failed flush counted as three errors.

### Known limits

- `sqlflow serve` refuses a non-zero `rate_limit`, which is reserved.
- `sqlflow serve` runs every request on one DuckDB connection, one at a time.
- A `sqlflow serve` timeout returns `504` and leaves the query running to
  completion.

## v1.0.0 — sqlflow, the Go engine

SQLFlow now ships a second engine: **sqlflow**, a Go rewrite of the Python
stream processor that reads the same configuration files. A `sqlflow.yml`
written for the Python engine is intended to run unmodified on sqlflow — same
YAML spec, same Jinja2 templating, same JSON Schema, same DuckDB SQL.

The Python engine (`sqlflow/`, `turbolytics/sql-flow`) is unchanged and still
maintained. sqlflow is the recommended engine for new pipelines.

### Why

- **Throughput**: ~793,000 msgs/sec versus the Python engine's tens of
  thousands on the same pipeline — roughly 20-60x.
- **Deployment**: a single binary, suitable for edge and IoT hardware.
- **Ergonomics**: background processing in Go rather than Python.

### Added

**Engine**
- Go pipeline: Kafka source (franz-go) → zero-copy JSON→Arrow → DuckDB via
  Arrow ADBC → sink, with batch accumulation and manual offset commits.
- `handlers.StructuredBatch` — schema declared up front, zero-copy Arrow ingest.
- `handlers.InferredMemBatch` — schema inferred from the JSON, matching
  `pyarrow.Table.from_pylist` semantics.
- `handlers.InferredDiskBatch` — disk-buffered via `read_json_auto` / `COPY TO`,
  with staged files cleaned up on close.
- `flush_interval_seconds` now actually bounds the batch wait (default 30s), so
  a low-traffic topic no longer stalls indefinitely.

**Sources**
- Kafka, with SASL/TLS on both source and sink: `PLAINTEXT`, `SSL`,
  `SASL_PLAINTEXT`, `SASL_SSL`; `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`.
  GSSAPI and encrypted PEM keys are rejected with a clear error rather than
  silently ignored.
- Kafka metadata injection: `kafka_topic`, `kafka_partition`, `kafka_offset`.
- WebSocket, with reconnect and backoff (verified against the live Bluesky
  jetstream).
- Webhook: `POST /events` on `:8001` with HMAC-SHA256 validation and
  backpressure.

**Sinks**
- `console`, `kafka`, `sqlcommand`, `iceberg`, `clickhouse`, `noop`.
- `kafka` sink blocks on broker acks before offsets commit.
- `sqlcommand` + `substitutions` (`uuid4`) — the path to Postgres, S3 parquet,
  local parquet, DuckLake and MotherDuck.
- `iceberg` via apache/iceberg-go, resolving the catalog from `.pyiceberg.yaml`
  exactly as pyiceberg does.
- `clickhouse`, with an explicit column list derived from the Arrow schema.

**Operations**
- Error policies: `pipeline.on_error.policy` ∈ `RAISE` / `IGNORE` / `DLQ`, with
  the DLQ as a full nested sink, applied at both `handler.write` and
  `handler.invoke`.
- Tumbling window table managers, including a final poll on shutdown so a window
  closing during shutdown is not stranded.
- Prometheus metrics: seven OTel instruments on `:8000/metrics`, the same port
  the Python engine uses.
- Strict config parsing — an unknown key is an error, not a silently dropped
  setting.

**CLI and packaging**
- `sqlflow run` (`--max-msgs`, `--metrics`, `--stats-json`, `--pprof`),
  `dev invoke`, `config validate`, `config example`, `tail`, `version`.
- `Dockerfile.sqlflow` — multi-stage, cgo, libduckdb baked in.
- `make release-binaries` — linux and darwin on amd64 and arm64.
- DuckDB version pinned in one place, the `DUCKDB_VERSION` file.
- `make benchmark-container`, which runs the benchmark inside the docker network
  (host→container port-forwarding understates throughput ~10x).

### Fixed

- **Consume-loop data loss**: reaching `--max-msgs`, or a partial batch left when
  the source ended, discarded the batch in flight while still reporting those
  messages as consumed.
- **Fixture data corruption**: `bufio.Scanner` reuses its buffer and both
  handlers retain the caller's slice, so any fixture line larger than the read
  buffer produced corrupted rows.

### Not supported in sqlflow

- **UDFs.** Dropped by decision — they belong to DuckDB (a macro, an extension,
  or an `ATTACH`ed database). A `udfs:` block is a hard error naming the
  functions rather than a silent skip.
- **ClickHouse sink**: nested types, decimals and intervals are rejected with an
  explicit unsupported-type error.
- **Webhook source**: fragmented-message framing differs from the Python
  implementation, and the webhook's own request metrics are not ported.
- `--with-http-debug` and a configurable log level are not implemented.

See [Differences from the Python engine](README.md#differences-from-the-python-engine)
for the complete list, including CLI flag differences (`run -c <config>` and
`--max-msgs`, versus the Python engine's positional config and
`--max-msgs-to-process`).
