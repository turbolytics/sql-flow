# A keyed Postgres sink on a native client

PR #290, issues #268 and #287. Verified against `main` at aae403f on 2026-09-14 with DuckDB v1.5.2 and postgres extension `c89234f`. Reviewed the same day; the review's decisions are folded in below. An adversarial review of the implementation followed, and its fixes are in "After the second review" at the end.

## The problem

sqlflow writes to Postgres through the DuckDB postgres extension: the pipeline attaches the database and a `sqlcommand` sink runs `INSERT ... ON CONFLICT` into it. Every shipped Postgres example, the tumbling window tutorial, and the Bluesky demo use that pattern. Four things are wrong with it, all measured in #290.

**Every upsert reads the whole target table.** The extension never sends `ON CONFLICT` to Postgres. It copies the key columns and `ctid` of every row in the target into DuckDB, decides the conflicts there, writes the new rows with a `COPY`, and updates the rest through a temporary table. With `SET pg_debug_show_queries = true`, one upsert of one row into a 1,000-row table is:

```
COPY (SELECT "bucket", "lang", ctid FROM "public"."probe_upsert" WHERE ctid BETWEEN '(0,0)'::tid AND '(4294967295,0)'::tid) TO STDOUT (FORMAT "binary");
CREATE LOCAL TEMPORARY TABLE "update_data_..."("posts" INTEGER, "updated_at" TIMESTAMP WITH TIME ZONE, __page_id_string VARCHAR) ON COMMIT DROP;
COPY "public"."probe_upsert" FROM STDIN (FORMAT BINARY)
UPDATE "public"."probe_upsert" SET ... FROM "update_data_..." WHERE "probe_upsert".ctid=__page_id_string::TID
```

The cost of a flush is the size of the table, not the size of the batch. The demo's table gains a row per language every minute and is never trimmed, so on Render every flush reads more than the last. DuckDB v1.5.5 with extension `41223e5` sends the same statements.

**It grows native memory per flush.** 3,600 flushes of 40 rows through the demo's upsert grew resident memory 55 KB a flush against a growing table and 5 to 13 KB against a table held at 40 rows. A plain `INSERT` of the same rows was flat. The growth lives in libpq inside libduckdb, where neither pprof nor `duckdb_memory()` sees it, and about half of it is glibc holding freed pages. `MALLOC_ARENA_MAX=2` cut it by two thirds. The rest survives `malloc_trim`.

**It hides the error class.** A refused connection comes back as `Internal: IO Error: Unable to connect to Postgres ...`, text inside an internal code. The sink cannot tell a network failure from a bad statement, so `sqlcommand` has no retry ladder and a window flush against a stopped Postgres exits 1 rather than 12 (#287).

**It ignores cancellation.** DuckDB's ADBC driver drops the context on a statement, so a drain deadline cannot interrupt a hung write.

Three of the five footguns the #268 post describes are the same defect. The extension's `COPY ... FROM STDIN` for the conflicting path carries no column list, so an omitted `NOT NULL DEFAULT` column arrives as `NULL`. Its catalog query reads only `pg_constraint`, so a unique index is not a conflict target. Postgres never sees `ON CONFLICT`, so its "cannot affect row a second time" check never runs and duplicate keys in one batch keep one row silently.

Until #288, the shipped `bluesky.postgres.windowed.yml` paired `late_rows: reemit` with this upsert. A reemit publishes only the late rows, and the upsert replaces the bucket's count with theirs. #288 moved the example to `drop`; the trap stays writable because the sink's contract lives in user SQL.

## The rule

DuckDB writes when the destination is DuckDB's own format or catalog: Parquet, CSV and JSON files, local or on S3, DuckDB files, DuckLake, MotherDuck, the state file. A native Go client writes when the destination is a server with its own wire protocol: Postgres, ClickHouse, Kafka, an Iceberg catalog. The test is whether DuckDB is producing the destination's format or impersonating a client of a database. Reads through `ATTACH` stay everywhere.

Postgres is the one destination on the wrong side of that line. This spec moves it.

## The change

### 1. A `postgres` sink

```yaml
sink:
  type: postgres
  postgres:
    dsn: '{{ SQLFLOW_POSTGRES_URI }}'
    table: posts_per_minute_by_lang
    mode: upsert
    key: [bucket, lang]
```

| Field | Rule |
| --- | --- |
| `dsn` | A libpq URI or key-value string, as pgx parses it. Required. |
| `table` | The target, optionally schema-qualified. Required. Never created by the sink. |
| `mode` | `upsert` or `append`. Required: the two are different promises. |
| `key` | The columns a row is identified by. Required for `upsert`, refused for `append`. |

The contract: after a successful `Flush`, the table holds one row per key from the batch with the batch's values (`upsert`), or every row of the batch appended (`append`). A second delivery of the same batch leaves an `upsert` table unchanged. That is what makes the engine's at-least-once delivery safe to the reader.

The sink runs on its own pgx connection. It touches no DuckDB connection and needs no lock, so a window sink and a pipeline sink of this type can flush while the engine holds either connection.

### 2. One flush

`WriteTable` retains the batch and buffers it. `Flush` delivers the buffered batches one at a time, in arrival order, each in its own transaction, and stops at the first failure with that batch and every later one still buffered. One transaction per batch, not one for all of them: after a failed flush the retry ladder calls `Flush` with two or more batches buffered, and a running total or a CDC stream carries the same key in consecutive batches. Merged in one statement those rows hit `ON CONFLICT DO UPDATE` twice and fail with `21000`, which the first attempt would have applied in order. A retry must not fail a batch the first attempt would have accepted.

For one batch, on the sink's connection:

1. `CREATE TEMP TABLE IF NOT EXISTS <staging> ON COMMIT DELETE ROWS AS SELECT <batch columns> FROM <target> WITH NO DATA`, then a `__seq bigint` column. Once per connection and column set, and again after every redial, because a temp table dies with its session. It empties itself at every commit, so there is no catalog churn per flush, no name to collide with another pipeline's, and nothing left behind by a crash. It has exactly the batch's columns with the target's types and none of the target's constraints or defaults, so the merge is the only place a row is judged, and the error names the target. A batch whose columns differ from the last drops and recreates it.
2. `COPY <staging> (<batch columns>, __seq) FROM STDIN (FORMAT binary)` through pgx `CopyFrom`. The sink fills `__seq` with the row's position in the batch. Cost is the batch.
3. The merge, on the server:
   - `upsert`: `INSERT INTO <target> (<batch columns>) SELECT DISTINCT ON (<key>) <batch columns> FROM <staging> ORDER BY <key>, __seq DESC ON CONFLICT (<key>) DO UPDATE SET <c> = EXCLUDED.<c>` for every batch column not in the key. A batch whose only columns are the key gets `DO NOTHING`.
   - `append`: `INSERT INTO <target> (<batch columns>) SELECT <batch columns> FROM <staging> ORDER BY __seq`.
4. `COMMIT`.

Two rows with one key in a batch: the last one in the batch wins, and the sink says so in its docs. The footgun in #268 was that the extension kept one of the two without saying which. Failing instead would make an ordinary CDC batch, two updates for one id, an exit 10 and a crash loop, and it would make `emit_sql`'s `GROUP BY` load-bearing for the sink's correctness. Kafka Connect's and Flink's JDBC sinks apply a batch in order and let the last row win, and so does this one. "Last" is the row's position in the batch, which is the handler's output order.

The `INSERT` names the batch's columns and no others, so a column the batch omits takes the table's default on insert and keeps its value on update. The demo's `updated_at = EXCLUDED.updated_at` has no equivalent: a column that should change on every republish is emitted from `emit_sql`. Postgres runs the `ON CONFLICT`, so a unique index is a valid conflict target. The three footguns close without a line of user SQL.

Every step takes the flush's context. A drain deadline cancels a `COPY` mid-stream and the transaction rolls back.

### 3. Startup

`New` parses the DSN and dials nothing. `Probe`, which `sinks.New` runs before the pipeline consumes anything, dials and checks:

- The table exists. Missing: `user.sink.invalid`.
- For `upsert`, a unique index or constraint covers exactly the key columns, in any order. Read from `pg_index` with `indisunique` and `indpred IS NULL`: a unique index counts, as it does for native Postgres, and a partial index does not, because `ON CONFLICT (<key>)` will not infer one without its predicate. Missing: `user.sink.invalid`, naming the key and the table.
- Every key column exists in the table.
- For `append`, a unique index or constraint on the table is a warning in the log: a redelivery after a crash inserts the same rows again and exits 10 on it. `append` is at-least-once for the reader, and the docs say so.

A refused or unresolvable host: `system.sink.unreachable`, exit 12. A server that answered and refused: `user.sink.invalid`, exit 10. The batch's columns are not known until the first `Flush`, because the handler's Arrow schema decides them. A column the table lacks fails that flush with `42703 undefined_column`, classified below.

### 4. Errors

pgx returns `*pgconn.PgError` with a SQLSTATE, and dial failures wrap `net.OpError`, which `isUnreachable` already classifies. The SQLSTATE class decides the rest:

| SQLSTATE class | Meaning | Code | Retried | Exit |
| --- | --- | --- | --- | --- |
| `08` | connection exception | `system.sink.unreachable` | yes | 12 |
| `57` | operator intervention, including `57P01` shutdown | `system.sink.unreachable` | yes | 12 |
| `53` | insufficient resources | `system.sink.unreachable` | yes | 12 |
| `40` | serialization failure, deadlock | `system.sink.unreachable` | yes | 12 |
| `22` | data exception: a value does not fit the column | `user.sink.encode_failed` | no | 10 |
| `23` | integrity constraint violation | `user.sink.invalid` | no | 10 |
| `21` | cardinality violation | `user.sink.invalid` | no | 10 |
| `42` | undefined column or table, syntax, privilege | `user.sink.invalid` | no | 10 |
| `28` | authentication | `user.sink.invalid` | no | 10 |
| `3D`, `3F` | database or schema does not exist | `user.sink.invalid` | no | 10 |
| anything else | | `system.sink.write_failed` | no | 1 |

No new error code. `user.sink.encode_failed` is the code #233 gave a value that fails identically on every attempt, and a server-side `22` is that value one hop later. The retry ladder from #233 retries by class, so the table above is the whole policy. `config.SinkRetries` gains `postgres`.

This closes #287 for this sink. `sqlcommand` keeps its behavior.

### 5. Types

The sink converts each Arrow column to the Go value pgx encodes for the target column's type. The target's types come from the `COPY` prepare, not from the Arrow schema, so a `BIGINT` column takes an `int32` batch column and a `TIMESTAMPTZ` column takes a naive timestamp as the wall-clock instant pgx sends.

| Arrow | Sent as | Accepting columns |
| --- | --- | --- |
| `bool` | `bool` | `boolean` |
| `int8`, `int16`, `int32`, `int64` | the same width, `int8` widened to `int16` | `smallint`, `integer`, `bigint` that fit |
| `uint8`, `uint16`, `uint32` | the next signed width | as above |
| `uint64`, `decimal(*, *)` | `pgtype.Numeric` from the decimal string | `numeric`; `bigint` when it fits |
| `float32`, `float64` | `float32`, `float64` | `real`, `double precision` |
| `utf8`, `large_utf8` | `string` | `text`, `varchar`, `json`, `jsonb` |
| `binary`, `large_binary` | `[]byte` | `bytea` |
| `date32` | `time.Time` at midnight UTC | `date` |
| `timestamp` with a zone | `time.Time`, the instant | `timestamptz`; `timestamp` receives the UTC wall clock |
| `timestamp` without a zone | `time.Time`, the wall clock read as UTC | `timestamp`; `timestamptz` receives it as a UTC instant |
| `list`, `struct`, `map` | JSON text the sink renders from the Arrow array | `jsonb`, `json`, `text` |
| `time32`, `time64`, `duration`, `interval`, `dictionary` | unsupported | fails the flush with `user.sink.encode_failed` naming the column and type |

A `list` of scalars could map to a Postgres array. It does not in this revision: JSON is one rule that covers every nesting, and the integration file records the gap. `docs/coverage/integrations/sink.postgres.yml` declares every key in `lattice.yml` with its outcome, written by hand from this table, and the type round-trip runner proves each row against a live Postgres.

### 6. Validate

- A window whose sink is `postgres` with `mode: upsert` and `late_rows: reemit` is refused, `user.config.invalid`. The sink replaces a bucket's row with what it is handed, and a reemit hands it only the late rows.
- `mode: append` with `reemit` warns, as `kafka` and `iceberg` do. `appendsOnly` reads the sink block, not only the type.
- Any `sqlcommand` sink, pipeline or window, whose SQL contains `ON CONFLICT`, in a config whose commands `ATTACH ... (TYPE POSTGRES)`, warns: every flush reads the whole target table's keys through the extension, and the `postgres` sink does the same write at the cost of the batch.

The validate schema and the CLI's example output regenerate with the new block.

### 7. Conformance and coverage

`sink.postgres` joins the registry as a shipped feature requiring `unit` and `integration` evidence. `docs/coverage/integrations/sink.postgres.yml` declares `implements: [Sink, Prober]` and the type table. The conformance test runs the harness against a testcontainers Postgres behind the proxy, the way ClickHouse does, so every sink invariant gets a cell.

One invariant is new:

```yaml
- id: sink.flush.idempotent_on_key
  family: resilience
  class: safety
  applies_to: sink
  claim: >
    Delivering the same batch twice leaves the destination holding it once.
    The engine promises at-least-once; a sink that identifies rows by key is
    what turns that into exactly-once for the reader.
  verified_by: harness
```

The harness gains a step after the sequence: `WriteTable(A)` and `Flush` again, then `ReadBack` must hold A and B and nothing more. A `SinkSubject` says whether it claims the invariant with `Keyed: true`; a subject that does not is skipped, and its registry file must exempt it with a reason. Exemptions for this revision, each with a `proven_by` test: `sink.clickhouse` (the table engine decides, and a second delivery into the conformance table's `Log` engine is two rows), `sink.iceberg` and `sink.kafka` (append by design), `sink.sqlcommand` (the user's SQL decides), `sink.console` and `sink.noop` (no destination holds a row to identify).

### 8. Examples and the demo

- `bluesky.postgres.windowed.yml` moves to the `postgres` sink with `late_rows: drop`, and its `ATTACH` goes. It is the tumbling window tutorial's config.
- `kafka.postgres.sink.yml` moves to `mode: append`.
- `kafka.postgres.join.yml` and `tigerdata.enrich.yml` read through `ATTACH` and stay.
- The Bluesky demo's `pipeline.yml` follows once a release carries the sink.

The examples test builds every sink. The `postgres` sink's probe fails against no server, and the test already skips a sink that needs a resource it cannot provide, so the test needs no change. The error text must not contain the substrings that mark a parity failure.

### 9. The image

`MALLOC_ARENA_MAX=2`, landed in #290 already. It is the usual setting for a Go and cgo process on a small container and it cut the extension's growth by two thirds. It stays after the sink lands, because DuckDB is still a cgo process.

## The client library

Three candidates were weighed. The sink uses **pgx v5**.

**pgx v5** (`github.com/jackc/pgx/v5`, v5.11.0, Go 1.25). Pure Go, no cgo, no shared library in the image. `CopyFrom` streams the binary `COPY` protocol from a Go row source. Every call takes a context. Cancelling one sends the server a cancel request, and if the server does not answer in time pgx closes the connection; the sink redials on its next flush either way. Errors are `*pgconn.PgError` with `Code` holding the SQLSTATE, which section 4 classifies directly. Memory is Go heap that pprof sees. It is the client the ecosystem has standardized on, and Kafka Connect's and Flink's JDBC sinks do the same staging-and-merge on the server that section 2 does. Cost: the Arrow to Go mapping in section 5, which the ClickHouse sink already has a shape for.

**The ADBC PostgreSQL driver.** sqlflow already loads DuckDB through the ADBC driver manager, and this driver ingests Arrow directly through `COPY`, so the type mapping would come with it. Rejected. It is a C++ shared library, a second `.so` in the image with its own release cadence. It does no upsert, so the merge is still SQL we run. Its cancellation goes through the same driver manager whose context handling the DuckDB driver drops. Its memory is native and invisible, which is the class of problem this spec exists to remove. It stays the fallback if section 5 proves costlier than expected.

**lib/pq.** In maintenance mode by its own README. No binary `COPY`. Rejected.

## Compatibility

- `sqlcommand` is unchanged. Every existing config runs as before, with the new validate warning where it applies.
- `config.Sink` gains a `postgres` block. The JSON schema and the golden example regenerate.
- `go.mod` gains `github.com/jackc/pgx/v5` and, for the conformance test, `testcontainers-go/modules/postgres`.
- The docs move after the release that carries the sink: the tumbling window tutorial, the configuration page's sink section, and the `ON CONFLICT` paragraph.

## What this removes

Nothing in code. In the taught pattern, it removes `INSTALL postgres`, `LOAD postgres`, `ATTACH` and the hand-written `ON CONFLICT` from every config that only writes to Postgres.

## What it does not do

- It does not create or alter the target table. A missing table or constraint fails at startup with a message that says what to create.
- It does not pool connections. One connection per sink; a pipeline with a pipeline sink and a window sink holds two.
- It does not map `list` to a Postgres array, or nested types to composite types. JSON text is the rule.
- It does not use `MERGE`. `ON CONFLICT` covers the contract and runs on every supported Postgres.
- It does not change the ClickHouse or Iceberg sinks. The same `key` and `mode` shape can reach ClickHouse later, with `ReplacingMergeTree` as the mode.
- It does not touch reads. `ATTACH` for a join or an enrichment stays the right tool.
- It does not report to duckdb/duckdb-postgres. That issue, with the #290 loop as the reproduction, is filed separately, and a fix there helps `sqlcommand` users without changing this design.

## Done when

- `go test -short -race ./...` passes with unit tests marked `sink.postgres` for the config rules, the merge statement, the SQLSTATE table, and the type conversion.
- `TestIntegrationSinkPostgres_Conformance` passes every sink invariant, including `sink.flush.idempotent_on_key`, against a testcontainers Postgres behind the proxy.
- `make coverage-check` passes with `sink.postgres` declared, its type table answering every lattice key, and every other sink exempt from the new invariant with a proof.
- `sqlflow validate` refuses `upsert` with `reemit`, warns on `append` with `reemit`, and warns on a `sqlcommand` upsert into an attached Postgres, each with a test.
- A test fails the first flush, lets the ladder retry with two buffered batches that share a key, and asserts the table holds the second batch's value. That is the case a merge-all-at-once flush fails with `21000`.
- A test delivers one batch with two rows for one key and asserts the table holds the last one.
- `bluesky.postgres.windowed.yml` runs against the local Postgres end to end and the read-back matches the `sqlcommand` version's rows.
- The window leak loop through the `postgres` sink reports the plain-insert rate, within noise, and #290's description gets that row.
- Stopping Postgres under the windowed example exits 12 with `system.sink.unreachable`, and a batch whose value does not fit its column exits 10.
- The CHANGELOG's `## Unreleased` names the sink, the validate rules, and `MALLOC_ARENA_MAX`.

## After the second review

An adversarial review ran the sink through `sinks.New` against Postgres 16 and broke it nine ways. Each finding is now a test, and the design above changes where the finding required it.

| Finding | Change |
| --- | --- |
| A server that holds packets blocked a flush and the probe forever; the ladder checks its deadline only between attempts | Each attempt, a probe or one batch's transaction, is bounded by the retry deadline (`WithPostgresTimeout`). `retry.deadline_seconds` now also bounds one attempt. |
| A connection closed with a FIN between flushes failed as `write_failed` and stopped the pipeline | A failure that closed the connection, or that pgx reports safe to retry, is `system.sink.unreachable` and retried. |
| `DROP TABLE IF EXISTS sqlflow_staging` resolved on `search_path` and dropped a user's table | Every staging reference is `pg_temp.sqlflow_staging`. |
| A nullable key column let a null key be inserted on every redelivery | The probe refuses a nullable key column. |
| `run` did not refuse `upsert` with `late_rows: reemit` | `config.Window.ReemitOverwrites` holds the rule, and validate and run both refuse it. |
| The batch was boxed into `[][]any` before the COPY: +177 MiB peak for 1M rows | A `CopyFromSource` streams one row at a time: +29 MiB. A schema check refuses an unsupported type before the transaction. |
| One refused value fails the whole batch | Documented on the config and in `kafka.postgres.sink.yml`. A sink-side DLQ is a separate change. |
| DuckDB `TIME`, `INTERVAL` and `ENUM` failed the first flush | `time32`, `time64`, `duration` and `month_day_nano_interval` convert to `pgtype.Time` and `pgtype.Interval`, and a dictionary writes its value. |
| A deferrable unique constraint and an invalid index passed the probe | The probe requires `indimmediate` and `indisvalid`. |

Not changed: a pooler in transaction mode does not keep a session temp table, so the sink requires a direct connection or session pooling, and the DSN's documentation says so. The harness's `idempotent_on_key` step re-delivers an identical row, so a sink that does `DO NOTHING` would pass it; replacement is proven for this sink by `LastRowInABatchWins` and `RetryAppliesBufferedBatchesInOrder`.
