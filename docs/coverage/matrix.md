# Feature coverage matrix

Generated from `docs/coverage/matrix.json` by `make coverage-matrix`.
Do not edit by hand.

Features are declared in `docs/coverage/features.yml`. A test attaches
to one by name -- `sink.clickhouse` is covered by `TestSinkClickhouse*`
or `test_sink_clickhouse*` -- and that is the cheap default: rename a
test and it is attributed, with no import and no marker.

Levels are derived from where a test ran, never declared, so they
cannot drift. `unit` is `go test -short`, `integration` is the Go
tests that need a real service, `release` is the image suite. A
**skipped** test is not coverage at any of them: a skip that reads as
a pass is how `sink.iceberg` shipped for months without ever being
written to.

The Tests column counts what attributes to the feature, so a thinly
covered one is visible at a glance. `docs/coverage/matrix.json`
names every one of them.

| Feature | What it does | unit | integration | release | Tests |
| --- | --- | --- | --- | --- | --- |
| `source.kafka` | Consumes a Kafka topic, tracking offsets and leader epochs. | ✅ | ✅ | ✅ | 28 |
| `source.webhook` | Accepts records over HTTP, with optional HMAC signature checks. | ✅ | — | — | 15 |
| `source.websocket` | Consumes a websocket stream, reconnecting on drop. | ✅ | — | ✅ | 6 |
| `sink.kafka` | Publishes result rows to a Kafka topic. | ✅ | — | ✅ | 7 |
| `sink.clickhouse` | Inserts result batches into a ClickHouse table. | ✅ | — | ✅ | 14 |
| `sink.iceberg` | Appends result batches to an Iceberg table through a catalog. | ✅ | — | ✅ | 13 |
| `sink.parquet` | Writes result batches as parquet files to a local path. | — | — | ✅ | 1 |
| `sink.sqlcommand` | Runs a SQL command against the pipeline's own DuckDB connection. | ✅ | — | — | 10 |
| `sink.console` | Writes result rows to stdout as JSON. | ✅ | — | ✅ | 3 |
| `sink.retry` | Retries a sink whose destination is not answering, bounded by a deadline. | ✅ | — | — | 69 |
| `handler.inferred_mem` | Infers a schema per batch and runs the query in memory. | ✅ | — | ✅ | 46 |
| `handler.inferred_disk` | Infers a schema per batch, staging the batch on disk. | ✅ | — | — | 9 |
| `handler.structured` | Binds a declared schema, ingesting through Arrow. | ✅ | — | ✅ | 8 |
| `state.durability` | Window state and the offsets that produced it commit together. | ✅ | — | ✅ | 11 |
| `state.offsets` | Kafka positions are stored in DuckDB and resumed on restart. | ✅ | — | ✅ | 22 |
| `state.corruption` | A damaged state file fails the start rather than silently resetting. | ✅ | — | ✅ | 6 |
| `lifecycle.drain` | SIGTERM writes the buffered batch before exiting. | ✅ | — | ✅ | 2 |
| `lifecycle.exit_codes` | The process exit status carries the error code a supervisor reads. | ✅ | — | ✅ | 8 |
| `core.consume_loop` | Accumulates a batch, flushes it, and commits in that order. | ✅ | — | — | 14 |
| `error.taxonomy` | Every failure carries a class.domain.reason code. | ✅ | — | — | 16 |
| `error.raise` | Policy RAISE stops the pipeline on a bad record. | ✅ | — | — | 1 |
| `error.ignore` | Policy IGNORE drops a bad record and keeps the pipeline running. | ✅ | — | ✅ | 5 |
| `error.dlq` | Policy DLQ diverts a bad record to a sink instead of dropping it. | ✅ | — | ✅ | 4 |
| `manager.tumbling_window` | Publishes and deletes closed windows on an interval. | ✅ | — | ✅ | 16 |
| `config.templating` | Renders a config through Jinja2 against SQLFLOW_ environment variables. | ✅ | — | ✅ | 45 |
| `config.validation` | Validates a config against the schema and reports where it is wrong. | ✅ | — | ✅ | 73 |
| `observability.metrics` | Exports pipeline counters and histograms over Prometheus. | ✅ | — | — | 7 |
| `observability.debug_api` | Serves ad-hoc SQL against the live DuckDB connection. | ✅ | — | — | 7 |
| `cli.invocation` | Resolves the config path and message limits from either flag form. | ✅ | — | — | 13 |
| `cli.dev_invoke` | Runs a pipeline against a fixture file, without a source. | ✅ | — | ✅ | 9 |
| `cli.version` | The shipped binary reports the version it was built from. | — | — | ✅ | 1 |

**31 features declared, 31 fully covered, 0 gap(s).**

## Covered only by another test's marker

These features have no test named for them. That is legitimate for a
capability an end-to-end run proves in passing, and a smell for one
that deserves its own test.

- `source.kafka` (release) — via `test_handler_inferred_mem_aggregates_every_message`, `test_state_durability_survives_a_restart`
- `source.websocket` (release) — via `test_handler_inferred_mem_preserves_arrays_and_unioned_fields`
- `sink.kafka` (release) — via `test_handler_inferred_mem_aggregates_every_message`
- `sink.console` (release) — via `test_handler_inferred_mem_invoke_renders_rows`
- `handler.structured` (release) — via `test_handler_inferred_mem_preserves_arrays_and_unioned_fields`
- `state.offsets` (release) — via `test_state_durability_survives_a_restart`
- `state.corruption` (release) — via `test_lifecycle_exit_codes_carry_the_error_code`
- `manager.tumbling_window` (release) — via `test_state_durability_survives_a_restart`
- `config.templating` (release) — via `test_config_validation_accepts_a_shipped_example`
- `cli.dev_invoke` (release) — via `test_handler_inferred_mem_invoke_renders_rows`

