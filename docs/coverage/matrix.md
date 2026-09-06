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
| `sink.clickhouse` | Inserts result batches into a ClickHouse table. | ✅ | ✅ | ✅ | 20 |
| `sink.iceberg` | Appends result batches to an Iceberg table through a catalog. | ✅ | — | ✅ | 13 |
| `sink.parquet` | Writes result batches as parquet files to a local path. | — | — | ✅ | 1 |
| `sink.sqlcommand` | Runs a SQL command against the pipeline's own DuckDB connection. | ✅ | — | — | 10 |
| `sink.console` | Writes result rows to stdout as JSON. | ✅ | — | ✅ | 3 |
| `sink.retry` | Retries a sink whose destination is not answering, bounded by a deadline. | ✅ | — | — | 71 |
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
| `tooling.conformance` | The harness proves the declared invariants for any integration. | ✅ | — | — | 16 |
| `tooling.coverage` | Tests attribute to features and invariants, and the registries match the code. | ✅ | — | — | 12 |

**33 features declared, 33 fully covered, 0 gap(s).**

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

## Markers naming an unknown feature

- `TestToolingConformanceSinks_ACorrectSinkPasses` marks `sink.noop`


# Invariant matrix

Generated from `docs/coverage/matrix.json` by `make coverage-matrix`.
Do not edit by hand.

Invariants are declared in `docs/coverage/invariants.yml`, integrations
in `docs/coverage/integrations.yml`. A cell is proven by the conformance
harness in `internal/conformance`, which emits a marker naming both
ids -- a harness test's name says nothing, because the same code runs
for every integration.

A covered cell names the levels that proved it: `u` unit, `i`
integration, `r` release. That is the question the matrix exists to
answer -- proven with a fake, or against the real thing, or in the
shipped image. **exempt** carries its reason in the JSON, and
**missing** means no evidence. Nothing here fails the build until an
invariant's `requires` is filled in, and none is yet.

## Invariants: resilience

| Invariant | Claim | `sink.kafka` | `sink.clickhouse` | `sink.iceberg` | `sink.sqlcommand` | `sink.console` | `sink.noop` |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `sink.write.buffers_only` | WriteTable does not reach the destination. Only Flush does. | ❌ missing | ✅ i | ❌ missing | ❌ missing | ❌ missing | — exempt |
| `sink.flush.keeps_batch` | A failed Flush leaves every undelivered row buffered. The next Flush re-attempts them. | ❌ missing | ✅ i | ❌ missing | — exempt | — exempt | — exempt |
| `sink.flush.no_hollow_success` | Flush returns nil only when every row since the last success was acknowledged by the destination. | ❌ missing | ❌ missing | ❌ missing | ❌ missing | ❌ missing | — exempt |
| `sink.flush.preserves_order` | Rows reach the destination in WriteTable order, across a retry. | ❌ missing | ❌ missing | ❌ missing | ❌ missing | ❌ missing | — exempt |
| `sink.flush.honours_context` | Flush returns ctx.Err() when the context ends. Rows stay buffered. | ❌ missing | ❌ missing | ❌ missing | — exempt | — exempt | — exempt |
| `sink.flush.empty_is_noop` | Flush with nothing buffered returns nil and touches nothing. | ❌ missing | ❌ missing | ❌ missing | ❌ missing | ❌ missing | ❌ missing |
| `sink.batch.reports_buffer` | Batch returns what is buffered, and nil when nothing is. | ❌ missing | ❌ missing | ❌ missing | ❌ missing | ❌ missing | — exempt |
| `sink.error.classifies` | The sink's errors classify as unreachable or rejected, so the retry ladder retries the right ones. | ❌ missing | ❌ missing | ❌ missing | — exempt | — exempt | — exempt |
| `sink.probe.fails_start` | A Prober whose destination is unreachable fails the start once, without retrying. | ❌ missing | ❌ missing | ❌ missing | — exempt | — exempt | — exempt |

## Invariants: checkpoint

| Invariant | Claim | `source.kafka` | `source.websocket` | `source.webhook` |
| --- | --- | --- | --- | --- |
| `pipeline.commit.after_flush` | Offsets and state commit only after Flush returned nil. | · | · | · |
| `pipeline.commit.nothing_on_failure` | A failed flush commits nothing. Not offsets, not state. | · | · | · |
| `pipeline.state.with_offsets` | Window state and the offsets that produced it commit atomically. | · | · | · |
| `source.commit.only_processed` | A source commits the marks the pipeline processed, never what it fetched. | ❌ missing | — exempt | — exempt |
| `source.resume.from_committed` | Restart resumes at the committed position. No gap, and no replay before it. | ❌ missing | — exempt | — exempt |
| `source.marks.never_regress` | A committed position never moves backwards. | ❌ missing | — exempt | — exempt |
| `source.commit.on_revoke` | Marks commit when a partition is revoked, before the rebalance completes. *(declared, tracked by #183)* | ❌ missing | — exempt | — exempt |

## Invariants: types

| Invariant | Claim | `sink.kafka` | `sink.clickhouse` | `sink.iceberg` | `sink.sqlcommand` | `sink.console` | `sink.noop` |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `type.roundtrip` | Every declared Arrow type reads back with the declared outcome: exact, coerced by the stated rule, or unsupported with a coded error. | ❌ missing | ❌ missing | ❌ missing | ❌ missing | ❌ missing | ❌ missing |
| `type.null` | A null in every declared type reads back as declared. | ❌ missing | ❌ missing | ❌ missing | ❌ missing | ❌ missing | ❌ missing |
| `type.timestamp.instant` | A timestamp reads back as the same instant. Zone-less is UTC, and the host zone never leaks into the type. | ❌ missing | ❌ missing | ❌ missing | ❌ missing | ❌ missing | ❌ missing |
| `type.nested` | list, struct, list-of-struct and list-of-list read back, or are declared unsupported. Never silently flattened. | ❌ missing | ❌ missing | ❌ missing | ❌ missing | ❌ missing | ❌ missing |
| `type.string.fidelity` | Unicode, escapes and the empty string round-trip byte for byte. | ❌ missing | ❌ missing | ❌ missing | ❌ missing | ❌ missing | ❌ missing |
| `type.undeclared.fails_loud` | An Arrow type absent from the table fails the batch with a coded error. Never coerced silently. | ❌ missing | ❌ missing | ❌ missing | ❌ missing | ❌ missing | ❌ missing |

## Invariants: lifecycle

| Invariant | Claim | `sink.kafka` | `sink.clickhouse` | `sink.iceberg` | `sink.sqlcommand` | `sink.console` | `sink.noop` |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `lifecycle.drain.on_cancel` | Cancel or SIGTERM flushes the buffered batch, then commits. | · | · | · | · | · | · |
| `lifecycle.drain.bounded` | The drain finishes or fails inside its deadline. *(declared, tracked by #161)* | · | · | · | · | · | · |
| `lifecycle.close.idempotent` | Close twice is safe. | ❌ missing | ❌ missing | ❌ missing | ❌ missing | ❌ missing | ❌ missing |
| `pipeline.batch.timeout` | A batch whose query exceeds the timeout fails the batch, not the process. *(declared, tracked by #163)* | · | · | · | · | · | · |

## Invariants: errors

| Invariant | Claim | Verified by |
| --- | --- | --- |
| `error.dlq.carries_provenance` | A DLQ record carries the payload, offset, partition and reason. *(declared, tracked by #166)* | named |
| `error.bad_record.threshold` | N bad records in a window fail the pipeline rather than discarding forever. *(declared, tracked by #166)* | named |

