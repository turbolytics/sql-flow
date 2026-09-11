# Feature coverage matrix

Generated from `docs/coverage/status/` by `make coverage-page`.
Do not edit by hand.

Features are declared in `docs/coverage/features.yml`. A test attaches
to one by saying so: `coverage.Covers(t, "sink.clickhouse")` in Go, or
the `covers` marker in pytest. A test used to attach by the shape of
its name, which credited a test whose name merely started the same way
and credited nothing when a name drifted.

Levels are derived from where a test ran, never declared, so they
cannot drift. `unit` is `go test -short`, `integration` is the Go
tests that need a real service, `release` is the image suite. A
**skipped** test is not coverage at any of them: a skip that reads as
a pass is how `sink.iceberg` shipped for months without ever being
written to.

Only statuses are committed. Test names and counts are in the coverage
report CI publishes on every run: they change whenever a test is
added, and this page changes only when a status does.

| Feature | What it does | unit | integration | release |
| --- | --- | --- | --- | --- |
| `source.kafka` | Consumes a Kafka topic, tracking offsets and leader epochs. | ✅ | ✅ | ✅ |
| `source.webhook` | Accepts records over HTTP, with optional HMAC signature checks. | ✅ | — | — |
| `source.websocket` | Consumes a websocket stream, reconnecting on drop. | ✅ | — | ✅ |
| `sink.kafka` | Publishes result rows to a Kafka topic. | ✅ | ✅ | ✅ |
| `sink.clickhouse` | Inserts result batches into a ClickHouse table. | ✅ | ✅ | ✅ |
| `sink.iceberg` | Appends result batches to an Iceberg table through a catalog. | ✅ | — | ✅ |
| `sink.parquet` | Writes result batches as parquet files to a local path. | — | — | ✅ |
| `sink.sqlcommand` | Runs a SQL command against the pipeline's own DuckDB connection. | ✅ | — | — |
| `sink.console` | Writes result rows to stdout as JSON. | ✅ | — | ✅ |
| `sink.noop` | Discards every result row, for measuring the engine without a sink. | ✅ | — | — |
| `sink.retry` | Retries a sink whose destination is not answering, bounded by a deadline. | ✅ | — | — |
| `handler.inferred_mem` | Infers a schema per batch and runs the query in memory. | ✅ | — | ✅ |
| `handler.inferred_disk` | Infers a schema per batch, staging the batch on disk. | ✅ | — | — |
| `handler.structured` | Binds a declared schema, ingesting through Arrow. | ✅ | — | ✅ |
| `state.durability` | Window state and the offsets that produced it commit together. | ✅ | — | ✅ |
| `state.offsets` | Kafka positions are stored in DuckDB and resumed on restart. | ✅ | — | ✅ |
| `state.corruption` | A damaged state file fails the start rather than silently resetting. | ✅ | — | ✅ |
| `lifecycle.drain` | SIGTERM writes the buffered batch before exiting. | ✅ | — | ✅ |
| `lifecycle.exit_codes` | The process exit status carries the error code a supervisor reads. | ✅ | — | ✅ |
| `core.consume_loop` | Accumulates a batch, flushes it, and commits in that order. | ✅ | — | — |
| `error.taxonomy` | Every failure carries a class.domain.reason code. | ✅ | — | — |
| `error.raise` | Policy RAISE stops the pipeline on a bad record. | ✅ | — | — |
| `error.ignore` | Policy IGNORE drops a bad record and keeps the pipeline running. | ✅ | — | ✅ |
| `error.dlq` | Policy DLQ diverts a bad record to a sink instead of dropping it. | ✅ | — | ✅ |
| `manager.tumbling_window` | Publishes and deletes closed windows on an interval. | ✅ | — | ✅ |
| `config.templating` | Renders a config through Jinja2 against SQLFLOW_ environment variables. | ✅ | — | ✅ |
| `config.validation` | Validates a config against the schema and reports where it is wrong. | ✅ | — | ✅ |
| `validate.template` | Reports referenced, provided, missing, and unused template variables. | ✅ | — | — |
| `validate.schema` | Validates a rendered config against the config JSON Schema, naming the line. | ✅ | — | — |
| `observability.metrics` | Exports pipeline counters and histograms over Prometheus. | ✅ | — | — |
| `observability.turbostats` | Reports the process's own state as one versioned document, served at /turbostats/v1. | ✅ | — | ✅ |
| `observability.debug_api` | Serves ad-hoc SQL against the live DuckDB connection. | ✅ | — | — |
| `cli.invocation` | Resolves the config path and message limits from either flag form. | ✅ | — | — |
| `cli.dev_invoke` | Runs a pipeline against a fixture file, without a source. | ✅ | — | ✅ |
| `cli.version` | The shipped binary reports the version it was built from. | — | — | ✅ |
| `tooling.conformance` | The harness proves the declared invariants for any integration. | ✅ | — | — |
| `tooling.coverage` | Tests attribute to features and invariants, and the registries match the code. | ✅ | — | — |

**37 features declared. 37 have at least one passing test attributed at every level they require, so 0 gap(s).**

That sentence counts attribution, not proof. A feature is green here when
a test named for it ran and passed; it says nothing about whether the
integration behind it keeps a batch it could not deliver, or commits
offsets only after a flush. Those are invariants, they are counted
separately below, and the two numbers are not interchangeable.

**32 invariants declared: 28 safety and 4 liveness. Of 136 (invariant, integration) cells: 57 proven, 51 missing, 0 skipped, 0 failing, 28 exempt. 0 gap(s).**

Safety says nothing bad happens. Liveness says something good
eventually does, and the two are not interchangeable: a sink that
never flushes satisfies every safety invariant on this page.
`keeps_batch` holds if you never flush, and `commit.after_flush`
holds if you never commit. Only a liveness claim says the pipeline
does anything at all.


# Invariant matrix

Generated from `docs/coverage/status/` by `make coverage-page`.
Do not edit by hand.

Invariants are declared in `docs/coverage/invariants.yml`, integrations
one per file in `docs/coverage/integrations/`. A cell is proven by the
conformance harness in `internal/conformance`, which emits a marker
naming both ids -- a harness test's name says nothing, because the same
code runs for every integration.

A covered cell names the levels that proved it: `u` unit, `i`
integration, `r` release. That is the question the matrix exists to
answer -- proven with a fake, or against the real thing, or in the
shipped image. **exempt** carries its reason in the integration's own
file, and **missing** means no evidence. Nothing here fails the build
until an invariant's `requires` is filled in, and none is yet.

## Safety invariants: resilience

| Invariant | Claim | `sink.clickhouse` | `sink.console` | `sink.iceberg` | `sink.kafka` | `sink.noop` | `sink.sqlcommand` |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `sink.write.buffers_only` | WriteTable does not reach the destination. Only Flush does. | ✅ i | ✅ u | ✅ u | ✅ i | — exempt | ✅ u |
| `sink.flush.keeps_batch` | A failed Flush leaves every undelivered row buffered. The next Flush re-attempts them. Delivery is at-least-once, so a row that arrives twice holds the claim and a row that never arrives breaks it. *(violated once: #221)* | ✅ i | ✅ u | ✅ u | ✅ i | — exempt | ✅ u |
| `sink.flush.no_hollow_success` | Flush returns nil only when every row since the last success was acknowledged by the destination. *(violated once: #221)* | ✅ i | ✅ u | ✅ u | ✅ i | — exempt | ✅ u |
| `sink.rows.counted_on_delivery` | sink_rows_written counts a row once, when a Flush acknowledged it. A failed flush counts nothing; the retry that delivers those rows counts them. | ✅ i | ✅ u | ✅ u | ✅ i | ❌ missing | ✅ u |
| `sink.flush.preserves_order` | Rows reach the destination in WriteTable order, across a retry. Repeats are permitted, because delivery is at-least-once; a row that overtakes one written before it is not. | ✅ i | ✅ u | ❌ missing | ✅ i | — exempt | ✅ u |
| `sink.flush.honours_context` | Flush returns ctx.Err() when the context ends. Rows stay buffered. *(violated once: #219)* | ✅ i | — exempt | — exempt | ✅ i | — exempt | — exempt |
| `sink.flush.empty_is_noop` | Flush with nothing buffered returns nil and touches nothing. | ✅ i | ✅ u | ✅ u | ✅ i | — exempt | ✅ u |
| `sink.buffer.reports_depth` | A sink reports how many rows it is holding, and the count rises when a flush fails and falls to zero when one succeeds. | ✅ i | ✅ u | ✅ u | ✅ i | — exempt | ✅ u |
| `sink.error.classifies` | The sink's errors classify as unreachable or rejected, so the retry ladder retries the right ones. | ❌ missing | ❌ missing | ❌ missing | ❌ missing | — exempt | ❌ missing |
| `sink.probe.fails_start` | A Prober whose destination is unreachable fails the start once, without retrying. | ✅ i | — exempt | — exempt | ✅ i | — exempt | — exempt |

## Safety invariants: checkpoint

| Invariant | Claim | `source.kafka` | `source.webhook` | `source.websocket` |
| --- | --- | --- | --- | --- |
| `source.commit.only_processed` | A source commits the marks the pipeline processed, never what it fetched. *(violated once: #154)* | ❌ missing | — exempt | — exempt |
| `source.resume.from_committed` | Restart resumes at the committed position. No gap, and no replay before it. | ❌ missing | — exempt | — exempt |
| `source.marks.never_regress` | A committed position never moves backwards. | ❌ missing | — exempt | — exempt |
| `source.commit.on_revoke` | Marks commit when a partition is revoked, before the rebalance completes. *(declared, tracked by #183)* | ❌ missing | — exempt | — exempt |

These checkpoint invariants are properties of the consume loop
rather than of anything a config file names. The columns are
its configurations, and `internal/conformance` runs each one
through every path that reaches a batch: the batch filling, the
flush interval elapsing, the source closing, and a cancel that
drains. An invariant holds only if it holds on all four.

| Invariant | Claim | `pipeline.stateful` | `pipeline.stateless` |
| --- | --- | --- | --- |
| `pipeline.commit.after_flush` | Offsets and state commit only after Flush returned nil. | ✅ u | ✅ u |
| `pipeline.commit.only_delivered_rows` | The pipeline never commits a position covering a row the destination did not take, and never leaves a delivered row uncommitted after a clean run. *(violated once: #154)* | ✅ u | ✅ u |
| `pipeline.commit.nothing_on_failure` | A failed flush commits nothing. Not offsets, not state. | ✅ u | ✅ u |
| `pipeline.state.with_offsets` | Window state and the offsets that produced it commit atomically. | ✅ u | — exempt |

## Safety invariants: types

| Invariant | Claim | `sink.clickhouse` | `sink.console` | `sink.iceberg` | `sink.kafka` | `sink.noop` | `sink.sqlcommand` |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `type.roundtrip` | Every declared Arrow type reads back with the declared outcome: exact, coerced by the stated rule, or unsupported with a coded error. *(violated once: #147, #150, #151)* | ✅ i | ❌ missing | ❌ missing | ❌ missing | ❌ missing | ❌ missing |
| `type.null` | A null in every declared type reads back as declared. | ✅ i | ❌ missing | ❌ missing | ❌ missing | ❌ missing | ❌ missing |
| `type.timestamp.instant` | A timestamp reads back as the same instant. Zone-less is UTC, and the host zone never leaks into the type. *(violated once: #153)* | ✅ i | ❌ missing | ❌ missing | ❌ missing | ❌ missing | ❌ missing |
| `type.nested` | list, struct, list-of-struct and list-of-list read back, or are declared unsupported. Never silently flattened. | ✅ i | ❌ missing | ❌ missing | ❌ missing | ❌ missing | ❌ missing |
| `type.string.fidelity` | Unicode, escapes and the empty string round-trip byte for byte. *(violated once: #149)* | ✅ i | ❌ missing | ❌ missing | ❌ missing | ❌ missing | ❌ missing |
| `type.undeclared.fails_loud` | An Arrow type absent from the table fails the batch with a coded error. Never coerced silently. | ✅ i | ❌ missing | ❌ missing | ❌ missing | ❌ missing | ❌ missing |

## Safety invariants: lifecycle

| Invariant | Claim | `sink.clickhouse` | `sink.console` | `sink.iceberg` | `sink.kafka` | `sink.noop` | `sink.sqlcommand` |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `lifecycle.close.idempotent` | Close twice is safe. | ✅ i | — exempt | — exempt | ✅ i | — exempt | — exempt |

These lifecycle invariants are properties of the consume loop
rather than of anything a config file names. The columns are
its configurations, and `internal/conformance` runs each one
through every path that reaches a batch: the batch filling, the
flush interval elapsing, the source closing, and a cancel that
drains. An invariant holds only if it holds on all four.

| Invariant | Claim | `pipeline.stateful` | `pipeline.stateless` |
| --- | --- | --- | --- |
| `lifecycle.drain.on_cancel` | Cancel or SIGTERM flushes the buffered batch, then commits. | ✅ u | ✅ u |

## Safety invariants: errors

These errors invariants are properties of the consume loop
rather than of anything a config file names. The columns are
its configurations, and `internal/conformance` runs each one
through every path that reaches a batch: the batch filling, the
flush interval elapsing, the source closing, and a cancel that
drains. An invariant holds only if it holds on all four.

| Invariant | Claim | `pipeline.stateful` | `pipeline.stateless` |
| --- | --- | --- | --- |
| `error.dlq.carries_provenance` | A DLQ record carries the payload, offset, partition and reason. *(declared, tracked by #166)* | ❌ missing | ❌ missing |
| `error.bad_record.threshold` | N bad records in a window fail the pipeline rather than discarding forever. *(declared, tracked by #166)* | ❌ missing | ❌ missing |

## Liveness invariants: lifecycle

These lifecycle invariants are properties of the consume loop
rather than of anything a config file names. The columns are
its configurations, and `internal/conformance` runs each one
through every path that reaches a batch: the batch filling, the
flush interval elapsing, the source closing, and a cancel that
drains. An invariant holds only if it holds on all four.

| Invariant | Claim | `pipeline.stateful` | `pipeline.stateless` |
| --- | --- | --- | --- |
| `pipeline.flush.eventually` | A batch that never reaches batchSize still reaches the sink, within the flush interval. | ✅ u | ✅ u |
| `pipeline.progress.no_silent_stall` | A configuration that cannot make progress fails at startup rather than running quietly. flush_interval_seconds of 0 removes the ticker entirely, so a batch a low-traffic topic never fills waits forever. Unenforced and untracked: the engine permits the configuration today. | ❌ missing | ❌ missing |
| `lifecycle.drain.bounded` | The drain finishes or fails inside its deadline. *(declared, tracked by #161)* | ❌ missing | ❌ missing |
| `pipeline.batch.timeout` | A batch whose query exceeds the timeout fails the batch, not the process. *(declared, tracked by #163)* | ❌ missing | ❌ missing |

