# ClickHouse invariants and the published page they back

Approved in conversation on 2026-10-10. Spec, plan and code share this branch
and one PR.

## The problem

The ClickHouse integration page (ClickHouse/ClickHouse#117740) makes claims
about SQLFlow's behavior. Only two kinds of claim have evidence in CI today:
the type table, proven cell by cell by `TestIntegrationSinkClickhouse_Types`,
and the worked examples' configs, which CI validates and loads. Every
behavioral claim was proven by hand-run scripts.

Five of those claims went stale between v1.1.0 and v2026.10.08 with no CI
signal: the retry contract, the flush interval floor, window closing, a
throughput row and the version pins. The next behavior change will do the
same.

## The goal

1. Every claim on the page maps to a declared entry.
2. Every entry has evidence that runs in CI.
3. A behavior change fails CI and names the claim.
4. A claim on the page with no entry fails CI.

## Decisions

- **ClickHouse claims are invariants**, in `docs/coverage/invariants.yml`,
  named `clickhouse.*`, reported by the same harness markers, generator and
  status files as every other invariant. No parallel registry.
- **`applies_to` accepts an integration id** as well as a kind. A
  `clickhouse.*` invariant applies to `sink.clickhouse` alone, so no other
  sink gets a cell for it.
- **The invariants file stays runtime-only.** A tooling claim, such as a
  `sqlflow validate` warning, is proven by a well-named test and cited
  through its feature.
- **Measurements stay measurements.** Throughput, the worked examples' counts
  and Cloud-only messages cite committed output files that name their
  versions. Nothing asserts a throughput.
- **The page's source of truth lives in this repo**, at
  `docs/published/clickhouse.mdx`. The ClickHouse fork receives a copy, by
  hand, with the user's go.

## The invariants

Each sets `applies_to: sink.clickhouse`, `verified_by: harness`, and
`requires` at the level its evidence runs, so absent evidence fails
`make coverage-check`.

| Invariant | Claim | Level |
| --- | --- | --- |
| `clickhouse.insert.by_name` | Columns are matched by name. A produced column the table lacks fails the batch as `system.sink.write_failed`, naming the column. A column the handler omits takes the table's DEFAULT. | integration |
| `clickhouse.table.not_created` | The sink never creates or alters a table. A table that does not exist fails the batch as `system.sink.write_failed`. | integration |
| `clickhouse.enum.null_is_zero` | A NULL into a non-Nullable Enum is stored as 0. With no 0 member the INSERT succeeds and reads of the column fail with `UNKNOWN_ELEMENT_OF_ENUM`. With a 0 member it reads as that member. Nullable reads NULL. | integration |
| `clickhouse.encode.fails_once` | A value the driver cannot encode fails the batch on the first attempt as `user.sink.encode_failed`. It is not retried, and the process exits with code 10. | integration |
| `clickhouse.temporal_text.two_shapes` | Text into a DateTime column takes `YYYY-MM-DD hh:mm:ss`, read as UTC, or the same with an offset, which is applied. Any other shape fails as `user.sink.encode_failed`. | integration |
| `clickhouse.decimal.text_only` | A Decimal column accepts its value as text. A float fails as `user.sink.encode_failed`. | integration |
| `clickhouse.replay.duplicates_until_merge` | A batch delivered twice into a ReplacingMergeTree reads twice until a merge, and once with FINAL. An identical block inside the table's deduplication window is dropped at insert. | integration |
| `clickhouse.flush.one_insert` | One flush is one INSERT, on one connection. | integration |
| `clickhouse.dsn.scheme` | The DSN scheme decides the protocol, TLS and the default port. `@` and `:` in a password parse raw. `/`, `#` and `?` must be percent-encoded. | unit |
| `clickhouse.tls.verifies` | With TLS, the server certificate is verified against the system roots. No option skips verification. | unit |
| `clickhouse.auth.fails_start` | Wrong credentials fail at startup with HTTP 401 and code 194, before any message is consumed. | integration |
| `clickhouse.type_text.per_destination` | Text into a non-String column must already be in that column's text form. The type table proves each destination with its own value. | integration |

Claims the existing invariants already back are cited, not re-proven:
`type.roundtrip`, `type.null`, `type.timestamp.instant`, `type.nested`,
`type.string.fidelity`, `type.undeclared.fails_loud`,
`pipeline.commit.after_flush`, `sink.probe.fails_start`,
`pipeline.flush.eventually`, `lifecycle.drain.on_cancel` and
`pipeline.window.counts_every_row`.

## Evidence

- New integration tests sit beside `TestIntegrationSinkClickhouse_Types` in
  `internal/sinks`, reuse its shared ClickHouse container, and emit
  `COVERS invariant=clickhouse.<id> integration=sink.clickhouse` through
  `internal/coverage`. The scenario reads as a story: what is written, what
  the table holds, what a read returns.
- Existing tests that already prove a claim gain the marker rather than a
  duplicate test.
- `clickhouse.encode.fails_once` needs the pipeline and the CLI, since the
  retry decision and the exit code live there. Its test runs the pipeline,
  not only the sink.
- The hand-run probe, dedup and window scripts retire. The throughput scripts
  and their output move to `docs/published/clickhouse/` as measured evidence.

## The page and its check

The page carries an anchor beside each claim, as an MDX comment:

- `{/* invariant: <id> */}` cites an invariant.
- `{/* measured: <path> */}` cites a committed output file.
- `{/* config: <file> */}` precedes a YAML block that must equal
  `dev/config/examples/published/<file>`.
- `{/* feature: <id> */}` cites a tooling claim's feature.

A page check in `scripts/coverage_matrix`, run by `make coverage-check` and
tested in `tests/tooling`, fails when:

1. an `invariant` anchor names an id `invariants.yml` does not declare;
2. a `clickhouse.*` invariant is cited nowhere on the page;
3. a `measured` anchor names a file that does not exist;
4. a `config` block differs from its published file;
5. a `feature` anchor names an id `features.yml` does not declare;
6. the page's type table differs from the one the generator renders.

The generated type table renders `VARCHAR` alone, with the text-form note
the reviewer accepted, instead of `VARCHAR`, `UUID`, `JSON` as one row.

## CI

`.github/workflows/ci.yml` skips a pull request that changes only `docs/**`
and Markdown. It re-includes `docs/coverage/**`. It must also re-include
`docs/published/**`, or a page-only edit skips the check that guards it.

## What breaks if this is wrong

A claim on the page drifts from the engine with no CI signal, which is the
failure this replaces. A wrong `requires` level fails `coverage-check` on a
claim whose evidence exists at another level.
