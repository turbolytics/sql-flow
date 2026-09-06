# Invariant matrix: design

Every integration must prove the same base properties: a failed flush keeps
its rows, a commit follows a flush, a value written is the value read back.
Today that proof is uneven. ClickHouse has a test for "a failed flush keeps
its batch"; Iceberg has the code and no test; the Kafka sink has neither and
violates it. This design declares the invariants once, verifies each one the
same way for every integration, and gates the build on the evidence.

The mechanism is the one `features.yml` already uses: a declared registry, a
generated matrix, and a gate that fails on a declared requirement with no
evidence. It adds a second axis (invariant x integration) and a harness that
produces most of the evidence, so a new integration inherits the whole
contract by supplying a subject rather than by writing tests.

## Why now

The inventory that motivated this, counted on 2026-09-06:

- 3 sources, 6 sinks plus the retry wrapper, 3 handlers.
- Open issues add MQTT source and sink (#192-#196, #201), NATS (#139), SQS
  (#62), Iceberg REST catalog with partitioning and rollover (#204-#208), and
  three vendor catalogs to verify (#211-#213).
- 7 data-integrity defects fixed since v1.0.4, each an invariant violated
  once: arrays (#147), string escapes (#149), ClickHouse arrays (#150),
  nested struct union (#151), timezone shift (#153), offsets committed ahead
  of processing (#154), hollow retry success (#221).
- The ClickHouse docs review (ClickHouse/ClickHouse#117740) found two more
  that were documented wrong rather than coded wrong: NULL coerces to the
  zero value and never evaluates DEFAULT; DateTime strings follow the server
  timezone.
- 137 tests carry invariant-shaped names, attributed per integration, not per
  invariant. "A failed flush keeps its batch" is tested for ClickHouse only.

Two defects the enumeration found, fixed by this work's first two PRs:

- The Kafka sink produces every row in `WriteTable`, and `Flush` clears the
  produce errors before returning them. Rows that failed are gone. A retry
  succeeds with nothing to send: #221's defect, in the sink #221 did not
  touch.
- `Batch()` is not a contract. Console, Kafka, Iceberg and sqlcommand return
  what they hold. ClickHouse returns nil while buffering in `s.tables`.

## Decisions

Made in the design conversation. Each one closes a fork.

| Decision | Choice | Rejected |
|---|---|---|
| Primary consumer | The CI gate. Evidence is a test in this repo. The rendered page is a view of the same data and carries #210's fields. | A proof page whose evidence can be a link. |
| Where an invariant attaches | By kind. Contract invariants attach to the `Sink` and `Source` interfaces and one harness verifies every implementation. Type invariants attach per integration through a declared table. | One harness for everything; named tests per integration. |
| Type enumeration | Declare expected, derive actual, diff is the gate. | Derive only; declare only. |
| Integration-level fault | toxiproxy. A `timeout` toxic hangs the connection, which is the partition #219 fixed for. | `docker pause`. |
| `Reject` seam | Deferred. `sink.error.classifies` stays declared and unenforced until a second seam exists. | Two seams per integration in the first PR. |
| Enforcement order | PR 2 fixes Kafka. PR 3 adds the Iceberg subject and flips the first `requires`. No temporary exemption. | Flip in PR 2 with Iceberg exempt. |

## Invariants

IDs are `family.subject.claim`. Each row is grounded: it has a test
somewhere today, was violated once, or is named by an open issue.

### Resilience: the sink contract

Applies to every `core.Sink`. Verified by the harness at unit level (fake
transport) and integration level (container behind toxiproxy).

| id | Claim | State on 2026-09-06 |
|---|---|---|
| `sink.write.buffers_only` | `WriteTable` does not reach the destination. Only `Flush` does. | Kafka violates. |
| `sink.flush.keeps_batch` | A failed `Flush` leaves every undelivered row buffered. The next `Flush` re-attempts them. | ClickHouse tested. Iceberg has `requeue`, untested. Kafka violates. |
| `sink.flush.no_hollow_success` | `Flush` returns nil only when every row since the last success was acknowledged. | Kafka violates after the first failure. |
| `sink.flush.preserves_order` | Rows reach the destination in `WriteTable` order, across a retry. | Iceberg requeues at head, untested. |
| `sink.flush.honours_context` | `Flush` returns `ctx.Err()` when the context ends. Rows stay buffered. | Kafka tested (#219). Others unknown. |
| `sink.flush.empty_is_noop` | `Flush` with nothing buffered returns nil and touches nothing. | Untested. |
| `sink.batch.reports_buffer` | `Batch()` returns what is buffered, nil when empty. | ClickHouse violates. |
| `sink.error.classifies` | The sink's errors classify as unreachable or rejected, so the ladder retries the right ones. | Ladder tested. Per-sink untested. |
| `sink.probe.fails_start` | A `Prober` with an unreachable destination fails the start once, without retrying. | Tested at ladder level. |

Console, noop and sqlcommand are exempt from `keeps_batch`,
`honours_context`, `classifies` and `probe.fails_start`. Nothing crosses a
network. `retriesHelp` in `internal/sinks/init.go` already encodes that rule.

### Checkpoint: the pipeline and source contract

| id | Claim | State |
|---|---|---|
| `pipeline.commit.after_flush` | Offsets and state commit only after `Flush` returned nil. | Tested in core. |
| `pipeline.commit.nothing_on_failure` | A failed flush commits nothing: not offsets, not state. | Tested in core. |
| `pipeline.state.with_offsets` | Window state and the offsets that produced it commit atomically. | Tested as `state.durability`. |
| `source.commit.only_processed` | A source commits the marks the pipeline processed, never what it fetched. | Kafka integration test (#154). |
| `source.resume.from_committed` | Restart resumes at the committed position: no gap, no replay before it. | Kafka integration test. |
| `source.marks.never_regress` | A committed position never moves backwards. | Tested at state level only. |

Websocket and webhook have no replayable position. They are exempt from the
`source.commit.*` and `source.resume.*` rows, and the exemption is the
documented fact that they are at-most-once.

### Types: per integration, from its declared table

Verified by a generated round-trip: one row of every declared Arrow type,
written through the real integration, read back through the integration's
own reader, diffed against the declaration.

| id | Claim |
|---|---|
| `type.roundtrip` | Every declared Arrow type reads back with the declared outcome: `exact`, `coerced` by the stated rule, or `unsupported` with a coded error. |
| `type.null` | A null in every declared type reads back as declared. For ClickHouse that is the zero value, and the DEFAULT expression is not evaluated. |
| `type.timestamp.instant` | A timestamp reads back as the same instant. Zone-less is UTC (#153). `TIMESTAMPTZ` does not leak the host zone into the type. DuckDB does that today: `timestamp[us, tz=America/New_York]`. |
| `type.nested` | list, struct, list-of-struct and list-of-list read back or are declared unsupported. Never silently flattened. |
| `type.string.fidelity` | Unicode, escapes and the empty string round-trip byte for byte (#149). |
| `type.undeclared.fails_loud` | An Arrow type absent from the table fails the batch with a coded error. Never coerced silently. |

The input boundary gets the same treatment. The inferred handler's JSON to
Arrow table (string, integer, float, bool, null, array, object, and int to
float promotion) is a declared table with JSON-shape keys, and
`type.roundtrip` covers handlers as well as sinks.

The type lattice is closed. DuckDB emits 26 distinct Arrow types across its
33 SQL types (measured through the Python binding on 2026-09-06). A type key
outside that set is a typo. A lattice type absent from an integration's table
is a gap.

### Lifecycle

| id | Claim | State |
|---|---|---|
| `lifecycle.drain.on_cancel` | Cancel or SIGTERM flushes the buffered batch, then commits. | Tested. |
| `lifecycle.drain.bounded` | The drain finishes or fails inside its deadline. | Planned, #161 and #182. |
| `lifecycle.close.idempotent` | `Close` twice is safe on every source and sink. | Webhook only. |

### Declared, not yet enforced

An invariant may carry `tracked_by: "#NNN"` and `requires: []`. It renders
as declared-unenforced and fails nothing until the issue lands and
`requires` is filled. This is how the registry absorbs planned work without
claiming it: `error.dlq.carries_provenance` (#166),
`error.bad_record.threshold` (#166), `source.commit.on_revoke` (#183),
`pipeline.batch.timeout` (#163).

### Out of the list

Throughput, memory and soak (#58, #170) are measurements, not invariants.
Exactly-once is not claimed. The engine is at-least-once, and the list says
so through `keeps_batch` plus `commit.after_flush`.

## Registries

Both live in `docs/coverage/` beside `features.yml`. Both are declared, not
derived, for the reason that file gives: the failure this catches is an
integration with no evidence at all. `features.yml` is unchanged.

### `invariants.yml`: the contract

```yaml
invariants:
  - id: sink.flush.keeps_batch
    family: resilience
    applies_to: sink              # sink | source | handler | pipeline
    claim: >
      A failed Flush leaves every undelivered row buffered; the next Flush
      re-attempts them.
    verified_by: harness          # harness | typetable | named
    requires: [integration]       # empty in PR 1; PR 3 sets this
    violated_once: ["#221"]

  - id: source.commit.on_revoke
    family: checkpoint
    applies_to: source
    claim: Marks commit when a partition is revoked, before the rebalance completes.
    verified_by: harness
    requires: []
    tracked_by: "#183"
```

`verified_by` decides how evidence attaches:

- `harness`: the conformance harness emits a structured marker when a case
  passes. No naming convention. The harness knows what it ran.
- `typetable`: the generated round-trip emits the same marker per
  (integration, type). The invariant is covered only when every declared
  type passed.
- `named`: the `features.yml` mechanism, for the few that live in core.
  `TestPipelineCommitAfterFlush_*` attributes by longest prefix.

### `integrations.yml`: the surface

One entry per case in `sinks.buildSink`, `sources.New` and the handler
switch.

```yaml
integrations:
  - id: sink.clickhouse
    kind: sink
    implements: [Sink, Prober]
    feature: sink.clickhouse         # the features.yml id
    exempt: []
    types:                           # Arrow type -> declared outcome
      int64:         exact
      string:        exact
      timestamp[us]: exact
      "timestamp[us, tz=*]": {coerced: "stored as UTC instant; zone dropped"}
      list<int64>:   exact
      decimal128:    {unsupported: sink.clickhouse.type_unsupported}
      struct:        {unsupported: sink.clickhouse.type_unsupported}
    nulls:
      default:       {coerced: "zero value of the column type; DEFAULT not evaluated"}
      "Nullable(*)": exact

  - id: sink.console
    kind: sink
    implements: [Sink]
    feature: sink.console
    exempt:
      - invariant: sink.flush.keeps_batch
        reason: nothing crosses a network; retriesHelp already excludes it
      - invariant: sink.flush.honours_context
        reason: same

  - id: source.webhook
    kind: source
    implements: [Source]
    feature: source.webhook
    exempt:
      - invariant: source.commit.only_processed
        reason: no replayable position; at-most-once by construction
```

Type keys use Arrow's `DataType.String()` form, so a declaration matches
what the runtime prints with no translation. `tz=*` and `Nullable(*)` are the
only wildcards: the concrete zone and the column DDL are the test's choice.
Handler entries use JSON-shape keys (`number.integer`, `number.float`,
`string`, `bool`, `null`, `array<...>`, `object`).

The generator validates the registries before any test result is read:

1. Every constructor case is declared. `sinks.Kinds()` and `sources.Kinds()`
   return the switch cases; a conformance test diffs them against the
   registry. A new `case "mqtt":` without an entry fails `go test -short`.
2. Every exemption names an invariant that exists and carries a reason.
3. Every type key is in the lattice. Every lattice type is declared, or is a
   gap.

## Harness

One package, `internal/conformance`. It knows the invariants and nothing
about any integration. An integration supplies a subject.

```go
type SinkSubject struct {
    Integration string                     // "sink.kafka"; the integrations.yml id
    New         func(t *testing.T) core.Sink
    Break       func(t *testing.T)         // the destination stops answering
    Heal        func(t *testing.T)         // and comes back
    ReadBack    func(t *testing.T) []Row   // what the destination holds, in order
}

func Sinks(t *testing.T, s SinkSubject)
func Sources(t *testing.T, s SourceSubject)
```

`Row` is `map[string]any`, one per destination row, decoded by the subject
through the destination's own reader: a ClickHouse query, an Iceberg scan, a
Kafka consume from the earliest offset. The harness compares rows by value;
it never inspects the sink.

`Sinks` runs one sequence and asserts each claim as its own subtest, so each
claim gets its own marker and its own cell:

1. `WriteTable(A)`.
2. `Break`. `Flush` must return an error.
3. `Heal`. `Flush` must return nil.
4. `ReadBack` must be `[A]`.

That sequence proves `keeps_batch`. Adding `WriteTable(B)` between steps 2
and 3 and asserting `[A, B]` proves `no_hollow_success` and
`preserves_order`. `honours_context` is the same break with a cancelled
context. `empty_is_noop` and `reports_buffer` are one call each. Every
later invariant is another assertion on this sequence, not another
sequence.

On each passing subtest the harness calls:

```go
coverage.Invariant(t, "sink.flush.keeps_batch", s.Integration)
// -> t.Logf("COVERS invariant=sink.flush.keeps_batch integration=sink.kafka")
```

and `coverage.Covers(t, s.Integration)` once, so `features.yml` credits the
integration too. A test that carries any marker is excluded from the
"unattributed" report.

### Two levels, one harness

| Level | `New` | `Break` / `Heal` | `ReadBack` |
|---|---|---|---|
| unit (`-short`) | the real sink over a fake transport | flip the fake | the fake's log |
| integration | the real sink over a container behind toxiproxy | add / remove a `timeout` toxic | query the container |

`conformance.Proxy(t, upstream)` wraps the testcontainers toxiproxy module
and returns the proxy address plus `Break` and `Heal`. Every integration
shares it. The `timeout` toxic hangs the connection rather than refusing it,
because a hang is the partition #219 fixed for and the case
`honours_context` needs.

Unit level needs a seam per sink: the transport behind an interface.
ClickHouse holds `driver.Conn`, already an interface. Kafka holds
`*kgo.Client` and needs one with `Produce` and `Flush`. Iceberg holds
`*table.Table` and needs one with `AppendTable`. Three seams, no behaviour
change.

### Kafka behind a proxy

The testcontainers Kafka module advertises `localhost:<mapped port>`. A
client that bootstraps through a proxy is told the broker's real address and
reconnects around the proxy. The Kafka sink takes a `kgo.Dialer` option that
rewrites the advertised address to the proxy at dial time. Host-side, no
broker configuration, and it is the seam a unit-level fake uses.

ClickHouse has no redirect. A DSN pointed at the proxy stays on it.

### Exemptions

A subject with nothing to break sets `Break: nil`. The harness skips the
network-shaped invariants with `t.Skip`. A skip is not coverage, so the cell
stays missing until `integrations.yml` carries the exemption with a reason.
Two independent statements that must agree.

## Generator and matrix

`scripts/coverage_matrix.py` grows a second axis. It loads both registries,
validates them, parses the structured markers from every suite report, and
renders invariant x integration x level. A required level with no passing
evidence for a non-exempt integration is a gap, and gaps fail
`make coverage-check` exactly as feature gaps do.

`matrix.json` gains an `invariants` section beside `features`. `matrix.md`
gains one table per family. Exempt cells render as `—` with the reason on
hover in the page; missing cells render as missing. Declared-unenforced
invariants render with their `tracked_by` issue.

The rendered page carries the fields #210 needs so that work adds evidence
rather than a second registry: versions exercised (sqlflow, the integration
client, the container image), the last green date per cell, and red rows
that stay visible.

## Rollout

Three PRs. Each merges green.

### PR 1: the foundation

1. Both registries, fully declared. Every invariant carries `requires: []`.
   No `types` blocks.
2. `internal/conformance` with `SinkSubject`, `Sinks`, and `Proxy`. One
   sequence, one assertion, one marker: `sink.flush.keeps_batch`.
3. One subject: ClickHouse at integration level, DSN pointed at the proxy.
   The existing `FailedFlushKeepsTheBatchBuffered` test shows the invariant
   holds, so the cell goes green.
4. `coverage.Invariant`, the marker parser, registry validation rules 1
   and 2, `sinks.Kinds()` and the agreement test, the second matrix axis and
   its rendering.

On merge the matrix shows `sink.flush.keeps_batch`: ClickHouse covered,
Kafka missing, Iceberg missing, console and noop and sqlcommand exempt.
Nothing fails. A gate cannot turn on until every non-exempt integration has
a subject, and Kafka's cannot pass until Kafka is fixed.

### PR 2: Kafka

Test first against the foundation. Add the Kafka subject with the dialer
seam. Watch `keeps_batch` fail with the harness's message. Then fix the sink:

- `WriteTable` buffers rows and produces nothing. That is `buffers_only`,
  and what the retry wrapper already assumes.
- `Flush` produces the buffer, waits on the context, and keeps every row
  whose promise failed. Kept rows go to the head, in order, so a retry
  re-sends those rows and nothing that was acknowledged.
- `Batch()` returns the buffer.

One fix satisfies `buffers_only`, `keeps_batch`, `no_hollow_success`,
`preserves_order` and `reports_buffer`. `requires` stays empty.

### PR 3: Iceberg, and the first enforced invariant

Add the Iceberg subject. The SQL-catalog sink has no network to proxy;
`Break` makes the warehouse directory read-only. Then flip
`sink.flush.keeps_batch` to `requires: [integration]`. Every non-exempt sink
has a subject, so the gate is legal, and from that commit a new sink cannot
merge without proving it.

### After

Additive, in any order:

- One assertion per new invariant on the existing sequence.
- Unit-level fakes behind the three seams, then `requires: [unit, integration]`.
- The `Sources` harness against the Kafka source.
- The `Reject` seam, then `sink.error.classifies`.
- The type table and its generator.
- `matrix.md` grows the #210 fields.

## Out of scope

- Fixing the seven coverage-matrix findings from the 2026-09-06 review that
  this design does not touch: the `sorted()` crash, the failing-test grep,
  the double-counted integration tests, the covered-vs-gaps disagreement,
  missing-report greps, and the ClickHouse skip-on-absent. The unattributed
  report change is in scope because the harness depends on it.
- Property-based or fuzzed inputs.
- Any invariant the list marks planned.
