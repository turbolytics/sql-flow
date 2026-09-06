# Invariant Matrix Foundation (PR 1) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Declare every invariant and every integration in two registries, verify one invariant (`sink.flush.keeps_batch`) for one integration (ClickHouse) through a reusable conformance harness behind toxiproxy, and render the result as a second axis of the coverage matrix. Nothing is enforced yet; the build stays green.

**Architecture:** Two declared YAML registries beside `features.yml`. A Go package `internal/conformance` that knows the invariants and drives any sink through a `SinkSubject` (constructor, break, heal, read-back). Evidence is emitted as a structured `COVERS invariant=… integration=…` log line, which `scripts/coverage_matrix.py` parses into an invariant × integration × level table. The integration-level fault is a toxiproxy `timeout` toxic between the sink and a ClickHouse container.

**Tech Stack:** Go 1.25, `github.com/zeebo/assert`, testcontainers-go v0.44.0 (`modules/clickhouse`, `modules/toxiproxy`), `github.com/Shopify/toxiproxy/v2/client` v2.12.0, `gopkg.in/yaml.v3`, Python 3.11 via `uv run --locked`, pytest 7.4.3, PyYAML 6.0.1.

**Spec:** `docs/superpowers/specs/2026-09-06-invariant-matrix-design.md`

## Global Constraints

- All prose (commit messages, comments, YAML comments, docs) follows Google Technical Writing One; see `CLAUDE.md`. Comments explain why, not what.
- Commit messages name the defect, the fix, and the evidence, and state what breaks if the change is wrong. No attribution trailers.
- Python runs through `uv run --locked`; never bare `python3` or `pytest`.
- Go tests use `github.com/zeebo/assert`, never `testify`.
- Integration tests are named `TestIntegration<Feature>_<Behaviour>`. `-short` must skip them before any container starts. They must never skip for any other reason.
- Every invariant in `invariants.yml` carries `requires: []` in this PR. Enforcement is PR 3.
- No `types:` blocks in `integrations.yml` in this PR.
- Container images are pinned: `clickhouse/clickhouse-server:26.8`, `ghcr.io/shopify/toxiproxy:2.12.0`.
- `internal/conformance` must not import `internal/sinks`, `internal/kafka`, `internal/webhook` or `internal/websocket`. Those packages' tests import it, and Go forbids the cycle.
- `docs/coverage/matrix.json` and `matrix.md` are regenerated, never edited by hand.

---

## File map

| File | Responsibility |
|---|---|
| `internal/coverage/coverage.go` | Modify. Add `Invariant(t, invariant, integration)`: the structured marker. |
| `docs/coverage/invariants.yml` | Create. The contract: every invariant from the spec, `requires: []`. |
| `docs/coverage/integrations.yml` | Create. The surface: every constructor case, `implements`, `exempt`. |
| `scripts/coverage_matrix.py` | Modify. Parse structured markers; load and validate registries; build and render the invariant axis; skip marker-carrying tests in the unattributed report. |
| `tests/tooling/test_coverage_matrix.py` | Modify. Tests for each generator change. |
| `internal/sinks/init.go` | Modify. `buildSink` becomes a lookup in a `builders` map; `Kinds()` returns its keys. |
| `internal/sources/init.go` | Modify. Same shape: `builders` map, `Kinds()`. |
| `internal/handlers/init.go` | Modify. Same shape: `builders` map, `Kinds()`. |
| `internal/coverage/registry.go` | Create. Loads `integrations.yml` for the agreement tests. Lives in `coverage` so it imports nothing under `internal/` that imports it back. |
| `internal/sinks/registry_test.go`, `internal/sources/registry_test.go`, `internal/handlers/registry_test.go` | Create. `Kinds()` equals the registry's ids of that kind. |
| `internal/conformance/conformance.go` | Create. `Row`, `SinkSubject`, `Sinks`. |
| `internal/conformance/conformance_test.go` | Create. The harness against an in-memory sink that keeps its batch, and one that drops it. |
| `internal/conformance/proxy.go` | Create. `Proxy`: a toxiproxy container in front of an upstream, with `Break` and `Heal`. |
| `internal/sinks/conformance_test.go` | Create. `TestIntegrationSinkClickhouse_Conformance`: ClickHouse container + proxy + the subject. |
| `docs/coverage/matrix.json`, `docs/coverage/matrix.md` | Regenerated. |
| `go.mod`, `go.sum` | Three new requirements. |

---

### Task 1: The structured marker, in Go and in the parser

**Files:**
- Modify: `internal/coverage/coverage.go`
- Modify: `scripts/coverage_matrix.py:93-127` (`COVERS`, `parse_go`)
- Modify: `scripts/coverage_matrix.py:130-143` (`parse_pytest`)
- Test: `tests/tooling/test_coverage_matrix.py`

**Interfaces:**
- Produces: `coverage.Invariant(t *testing.T, invariant, integration string)` — logs `COVERS invariant=<id> integration=<id>`.
- Produces: `parse_go(path) -> (results, covers, invariants)` where `invariants` is `{test_name: [(invariant_id, integration_id), ...]}`. `parse_pytest` returns the same triple; its third element is always `{}` in this PR.

- [ ] **Step 1: Write the failing parser test**

Append to `tests/tooling/test_coverage_matrix.py`, after `test_parse_go_ignores_package_level_events`:

```python
def test_parse_go_reads_structured_invariant_markers(tmp_path):
    """The harness emits one line per (invariant, integration). Both ids are
    carried; the parser must not collapse them into the feature list."""
    path = write(tmp_path, "go.json", "\n".join([
        json.dumps({"Action": "output", "Test": "TestA",
                    "Output": "    x.go:1: COVERS invariant=sink.flush.keeps_batch integration=sink.clickhouse\n"}),
        json.dumps({"Action": "output", "Test": "TestA",
                    "Output": "    x.go:2: COVERS sink.clickhouse\n"}),
        json.dumps({"Action": "pass", "Test": "TestA"}),
    ]))
    results, covers, invariants = cm.parse_go(path)

    assert results == {"TestA": cm.PASS}
    assert covers == {"TestA": ["sink.clickhouse"]}
    assert invariants == {"TestA": [("sink.flush.keeps_batch", "sink.clickhouse")]}


def test_parse_pytest_returns_an_empty_invariant_map(tmp_path):
    path = write(tmp_path, "pytest.json", json.dumps({"tests": [
        {"nodeid": "test_a", "outcome": "passed", "covers": []},
    ]}))
    results, covers, invariants = cm.parse_pytest(path)
    assert invariants == {}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run --locked pytest tests/tooling/test_coverage_matrix.py -k "structured_invariant or empty_invariant_map" -v`
Expected: FAIL — `ValueError: not enough values to unpack (expected 3, got 2)`.

- [ ] **Step 3: Change the parsers**

In `scripts/coverage_matrix.py`, replace the `COVERS` regex and `parse_go` (lines 93–127) with:

```python
# A plain marker names an extra feature. A structured one names an invariant
# and the integration it was proven on; the harness emits those, and a test
# name cannot carry two ids.
COVERS = re.compile(r"COVERS ([a-z0-9_.]+)(?:\s|$)")
COVERS_INVARIANT = re.compile(
    r"COVERS invariant=([a-z0-9_.]+) integration=([a-z0-9_.]+)")


def parse_go(path):
    """Read `go test -json` into ({test: outcome}, {test: [features]},
    {test: [(invariant, integration)]}).

    Markers come from coverage.Covers and coverage.Invariant, which write to
    the test log. `go test -json` carries that as an output event, so both are
    read from ordinary suite output with no plugin and no build tag.
    """
    results, covers, invariants = {}, {}, {}
    with open(path) as fh:
        for line in fh:
            line = line.strip()
            if not line.startswith("{"):
                continue
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue
            name, action = event.get("Test"), event.get("Action")
            if not name:
                continue
            if action == "output":
                out = event.get("Output", "")
                structured = COVERS_INVARIANT.findall(out)
                if structured:
                    invariants.setdefault(name, []).extend(structured)
                    continue
                found = COVERS.findall(out)
                if found:
                    covers.setdefault(name, []).extend(found)
            # Subtests report their own outcome; the parent aggregates them.
            elif action == "pass":
                results.setdefault(name, PASS)
            elif action == "skip":
                results[name] = SKIP if results.get(name) != PASS else PASS
            elif action == "fail":
                results[name] = FAIL
    return results, covers, invariants
```

The `continue` after a structured match matters: `COVERS invariant=...` would otherwise also match the plain pattern and record `invariant` as a feature.

Change `parse_pytest`'s last line from `return results, covers` to `return results, covers, {}`.

Update the two call sites in `main()` (lines 395–403):

```python
    go_results, go_covers, go_invariants = (
        parse_go(args.go) if args.go and os.path.exists(args.go) else ({}, {}, {}))
    it_results, it_covers, it_invariants = (
        parse_go(args.go_integration)
        if args.go_integration and os.path.exists(args.go_integration)
        else ({}, {}, {}))
    py_results, py_covers, py_invariants = (
        parse_pytest(args.pytest)
        if args.pytest and os.path.exists(args.pytest) else ({}, {}, {}))
```

The three `*_invariants` values are unused until Task 3. Update the two existing parser tests (`test_parse_go_reads_outcomes_and_markers`, `test_parse_pytest_reads_outcomes_and_markers`) to unpack three values: `results, covers, _ = cm.parse_go(path)`.

- [ ] **Step 4: Run the whole tooling suite**

Run: `uv run --locked pytest tests/tooling -q`
Expected: 46 passed.

- [ ] **Step 5: Write the failing Go test for the marker**

Create `internal/coverage/coverage_test.go`:

```go
package coverage

import (
	"strings"
	"testing"

	"github.com/zeebo/assert"
)

// The generator parses this line with a fixed regex. A format drift here is a
// silent loss of every invariant cell, so the exact bytes are pinned.
func TestCoverageInvariant_EmitsTheStructuredMarker(t *testing.T) {
	var got string
	rec := &recorder{t: t, sink: &got}
	Invariant(rec, "sink.flush.keeps_batch", "sink.clickhouse")
	assert.True(t, strings.HasSuffix(got,
		"COVERS invariant=sink.flush.keeps_batch integration=sink.clickhouse"))
}

// recorder captures Logf so the test can read what Invariant wrote.
type recorder struct {
	testing.TB
	t    *testing.T
	sink *string
}

func (r *recorder) Helper() {}
func (r *recorder) Logf(format string, args ...any) {
	*r.sink = strings.TrimSpace(sprintf(format, args...))
}
```

`Invariant` must therefore take `testing.TB`, not `*testing.T`, so a recorder can stand in. Add the `sprintf` helper as `fmt.Sprintf` via an import of `fmt` in the test file.

- [ ] **Step 6: Run it to verify it fails**

Run: `go test ./internal/coverage/ -run TestCoverageInvariant -v`
Expected: FAIL — `undefined: Invariant`.

- [ ] **Step 7: Add `Invariant`**

In `internal/coverage/coverage.go`, change `Covers` to take `testing.TB` and add:

```go
// Invariant records that this test proved one invariant for one integration.
//
// The line carries both ids. A test name can attribute to one feature by
// prefix, but an invariant is proven per integration, and the harness that
// proves it runs the same code for every integration; only the marker knows
// which one this run was.
func Invariant(t testing.TB, invariant, integration string) {
	t.Helper()
	t.Logf("COVERS invariant=%s integration=%s", invariant, integration)
}
```

- [ ] **Step 8: Run the Go tests**

Run: `go test ./internal/coverage/ -v`
Expected: PASS.

- [ ] **Step 9: Commit**

```bash
git add internal/coverage/ scripts/coverage_matrix.py tests/tooling/test_coverage_matrix.py
git commit -m "coverage: a structured marker carries an invariant and the integration it was proven on

A feature attributes by test name. An invariant cannot: the harness that
proves it runs the same code for every integration, so the name says
nothing about which one this run was. coverage.Invariant writes both ids
to the test log, and the generator reads them back as a third result.

If the format drifts, every invariant cell goes missing at once, so the
exact bytes are pinned by a test on each side."
```

---

### Task 2: The two registries, loaded and validated

**Files:**
- Create: `docs/coverage/invariants.yml`
- Create: `docs/coverage/integrations.yml`
- Modify: `scripts/coverage_matrix.py:37-56` (paths, `load_features`)
- Test: `tests/tooling/test_coverage_matrix.py`

**Interfaces:**
- Produces: `load_invariants() -> list[dict]`, `load_integrations() -> list[dict]`, and `validate_registries(invariants, integrations, features) -> list[str]` returning human-readable problems, empty when the registries are consistent.

- [ ] **Step 1: Write `invariants.yml`**

```yaml
# The invariants every integration must hold, and the evidence each one
# requires.
#
# This file is declared, not derived, for the reason features.yml gives: the
# failure it catches is an integration with no evidence at all. Seven of these
# have been violated and fixed since v1.0.4; each row names the fix.
#
# `applies_to` is the interface the invariant is a property of. Every
# integration of that kind must prove it, or be exempt in integrations.yml
# with a reason.
#
# `verified_by` says how evidence attaches:
#   harness    internal/conformance emits COVERS invariant=… integration=…
#   typetable  the generated type round-trip emits the same marker per type
#   named      a test named Test<Invariant>_* attributes by prefix
#
# `requires` lists the levels evidence must exist at. Every entry is empty in
# this revision: the matrix reports, and nothing fails. An invariant is
# enforced by filling it in, which is legal only once every non-exempt
# integration has a subject.
#
# `tracked_by` marks an invariant the code does not yet satisfy. It renders as
# declared-unenforced until the issue lands.

invariants:
  # --- Resilience: the sink contract -------------------------------------
  - id: sink.write.buffers_only
    family: resilience
    applies_to: sink
    claim: WriteTable does not reach the destination. Only Flush does.
    verified_by: harness
    requires: []

  - id: sink.flush.keeps_batch
    family: resilience
    applies_to: sink
    claim: >
      A failed Flush leaves every undelivered row buffered. The next Flush
      re-attempts them.
    verified_by: harness
    requires: []
    violated_once: ["#221"]

  - id: sink.flush.no_hollow_success
    family: resilience
    applies_to: sink
    claim: >
      Flush returns nil only when every row since the last success was
      acknowledged by the destination.
    verified_by: harness
    requires: []
    violated_once: ["#221"]

  - id: sink.flush.preserves_order
    family: resilience
    applies_to: sink
    claim: Rows reach the destination in WriteTable order, across a retry.
    verified_by: harness
    requires: []

  - id: sink.flush.honours_context
    family: resilience
    applies_to: sink
    claim: Flush returns ctx.Err() when the context ends. Rows stay buffered.
    verified_by: harness
    requires: []
    violated_once: ["#219"]

  - id: sink.flush.empty_is_noop
    family: resilience
    applies_to: sink
    claim: Flush with nothing buffered returns nil and touches nothing.
    verified_by: harness
    requires: []

  - id: sink.batch.reports_buffer
    family: resilience
    applies_to: sink
    claim: Batch() returns what is buffered, and nil when nothing is.
    verified_by: harness
    requires: []

  - id: sink.error.classifies
    family: resilience
    applies_to: sink
    claim: >
      The sink's errors classify as unreachable or rejected, so the retry
      ladder retries the right ones.
    verified_by: harness
    requires: []

  - id: sink.probe.fails_start
    family: resilience
    applies_to: sink
    claim: >
      A Prober whose destination is unreachable fails the start once, without
      retrying.
    verified_by: harness
    requires: []

  # --- Checkpoint: the pipeline and source contract ----------------------
  - id: pipeline.commit.after_flush
    family: checkpoint
    applies_to: pipeline
    claim: Offsets and state commit only after Flush returned nil.
    verified_by: named
    requires: []

  - id: pipeline.commit.nothing_on_failure
    family: checkpoint
    applies_to: pipeline
    claim: A failed flush commits nothing. Not offsets, not state.
    verified_by: named
    requires: []

  - id: pipeline.state.with_offsets
    family: checkpoint
    applies_to: pipeline
    claim: Window state and the offsets that produced it commit atomically.
    verified_by: named
    requires: []

  - id: source.commit.only_processed
    family: checkpoint
    applies_to: source
    claim: >
      A source commits the marks the pipeline processed, never what it
      fetched.
    verified_by: harness
    requires: []
    violated_once: ["#154"]

  - id: source.resume.from_committed
    family: checkpoint
    applies_to: source
    claim: >
      Restart resumes at the committed position. No gap, no replay before
      it.
    verified_by: harness
    requires: []

  - id: source.marks.never_regress
    family: checkpoint
    applies_to: source
    claim: A committed position never moves backwards.
    verified_by: harness
    requires: []

  - id: source.commit.on_revoke
    family: checkpoint
    applies_to: source
    claim: >
      Marks commit when a partition is revoked, before the rebalance
      completes.
    verified_by: harness
    requires: []
    tracked_by: "#183"

  # --- Types: per integration, from its declared table -------------------
  - id: type.roundtrip
    family: types
    applies_to: sink
    claim: >
      Every declared Arrow type reads back with the declared outcome: exact,
      coerced by the stated rule, or unsupported with a coded error.
    verified_by: typetable
    requires: []
    violated_once: ["#147", "#150", "#151"]

  - id: type.null
    family: types
    applies_to: sink
    claim: A null in every declared type reads back as declared.
    verified_by: typetable
    requires: []

  - id: type.timestamp.instant
    family: types
    applies_to: sink
    claim: >
      A timestamp reads back as the same instant. Zone-less is UTC. The host
      zone never leaks into the type.
    verified_by: typetable
    requires: []
    violated_once: ["#153"]

  - id: type.nested
    family: types
    applies_to: sink
    claim: >
      list, struct, list-of-struct and list-of-list read back or are
      declared unsupported. Never silently flattened.
    verified_by: typetable
    requires: []

  - id: type.string.fidelity
    family: types
    applies_to: sink
    claim: Unicode, escapes and the empty string round-trip byte for byte.
    verified_by: typetable
    requires: []
    violated_once: ["#149"]

  - id: type.undeclared.fails_loud
    family: types
    applies_to: sink
    claim: >
      An Arrow type absent from the table fails the batch with a coded
      error. Never coerced silently.
    verified_by: typetable
    requires: []

  # --- Lifecycle ---------------------------------------------------------
  - id: lifecycle.drain.on_cancel
    family: lifecycle
    applies_to: pipeline
    claim: Cancel or SIGTERM flushes the buffered batch, then commits.
    verified_by: named
    requires: []

  - id: lifecycle.drain.bounded
    family: lifecycle
    applies_to: pipeline
    claim: The drain finishes or fails inside its deadline.
    verified_by: named
    requires: []
    tracked_by: "#161"

  - id: lifecycle.close.idempotent
    family: lifecycle
    applies_to: sink
    claim: Close twice is safe.
    verified_by: harness
    requires: []

  # --- Error policy: declared, tracked ------------------------------------
  - id: error.dlq.carries_provenance
    family: errors
    applies_to: pipeline
    claim: A DLQ record carries the payload, offset, partition and reason.
    verified_by: named
    requires: []
    tracked_by: "#166"

  - id: error.bad_record.threshold
    family: errors
    applies_to: pipeline
    claim: N bad records in a window fail the pipeline rather than discard forever.
    verified_by: named
    requires: []
    tracked_by: "#166"

  - id: pipeline.batch.timeout
    family: lifecycle
    applies_to: pipeline
    claim: A batch whose query exceeds the timeout fails the batch, not the process.
    verified_by: named
    requires: []
    tracked_by: "#163"
```

- [ ] **Step 2: Write `integrations.yml`**

```yaml
# Every integration sqlflow can construct, and what each one claims.
#
# One entry per case in sinks.buildSink, sources.New and handlers.New. A test
# in each package holds Kinds() equal to the ids here, so a new case without
# an entry fails `go test -short` in seconds.
#
# `implements` lists the interfaces the integration satisfies beyond its
# kind's base contract. `Prober` means the sink checks its destination before
# the first batch.
#
# `exempt` names invariants that cannot apply, each with a reason. An
# exemption is one of two statements that must agree: the harness skips what
# it cannot exercise, and the registry excuses it. Either alone leaves the
# cell missing.
#
# `feature` ties the entry to features.yml.

integrations:
  # --- Sinks ---------------------------------------------------------------
  - id: sink.kafka
    kind: sink
    implements: [Sink]
    feature: sink.kafka
    exempt: []

  - id: sink.clickhouse
    kind: sink
    implements: [Sink, Prober]
    feature: sink.clickhouse
    exempt: []

  - id: sink.iceberg
    kind: sink
    implements: [Sink, Prober]
    feature: sink.iceberg
    exempt: []

  - id: sink.sqlcommand
    kind: sink
    implements: [Sink]
    feature: sink.sqlcommand
    exempt:
      - invariant: sink.flush.keeps_batch
        reason: writes through the pipeline's own DuckDB connection; a failure is not a network blip. retriesHelp already excludes it.
      - invariant: sink.flush.honours_context
        reason: same
      - invariant: sink.error.classifies
        reason: same
      - invariant: sink.probe.fails_start
        reason: nothing to dial

  - id: sink.console
    kind: sink
    implements: [Sink]
    feature: sink.console
    exempt:
      - invariant: sink.flush.keeps_batch
        reason: stdout cannot be temporarily unavailable. retriesHelp already excludes it.
      - invariant: sink.flush.honours_context
        reason: same
      - invariant: sink.error.classifies
        reason: same
      - invariant: sink.probe.fails_start
        reason: nothing to dial

  - id: sink.noop
    kind: sink
    implements: [Sink]
    feature: sink.console
    exempt:
      - invariant: sink.flush.keeps_batch
        reason: discards by design
      - invariant: sink.flush.no_hollow_success
        reason: discards by design
      - invariant: sink.flush.preserves_order
        reason: discards by design
      - invariant: sink.flush.honours_context
        reason: nothing to wait on
      - invariant: sink.batch.reports_buffer
        reason: buffers nothing by design
      - invariant: sink.error.classifies
        reason: never fails
      - invariant: sink.probe.fails_start
        reason: nothing to dial

  # --- Sources -------------------------------------------------------------
  - id: source.kafka
    kind: source
    implements: [Source, MarkCommitter]
    feature: source.kafka
    exempt: []

  - id: source.websocket
    kind: source
    implements: [Source]
    feature: source.websocket
    exempt:
      - invariant: source.commit.only_processed
        reason: no replayable position; at-most-once by construction
      - invariant: source.resume.from_committed
        reason: same
      - invariant: source.marks.never_regress
        reason: same
      - invariant: source.commit.on_revoke
        reason: no partitions

  - id: source.webhook
    kind: source
    implements: [Source]
    feature: source.webhook
    exempt:
      - invariant: source.commit.only_processed
        reason: no replayable position; at-most-once by construction
      - invariant: source.resume.from_committed
        reason: same
      - invariant: source.marks.never_regress
        reason: same
      - invariant: source.commit.on_revoke
        reason: no partitions

  # --- Handlers ------------------------------------------------------------
  - id: handler.structured
    kind: handler
    implements: [Handler]
    feature: handler.structured
    exempt: []

  - id: handler.inferred_mem
    kind: handler
    implements: [Handler, MetadataWriter]
    feature: handler.inferred_mem
    exempt: []

  - id: handler.inferred_disk
    kind: handler
    implements: [Handler]
    feature: handler.inferred_disk
    exempt: []
```

`sink.noop` maps to `feature: sink.console` because `features.yml` declares no noop feature and noop is a benchmarking stand-in; the generator only requires that `feature` names a declared feature.

- [ ] **Step 3: Write the failing loader and validator tests**

Append to `tests/tooling/test_coverage_matrix.py`:

```python
# --- Registries ------------------------------------------------------------

INVARIANTS = [
    {"id": "sink.flush.keeps_batch", "family": "resilience", "applies_to": "sink",
     "claim": "kept", "verified_by": "harness", "requires": []},
    {"id": "source.commit.only_processed", "family": "checkpoint",
     "applies_to": "source", "claim": "c", "verified_by": "harness", "requires": []},
]

INTEGRATIONS = [
    {"id": "sink.clickhouse", "kind": "sink", "implements": ["Sink"],
     "feature": "sink.clickhouse", "exempt": []},
    {"id": "sink.console", "kind": "sink", "implements": ["Sink"],
     "feature": "sink.console",
     "exempt": [{"invariant": "sink.flush.keeps_batch", "reason": "stdout"}]},
    {"id": "source.kafka", "kind": "source", "implements": ["Source"],
     "feature": "source.kafka", "exempt": []},
]


def test_the_committed_registries_load_and_validate():
    """The real files, not fixtures: a typo in either fails here before it
    can fail in CI."""
    invariants = cm.load_invariants()
    integrations = cm.load_integrations()
    features = cm.load_features()
    assert cm.validate_registries(invariants, integrations, features) == []
    assert len(invariants) >= 20
    assert {i["kind"] for i in integrations} == {"sink", "source", "handler"}


def test_every_committed_invariant_is_unenforced_in_this_revision():
    """PR 1 reports and fails nothing. Flipping a `requires` is PR 3."""
    for inv in cm.load_invariants():
        assert inv["requires"] == [], inv["id"]


def test_validate_rejects_an_exemption_naming_no_invariant():
    bad = [dict(INTEGRATIONS[1], exempt=[
        {"invariant": "sink.flush.nonexistent", "reason": "r"}])]
    problems = cm.validate_registries(INVARIANTS, bad, FEATURES)
    assert any("sink.flush.nonexistent" in p for p in problems)


def test_validate_rejects_an_exemption_without_a_reason():
    bad = [dict(INTEGRATIONS[1], exempt=[
        {"invariant": "sink.flush.keeps_batch"}])]
    problems = cm.validate_registries(INVARIANTS, bad, FEATURES)
    assert any("reason" in p for p in problems)


def test_validate_rejects_an_exemption_from_an_invariant_of_another_kind():
    """A sink cannot be exempt from a source invariant; that is a typo."""
    bad = [dict(INTEGRATIONS[0], exempt=[
        {"invariant": "source.commit.only_processed", "reason": "r"}])]
    problems = cm.validate_registries(INVARIANTS, bad, FEATURES)
    assert any("applies_to" in p for p in problems)


def test_validate_rejects_an_integration_naming_no_feature():
    bad = [dict(INTEGRATIONS[0], feature="sink.nonexistent")]
    problems = cm.validate_registries(INVARIANTS, bad, FEATURES)
    assert any("sink.nonexistent" in p for p in problems)


def test_validate_rejects_a_duplicate_invariant_id():
    problems = cm.validate_registries(INVARIANTS + [INVARIANTS[0]], INTEGRATIONS, FEATURES)
    assert any("duplicate" in p for p in problems)


def test_validate_rejects_a_requires_level_that_does_not_exist():
    bad = [dict(INVARIANTS[0], requires=["nightly"])]
    problems = cm.validate_registries(bad, INTEGRATIONS, FEATURES)
    assert any("nightly" in p for p in problems)
```

- [ ] **Step 4: Run them to verify they fail**

Run: `uv run --locked pytest tests/tooling/test_coverage_matrix.py -k "registr or validate or unenforced" -v`
Expected: FAIL — `AttributeError: module 'coverage_matrix' has no attribute 'load_invariants'`.

- [ ] **Step 5: Add the loaders and the validator**

In `scripts/coverage_matrix.py`, after `REGISTRY = ...` (line 38):

```python
INVARIANTS = os.path.join(REPO, "docs", "coverage", "invariants.yml")
INTEGRATIONS = os.path.join(REPO, "docs", "coverage", "integrations.yml")

FAMILIES = ("resilience", "checkpoint", "types", "lifecycle", "errors")
KINDS = ("sink", "source", "handler", "pipeline")
VERIFIERS = ("harness", "typetable", "named")
```

After `load_features` (line 56):

```python
def load_invariants():
    with open(INVARIANTS) as fh:
        return yaml.safe_load(fh)["invariants"]


def load_integrations():
    with open(INTEGRATIONS) as fh:
        return yaml.safe_load(fh)["integrations"]


def validate_registries(invariants, integrations, features):
    """Every problem a human can fix, as one line each. Empty when consistent.

    Checked before any test result is read: a registry error is a typo, and a
    typo that reached the matrix would read as a missing cell, which is the
    signal the matrix exists to carry. It must not be spendable on typos.
    """
    problems = []
    by_id = {}
    for inv in invariants:
        if inv["id"] in by_id:
            problems.append(f"invariants.yml: duplicate id {inv['id']}")
        by_id[inv["id"]] = inv
        for key in ("family", "applies_to", "claim", "verified_by", "requires"):
            if key not in inv:
                problems.append(f"invariants.yml: {inv['id']} has no {key}")
        if inv.get("family") not in FAMILIES:
            problems.append(f"invariants.yml: {inv['id']} family {inv.get('family')!r} is not one of {FAMILIES}")
        if inv.get("applies_to") not in KINDS:
            problems.append(f"invariants.yml: {inv['id']} applies_to {inv.get('applies_to')!r} is not one of {KINDS}")
        if inv.get("verified_by") not in VERIFIERS:
            problems.append(f"invariants.yml: {inv['id']} verified_by {inv.get('verified_by')!r} is not one of {VERIFIERS}")
        for lvl in inv.get("requires", []):
            if lvl not in LEVELS:
                problems.append(f"invariants.yml: {inv['id']} requires {lvl!r}, which is not a level")

    feature_ids = {f["id"] for f in features}
    seen = set()
    for integ in integrations:
        if integ["id"] in seen:
            problems.append(f"integrations.yml: duplicate id {integ['id']}")
        seen.add(integ["id"])
        if integ.get("kind") not in ("sink", "source", "handler"):
            problems.append(f"integrations.yml: {integ['id']} kind {integ.get('kind')!r} is not sink, source or handler")
        if integ.get("feature") not in feature_ids:
            problems.append(f"integrations.yml: {integ['id']} names feature {integ.get('feature')!r}, which features.yml does not declare")
        for ex in integ.get("exempt", []):
            inv = by_id.get(ex.get("invariant"))
            if inv is None:
                problems.append(f"integrations.yml: {integ['id']} is exempt from {ex.get('invariant')!r}, which invariants.yml does not declare")
                continue
            if not ex.get("reason"):
                problems.append(f"integrations.yml: {integ['id']} exemption from {inv['id']} has no reason")
            if inv["applies_to"] != integ.get("kind"):
                problems.append(f"integrations.yml: {integ['id']} is a {integ.get('kind')} but {inv['id']} applies_to {inv['applies_to']}")
    return problems
```

- [ ] **Step 6: Run the tooling suite**

Run: `uv run --locked pytest tests/tooling -q`
Expected: 54 passed. If `test_the_committed_registries_load_and_validate` fails, the message names the YAML line to fix; fix the YAML, not the validator.

- [ ] **Step 7: Commit**

```bash
git add docs/coverage/invariants.yml docs/coverage/integrations.yml scripts/coverage_matrix.py tests/tooling/test_coverage_matrix.py
git commit -m "coverage: declare every invariant and every integration

Two registries beside features.yml. invariants.yml is the contract: 28
claims, each grounded in a test that exists, a defect that was fixed, or
an issue that is open. integrations.yml is the surface: every constructor
case, what it implements, and what it is exempt from with a reason.

Every requires is empty. This revision reports and fails nothing; an
invariant is enforced by filling one in, which is legal only once every
non-exempt integration has a subject.

The generator validates both before reading a single test result. A typo
that reached the matrix would read as a missing cell, and that signal
must not be spendable on typos. Nine tests pin the rules."
```

---

### Task 3: The invariant axis in the matrix

**Files:**
- Modify: `scripts/coverage_matrix.py` (`build`, `snapshot`, `render`, `main`)
- Test: `tests/tooling/test_coverage_matrix.py`

**Interfaces:**
- Consumes: `parse_go` triple from Task 1; `load_invariants`, `load_integrations`, `validate_registries` from Task 2.
- Produces: `build_invariants(invariants, integrations, evidence) -> dict` where `evidence` is `{level: {test: [(invariant, integration)]}}` merged with outcomes, and the snapshot gains `"invariants": [...]` and `"invariant_gaps": [...]`. `render` gains one table per family.

- [ ] **Step 1: Write the failing tests**

Append to `tests/tooling/test_coverage_matrix.py`:

```python
# --- The invariant axis ----------------------------------------------------

def inv_snap(evidence=None, results=None, invariants=None, integrations=None):
    """evidence: {level: {test: [(invariant, integration)]}}
    results:  {level: {test: outcome}}"""
    evidence = evidence or {}
    results = results or {}
    cells = cm.build_invariants(
        invariants or INVARIANTS, integrations or INTEGRATIONS,
        {lvl: results.get(lvl, {}) for lvl in cm.LEVELS},
        {lvl: evidence.get(lvl, {}) for lvl in cm.LEVELS})
    return cm.snapshot_invariants(invariants or INVARIANTS, integrations or INTEGRATIONS, cells)


def cell(s, invariant, integration, lvl):
    for inv in s["invariants"]:
        if inv["id"] == invariant:
            return inv["integrations"][integration][lvl]
    raise AssertionError(f"{invariant} not in the snapshot")


def test_a_passing_harness_case_covers_the_cell():
    s = inv_snap(
        evidence={"integration": {"TestIntegrationSinkClickhouse_Conformance/keeps_batch":
                                  [("sink.flush.keeps_batch", "sink.clickhouse")]}},
        results={"integration": {"TestIntegrationSinkClickhouse_Conformance/keeps_batch": cm.PASS}},
    )
    c = cell(s, "sink.flush.keeps_batch", "sink.clickhouse", "integration")
    assert c["status"] == "covered"
    assert c["tests"] == ["TestIntegrationSinkClickhouse_Conformance/keeps_batch"]


def test_a_marker_from_a_failing_test_is_not_coverage():
    """The harness emits the marker only on pass, but a parent test can fail
    after a subtest passed. The outcome wins."""
    s = inv_snap(
        evidence={"integration": {"TestX": [("sink.flush.keeps_batch", "sink.clickhouse")]}},
        results={"integration": {"TestX": cm.FAIL}},
    )
    assert cell(s, "sink.flush.keeps_batch", "sink.clickhouse", "integration")["status"] == "failing"


def test_a_marker_from_a_skipped_test_is_not_coverage():
    s = inv_snap(
        evidence={"integration": {"TestX": [("sink.flush.keeps_batch", "sink.clickhouse")]}},
        results={"integration": {"TestX": cm.SKIP}},
    )
    assert cell(s, "sink.flush.keeps_batch", "sink.clickhouse", "integration")["status"] == "skipped"


def test_an_integration_with_no_evidence_is_missing():
    s = inv_snap()
    assert cell(s, "sink.flush.keeps_batch", "sink.clickhouse", "integration")["status"] == "missing"


def test_an_exempt_integration_renders_exempt_with_its_reason():
    s = inv_snap()
    c = cell(s, "sink.flush.keeps_batch", "sink.console", "integration")
    assert c["status"] == "exempt"
    assert c["reason"] == "stdout"


def test_an_invariant_applies_only_to_its_kind():
    """source.kafka has no cell under a sink invariant at all."""
    s = inv_snap()
    inv = next(i for i in s["invariants"] if i["id"] == "sink.flush.keeps_batch")
    assert "source.kafka" not in inv["integrations"]
    assert set(inv["integrations"]) == {"sink.clickhouse", "sink.console"}


def test_an_unrequired_level_is_not_a_gap():
    s = inv_snap()
    assert s["invariant_gaps"] == []


def test_a_required_level_with_no_evidence_is_a_gap():
    required = [dict(INVARIANTS[0], requires=["integration"])]
    s = inv_snap(invariants=required)
    assert {"invariant": "sink.flush.keeps_batch", "integration": "sink.clickhouse",
            "level": "integration", "status": "missing"} in s["invariant_gaps"]
    # console is exempt, so it is not a gap even though the level is required
    assert not any(g["integration"] == "sink.console" for g in s["invariant_gaps"])


def test_a_marker_naming_an_unknown_invariant_or_integration_is_reported():
    s = inv_snap(
        evidence={"integration": {"TestX": [("sink.flush.bogus", "sink.clickhouse"),
                                            ("sink.flush.keeps_batch", "sink.bogus")]}},
        results={"integration": {"TestX": cm.PASS}},
    )
    assert {"test": "TestX", "invariant": "sink.flush.bogus", "integration": "sink.clickhouse"} in s["unknown_invariant_markers"]
    assert {"test": "TestX", "invariant": "sink.flush.keeps_batch", "integration": "sink.bogus"} in s["unknown_invariant_markers"]


def test_two_unknown_invariant_markers_do_not_crash_the_snapshot():
    """Regression guard for the sorted()-over-dicts crash in the feature
    snapshot: sort by key, never by dict."""
    s = inv_snap(
        evidence={"integration": {"TestX": [("a.b.c", "sink.clickhouse")],
                                  "TestY": [("d.e.f", "sink.clickhouse")]}},
        results={"integration": {"TestX": cm.PASS, "TestY": cm.PASS}},
    )
    assert len(s["unknown_invariant_markers"]) == 2


def test_a_tracked_invariant_renders_as_declared_unenforced():
    tracked = [dict(INVARIANTS[0], tracked_by="#183")]
    s = inv_snap(invariants=tracked)
    inv = next(i for i in s["invariants"] if i["id"] == "sink.flush.keeps_batch")
    assert inv["tracked_by"] == "#183"


def test_render_carries_one_table_per_family():
    s = inv_snap()
    md = cm.render_invariants(s)
    assert "## Invariants: resilience" in md
    assert "## Invariants: checkpoint" in md
    assert "| `sink.flush.keeps_batch` |" in md


def test_render_marks_an_exempt_cell_and_names_the_reason():
    s = inv_snap()
    md = cm.render_invariants(s)
    line = next(l for l in md.splitlines() if "`sink.flush.keeps_batch`" in l)
    assert "exempt" in line


def test_a_test_carrying_any_marker_is_not_unattributed():
    """Review finding: a marker-attributed test was listed under 'match no
    declared feature'. The harness attributes by marker only, so its tests
    would all land there."""
    s = snap(
        integration_results={"TestIntegrationSinkClickhouse_Conformance/keeps_batch": cm.PASS},
        integration_covers={"TestIntegrationSinkClickhouse_Conformance/keeps_batch": ["sink.clickhouse"]},
    )
    assert s["unattributed"]["integration"] == []
```

- [ ] **Step 2: Run them to verify they fail**

Run: `uv run --locked pytest tests/tooling/test_coverage_matrix.py -k "invariant or unattributed or render_carries_one or render_marks" -v`
Expected: FAIL — `AttributeError: ... has no attribute 'build_invariants'` and the unattributed test asserting `[] == ['TestIntegration...']`.

- [ ] **Step 3: Add `build_invariants` and `snapshot_invariants`**

In `scripts/coverage_matrix.py`, after `build` (line 186):

```python
def build_invariants(invariants, integrations, results, evidence):
    """Attribute harness markers to (invariant, integration, level) cells.

    results:  {level: {test: outcome}}
    evidence: {level: {test: [(invariant, integration)]}}

    A marker is only ever emitted by a passing subtest, but the parent that
    ran it can still fail or be skipped afterwards, and that verdict is the
    one `go test` reports. The outcome recorded here is the test's, not the
    marker's presence.
    """
    known_inv = {i["id"] for i in invariants}
    known_integ = {i["id"] for i in integrations}
    cells = {}          # (invariant, integration, level) -> [(test, outcome)]
    unknown = []        # (test, invariant, integration)

    for level in LEVELS:
        for test, pairs in sorted(evidence.get(level, {}).items()):
            outcome = results.get(level, {}).get(test, FAIL)
            for inv, integ in pairs:
                if inv not in known_inv or integ not in known_integ:
                    unknown.append((test, inv, integ))
                    continue
                cells.setdefault((inv, integ, level), []).append((test, outcome))
    return {"cells": cells, "unknown": unknown}


def snapshot_invariants(invariants, integrations, built):
    """The invariant half of matrix.json. Same bare-name encoding as the
    feature half, plus `exempt` cells that carry their reason, so the page
    can show why a cell is blank without a second lookup."""
    by_kind = {}
    for integ in integrations:
        by_kind.setdefault(integ["kind"], []).append(integ)
    exemptions = {
        (ex["invariant"], integ["id"]): ex["reason"]
        for integ in integrations for ex in integ.get("exempt", [])
    }

    out = {"invariants": [], "invariant_gaps": [], "unknown_invariant_markers": []}
    for inv in invariants:
        required = set(inv.get("requires", []))
        entry = {
            "id": inv["id"],
            "family": inv["family"],
            "applies_to": inv["applies_to"],
            "claim": " ".join(inv["claim"].split()),
            "verified_by": inv["verified_by"],
            "requires": sorted(required),
            "integrations": {},
        }
        if inv.get("tracked_by"):
            entry["tracked_by"] = inv["tracked_by"]
        if inv.get("violated_once"):
            entry["violated_once"] = list(inv["violated_once"])

        # pipeline invariants have no per-integration cell; they attach to core.
        subjects = by_kind.get(inv["applies_to"], [])
        for integ in subjects:
            levels = {}
            reason = exemptions.get((inv["id"], integ["id"]))
            for level in LEVELS:
                if reason is not None:
                    levels[level] = {"status": "exempt", "reason": reason}
                    continue
                tests = sorted(built["cells"].get((inv["id"], integ["id"], level), []))
                if level not in required and not tests:
                    levels[level] = {"status": "not_required"}
                    continue
                state = status(tests)
                c = {"status": state, "tests": [n for n, _ in tests]}
                for key, members in (
                    ("skipped", [n for n, o in tests if o == SKIP]),
                    ("failing", [n for n, o in tests if o == FAIL]),
                ):
                    if members:
                        c[key] = members
                levels[level] = c
                if level in required and state != "covered":
                    out["invariant_gaps"].append({
                        "invariant": inv["id"], "integration": integ["id"],
                        "level": level, "status": state})
            entry["integrations"][integ["id"]] = levels
        out["invariants"].append(entry)

    out["unknown_invariant_markers"] = [
        {"test": t, "invariant": i, "integration": g}
        for t, i, g in sorted(set(built["unknown"]))
    ]
    return out
```

`sorted(set(...))` over tuples is safe; this is the shape the feature snapshot's `unknown_markers` should have used.

- [ ] **Step 4: Add `render_invariants`**

After `render` (line 380):

```python
INV_MARK = {
    "covered": "✅",
    "skipped": "⚠️ skipped",
    "missing": "❌ missing",
    "failing": "🔥 failing",
    "not_required": "·",
    "exempt": "— exempt",
}


def render_invariants(snap):
    """One table per family. Columns are the integrations the family's
    invariants apply to; a cell is the worst status across the levels the
    invariant requires, or across all levels when it requires none."""
    lines = [
        "# Invariant matrix",
        "",
        "Generated from `docs/coverage/matrix.json` by `make coverage-matrix`.",
        "Do not edit by hand.",
        "",
        "Invariants are declared in `docs/coverage/invariants.yml`; integrations",
        "in `docs/coverage/integrations.yml`. A cell is proven by the conformance",
        "harness (`internal/conformance`), which emits a marker naming both ids.",
        "An **exempt** cell carries its reason in the JSON. A **missing** cell has",
        "no evidence. Nothing here fails the build until an invariant's `requires`",
        "is filled in.",
        "",
    ]
    rank = ["failing", "missing", "skipped", "covered", "exempt", "not_required"]

    def worst(levels, required):
        consider = required or list(levels)
        statuses = [levels[l]["status"] for l in consider if l in levels]
        return min(statuses, key=rank.index) if statuses else "missing"

    families = []
    for inv in snap["invariants"]:
        if inv["family"] not in families:
            families.append(inv["family"])

    for family in families:
        rows = [i for i in snap["invariants"] if i["family"] == family]
        columns = []
        for inv in rows:
            for integ in inv["integrations"]:
                if integ not in columns:
                    columns.append(integ)
        lines += [f"## Invariants: {family}", ""]
        if columns:
            lines.append("| Invariant | Claim | " + " | ".join(f"`{c}`" for c in columns) + " |")
            lines.append("| --- | --- | " + " | ".join("---" for _ in columns) + " |")
        else:
            lines.append("| Invariant | Claim | evidence |")
            lines.append("| --- | --- | --- |")
        for inv in rows:
            claim = inv["claim"]
            if inv.get("tracked_by"):
                claim += f" *(declared, tracked by {inv['tracked_by']})*"
            if columns:
                cells = " | ".join(
                    INV_MARK[worst(inv["integrations"][c], inv["requires"])]
                    if c in inv["integrations"] else "·"
                    for c in columns)
                lines.append(f"| `{inv['id']}` | {claim} | {cells} |")
            else:
                lines.append(f"| `{inv['id']}` | {claim} | {inv['verified_by']} |")
        lines.append("")

    if snap["invariant_gaps"]:
        lines += ["## Invariant gaps", "", "These fail `make coverage-check`.", ""]
        for g in snap["invariant_gaps"]:
            lines.append(f"- `{g['invariant']}` on `{g['integration']}` requires **{g['level']}** and is *{g['status']}*.")
        lines.append("")

    if snap["unknown_invariant_markers"]:
        lines += ["## Markers naming an unknown invariant or integration", ""]
        for m in snap["unknown_invariant_markers"]:
            lines.append(f"- `{m['test']}` marks `{m['invariant']}` on `{m['integration']}`")
        lines.append("")

    return "\n".join(lines) + "\n"
```

- [ ] **Step 5: Exclude marker-carrying tests from the unattributed report**

In `build` (line 170–175), change:

```python
            feature_id = match(strip_level_prefix(name), prefixes)
            if feature_id:
                coverage[feature_id][level].append((name, outcome))
            else:
                unmatched[level].append(name)
```

to:

```python
            feature_id = match(strip_level_prefix(name), prefixes)
            if feature_id:
                coverage[feature_id][level].append((name, outcome))
            elif not extras.get(name):
                # A test with a marker is attributed by it. Listing it here
                # would tell the reader to rename a test that already says
                # what it covers.
                unmatched[level].append(name)
```

- [ ] **Step 6: Wire `main`**

Replace the body of `main()` from `features = load_features()` to the `rendered = render(snap)` line with:

```python
    features = load_features()
    invariants = load_invariants()
    integrations = load_integrations()
    problems = validate_registries(invariants, integrations, features)
    if problems:
        for p in problems:
            print(p, file=sys.stderr)
        return 2

    go_results, go_covers, go_invariants = (
        parse_go(args.go) if args.go and os.path.exists(args.go) else ({}, {}, {}))
    it_results, it_covers, it_invariants = (
        parse_go(args.go_integration)
        if args.go_integration and os.path.exists(args.go_integration)
        else ({}, {}, {}))
    py_results, py_covers, py_invariants = (
        parse_pytest(args.pytest)
        if args.pytest and os.path.exists(args.pytest) else ({}, {}, {}))

    coverage, secondary, unmatched, unknown = build(
        features, go_results, py_results, go_covers, py_covers,
        it_results, it_covers)
    snap = snapshot(features, coverage, secondary, unmatched, unknown)

    built = build_invariants(
        invariants, integrations,
        {"unit": go_results, "integration": it_results, "release": py_results},
        {"unit": go_invariants, "integration": it_invariants, "release": py_invariants})
    snap.update(snapshot_invariants(invariants, integrations, built))
    snap["version"] = 3

    rendered = render(snap) + "\n" + render_invariants(snap)
```

And extend the gap report at the end of `main`:

```python
    gaps = snap["gaps"] + snap["invariant_gaps"]
    if args.check and gaps:
        print(f"\n{len(gaps)} coverage gap(s):", file=sys.stderr)
        for gap in snap["gaps"]:
            print(f"  {gap['feature']}: {gap['level']} is {gap['status']}",
                  file=sys.stderr)
        for gap in snap["invariant_gaps"]:
            print(f"  {gap['invariant']} on {gap['integration']}: "
                  f"{gap['level']} is {gap['status']}", file=sys.stderr)
        return 1
    return 0
```

Update `test_the_snapshot_declares_its_schema_version` to expect `3` if it asserts on `snapshot(...)` output directly — check: it calls `snap(...)`, which calls `cm.snapshot`, which still writes `2`; `main` bumps to `3`. Leave that test alone and add nothing: the bump is `main`'s, so the committed file reads `3` and the unit snapshot reads `2`. Instead, change `snapshot()` line 231 to `"version": 3` and update the existing test to assert `3`. One number, one place.

- [ ] **Step 7: Run the tooling suite**

Run: `uv run --locked pytest tests/tooling -q`
Expected: 69 passed.

- [ ] **Step 8: Regenerate against the existing reports and confirm the shape**

Run: `SQLFLOW_CLICKHOUSE_DSN=clickhouse://127.0.0.1:1/default make coverage-write && uv run --locked python -c "import json; m=json.load(open('docs/coverage/matrix.json')); print(m['version'], len(m['invariants']), m['invariant_gaps'], m['unknown_invariant_markers'])"`
Expected: `3 28 [] []`, and `docs/coverage/matrix.md` ends with the invariant tables, every sink cell `❌ missing` or `— exempt`.

- [ ] **Step 9: Commit**

```bash
git add scripts/coverage_matrix.py tests/tooling/test_coverage_matrix.py docs/coverage/matrix.json docs/coverage/matrix.md
git commit -m "coverage: the matrix gains an invariant axis

invariant x integration x level, beside the feature table. A cell is
proven by a structured marker from a passing test; failing and skipped
outcomes win over the marker, as they do for features. Exempt cells carry
their reason. A required level with no evidence is a gap and fails
--check, exactly as a feature gap does; today no level is required.

Two fixes in passing that the harness depends on. A test carrying a
marker no longer appears as unattributed, which would have listed every
harness test under 'match no declared feature'. Unknown markers sort by
tuple, not by dict, so two of them report rather than crash.

Evidence: 69 tooling tests pass. The regenerated matrix carries 28
invariants, 0 gaps, and every sink cell missing or exempt.

If the marker parsing is wrong, every invariant cell reads missing. That
fails toward 'unproven', which is the direction it must fail."
```

---

### Task 4: `Kinds()` on every constructor, held equal to the registry

**Files:**
- Create: `internal/coverage/registry.go`
- Modify: `internal/sinks/init.go:106-145` (`buildSink`)
- Modify: `internal/sources/init.go:16-98` (`New`)
- Modify: `internal/handlers/init.go:13-…` (`New`)
- Test: `internal/coverage/registry_test.go`, `internal/sinks/registry_test.go`, `internal/sources/registry_test.go`, `internal/handlers/registry_test.go`

**Interfaces:**
- Produces: `coverage.Integrations(kind string) ([]string, error)` — ids of that kind from `integrations.yml`, as the bare type name (`sink.clickhouse` → `clickhouse`).
- Produces: `sinks.Kinds() []string`, `sources.Kinds() []string`, `handlers.Kinds() []string` — sorted constructor cases.

- [ ] **Step 1: Write the failing registry-reader test**

Create `internal/coverage/registry_test.go`:

```go
package coverage

import (
	"testing"

	"github.com/zeebo/assert"
)

func TestCoverageRegistry_ListsSinksByBareName(t *testing.T) {
	got, err := Integrations("sink")
	assert.NoError(t, err)
	// The registry's ids are prefixed by kind; the constructor switch is
	// not. The bare name is what both sides can compare.
	assert.DeepEqual(t, []string{"clickhouse", "console", "iceberg", "kafka", "noop", "sqlcommand"}, got)
}

func TestCoverageRegistry_RejectsAnUnknownKind(t *testing.T) {
	_, err := Integrations("router")
	assert.Error(t, err)
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `go test ./internal/coverage/ -run TestCoverageRegistry -v`
Expected: FAIL — `undefined: Integrations`.

- [ ] **Step 3: Write the reader**

Create `internal/coverage/registry.go`:

```go
package coverage

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

// Integrations returns the bare type names integrations.yml declares for one
// kind, sorted. "sink.clickhouse" is reported as "clickhouse", which is the
// form the constructor switches use, so a test can hold the two equal.
//
// The file is located relative to this source file rather than the working
// directory, because `go test ./...` runs each package from its own dir.
func Integrations(kind string) ([]string, error) {
	switch kind {
	case "sink", "source", "handler":
	default:
		return nil, fmt.Errorf("coverage: %q is not an integration kind", kind)
	}
	_, self, _, _ := runtime.Caller(0)
	path := filepath.Join(filepath.Dir(self), "..", "..", "docs", "coverage", "integrations.yml")
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("coverage: read %s: %w", path, err)
	}
	var doc struct {
		Integrations []struct {
			ID   string `yaml:"id"`
			Kind string `yaml:"kind"`
		} `yaml:"integrations"`
	}
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		return nil, fmt.Errorf("coverage: parse %s: %w", path, err)
	}
	var out []string
	for _, i := range doc.Integrations {
		if i.Kind == kind {
			out = append(out, strings.TrimPrefix(i.ID, kind+"."))
		}
	}
	sort.Strings(out)
	return out, nil
}
```

- [ ] **Step 4: Run it**

Run: `go test ./internal/coverage/ -v`
Expected: PASS.

- [ ] **Step 5: Write the three failing agreement tests**

Create `internal/sinks/registry_test.go`:

```go
package sinks

import (
	"testing"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

// A sink the switch can build and the registry does not name has no
// invariant cells at all, which is the sink.iceberg failure: nothing written
// down, so nothing can be missing. This fails in the unit pass, in seconds.
func TestSinkRegistry_MatchesTheConstructorSwitch(t *testing.T) {
	declared, err := coverage.Integrations("sink")
	assert.NoError(t, err)
	assert.DeepEqual(t, declared, Kinds())
}
```

Create `internal/sources/registry_test.go` with the same body, package `sources`, `coverage.Integrations("source")`, test name `TestSourceRegistry_MatchesTheConstructorSwitch`.

Create `internal/handlers/registry_test.go` with the same body, package `handlers`, `coverage.Integrations("handler")`, test name `TestHandlerRegistry_MatchesTheConstructorSwitch`.

The handler switch cases are `handlers.StructuredBatch`, `handlers.InferredMemBatch`, `handlers.InferredDiskBatch`; `Kinds()` for handlers must return the registry's form (`structured`, `inferred_mem`, `inferred_disk`). The mapping lives in the `builders` map keys below.

- [ ] **Step 6: Run them to verify they fail**

Run: `go test -short ./internal/sinks/ ./internal/sources/ ./internal/handlers/ -run Registry`
Expected: FAIL — `undefined: Kinds` in each package.

- [ ] **Step 7: Refactor `buildSink` into a map**

In `internal/sinks/init.go`, replace `buildSink` (lines 106–145) with:

```go
// builders is the constructor for each sink type. A map rather than a switch
// so Kinds can list it: the registry test holds the two equal, and a new
// sink without a registry entry fails the unit pass.
var builders = map[string]func(ctx context.Context, sink config.Sink, conn adbc.Connection) (core.Sink, error){
	"noop": func(context.Context, config.Sink, adbc.Connection) (core.Sink, error) {
		return &NoopSink{}, nil
	},
	"console": func(context.Context, config.Sink, adbc.Connection) (core.Sink, error) {
		return NewConsoleSink(), nil
	},
	"kafka": func(_ context.Context, sink config.Sink, _ adbc.Connection) (core.Sink, error) {
		if sink.Kafka == nil {
			return nil, errs.New(errs.CodeSinkInvalid, "sink: kafka sink requires a kafka block")
		}
		return NewKafkaSink(*sink.Kafka)
	},
	"sqlcommand": func(_ context.Context, sink config.Sink, conn adbc.Connection) (core.Sink, error) {
		if sink.SQLCommand == nil {
			return nil, errs.New(errs.CodeSinkInvalid, "sink: sqlcommand sink requires a sqlcommand block")
		}
		return NewSQLCommandSink(conn, sink.SQLCommand.SQL, sink.SQLCommand.Substitutions)
	},
	"clickhouse": func(_ context.Context, sink config.Sink, _ adbc.Connection) (core.Sink, error) {
		if sink.Clickhouse == nil {
			return nil, errs.New(errs.CodeSinkInvalid, "sink: clickhouse sink requires a clickhouse block")
		}
		return NewClickhouseSink(*sink.Clickhouse)
	},
	"iceberg": func(ctx context.Context, sink config.Sink, _ adbc.Connection) (core.Sink, error) {
		if sink.Iceberg == nil {
			return nil, errs.New(errs.CodeSinkInvalid, "sink: iceberg sink requires an iceberg block")
		}
		return NewIcebergSink(ctx, sink.Iceberg.CatalogName, sink.Iceberg.TableName)
	},
}

// Kinds lists every sink type the engine can build, sorted.
func Kinds() []string {
	out := make([]string, 0, len(builders))
	for k := range builders {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// buildSink constructs the sink itself. Whether a retry ladder belongs around
// it is retriesHelp's decision, not this one's.
func buildSink(ctx context.Context, sink config.Sink, conn adbc.Connection) (core.Sink, error) {
	kind := sink.Type
	if kind == "" {
		// The Python engine falls back to console for an unset type.
		kind = "console"
	}
	build, ok := builders[kind]
	if !ok {
		return nil, errs.New(errs.CodeSinkInvalid, "sink: %q not supported", sink.Type)
	}
	return build(ctx, sink, conn)
}
```

Add `"sort"` to the imports.

- [ ] **Step 8: Refactor `sources.New` the same way**

In `internal/sources/init.go`, move each `case` body into a map entry keyed `"kafka"`, `"websocket"`, `"webhook"` with signature `func(c config.Source, l *zap.Logger, mp metric.MeterProvider) (core.Source, error)`. Keep every body byte-identical to the case it came from. `New` becomes:

```go
func New(c config.Source, l *zap.Logger, mp metric.MeterProvider) (core.Source, error) {
	build, ok := builders[c.Type]
	if !ok {
		return nil, errs.New(errs.CodeSourceInvalid, "source: %q not supported", c.Type)
	}
	return build(c, l, mp)
}

// Kinds lists every source type the engine can build, sorted.
func Kinds() []string {
	out := make([]string, 0, len(builders))
	for k := range builders {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
```

- [ ] **Step 9: Refactor `handlers.New` the same way**

In `internal/handlers/init.go`, the config carries the Python-era names. Key the map by the registry's bare name and translate once:

```go
// handlerKinds maps the config's Python-era type names to the registry's.
var handlerKinds = map[string]string{
	"handlers.StructuredBatch":   "structured",
	"handlers.InferredMemBatch":  "inferred_mem",
	"handlers.InferredDiskBatch": "inferred_disk",
}
```

Move each case body into `builders[<bare name>]` with signature `func(conn adbc.Connection, c config.Handler, l *zap.Logger) (core.Handler, error)`. `New` looks up `handlerKinds[c.Type]` then `builders[...]`; an unknown type keeps its existing error. `Kinds()` is the same shape as the other two.

- [ ] **Step 10: Run the unit pass for the three packages plus their existing tests**

Run: `go test -short ./internal/sinks/ ./internal/sources/ ./internal/handlers/ ./internal/coverage/`
Expected: all `ok`. In particular `TestSinkRetry_New*` and the config-driven tests must still pass — the refactor changes no behaviour, and those tests prove it.

- [ ] **Step 11: Run the whole unit pass**

Run: `go build ./... && go vet ./... && go test -short ./...`
Expected: all `ok`.

- [ ] **Step 12: Commit**

```bash
git add internal/coverage/registry.go internal/coverage/registry_test.go internal/sinks/init.go internal/sinks/registry_test.go internal/sources/init.go internal/sources/registry_test.go internal/handlers/init.go internal/handlers/registry_test.go
git commit -m "integrations: the constructor switches and integrations.yml cannot drift

Each constructor is a map now, and Kinds() lists its keys. A test per
package holds that list equal to the registry, so a new case without an
entry fails go test -short in seconds. A sink the engine can build and
the registry does not name has no invariant cells at all, which is the
sink.iceberg failure: nothing written down, so nothing can be missing.

No behaviour change. Every existing sink, source and handler test passes
unchanged, and the unsupported-type errors are the same strings."
```

---

### Task 5: The harness, proven against an in-memory sink

**Files:**
- Create: `internal/conformance/conformance.go`
- Test: `internal/conformance/conformance_test.go`

**Interfaces:**
- Produces:
  ```go
  type Row map[string]any
  type SinkSubject struct {
      Integration string
      New         func(t *testing.T) core.Sink
      Break       func(t *testing.T)
      Heal        func(t *testing.T)
      ReadBack    func(t *testing.T) []Row
      Table       func(t *testing.T, id int64) arrow.Table   // a one-row table the subject's destination accepts
  }
  func Sinks(t *testing.T, s SinkSubject)
  ```
  `Table` is the subject's, not the harness's: a ClickHouse table needs columns the DDL declared, and the harness must not know DDL. The harness compares `ReadBack` rows by the `id` key only.

- [ ] **Step 1: Write the failing harness tests**

Create `internal/conformance/conformance_test.go`:

```go
package conformance

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/turbolytics/sql-flow/internal/core"
	"github.com/zeebo/assert"
)

// memSink is the smallest sink that honours the contract: it buffers on
// WriteTable, delivers on Flush, and keeps what it could not deliver.
type memSink struct {
	mu        sync.Mutex
	down      bool
	buffered  []arrow.Table
	delivered []int64
}

func (m *memSink) WriteTable(_ context.Context, t arrow.Table) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	t.Retain()
	m.buffered = append(m.buffered, t)
	return nil
}

func (m *memSink) Flush(context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.down {
		return errors.New("destination unreachable")
	}
	for _, t := range m.buffered {
		m.delivered = append(m.delivered, ids(t)...)
		t.Release()
	}
	m.buffered = nil
	return nil
}

func (m *memSink) Batch() (arrow.Table, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.buffered) == 0 {
		return nil, nil
	}
	return m.buffered[0], nil
}

// dropSink is memSink with #221's defect: a failed Flush discards the buffer.
type dropSink struct{ memSink }

func (d *dropSink) Flush(ctx context.Context) error {
	d.mu.Lock()
	down := d.down
	if down {
		d.buffered = nil
	}
	d.mu.Unlock()
	return d.memSink.Flush(ctx)
}

func ids(t arrow.Table) []int64 {
	var out []int64
	col := t.Column(0)
	for _, chunk := range col.Data().Chunks() {
		arr := chunk.(*array.Int64)
		for i := 0; i < arr.Len(); i++ {
			out = append(out, arr.Value(i))
		}
	}
	return out
}

func oneRow(t *testing.T, id int64) arrow.Table {
	t.Helper()
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)
	b := array.NewRecordBuilder(memory.NewGoAllocator(), schema)
	defer b.Release()
	b.Field(0).(*array.Int64Builder).Append(id)
	rec := b.NewRecord()
	defer rec.Release()
	return array.NewTableFromRecords(schema, []arrow.Record{rec})
}

func subjectFor(sink interface {
	core.Sink
	set(down bool)
	rows() []Row
}) SinkSubject {
	return SinkSubject{
		Integration: "sink.mem",
		New:         func(*testing.T) core.Sink { return sink },
		Break:       func(*testing.T) { sink.set(true) },
		Heal:        func(*testing.T) { sink.set(false) },
		ReadBack:    func(*testing.T) []Row { return sink.rows() },
		Table:       oneRow,
	}
}

func (m *memSink) set(down bool) { m.mu.Lock(); m.down = down; m.mu.Unlock() }
func (m *memSink) rows() []Row {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]Row, 0, len(m.delivered))
	for _, id := range m.delivered {
		out = append(out, Row{"id": id})
	}
	return out
}

func TestConformanceSinks_ACorrectSinkPasses(t *testing.T) {
	Sinks(t, subjectFor(&memSink{}))
}

// The harness must catch the defect it exists for. Run it against a sink
// that drops its buffer and confirm the keeps_batch subtest fails, using a
// child *testing.T so the failure is observed rather than reported.
func TestConformanceSinks_ADroppingSinkFailsKeepsBatch(t *testing.T) {
	var failed bool
	t.Run("probe", func(inner *testing.T) {
		inner.Cleanup(func() { failed = inner.Failed() })
		// Run the harness under a subtest whose failure we can inspect.
		inner.Run("harness", func(h *testing.T) {
			Sinks(h, subjectFor(&dropSink{}))
		})
	})
	assert.True(t, failed)
}
```

`go test` marks a test failed if any subtest fails, so `inner.Failed()` in `Cleanup` is true when the harness's `keeps_batch` subtest failed. The outer test then passes on `assert.True(t, failed)`, but `go test` still reports the inner `probe/harness/keeps_batch` failure as a failure of the whole test. Avoid that: the dropping-sink case must not use real `t.Run` failure. Replace the second test with a `Check` variant — see Step 3 — so the harness exposes its verdicts as values.

- [ ] **Step 2: Run to verify it fails**

Run: `go test -short ./internal/conformance/ -v`
Expected: FAIL — `undefined: SinkSubject`, `undefined: Sinks`, `undefined: Row`.

- [ ] **Step 3: Write the harness**

Create `internal/conformance/conformance.go`:

```go
// Package conformance proves the invariants every integration must hold.
//
// It knows the invariants and nothing about any integration. An integration
// supplies a subject: how to build it, how to make its destination stop
// answering, how to bring it back, and how to read what it delivered. The
// harness runs one sequence and asserts each invariant as its own subtest,
// so each gets its own marker and its own cell in the matrix.
//
// Evidence is emitted, not inferred. A passing subtest logs
//
//	COVERS invariant=<id> integration=<id>
//
// which the coverage generator reads from `go test -json` output. Test names
// carry nothing, because the same code proves the same invariant for every
// integration and only the marker knows which one this run was.
package conformance

import (
	"context"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/turbolytics/sql-flow/internal/core"
	"github.com/turbolytics/sql-flow/internal/coverage"
)

// Row is one delivered row, decoded by the subject through the destination's
// own reader. The harness compares rows by their "id" value only, because
// the destination decides what other columns a row has.
type Row map[string]any

// SinkSubject is what an integration hands the harness.
type SinkSubject struct {
	// Integration is the integrations.yml id, e.g. "sink.clickhouse".
	Integration string
	// New builds the sink under test. Called once per sequence.
	New func(t *testing.T) core.Sink
	// Break makes the destination stop answering. Nil means nothing can
	// break, and the network-shaped invariants are skipped; the registry
	// must then exempt the integration, or the cell stays missing.
	Break func(t *testing.T)
	// Heal reverses Break.
	Heal func(t *testing.T)
	// ReadBack returns every row the destination holds, in delivery order.
	ReadBack func(t *testing.T) []Row
	// Table returns a one-row table with an int64 "id" column that the
	// destination accepts. The subject owns the schema because a ClickHouse
	// table has the columns its DDL declared, and the harness has no DDL.
	Table func(t *testing.T, id int64) arrow.Table
}

// flushTimeout bounds a Flush against a broken destination. A hung
// connection must return as ctx.Err(), not hang the suite.
const flushTimeout = 10 * time.Second

// Sinks proves every sink invariant the subject can exercise.
func Sinks(t *testing.T, s SinkSubject) {
	t.Helper()
	if s.Integration == "" {
		t.Fatal("conformance: SinkSubject.Integration is required")
	}
	for _, v := range sinkVerdicts(t, s) {
		v := v
		t.Run(v.invariant, func(t *testing.T) {
			if v.skipped != "" {
				t.Skip(v.skipped)
			}
			if v.failure != "" {
				t.Fatal(v.failure)
			}
			coverage.Invariant(t, v.invariant, s.Integration)
		})
	}
	coverage.Covers(t, s.Integration)
}

// verdict is one invariant's outcome. Separated from the subtest that
// reports it so the harness's own tests can assert on a failure without
// failing.
type verdict struct {
	invariant string
	failure   string // non-empty: the invariant does not hold
	skipped   string // non-empty: the subject cannot exercise it
}

// sinkVerdicts runs the sequence once and judges each invariant.
//
//  1. WriteTable(A)
//  2. Break; Flush must fail
//  3. Heal; Flush must succeed
//  4. ReadBack must be [A]
func sinkVerdicts(t *testing.T, s SinkSubject) []verdict {
	t.Helper()
	const keepsBatch = "sink.flush.keeps_batch"

	if s.Break == nil {
		return []verdict{{invariant: keepsBatch, skipped: "subject has nothing to break; exempt it in integrations.yml"}}
	}

	sink := s.New(t)
	a := s.Table(t, 1)
	defer a.Release()

	ctx := context.Background()
	if err := sink.WriteTable(ctx, a); err != nil {
		t.Fatalf("conformance: WriteTable before any fault: %v", err)
	}

	s.Break(t)
	bctx, cancel := context.WithTimeout(ctx, flushTimeout)
	err := sink.Flush(bctx)
	cancel()
	if err == nil {
		// A flush that "succeeds" into a broken destination proves nothing
		// about keeping the batch; the fault did not take.
		t.Fatalf("conformance: Flush succeeded while the destination was broken; Break did not break it")
	}

	s.Heal(t)
	if err := sink.Flush(ctx); err != nil {
		return []verdict{{invariant: keepsBatch,
			failure: "Flush after Heal failed: " + err.Error() + "; the retry did not deliver"}}
	}

	got := s.ReadBack(t)
	if len(got) != 1 || got[0]["id"] != int64(1) {
		return []verdict{{invariant: keepsBatch,
			failure: "after a failed Flush and a successful retry the destination holds " +
				describe(got) + "; want exactly the row that failed"}}
	}
	return []verdict{{invariant: keepsBatch}}
}

func describe(rows []Row) string {
	if len(rows) == 0 {
		return "no rows"
	}
	ids := ""
	for i, r := range rows {
		if i > 0 {
			ids += ", "
		}
		ids += "id=" + itoa(r["id"])
	}
	return "rows [" + ids + "]"
}

func itoa(v any) string {
	switch n := v.(type) {
	case int64:
		return time.Duration(n).String()[:0] + fmtInt(n)
	default:
		return "?"
	}
}
```

Replace the last helper with a plain `strconv.FormatInt` — the version above is a placeholder that must not ship:

```go
import "strconv"

func itoa(v any) string {
	if n, ok := v.(int64); ok {
		return strconv.FormatInt(n, 10)
	}
	return "?"
}
```

Then rewrite the dropping-sink test to use `sinkVerdicts` directly:

```go
func TestConformanceSinks_ADroppingSinkFailsKeepsBatch(t *testing.T) {
	vs := sinkVerdicts(t, subjectFor(&dropSink{}))
	assert.Equal(t, 1, len(vs))
	assert.Equal(t, "sink.flush.keeps_batch", vs[0].invariant)
	assert.True(t, vs[0].failure != "")
	assert.True(t, strings.Contains(vs[0].failure, "no rows"))
}

func TestConformanceSinks_ASubjectWithNothingToBreakIsSkipped(t *testing.T) {
	s := subjectFor(&memSink{})
	s.Break, s.Heal = nil, nil
	vs := sinkVerdicts(t, s)
	assert.True(t, vs[0].skipped != "")
}
```

Add `"strings"` to the test imports.

- [ ] **Step 4: Run the harness tests**

Run: `go test -short ./internal/conformance/ -v`
Expected: PASS. The `-v` output for `ACorrectSinkPasses` must contain the line `COVERS invariant=sink.flush.keeps_batch integration=sink.mem` — confirm it by eye; Task 1's parser test already pins the format.

- [ ] **Step 5: Confirm the import rule**

Run: `go list -deps ./internal/conformance/ | grep -E 'internal/(sinks|kafka|webhook|websocket)$'`
Expected: no output. If any prints, the package has the cycle the Global Constraints forbid.

- [ ] **Step 6: Commit**

```bash
git add internal/conformance/
git commit -m "conformance: one harness proves a sink invariant for any sink

The harness knows the invariant and nothing about the integration. A
subject supplies New, Break, Heal, ReadBack and a one-row Table; the
harness writes a row, breaks the destination, sees Flush fail, heals it,
sees Flush succeed, and reads the row back. That proves
sink.flush.keeps_batch, and a passing subtest emits the marker the
matrix reads.

Verdicts are values, separate from the subtests that report them, so
the harness's own tests can run it against a sink with #221's defect and
assert that it catches it without failing themselves.

If the sequence is wrong, a sink that drops its batch passes. The
dropSink test is the guard: it drops, and the verdict names it."
```

---

### Task 6: `Proxy`: toxiproxy in front of an upstream

**Files:**
- Create: `internal/conformance/proxy.go`
- Modify: `go.mod`, `go.sum`

**Interfaces:**
- Produces:
  ```go
  type Proxy struct { Addr string }             // host:port the sink dials
  func NewProxy(t *testing.T, network *testcontainers.DockerNetwork, upstream string) *Proxy
  func (p *Proxy) Break(t *testing.T)           // add a timeout toxic: connections hang
  func (p *Proxy) Heal(t *testing.T)            // remove it
  ```
  `upstream` is `<network alias>:<port>` of a container on `network`. `NewProxy` registers `t.Cleanup` to terminate the container.

- [ ] **Step 1: Add the dependencies**

Run:
```bash
go get github.com/testcontainers/testcontainers-go/modules/toxiproxy@v0.44.0 \
       github.com/testcontainers/testcontainers-go/modules/clickhouse@v0.44.0 \
       github.com/Shopify/toxiproxy/v2@v2.12.0
go mod tidy
```
Expected: `go.mod` gains the three requirements; `go build ./...` still succeeds.

- [ ] **Step 2: Write `proxy.go`**

There is no standalone test for `Proxy`: an upstream the proxy container can reach must itself be a container, and Task 7 supplies one. The ClickHouse conformance test is `Proxy`'s test.

```go
package conformance

import (
	"context"
	"testing"

	toxiclient "github.com/Shopify/toxiproxy/v2/client"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/toxiproxy"
	"github.com/testcontainers/testcontainers-go/network"
)

// toxiproxyImage is pinned so a proxy upgrade is a commit rather than a
// Tuesday.
const toxiproxyImage = "ghcr.io/shopify/toxiproxy:2.12.0"

// proxyName is the one proxy each Proxy container carries. The module
// assigns it the first proxied port, 8666.
const (
	proxyName = "upstream"
	proxyPort = 8666
)

// Proxy is a toxiproxy container between a sink and its destination.
//
// Break adds a timeout toxic, which holds every connection open and never
// answers. That is a partition, not a refusal: the failure #219 fixed for,
// and the one honours_context needs. A refused connection is a different
// fault and a different invariant.
type Proxy struct {
	// Addr is the host:port the sink dials instead of the destination.
	Addr string

	client *toxiclient.Client
}

// NewProxy starts toxiproxy on network, forwarding to upstream, which must be
// "<alias>:<port>" of a container already on that network. The proxy
// container is terminated when the test ends.
func NewProxy(t *testing.T, nw *testcontainers.DockerNetwork, upstream string) *Proxy {
	t.Helper()
	ctx := context.Background()

	ctr, err := toxiproxy.Run(ctx, toxiproxyImage,
		network.WithNetwork([]string{"toxiproxy"}, nw),
		toxiproxy.WithProxy(proxyName, upstream),
	)
	if err != nil {
		t.Fatalf("conformance: start toxiproxy: %v", err)
	}
	t.Cleanup(func() {
		// Teardown must not change the verdict.
		_ = ctr.Terminate(context.Background())
	})

	host, port, err := ctr.ProxiedEndpoint(proxyPort)
	if err != nil {
		t.Fatalf("conformance: toxiproxy proxied endpoint: %v", err)
	}
	uri, err := ctr.URI(ctx)
	if err != nil {
		t.Fatalf("conformance: toxiproxy control uri: %v", err)
	}
	return &Proxy{Addr: host + ":" + port, client: toxiclient.NewClient(uri)}
}

// Break makes every connection through the proxy hang without answering.
func (p *Proxy) Break(t *testing.T) {
	t.Helper()
	proxy, err := p.client.Proxy(proxyName)
	if err != nil {
		t.Fatalf("conformance: look up proxy: %v", err)
	}
	// timeout=0 holds the connection open forever; a positive value would
	// close it after that many milliseconds, which is a refusal in slow
	// motion rather than a hang.
	if _, err := proxy.AddToxic("hang", "timeout", "downstream", 1.0,
		toxiclient.Attributes{"timeout": 0}); err != nil {
		t.Fatalf("conformance: add timeout toxic: %v", err)
	}
}

// Heal removes the hang.
func (p *Proxy) Heal(t *testing.T) {
	t.Helper()
	proxy, err := p.client.Proxy(proxyName)
	if err != nil {
		t.Fatalf("conformance: look up proxy: %v", err)
	}
	if err := proxy.RemoveToxic("hang"); err != nil {
		t.Fatalf("conformance: remove timeout toxic: %v", err)
	}
}
```

`"downstream"` is the direction from the proxy toward the client; toxiproxy applies a downstream toxic to bytes flowing back to the sink, so a request goes out and its response never returns. That is the hang the sink observes.

- [ ] **Step 3: Build and vet**

Run: `go build ./... && go vet ./internal/conformance/`
Expected: no output.

- [ ] **Step 4: Commit**

```bash
git add go.mod go.sum internal/conformance/proxy.go
git commit -m "conformance: a toxiproxy container stands between a sink and its destination

Break adds a timeout toxic. Every connection through the proxy hangs
without answering, which is a partition rather than a refusal: the fault
that #219 fixed for, and the one honours_context will need. Heal removes
it. The subject in the next commit is the proof; a proxy needs an
upstream it can reach, and only a container is one."
```

---

### Task 7: The ClickHouse subject at integration level

**Files:**
- Create: `internal/sinks/conformance_test.go`

**Interfaces:**
- Consumes: `conformance.SinkSubject`, `conformance.Sinks`, `conformance.NewProxy`, `conformance.Row` from Tasks 5–6; `NewClickhouseSink`, `ClickhouseSink.conn`, `ClickhouseSink.table` from the existing sink.

- [ ] **Step 1: Write the integration test**

Create `internal/sinks/conformance_test.go`:

```go
package sinks

// The ClickHouse sink under the conformance harness, against a real server
// behind toxiproxy.
//
// This is the first subject. Everything the harness needs from an
// integration is here: build the sink, break and heal its network, read back
// what it delivered, and produce a one-row table its DDL accepts. A second
// sink supplies the same five things and nothing else.

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/testcontainers/testcontainers-go"
	tcclickhouse "github.com/testcontainers/testcontainers-go/modules/clickhouse"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/conformance"
	"github.com/zeebo/assert"
)

// clickhouseImage is pinned so a server upgrade is a commit rather than a
// Tuesday. 26.8 is the self-hosted version the ClickHouse docs page was
// verified against.
const clickhouseImage = "clickhouse/clickhouse-server:26.8"

func TestIntegrationSinkClickhouse_Conformance(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test: -short runs the unit pass only")
	}
	ctx := context.Background()

	nw, err := network.New(ctx)
	if err != nil {
		t.Fatalf("docker network: %v", err)
	}
	t.Cleanup(func() { _ = nw.Remove(context.Background()) })

	ch, err := tcclickhouse.Run(ctx, clickhouseImage,
		network.WithNetwork([]string{"clickhouse"}, nw),
		testcontainers.WithExposedPorts("8123/tcp"),
	)
	if err != nil {
		t.Fatalf("start clickhouse: %v", err)
	}
	t.Cleanup(func() { _ = ch.Terminate(context.Background()) })

	// The sink speaks HTTP on 8123 by default; the proxy forwards to the
	// container's alias on the shared network.
	proxy := conformance.NewProxy(t, nw, "clickhouse:8123")

	table := fmt.Sprintf("conformance_%d", time.Now().UnixNano())
	dsn := "clickhouse://" + proxy.Addr + "/default"

	// A direct connection for DDL and read-back, so the harness's own
	// reads never go through the fault it injects.
	direct, err := NewClickhouseSink(config.ClickhouseSink{
		DSN: mustConnectionString(t, ch), Table: table})
	assert.NoError(t, err)
	t.Cleanup(func() { direct.Close() })
	assert.NoError(t, direct.conn.Exec(ctx,
		"CREATE TABLE "+table+" (id Int64) ENGINE = MergeTree() ORDER BY id"))

	conformance.Sinks(t, conformance.SinkSubject{
		Integration: "sink.clickhouse",
		New: func(t *testing.T) core.Sink {
			s, err := NewClickhouseSink(config.ClickhouseSink{DSN: dsn, Table: table})
			assert.NoError(t, err)
			t.Cleanup(func() { s.Close() })
			return s
		},
		Break: proxy.Break,
		Heal:  proxy.Heal,
		ReadBack: func(t *testing.T) []conformance.Row {
			rows, err := direct.conn.Query(ctx, "SELECT id FROM "+table+" ORDER BY id")
			assert.NoError(t, err)
			defer rows.Close()
			var out []conformance.Row
			for rows.Next() {
				var id int64
				assert.NoError(t, rows.Scan(&id))
				out = append(out, conformance.Row{"id": id})
			}
			assert.NoError(t, rows.Err())
			return out
		},
		Table: func(t *testing.T, id int64) arrow.Table {
			schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)
			b := array.NewRecordBuilder(memory.NewGoAllocator(), schema)
			defer b.Release()
			b.Field(0).(*array.Int64Builder).Append(id)
			rec := b.NewRecord()
			defer rec.Release()
			return array.NewTableFromRecords(schema, []arrow.Record{rec})
		},
	})
}

// mustConnectionString is the container's HTTP endpoint on the host, for the
// direct connection. The module's ConnectionString reports the native port;
// the sink's dsn parser treats a clickhouse:// port as HTTP, so the mapped
// 8123 is looked up by hand.
func mustConnectionString(t *testing.T, ch *tcclickhouse.ClickHouseContainer) string {
	t.Helper()
	host, err := ch.Host(context.Background())
	assert.NoError(t, err)
	port, err := ch.MappedPort(context.Background(), "8123/tcp")
	assert.NoError(t, err)
	return fmt.Sprintf("clickhouse://%s:%s/default", host, port.Port())
}
```

Add `"github.com/turbolytics/sql-flow/internal/core"` to the imports. If `ClickhouseSink` has no `Close` method, add one to `clickhouse.go`:

```go
// Close releases the connection. Buffered batches are dropped; a caller that
// wants them delivered flushes first.
func (s *ClickhouseSink) Close() error { return s.conn.Close() }
```

- [ ] **Step 2: Run it, expect a pass, and time it**

Run: `time go test ./internal/sinks/ -run '^TestIntegrationSinkClickhouse_Conformance$' -v 2>&1 | tail -25`
Expected: PASS, with these lines in the output:

```
=== RUN   TestIntegrationSinkClickhouse_Conformance/sink.flush.keeps_batch
    conformance.go:NN: COVERS invariant=sink.flush.keeps_batch integration=sink.clickhouse
--- PASS: TestIntegrationSinkClickhouse_Conformance
```

Expected wall time under 90s; the ClickHouse image pull is the bulk of it on a cold cache.

If the broken `Flush` does not return within `flushTimeout`, the HTTP client is not honouring the context — that would itself be a finding against `sink.flush.honours_context`, and is reported by the harness's `t.Fatalf("... Break did not break it")` after 10s. Check the `timeout` toxic direction first (`downstream`), then the DSN really points at `proxy.Addr`.

- [ ] **Step 3: Confirm `-short` skips before any container starts**

Run: `go test -short ./internal/sinks/ -run '^TestIntegrationSinkClickhouse_Conformance$' -v 2>&1 | grep -E "SKIP|PASS|FAIL"`
Expected: `--- SKIP: TestIntegrationSinkClickhouse_Conformance` in well under a second.

- [ ] **Step 4: Run the whole integration pass as CI does**

Run: `go test -json -run '^TestIntegration' ./... > .coverage/go-integration.json; grep -c '"Action":"fail"' .coverage/go-integration.json`
Expected: `0`. The Kafka integration tests still pass alongside.

- [ ] **Step 5: Commit**

```bash
git add internal/sinks/conformance_test.go internal/sinks/clickhouse.go
git commit -m "sinks: ClickHouse is the first subject of the conformance harness

A real server behind toxiproxy. The harness writes a row, hangs the
connection, sees Flush fail, removes the hang, sees Flush succeed, and
reads the row back from a direct connection that never crosses the
fault. sink.flush.keeps_batch holds for ClickHouse, and the marker says
so.

Everything the harness needed from the integration is here: five
functions and a DDL. A second sink supplies the same five and nothing
else, which is the claim this commit exists to test.

Evidence: passes in 41s locally with a warm image cache; -short skips
before any container starts; the full integration pass reports no
failure."
```

Replace `41s` with the measured time from Step 2.

---

### Task 8: Regenerate the matrix, and describe the new files where readers look

**Files:**
- Regenerate: `docs/coverage/matrix.json`, `docs/coverage/matrix.md`
- Modify: `docs/coverage/features.yml:1-30` (the header comment, one pointer)
- Modify: `README.md` Development section (one sentence)

- [ ] **Step 1: Regenerate from a full run**

Run: `SQLFLOW_CLICKHOUSE_DSN=clickhouse://127.0.0.1:1/default make coverage-matrix 2>&1 | tail -3`
Expected: `wrote docs/coverage/matrix.json and docs/coverage/matrix.md`. The DSN override is needed only if a dev-stack ClickHouse is up on this machine; it keeps the seven skip-on-absent unit tests skipping, which is what CI sees.

- [ ] **Step 2: Confirm the ClickHouse cell is covered and nothing else moved**

Run:
```bash
uv run --locked python - <<'EOF'
import json
m = json.load(open("docs/coverage/matrix.json"))
inv = next(i for i in m["invariants"] if i["id"] == "sink.flush.keeps_batch")
for integ, levels in inv["integrations"].items():
    print(f"{integ:18} {levels['integration']['status']}")
print("gaps:", m["gaps"], m["invariant_gaps"])
print("unattributed:", m["unattributed"])
EOF
```
Expected:
```
sink.kafka         missing
sink.clickhouse    covered
sink.iceberg       missing
sink.sqlcommand    exempt
sink.console       exempt
sink.noop          exempt
gaps: [] []
unattributed: {'unit': [], 'integration': [], 'release': []}
```

`unattributed.integration` must be empty: the conformance test attributes by marker, and Task 3's rule keeps it out of that list.

- [ ] **Step 3: Run the gate**

Run: `git add docs/coverage/ && SQLFLOW_CLICKHOUSE_DSN=clickhouse://127.0.0.1:1/default make coverage-check >/dev/null 2>&1; echo "exit=$?"`
Expected: `exit=0`.

- [ ] **Step 4: Point readers at the new registries**

In `docs/coverage/features.yml`, after the line `# invents a feature.` (the end of the header comment), add:

```yaml
#
# Invariants -- what every integration must hold -- are a separate axis.
# See invariants.yml and integrations.yml beside this file.
```

In `README.md`, in the Development section, after the sentence ending `and no dependency is resolved at install time.`, add:

```markdown
`docs/coverage/matrix.md` is the coverage matrix: one table per feature, and
one per invariant family. `internal/conformance` proves the invariants; a new
sink or source supplies a subject and inherits the whole contract.
```

- [ ] **Step 5: Run everything once more**

Run: `go build ./... && go vet ./... && go test -short ./... && uv run --locked pytest tests/tooling -q && git diff --exit-code docs/coverage/`
Expected: every package `ok`, `69 passed`, and the last command silent (the regenerated matrix is what is staged).

- [ ] **Step 6: Commit**

```bash
git add docs/coverage/ README.md
git commit -m "coverage: the matrix carries its first proven invariant

sink.flush.keeps_batch on sink.clickhouse is covered at integration
level, by the harness, behind toxiproxy. Kafka and Iceberg are missing;
console, noop and sqlcommand are exempt with reasons. No level is
required, so nothing fails.

That is the foundation working: a declared contract, one mechanism that
proves it, and a matrix that says honestly which integrations have not
been asked yet. The next commit adds the Kafka subject, watches it fail,
and fixes the sink."
```

---

## Self-review

**Spec coverage.** Registries (Task 2), validation rules 1 and 2 (Tasks 2 and 4), structured marker (Task 1), harness with one sequence and one assertion (Task 5), `Proxy` with the `timeout` toxic (Task 6), the ClickHouse subject (Task 7), the second matrix axis with gaps and exempt cells (Task 3), the unattributed-report fix (Task 3), `Kinds()` agreement (Task 4), every `requires` empty (Task 2's test enforces it), no `types` blocks (Task 2). PR 2 and PR 3 are their own plans. The #210 page fields are listed under "After" in the spec and are not in this plan.

**Placeholders.** Task 5 Step 3 contained a deliberately marked throwaway `itoa`; the replacement immediately follows and the plan says the first must not ship. No other TBDs.

**Type consistency.** `parse_go` returns a 3-tuple everywhere after Task 1. `build_invariants(invariants, integrations, results, evidence)` and `snapshot_invariants(invariants, integrations, built)` match between Task 3's code and tests. `SinkSubject` has the same five fields plus `Table` in Task 5's definition, Task 5's tests and Task 7's subject. `Proxy.Break`/`Heal` take `*testing.T`, matching `SinkSubject.Break`/`Heal`. `coverage.Invariant` takes `testing.TB`; `*testing.T` satisfies it at every call site. `Kinds()` returns sorted bare names in all three packages, and `coverage.Integrations` returns the same form.
