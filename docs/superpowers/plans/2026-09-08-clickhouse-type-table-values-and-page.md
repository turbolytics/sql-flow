# ClickHouse Type Table, PR 3: Values, Timestamps and the Published Page

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `exact` mean the value survived, prove a timestamp keeps its instant, and generate the ClickHouse page's type table from the declaration a test judges.

**Architecture:** Every non-unsupported row gains a declared `expect:` — the value the destination must hold, written down rather than derived from what the sink returned. `type.roundtrip` compares against it, so `exact` stops meaning "something non-null came back". `type.timestamp.instant` then adds what equality alone cannot say: that a zone-less value is UTC and the host's zone never reaches the destination. Finally a renderer turns the declaration into `docs/coverage/clickhouse-types.mdx`, gated by `make coverage-check` like `matrix.md`.

**Tech Stack:** Go 1.x with CGO, arrow-go v18, clickhouse-go, testcontainers-go, Python 3 for the generator.

**Spec:** `docs/superpowers/specs/2026-09-07-clickhouse-type-table-design.md` — PR 3 of "Sequence", extended with the value comparison the spec's `TypeDecl.Expect` field described and PR 1 did not implement.

## Global Constraints

- All prose follows Google Technical Writing One, per `CLAUDE.md`. Active voice. One idea per sentence. Explain why, not what.
- No attribution lines in commit messages.
- The unit pass is `CGO_ENABLED=1 go test -short ./...`. It must stay green after every task.
- **Never run `make coverage-matrix` on a machine with the dev stack up.** Check `docker ps` first. A live ClickHouse on `127.0.0.1:8123` or Kafka on `:9092` makes ten tests run that skip on a runner, and the committed file then disagrees with the gate. If either is up, push and take CI's `coverage-matrix` artifact.
- **An `expect:` value is a claim about what the destination should hold, not a transcript of what it returned.** When the run disagrees, decide which side is wrong by reading the value. Recording the server's answer without checking it makes the test tautological, which is worse than no test.
- Integration tests are named `TestIntegration<Feature>_<Behaviour>`, start what they need in a container, and never skip.
- Every test carries a `coverage.Covers` marker.
- `requires:` on all six type invariants stays `[]`.

---

### Task 1: `exact` means the value survived

`judgeTypeRow` reads a row back and checks only that it is not nil, so 37 rows
declared `exact` and 2 declared `coerced` assert that the sink accepted the
value and something came back. The spec's `TypeDecl` carried an `Expect` field
for this and PR 1 did not implement it.

**Files:**
- Modify: `internal/coverage/registry.go`
- Modify: `internal/conformance/types.go`
- Modify: `internal/conformance/types_test.go`

**Interfaces:**
- Consumes: `coverage.TypeDecl` from PR 1.
- Produces: `coverage.TypeDecl.Expect string`, and a `type.roundtrip` that compares. Task 2 fills the declarations; Task 4's renderer reads `Expect` for the page's examples.

- [ ] **Step 1: Write the failing self-tests**

Append to `internal/conformance/types_test.go`:

```go
// The hole this closes: a sink that takes a value, stores something else, and
// returns it without an error passed type.roundtrip for as long as the
// read-back was not nil.
func TestToolingConformanceTypes_AValueChangedInFlightIsCaught(t *testing.T) {
	coverage.Covers(t, "tooling.conformance")

	declared := []coverage.TypeDecl{
		{Key: "utf8", Outcome: "exact", Columns: []string{"String"}, Expect: "hello"},
		{Key: "list<int64>", Outcome: "exact", Columns: []string{"Array(Int64)"}, Expect: "[1]"},
	}
	s := typeSubject(newTypeSink("utf8", "list<int64>"), declared)
	s.ListElementNulls = coverage.NullRule{Outcome: "exact"}
	s.Prepare = func(t *testing.T, key, columnType string) TypeDestination {
		sink := newTypeSink("utf8", "list<int64>")
		return TypeDestination{
			Sink: core.Sink(sink),
			// A destination that stores something other than what it took.
			ReadBack:            func(t *testing.T) (any, error) { return "something else", nil },
			ReadBackNullElement: func(t *testing.T) (bool, error) { return true, nil },
		}
	}

	assertTypeFailure(t, typeVerdicts(t, s), typeRoundtrip, "something else")
}

// A row claiming a value survives and declaring no expectation cannot be
// judged, and must say so rather than pass.
func TestToolingConformanceTypes_AnExactRowWithNoExpectationIsCaught(t *testing.T) {
	coverage.Covers(t, "tooling.conformance")

	declared := []coverage.TypeDecl{
		{Key: "int64", Outcome: "exact", Columns: []string{"Int64"}},
	}
	s := typeSubject(newTypeSink("int64"), declared)

	assertTypeFailure(t, typeVerdicts(t, s), typeRoundtrip, "no expect")
}

// An unsupported row needs no expectation: it never reaches a destination.
func TestToolingConformanceTypes_AnUnsupportedRowNeedsNoExpectation(t *testing.T) {
	coverage.Covers(t, "tooling.conformance")

	declared := []coverage.TypeDecl{
		{Key: "int64", Outcome: "unsupported", Code: "user.sink.type_unsupported"},
	}
	v := typeVerdicts(t, typeSubject(newTypeSink(), declared))
	for _, entry := range v {
		if entry.invariant == typeRoundtrip && entry.failure != "" {
			t.Fatalf("type.roundtrip failed on an unsupported row: %s", entry.failure)
		}
	}
}
```

- [ ] **Step 2: Run them to verify they fail**

Run: `CGO_ENABLED=1 go test ./internal/conformance/ -run TestToolingConformanceTypes_A 2>&1 | tail -5`
Expected: FAIL to compile, `unknown field Expect in struct literal`.

- [ ] **Step 3: Add the field**

In `internal/coverage/registry.go`, add to `TypeDecl` below `Rule`:

```go
	// Expect is what the destination must hold after the row is written, as
	// the subject's ReadBack renders it. Required when Outcome is exact or
	// coerced.
	//
	// Declared rather than derived from the run. A test that records whatever
	// the sink returned and compares the sink to itself proves nothing, and
	// this value is what the published page's "exact" means.
	Expect string `yaml:"expect"`
```

- [ ] **Step 4: Compare in the runner**

In `internal/conformance/types.go`, replace `judgeTypeRow`'s final non-null
check:

```go
		if got == nil {
			return fmt.Sprintf("%s into %s: the value read back as null", d.Key, columnType)
		}
		if d.Expect == "" {
			return fmt.Sprintf("%s into %s is declared %s and the table names no "+
				"expect, so nothing checks what the destination holds. A row that "+
				"claims a value survives must say what survives", d.Key, columnType, d.Outcome)
		}
		if s, ok := got.(string); ok && s != d.Expect {
			return fmt.Sprintf("%s into %s read back as %q, and the table expects "+
				"%q. Either the sink changed what it stores, or the table was "+
				"wrong when it was written", d.Key, columnType, s, d.Expect)
		}
```

- [ ] **Step 5: Run the self-tests**

Run: `CGO_ENABLED=1 go test ./internal/conformance/ -run TestToolingConformanceTypes -v 2>&1 | grep -E "^(---|FAIL|ok)"`
Expected: PASS. The baseline `ADoubleThatHonoursItsTablePasses` needs `Expect`
on its rows; its double returns the written value, so `Expect` is that value's
rendering. Add `Expect: "-9223372036854775808"` to its `int64` row and set the
`utf8` row's to the fidelity-free canonical string the value table writes.

- [ ] **Step 6: Commit**

```bash
git add internal/coverage/registry.go internal/conformance/types.go internal/conformance/types_test.go
git commit -m "conformance: exact means the value survived, not that something came back

judgeTypeRow read a row back and checked only that it was not nil, so a sink
that took a value, stored something else and returned it without an error
passed type.roundtrip. 37 rows declared exact on that basis.

The spec carried an Expect field for this and PR 1 did not implement it. A row
claiming a value survives now says what survives, and a row that does not is
reported rather than passed."
```

---

### Task 2: Declare what ClickHouse holds

**Files:**
- Modify: `docs/coverage/integrations.yml`

**Interfaces:**
- Consumes: `TypeDecl.Expect` from Task 1.
- Produces: an `expect:` on all 39 non-unsupported rows.

- [ ] **Step 1: Declare the scalar expectations**

The subject's `ReadBack` renders through the server's `toString`, so each
`expect` is that rendering of the canonical value
`internal/conformance/lattice.go` writes. Add `expect:` to each row. These are
predictions from the canonical values; Step 3 checks them against a server.

```yaml
      bool:        {outcome: exact, columns: [Bool, UInt8], expect: "true"}
      int8:        {outcome: exact, columns: [Int8], expect: "-128"}
      int16:       {outcome: exact, columns: [Int16], expect: "-32768"}
      int32:       {outcome: exact, columns: [Int32], expect: "-2147483648"}
      int64:       {outcome: exact, columns: [Int64], expect: "-9223372036854775808"}
      uint8:       {outcome: exact, columns: [UInt8], expect: "255"}
      uint16:      {outcome: exact, columns: [UInt16], expect: "65535"}
      uint32:      {outcome: exact, columns: [UInt32], expect: "4294967295"}
      uint64:      {outcome: exact, columns: [UInt64], expect: "18446744073709551615"}
```

Keep the existing block style for the rest rather than the flow style above if
the file already uses it; the flow style is shown only to keep this plan
readable. The remaining scalar rows take:

- `float32` → `3.4028235e38`
- `float64` → `1.7976931348623157e308`
- `utf8` → the canonical string `L'Œil 👁 "quoted" back\slash	tab`
- `binary` → the four bytes the value table writes, as the server renders them
- `date32` → `2026-09-08`
- `timestamp[s]` → `2026-09-08 12:00:00`
- `timestamp[ms]` → `2026-09-08 12:00:00.123`
- `timestamp[us]` → `2026-09-08 12:00:00.123456`
- `timestamp[ns]` → `2026-09-08 12:00:00.123456789`
- `timestamp[us, tz=*]` → `2026-09-08 12:00:00.123456`, the same instant with
  the zone dropped, which is what its `coerced` rule already states

- [ ] **Step 2: Declare the container expectations**

Each `list<T>` holds three copies of its element's canonical value, so its
rendering is the element's expectation three times inside brackets. For
`list<int64>`:

```yaml
        expect: "[-9223372036854775808,-9223372036854775808,-9223372036854775808]"
```

Apply the same shape to every supported `list<T>` row, and to
`list<list<int64>>`, whose value table writes three inner lists of three.

- [ ] **Step 3: Run it against a server and correct what is wrong**

Run: `CGO_ENABLED=1 go test ./internal/sinks/ -run TestIntegrationSinkClickhouse_Types -v 2>&1 | grep -E "types.go:1|    ---"`

Every mismatch names both sides: what came back and what the table expected.
For each one, **read the value before changing anything**:

- The rendering differs in form only — a float's exponent notation, a
  timestamp's fractional digits — and the value is right. Correct `expect:`.
- The value itself is wrong. That is a sink defect, and it is the finding this
  task exists to produce. Record it in the PR description.

Do not paste the server's answer into `expect:` without reading it. A table
copied from the run compares the sink to itself.

- [ ] **Step 4: Run the full unit pass and commit**

Run: `CGO_ENABLED=1 go test -short ./...` — expected 16 packages ok.

```bash
git add docs/coverage/integrations.yml
git commit -m "coverage: ClickHouse declares what it holds, not just that it took it

One expect per row that claims a value survives, written from the canonical
value the lattice sends rather than from what the server returned. A table
copied from the run compares the sink to itself.

This is what the published page's exact means, and until now nothing checked
it."
```

---

### Task 3: `type.timestamp.instant`

Equality alone cannot say what this invariant claims. A timestamp that
round-trips through a host in UTC−4 and a destination in UTC−4 compares equal
and is still four hours wrong for everyone else. #153 was exactly that: the
driver parsed a zone-less string in `time.Local`, so the stored value depended
on where the process ran.

**Files:**
- Modify: `internal/conformance/types.go`
- Modify: `internal/conformance/types_test.go`
- Modify: `internal/sinks/conformance_types_test.go`

**Interfaces:**
- Consumes: the declarations from Task 2.
- Produces: the `type.timestamp.instant` verdict.

- [ ] **Step 1: Write the self-tests**

Append to `internal/conformance/types_test.go`:

```go
// #153's defect: clickhouse-go parses a zone-less string in time.Local, so the
// stored value depended on the host's offset. A test on a UTC laptop saw
// nothing, which is why the runner moves the host zone before it writes.
func TestToolingConformanceTypes_AHostZoneLeakIsCaught(t *testing.T) {
	coverage.Covers(t, "tooling.conformance")

	declared := []coverage.TypeDecl{
		{Key: "timestamp[us]", Outcome: "exact", Columns: []string{"DateTime64(6)"},
			Expect: "2026-09-08 12:00:00.123456"},
	}
	s := typeSubject(newTypeSink("timestamp[us]"), declared)
	s.Prepare = func(t *testing.T, key, columnType string) TypeDestination {
		sink := newTypeSink("timestamp[us]")
		return TypeDestination{
			Sink: core.Sink(sink),
			// A destination that renders in whatever zone the host is in,
			// which is the shape of a leak.
			ReadBack: func(t *testing.T) (any, error) {
				return "2026-09-08 21:00:00.123456", nil
			},
		}
	}

	assertTypeFailure(t, typeVerdicts(t, s), typeInstant, "2026-09-08 21:00:00.123456")
}

// A table with no temporal row cannot prove the claim and must say so.
func TestToolingConformanceTypes_ATableWithNoTemporalRowIsNotInstantProof(t *testing.T) {
	coverage.Covers(t, "tooling.conformance")

	declared := []coverage.TypeDecl{
		{Key: "int64", Outcome: "exact", Columns: []string{"Int64"}, Expect: "-128"},
	}
	assertTypeFailure(t, typeVerdicts(t, typeSubject(newTypeSink("int64"), declared)),
		typeInstant, "no temporal row")
}
```

- [ ] **Step 2: Run them to verify they fail**

Run: `CGO_ENABLED=1 go test ./internal/conformance/ -run TestToolingConformanceTypes_A 2>&1 | tail -4`
Expected: FAIL to compile, `undefined: typeInstant`.

- [ ] **Step 3: Write the verdict**

Add `typeInstant = "type.timestamp.instant"` to the constant block, add
`judgeTimestampInstant(t, s)` to the slice `typeVerdicts` returns, and:

```go
// judgeTimestampInstant writes every temporal row with the host in a zone that
// is not UTC.
//
// Equality alone cannot make this claim. A timestamp written from a host in
// UTC-4 into a destination reading UTC-4 compares equal and is still four
// hours wrong for everyone else, which is what #153 was: clickhouse-go parsed
// a zone-less string in time.Local, so the stored value depended on where the
// process ran, and every test on a UTC machine agreed with it.
//
// Moving time.Local is a process-wide change, so this runs before any subtest
// and restores it afterwards. It is safe because no test in this package runs
// in parallel; a t.Parallel() anywhere in the package invalidates it.
func judgeTimestampInstant(t *testing.T, s TypeSubject) verdict {
	t.Helper()
	v := verdict{invariant: typeInstant}

	var temporal []coverage.TypeDecl
	for _, d := range s.Declared {
		if d.Outcome == "unsupported" {
			continue
		}
		if strings.HasPrefix(d.Key, "timestamp[") || d.Key == "date32" {
			temporal = append(temporal, d)
		}
	}
	if len(temporal) == 0 {
		v.failure = "the type table declares no temporal row, so nothing here " +
			"proves a timestamp keeps its instant"
		return v
	}

	// Deliberately not UTC, and deliberately not the zone the value carries.
	// A runner that leaves the host in UTC proves nothing about a leak.
	restore := time.Local
	time.Local = time.FixedZone("conformance", 9*3600)
	defer func() { time.Local = restore }()

	for _, d := range temporal {
		for _, columnType := range d.Columns {
			if f := judgeTypeRow(t, s, d, false); f != "" {
				v.failure = fmt.Sprintf("with the host nine hours from UTC: %s", f)
				return v
			}
			_ = columnType
		}
	}
	return v
}
```

Add `"time"` to the file's imports.

- [ ] **Step 4: Run the self-tests**

Run: `CGO_ENABLED=1 go test ./internal/conformance/ -run TestToolingConformanceTypes -v 2>&1 | grep -E "^(---|FAIL|ok)"`
Expected: PASS. The baseline double's table needs a temporal row with an
`Expect`; give it `timestamp[us]` and the rendering its own value table writes.

- [ ] **Step 5: Confirm no test in the package runs in parallel**

Run: `grep -rn "t.Parallel()" internal/conformance/ internal/sinks/`
Expected: no output. `judgeTimestampInstant` moves `time.Local`, which is
process-wide; a parallel test would see it. If this finds one, stop and report
rather than proceeding.

- [ ] **Step 6: Run against a server**

Run: `CGO_ENABLED=1 go test ./internal/sinks/ -run TestIntegrationSinkClickhouse_Types -v 2>&1 | grep -E "types.go:1|    ---"`
Expected: six subtests, `type.timestamp.instant` among them.

A failure here with the host nine hours from UTC and none without it is a live
zone leak. Record the offset and the stored value in the PR description.

- [ ] **Step 7: Run the full unit pass and commit**

```bash
git add internal/conformance/types.go internal/conformance/types_test.go
git commit -m "conformance: a timestamp keeps its instant, whatever zone the host is in

Equality alone cannot make this claim. A timestamp written from a host in
UTC-4 into a destination reading UTC-4 compares equal and is still four hours
wrong for everyone else. That is #153: clickhouse-go parsed a zone-less string
in time.Local, so the stored value depended on where the process ran, and every
test on a UTC machine agreed with it.

The runner moves the host nine hours from UTC before it writes, so a leak
changes the value rather than hiding in agreement."
```

---

### Task 4: The published page, generated and gated

**Files:**
- Create: `docs/coverage/clickhouse-types.mdx`
- Modify: `scripts/coverage_matrix.py`
- Modify: `Makefile`
- Test: `tests/tooling/test_coverage_matrix.py`

**Interfaces:**
- Consumes: `lattice.yml`'s `duckdb` field and `integrations.yml`'s `types:`.
- Produces: a generated fragment, diffed by `make coverage-check`.

- [ ] **Step 1: Write the failing Python tests**

Append to `tests/tooling/test_coverage_matrix.py`:

```python
def test_the_page_is_keyed_by_duckdb_type_not_arrow():
    """A user writes a CAST and never sees an Arrow type."""
    md = cm.render_types_page(LATTICE, typed({
        "int64": {"outcome": "exact", "columns": ["Int64"], "expect": "1"},
        "utf8": {"outcome": "exact", "columns": ["String"], "expect": "a"},
    })[0])
    assert "BIGINT" in md
    assert "int64" not in md.split("## Known limits")[0].replace("Int64", "")


def test_the_page_lists_every_column_type_that_accepts_a_key():
    md = cm.render_types_page(LATTICE, typed({
        "int64": {"outcome": "exact", "columns": ["Int64", "Int128"], "expect": "1"},
        "utf8": {"outcome": "exact", "columns": ["String"], "expect": "a"},
    })[0])
    assert "Int64" in md and "Int128" in md


def test_the_page_separates_what_is_unsupported():
    md = cm.render_types_page(LATTICE, typed({
        "int64": {"outcome": "exact", "columns": ["Int64"], "expect": "1"},
        "utf8": {"outcome": "unsupported", "code": "user.sink.type_unsupported"},
    })[0])
    assert "VARCHAR" in md.split("Unsupported")[1]


def test_the_page_states_a_coercion_rule():
    md = cm.render_types_page(LATTICE, typed({
        "int64": {"outcome": "coerced", "columns": ["Int64"],
                  "rule": "rounded to the nearest whole number", "expect": "1"},
        "utf8": {"outcome": "exact", "columns": ["String"], "expect": "a"},
    })[0])
    assert "rounded to the nearest whole number" in md
```

- [ ] **Step 2: Run them to verify they fail**

Run: `uv run pytest tests/tooling/test_coverage_matrix.py -q -k page`
Expected: FAIL, `module 'coverage_matrix' has no attribute 'render_types_page'`.

- [ ] **Step 3: Write the renderer**

Add to `scripts/coverage_matrix.py`:

```python
TYPES_PAGE = os.path.join(REPO, "docs", "coverage", "clickhouse-types.mdx")


def render_types_page(lattice, integration):
    """The type mapping table, as the ClickHouse integration page publishes it.

    Keyed by DuckDB SQL type rather than by Arrow type, because a user writes a
    CAST and never sees an Arrow type. The Arrow key is the join, and it stays
    out of the output.

    Generated from the same declaration the type runner judges, so the page
    cannot claim something no test proved. That is the whole point: every row of
    the hand-written table it replaces was measured once, by hand, and nothing
    held it to the sink afterwards.
    """
    by_key = integration.get("types", {})
    supported, unsupported = [], []

    for entry in lattice:
        decl = by_key.get(entry["key"])
        if not decl:
            continue
        casts = ", ".join(f"`{d}`" for d in entry["duckdb"])
        if decl["outcome"] == "unsupported":
            unsupported.append(f"- {casts}")
            continue
        cols = ", ".join(f"`{c}`" for c in decl.get("columns", []))
        note = decl.get("rule", "")
        supported.append(f"| {casts} | {cols} | {note} |")

    out = [
        "<!-- Generated by scripts/coverage_matrix.py. Do not edit. -->",
        "<!-- Every row is proven by TestIntegrationSinkClickhouse_Types. -->",
        "",
        "## Type mapping {#type-mapping}",
        "",
        "sqlflow hands each batch to ClickHouse as rows built from the Arrow "
        "table DuckDB produces, so what matters is the type each SQL column "
        "ends up with. Cast in the handler SQL to control it.",
        "",
        "| DuckDB SQL type (cast to) | ClickHouse column types that accept it | Notes |",
        "| --- | --- | --- |",
    ]
    out += supported
    out += [
        "",
        "Values are matched to columns **by name**, not by position.",
        "",
        "### Unsupported {#unsupported}",
        "",
        "These fail the batch with `user.sink.type_unsupported` rather than "
        "reaching the table. Cast or flatten them in the handler SQL.",
        "",
    ]
    out += unsupported
    out.append("")
    return "\n".join(out)
```

- [ ] **Step 4: Write it, and gate it**

In `main`, beside the `matrix.md` write, add the page and include it in the
`--write` path and the check. Then add it to the `coverage-check` diff in the
`Makefile` alongside `docs/coverage/matrix.md`.

- [ ] **Step 5: Run the Python suite**

Run: `uv run pytest tests/tooling -q`
Expected: all pass, including the four new page tests.

- [ ] **Step 6: Generate, read, and commit**

Check `docker ps` first, per the Global Constraints.

Run: `make coverage-matrix`, then **read `docs/coverage/clickhouse-types.mdx`
end to end** and compare it against the hand-written table in CH#117740. A row
the generated page contradicts is a finding in one of them; say which in the PR
description.

```bash
git add docs/coverage/clickhouse-types.mdx scripts/coverage_matrix.py Makefile tests/tooling/test_coverage_matrix.py docs/coverage/matrix.json docs/coverage/matrix.md
git commit -m "coverage: generate the ClickHouse page's type table from the declaration

Every row of the hand-written table in ClickHouse/ClickHouse#117740 was
measured once, by hand, and nothing held it to the sink afterwards. The first
type change would have made the page wrong with no test to say so.

Keyed by DuckDB SQL type rather than Arrow type, because a user writes a CAST
and never sees an Arrow type, and gated by coverage-check so it cannot drift
from what the runner proves."
```

---

## Self-Review

**Spec coverage.** The spec's PR 3 is "the session-zone pin, the four timestamp
units and the `tz=*` row, so `type.timestamp.instant` gets its cell. Then the
renderer, and the generated fragment replaces the hand-written table." Task 3
covers the first, Task 4 the second. Tasks 1 and 2 are new: the spec's
`TypeDecl` declared an `Expect` field that PR 1 did not implement, and
publishing 37 unverified `exact` rows would defeat the point of generating the
page at all.

**Placeholders.** Task 2 Step 1 lists nine rows in full and describes the
remaining thirty by their canonical value rather than writing each line. That
is a deliberate compression: the values come from
`internal/conformance/lattice.go`, which the executor reads, and Step 3 is the
step that settles them. Every other step carries its content.

**Type consistency.** `TypeDecl.Expect` (Task 1) is declared in Task 2 and read
by `judgeTypeRow` and `render_types_page`. `typeInstant` (Task 3) joins the same
constant block as `typeRoundtrip`, `typeNull`, `typeNested`, `typeFidelity` and
`typeUndeclared`, and reaches the same returned slice. `render_types_page(lattice,
integration)` takes the same shapes `validate_types` already takes.

**Risk this plan carries.** `judgeTimestampInstant` moves `time.Local`, which is
process-wide. Task 3 Step 5 checks that nothing in the affected packages runs in
parallel, and says to stop rather than proceed if that check fails.
