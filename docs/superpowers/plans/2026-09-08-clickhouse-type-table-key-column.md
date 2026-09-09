# Type Table: the Unit Is (Key, Column) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the declared unit a (Arrow key, destination column) pair, so a row can write different content into different columns.

**Architecture:** `columns:` stops being a list of names and becomes a list of entries carrying the value to write, what the destination must hold, and whether the pair belongs to the timestamp claim. That subsumes two fields added ad hoc during PR 3 — `expect_per_column` and `temporal_string` — so this removes mechanism rather than adding it.

**Tech Stack:** Go 1.x with CGO, arrow-go v18, clickhouse-go, testcontainers-go, Python 3 for the generator.

**Spec:** `docs/superpowers/specs/2026-09-07-clickhouse-type-table-design.md`. This changes the spec's `types:` shape, which declared one outcome and one column list per Arrow key.

## Why the key alone is the wrong unit

DuckDB's `VARCHAR` is the universal text carrier, and ClickHouse parses text
into `UUID`, `IPv4`, `Decimal`, `Enum` and `FixedString`. Those are five
destinations for one Arrow key, and each demands different content: the
canonical `utf8` value is a hostile string full of escapes, and no `UUID`
column will take it.

So `utf8` is not one case with several destinations. It is several cases that
share an Arrow type, and the value belongs to the pair rather than to the key.

The same shape explains the two fields PR 3 added:

- `expect_per_column` exists because one value renders differently per column
  (`bool` is `true` in `Bool` and `1` in `UInt8`).
- `temporal_string` exists because a `utf8` value bound for a `DateTime` column
  is #153, and no Arrow key describes that pairing.

Both are the (key, column) unit showing through a design that only had keys.

## Global Constraints

- All prose follows Google Technical Writing One, per `CLAUDE.md`.
- No attribution lines in commit messages.
- The unit pass is `CGO_ENABLED=1 go test -short ./...`, green after every task.
- **Never run `make coverage-matrix` with the dev stack up.** Check `docker ps`;
  if `clickhouse` or `kafka1` is running, push and take CI's artifact.
- `value:` is a string written as a `utf8` array. It is meaningful only on the
  `utf8` row, because text is the only carrier a destination reparses. The
  runner rejects it elsewhere rather than silently ignoring it.
- `requires:` on all six type invariants stays `[]`.

## The new shape

```yaml
      utf8:
        outcome: exact
        columns:
          - type: String
            expect: "L'Œil 👁 …"
          - type: "UUID"
            value: "0b7e2c9a-...-e3f1"
            expect: "0b7e2c9a-...-e3f1"
          - type: "Decimal(10, 2)"
            value: "1234.56"
            expect: "1234.56"
          - type: "DateTime64(3)"
            value: "2026-09-01 12:00:00.123"
            expect: "2026-09-01 12:00:00.123"
            instant: true
```

`value` absent means the key's canonical value from
`internal/conformance/lattice.go`. `instant: true` marks a pair the timestamp
verdict must exercise under a shifted host clock.

---

### Task 1: The declaration carries column entries

**Files:**
- Modify: `internal/coverage/registry.go`
- Modify: `internal/coverage/registry_test.go`
- Modify: `docs/coverage/integrations.yml`
- Modify: `scripts/coverage_matrix.py`, `tests/tooling/test_coverage_matrix.py`

**Interfaces:**
- Produces: `coverage.ColumnDecl{Type, Value, Expect string; Instant bool}` and
  `TypeDecl.Columns []ColumnDecl`. Task 2's runner reads both.

- [ ] **Step 1: Write the failing reader test**

Append to `internal/coverage/registry_test.go`:

```go
// The unit is a pair, not a key. DuckDB's VARCHAR is the universal text
// carrier and ClickHouse reparses text into UUID, Decimal and Enum, so one
// Arrow key reaches destinations that each demand different content.
func TestToolingCoverageRegistry_AColumnCarriesItsOwnValue(t *testing.T) {
	Covers(t, "tooling.coverage")

	declared, err := TypesFor("sink.clickhouse")
	assert.NoError(t, err)

	byKey := map[string]TypeDecl{}
	for _, d := range declared {
		byKey[d.Key] = d
	}

	for _, d := range declared {
		if d.Outcome == "unsupported" {
			continue
		}
		if len(d.Columns) == 0 {
			t.Errorf("%s is %s and names no column", d.Key, d.Outcome)
		}
		for _, c := range d.Columns {
			if c.Type == "" {
				t.Errorf("%s has a column entry with no type", d.Key)
			}
			if c.Expect == "" {
				t.Errorf("%s into %s names no expect", d.Key, c.Type)
			}
			// Only text is reparsed by a destination, so only the utf8 row
			// may override the value it writes.
			if c.Value != "" && d.Key != "utf8" {
				t.Errorf("%s into %s declares a value, and only utf8 may: a "+
					"destination reparses text and nothing else", d.Key, c.Type)
			}
		}
	}
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `CGO_ENABLED=1 go test ./internal/coverage/ -run AColumnCarriesItsOwnValue`
Expected: FAIL to compile, `c.Type undefined (type string has no field Type)`.

- [ ] **Step 3: Change the Go types**

In `internal/coverage/registry.go`, replace `TypeDecl.Columns`,
`TypeDecl.Expect` and `TypeDecl.ExpectPerColumn` with:

```go
	// Columns are the destination column types that accept this key, each
	// carrying what to write and what must come back.
	//
	// The pair is the unit rather than the key. One Arrow key reaches several
	// destinations that demand different content -- DuckDB's VARCHAR is the
	// universal text carrier, and ClickHouse reparses text into UUID, Decimal
	// and Enum -- so the value belongs here.
	Columns []ColumnDecl `yaml:"columns"`
```

and add:

```go
// ColumnDecl is one destination column type, and what the integration claims
// about writing this Arrow key into it.
type ColumnDecl struct {
	// Type is the destination column type, as its DDL spells it.
	Type string `yaml:"type"`

	// Value overrides the key's canonical value. Empty means the canonical
	// one. Only the utf8 row may set it: text is the only thing a destination
	// reparses, and a value declared for any other key would be ignored.
	Value string `yaml:"value"`

	// Expect is what the destination must hold afterwards, as ReadBack renders
	// it. Declared rather than recorded from a run: a table copied from the
	// sink compares the sink to itself.
	Expect string `yaml:"expect"`

	// Instant marks a pair the timestamp claim must exercise with the host
	// clock moved off UTC. It is how a text value bound for a temporal column
	// -- #153's shape, which no Arrow key describes -- reaches that verdict.
	Instant bool `yaml:"instant"`
}
```

Delete `TemporalString`, `TemporalStringFor` and `ExpectPerColumn`: both are
this shape seen through a design that only had keys.

- [ ] **Step 4: Migrate the declaration**

Rewrite each `sink.clickhouse` row's `columns:` from a name list to entries,
moving `expect:` onto each entry and folding `expect_per_column` in. 39 rows,
42 column entries, of which three rows have more than one column: `bool`,
`utf8` and `date32`.

Convert `temporal_string` into a `utf8` column entry:

```yaml
          - type: "DateTime64(3)"
            value: "2026-09-01 12:00:00.123"
            expect: "2026-09-01 12:00:00.123"
            instant: true
```

- [ ] **Step 5: Update the generator**

`render_types_page` reads `decl["columns"]` as strings. Take `c["type"]` from
each entry instead, and update `validate_types`' "names no destination column
type" check to look at entries. Update the page tests' fixtures to the new
shape.

- [ ] **Step 6: Run the reader test, the tooling suite and the unit pass**

Run: `CGO_ENABLED=1 go test ./internal/coverage/`
Run: `uv run pytest tests/tooling -q`
Run: `CGO_ENABLED=1 go test -short ./...`

`internal/conformance` will not compile until Task 2. Everything else passes.

- [ ] **Step 7: Commit**

```bash
git add docs/coverage/integrations.yml internal/coverage/registry.go internal/coverage/registry_test.go scripts/coverage_matrix.py tests/tooling/test_coverage_matrix.py
git commit -m "coverage: the declared unit is a (key, column) pair

DuckDB's VARCHAR is the universal text carrier and ClickHouse reparses text
into UUID, Decimal and Enum, so one Arrow key reaches destinations that each
demand different content. utf8 is not one case with several destinations; it
is several cases sharing an Arrow type.

This removes two fields rather than adding one. expect_per_column existed
because one value renders differently per column, and temporal_string existed
because a text value bound for a DateTime column is #153 and no Arrow key
describes it. Both were the pair showing through a design that had only keys."
```

---

### Task 2: The runner writes what the column asks for

**Files:**
- Modify: `internal/conformance/types.go`
- Modify: `internal/conformance/types_test.go`
- Modify: `internal/sinks/conformance_types_test.go`

**Interfaces:**
- Consumes: `coverage.ColumnDecl` from Task 1.
- Produces: a runner that builds each row's array per column entry.

- [ ] **Step 1: Write the failing self-test**

Append to `internal/conformance/types_test.go`:

```go
// The value follows the column. A row whose columns demand different content
// must write different content, or the destinations that reparse text cannot
// be covered at all.
func TestToolingConformanceTypes_AColumnValueOverridesTheCanonicalOne(t *testing.T) {
	coverage.Covers(t, "tooling.conformance")

	declared := []coverage.TypeDecl{
		{Key: "utf8", Outcome: "exact", Columns: []coverage.ColumnDecl{
			{Type: "String", Expect: "canonical"},
			{Type: "UUID", Value: "0b7e2c9a", Expect: "0b7e2c9a"},
		}},
	}
	var wrote []string
	s := typeSubject(newTypeSink("utf8"), declared)
	s.Prepare = func(t *testing.T, key, columnType string) TypeDestination {
		sink := newTypeSink("utf8")
		return TypeDestination{
			Sink: core.Sink(sink),
			ReadBack: func(t *testing.T) (any, error) {
				wrote = append(wrote, sink.lastString())
				if columnType == "UUID" {
					return "0b7e2c9a", nil
				}
				return "canonical", nil
			},
		}
	}

	typeVerdicts(t, s)
	if !slices.Contains(wrote, "0b7e2c9a") {
		t.Fatalf("the UUID column never received its declared value; wrote %v", wrote)
	}
}
```

Add `"slices"` to the imports.

- [ ] **Step 2: Run it to verify it fails**

Run: `CGO_ENABLED=1 go test ./internal/conformance/ -run AColumnValueOverrides`
Expected: FAIL to compile — `Columns` is `[]string`.

- [ ] **Step 3: Rework judgeTypeRow**

Iterate `d.Columns` as entries. Per entry, build the array from
`LatticeString(c.Value)` when `c.Value` is set and from `LatticeArray(d.Key,
null)` otherwise, and compare against `c.Expect`. Delete the `want :=
d.Expect` / `ExpectPerColumn` block.

Add to `internal/conformance/lattice.go`:

```go
// LatticeString builds a one-row utf8 array holding v. The caller releases it.
//
// It exists for the column entries that declare their own value: a UUID or a
// Decimal column reparses text, so the pair needs content the key's canonical
// value cannot supply.
func LatticeString(v string) arrow.Array {
	b := array.NewStringBuilder(memory.NewGoAllocator())
	defer b.Release()
	b.Append(v)
	return b.NewArray()
}
```

- [ ] **Step 4: Fold the instant pairs into judgeTimestampInstant**

Replace the `judgeTemporalString` call with a pass over every column entry
carrying `Instant: true`, writing it under the shifted host clock. The temporal
Arrow rows keep the treatment they have. Delete `judgeTemporalString` and the
`TypeSubject.TemporalString` field.

- [ ] **Step 5: Update the ClickHouse subject**

Drop the `coverage.TemporalStringFor` call and the `TemporalString` field from
the subject literal.

- [ ] **Step 6: Run the self-tests and the unit pass**

Run: `CGO_ENABLED=1 go test ./internal/conformance/ -v 2>&1 | grep -E "^(---|FAIL|ok)"`
Run: `CGO_ENABLED=1 go test -short ./...`
Expected: 16 packages ok.

- [ ] **Step 7: Commit**

```bash
git add internal/conformance/types.go internal/conformance/types_test.go internal/conformance/lattice.go internal/sinks/conformance_types_test.go
git commit -m "conformance: the value follows the column, not the key

A row whose columns demand different content must write different content, or
the destinations that reparse text cannot be covered at all. judgeTypeRow
builds each array from the column entry, and the timestamp verdict picks up
any pair marked instant, which is what temporal_string was."
```

---

### Task 3: The rows the old page claimed

**Files:**
- Modify: `docs/coverage/integrations.yml`

**Interfaces:**
- Consumes: Tasks 1 and 2.

- [ ] **Step 1: Declare the reparsed destinations**

Add column entries to the `utf8` row for the destinations
ClickHouse/ClickHouse#117740 claims and this framework has never written:
`Enum8('a' = 1)` with value `a`, `FixedString(36)`, `UUID`, `IPv4`, and
`Decimal(10, 2)` with value `1234.56`. Predict each `expect` from the value.

`FixedString(36)` pads to its width, so its `expect` is the padded form. Use a
36-character value to avoid needing to predict the padding.

- [ ] **Step 2: Run against a server and settle the predictions**

Run: `CGO_ENABLED=1 go test ./internal/sinks/ -run TestIntegrationSinkClickhouse_Types -v`

Read every mismatch before recording it. A rendering that differs in form is a
correction to `expect`; a value that differs is a finding, and the `Decimal`
row is the one to watch, because the old page says a `DOUBLE` fails there and
only a string works.

- [ ] **Step 3: Run everything and take CI's matrix**

Run: `CGO_ENABLED=1 go test -short ./...` and `uv run pytest tests/tooling -q`.
Check `docker ps`; if the dev stack is up, push and take CI's artifact rather
than generating the matrix locally.

- [ ] **Step 4: Read the generated page against the one it replaces**

Compare `docs/coverage/clickhouse-types.mdx` with the table in
ClickHouse/ClickHouse#117740. The three narrowed rows should now be present.
Any remaining difference is a finding in one of them; name it in the PR.

- [ ] **Step 5: Commit**

```bash
git add docs/coverage/integrations.yml docs/coverage/clickhouse-types.mdx docs/coverage/matrix.json docs/coverage/matrix.md
git commit -m "coverage: prove the reparsed destinations the page already claims

Enum, FixedString, UUID, IPv4 and Decimal all take text and parse it, and the
hand-written page has claimed them since it was measured by hand. Nothing had
written one, because the value belonged to the Arrow key and utf8 already
carried the hostile string type.string.fidelity needs.

Decimal matters most: the type is unsupported natively, so the string form is
the only route to a Decimal column."
```

---

## Self-Review

**Scope.** This changes the spec's `types:` shape, which the user approved
explicitly after the (key, column) question. It removes `expect_per_column` and
`temporal_string`, both added during PR 3 without being asked for, so the net
mechanism count falls.

**Placeholders.** Task 1 Step 4 and Task 3 Step 1 describe a mechanical
migration across 42 entries rather than listing each. The shape is given, the
counts are given, and the run settles the values.

**Type consistency.** `coverage.ColumnDecl` (Task 1) is the element type of
`TypeDecl.Columns`, read by `judgeTypeRow` and `judgeTimestampInstant` (Task 2)
and by `render_types_page` (Task 1 Step 5). `LatticeString` (Task 2) is called
only from `judgeTypeRow`.

**Risk.** This lands on the branch PR 238 already proposes, because that PR
introduces `expect_per_column` and merging a design about to be replaced would
put a dead field in main's history.
