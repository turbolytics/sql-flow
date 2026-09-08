# ClickHouse Type Table, PR 2: Nesting and String Fidelity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prove that a container does not launder what it holds, and that a string survives the sink byte for byte.

**Architecture:** The lattice grows from 29 keys to 50 by adding `list<T>` for every element type ClickHouse converts, plus two unsupported-element rows and two depth-3 witnesses. Two new verdicts join the runner: `type.nested`, which judges the constructor rows and the null-element position, and `type.string.fidelity`, which writes a corpus of hostile strings through the `utf8` row rather than the single canonical value.

**Tech Stack:** Go 1.x with CGO, arrow-go v18, clickhouse-go, testcontainers-go, Python 3 for the generator.

**Spec:** `docs/superpowers/specs/2026-09-07-clickhouse-type-table-design.md` — the "Depth" section and PR 2 of "Sequence".

## Global Constraints

- All prose follows Google Technical Writing One, per `CLAUDE.md`. Active voice. One idea per sentence. Explain why, not what.
- No attribution lines in commit messages.
- The unit pass is `CGO_ENABLED=1 go test -short ./...`. It must stay green after every task.
- **Never run `make coverage-matrix` on a machine with the dev stack up.** PR 1 shipped a wrong matrix that way: a live ClickHouse on `127.0.0.1:8123` and a live Kafka on `:9092` make ten tests run that skip in CI, and the committed file then disagrees with the gate. Check `docker ps` first. If those containers are up, take CI's `coverage-matrix` artifact instead of generating locally.
- Arrow keys are canonical, produced by `conformance.CanonicalKey`. `CanonicalKey(arrow.ListOf(x))` is `list<` + `CanonicalKey(x)` + `>`.
- Integration tests are named `TestIntegration<Feature>_<Behaviour>`, start what they need in a container, and never skip.
- Every test carries a `coverage.Covers` marker. A test without one is reported as proving nothing.
- `requires:` on all six type invariants stays `[]`.

---

### Task 1: The lattice grows to depth 2

The lattice carries one `list<int64>` today. `type.nested` cannot be judged from one container row, and the bug it guards against is per-element: `goElemType` (`clickhouse.go:446`) and `arrowValue` (`clickhouse.go:369`) are two switches over the same type set, and nothing holds them equal.

**Files:**
- Modify: `docs/coverage/lattice.yml`
- Modify: `internal/conformance/lattice.go`
- Test: `internal/coverage/registry_test.go`, `internal/conformance/lattice_test.go`

**Interfaces:**
- Consumes: `coverage.Lattice()`, `conformance.CanonicalKey`, `conformance.LatticeArray` from PR 1.
- Produces: 21 new lattice keys, and `conformance.LatticeListWithNullElement(key string) (arrow.Array, error)`. Tasks 2 and 3 use both.

- [ ] **Step 1: Update the count assertion**

In `internal/coverage/registry_test.go`, change the assertion in
`TestToolingCoverageRegistry_LatticeIsClosedAndWellFormed`:

```go
	assert.Equal(t, len(entries), 50)
```

- [ ] **Step 2: Run it to verify it fails**

Run: `CGO_ENABLED=1 go test ./internal/coverage/ -run TestToolingCoverageRegistry_LatticeIsClosedAndWellFormed`
Expected: FAIL, `29 != 50`.

- [ ] **Step 3: Add the container keys to `lattice.yml`**

Replace the `list<int64>` entry in the `--- Constructors ---` section with the
block below, leaving `fixed_size_list<int32>[3]`, `struct`, `map`,
`sparse_union` and `dictionary` where they are.

```yaml
  # One list key per element type a sink might convert. This looks repetitive
  # and it is the point: a sink converts a list element through a different
  # code path than a bare column -- in ClickHouse, goElemType rather than
  # arrowValue -- and the two are switches over the same type set that nothing
  # holds equal. A divergence shows up as one element type that works alone
  # and fails inside an Array.
  - key: "list<bool>"
    duckdb: ["BOOLEAN[]"]
    depth: 2

  - key: "list<int8>"
    duckdb: ["TINYINT[]"]
    depth: 2

  - key: "list<int16>"
    duckdb: ["SMALLINT[]"]
    depth: 2

  - key: "list<int32>"
    duckdb: ["INTEGER[]"]
    depth: 2

  - key: "list<int64>"
    duckdb: ["BIGINT[]"]
    depth: 2

  - key: "list<uint8>"
    duckdb: ["UTINYINT[]"]
    depth: 2

  - key: "list<uint16>"
    duckdb: ["USMALLINT[]"]
    depth: 2

  - key: "list<uint32>"
    duckdb: ["UINTEGER[]"]
    depth: 2

  - key: "list<uint64>"
    duckdb: ["UBIGINT[]"]
    depth: 2

  - key: "list<float32>"
    duckdb: ["REAL[]"]
    depth: 2

  - key: "list<float64>"
    duckdb: ["DOUBLE[]"]
    depth: 2

  - key: "list<utf8>"
    duckdb: ["VARCHAR[]"]
    depth: 2

  - key: "list<binary>"
    duckdb: ["BLOB[]"]
    depth: 2

  - key: "list<date32>"
    duckdb: ["DATE[]"]
    depth: 2

  - key: "list<timestamp[s]>"
    duckdb: ["TIMESTAMP_S[]"]
    depth: 2

  - key: "list<timestamp[ms]>"
    duckdb: ["TIMESTAMP_MS[]"]
    depth: 2

  - key: "list<timestamp[us]>"
    duckdb: ["TIMESTAMP[]"]
    depth: 2

  - key: "list<timestamp[ns]>"
    duckdb: ["TIMESTAMP_NS[]"]
    depth: 2

  - key: "list<timestamp[us, tz=*]>"
    duckdb: ["TIMESTAMPTZ[]"]
    depth: 2

  # A container holding something the sink cannot convert. These are the rows
  # that prove a list does not launder its element: an unsupported element
  # must fail the batch from inside a supported container, exactly as it does
  # on its own.
  - key: "list<decimal(*, *)>"
    duckdb: ["DECIMAL(p, s)[]", "HUGEINT[]"]
    depth: 2

  - key: "list<time64[us]>"
    duckdb: ["TIME[]"]
    depth: 2

  # Depth 3, two witnesses. A sink nests through one recursive pair, so depth 4
  # re-runs the code depth 3 already ran. list-of-struct is the second witness
  # because struct is unsupported: it proves the recursion carries the failure
  # up rather than flattening it away.
  - key: "list<list<int64>>"
    duckdb: ["BIGINT[][]"]
    depth: 2

  - key: "list<struct>"
    duckdb: ["STRUCT(...)[]"]
    depth: 2
```

- [ ] **Step 4: Add the value builders**

In `internal/conformance/lattice.go`, replace the `"list<int64>"` entry of
`latticeType` with these, keeping the other constructor entries:

```go
	"list<bool>":                arrow.ListOf(arrow.FixedWidthTypes.Boolean),
	"list<int8>":                arrow.ListOf(arrow.PrimitiveTypes.Int8),
	"list<int16>":               arrow.ListOf(arrow.PrimitiveTypes.Int16),
	"list<int32>":               arrow.ListOf(arrow.PrimitiveTypes.Int32),
	"list<int64>":               arrow.ListOf(arrow.PrimitiveTypes.Int64),
	"list<uint8>":               arrow.ListOf(arrow.PrimitiveTypes.Uint8),
	"list<uint16>":              arrow.ListOf(arrow.PrimitiveTypes.Uint16),
	"list<uint32>":              arrow.ListOf(arrow.PrimitiveTypes.Uint32),
	"list<uint64>":              arrow.ListOf(arrow.PrimitiveTypes.Uint64),
	"list<float32>":             arrow.ListOf(arrow.PrimitiveTypes.Float32),
	"list<float64>":             arrow.ListOf(arrow.PrimitiveTypes.Float64),
	"list<utf8>":                arrow.ListOf(arrow.BinaryTypes.String),
	"list<binary>":              arrow.ListOf(arrow.BinaryTypes.Binary),
	"list<date32>":              arrow.ListOf(arrow.FixedWidthTypes.Date32),
	"list<time64[us]>":          arrow.ListOf(arrow.FixedWidthTypes.Time64us),
	"list<timestamp[s]>":        arrow.ListOf(&arrow.TimestampType{Unit: arrow.Second}),
	"list<timestamp[ms]>":       arrow.ListOf(&arrow.TimestampType{Unit: arrow.Millisecond}),
	"list<timestamp[us]>":       arrow.ListOf(&arrow.TimestampType{Unit: arrow.Microsecond}),
	"list<timestamp[ns]>":       arrow.ListOf(&arrow.TimestampType{Unit: arrow.Nanosecond}),
	"list<timestamp[us, tz=*]>": arrow.ListOf(&arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "Asia/Tokyo"}),
	"list<decimal(*, *)>":       arrow.ListOf(&arrow.Decimal128Type{Precision: 38, Scale: 0}),
	"list<list<int64>>":         arrow.ListOf(arrow.ListOf(arrow.PrimitiveTypes.Int64)),
	"list<struct>": arrow.ListOf(arrow.StructOf(
		arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Int32, Nullable: true})),
```

- [ ] **Step 5: Make the list appender generic**

`appendLatticeValue`'s `*array.ListBuilder` case appends three int64s, so it
builds nothing but `list<int64>`. Replace that case with one that fills the
element builder by delegating to the same switch:

```go
	case *array.ListBuilder:
		bldr.Append(true)
		// Three elements, filled through the same switch that fills a bare
		// column, so a list of T carries the same value as a T. A divergence
		// between the two is then a real difference in the sink, not in the
		// fixture.
		elem := bldr.ValueBuilder()
		for i := 0; i < 3; i++ {
			if err := appendLatticeValue(elem, key); err != nil {
				return err
			}
		}
```

`appendLatticeValue` switches on the builder's concrete type, so passing it a
nested `*array.ListBuilder` or `*array.StructBuilder` recurses correctly and
an unsupported element builder reports through the same `default` arm.

- [ ] **Step 6: Add the null-element builder**

Append to `internal/conformance/lattice.go`:

```go
// LatticeListWithNullElement builds a one-row list holding [value, null,
// value]. The caller releases it.
//
// This is the third null position, and the one no published table mentions.
// A null column and a null list are both visible to anyone who looks; a null
// *inside* a list is not, and ClickHouse's Array(T) has no way to hold one.
func LatticeListWithNullElement(key string) (arrow.Array, error) {
	dt, ok := latticeType[key]
	if !ok {
		return nil, fmt.Errorf("conformance: no lattice value for %q", key)
	}
	lt, ok := dt.(*arrow.ListType)
	if !ok {
		return nil, fmt.Errorf("conformance: %q is not a list", key)
	}

	b := array.NewBuilder(memory.NewGoAllocator(), lt).(*array.ListBuilder)
	defer b.Release()

	b.Append(true)
	elem := b.ValueBuilder()
	if err := appendLatticeValue(elem, key); err != nil {
		return nil, err
	}
	elem.AppendNull()
	if err := appendLatticeValue(elem, key); err != nil {
		return nil, err
	}
	return b.NewArray(), nil
}
```

- [ ] **Step 7: Test the null-element builder**

Append to `internal/conformance/lattice_test.go`:

```go
// The third null position. A sink that drops it, or silently substitutes a
// value for it, does so without an error, so only a direct test sees it.
func TestToolingConformanceLattice_BuildsAListWithANullElement(t *testing.T) {
	coverage.Covers(t, "tooling.conformance")

	arr, err := LatticeListWithNullElement("list<int64>")
	assert.NoError(t, err)
	defer arr.Release()

	assert.Equal(t, arr.Len(), 1)
	assert.Equal(t, arr.IsNull(0), false)

	values := arr.(*array.List).ListValues()
	assert.Equal(t, values.Len(), 3)
	assert.Equal(t, values.IsNull(0), false)
	assert.Equal(t, values.IsNull(1), true)
	assert.Equal(t, values.IsNull(2), false)
}

// A key that is not a list has no element to null, and asking for one is a
// mistake worth naming rather than a nil to dereference later.
func TestToolingConformanceLattice_RejectsANullElementOnANonList(t *testing.T) {
	coverage.Covers(t, "tooling.conformance")

	_, err := LatticeListWithNullElement("int64")
	assert.Error(t, err)
}
```

- [ ] **Step 8: Run the conformance and coverage tests**

Run: `CGO_ENABLED=1 go test ./internal/conformance/ ./internal/coverage/ -v 2>&1 | grep -E "^(---|FAIL|ok)"`
Expected: PASS. `TestToolingConformanceLattice_EveryDeclaredKeyHasAValue` holds the 50 YAML keys equal to the 50 builders, so a typo in either list fails here and names the key.

- [ ] **Step 9: Run the full unit pass**

Run: `CGO_ENABLED=1 go test -short ./...`
Expected: 16 packages ok. `internal/coverage` will still fail its
ClickHouse-declares-every-key test, because `integrations.yml` has no rows for
the 21 new keys yet. That failure is Task 2's starting point; if any *other*
package fails, stop and investigate.

- [ ] **Step 10: Commit**

```bash
git add docs/coverage/lattice.yml internal/conformance/lattice.go internal/conformance/lattice_test.go internal/coverage/registry_test.go
git commit -m "coverage: the lattice reaches depth 2, one key per element type

One list key per element type a sink might convert, which looks repetitive
and is the point. A sink converts a list element through a different path
than a bare column -- goElemType rather than arrowValue in ClickHouse -- and
the two are switches over the same type set that nothing holds equal. A
divergence is one element type that works alone and fails inside an Array.

Two rows hold an element the sink cannot convert, which is what proves a
container does not launder what it holds, and two more reach depth 3, where
the recursion stops paying for itself."
```

---

### Task 2: ClickHouse declares the container rows

**Files:**
- Modify: `docs/coverage/integrations.yml`
- Test: `internal/coverage/registry_test.go` (already written, currently failing)

**Interfaces:**
- Consumes: the 21 keys from Task 1.
- Produces: `types:` rows for all 50 keys and a `nulls: list_element` rule. Task 3 reads both through `coverage.TypesFor` and `coverage.NullsFor`.

- [ ] **Step 1: Confirm the failing test**

Run: `CGO_ENABLED=1 go test ./internal/coverage/ -run TestToolingCoverageRegistry_ClickhouseDeclaresEveryLatticeKey -v`
Expected: FAIL, once per undeclared key, naming each.

- [ ] **Step 2: Declare the supported container rows**

In `docs/coverage/integrations.yml`, under `sink.clickhouse`'s `types:`,
replace the existing `list<int64>` entry with these. ClickHouse's `Array(T)`
takes the same element type the bare column takes, so each row's `columns`
mirrors its element's row.

```yaml
      "list<bool>":
        outcome: exact
        columns: ["Array(Bool)"]
      "list<int8>":
        outcome: exact
        columns: ["Array(Int8)"]
      "list<int16>":
        outcome: exact
        columns: ["Array(Int16)"]
      "list<int32>":
        outcome: exact
        columns: ["Array(Int32)"]
      "list<int64>":
        outcome: exact
        columns: ["Array(Int64)"]
      "list<uint8>":
        outcome: exact
        columns: ["Array(UInt8)"]
      "list<uint16>":
        outcome: exact
        columns: ["Array(UInt16)"]
      "list<uint32>":
        outcome: exact
        columns: ["Array(UInt32)"]
      "list<uint64>":
        outcome: exact
        columns: ["Array(UInt64)"]
      "list<float32>":
        outcome: exact
        columns: ["Array(Float32)"]
      "list<float64>":
        outcome: exact
        columns: ["Array(Float64)"]
      "list<utf8>":
        outcome: exact
        columns: ["Array(String)"]
      "list<binary>":
        outcome: exact
        columns: ["Array(String)"]
      "list<date32>":
        outcome: exact
        columns: ["Array(Date32)"]
      "list<timestamp[s]>":
        outcome: exact
        columns: ["Array(DateTime)"]
      "list<timestamp[ms]>":
        outcome: exact
        columns: ["Array(DateTime64(3))"]
      "list<timestamp[us]>":
        outcome: exact
        columns: ["Array(DateTime64(6))"]
      "list<timestamp[ns]>":
        outcome: exact
        columns: ["Array(DateTime64(9))"]
      "list<timestamp[us, tz=*]>":
        outcome: coerced
        rule: stored as the same UTC instant; the zone is dropped and the column's own zone governs rendering
        columns: ["Array(DateTime64(6))"]

      # A list does not launder its element. arrowListValue recurses into
      # arrowValue for each one, so an element the sink cannot convert fails
      # the batch from inside the Array exactly as it does on its own.
      "list<decimal(*, *)>":
        outcome: unsupported
        code: user.sink.type_unsupported
        columns: []
      "list<time64[us]>":
        outcome: unsupported
        code: user.sink.type_unsupported
        columns: []
      "list<struct>":
        outcome: unsupported
        code: user.sink.type_unsupported
        columns: []

      # #150 added Array(Array(T)): goElemType recurses, so a nested list
      # becomes [][]T rather than a slice of something the driver rejects.
      "list<list<int64>>":
        outcome: exact
        columns: ["Array(Array(Int64))"]
```

- [ ] **Step 3: Declare what a null element does**

Extend the `nulls:` block on `sink.clickhouse`. The `list_element` rule is
predicted from reading `clickhouse.go:429-431`, which appends the element
type's zero value. Step 6 of Task 3 confirms or corrects it against a real
server; correct this file if they disagree, and never relax the assertion.

```yaml
    nulls:
      default:
        outcome: coerced
        rule: the column type's zero value; the DEFAULT expression is not evaluated because every produced column is named in the INSERT
      list_element:
        outcome: coerced
        rule: the element type's zero value, because ClickHouse's Array(T) holds no nulls unless T is Nullable
```

- [ ] **Step 4: Run the declaration test**

Run: `CGO_ENABLED=1 go test ./internal/coverage/ -run TestToolingCoverageRegistry_ClickhouseDeclaresEveryLatticeKey -v`
Expected: PASS.

- [ ] **Step 5: Run the generator's lattice check**

Run: `uv run pytest tests/tooling/test_coverage_matrix.py -q`
Expected: 164 passed. `test_the_real_registries_agree` holds the shipped
`lattice.yml` and `integrations.yml` consistent, so a missed key fails there.

- [ ] **Step 6: Run the full unit pass and commit**

Run: `CGO_ENABLED=1 go test -short ./...`
Expected: 16 packages ok.

```bash
git add docs/coverage/integrations.yml
git commit -m "coverage: ClickHouse declares what it does with a container

Array(T) takes the element type the bare column takes, so most rows mirror
their element's. Three do not: a decimal, a time64 and a struct inside a list
are declared unsupported, because arrowListValue recurses into arrowValue and
an element the sink cannot convert must fail the batch from inside the Array
exactly as it does alone.

The null-element rule is predicted from the code and not yet measured. If the
server disagrees, this file is what changes."
```

---

### Task 3: `type.nested`

**Files:**
- Modify: `internal/conformance/types.go`
- Modify: `internal/conformance/types_test.go`
- Modify: `internal/sinks/conformance_types_test.go`

**Interfaces:**
- Consumes: `LatticeListWithNullElement` from Task 1, the declaration from Task 2, `coverage.NullRule`.
- Produces: the `type.nested` verdict and `TypeSubject.ListElementNulls coverage.NullRule`. Task 4 leaves both untouched.

- [ ] **Step 1: Write the runner's self-tests**

Append to `internal/conformance/types_test.go`. `typeSink` accepts a fixed set
of canonical keys, so `newTypeSink("list<int64>")` accepts that list and
refuses everything else.

```go
// A container that takes an element the sink cannot convert has laundered it:
// the row lands, the column reads back, and one value in it is a fiction.
// This is what type.nested means by "never silently flattened".
func TestToolingConformanceTypes_AnUnsupportedElementInsideAListIsCaught(t *testing.T) {
	coverage.Covers(t, "tooling.conformance")

	declared := []coverage.TypeDecl{
		{Key: "list<struct>", Outcome: "unsupported", Code: "user.sink.type_unsupported"},
	}
	// The double takes the list despite its element, which is the defect.
	s := typeSubject(newTypeSink("list<struct>"), declared)
	s.ListElementNulls = coverage.NullRule{Outcome: "coerced", Rule: "zero value"}

	assertTypeFailure(t, typeVerdicts(t, s), typeNested, "list<struct>")
}

// A null inside a list has no error path. The sink either keeps it, drops it,
// or substitutes a value, and all three look identical from outside.
func TestToolingConformanceTypes_ANullElementAgainstTheWrongRuleIsCaught(t *testing.T) {
	coverage.Covers(t, "tooling.conformance")

	declared := []coverage.TypeDecl{
		{Key: "list<int64>", Outcome: "exact", Columns: []string{"Array(Int64)"}},
	}
	s := typeSubject(newTypeSink("list<int64>"), declared)
	// The double keeps whatever it is given, so a rule claiming coercion is
	// wrong about it.
	s.ListElementNulls = coverage.NullRule{Outcome: "coerced", Rule: "the element zero value"}

	assertTypeFailure(t, typeVerdicts(t, s), typeNested, "list<int64>")
}

// A subject with no container rows at all cannot prove the invariant, and
// must say so rather than pass vacuously.
func TestToolingConformanceTypes_ATableWithNoContainerRowsIsNotNestedProof(t *testing.T) {
	coverage.Covers(t, "tooling.conformance")

	declared := []coverage.TypeDecl{
		{Key: "int64", Outcome: "exact", Columns: []string{"Int64"}},
	}
	s := typeSubject(newTypeSink("int64"), declared)

	assertTypeFailure(t, typeVerdicts(t, s), typeNested, "no container")
}
```

- [ ] **Step 2: Run them to verify they fail**

Run: `CGO_ENABLED=1 go test ./internal/conformance/ -run TestToolingConformanceTypes_A 2>&1 | tail -6`
Expected: FAIL to compile, `undefined: typeNested` and
`s.ListElementNulls undefined`.

- [ ] **Step 3: Add the invariant id and the subject field**

In `internal/conformance/types.go`, extend the constant block:

```go
const (
	typeRoundtrip  = "type.roundtrip"
	typeNull       = "type.null"
	typeNested     = "type.nested"
	typeUndeclared = "type.undeclared.fails_loud"
)
```

And add to `TypeSubject`, below `Nulls`:

```go
	// ListElementNulls is what the integration does with a null held inside a
	// non-null list. It is a separate statement from Nulls because the answer
	// is different: ClickHouse's Array(T) is not nullable at all, so the
	// column-level rule says nothing about it.
	ListElementNulls coverage.NullRule
```

- [ ] **Step 4: Write the nested verdict**

Add to `internal/conformance/types.go`, and add `typeNestedVerdict(t, s)` to
the slice `typeVerdicts` returns, between `nulls` and `judgeUndeclaredType`:

```go
	return []verdict{roundtrip, nulls, judgeNested(t, s), judgeUndeclaredType(t, s)}
```

```go
// judgeNested proves that a container carries what it holds, and hides
// nothing.
//
// type.roundtrip already writes every declared row, containers included, so
// this adds the two claims that row cannot make. First, that a table declaring
// no container at all is not proof: an integration that never writes a list
// has not shown it handles one. Second, that a null inside a non-null list
// behaves as declared -- the position with no error path, where a sink can
// keep, drop or substitute and all three look the same from outside.
func judgeNested(t *testing.T, s TypeSubject) verdict {
	t.Helper()
	v := verdict{invariant: typeNested}

	var containers []coverage.TypeDecl
	for _, d := range s.Declared {
		if strings.HasPrefix(d.Key, "list<") || d.Key == "struct" ||
			d.Key == "map" || d.Key == "dictionary" ||
			strings.HasPrefix(d.Key, "fixed_size_list<") {
			containers = append(containers, d)
		}
	}
	if len(containers) == 0 {
		v.failure = "the type table declares no container row, so nothing here " +
			"proves what happens to a list, a struct or a map. An integration " +
			"that never writes one has not shown it handles one"
		return v
	}

	for _, d := range containers {
		if !strings.HasPrefix(d.Key, "list<") {
			continue
		}
		if f := judgeListElementNull(t, s, d); f != "" {
			v.failure = f
			return v
		}
	}
	return v
}

// judgeListElementNull writes [value, null, value] and holds the read-back
// against the declared rule.
func judgeListElementNull(t *testing.T, s TypeSubject, d coverage.TypeDecl) string {
	t.Helper()

	columns := d.Columns
	if d.Outcome == "unsupported" {
		columns = []string{""}
	}

	for _, columnType := range columns {
		arr, err := LatticeListWithNullElement(d.Key)
		if err != nil {
			return fmt.Sprintf("%s: %v", d.Key, err)
		}

		dest := s.Prepare(t, d.Key, columnType)
		tbl := oneColumnTable(arr)

		ctx := context.Background()
		writeErr := dest.Sink.WriteTable(ctx, tbl)
		if writeErr == nil {
			writeErr = dest.Sink.Flush(ctx)
		}
		tbl.Release()
		arr.Release()

		// An unsupported element must fail from inside the container exactly
		// as it does alone. A container that takes it has laundered it.
		if d.Outcome == "unsupported" {
			if writeErr == nil {
				return fmt.Sprintf("%s holds an element the table declares "+
					"unsupported, and the sink took the list anyway. The value "+
					"reached the destination as something else, which is the "+
					"silent flattening this invariant forbids", d.Key)
			}
			continue
		}

		if writeErr != nil {
			return fmt.Sprintf("%s into %s is declared %s and the sink refused a "+
				"list holding a null element: %v", d.Key, columnType, d.Outcome, writeErr)
		}

		got, err := dest.ReadBack(t)
		if err != nil {
			return fmt.Sprintf("%s into %s: read back: %v", d.Key, columnType, err)
		}
		if got == nil {
			return fmt.Sprintf("%s into %s: a list holding one null read back as "+
				"null in its entirety; one absent element must not erase the row",
				d.Key, columnType)
		}
		if f := judgeNull(s.ListElementNulls, d.Key, columnType, got); f != "" {
			return f
		}
	}
	return ""
}
```

`judgeNull`'s `coerced` arm asserts a non-nil read-back, which is what a
zero-value substitution produces. Its `exact` arm asserts nil, which a
destination preserving the null would produce only if it rendered the whole
list as null; ClickHouse renders `[1,0,3]`, so the coerced arm is the live
one. Add `"strings"` to the file's imports.

- [ ] **Step 5: Run the self-tests**

Run: `CGO_ENABLED=1 go test ./internal/conformance/ -run TestToolingConformanceTypes -v 2>&1 | grep -E "^(---|FAIL|ok)"`
Expected: PASS, including the three new tests and the ten from PR 1.

- [ ] **Step 6: Wire the ClickHouse subject and run it**

In `internal/sinks/conformance_types_test.go`, read the second rule and pass
it. Add below the existing `nulls` lookup:

```go
	elemNulls, err := coverage.NullElementsFor("sink.clickhouse")
	assert.NoError(t, err)
```

and add to the `conformance.TypeSubject` literal:

```go
		ListElementNulls: elemNulls,
```

Add the reader to `internal/coverage/registry.go`, beside `NullsFor`:

```go
// NullElementsFor returns an integration's rule for a null held inside a
// non-null list.
//
// Separate from NullsFor because the answers differ: a ClickHouse column can
// be Nullable and its Array(T) elements cannot, so the column-level rule says
// nothing about what a list holds.
func NullElementsFor(integration string) (NullRule, error) {
	raw, err := readRegistry("integrations.yml")
	if err != nil {
		return NullRule{}, err
	}

	var doc struct {
		Integrations []struct {
			ID    string              `yaml:"id"`
			Nulls map[string]NullRule `yaml:"nulls"`
		} `yaml:"integrations"`
	}
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		return NullRule{}, fmt.Errorf("coverage: parse integrations.yml: %w", err)
	}

	for _, entry := range doc.Integrations {
		if entry.ID != integration {
			continue
		}
		return entry.Nulls["list_element"], nil
	}
	return NullRule{}, fmt.Errorf("coverage: integrations.yml declares no %q", integration)
}
```

Run: `CGO_ENABLED=1 go test ./internal/sinks/ -run TestIntegrationSinkClickhouse_Types -v 2>&1 | grep -E "^(=== RUN|    types.go|---|    ---|FAIL|ok)"`
Expected: four subtests, `type.nested` among them.

**A failing row is a finding, not a test bug.** For each, decide which side is
wrong and correct `integrations.yml` to what the sink does, or fix the runner.
Never relax an assertion. The two rows most likely to need correcting are the
`list_element` rule and `list<binary>`, whose ClickHouse column type is
`Array(String)` rather than the `Array(Binary)` a reader might expect.

- [ ] **Step 7: Run the full unit pass and commit**

Run: `CGO_ENABLED=1 go test -short ./...`
Expected: 16 packages ok.

```bash
git add internal/conformance/types.go internal/conformance/types_test.go internal/sinks/conformance_types_test.go internal/coverage/registry.go
git commit -m "conformance: a container carries what it holds, and hides nothing

type.roundtrip already writes every declared row, containers included, so
type.nested adds the two claims that row cannot make.

A table declaring no container is not proof: an integration that never writes
a list has not shown it handles one, and a vacuous pass is worse than a
missing cell.

A null inside a non-null list is the position with no error path. A sink can
keep it, drop it or substitute for it, and all three look the same from
outside. ClickHouse appends the element type's zero value, so [1, null, 3]
lands as [1, 0, 3] -- silent, and absent from the published table until now."
```

---

### Task 4: `type.string.fidelity`

`#149` shipped a sink that stored `a\nb` as the four characters `a`, `\`, `n`,
`b`. One canonical value per type cannot catch that class: it needs a string
chosen to break things.

**Files:**
- Modify: `internal/conformance/types.go`
- Modify: `internal/conformance/types_test.go`

**Interfaces:**
- Consumes: `TypeSubject.Declared`, `oneColumnTable` from PR 1.
- Produces: the `type.string.fidelity` verdict. Nothing later depends on it.

- [ ] **Step 1: Write the self-tests**

Append to `internal/conformance/types_test.go`:

```go
// #149's defect: jsonparser returns the bytes between a string's quotes with
// escapes undecoded, and the sink stored them verbatim. Every affected value
// round-tripped through a test that used one tidy string.
func TestToolingConformanceTypes_AMangledStringIsCaught(t *testing.T) {
	coverage.Covers(t, "tooling.conformance")

	declared := []coverage.TypeDecl{
		{Key: "utf8", Outcome: "exact", Columns: []string{"String"}},
	}
	s := typeSubject(newTypeSink("utf8"), declared)
	s.Prepare = func(t *testing.T, key, columnType string) TypeDestination {
		sink := newTypeSink("utf8")
		return TypeDestination{
			Sink: core.Sink(sink),
			// A destination that strips backslashes, which is the shape of
			// the defect: no error, and the row is simply wrong.
			ReadBack: func(t *testing.T) (any, error) {
				v := sink.lastString()
				return strings.ReplaceAll(v, "\\", ""), nil
			},
		}
	}

	assertTypeFailure(t, typeVerdicts(t, s), typeFidelity, "back")
}

// The empty string is not null. A destination that conflates them loses the
// difference between "sent nothing" and "sent no characters".
func TestToolingConformanceTypes_AnEmptyStringReadBackAsNullIsCaught(t *testing.T) {
	coverage.Covers(t, "tooling.conformance")

	declared := []coverage.TypeDecl{
		{Key: "utf8", Outcome: "exact", Columns: []string{"String"}},
	}
	s := typeSubject(newTypeSink("utf8"), declared)
	s.Prepare = func(t *testing.T, key, columnType string) TypeDestination {
		sink := newTypeSink("utf8")
		return TypeDestination{
			Sink: core.Sink(sink),
			ReadBack: func(t *testing.T) (any, error) {
				if sink.lastString() == "" {
					return nil, nil
				}
				return sink.lastString(), nil
			},
		}
	}

	assertTypeFailure(t, typeVerdicts(t, s), typeFidelity, "empty")
}

// An integration with no string row cannot prove the claim and must say so.
func TestToolingConformanceTypes_ATableWithNoStringRowIsNotFidelityProof(t *testing.T) {
	coverage.Covers(t, "tooling.conformance")

	declared := []coverage.TypeDecl{
		{Key: "int64", Outcome: "exact", Columns: []string{"Int64"}},
	}
	assertTypeFailure(t, typeVerdicts(t, typeSubject(newTypeSink("int64"), declared)),
		typeFidelity, "no utf8 row")
}
```

Add `lastString` to `typeSink` in the same file:

```go
// lastString returns the single string value of the last delivered table, or
// "" when there is none.
func (s *typeSink) lastString() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.delivered == nil || s.delivered.NumCols() == 0 {
		return ""
	}
	chunk := s.delivered.Column(0).Data().Chunk(0)
	str, ok := chunk.(*array.String)
	if !ok || str.IsNull(0) {
		return ""
	}
	return str.Value(0)
}
```

Add `"github.com/apache/arrow-go/v18/arrow/array"` to the test file's imports.

- [ ] **Step 2: Run them to verify they fail**

Run: `CGO_ENABLED=1 go test ./internal/conformance/ -run TestToolingConformanceTypes 2>&1 | tail -5`
Expected: FAIL to compile, `undefined: typeFidelity`.

- [ ] **Step 3: Write the corpus and the verdict**

In `internal/conformance/types.go`, add `typeFidelity` to the constant block:

```go
	typeFidelity   = "type.string.fidelity"
```

Add `judgeStringFidelity(t, s)` to the slice `typeVerdicts` returns:

```go
	return []verdict{roundtrip, nulls, judgeNested(t, s),
		judgeStringFidelity(t, s), judgeUndeclaredType(t, s)}
```

```go
// fidelityCorpus is the set of strings a sink must carry unchanged.
//
// Each entry is here because something broke on it. The escapes are #149,
// where jsonparser returned the bytes between the quotes undecoded and the
// sink stored them verbatim, so "a\nb" landed as four characters. The empty
// string is here because a destination that conflates it with NULL loses the
// difference between sending nothing and sending no characters. The rest
// cover the byte ranges a naive length or encoding assumption truncates.
var fidelityCorpus = []struct {
	name  string
	value string
}{
	{"newline", "a\nb"},
	{"tab", "a\tb"},
	{"carriage return", "a\rb"},
	{"double quote", `say "hi"`},
	{"single quote", "it's"},
	{"backslash", `back\slash`},
	{"backtick and dollar", "`cmd` $var"},
	{"unicode", "L'Œil café 東京"},
	{"astral plane", "👁🌍"},
	{"combining marks", "éà"},
	{"empty", ""},
	{"leading and trailing space", "  padded  "},
	{"null-looking text", "NULL"},
	{"sql fragment", "'); DROP TABLE t; --"},
}

// judgeStringFidelity writes each corpus entry through the utf8 row and reads
// it back.
//
// One canonical value per type cannot catch this class of defect: #149
// corrupted every string carrying a backslash, and the sink's tests passed
// throughout because they used tidy strings. The corpus is the test.
func judgeStringFidelity(t *testing.T, s TypeSubject) verdict {
	t.Helper()
	v := verdict{invariant: typeFidelity}

	var utf8Row *coverage.TypeDecl
	for i := range s.Declared {
		if s.Declared[i].Key == "utf8" {
			utf8Row = &s.Declared[i]
			break
		}
	}
	if utf8Row == nil {
		v.failure = "the type table has no utf8 row, so nothing here proves a " +
			"string survives the sink"
		return v
	}
	if utf8Row.Outcome == "unsupported" {
		return v
	}

	for _, columnType := range utf8Row.Columns {
		for _, entry := range fidelityCorpus {
			if f := judgeOneString(t, s, columnType, entry.name, entry.value); f != "" {
				v.failure = f
				return v
			}
		}
	}
	return v
}

func judgeOneString(t *testing.T, s TypeSubject, columnType, name, value string) string {
	t.Helper()

	b := array.NewStringBuilder(memory.NewGoAllocator())
	defer b.Release()
	b.Append(value)

	arr := b.NewArray()
	defer arr.Release()

	dest := s.Prepare(t, "utf8", columnType)
	tbl := oneColumnTable(arr)
	defer tbl.Release()

	ctx := context.Background()
	err := dest.Sink.WriteTable(ctx, tbl)
	if err == nil {
		err = dest.Sink.Flush(ctx)
	}
	if err != nil {
		return fmt.Sprintf("the %s string into %s: the sink refused it: %v",
			name, columnType, err)
	}

	got, err := dest.ReadBack(t)
	if err != nil {
		return fmt.Sprintf("the %s string into %s: read back: %v", name, columnType, err)
	}
	// The empty string is a value, not an absence.
	if got == nil {
		return fmt.Sprintf("the %s string into %s read back as null. An empty "+
			"string is a value: conflating it with NULL loses the difference "+
			"between sending nothing and sending no characters", name, columnType)
	}
	if s, ok := got.(string); ok && s != value {
		return fmt.Sprintf("the %s string into %s read back as %q, and %q went in. "+
			"A string must survive byte for byte", name, columnType, s, value)
	}
	return ""
}
```

Add `"github.com/apache/arrow-go/v18/arrow/memory"` to the imports if it is
not already there; `array` and `fmt` already are.

- [ ] **Step 4: Run the self-tests**

Run: `CGO_ENABLED=1 go test ./internal/conformance/ -run TestToolingConformanceTypes -v 2>&1 | grep -E "^(---|FAIL|ok)"`
Expected: PASS, sixteen tests.

- [ ] **Step 5: Run the ClickHouse subject**

Run: `CGO_ENABLED=1 go test ./internal/sinks/ -run TestIntegrationSinkClickhouse_Types -v 2>&1 | grep -E "^(=== RUN|    types.go|---|    ---|FAIL|ok)"`
Expected: five subtests, `type.string.fidelity` among them.

A corpus entry the server does not return verbatim is a finding. Record what
it does in the PR description, and change the entry only if the difference is
the *test's* fault — for instance if `toString` renders something the column
never stored.

- [ ] **Step 6: Run everything and regenerate the matrix**

Run: `CGO_ENABLED=1 go test -short ./...` — expected 16 packages ok.
Run: `uv run pytest tests/tooling -q` — expected 164 passed.

Then check `docker ps`. **If a `clickhouse` or `kafka1` container is running,
do not generate the matrix locally**: push first and take CI's
`coverage-matrix` artifact, per the Global Constraints. Otherwise:

Run: `make coverage-matrix`
Expected: `type.nested` and `type.string.fidelity` read `✅ i` for
`sink.clickhouse`, taking the cell count from 29 proven to 31.

- [ ] **Step 7: Commit**

```bash
git add internal/conformance/types.go internal/conformance/types_test.go docs/coverage/matrix.json docs/coverage/matrix.md
git commit -m "conformance: a string survives the sink byte for byte

#149 stored a\\nb as four characters and every test passed, because they all
used tidy strings. One canonical value per type cannot catch that class.

Fourteen strings, each here because something broke on it: the escapes are
#149, the empty string is here because a destination that conflates it with
NULL loses the difference between sending nothing and sending no characters,
and the rest cover byte ranges a naive length or encoding assumption
truncates."
```

---

## Self-Review

**Spec coverage.** The spec's PR 2 is "Depth 2 and the two depth-3 witnesses,
including the unsupported-element rows. `type.nested` gets its cell. The
null-inside-a-list coercion is declared here, as measured." Task 1 adds depth 2
and both witnesses, Task 2 declares them including the three unsupported-element
rows, Task 3 delivers the cell and measures the null-element coercion.
`type.string.fidelity` is Task 4, carried over from the PR 1 plan's self-review,
which deferred it here alongside the empty string.

The spec's tier 2 says "`list<T>` for every `T` that is exact at depth 1".
ClickHouse has eighteen such T, and `timestamp[us, tz=*]` is coerced rather
than exact — it is included anyway, because a coercion inside a container is
where a rule and its container can disagree.

**Placeholders.** None. Task 2 Step 3 and Task 3 Step 6 state a prediction and
name the file to change if the server disagrees, which is a decision procedure
rather than a gap.

**Type consistency.** `LatticeListWithNullElement` (Task 1) is called only by
`judgeListElementNull` (Task 3). `TypeSubject.ListElementNulls` (Task 3 Step 3)
is set from `coverage.NullElementsFor` (Task 3 Step 6) and read by
`judgeListElementNull`. `typeNested` and `typeFidelity` join the same constant
block and both reach `typeVerdicts`' returned slice. `judgeNull` is reused
unchanged from PR 1 by `judgeListElementNull`.

**Not in this plan.** `type.timestamp.instant` and the rendered mdx fragment
are PR 3. `requires:` stays `[]`, so nothing here begins enforcing.
