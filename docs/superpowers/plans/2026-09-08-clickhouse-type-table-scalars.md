# ClickHouse Type Table, PR 1: Scalars Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prove what the ClickHouse sink does with every Arrow type DuckDB can emit at depth 1, from a declaration a human wrote and a test judges.

**Architecture:** `docs/coverage/lattice.yml` declares the closed set of 29 Arrow keys. `integrations.yml` gains a `types:` block per integration naming an outcome per key. `internal/conformance` owns the canonical key function and the value table, and a new `conformance.Types` runner writes one row per declared type through the real sink and reads it back. The generator gains a lattice check. Evidence attaches by the existing `COVERS invariant=... integration=...` marker, so no new plumbing reaches `matrix.md`.

**Tech Stack:** Go 1.x with CGO, arrow-go v18, ADBC DuckDB driver, clickhouse-go, testcontainers-go, Python 3 for the generator, pytest for its tests.

**Spec:** `docs/superpowers/specs/2026-09-07-clickhouse-type-table-design.md`

## Global Constraints

- All prose — commit messages, comments, docs — follows Google Technical Writing One, per `CLAUDE.md`. Active voice. One idea per sentence. Explain why, not what.
- No attribution lines in commit messages.
- The unit pass is `CGO_ENABLED=1 go test -short ./...`. It must stay green after every task.
- An integration test is named `TestIntegration<Feature>_<Behaviour>`. It starts what it needs in a container and **fails if it cannot**. It must never skip.
- Arrow keys are **canonical**, produced by `conformance.CanonicalKey`. Never the raw `DataType.String()`: DuckDB's ADBC output names a list child `l` and `arrow.ListOf` names it `item`.
- The ClickHouse image is pinned to `clickhouse/clickhouse-server:26.8`, already declared at `internal/sinks/conformance_test.go:34`.
- `requires:` on all six type invariants stays `[]` in this PR. The matrix reports; nothing fails.
- Markers are emitted only by a passing subtest, through `coverage.Invariant(t, invariant, integration)`.

---

### Task 1: A coded error for an unsupported Arrow type

`type.undeclared.fails_loud` requires a coded error. `arrowValue` returns a bare `fmt.Errorf` today, so the invariant cannot pass on any sink. No sink-side code exists: `CodeSQLTypeUnsupported` is handler-side, and the `sink.clickhouse.type_unsupported` an earlier spec used was never real.

**Files:**
- Modify: `internal/errs/registry.go` (constant block near `CodeSinkInvalid`, and the `registry` map)
- Modify: `internal/sinks/clickhouse.go:404`, `internal/sinks/clickhouse.go:432`
- Test: `internal/sinks/clickhouse_test.go`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces: `errs.CodeSinkTypeUnsupported Code = "user.sink.type_unsupported"`. Tasks 5 and 6 declare this string in `integrations.yml` and assert it.

- [ ] **Step 1: Write the failing test**

Append to `internal/sinks/clickhouse_test.go`. The file is `package sinks`, so `arrowValue` is reachable.

```go
// A type the sink cannot convert must fail with a code. An uncoded error
// reaches the operator as system.internal.unexpected, which blames sqlflow
// for a column the user chose.
func TestSinkClickhouse_UnsupportedArrowTypeCarriesACode(t *testing.T) {
	b := array.NewTime64Builder(memory.NewGoAllocator(), &arrow.Time64Type{Unit: arrow.Microsecond})
	defer b.Release()
	b.Append(arrow.Time64(12 * 3600 * 1e6))

	arr := b.NewArray()
	defer arr.Release()

	_, err := arrowValue(arr, 0)
	assert.Error(t, err)
	if !errs.HasCode(err, errs.CodeSinkTypeUnsupported) {
		t.Fatalf("code = %s, want %s", errs.CodeOf(err), errs.CodeSinkTypeUnsupported)
	}
}
```

Ensure these imports are present in the file: `github.com/apache/arrow-go/v18/arrow`, `github.com/apache/arrow-go/v18/arrow/array`, `github.com/apache/arrow-go/v18/arrow/memory`, `github.com/turbolytics/sql-flow/internal/errs`, `github.com/zeebo/assert`.

- [ ] **Step 2: Run test to verify it fails**

Run: `CGO_ENABLED=1 go test ./internal/sinks/ -run TestSinkClickhouse_UnsupportedArrowTypeCarriesACode`
Expected: FAIL to compile with `undefined: errs.CodeSinkTypeUnsupported`.

- [ ] **Step 3: Add the code and its definition**

In `internal/errs/registry.go`, add to the constant block beside `CodeSinkInvalid`:

```go
	CodeSinkInvalid           Code = "user.sink.invalid"
	CodeSinkTypeUnsupported   Code = "user.sink.type_unsupported"
```

And to the `registry` map, beside the `CodeSinkInvalid` entry:

```go
	CodeSinkTypeUnsupported: {
		CodeSinkTypeUnsupported,
		"A result column has an Arrow type the sink cannot convert.",
		"Cast the column in the handler SQL to a type the sink's type table declares.",
	},
```

- [ ] **Step 4: Wrap the two conversion errors**

In `internal/sinks/clickhouse.go`, replace `arrowValue`'s default arm:

```go
	default:
		return nil, errs.New(errs.CodeSinkTypeUnsupported,
			"unsupported arrow type %s", arr.DataType())
	}
```

And `arrowListValue`'s assignability guard. A list element the element switch renders as the wrong Go type is the same defect seen through a container, so it carries the same code:

```go
		if !rv.Type().AssignableTo(out.Type().Elem()) {
			return nil, errs.New(errs.CodeSinkTypeUnsupported,
				"list element %s is not assignable to %s", rv.Type(), out.Type().Elem())
		}
```

`appendTables` at `clickhouse.go:274` already wraps with `%w`, and `errs.CodeOf` unwraps through `errors.As`, so the code survives to the caller unchanged.

- [ ] **Step 5: Run the test and the full unit pass**

Run: `CGO_ENABLED=1 go test ./internal/sinks/ -run TestSinkClickhouse_UnsupportedArrowTypeCarriesACode`
Expected: PASS

Run: `CGO_ENABLED=1 go test -short ./...`
Expected: all packages ok. `internal/errs` holds every code to a definition, so a missing `registry` entry fails there.

- [ ] **Step 6: Commit**

```bash
git add internal/errs/registry.go internal/sinks/clickhouse.go internal/sinks/clickhouse_test.go
git commit -m "fix(sinks): an unsupported Arrow type fails with a code, not a bare error

arrowValue returned fmt.Errorf, so a column the sink cannot convert reached
the operator as system.internal.unexpected -- sqlflow's fault, for a type the
user chose. type.undeclared.fails_loud requires a coded error and could not
pass on any sink.

Adds user.sink.type_unsupported. If this is wrong, an operator reads an
internal-error exit code for a config mistake and files a bug instead of
casting the column."
```

---

### Task 2: The canonical key

The key is a function of an Arrow type, and it must give the same answer whether DuckDB built the type or `arrow.ListOf` did.

**Files:**
- Create: `internal/conformance/lattice.go`
- Create: `internal/conformance/lattice_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces: `conformance.CanonicalKey(dt arrow.DataType) string`. Tasks 3, 5 and 6 key every declaration and lookup on its output.

- [ ] **Step 1: Write the failing test**

Create `internal/conformance/lattice_test.go`:

```go
package conformance

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

// DuckDB's ADBC output names a list's child "l"; arrow.ListOf names it
// "item". Both are the same logical type, and a key that told them apart
// would match the constructor and miss the engine.
func TestToolingCoverageCanonicalKey_IgnoresChildNamesAndNullability(t *testing.T) {
	duckdbShape := arrow.ListOfField(
		arrow.Field{Name: "l", Type: arrow.PrimitiveTypes.Int32, Nullable: true})
	goShape := arrow.ListOf(arrow.PrimitiveTypes.Int32)

	assert.NotEqual(t, duckdbShape.String(), goShape.String())
	assert.Equal(t, CanonicalKey(duckdbShape), CanonicalKey(goShape))
	assert.Equal(t, CanonicalKey(goShape), "list<int32>")
}

// The zone is the session's, not the type's. Pinning it into the key would
// make the declaration depend on the host that ran the test.
func TestToolingCoverageCanonicalKey_WildcardsTheTimestampZone(t *testing.T) {
	coverage.Covers(t, "tooling.coverage")

	zoned := &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "Asia/Tokyo"}
	bare := &arrow.TimestampType{Unit: arrow.Microsecond}

	assert.Equal(t, CanonicalKey(zoned), "timestamp[us, tz=*]")
	assert.Equal(t, CanonicalKey(bare), "timestamp[us]")
}

// Precision and scale are the user's choice, so one key covers the family.
func TestToolingCoverageCanonicalKey_CollapsesTheDecimalFamily(t *testing.T) {
	p38 := &arrow.Decimal128Type{Precision: 38, Scale: 0}
	p18 := &arrow.Decimal128Type{Precision: 18, Scale: 3}

	assert.Equal(t, CanonicalKey(p38), "decimal(*, *)")
	assert.Equal(t, CanonicalKey(p18), "decimal(*, *)")
}

func TestToolingCoverageCanonicalKey_NamesEachConstructor(t *testing.T) {
	cases := map[string]arrow.DataType{
		"struct":       arrow.StructOf(arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Int32, Nullable: true}),
		"map":          arrow.MapOf(arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int32),
		"dictionary":   &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Uint8, ValueType: arrow.BinaryTypes.String},
		"int64":        arrow.PrimitiveTypes.Int64,
		"utf8":         arrow.BinaryTypes.String,
		"date32":       arrow.FixedWidthTypes.Date32,
		"list<list<int64>>": arrow.ListOf(arrow.ListOf(arrow.PrimitiveTypes.Int64)),
		"fixed_size_list<int32>[3]": arrow.FixedSizeListOf(3, arrow.PrimitiveTypes.Int32),
	}
	for want, dt := range cases {
		if got := CanonicalKey(dt); got != want {
			t.Errorf("CanonicalKey(%s) = %q, want %q", dt, got, want)
		}
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `CGO_ENABLED=1 go test ./internal/conformance/ -run TestToolingCoverageCanonicalKey`
Expected: FAIL to compile with `undefined: CanonicalKey`.

- [ ] **Step 3: Write the implementation**

Create `internal/conformance/lattice.go`:

```go
package conformance

// The canonical form of an Arrow type, and the value table the type runner
// writes.
//
// The key is canonical rather than raw because DataType.String() is not one
// name per type. DuckDB's ADBC output calls a list's child "l" and arrow.ListOf
// calls it "item", so the same logical type has two spellings, decided by
// whoever built it. A registry keyed on the raw string would match the
// constructor and silently miss the engine.

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
)

// CanonicalKey is the docs/coverage/lattice.yml key for an Arrow type.
//
// It drops what the declaration must not depend on: a child field's name, its
// nullability, a decimal's precision and scale, and a timestamp's zone. It
// keeps what changes the sink's behaviour: the width, the time unit, and the
// element type of a container.
func CanonicalKey(dt arrow.DataType) string {
	switch t := dt.(type) {
	case *arrow.ListType:
		return "list<" + CanonicalKey(t.Elem()) + ">"
	case *arrow.LargeListType:
		return "large_list<" + CanonicalKey(t.Elem()) + ">"
	case *arrow.FixedSizeListType:
		return fmt.Sprintf("fixed_size_list<%s>[%d]", CanonicalKey(t.Elem()), t.Len())
	case *arrow.StructType:
		return "struct"
	case *arrow.MapType:
		return "map"
	case *arrow.SparseUnionType:
		return "sparse_union"
	case *arrow.DenseUnionType:
		return "dense_union"
	case *arrow.DictionaryType:
		return "dictionary"
	case *arrow.Decimal128Type, *arrow.Decimal256Type:
		// Precision and scale are the user's choice and do not change which
		// branch of the sink's conversion runs.
		return "decimal(*, *)"
	case *arrow.TimestampType:
		// The zone comes from the DuckDB session, so it is a property of the
		// host that ran the query rather than of the type.
		if t.TimeZone != "" {
			return "timestamp[" + t.Unit.String() + ", tz=*]"
		}
		return "timestamp[" + t.Unit.String() + "]"
	default:
		return dt.String()
	}
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `CGO_ENABLED=1 go test ./internal/conformance/ -run TestToolingCoverageCanonicalKey -v`
Expected: PASS, four tests.

If a constructor case prints something other than the plan's expected string, the test names both. Correct the implementation, not the test: the expected strings are the keys `lattice.yml` uses in Task 3.

- [ ] **Step 5: Commit**

```bash
git add internal/conformance/lattice.go internal/conformance/lattice_test.go
git commit -m "conformance: an Arrow type has one canonical key, whoever built it

DataType.String() is not one name per type. DuckDB through ADBC prints
list<l: int32, nullable>; arrow.ListOf prints list<item: int32, nullable>.
A type table keyed on the raw string matches the constructor and misses the
engine, which is the only one whose types reach a sink.

CanonicalKey drops child names, nullability, decimal precision and the
session timezone. If it drops too much, two types the sink treats
differently share a cell and one of them is never proven."
```

---

### Task 3: `lattice.yml` and its reader

**Files:**
- Create: `docs/coverage/lattice.yml`
- Modify: `internal/coverage/registry.go`
- Modify: `internal/coverage/registry_test.go`

**Interfaces:**
- Consumes: `conformance.CanonicalKey` from Task 2, for the key spellings.
- Produces: `coverage.LatticeEntry{Key string; DuckDB []string; Depth int}` and `coverage.Lattice() ([]LatticeEntry, error)`. Tasks 4, 5 and 6 read it.

- [ ] **Step 1: Create the lattice file**

Create `docs/coverage/lattice.yml`:

```yaml
# Every Arrow type DuckDB can hand a sink, and nothing else.
#
# Measured on 2026-09-08 through internal/duckdb.Open -- the engine's own ADBC
# connection -- by casting NULL to each SQL type and reading the Arrow schema
# back. pyarrow is a different binding that prints different names, and an
# earlier revision keyed on its spellings: `string` for `utf8`, `double` for
# `float64`, `decimal128(38, 0)` for `decimal(38, 0)`.
#
# The set is closed. A type key an integration declares that is not here is a
# typo, and a typo must not reach the matrix as a missing cell. A key here that
# an integration does not declare is a gap, and reporting that gap is the whole
# reason this file exists: nothing written down means nothing can be missing.
#
# `key` is conformance.CanonicalKey's output, not DataType.String(). See
# internal/conformance/lattice.go for why they differ.
#
# `duckdb` lists the SQL types a user casts to in handler SQL. It is what the
# rendered integration page is keyed by, so a type reachable by no cast has an
# empty list rather than a missing field.
#
# `depth` is 1 for a scalar and 2 for a container of scalars. The type runner
# covers depth 1 in PR 1; depth 2 and the two depth-3 witnesses land in PR 2.

lattice:
  # --- Booleans and integers ------------------------------------------------
  - key: bool
    duckdb: [BOOLEAN]
    depth: 1

  - key: int8
    duckdb: [TINYINT]
    depth: 1

  - key: int16
    duckdb: [SMALLINT]
    depth: 1

  - key: int32
    duckdb: [INTEGER]
    depth: 1

  - key: int64
    duckdb: [BIGINT]
    depth: 1

  - key: uint8
    duckdb: [UTINYINT]
    depth: 1

  - key: uint16
    duckdb: [USMALLINT]
    depth: 1

  - key: uint32
    duckdb: [UINTEGER]
    depth: 1

  - key: uint64
    duckdb: [UBIGINT]
    depth: 1

  # --- Floating point and decimals -----------------------------------------
  - key: float32
    duckdb: [REAL]
    depth: 1

  - key: float64
    duckdb: [DOUBLE]
    depth: 1

  # HUGEINT is here, not with the integers. DuckDB promotes integer overflow to
  # HUGEINT, so a user reaches this key without asking for it.
  - key: "decimal(*, *)"
    duckdb: ["DECIMAL(p, s)", HUGEINT, UHUGEINT]
    depth: 1

  # --- Bytes ----------------------------------------------------------------
  - key: utf8
    duckdb: [VARCHAR, UUID, JSON]
    depth: 1

  - key: binary
    duckdb: [BLOB, BIT, VARINT]
    depth: 1

  # --- Dates and times ------------------------------------------------------
  - key: date32
    duckdb: [DATE]
    depth: 1

  # TIMETZ is here and not under a zoned key: DuckDB drops the zone on the way
  # to Arrow, which is a coercion the type table must state.
  - key: "time64[us]"
    duckdb: [TIME, TIMETZ]
    depth: 1

  - key: "time64[ns]"
    duckdb: [TIME_NS]
    depth: 1

  - key: "timestamp[s]"
    duckdb: [TIMESTAMP_S]
    depth: 1

  - key: "timestamp[ms]"
    duckdb: [TIMESTAMP_MS]
    depth: 1

  - key: "timestamp[us]"
    duckdb: [TIMESTAMP]
    depth: 1

  - key: "timestamp[ns]"
    duckdb: [TIMESTAMP_NS]
    depth: 1

  - key: "timestamp[us, tz=*]"
    duckdb: [TIMESTAMPTZ]
    depth: 1

  - key: month_day_nano_interval
    duckdb: [INTERVAL]
    depth: 1

  # --- Constructors ---------------------------------------------------------
  # A constructor is one key however deep it nests. An unsupported constructor
  # fails on its outermost type, so struct-of-struct proves nothing a struct
  # does not.
  - key: "list<int64>"
    duckdb: ["BIGINT[]"]
    depth: 2

  - key: "fixed_size_list<int32>[3]"
    duckdb: ["INTEGER[3]"]
    depth: 2

  - key: struct
    duckdb: ["STRUCT(...)"]
    depth: 2

  - key: map
    duckdb: ["MAP(K, V)"]
    depth: 2

  - key: sparse_union
    duckdb: ["UNION(...)"]
    depth: 2

  - key: dictionary
    duckdb: [ENUM]
    depth: 2
```

- [ ] **Step 2: Write the failing test**

Append to `internal/coverage/registry_test.go`:

```go
// The lattice is the closed set every integration's type table is checked
// against. A duplicate key would silently drop a row, and a key with no
// DuckDB cast would render as a blank cell on the integration page.
func TestToolingCoverage_LatticeIsClosedAndWellFormed(t *testing.T) {
	Covers(t, "tooling.coverage")

	entries, err := Lattice()
	assert.NoError(t, err)
	assert.Equal(t, len(entries), 29)

	seen := map[string]bool{}
	for _, e := range entries {
		if seen[e.Key] {
			t.Errorf("lattice.yml: duplicate key %q", e.Key)
		}
		seen[e.Key] = true

		if len(e.DuckDB) == 0 {
			t.Errorf("lattice.yml: %q names no DuckDB SQL type", e.Key)
		}
		if e.Depth != 1 && e.Depth != 2 {
			t.Errorf("lattice.yml: %q has depth %d, want 1 or 2", e.Key, e.Depth)
		}
	}
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `CGO_ENABLED=1 go test ./internal/coverage/ -run TestToolingCoverage_LatticeIsClosedAndWellFormed`
Expected: FAIL to compile with `undefined: Lattice`.

- [ ] **Step 4: Write the reader**

Append to `internal/coverage/registry.go`:

```go
// LatticeEntry is one Arrow type sinks must account for.
type LatticeEntry struct {
	// Key is conformance.CanonicalKey's output for the type.
	Key string `yaml:"key"`

	// DuckDB names the SQL types a user casts to in handler SQL. The rendered
	// integration page is keyed by these, not by the Arrow key.
	DuckDB []string `yaml:"duckdb"`

	// Depth is 1 for a scalar, 2 for a container.
	Depth int `yaml:"depth"`
}

// Lattice returns every Arrow type declared in lattice.yml, in file order.
//
// The set is closed, which is what makes a gap visible: a type an integration
// does not declare is reported rather than absent. An open set would let a
// type nobody thought of pass unnoticed, which is the sink.iceberg failure --
// nothing written down, so nothing could be missing.
func Lattice() ([]LatticeEntry, error) {
	raw, err := readRegistry("lattice.yml")
	if err != nil {
		return nil, err
	}

	var doc struct {
		Lattice []LatticeEntry `yaml:"lattice"`
	}
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		return nil, fmt.Errorf("coverage: parse lattice.yml: %w", err)
	}
	return doc.Lattice, nil
}
```

- [ ] **Step 5: Run the test and the full unit pass**

Run: `CGO_ENABLED=1 go test ./internal/coverage/ -run TestToolingCoverage_LatticeIsClosedAndWellFormed -v`
Expected: PASS

Run: `CGO_ENABLED=1 go test -short ./...`
Expected: all packages ok.

- [ ] **Step 6: Commit**

```bash
git add docs/coverage/lattice.yml internal/coverage/registry.go internal/coverage/registry_test.go
git commit -m "coverage: declare the closed set of Arrow types a sink must account for

Measured through internal/duckdb.Open, the engine's own ADBC connection, not
through pyarrow: the two bindings print different names for the same type,
and an earlier draft keyed on pyarrow's.

The set is closed so a gap is visible. Without it a type nobody thought of
passes unnoticed, which is how Iceberg shipped for months with a test that
skipped and printed ok."
```

---

### Task 4: The value table, held equal to the lattice

The runner needs one Arrow array per key. Values live in Go, beside the builders. A test holds the two key sets equal, so a lattice entry with no value fails in seconds.

**Files:**
- Modify: `internal/conformance/lattice.go`
- Modify: `internal/conformance/lattice_test.go`

**Interfaces:**
- Consumes: `coverage.Lattice()` from Task 3, `CanonicalKey` from Task 2.
- Produces: `conformance.LatticeArray(key string, null bool) (arrow.Array, error)` and `conformance.LatticeKeys() []string`. Task 6's runner calls both.

- [ ] **Step 1: Write the failing test**

Append to `internal/conformance/lattice_test.go`:

```go
// Two statements that must agree: lattice.yml says which types exist, and the
// value table says how to build one. Either alone leaves a type declared and
// unexercised, or exercised and uncounted.
func TestToolingConformanceLattice_EveryDeclaredKeyHasAValue(t *testing.T) {
	coverage.Covers(t, "tooling.conformance")

	declared, err := coverage.Lattice()
	assert.NoError(t, err)

	built := map[string]bool{}
	for _, k := range LatticeKeys() {
		built[k] = true
	}

	for _, e := range declared {
		if !built[e.Key] {
			t.Errorf("lattice.yml declares %q; the value table builds no array for it", e.Key)
		}
	}
	for k := range built {
		found := false
		for _, e := range declared {
			if e.Key == k {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("the value table builds %q; lattice.yml does not declare it", k)
		}
	}
}

// The array the table builds must carry the type the key names, or a row
// would be judged against a different type than the one it claims.
func TestToolingConformanceLattice_EveryValueCarriesItsOwnKey(t *testing.T) {
	for _, k := range LatticeKeys() {
		arr, err := LatticeArray(k, false)
		if err != nil {
			t.Errorf("LatticeArray(%q): %v", k, err)
			continue
		}
		if got := CanonicalKey(arr.DataType()); got != k {
			t.Errorf("LatticeArray(%q) built a %s", k, got)
		}
		assert.Equal(t, arr.Len(), 1)
		assert.Equal(t, arr.IsNull(0), false)
		arr.Release()
	}
}

// type.null needs a null in every declared type, so every key builds one.
func TestToolingConformanceLattice_EveryKeyBuildsANull(t *testing.T) {
	for _, k := range LatticeKeys() {
		arr, err := LatticeArray(k, true)
		if err != nil {
			t.Errorf("LatticeArray(%q, null): %v", k, err)
			continue
		}
		assert.Equal(t, arr.Len(), 1)
		assert.Equal(t, arr.IsNull(0), true)
		arr.Release()
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `CGO_ENABLED=1 go test ./internal/conformance/ -run TestToolingConformanceLattice`
Expected: FAIL to compile with `undefined: LatticeKeys` and `undefined: LatticeArray`.

- [ ] **Step 3: Write the value table**

Append to `internal/conformance/lattice.go`. Add `sort`, `time`, `github.com/apache/arrow-go/v18/arrow/array`, `github.com/apache/arrow-go/v18/arrow/decimal128` and `github.com/apache/arrow-go/v18/arrow/memory` to the imports.

```go
// latticeType is the concrete Arrow type each key builds.
//
// Values are chosen to catch a width or sign error rather than to be tidy:
// the integer bounds, a float with a fraction no smaller width holds, and a
// string carrying unicode, an escape and a quote.
var latticeType = map[string]arrow.DataType{
	"bool":    arrow.FixedWidthTypes.Boolean,
	"int8":    arrow.PrimitiveTypes.Int8,
	"int16":   arrow.PrimitiveTypes.Int16,
	"int32":   arrow.PrimitiveTypes.Int32,
	"int64":   arrow.PrimitiveTypes.Int64,
	"uint8":   arrow.PrimitiveTypes.Uint8,
	"uint16":  arrow.PrimitiveTypes.Uint16,
	"uint32":  arrow.PrimitiveTypes.Uint32,
	"uint64":  arrow.PrimitiveTypes.Uint64,
	"float32": arrow.PrimitiveTypes.Float32,
	"float64": arrow.PrimitiveTypes.Float64,
	"decimal(*, *)": &arrow.Decimal128Type{Precision: 38, Scale: 0},
	"utf8":          arrow.BinaryTypes.String,
	"binary":        arrow.BinaryTypes.Binary,
	"date32":        arrow.FixedWidthTypes.Date32,
	"time64[us]":    arrow.FixedWidthTypes.Time64us,
	"time64[ns]":    arrow.FixedWidthTypes.Time64ns,
	"timestamp[s]":  &arrow.TimestampType{Unit: arrow.Second},
	"timestamp[ms]": &arrow.TimestampType{Unit: arrow.Millisecond},
	"timestamp[us]": &arrow.TimestampType{Unit: arrow.Microsecond},
	"timestamp[ns]": &arrow.TimestampType{Unit: arrow.Nanosecond},
	// The zone is deliberately not UTC. A runner on a UTC host proves nothing
	// about zone leakage when the value it writes is already UTC.
	"timestamp[us, tz=*]":     &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "Asia/Tokyo"},
	"month_day_nano_interval": arrow.FixedWidthTypes.MonthDayNanoInterval,

	"list<int64>":               arrow.ListOf(arrow.PrimitiveTypes.Int64),
	"fixed_size_list<int32>[3]": arrow.FixedSizeListOf(3, arrow.PrimitiveTypes.Int32),
	"struct":                    arrow.StructOf(arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Int32, Nullable: true}),
	"map":                       arrow.MapOf(arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int32),
	"sparse_union": arrow.SparseUnionOf(
		[]arrow.Field{{Name: "n", Type: arrow.PrimitiveTypes.Int32, Nullable: true}},
		[]arrow.UnionTypeCode{0}),
	"dictionary": &arrow.DictionaryType{
		IndexType: arrow.PrimitiveTypes.Uint8, ValueType: arrow.BinaryTypes.String},
}

// LatticeKeys returns every key the value table can build, sorted.
func LatticeKeys() []string {
	out := make([]string, 0, len(latticeType))
	for k := range latticeType {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// LatticeArray builds a one-row array of the key's type. The caller releases
// it.
//
// A null row and a valued row are the same call, because type.null asks the
// same question of the same type: what the destination holds afterwards.
func LatticeArray(key string, null bool) (arrow.Array, error) {
	dt, ok := latticeType[key]
	if !ok {
		return nil, fmt.Errorf("conformance: no lattice value for %q", key)
	}

	mem := memory.NewGoAllocator()
	b := array.NewBuilder(mem, dt)
	defer b.Release()

	if null {
		b.AppendNull()
		return b.NewArray(), nil
	}
	if err := appendLatticeValue(b, key); err != nil {
		return nil, err
	}
	return b.NewArray(), nil
}

// latticeInstant is the one moment every temporal key writes, so a shifted
// value names the unit that shifted it.
var latticeInstant = time.Date(2026, 9, 8, 12, 0, 0, 123456789, time.UTC)

func appendLatticeValue(b array.Builder, key string) error {
	switch bldr := b.(type) {
	case *array.BooleanBuilder:
		bldr.Append(true)
	case *array.Int8Builder:
		bldr.Append(-128)
	case *array.Int16Builder:
		bldr.Append(-32768)
	case *array.Int32Builder:
		bldr.Append(-2147483648)
	case *array.Int64Builder:
		bldr.Append(-9223372036854775808)
	case *array.Uint8Builder:
		bldr.Append(255)
	case *array.Uint16Builder:
		bldr.Append(65535)
	case *array.Uint32Builder:
		bldr.Append(4294967295)
	case *array.Uint64Builder:
		bldr.Append(18446744073709551615)
	case *array.Float32Builder:
		bldr.Append(3.4028235e38)
	case *array.Float64Builder:
		bldr.Append(1.7976931348623157e308)
	case *array.Decimal128Builder:
		bldr.Append(decimal128.FromI64(170141183460469231))
	case *array.StringBuilder:
		// #149's defect: escapes reached the destination undecoded. The quote
		// and the backslash are the two that were corrupted.
		bldr.Append("L'Œil 👁 \"quoted\" back\\slash\ttab")
	case *array.BinaryBuilder:
		bldr.Append([]byte{0x00, 0xff, 0x7f, 0x80})
	case *array.Date32Builder:
		bldr.Append(arrow.Date32FromTime(latticeInstant))
	case *array.Time64Builder:
		bldr.Append(arrow.Time64(12*3600*1e6 + 123456))
	case *array.TimestampBuilder:
		ts, err := arrow.TimestampFromTime(latticeInstant, bldr.Type().(*arrow.TimestampType).Unit)
		if err != nil {
			return fmt.Errorf("conformance: %s: %w", key, err)
		}
		bldr.Append(ts)
	case *array.MonthDayNanoIntervalBuilder:
		bldr.Append(arrow.MonthDayNanoInterval{Months: 1, Days: 2, Nanoseconds: 3})
	case *array.ListBuilder:
		bldr.Append(true)
		bldr.ValueBuilder().(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)
	case *array.FixedSizeListBuilder:
		bldr.Append(true)
		bldr.ValueBuilder().(*array.Int32Builder).AppendValues([]int32{1, 2, 3}, nil)
	case *array.StructBuilder:
		bldr.Append(true)
		bldr.FieldBuilder(0).(*array.Int32Builder).Append(7)
	case *array.MapBuilder:
		bldr.Append(true)
		bldr.KeyBuilder().(*array.StringBuilder).Append("k")
		bldr.ItemBuilder().(*array.Int32Builder).Append(9)
	case *array.SparseUnionBuilder:
		bldr.Append(0)
		bldr.Child(0).(*array.Int32Builder).Append(5)
	case *array.BinaryDictionaryBuilder:
		if err := bldr.AppendString("a"); err != nil {
			return fmt.Errorf("conformance: %s: %w", key, err)
		}
	default:
		return fmt.Errorf("conformance: no value appender for %q (%T)", key, b)
	}
	return nil
}
```

- [ ] **Step 4: Run the tests**

Run: `CGO_ENABLED=1 go test ./internal/conformance/ -run TestToolingConformanceLattice -v`
Expected: PASS, three tests.

A builder type the plan named wrongly fails `TestToolingConformanceLattice_EveryValueCarriesItsOwnKey` with the concrete `%T` in the message. Correct the case to that type.

- [ ] **Step 5: Run the full unit pass**

Run: `CGO_ENABLED=1 go test -short ./...`
Expected: all packages ok.

- [ ] **Step 6: Commit**

```bash
git add internal/conformance/lattice.go internal/conformance/lattice_test.go
git commit -m "conformance: one Arrow value per lattice key, held equal to the registry

The runner needs an array per declared type. Values live beside the builders
rather than in YAML: encoding 29 Arrow types as YAML means writing a decoder
for 29 Arrow types.

A test holds the two key sets equal in both directions. Without it a lattice
entry with no value is declared and never exercised, and a value with no
entry is exercised and never counted."
```

---

### Task 5: The `types:` declaration and the generator's lattice check

**Files:**
- Modify: `docs/coverage/integrations.yml` (the `sink.clickhouse` entry)
- Modify: `internal/coverage/registry.go`
- Modify: `scripts/coverage_matrix.py:101-181` (`validate_registries`) and its loaders
- Test: `tests/tooling/test_coverage_matrix.py`

**Interfaces:**
- Consumes: `coverage.Lattice()` from Task 3, `errs.CodeSinkTypeUnsupported`'s string from Task 1.
- Produces: `coverage.TypeDecl{Key, Outcome, Rule, Code string; Expect any; Columns []string}` and `coverage.TypesFor(integration string) ([]TypeDecl, error)`. Task 6's runner reads it.

- [ ] **Step 1: Declare the ClickHouse type table**

In `docs/coverage/integrations.yml`, add a `types:` block to the `sink.clickhouse` entry, after `exempt: []`:

```yaml
    # Arrow key -> what the sink does with it. Keys come from lattice.yml and
    # are conformance.CanonicalKey's output.
    #
    # `columns` are the ClickHouse column types that accept the key. Every one
    # is exercised, and the cell is covered only when all of them pass. That
    # list is what the integration page publishes.
    #
    # Outcomes here are the starting declaration, written from reading
    # internal/sinks/clickhouse.go. The runner's first job is to confirm or
    # correct each one; a row the test disagrees with is a finding, not a
    # test to relax.
    types:
      bool:
        outcome: exact
        columns: [Bool, UInt8]
      int8:
        outcome: exact
        columns: [Int8]
      int16:
        outcome: exact
        columns: [Int16]
      int32:
        outcome: exact
        columns: [Int32]
      int64:
        outcome: exact
        columns: [Int64]
      uint8:
        outcome: exact
        columns: [UInt8]
      uint16:
        outcome: exact
        columns: [UInt16]
      uint32:
        outcome: exact
        columns: [UInt32]
      uint64:
        outcome: exact
        columns: [UInt64]
      float32:
        outcome: exact
        columns: [Float32]
      float64:
        outcome: exact
        columns: [Float64]
      utf8:
        outcome: exact
        columns: [String, LowCardinality(String)]
      binary:
        outcome: exact
        columns: [String]
      date32:
        outcome: exact
        columns: [Date, Date32]
      "timestamp[s]":
        outcome: exact
        columns: [DateTime]
      "timestamp[ms]":
        outcome: exact
        columns: ["DateTime64(3)"]
      "timestamp[us]":
        outcome: exact
        columns: ["DateTime64(6)"]
      "timestamp[ns]":
        outcome: exact
        columns: ["DateTime64(9)"]
      "timestamp[us, tz=*]":
        outcome: coerced
        rule: stored as the same UTC instant; the column's own zone governs rendering
        columns: ["DateTime64(6)"]

      # arrowValue has no case for these, so each falls to its default at
      # clickhouse.go:404. HUGEINT is the one a user reaches by accident:
      # DuckDB promotes integer overflow to it.
      "decimal(*, *)":
        outcome: unsupported
        code: user.sink.type_unsupported
        columns: []
      "time64[us]":
        outcome: unsupported
        code: user.sink.type_unsupported
        columns: []
      "time64[ns]":
        outcome: unsupported
        code: user.sink.type_unsupported
        columns: []
      month_day_nano_interval:
        outcome: unsupported
        code: user.sink.type_unsupported
        columns: []
      "fixed_size_list<int32>[3]":
        outcome: unsupported
        code: user.sink.type_unsupported
        columns: []
      struct:
        outcome: unsupported
        code: user.sink.type_unsupported
        columns: []
      map:
        outcome: unsupported
        code: user.sink.type_unsupported
        columns: []
      sparse_union:
        outcome: unsupported
        code: user.sink.type_unsupported
        columns: []
      dictionary:
        outcome: unsupported
        code: user.sink.type_unsupported
        columns: []

      # Depth 2. Declared here so the lattice check passes; PR 2 exercises it.
      "list<int64>":
        outcome: exact
        columns: ["Array(Int64)"]
    nulls:
      default:
        outcome: coerced
        rule: the column type's zero value; the DEFAULT expression is not evaluated
      "Nullable(*)":
        outcome: exact
```

- [ ] **Step 2: Write the failing Go test for the reader**

Append to `internal/coverage/registry_test.go`:

```go
// Every lattice key must have an outcome, or the sink's behaviour on that
// type is undeclared and the matrix cannot tell a gap from a pass.
func TestToolingCoverage_ClickhouseDeclaresEveryLatticeKey(t *testing.T) {
	Covers(t, "tooling.coverage")

	declared, err := TypesFor("sink.clickhouse")
	assert.NoError(t, err)

	byKey := map[string]TypeDecl{}
	for _, d := range declared {
		byKey[d.Key] = d
	}

	entries, err := Lattice()
	assert.NoError(t, err)
	for _, e := range entries {
		d, ok := byKey[e.Key]
		if !ok {
			t.Errorf("sink.clickhouse declares no outcome for %q", e.Key)
			continue
		}
		switch d.Outcome {
		case "exact":
		case "coerced":
			if d.Rule == "" {
				t.Errorf("sink.clickhouse: %q is coerced with no rule", e.Key)
			}
		case "unsupported":
			if d.Code == "" {
				t.Errorf("sink.clickhouse: %q is unsupported with no code", e.Key)
			}
		default:
			t.Errorf("sink.clickhouse: %q has outcome %q", e.Key, d.Outcome)
		}
	}
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `CGO_ENABLED=1 go test ./internal/coverage/ -run TestToolingCoverage_ClickhouseDeclaresEveryLatticeKey`
Expected: FAIL to compile with `undefined: TypesFor`.

- [ ] **Step 4: Write the reader**

Append to `internal/coverage/registry.go`:

```go
// TypeDecl is one integration's declared outcome for one Arrow type.
type TypeDecl struct {
	// Key is the lattice.yml key. It is the map key in the YAML, so it is
	// filled in by TypesFor rather than unmarshalled.
	Key string `yaml:"-"`

	// Outcome is exact, coerced or unsupported.
	Outcome string `yaml:"outcome"`

	// Rule states the coercion in prose, for the rendered page. Required when
	// Outcome is coerced.
	Rule string `yaml:"rule"`

	// Expect is what a coerced row reads back as. Prose cannot be asserted;
	// this can.
	Expect any `yaml:"expect"`

	// Code is the errs code an unsupported type must fail with.
	Code string `yaml:"code"`

	// Columns are the destination column types that accept this key. Every one
	// is exercised.
	Columns []string `yaml:"columns"`
}

// TypesFor returns one integration's type table, sorted by key.
//
// Sorted rather than in file order: a YAML mapping has no order, so file
// order is whatever the parser chose, and a runner that iterated it would
// report its rows in a different sequence run to run.
func TypesFor(integration string) ([]TypeDecl, error) {
	raw, err := readRegistry("integrations.yml")
	if err != nil {
		return nil, err
	}

	var doc struct {
		Integrations []struct {
			ID    string              `yaml:"id"`
			Types map[string]TypeDecl `yaml:"types"`
		} `yaml:"integrations"`
	}
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		return nil, fmt.Errorf("coverage: parse integrations.yml: %w", err)
	}

	for _, entry := range doc.Integrations {
		if entry.ID != integration {
			continue
		}
		out := make([]TypeDecl, 0, len(entry.Types))
		for key, decl := range entry.Types {
			decl.Key = key
			out = append(out, decl)
		}
		sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
		return out, nil
	}
	return nil, fmt.Errorf("coverage: integrations.yml declares no %q", integration)
}
```

- [ ] **Step 5: Run the Go test**

Run: `CGO_ENABLED=1 go test ./internal/coverage/ -run TestToolingCoverage_ClickhouseDeclaresEveryLatticeKey -v`
Expected: PASS

- [ ] **Step 6: Write the failing Python test for the lattice check**

Append to `tests/tooling/test_coverage_matrix.py`:

```python
LATTICE = [
    {"key": "int64", "duckdb": ["BIGINT"], "depth": 1},
    {"key": "utf8", "duckdb": ["VARCHAR"], "depth": 1},
]


def test_a_type_key_outside_the_lattice_is_reported():
    integrations = [{
        "id": "sink.clickhouse", "kind": "sink", "feature": "sink.clickhouse",
        "types": {"int64": {"outcome": "exact", "columns": ["Int64"]},
                  "utf8": {"outcome": "exact", "columns": ["String"]},
                  "decimal128(38, 0)": {"outcome": "unsupported",
                                        "code": "user.sink.type_unsupported"}},
    }]
    problems = cm.validate_types(LATTICE, integrations)
    assert any("decimal128(38, 0)" in p and "lattice.yml" in p for p in problems)


def test_a_lattice_key_the_sink_does_not_declare_is_reported():
    integrations = [{
        "id": "sink.clickhouse", "kind": "sink", "feature": "sink.clickhouse",
        "types": {"int64": {"outcome": "exact", "columns": ["Int64"]}},
    }]
    problems = cm.validate_types(LATTICE, integrations)
    assert any("utf8" in p and "sink.clickhouse" in p for p in problems)


def test_a_sink_with_no_type_table_is_not_reported_key_by_key():
    """One line saying the sink has no table, not 29 saying it lacks each key."""
    integrations = [{
        "id": "sink.kafka", "kind": "sink", "feature": "sink.kafka",
    }]
    problems = cm.validate_types(LATTICE, integrations)
    assert problems == []


def test_a_coerced_row_without_a_rule_is_reported():
    integrations = [{
        "id": "sink.clickhouse", "kind": "sink", "feature": "sink.clickhouse",
        "types": {"int64": {"outcome": "coerced", "columns": ["Int64"]},
                  "utf8": {"outcome": "exact", "columns": ["String"]}},
    }]
    problems = cm.validate_types(LATTICE, integrations)
    assert any("int64" in p and "rule" in p for p in problems)
```

- [ ] **Step 7: Run the Python test to verify it fails**

Run: `uv run pytest tests/tooling/test_coverage_matrix.py -k validate_types -q` — or, if that selects nothing, `uv run pytest tests/tooling/test_coverage_matrix.py -q -k "lattice or type_key or coerced_row"`
Expected: FAIL with `AttributeError: module 'coverage_matrix' has no attribute 'validate_types'`.

- [ ] **Step 8: Write the generator check**

In `scripts/coverage_matrix.py`, add the loader beside `load_integrations` (around line 96):

```python
LATTICE = os.path.join(REPO, "docs", "coverage", "lattice.yml")

# The outcomes a type table may declare. Anything else is a typo, and a typo
# must not reach the matrix as a missing cell.
OUTCOMES = ("exact", "coerced", "unsupported")


def load_lattice():
    with open(LATTICE) as fh:
        return yaml.safe_load(fh)["lattice"]
```

Add `validate_types` after `validate_registries` (after line 181):

```python
def validate_types(lattice, integrations):
    """Every problem in a type table, one line each. Empty when consistent.

    The lattice is closed, so a key outside it is a typo and a key inside it
    that a sink does not declare is a gap. Both are reported here rather than
    rendered as a missing cell: "missing" is the signal the matrix exists to
    carry, and it must not be spendable on a misspelling.

    A sink with no type table at all gets one line, not one per key. Five of
    the six sinks have no table in this revision, and 29 lines each would bury
    the one sink that does.
    """
    problems = []
    keys = {entry["key"] for entry in lattice}

    for integ in integrations:
        types = integ.get("types")
        if not types:
            continue

        iid = integ["id"]
        for key, decl in sorted(types.items()):
            if key not in keys:
                problems.append(
                    f"integrations.yml: {iid} declares type {key!r}, "
                    "which lattice.yml does not list")
                continue

            outcome = decl.get("outcome")
            if outcome not in OUTCOMES:
                problems.append(
                    f"integrations.yml: {iid} type {key!r} outcome "
                    f"{outcome!r} is not one of {OUTCOMES}")
            # A coercion stated as an outcome and no rule is an excuse. The
            # rule is what the integration page publishes, and what a reader
            # needs to predict what their column will hold.
            if outcome == "coerced" and not decl.get("rule"):
                problems.append(
                    f"integrations.yml: {iid} type {key!r} is coerced with no rule")
            if outcome == "unsupported" and not decl.get("code"):
                problems.append(
                    f"integrations.yml: {iid} type {key!r} is unsupported with no code")
            if outcome in ("exact", "coerced") and not decl.get("columns"):
                problems.append(
                    f"integrations.yml: {iid} type {key!r} is {outcome} but names "
                    "no destination column type to write it to")

        for key in sorted(keys - set(types)):
            problems.append(
                f"integrations.yml: {iid} has a type table and declares no "
                f"outcome for {key!r}, which lattice.yml lists")

    return problems
```

Wire it into `main`, beside the existing call at line 994:

```python
    problems = validate_registries(invariants, integrations, features)
    problems += validate_types(load_lattice(), integrations)
```

- [ ] **Step 9: Run the Python tests**

Run: `uv run pytest tests/tooling/test_coverage_matrix.py -q`
Expected: PASS, including the four new tests.

- [ ] **Step 10: Run the generator against the real registries**

Run: `uv run python scripts/coverage_matrix.py --go /dev/null --go-integration /dev/null --pytest /dev/null 2>&1 | head -30`
Expected: no `integrations.yml:` problem lines. Type cells still read missing, because no marker has been emitted yet.

- [ ] **Step 11: Run the full unit pass and commit**

Run: `CGO_ENABLED=1 go test -short ./...`
Expected: all packages ok.

```bash
git add docs/coverage/integrations.yml internal/coverage/registry.go internal/coverage/registry_test.go scripts/coverage_matrix.py tests/tooling/test_coverage_matrix.py
git commit -m "coverage: ClickHouse declares an outcome for every Arrow type

Written by hand from reading the sink, not generated from it. A table derived
from the sink can never report that the sink is missing a type, and the eight
types it is missing are the point.

The generator holds the table to the lattice in both directions. Without the
gap half, a sink covers the types someone remembered and the matrix reports
full coverage."
```

---

### Task 6: The type runner, and ClickHouse under it

**Files:**
- Create: `internal/conformance/types.go`
- Create: `internal/conformance/types_test.go`
- Create: `internal/sinks/conformance_types_test.go`

**Interfaces:**
- Consumes: `CanonicalKey`, `LatticeArray`, `LatticeKeys` from Tasks 2 and 4; `coverage.TypesFor` from Task 5; `errs.CodeSinkTypeUnsupported` from Task 1.
- Produces: `conformance.Types(t *testing.T, s TypeSubject)`, `conformance.TypeSubject`, `conformance.TypeDestination`. PR 2 and the MotherDuck column reuse both.

- [ ] **Step 1: Write the runner's self-test**

Create `internal/conformance/types_test.go`:

```go
package conformance

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/turbolytics/sql-flow/internal/core"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/turbolytics/sql-flow/internal/errs"
	"github.com/zeebo/assert"
)

// The runner gates every sink's type behaviour, so a runner that passes a
// sink which drops a value is worse than no runner.

// typeDouble accepts every type and returns what it was given.
type typeDouble struct {
	*memSink
	reject map[string]bool
}

func newTypeDouble(reject ...string) *typeDouble {
	d := &typeDouble{memSink: newMemSink(), reject: map[string]bool{}}
	for _, k := range reject {
		d.reject[k] = true
	}
	return d
}

func (d *typeDouble) WriteTable(ctx context.Context, tbl arrow.Table) error {
	key := CanonicalKey(tbl.Schema().Field(0).Type)
	if d.reject[key] {
		return errs.New(errs.CodeSinkTypeUnsupported, "unsupported arrow type %s", key)
	}
	return d.memSink.WriteTable(ctx, tbl)
}

func typeSubject(d *typeDouble, declared []coverage.TypeDecl) TypeSubject {
	return TypeSubject{
		Integration: "sink.conformance_double",
		Declared:    declared,
		Prepare: func(t *testing.T, key, columnType string) TypeDestination {
			return TypeDestination{
				Sink:     core.Sink(d),
				ReadBack: func(t *testing.T) (any, error) { return d.LastValue(), nil },
			}
		},
	}
}

func TestToolingConformanceTypes_ADoubleThatHonoursItsTablePasses(t *testing.T) {
	coverage.Covers(t, "tooling.conformance")

	declared := []coverage.TypeDecl{
		{Key: "int64", Outcome: "exact", Columns: []string{"Int64"}},
	}
	Types(t, typeSubject(newTypeDouble(), declared))
}

// The defect the runner exists to catch: a table claiming exact for a type
// the sink refuses.
func TestToolingConformanceTypes_ATypeDeclaredExactThatFailsIsCaught(t *testing.T) {
	declared := []coverage.TypeDecl{
		{Key: "int64", Outcome: "exact", Columns: []string{"Int64"}},
	}
	v := typeVerdicts(t, typeSubject(newTypeDouble("int64"), declared))
	assertFailureMentions(t, v, "type.roundtrip", "int64")
}

// The mirror defect: a table claiming unsupported for a type that works. It
// hides working support, and on the published page it tells a user to cast
// a column they did not have to.
func TestToolingConformanceTypes_ATypeDeclaredUnsupportedThatWorksIsCaught(t *testing.T) {
	declared := []coverage.TypeDecl{
		{Key: "int64", Outcome: "unsupported", Code: "user.sink.type_unsupported"},
	}
	v := typeVerdicts(t, typeSubject(newTypeDouble(), declared))
	assertFailureMentions(t, v, "type.roundtrip", "int64")
}

// An unsupported type must fail with the declared code. A bare error reaches
// the operator as system.internal.unexpected.
func TestToolingConformanceTypes_AnUncodedFailureIsCaught(t *testing.T) {
	declared := []coverage.TypeDecl{
		{Key: "int64", Outcome: "unsupported", Code: "user.sink.other"},
	}
	v := typeVerdicts(t, typeSubject(newTypeDouble("int64"), declared))
	assertFailureMentions(t, v, "type.roundtrip", "user.sink.other")
}

// A type absent from the table must fail loudly rather than be coerced.
func TestToolingConformanceTypes_AnUndeclaredTypeThatSucceedsIsCaught(t *testing.T) {
	declared := []coverage.TypeDecl{
		{Key: "int64", Outcome: "exact", Columns: []string{"Int64"}},
	}
	v := typeVerdicts(t, typeSubject(newTypeDouble(), declared))
	assertFailureMentions(t, v, "type.undeclared.fails_loud", "")
}

func assertFailureMentions(t *testing.T, verdicts []verdict, invariant, substring string) {
	t.Helper()
	for _, v := range verdicts {
		if v.invariant != invariant {
			continue
		}
		if v.failure == "" {
			t.Fatalf("%s passed; it must fail", invariant)
		}
		if substring != "" && !strings.Contains(v.failure, substring) {
			t.Fatalf("%s failure %q does not name %q", invariant, v.failure, substring)
		}
		return
	}
	t.Fatalf("no verdict for %s", invariant)
}
```

Add `strings` to the imports. `memSink` already exists at `internal/conformance/conformance_test.go:202`; add a `LastValue()` method to it there returning the first column's first value of the last table it took, and a `Value` field to store it.

- [ ] **Step 2: Run test to verify it fails**

Run: `CGO_ENABLED=1 go test ./internal/conformance/ -run TestToolingConformanceTypes`
Expected: FAIL to compile with `undefined: Types`, `undefined: TypeSubject`, `undefined: typeVerdicts`.

- [ ] **Step 3: Write the runner**

Create `internal/conformance/types.go`:

```go
package conformance

// The type table runner.
//
// Every sink converts Arrow to whatever its destination speaks, and that
// conversion is where five of the seven post-v1.0.4 defects were: a dropped
// escape (#149), a shifted timestamp (#153), a rejected list (#150), a
// silently dropped struct field (#151) and a flattened array (#147). None of
// them was visible to a test that wrote an int64 and read it back.
//
// The subject supplies a destination per row rather than one wide table.
// type.undeclared.fails_loud requires the batch to fail, and a shared table
// would let one unsupported column destroy every other row's evidence.

import (
	"context"
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/turbolytics/sql-flow/internal/core"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/turbolytics/sql-flow/internal/errs"
)

const (
	typeRoundtrip  = "type.roundtrip"
	typeNull       = "type.null"
	typeUndeclared = "type.undeclared.fails_loud"
)

// TypeDestination is one column of one type, and the way back to it.
type TypeDestination struct {
	// Sink writes to a destination holding one column named "v".
	Sink core.Sink

	// ReadBack returns the single value that column holds. It must not go
	// through the sink.
	ReadBack func(t *testing.T) (any, error)
}

// TypeSubject is what an integration hands the type runner.
type TypeSubject struct {
	// Integration is the integrations.yml id.
	Integration string

	// Declared is the integration's type table. Passed in rather than read
	// here so the harness's own tests can drive it with a table of one row.
	Declared []coverage.TypeDecl

	// Prepare creates a destination with a single column "v" of columnType,
	// and a sink writing to it. columnType is empty for an unsupported row:
	// there is no column type that accepts the value, and the batch must fail
	// before any destination is needed.
	Prepare func(t *testing.T, key, columnType string) TypeDestination
}

// Types proves the type invariants the subject's table declares.
func Types(t *testing.T, s TypeSubject) {
	t.Helper()
	if s.Integration == "" {
		t.Fatal("conformance: TypeSubject.Integration is required")
	}
	if s.Prepare == nil {
		t.Fatalf("conformance: %s needs Prepare", s.Integration)
	}

	feature, hasFeature, err := coverage.FeatureFor(s.Integration)
	if err != nil {
		t.Fatalf("conformance: %v", err)
	}

	for _, v := range typeVerdicts(t, s) {
		t.Run(v.invariant, func(t *testing.T) {
			if hasFeature {
				coverage.Covers(t, feature)
			}
			if v.skipped != "" {
				t.Skip(v.skipped)
			}
			if v.failure != "" {
				t.Fatal(v.failure)
			}
			coverage.Invariant(t, v.invariant, s.Integration)
		})
	}
}

// typeVerdicts judges each type invariant across every declared row.
//
// One verdict per invariant, not per row: the cell is covered only when every
// declared type passed, so a single failing row fails the invariant. The
// failure names the row, because "type.roundtrip failed" without a type is
// not actionable.
func typeVerdicts(t *testing.T, s TypeSubject) []verdict {
	t.Helper()

	roundtrip := verdict{invariant: typeRoundtrip}
	nulls := verdict{invariant: typeNull}

	for _, d := range s.Declared {
		if f := judgeRow(t, s, d, false); f != "" && roundtrip.failure == "" {
			roundtrip.failure = f
		}
		if f := judgeRow(t, s, d, true); f != "" && nulls.failure == "" {
			nulls.failure = f
		}
	}

	return []verdict{roundtrip, nulls, judgeUndeclared(t, s)}
}

// judgeRow writes one value of one type and judges the declared outcome.
func judgeRow(t *testing.T, s TypeSubject, d coverage.TypeDecl, null bool) string {
	t.Helper()

	arr, err := LatticeArray(d.Key, null)
	if err != nil {
		return fmt.Sprintf("%s: %v", d.Key, err)
	}
	defer arr.Release()

	// An unsupported type has no column that accepts it, so it is written
	// once, to no particular column.
	columns := d.Columns
	if d.Outcome == "unsupported" {
		columns = []string{""}
	}

	for _, columnType := range columns {
		dest := s.Prepare(t, d.Key, columnType)
		tbl := oneColumnTable(arr)

		ctx := context.Background()
		err := dest.Sink.WriteTable(ctx, tbl)
		if err == nil {
			err = dest.Sink.Flush(ctx)
		}
		tbl.Release()

		if d.Outcome == "unsupported" {
			if err == nil {
				return fmt.Sprintf("%s is declared unsupported and the sink took it; "+
					"either the sink gained support and the table is stale, or the "+
					"value reached the destination in some other type", d.Key)
			}
			if got := string(errs.CodeOf(err)); got != d.Code {
				return fmt.Sprintf("%s failed with code %s, and the table declares %s: %v",
					d.Key, got, d.Code, err)
			}
			continue
		}

		if err != nil {
			return fmt.Sprintf("%s into %s is declared %s and the sink refused it: %v",
				d.Key, columnType, d.Outcome, err)
		}

		got, err := dest.ReadBack(t)
		if err != nil {
			return fmt.Sprintf("%s into %s: read back: %v", d.Key, columnType, err)
		}
		if null && got != nil && d.Outcome == "exact" {
			return fmt.Sprintf("%s into %s: a null read back as %v", d.Key, columnType, got)
		}
	}
	return ""
}

// judgeUndeclared writes a type the lattice does not carry. It must fail with
// a code rather than be coerced into something the destination accepts.
func judgeUndeclared(t *testing.T, s TypeSubject) verdict {
	t.Helper()
	v := verdict{invariant: typeUndeclared}

	// float16 is in Arrow and not in DuckDB's output, so no integration
	// declares it and none legitimately supports it.
	b := array.NewFloat16Builder(memoryAllocator())
	defer b.Release()
	b.AppendNull()
	arr := b.NewArray()
	defer arr.Release()

	dest := s.Prepare(t, CanonicalKey(arr.DataType()), "")
	tbl := oneColumnTable(arr)
	defer tbl.Release()

	ctx := context.Background()
	err := dest.Sink.WriteTable(ctx, tbl)
	if err == nil {
		err = dest.Sink.Flush(ctx)
	}
	if err == nil {
		v.failure = "a float16 column, which no type table declares, reached the " +
			"destination without an error; an undeclared type must fail the batch " +
			"rather than be coerced into whatever the destination accepts"
		return v
	}
	if errs.CodeOf(err) == errs.CodeInternalUnexpected {
		v.failure = fmt.Sprintf("a float16 column failed with no code, so an operator "+
			"reads it as sqlflow's fault rather than as a column to cast: %v", err)
	}
	return v
}

func oneColumnTable(arr arrow.Array) arrow.Table {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "v", Type: arr.DataType(), Nullable: true},
	}, nil)
	col := arrow.NewColumnFromArr(schema.Field(0), arr)
	defer col.Release()
	return array.NewTable(schema, []arrow.Column{col}, 1)
}
```

Add a `memoryAllocator()` helper returning `memory.NewGoAllocator()`, or inline it and import `github.com/apache/arrow-go/v18/arrow/memory`.

- [ ] **Step 4: Run the self-tests**

Run: `CGO_ENABLED=1 go test ./internal/conformance/ -run TestToolingConformanceTypes -v`
Expected: PASS, five tests.

- [ ] **Step 5: Write the ClickHouse subject**

Create `internal/sinks/conformance_types_test.go`:

```go
package sinks

// The ClickHouse sink under the type runner, against a real server.
//
// This is the first type table, so it is also the proof the runner needs
// nothing sink-specific: a destination per column type, a sink writing to it,
// and a read-back that does not go through the sink.

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tcclickhouse "github.com/testcontainers/testcontainers-go/modules/clickhouse"
	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/conformance"
	"github.com/turbolytics/sql-flow/internal/core"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

func TestIntegrationSinkClickhouse_Types(t *testing.T) {
	coverage.Covers(t, "sink.clickhouse")

	if testing.Short() {
		t.Skip("integration test: -short runs the unit pass only")
	}
	ctx := context.Background()

	ch, err := tcclickhouse.Run(ctx, clickhouseImage,
		testcontainers.WithExposedPorts("8123/tcp"),
		tcclickhouse.WithUsername(clickhouseUser),
		tcclickhouse.WithPassword(clickhousePassword),
		tcclickhouse.WithDatabase(clickhouseDatabase),
	)
	if err != nil {
		t.Fatalf("start clickhouse: %v", err)
	}
	t.Cleanup(func() { _ = ch.Terminate(context.Background()) })

	declared, err := coverage.TypesFor("sink.clickhouse")
	assert.NoError(t, err)

	conformance.Types(t, conformance.TypeSubject{
		Integration: "sink.clickhouse",
		Declared:    declared,

		// One table per row. An unsupported row fails before any INSERT, so
		// it gets a table of one Int64 column that its value will never
		// reach.
		Prepare: func(t *testing.T, key, columnType string) conformance.TypeDestination {
			if columnType == "" {
				columnType = "Int64"
			}
			table := fmt.Sprintf("t_%d", time.Now().UnixNano())
			direct := mustDirectSink(t, ch, table)
			assert.NoError(t, direct.conn.Exec(context.Background(),
				fmt.Sprintf("CREATE TABLE %s (v %s) ENGINE = MergeTree() ORDER BY tuple()",
					table, columnType)))

			return conformance.TypeDestination{
				Sink: core.Sink(mustDirectSink(t, ch, table)),
				ReadBack: func(t *testing.T) (any, error) {
					rows, err := direct.conn.Query(context.Background(),
						"SELECT v FROM "+table+" LIMIT 1")
					if err != nil {
						return nil, err
					}
					defer rows.Close()
					if !rows.Next() {
						return nil, rows.Err()
					}
					var v any
					if err := rows.Scan(&v); err != nil {
						return nil, err
					}
					return v, rows.Err()
				},
			}
		},
	})
}
```

- [ ] **Step 6: Run the integration test**

Run: `CGO_ENABLED=1 go test ./internal/sinks/ -run TestIntegrationSinkClickhouse_Types -v 2>&1 | tail -60`
Expected: the container starts and the three subtests report.

**Rows that fail here are findings, not test bugs.** For each one, decide which side is wrong:

- The declaration predicted the sink's behaviour and the sink does something else. Correct `integrations.yml` to what the sink does, and note the row in the PR description. The spec names three expected corrections: a null inside a list, the eight unsupported keys, and whether `timestamp[us, tz=*]` holds its instant.
- The runner is wrong. Fix the runner.

Do not relax an assertion to make a row pass.

- [ ] **Step 7: Regenerate the matrix**

Run: `make coverage-matrix`
Expected: `docs/coverage/matrix.md` shows `type.roundtrip`, `type.null` and `type.undeclared.fails_loud` proven at integration level for `sink.clickhouse`. The other three type invariants stay missing; they land in PRs 2 and 3.

- [ ] **Step 8: Run the full unit pass and commit**

Run: `CGO_ENABLED=1 go test -short ./...`
Expected: all packages ok.

```bash
git add internal/conformance/types.go internal/conformance/types_test.go internal/conformance/conformance_test.go internal/sinks/conformance_types_test.go docs/coverage/matrix.json docs/coverage/matrix.md docs/coverage/integrations.yml
git commit -m "conformance: prove what ClickHouse does with every Arrow type DuckDB emits

Five of the seven post-v1.0.4 defects were in a sink's Arrow conversion: a
dropped escape, a shifted timestamp, a rejected list, a dropped struct field
and a flattened array. A test that writes an int64 and reads it back sees
none of them.

The runner writes one row per declared type through the real sink and reads
it back through the destination's own reader. One destination per row,
because an undeclared type must fail its own batch and a shared table would
blame every other column for it."
```

---

## Self-Review

**Spec coverage.** Every PR 1 item in the spec's Sequence section has a task: `lattice.yml` (Task 3), `types:` for `sink.clickhouse` (Task 5), `conformance.Types` (Task 6), the generator's lattice check (Task 5), `CodeSinkTypeUnsupported` (Task 1). The canonical key and the value table (Tasks 2 and 4) are new since the spec was written and are recorded in it under "The key is canonical, not raw".

`type.string.fidelity` is named in the spec's PR 1 but gets no separate cell here. Its value is carried on the `utf8` row — unicode, a quote, a backslash and a tab — so the escape defect of #149 is exercised. It earns its own cell in PR 2 alongside the empty string and the null-inside-a-list case, where a per-row invariant split is worth the runner change.

**Placeholders.** None. Every step names the file, the command, and the expected result. Task 6 Step 6 deliberately gives a decision procedure rather than an expected pass, because the run's purpose is to discover which declared rows are wrong.

**Type consistency.** `CanonicalKey` (Task 2) is used by Tasks 3, 4, 5 and 6. `LatticeArray`/`LatticeKeys` (Task 4) are called only in Task 6's runner and Task 4's own tests. `coverage.TypeDecl` (Task 5) is the element type of `TypeSubject.Declared` (Task 6). `errs.CodeSinkTypeUnsupported` (Task 1) appears as the literal `user.sink.type_unsupported` in Task 5's YAML and is compared against `errs.CodeOf` in Task 6.
