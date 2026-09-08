# The ClickHouse type table

Status: design, 2026-09-07.

## The problem

The ClickHouse integration page in [ClickHouse/ClickHouse#117740][pr] publishes
a type mapping table. Every row of it was measured by hand against Cloud 26.2
and self-hosted 26.8. Nothing holds it to the sink, so the first type change
makes the page wrong and no test says so.

The coverage framework already declares the claim. `invariants.yml` carries six
type invariants, each `verified_by: typetable`, and `scripts/coverage_matrix.py`
accepts that verifier. No Go code emits a typetable marker, `integrations.yml`
carries no `types:` block, and all 36 type cells in `matrix.md` read missing.
The mechanism is designed and unbuilt.

This design builds it, with ClickHouse as the first column.

[pr]: https://github.com/ClickHouse/ClickHouse/pull/117740

## What the lattice is

DuckDB 1.5.2 emits 24 distinct scalar Arrow types across 72 SQL type names,
plus six type constructors. Measured on 2026-09-07 by casting `NULL` to every
name `duckdb_types()` reports and reading the Arrow schema back.

The 24 scalars collapse into 23 keys, because `DECIMAL(p, s)` is a family:

| Arrow key | DuckDB SQL types that produce it |
|---|---|
| `bool` | `BOOLEAN`, `LOGICAL` |
| `int8` | `TINYINT` |
| `int16` | `SMALLINT` |
| `int32` | `INTEGER` |
| `int64` | `BIGINT`, `OID` |
| `uint8` | `UTINYINT` |
| `uint16` | `USMALLINT` |
| `uint32` | `UINTEGER` |
| `uint64` | `UBIGINT` |
| `float` | `REAL` |
| `double` | `DOUBLE` |
| `decimal128(*, *)` | `DECIMAL(p, s)`, `NUMERIC`, `HUGEINT`, `UHUGEINT` |
| `string` | `VARCHAR`, `JSON`, `UUID`, `CHAR` |
| `binary` | `BLOB`, `BIT`, `VARINT`, `GEOMETRY` |
| `date32[day]` | `DATE` |
| `time64[us]` | `TIME`, `TIMETZ` |
| `time64[ns]` | `TIME_NS` |
| `timestamp[s]` | `TIMESTAMP_S` |
| `timestamp[ms]` | `TIMESTAMP_MS` |
| `timestamp[us]` | `TIMESTAMP`, `DATETIME` |
| `timestamp[ns]` | `TIMESTAMP_NS` |
| `timestamp[us, tz=*]` | `TIMESTAMPTZ` |
| `month_day_nano_interval` | `INTERVAL` |

And six constructors:

| Arrow key | DuckDB SQL type |
|---|---|
| `list<T>` | `T[]` |
| `fixed_size_list<T>[n]` | `T[n]` |
| `struct<...>` | `STRUCT(...)` |
| `map<K, V>` | `MAP(K, V)` |
| `sparse_union<...>` | `UNION(...)` |
| `dictionary<values=string, indices=uint8>` | `ENUM` |

Twenty-nine keys at depth 1: 23 scalars and 6 constructors. The spec of
2026-09-06 recorded 26; that count predates the measurement and this file
replaces it.

`HUGEINT` maps to `decimal128(38, 0)`, not to an integer type. DuckDB promotes
integer overflow to `HUGEINT`, so a user reaches this key by accident.

### The timezone key is host dependent

`TIMESTAMPTZ` writes the *session* zone into the Arrow type string:

```
TZ=UTC              -> timestamp[us, tz=UTC]
TZ=America/New_York -> timestamp[us, tz=America/New_York]
TZ=Asia/Tokyo       -> timestamp[us, tz=Asia/Tokyo]
```

So `tz=*` is required, not cosmetic, and the runner pins the session zone to a
non-UTC value. A runner that leaves the zone unset proves nothing about zone
leakage on a laptop that is already UTC.

## Depth

Depth is bounded by the code's recursion, not by the type space.

ClickHouse nests through one pair of functions. `arrowListValue` recurses into
`arrowValue` at `internal/sinks/clickhouse.go:423`, and `goSliceType` and
`goElemType` recurse into each other at `clickhouse.go:442-449`. Depth 4 re-runs
the code depth 3 already ran, so the table stops at depth 3.

Three tiers:

1. **Every lattice key at depth 1.** Twenty-nine rows.
2. **`list<T>` for every `T` that is exact at depth 1**, plus `list<T>` for two
   `T` that are unsupported at depth 1. The second half is the point: it proves
   a container does not launder an unsupported element, which is what
   `type.nested` means by "never silently flattened".
3. **Two depth-3 witnesses.** `list<list<int64>>` and `list<struct<a int32>>`.

An unsupported constructor gets one row, at depth 1. A `struct` fails on the
outermost type, so struct-of-struct proves nothing further. Unsupported has no
depth.

Nulls are a second value on existing rows, not new rows. Three positions:

- a null scalar,
- a null list,
- a null element inside a non-null list.

The third is unmeasured today and the published page does not mention it.

That is roughly 55 declared rows, each written and read back once.

## Design

### `docs/coverage/lattice.yml`

New file. The closed set of Arrow keys, stated once and shared by every
integration:

```yaml
lattice:
  - key: int64
    duckdb: [BIGINT, OID]
    value: 9223372036854775807
    depth: 1

  - key: "timestamp[us, tz=*]"
    duckdb: [TIMESTAMPTZ]
    value: "2026-09-07T12:00:00Z"
    session_tz: Asia/Tokyo
    depth: 1

  - key: "list<int64>"
    duckdb: ["BIGINT[]"]
    element: int64
    depth: 2
```

The lattice is what makes a gap visible. Without it, a type nobody thought of is
invisible, which is the failure `integrations.yml` names as the Iceberg failure:
nothing written down, so nothing can be missing.

### `types:` in `integrations.yml`

Each integration declares an outcome per lattice key, and the destination column
types that accept it:

```yaml
  - id: sink.clickhouse
    kind: sink
    implements: [Sink, Prober]
    feature: sink.clickhouse
    exempt: []
    types:
      int64:
        outcome: exact
        columns: [Int64, Nullable(Int64)]
      string:
        outcome: exact
        columns: [String, LowCardinality(String), FixedString(36), Enum8('a' = 1)]
      "timestamp[us, tz=*]":
        outcome: coerced
        rule: stored as the same UTC instant; the column's own zone governs rendering
        columns: [DateTime, DateTime64(3)]
      "decimal128(*, *)":
        outcome: unsupported
        code: user.sink.type_unsupported
      "struct<...>":
        outcome: unsupported
        code: user.sink.type_unsupported
    nulls:
      default:
        outcome: coerced
        rule: the column type's zero value; the DEFAULT expression is not evaluated
      "Nullable(*)":
        outcome: exact
      "list element":
        outcome: coerced
        rule: the element type's zero value
```

A key takes a *list* of column types. That is how `string` reproduces the
page's "`String`, `LowCardinality(String)`, `Enum`, `FixedString`" row with no
new machinery: every listed column type is exercised, and the cell is covered
only when all of them pass.

The declaration is written by hand and the test judges it. It is not generated
from the sink. A table derived from the sink can never report that the sink is
missing a type, and "the sink is missing `decimal128`" is exactly what this must
report.

### `internal/conformance/types.go`

`conformance.Types(t, TypeSubject{...})`, beside `conformance.Sinks`. The
subject supplies four things:

```go
type TypeSubject struct {
	// Integration is the integrations.yml id.
	Integration string

	// CreateColumn creates a destination holding one column of the given
	// destination type, and returns a handle to it.
	CreateColumn func(t *testing.T, columnType string) Destination

	// New builds the sink writing to that destination.
	New func(t *testing.T, dest Destination) core.Sink

	// ReadBack returns the single value the destination holds.
	ReadBack func(t *testing.T, dest Destination) any
}
```

Per declared row the runner builds a one-row Arrow table of that key, flushes,
reads back, and judges the declared outcome. It emits one marker per row:

```
COVERS invariant=type.roundtrip integration=sink.clickhouse
```

**One destination per row, not one wide table.** `type.undeclared.fails_loud`
requires the *batch* to fail. A shared table would let one unsupported column
destroy every other row's evidence, and one type's defect would be attributed
to every type in the table.

### The generator

`scripts/coverage_matrix.py` gains one check and one rule.

The check: every declared type key exists in the lattice, and every lattice key
is declared by every non-exempt sink or reported as a gap. A key outside the
lattice is a typo, and a typo must not reach the matrix as a missing cell.

The rule: a type invariant's cell is covered only when every declared row
passed. One failing row fails the cell. Six invariants times one integration is
six cells, backed by roughly 55 rows.

### The rendered page

A renderer writes the mdx fragment for [#117740][pr] from the same declaration
the test judges. The page's table is keyed by DuckDB SQL type, and the framework
is keyed by Arrow type, so the renderer joins `lattice.yml`'s `duckdb` field
against the integration's `types:` block and collapses the middle. The published
table and the proven table cannot then disagree.

## Defects this surfaces

Read from the code, not yet run. The runner confirms or corrects each.

**`type.undeclared.fails_loud` fails today.** `appendTables`' error returns raw
at `clickhouse.go:221`, so an unsupported Arrow type produces
`fmt.Errorf("unsupported arrow type %s")` with no code. The invariant requires a
coded error. `internal/errs/registry.go` has no sink-side code for this:
`CodeSQLTypeUnsupported` is handler-side, and the `sink.clickhouse.type_unsupported`
the earlier spec used is not a real code. This work adds
`CodeSinkTypeUnsupported Code = "user.sink.type_unsupported"` and wraps the
conversion error with it.

**A null inside a list is silently coerced.** `clickhouse.go:429-431` appends
the element type's zero value, so `[1, null, 3]` stores `[1, 0, 3]`. No error,
no log line. The page's `NULL` row covers null columns and null lists, not this
position.

**Eight lattice keys fail the batch and the page names two of them.**
`decimal128`, `month_day_nano_interval`, `time64[us]`, `time64[ns]`,
`dictionary`, `map`, `sparse_union` and `fixed_size_list` all fall to
`arrowValue`'s default at `clickhouse.go:404`. The page's Known limits section
names `Map` and `Tuple`. `HUGEINT` is the one a user hits without asking for it.

## Level and CI

The type table runs at integration level, as
`TestIntegrationSinkClickhouse_Types`. It starts the pinned
`clickhouse/clickhouse-server:26.8` container and fails if it cannot. It never
skips, for the reason `features.yml` gives: a test that skips wherever the
service is absent runs on laptops and nowhere else.

`requires:` on the six type invariants stays empty in this work. The matrix
reports and nothing fails. Enforcement is legal only once every non-exempt sink
has a type table, and this design gives one sink a table.

## Not in this design

- MotherDuck, Kafka, Iceberg, console, sqlcommand and parquet type tables. Each
  is a second `types:` block and a second subject. This design exists to make
  the second one cheap.
- The handler input boundary. JSON shape to Arrow is a declared table too, with
  JSON-shape keys, and it is a separate column.
- Fixing the defects listed above beyond `type.undeclared.fails_loud`. The
  runner's job is to state what the sink does. Changing what it does is a
  separate decision per row, taken once the row is measured.
- Property-based or fuzzed values. One canonical value and one null per row.

## Sequence

**PR 1: the lattice and the runner, on the scalar tier.**
`lattice.yml` with the 29 keys, `types:` for `sink.clickhouse` at depth 1,
`conformance.Types`, the generator's lattice check, and
`CodeSinkTypeUnsupported`. `type.roundtrip`, `type.null`,
`type.string.fidelity` and `type.undeclared.fails_loud` get cells.

**PR 2: nesting.** Depth 2 and the two depth-3 witnesses, including the
unsupported-element rows. `type.nested` gets its cell. The null-inside-a-list
coercion is declared here, as measured.

**PR 3: timestamps and the rendered page.** The session-zone pin, the four
timestamp units and the `tz=*` row, so `type.timestamp.instant` gets its cell.
Then the renderer, and the generated fragment replaces the hand-written table in
[#117740][pr].
