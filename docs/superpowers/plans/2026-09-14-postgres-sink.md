# Keyed Postgres Sink Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A `type: postgres` sink that writes a batch to a Postgres table over pgx, with `mode: upsert | append` and a declared `key`, at the cost of the batch rather than the table.

**Architecture:** The sink buffers Arrow tables and delivers them one at a time, each in its own transaction: `COPY` into a session temp table, then a server-side `INSERT ... ON CONFLICT` merge. It never touches DuckDB. Errors classify from SQLSTATE into the existing `errs` codes so the retry ladder, the exit codes, and the drain deadline work as they do for ClickHouse. The conformance harness gains one invariant, `sink.flush.idempotent_on_key`, and the validate command gains three rules.

**Tech Stack:** Go 1.25, `github.com/jackc/pgx/v5` v5.11.0, `github.com/testcontainers/testcontainers-go/modules/postgres` v0.44.0, Postgres 16 in the integration tests.

**Spec:** `docs/superpowers/specs/2026-09-14-postgres-sink-design.md`. Read it first. Every task below argues from it.

## Global Constraints

- The branch is `bench/component-leak-loops`, rebased on `origin/main` at 161aeb2. Work in the worktree `.claude/worktrees/bench-components`. Never touch the user's main checkout.
- Every Go test carries `coverage.Covers(t, "<feature>")` as its first statement (`sink.postgres`, `validate.schema`, `tooling.conformance`, or the sink the test is about). A test that skips covers nothing, so integration tests skip only under `testing.Short()`.
- Integration tests are named `TestIntegration<Feature>_<Behaviour>` and start what they need with testcontainers. They never skip for a missing service.
- Error messages from constructors must not contain the substrings `not supported` or `requires a`: `internal/cli/examples_test.go` reads those as a parity failure rather than a missing resource.
- Prose in comments, commits, the CHANGELOG, and YAML follows Google Technical Writing One. No em dashes. No attribution lines in commits.
- `gofmt` and `go vet ./...` clean after every task. `go test -short -race ./...` green at every commit.
- Commit after every task with the message given. Do not push until the last task says so.
- Do not run `make release-image`, do not tag, and do not edit files under `docs/coverage/status/`; CI regenerates those.

## Deviations from the spec, decided here

1. **A type the sink has no conversion for fails with `user.sink.type_unsupported`, not `user.sink.encode_failed`.** The spec's type table says `encode_failed` for `time32`, `time64`, `duration`, `interval` and `dictionary`. `user.sink.type_unsupported` is the code the ClickHouse sink and its integration file already use for exactly that case, and `encode_failed` is reserved by #233 for a value the client or server refused. Both are user class, exit 10, never retried. The integration file declares `code: user.sink.type_unsupported` on those rows.
2. **A client-side encode failure from pgx is `user.sink.encode_failed`.** The spec's SQLSTATE table ends with "anything else is `system.sink.write_failed`", but a `CopyFrom` error that is neither a `*pgconn.PgError` nor a network error is pgx refusing to encode a Go value for the column's type, which fails identically every attempt. Section 4 of the spec is about server answers; this is the client.
3. **The `append` startup warning needs a logger the sink builder does not have.** `sinks.New` gains `WithLogger`, and a sink can implement `Warner` (`Warnings() []string`) which `New` logs after the probe. `run` passes its logger at the three build sites.
4. **The conformance harness detects a keyed sink through an interface, not a subject field.** `core.KeyedSink` (`Key() []string`) is implemented by the Postgres sink. The harness reads it from the raw sink `New` returns, and the six exemptions are proven by structural tests that the other sinks do not implement it. That is the same shape as the existing `ImplementsNoProber` proofs.

## File structure

| File | Responsibility |
| --- | --- |
| `internal/config/config.go` | `PostgresSink` config block on `Sink` |
| `internal/config/defaults.go` | `SinkRetries` includes `postgres` |
| `internal/core/turbine.go` | `KeyedSink` interface |
| `internal/sinks/postgres.go` | The sink: construction, buffering, per-batch transaction, probe, warnings, close |
| `internal/sinks/postgres_sql.go` | Pure SQL builders: table name parsing, staging DDL, merge statement, catalog checks |
| `internal/sinks/postgres_types.go` | Arrow cell to pgx value, JSON for containers |
| `internal/sinks/postgres_errors.go` | SQLSTATE classification into `errs` codes |
| `internal/sinks/postgres_test.go` | Unit tests, no server |
| `internal/sinks/postgres_integration_test.go` | Behaviour against a real Postgres: retry with a shared key, last row wins, defaults, probe checks, exit classification |
| `internal/sinks/conformance_postgres_test.go` | The resilience harness and the type runner against Postgres |
| `internal/sinks/init.go` | Builder entry, `WithLogger`, `Warner` logging, coded probe errors pass through |
| `internal/sinks/structural_test.go` | `IsNotKeyed` proofs for the six other sinks |
| `internal/conformance/conformance.go` | `sink.flush.idempotent_on_key` step |
| `internal/validate/sinks.go`, `sinks_test.go` | The three validate rules and the config-shape rules |
| `internal/validate/window.go` | `appendsOnly` reads the sink block |
| `internal/cli/run/root.go`, `managers.go` | Pass the logger to `sinks.New` |
| `docs/coverage/invariants.yml`, `features.yml`, `integrations/sink.postgres.yml`, six other integration files | Registry |
| `dev/config/examples/bluesky/bluesky.postgres.windowed.yml`, `kafka.postgres.sink.yml` | Examples on the new sink |
| `internal/managers/window_leakloop_test.go` | The demo's window through the new sink |
| `README.md`, `CHANGELOG.md` | Sink retries table, Unreleased notes |

---

### Task 1: Dependencies and the config block

**Files:**
- Modify: `go.mod`, `go.sum`
- Modify: `internal/config/config.go` (the `Sink` struct at line 85 and the sink config types above it)
- Modify: `internal/config/defaults.go:29-36`
- Modify: `README.md:662-667`
- Create: `internal/config/postgres_sink_test.go`

**Interfaces:**
- Produces: `config.PostgresSink{DSN, Table, Mode string; Key []string}` and `config.Sink.Postgres *PostgresSink`. `config.SinkRetries("postgres") == true`.

- [ ] **Step 1: Add the dependencies**

```bash
cd /Users/danielmican/code/github.com/turbolytics/sql-flow/.claude/worktrees/bench-components
go get github.com/jackc/pgx/v5@v5.11.0
go get github.com/testcontainers/testcontainers-go/modules/postgres@v0.44.0
go mod tidy
```

Expected: `go.mod` lists both. `go build ./...` still passes.

- [ ] **Step 2: Write the failing config test**

`internal/config/postgres_sink_test.go`:

```go
package config

import (
	"testing"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
	"gopkg.in/yaml.v3"
)

// The postgres block decodes with the four fields the spec names, and the
// sink is one the retry ladder wraps: it crosses a network to somebody
// else's server.
func TestSinkPostgres_ConfigBlockDecodes(t *testing.T) {
	coverage.Covers(t, "sink.postgres")
	var s Sink
	assert.NoError(t, yaml.Unmarshal([]byte(`
type: postgres
postgres:
  dsn: postgres://u:p@localhost:5432/db
  table: public.rollups
  mode: upsert
  key: [bucket, lang]
`), &s))
	assert.Equal(t, "postgres", s.Type)
	assert.Equal(t, "postgres://u:p@localhost:5432/db", s.Postgres.DSN)
	assert.Equal(t, "public.rollups", s.Postgres.Table)
	assert.Equal(t, "upsert", s.Postgres.Mode)
	assert.DeepEqual(t, []string{"bucket", "lang"}, s.Postgres.Key)
	assert.That(t, SinkRetries("postgres"))
}
```

- [ ] **Step 3: Run it to see it fail**

Run: `go test ./internal/config -run TestSinkPostgres_ConfigBlockDecodes`
Expected: FAIL, `s.Postgres undefined`.

- [ ] **Step 4: Add the block and the retry entry**

In `internal/config/config.go`, after `type ClickhouseSink struct { ... }`:

```go
// PostgresSink writes result rows to a Postgres table over a native client,
// at the cost of the batch: a COPY into a staging table and a server-side
// merge, in one transaction per batch.
type PostgresSink struct {
	// A libpq connection URI or key-value string.
	DSN string `yaml:"dsn"`
	// The target table, optionally schema-qualified. The sink never creates
	// it.
	Table string `yaml:"table"`
	// upsert replaces the row a key already identifies; append inserts every
	// row. Required: the two are different promises to the reader.
	Mode string `yaml:"mode" jsonschema:"enum=upsert,enum=append"`
	// The columns a row is identified by. Required for upsert, refused for
	// append. A unique index or constraint must cover exactly these columns.
	Key []string `yaml:"key,omitempty"`
}
```

In the `Sink` struct, after the `Clickhouse` field:

```go
	// Postgres-specific sink configuration.
	Postgres *PostgresSink `yaml:"postgres,omitempty"`
```

In `internal/config/defaults.go`, change the case to `case "clickhouse", "iceberg", "postgres":` and add to the comment above it: `The Postgres sink writes over pgx to a server of its own, so a refused connection there is a blip like any other.`

In `README.md` line 664, change `ClickHouse and Iceberg flushes retry` to `ClickHouse, Iceberg and Postgres flushes retry`.

- [ ] **Step 5: Run the test and the package**

Run: `go test ./internal/config`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add go.mod go.sum internal/config/config.go internal/config/defaults.go internal/config/postgres_sink_test.go README.md
git commit -m "config: a postgres sink block with table, mode and key, wrapped in the retry ladder"
```

---

### Task 2: The SQL builders

**Files:**
- Create: `internal/sinks/postgres_sql.go`
- Create: `internal/sinks/postgres_test.go`

**Interfaces:**
- Produces:
  - `const PostgresModeUpsert = "upsert"`, `PostgresModeAppend = "append"`, `postgresStaging = "sqlflow_staging"`, `postgresSeq = "__seq"`
  - `func parsePostgresTable(name string) (pgx.Identifier, error)`
  - `func postgresStagingSQL(target pgx.Identifier, cols []string) []string`
  - `func postgresMergeSQL(target pgx.Identifier, mode string, key, cols []string) string`
  - `func quoteColumns(cols []string) string`
  - `const postgresUniqueIndexSQL`, `postgresAnyUniqueIndexSQL`, `postgresColumnsSQL`, `postgresTableExistsSQL string`

- [ ] **Step 1: Write the failing tests**

`internal/sinks/postgres_test.go`:

```go
package sinks

import (
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

func TestSinkPostgres_TableNameParses(t *testing.T) {
	coverage.Covers(t, "sink.postgres")
	id, err := parsePostgresTable("rollups")
	assert.NoError(t, err)
	assert.DeepEqual(t, pgx.Identifier{"rollups"}, id)

	id, err = parsePostgresTable("analytics.rollups")
	assert.NoError(t, err)
	assert.DeepEqual(t, pgx.Identifier{"analytics", "rollups"}, id)

	for _, bad := range []string{"", "a.b.c", ".t", "t.", `pub"lic.t`} {
		_, err := parsePostgresTable(bad)
		assert.Error(t, err)
	}
}

// The staging table has the batch's columns with the target's types and
// nothing else, plus the sequence the merge orders by.
func TestSinkPostgres_StagingDDLTakesTheTargetsTypes(t *testing.T) {
	coverage.Covers(t, "sink.postgres")
	got := postgresStagingSQL(pgx.Identifier{"public", "t"}, []string{"bucket", "lang", "posts"})
	assert.DeepEqual(t, []string{
		`DROP TABLE IF EXISTS sqlflow_staging`,
		`CREATE TEMP TABLE sqlflow_staging ON COMMIT DELETE ROWS AS SELECT "bucket", "lang", "posts" FROM "public"."t" WITH NO DATA`,
		`ALTER TABLE sqlflow_staging ADD COLUMN __seq bigint`,
	}, got)
}

// upsert names the batch's columns and no others, resolves two rows with one
// key to the last one in the batch, and updates every non-key column.
func TestSinkPostgres_UpsertMergeIsLastRowWins(t *testing.T) {
	coverage.Covers(t, "sink.postgres")
	got := postgresMergeSQL(pgx.Identifier{"t"}, PostgresModeUpsert,
		[]string{"bucket", "lang"}, []string{"bucket", "lang", "posts", "updated_at"})
	assert.Equal(t, `INSERT INTO "t" ("bucket", "lang", "posts", "updated_at") `+
		`SELECT DISTINCT ON ("bucket", "lang") "bucket", "lang", "posts", "updated_at" FROM sqlflow_staging `+
		`ORDER BY "bucket", "lang", __seq DESC `+
		`ON CONFLICT ("bucket", "lang") DO UPDATE SET "posts" = EXCLUDED."posts", "updated_at" = EXCLUDED."updated_at"`, got)
}

// A batch whose only columns are the key has nothing to update.
func TestSinkPostgres_UpsertOfKeyOnlyDoesNothingOnConflict(t *testing.T) {
	coverage.Covers(t, "sink.postgres")
	got := postgresMergeSQL(pgx.Identifier{"t"}, PostgresModeUpsert, []string{"id"}, []string{"id"})
	assert.That(t, strings.HasSuffix(got, `ON CONFLICT ("id") DO NOTHING`))
}

func TestSinkPostgres_AppendMergeKeepsBatchOrder(t *testing.T) {
	coverage.Covers(t, "sink.postgres")
	got := postgresMergeSQL(pgx.Identifier{"s", "t"}, PostgresModeAppend, nil, []string{"a", "b"})
	assert.Equal(t, `INSERT INTO "s"."t" ("a", "b") SELECT "a", "b" FROM sqlflow_staging ORDER BY __seq`, got)
}
```

- [ ] **Step 2: Run them to see them fail**

Run: `go test ./internal/sinks -run 'TestSinkPostgres_' 2>&1 | head -5`
Expected: build failure, `undefined: parsePostgresTable`.

- [ ] **Step 3: Write the builders**

`internal/sinks/postgres_sql.go`:

```go
package sinks

import (
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/turbolytics/sql-flow/internal/errs"
)

// The Postgres sink's SQL. Pure functions of the config and the batch's
// columns, so the statements are tested without a server.

const (
	PostgresModeUpsert = "upsert"
	PostgresModeAppend = "append"

	// postgresStaging is the session temp table every flush copies into.
	// Temp tables live in a per-session schema, so two sinks on two
	// connections never collide, and the table dies with the connection.
	postgresStaging = "sqlflow_staging"
	// postgresSeq is the row's position in the batch, which is what "last
	// row wins" orders by.
	postgresSeq = "__seq"
)

// parsePostgresTable reads "table" or "schema.table" into a quoted
// identifier. Anything else is refused: a name with three parts or a quote
// in it is a config error, not something to pass to the server and see.
func parsePostgresTable(name string) (pgx.Identifier, error) {
	parts := strings.Split(name, ".")
	if len(parts) > 2 {
		return nil, errs.New(errs.CodeSinkInvalid, "postgres sink: table %q has more than one dot; use table or schema.table", name)
	}
	for _, p := range parts {
		if p == "" || strings.ContainsAny(p, `"`) {
			return nil, errs.New(errs.CodeSinkInvalid, "postgres sink: table %q is not a table name; use table or schema.table, unquoted", name)
		}
	}
	return pgx.Identifier(parts), nil
}

// quoteColumns renders a column list for a statement.
func quoteColumns(cols []string) string {
	quoted := make([]string, len(cols))
	for i, c := range cols {
		quoted[i] = pgx.Identifier{c}.Sanitize()
	}
	return strings.Join(quoted, ", ")
}

// postgresStagingSQL creates the staging table for one column set. It has
// exactly the batch's columns with the target's types and none of the
// target's constraints or defaults, so the merge is the only place a row is
// judged and the error names the target. ON COMMIT DELETE ROWS empties it at
// every commit.
func postgresStagingSQL(target pgx.Identifier, cols []string) []string {
	return []string{
		"DROP TABLE IF EXISTS " + postgresStaging,
		fmt.Sprintf("CREATE TEMP TABLE %s ON COMMIT DELETE ROWS AS SELECT %s FROM %s WITH NO DATA",
			postgresStaging, quoteColumns(cols), target.Sanitize()),
		fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s bigint", postgresStaging, postgresSeq),
	}
}

// postgresMergeSQL moves the staging table into the target.
//
// upsert resolves two rows with one key to the last one in the batch before
// the INSERT sees them, because ON CONFLICT DO UPDATE refuses to touch a row
// twice in one statement (21000), and a CDC batch carrying two updates for
// one id is ordinary. The INSERT names the batch's columns and no others, so
// a column the batch omits takes the table's default on insert and keeps its
// value on update.
func postgresMergeSQL(target pgx.Identifier, mode string, key, cols []string) string {
	list := quoteColumns(cols)
	if mode == PostgresModeAppend {
		return fmt.Sprintf("INSERT INTO %s (%s) SELECT %s FROM %s ORDER BY %s",
			target.Sanitize(), list, list, postgresStaging, postgresSeq)
	}
	keyList := quoteColumns(key)
	var sets []string
	for _, c := range cols {
		if containsString(key, c) {
			continue
		}
		q := pgx.Identifier{c}.Sanitize()
		sets = append(sets, q+" = EXCLUDED."+q)
	}
	action := "DO NOTHING"
	if len(sets) > 0 {
		action = "DO UPDATE SET " + strings.Join(sets, ", ")
	}
	return fmt.Sprintf("INSERT INTO %s (%s) SELECT DISTINCT ON (%s) %s FROM %s ORDER BY %s, %s DESC ON CONFLICT (%s) %s",
		target.Sanitize(), list, keyList, list, postgresStaging, keyList, postgresSeq, keyList, action)
}

func containsString(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

// The catalog queries the probe runs. $1 is the target as a regclass text,
// e.g. "public"."t".
const (
	postgresTableExistsSQL = `SELECT to_regclass($1::text) IS NOT NULL`

	postgresColumnsSQL = `SELECT attname::text FROM pg_attribute
WHERE attrelid = $1::text::regclass AND attnum > 0 AND NOT attisdropped ORDER BY attnum`

	// A unique index or constraint whose columns are exactly $2, in any
	// order. A partial index (indpred) or an expression index (indexprs) is
	// not a conflict target for ON CONFLICT (<columns>), so neither counts.
	postgresUniqueIndexSQL = `SELECT EXISTS (
  SELECT 1 FROM pg_index i
  WHERE i.indrelid = $1::text::regclass AND i.indisunique
    AND i.indpred IS NULL AND i.indexprs IS NULL
    AND (SELECT array_agg(a.attname::text ORDER BY a.attname)
         FROM unnest(i.indkey) WITH ORDINALITY AS k(attnum, ord)
         JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum) = $2::text[])`

	postgresAnyUniqueIndexSQL = `SELECT EXISTS (
  SELECT 1 FROM pg_index WHERE indrelid = $1::text::regclass AND indisunique)`
)
```

- [ ] **Step 4: Run the tests**

Run: `go test ./internal/sinks -run 'TestSinkPostgres_'`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/sinks/postgres_sql.go internal/sinks/postgres_test.go
git commit -m "sinks: the Postgres sink's staging DDL, merge statement and catalog queries"
```

---

### Task 3: Arrow values for pgx

**Files:**
- Create: `internal/sinks/postgres_types.go`
- Modify: `internal/sinks/postgres_test.go`

**Interfaces:**
- Produces: `func postgresRows(tbl arrow.Table) ([][]any, error)` returning one `[]any` per row with the batch's columns then the sequence; `func postgresValue(arr arrow.Array, i int) (any, error)`.

- [ ] **Step 1: Write the failing tests**

Append to `internal/sinks/postgres_test.go`:

```go
// Every scalar the lattice names has a pgx value, nulls are nil, and a type
// with no conversion fails with the code the operator can act on.
func TestSinkPostgres_ScalarsConvert(t *testing.T) {
	coverage.Covers(t, "sink.postgres")
	mem := memory.NewGoAllocator()

	i8 := array.NewInt8Builder(mem)
	i8.Append(-128)
	i8.AppendNull()
	arr := i8.NewArray()
	defer arr.Release()
	v, err := postgresValue(arr, 0)
	assert.NoError(t, err)
	assert.Equal(t, int16(-128), v)
	v, err = postgresValue(arr, 1)
	assert.NoError(t, err)
	assert.Nil(t, v)

	u64 := array.NewUint64Builder(mem)
	u64.Append(7)
	u64.Append(18446744073709551615)
	uarr := u64.NewArray()
	defer uarr.Release()
	v, err = postgresValue(uarr, 0)
	assert.NoError(t, err)
	assert.Equal(t, int64(7), v)
	v, err = postgresValue(uarr, 1)
	assert.NoError(t, err)
	n, ok := v.(pgtype.Numeric)
	assert.That(t, ok)
	text, err := n.Value()
	assert.NoError(t, err)
	assert.Equal(t, "18446744073709551615", text)

	ts := array.NewTimestampBuilder(mem, &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "Asia/Tokyo"})
	ts.Append(arrow.Timestamp(time.Date(2026, 9, 8, 12, 0, 0, 123456000, time.UTC).UnixMicro()))
	tarr := ts.NewArray()
	defer tarr.Release()
	v, err = postgresValue(tarr, 0)
	assert.NoError(t, err)
	assert.Equal(t, time.Date(2026, 9, 8, 12, 0, 0, 123456000, time.UTC), v.(time.Time).UTC())

	d := array.NewDecimal128Builder(mem, &arrow.Decimal128Type{Precision: 10, Scale: 2})
	d.Append(decimal128.FromI64(123456))
	darr := d.NewArray()
	defer darr.Release()
	v, err = postgresValue(darr, 0)
	assert.NoError(t, err)
	text, err = v.(pgtype.Numeric).Value()
	assert.NoError(t, err)
	assert.Equal(t, "1234.56", text)
}

func TestSinkPostgres_UnsupportedTypeCarriesACode(t *testing.T) {
	coverage.Covers(t, "sink.postgres")
	b := array.NewTime64Builder(memory.NewGoAllocator(), &arrow.Time64Type{Unit: arrow.Microsecond})
	b.AppendNull()
	arr := b.NewArray()
	defer arr.Release()
	// A null of an unsupported type still fails: an all-null column is the
	// ordinary shape of a field the producer stopped sending.
	_, err := postgresValue(arr, 0)
	assert.Error(t, err)
	assert.Equal(t, errs.CodeSinkTypeUnsupported, errs.CodeOf(err))
}

// Containers go as JSON text rendered from the Arrow array, and an element
// the sink cannot convert fails from inside the container as it would alone.
func TestSinkPostgres_ContainersRenderAsJSON(t *testing.T) {
	coverage.Covers(t, "sink.postgres")
	mem := memory.NewGoAllocator()
	lb := array.NewListBuilder(mem, arrow.PrimitiveTypes.Int64)
	lb.Append(true)
	vb := lb.ValueBuilder().(*array.Int64Builder)
	vb.Append(1)
	vb.AppendNull()
	vb.Append(3)
	arr := lb.NewArray()
	defer arr.Release()
	v, err := postgresValue(arr, 0)
	assert.NoError(t, err)
	assert.Equal(t, "[1,null,3]", v)

	sb := array.NewStructBuilder(mem, arrow.StructOf(arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Int32, Nullable: true}))
	sb.Append(true)
	sb.FieldBuilder(0).(*array.Int32Builder).Append(7)
	sarr := sb.NewArray()
	defer sarr.Release()
	v, err = postgresValue(sarr, 0)
	assert.NoError(t, err)
	assert.Equal(t, `{"a":7}`, v)

	db := array.NewListBuilder(mem, &arrow.Decimal128Type{Precision: 38, Scale: 0})
	db.Append(true)
	darr := db.NewArray()
	defer darr.Release()
	_, err = postgresValue(darr, 0)
	assert.Error(t, err)
	assert.Equal(t, errs.CodeSinkTypeUnsupported, errs.CodeOf(err))
}

// postgresRows appends the row's position in the batch, which the merge
// orders by, and names the column in a conversion failure.
func TestSinkPostgres_RowsCarryTheSequence(t *testing.T) {
	coverage.Covers(t, "sink.postgres")
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{{Name: "k", Type: arrow.PrimitiveTypes.Int64}}, nil)
	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()
	b.Field(0).(*array.Int64Builder).AppendValues([]int64{10, 20}, nil)
	rec := b.NewRecord()
	defer rec.Release()
	tbl := array.NewTableFromRecords(schema, []arrow.Record{rec})
	defer tbl.Release()

	rows, err := postgresRows(tbl)
	assert.NoError(t, err)
	assert.DeepEqual(t, [][]any{{int64(10), int64(0)}, {int64(20), int64(1)}}, rows)

	bad := arrow.NewSchema([]arrow.Field{{Name: "when", Type: &arrow.Time64Type{Unit: arrow.Microsecond}}}, nil)
	bb := array.NewRecordBuilder(mem, bad)
	defer bb.Release()
	bb.Field(0).(*array.Time64Builder).Append(1)
	brec := bb.NewRecord()
	defer brec.Release()
	btbl := array.NewTableFromRecords(bad, []arrow.Record{brec})
	defer btbl.Release()
	_, err = postgresRows(btbl)
	assert.Error(t, err)
	assert.That(t, strings.Contains(err.Error(), `column "when"`))
}
```

Add to the test file's imports: `"time"`, `"github.com/apache/arrow-go/v18/arrow"`, `"github.com/apache/arrow-go/v18/arrow/array"`, `"github.com/apache/arrow-go/v18/arrow/decimal128"`, `"github.com/apache/arrow-go/v18/arrow/memory"`, `"github.com/jackc/pgx/v5/pgtype"`, `"github.com/turbolytics/sql-flow/internal/errs"`.

- [ ] **Step 2: Run them to see them fail**

Run: `go test ./internal/sinks -run 'TestSinkPostgres_' 2>&1 | head -5`
Expected: build failure, `undefined: postgresValue`.

- [ ] **Step 3: Write the conversion**

`internal/sinks/postgres_types.go`:

```go
package sinks

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/turbolytics/sql-flow/internal/errs"
)

// Arrow to pgx. pgx encodes each Go value for the staging column's own type,
// which it reads from the server, so a BIGINT column takes an int32 value and
// a TIMESTAMPTZ column takes a time.Time as the instant. The sink's job is to
// hand it a Go value it can encode, and to refuse a type it cannot, with a
// code the operator can act on.

// postgresRows converts a whole batch before anything is sent. Memory is the
// batch, and a value the sink cannot convert fails before a transaction is
// opened. Each row ends with its position in the batch.
func postgresRows(tbl arrow.Table) ([][]any, error) {
	reader := array.NewTableReader(tbl, 0)
	defer reader.Release()

	rows := make([][]any, 0, tbl.NumRows())
	seq := int64(0)
	for reader.Next() {
		rec := reader.Record()
		for i := 0; i < int(rec.NumRows()); i++ {
			row := make([]any, rec.NumCols()+1)
			for c := 0; c < int(rec.NumCols()); c++ {
				v, err := postgresValue(rec.Column(c), i)
				if err != nil {
					return nil, errs.Wrap(errs.CodeOf(err), err, "postgres sink: column %q", rec.ColumnName(c))
				}
				row[c] = v
			}
			row[rec.NumCols()] = seq
			seq++
			rows = append(rows, row)
		}
	}
	return rows, reader.Err()
}

// postgresValue converts one cell. A null is nil, and Postgres stores it as
// NULL or refuses it under the column's own constraint. Convertibility is
// checked before absence: a null of a type the sink cannot convert fails,
// because an all-null column is the ordinary shape of a field the producer
// stopped sending, and it must fail the same way as the column with a value.
func postgresValue(arr arrow.Array, i int) (any, error) {
	switch arr.(type) {
	case *array.List, *array.LargeList, *array.FixedSizeList, *array.Struct, *array.Map:
		if err := jsonRenderable(arr.DataType()); err != nil {
			return nil, err
		}
		if arr.IsNull(i) {
			return nil, nil
		}
		// Rendered from the Arrow array itself: GetOneForMarshal is what
		// the array's own MarshalJSON uses for each element.
		b, err := json.Marshal(arr.(interface{ GetOneForMarshal(int) any }).GetOneForMarshal(i))
		if err != nil {
			return nil, errs.Wrap(errs.CodeSinkEncodeFailed, err, "rendering %s as JSON", arr.DataType())
		}
		return string(b), nil
	}

	v, err := postgresScalar(arr, i)
	if err != nil {
		return nil, err
	}
	if arr.IsNull(i) {
		return nil, nil
	}
	return v, nil
}

// jsonRenderable refuses a container holding a type the sink has no
// conversion for. A list does not launder its element.
func jsonRenderable(dt arrow.DataType) error {
	switch t := dt.(type) {
	case *arrow.ListType:
		return jsonRenderable(t.Elem())
	case *arrow.LargeListType:
		return jsonRenderable(t.Elem())
	case *arrow.FixedSizeListType:
		return jsonRenderable(t.Elem())
	case *arrow.MapType:
		if err := jsonRenderable(t.KeyType()); err != nil {
			return err
		}
		return jsonRenderable(t.ItemType())
	case *arrow.StructType:
		for _, f := range t.Fields() {
			if err := jsonRenderable(f.Type); err != nil {
				return err
			}
		}
		return nil
	case *arrow.BooleanType, *arrow.Int8Type, *arrow.Int16Type, *arrow.Int32Type, *arrow.Int64Type,
		*arrow.Uint8Type, *arrow.Uint16Type, *arrow.Uint32Type, *arrow.Uint64Type,
		*arrow.Float32Type, *arrow.Float64Type, *arrow.StringType, *arrow.LargeStringType,
		*arrow.BinaryType, *arrow.LargeBinaryType, *arrow.Date32Type, *arrow.Date64Type, *arrow.TimestampType:
		return nil
	default:
		return errs.New(errs.CodeSinkTypeUnsupported, "no conversion for arrow type %s inside a container", dt)
	}
}

func postgresScalar(arr arrow.Array, i int) (any, error) {
	switch a := arr.(type) {
	case *array.Boolean:
		return a.Value(i), nil
	case *array.Int8:
		return int16(a.Value(i)), nil
	case *array.Int16:
		return a.Value(i), nil
	case *array.Int32:
		return a.Value(i), nil
	case *array.Int64:
		return a.Value(i), nil
	case *array.Uint8:
		return int16(a.Value(i)), nil
	case *array.Uint16:
		return int32(a.Value(i)), nil
	case *array.Uint32:
		return int64(a.Value(i)), nil
	case *array.Uint64:
		v := a.Value(i)
		if v <= math.MaxInt64 {
			return int64(v), nil
		}
		return numericFromString(strconv.FormatUint(v, 10))
	case *array.Float32:
		return a.Value(i), nil
	case *array.Float64:
		return a.Value(i), nil
	case *array.String:
		return a.Value(i), nil
	case *array.LargeString:
		return a.Value(i), nil
	case *array.Binary:
		return a.Value(i), nil
	case *array.LargeBinary:
		return a.Value(i), nil
	case *array.Date32:
		return a.Value(i).ToTime(), nil
	case *array.Date64:
		return a.Value(i).ToTime(), nil
	case *array.Timestamp:
		// ToTime returns the instant in UTC for a zoned type, and the wall
		// clock read as UTC for a naive one. pgx encodes the instant into
		// timestamptz and the UTC wall clock into timestamp, which is what
		// the spec's type table promises for both.
		return a.Value(i).ToTime(a.DataType().(*arrow.TimestampType).Unit), nil
	case *array.Decimal128:
		return numericFromString(a.Value(i).ToString(a.DataType().(*arrow.Decimal128Type).Scale))
	case *array.Decimal256:
		return numericFromString(a.Value(i).ToString(a.DataType().(*arrow.Decimal256Type).Scale))
	default:
		return nil, errs.New(errs.CodeSinkTypeUnsupported, "no conversion for arrow type %s", arr.DataType())
	}
}

func numericFromString(s string) (any, error) {
	var n pgtype.Numeric
	if err := n.Scan(s); err != nil {
		return nil, errs.Wrap(errs.CodeSinkEncodeFailed, err, fmt.Sprintf("%q as numeric", s))
	}
	return n, nil
}
```

- [ ] **Step 4: Run the tests**

Run: `go test ./internal/sinks -run 'TestSinkPostgres_'`
Expected: PASS. If `a.Value(i).ToString(scale)` does not compile against arrow-go v18.6.0, read `$(go env GOMODCACHE)/github.com/apache/arrow-go/v18@v18.6.0/arrow/decimal128/decimal128.go` for the method that renders a `Num` with a scale and use it; the test pins the output `1234.56`. If `pgtype.Numeric.Value()` renders `1234.56` differently, compare `n.Int.String()` and `n.Exp` instead, and say so in the commit.

- [ ] **Step 5: Commit**

```bash
git add internal/sinks/postgres_types.go internal/sinks/postgres_test.go
git commit -m "sinks: Arrow cells to pgx values, containers as JSON rendered from the array"
```

---

### Task 4: Error classification

**Files:**
- Create: `internal/sinks/postgres_errors.go`
- Modify: `internal/sinks/postgres_test.go`

**Interfaces:**
- Produces: `func postgresError(err error, format string, args ...any) error` and `func postgresCopyError(err error) error`.

- [ ] **Step 1: Write the failing test**

Append to `internal/sinks/postgres_test.go`:

```go
// The SQLSTATE class is the whole retry policy: connection, shutdown,
// resource and serialization classes retry as unreachable; data, constraint,
// syntax, auth and missing-object classes are the user's and never retry.
func TestSinkPostgres_SQLStateClassifies(t *testing.T) {
	coverage.Covers(t, "sink.postgres")
	cases := []struct {
		state string
		code  errs.Code
		exit  int
	}{
		{"08006", errs.CodeSinkUnreachable, errs.ExitSinkUnreachable},
		{"57P01", errs.CodeSinkUnreachable, errs.ExitSinkUnreachable},
		{"53300", errs.CodeSinkUnreachable, errs.ExitSinkUnreachable},
		{"40001", errs.CodeSinkUnreachable, errs.ExitSinkUnreachable},
		{"22003", errs.CodeSinkEncodeFailed, errs.ExitUserError},
		{"23502", errs.CodeSinkInvalid, errs.ExitUserError},
		{"21000", errs.CodeSinkInvalid, errs.ExitUserError},
		{"42703", errs.CodeSinkInvalid, errs.ExitUserError},
		{"28P01", errs.CodeSinkInvalid, errs.ExitUserError},
		{"3D000", errs.CodeSinkInvalid, errs.ExitUserError},
		{"XX000", errs.CodeSinkWriteFailed, errs.ExitInternal},
	}
	for _, c := range cases {
		err := postgresError(&pgconn.PgError{Code: c.state, Message: "m"}, "merge")
		assert.Equal(t, c.code, errs.CodeOf(err))
		assert.Equal(t, c.exit, errs.ExitCode(err))
		assert.That(t, strings.Contains(err.Error(), c.state))
	}

	// A dial failure is unreachable, and a context that ran out stays
	// visible through the wrap so the harness can see the deadline.
	err := postgresError(&net.OpError{Op: "dial", Err: syscall.ECONNREFUSED}, "connect")
	assert.Equal(t, errs.CodeSinkUnreachable, errs.CodeOf(err))
	err = postgresError(context.DeadlineExceeded, "copy")
	assert.Equal(t, errs.CodeSinkUnreachable, errs.CodeOf(err))
	assert.That(t, errors.Is(err, context.DeadlineExceeded))

	// pgx refusing to encode a Go value is the client's, and permanent.
	err = postgresCopyError(errors.New("unable to encode 1.5 into binary format for int8"))
	assert.Equal(t, errs.CodeSinkEncodeFailed, errs.CodeOf(err))
	assert.That(t, !retryable(err))
}
```

Add imports: `"context"`, `"errors"`, `"net"`, `"syscall"`, `"github.com/jackc/pgx/v5/pgconn"`.

- [ ] **Step 2: Run it to see it fail**

Run: `go test ./internal/sinks -run TestSinkPostgres_SQLStateClassifies 2>&1 | head -3`
Expected: build failure, `undefined: postgresError`.

- [ ] **Step 3: Write the classifier**

`internal/sinks/postgres_errors.go`:

```go
package sinks

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/turbolytics/sql-flow/internal/errs"
)

// postgresError codes a failure by its SQLSTATE class, which is the whole
// retry policy for this sink: the ladder retries system.sink.unreachable and
// nothing user class, so the table below decides both the code and whether
// another attempt is made. No new code: a server-side data exception is the
// value #233's encode_failed describes, one hop later.
//
//	08  connection exception             unreachable   retried, exit 12
//	57  operator intervention, shutdown  unreachable   retried, exit 12
//	53  insufficient resources           unreachable   retried, exit 12
//	40  serialization, deadlock          unreachable   retried, exit 12
//	22  data exception                   encode_failed exit 10
//	21  cardinality violation            sink invalid  exit 10
//	23  integrity constraint             sink invalid  exit 10
//	42  syntax, undefined object, grant  sink invalid  exit 10
//	28  authentication                   sink invalid  exit 10
//	3D  3F  database, schema missing     sink invalid  exit 10
//	else                                 write_failed  exit 1
func postgresError(err error, format string, args ...any) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		args = append(args, pgErr.Code)
		format += " (SQLSTATE %s)"
		class := ""
		if len(pgErr.Code) >= 2 {
			class = pgErr.Code[:2]
		}
		switch class {
		case "08", "57", "53", "40":
			return errs.Wrap(errs.CodeSinkUnreachable, err, format, args...)
		case "22":
			return errs.Wrap(errs.CodeSinkEncodeFailed, err, format, args...)
		case "21", "23", "42", "28", "3D", "3F":
			return errs.Wrap(errs.CodeSinkInvalid, err, format, args...)
		default:
			return errs.Wrap(errs.CodeSinkWriteFailed, err, format, args...)
		}
	}
	if isUnreachable(err) || errors.Is(err, context.Canceled) {
		return errs.Wrap(errs.CodeSinkUnreachable, err, format, args...)
	}
	return errs.Wrap(errs.CodeSinkWriteFailed, err, format, args...)
}

// postgresCopyError codes a CopyFrom failure. A server answer or a network
// failure classifies as any other statement's. Anything else is pgx refusing
// to encode a Go value for the column's type, which fails identically on
// every attempt.
func postgresCopyError(err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) || isUnreachable(err) || errors.Is(err, context.Canceled) {
		return postgresError(err, "postgres sink: copy into staging")
	}
	return errs.Wrap(errs.CodeSinkEncodeFailed, err, "postgres sink: encode a value for its column")
}
```

- [ ] **Step 4: Run the tests**

Run: `go test ./internal/sinks -run 'TestSinkPostgres_'`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/sinks/postgres_errors.go internal/sinks/postgres_test.go
git commit -m "sinks: Postgres failures coded by SQLSTATE class, which is the whole retry policy"
```

---

### Task 5: The sink, the builder, the logger, and the probe passthrough

**Files:**
- Create: `internal/sinks/postgres.go`
- Modify: `internal/core/turbine.go` (after `BufferedRowReporter`)
- Modify: `internal/sinks/init.go` (options, `New`, `builders`)
- Modify: `internal/sinks/probe.go:41-56`
- Modify: `internal/cli/run/root.go:80-93` and its call sites, `internal/cli/run/managers.go:118-122`, and the DLQ build in `root.go:64-68`
- Modify: `internal/sinks/postgres_test.go`

**Interfaces:**
- Produces: `func NewPostgresSink(conf config.PostgresSink) (*PostgresSink, error)`; `*PostgresSink` implements `core.Sink`, `core.BufferedRowReporter`, `core.KeyedSink`, `Prober`, `Warner`, `io.Closer`. `core.KeyedSink interface { Key() []string }`. `sinks.WithLogger(*zap.Logger) Option`. `sinks.Warner interface { Warnings() []string }`.

- [ ] **Step 1: Write the failing unit tests**

Append to `internal/sinks/postgres_test.go`:

```go
func TestSinkPostgres_NewChecksTheBlock(t *testing.T) {
	coverage.Covers(t, "sink.postgres")
	good := config.PostgresSink{DSN: "postgres://u:p@localhost:5432/db", Table: "t", Mode: "upsert", Key: []string{"id"}}
	s, err := NewPostgresSink(good)
	assert.NoError(t, err)
	assert.DeepEqual(t, []string{"id"}, s.Key())

	for name, bad := range map[string]config.PostgresSink{
		"no dsn":            {Table: "t", Mode: "upsert", Key: []string{"id"}},
		"no table":          {DSN: good.DSN, Mode: "upsert", Key: []string{"id"}},
		"no mode":           {DSN: good.DSN, Table: "t", Key: []string{"id"}},
		"bad mode":          {DSN: good.DSN, Table: "t", Mode: "merge", Key: []string{"id"}},
		"upsert no key":     {DSN: good.DSN, Table: "t", Mode: "upsert"},
		"append with key":   {DSN: good.DSN, Table: "t", Mode: "append", Key: []string{"id"}},
		"duplicate key":     {DSN: good.DSN, Table: "t", Mode: "upsert", Key: []string{"id", "id"}},
		"bad dsn":           {DSN: "postgres://[::1", Table: "t", Mode: "append"},
		"three-part table":  {DSN: good.DSN, Table: "a.b.c", Mode: "append"},
	} {
		_, err := NewPostgresSink(bad)
		if err == nil {
			t.Fatalf("%s: built", name)
		}
		assert.Equal(t, errs.CodeSinkInvalid, errs.CodeOf(err))
		// The examples test reads these substrings as a parity failure.
		assert.That(t, !strings.Contains(err.Error(), "not supported"))
		assert.That(t, !strings.Contains(err.Error(), "requires a"))
	}

	a, err := NewPostgresSink(config.PostgresSink{DSN: good.DSN, Table: "t", Mode: "append"})
	assert.NoError(t, err)
	assert.Equal(t, 0, len(a.Key()))
}

// New dials nothing: a sink pointed at a host that does not resolve builds,
// and Probe is where the pipeline learns the destination is not there.
func TestSinkPostgres_NewDoesNotDial(t *testing.T) {
	coverage.Covers(t, "sink.postgres")
	s, err := NewPostgresSink(config.PostgresSink{DSN: "postgres://u:p@no-such-host.invalid:5432/db", Table: "t", Mode: "append"})
	assert.NoError(t, err)
	assert.Equal(t, 0, s.BufferedRows())
	assert.NoError(t, s.Close())
	assert.NoError(t, s.Close())
}

// WriteTable buffers and Flush of nothing is a noop, without a server.
func TestSinkPostgres_WriteTableBuffersOnly(t *testing.T) {
	coverage.Covers(t, "sink.postgres")
	s, err := NewPostgresSink(config.PostgresSink{DSN: "postgres://u:p@no-such-host.invalid:5432/db", Table: "t", Mode: "append"})
	assert.NoError(t, err)
	assert.NoError(t, s.Flush(context.Background()))

	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)
	b := array.NewRecordBuilder(memory.NewGoAllocator(), schema)
	defer b.Release()
	b.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2}, nil)
	rec := b.NewRecord()
	defer rec.Release()
	tbl := array.NewTableFromRecords(schema, []arrow.Record{rec})
	defer tbl.Release()

	assert.NoError(t, s.WriteTable(context.Background(), tbl))
	assert.Equal(t, 2, s.BufferedRows())
}

// A dead-end host fails a flush with the retryable code, and the batch stays
// buffered for the retry.
func TestSinkPostgres_FlushAgainstNoServerIsUnreachableAndKeepsTheBatch(t *testing.T) {
	coverage.Covers(t, "sink.postgres")
	s, err := NewPostgresSink(config.PostgresSink{DSN: "postgres://u:p@127.0.0.1:1/db?connect_timeout=2", Table: "t", Mode: "append"})
	assert.NoError(t, err)
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)
	b := array.NewRecordBuilder(memory.NewGoAllocator(), schema)
	defer b.Release()
	b.Field(0).(*array.Int64Builder).Append(1)
	rec := b.NewRecord()
	defer rec.Release()
	tbl := array.NewTableFromRecords(schema, []arrow.Record{rec})
	defer tbl.Release()
	assert.NoError(t, s.WriteTable(context.Background(), tbl))

	err = s.Flush(context.Background())
	assert.Error(t, err)
	assert.Equal(t, errs.CodeSinkUnreachable, errs.CodeOf(err))
	assert.Equal(t, 1, s.BufferedRows())
}
```

Add import `"github.com/turbolytics/sql-flow/internal/config"`.

- [ ] **Step 2: Run them to see them fail**

Run: `go test ./internal/sinks -run 'TestSinkPostgres_' 2>&1 | head -3`
Expected: build failure, `undefined: NewPostgresSink`.

- [ ] **Step 3: Add the interface to core**

In `internal/core/turbine.go`, after the `BufferedRowReporter` interface:

```go
// KeyedSink is implemented by a sink that identifies a row by declared key
// columns, so delivering the same batch twice leaves the destination holding
// it once. Key returns those columns, or nothing for a sink that appends.
//
// Optional, like BufferedRowReporter. The conformance harness reads it to
// decide whether sink.flush.idempotent_on_key applies.
type KeyedSink interface {
	Key() []string
}
```

- [ ] **Step 4: Write the sink**

`internal/sinks/postgres.go`:

```go
package sinks

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/jackc/pgx/v5"
	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/errs"
)

// PostgresSink writes batches to a Postgres table over pgx.
//
// One transaction per batch: a COPY into a session temp table, then a
// server-side INSERT ... ON CONFLICT. The cost is the batch, whatever the
// table holds. It touches no DuckDB connection, takes no lock, and every
// step runs on the flush's context, so a drain deadline can stop it.
//
// See docs/superpowers/specs/2026-09-14-postgres-sink-design.md.
type PostgresSink struct {
	connConfig *pgx.ConnConfig
	table      pgx.Identifier
	mode       string
	key        []string

	// mu guards the buffer.
	mu     sync.Mutex
	tables []arrow.Table

	// connMu guards the connection and the staging table, which Probe and
	// Flush both use. staging is the column set the temp table was created
	// for; nil means it does not exist on this connection.
	connMu   sync.Mutex
	conn     *pgx.Conn
	staging  []string
	warnings []string
}

func NewPostgresSink(conf config.PostgresSink) (*PostgresSink, error) {
	if strings.TrimSpace(conf.DSN) == "" {
		return nil, errs.New(errs.CodeSinkInvalid, "postgres sink: dsn is required")
	}
	if strings.TrimSpace(conf.Table) == "" {
		return nil, errs.New(errs.CodeSinkInvalid, "postgres sink: table is required")
	}
	switch conf.Mode {
	case PostgresModeUpsert:
		if len(conf.Key) == 0 {
			return nil, errs.New(errs.CodeSinkInvalid, "postgres sink: mode upsert needs key, the columns a row is identified by")
		}
	case PostgresModeAppend:
		if len(conf.Key) > 0 {
			return nil, errs.New(errs.CodeSinkInvalid, "postgres sink: mode append takes no key; every row is inserted as it is")
		}
	case "":
		return nil, errs.New(errs.CodeSinkInvalid, "postgres sink: mode is required: upsert or append")
	default:
		return nil, errs.New(errs.CodeSinkInvalid, "postgres sink: mode must be upsert or append, not %q", conf.Mode)
	}
	seen := map[string]bool{}
	for _, k := range conf.Key {
		if seen[k] {
			return nil, errs.New(errs.CodeSinkInvalid, "postgres sink: key names %q twice", k)
		}
		seen[k] = true
	}

	table, err := parsePostgresTable(conf.Table)
	if err != nil {
		return nil, err
	}
	// ParseConfig reads the DSN and the PG* environment and dials nothing.
	cc, err := pgx.ParseConfig(conf.DSN)
	if err != nil {
		return nil, errs.Wrap(errs.CodeSinkInvalid, err, "postgres sink: dsn")
	}
	return &PostgresSink{
		connConfig: cc,
		table:      table,
		mode:       conf.Mode,
		key:        append([]string(nil), conf.Key...),
	}, nil
}

// Key reports the columns a row is identified by, or nothing for append.
func (s *PostgresSink) Key() []string {
	if s.mode != PostgresModeUpsert {
		return nil
	}
	return append([]string(nil), s.key...)
}

func (s *PostgresSink) WriteTable(ctx context.Context, batch arrow.Table) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	batch.Retain()
	s.tables = append(s.tables, batch)
	return nil
}

// Flush delivers the buffered batches one at a time, in arrival order, each
// in its own transaction, and stops at the first failure with that batch and
// every later one still buffered.
//
// One transaction per batch, not one for all of them: after a failed flush
// the retry ladder calls Flush with two or more batches buffered, and a
// running total or a CDC stream carries the same key in consecutive batches.
// Merged in one statement those rows would hit ON CONFLICT DO UPDATE twice
// and fail with 21000, which the first attempt would have applied in order.
func (s *PostgresSink) Flush(ctx context.Context) error {
	for {
		s.mu.Lock()
		if len(s.tables) == 0 {
			s.mu.Unlock()
			return nil
		}
		head := s.tables[0]
		s.mu.Unlock()

		if err := s.send(ctx, head); err != nil {
			return err
		}

		// Still at the head: WriteTable only appends, and nothing else
		// removes.
		s.mu.Lock()
		s.tables = s.tables[1:]
		s.mu.Unlock()
		head.Release()
	}
}

// BufferedRows reports the rows no flush has delivered.
func (s *PostgresSink) BufferedRows() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	var rows int64
	for _, t := range s.tables {
		rows += t.NumRows()
	}
	return int(rows)
}

// send is one batch's transaction. It releases nothing.
func (s *PostgresSink) send(ctx context.Context, tbl arrow.Table) error {
	// A handler whose query matched nothing yields an empty, column-less
	// table; there is nothing to copy.
	if tbl.NumRows() == 0 || tbl.NumCols() == 0 {
		return nil
	}
	cols := make([]string, tbl.NumCols())
	for i, f := range tbl.Schema().Fields() {
		cols[i] = f.Name
	}
	for _, k := range s.key {
		if !containsString(cols, k) {
			return errs.New(errs.CodeSinkInvalid, "postgres sink: key column %q is not in the batch, which has %s",
				k, strings.Join(cols, ", "))
		}
	}
	rows, err := postgresRows(tbl)
	if err != nil {
		return err
	}

	s.connMu.Lock()
	defer s.connMu.Unlock()
	conn, err := s.connect(ctx)
	if err != nil {
		return err
	}
	if err := s.ensureStaging(ctx, conn, cols); err != nil {
		return s.failed(conn, err)
	}

	tx, err := conn.Begin(ctx)
	if err != nil {
		return s.failed(conn, postgresError(err, "postgres sink: begin"))
	}
	// Rollback after Commit is a no-op that returns ErrTxClosed. On the
	// flush's own context, which may already be done, the rollback would
	// fail; WithoutCancel lets it run.
	defer tx.Rollback(context.WithoutCancel(ctx))

	if _, err := tx.CopyFrom(ctx, pgx.Identifier{postgresStaging}, append(cols, postgresSeq), pgx.CopyFromRows(rows)); err != nil {
		return s.failed(conn, postgresCopyError(err))
	}
	if _, err := tx.Exec(ctx, postgresMergeSQL(s.table, s.mode, s.key, cols)); err != nil {
		return s.failed(conn, postgresError(err, "postgres sink: merge into %s", s.table.Sanitize()))
	}
	if err := tx.Commit(ctx); err != nil {
		return s.failed(conn, postgresError(err, "postgres sink: commit"))
	}
	return nil
}

// connect returns the open connection, dialing when there is none. A
// connection a failure closed is replaced, and the staging table with it,
// because a temp table dies with its session.
func (s *PostgresSink) connect(ctx context.Context) (*pgx.Conn, error) {
	if s.conn != nil && !s.conn.IsClosed() {
		return s.conn, nil
	}
	s.conn, s.staging = nil, nil
	conn, err := pgx.ConnectConfig(ctx, s.connConfig)
	if err != nil {
		return nil, postgresError(err, "postgres sink: connect")
	}
	s.conn = conn
	return conn, nil
}

// ensureStaging creates the temp table for this column set, once per
// connection and column set. A batch whose columns differ from the last
// drops and recreates it.
func (s *PostgresSink) ensureStaging(ctx context.Context, conn *pgx.Conn, cols []string) error {
	if s.staging != nil && sameStrings(s.staging, cols) {
		return nil
	}
	for _, q := range postgresStagingSQL(s.table, cols) {
		if _, err := conn.Exec(ctx, q); err != nil {
			return postgresError(err, "postgres sink: staging table for %s", s.table.Sanitize())
		}
	}
	s.staging = append([]string(nil), cols...)
	return nil
}

// failed forgets a connection the failure closed.
func (s *PostgresSink) failed(conn *pgx.Conn, err error) error {
	if conn.IsClosed() {
		s.conn, s.staging = nil, nil
	}
	return err
}

func sameStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// Probe dials and checks the target: the table exists, every key column is
// a column of it, and for upsert a unique index or constraint covers exactly
// the key. For append a unique index is a warning: a redelivery after a
// crash inserts the same rows again and fails, so append is at-least-once
// for the reader.
func (s *PostgresSink) Probe(ctx context.Context) error {
	s.connMu.Lock()
	defer s.connMu.Unlock()
	conn, err := s.connect(ctx)
	if err != nil {
		return err
	}
	name := s.table.Sanitize()

	var exists bool
	if err := conn.QueryRow(ctx, postgresTableExistsSQL, name).Scan(&exists); err != nil {
		return s.failed(conn, postgresError(err, "postgres sink: looking up %s", name))
	}
	if !exists {
		return errs.New(errs.CodeSinkInvalid, "postgres sink: table %s does not exist; the sink writes to a table it does not create", name)
	}

	rows, err := conn.Query(ctx, postgresColumnsSQL, name)
	if err != nil {
		return s.failed(conn, postgresError(err, "postgres sink: reading the columns of %s", name))
	}
	var columns []string
	for rows.Next() {
		var c string
		if err := rows.Scan(&c); err != nil {
			rows.Close()
			return postgresError(err, "postgres sink: reading the columns of %s", name)
		}
		columns = append(columns, c)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return s.failed(conn, postgresError(err, "postgres sink: reading the columns of %s", name))
	}
	for _, k := range s.key {
		if !containsString(columns, k) {
			return errs.New(errs.CodeSinkInvalid, "postgres sink: key column %q is not a column of %s, which has %s",
				k, name, strings.Join(columns, ", "))
		}
	}

	if s.mode == PostgresModeUpsert {
		sorted := append([]string(nil), s.key...)
		sort.Strings(sorted)
		var unique bool
		if err := conn.QueryRow(ctx, postgresUniqueIndexSQL, name, sorted).Scan(&unique); err != nil {
			return s.failed(conn, postgresError(err, "postgres sink: reading the indexes of %s", name))
		}
		if !unique {
			return errs.New(errs.CodeSinkInvalid,
				"postgres sink: no unique index or constraint covers exactly (%s) on %s, and ON CONFLICT needs one; "+
					"a partial or expression index does not count. Add PRIMARY KEY (%s) or CREATE UNIQUE INDEX ON %s (%s)",
				strings.Join(s.key, ", "), name, strings.Join(s.key, ", "), name, strings.Join(s.key, ", "))
		}
		return nil
	}

	var anyUnique bool
	if err := conn.QueryRow(ctx, postgresAnyUniqueIndexSQL, name).Scan(&anyUnique); err != nil {
		return s.failed(conn, postgresError(err, "postgres sink: reading the indexes of %s", name))
	}
	if anyUnique {
		s.warnings = append(s.warnings, fmt.Sprintf(
			"postgres sink: %s has a unique index and the mode is append; a redelivery after a crash inserts the same rows again and fails. append is at-least-once for the reader; use upsert with a key for exactly-once", name))
	}
	return nil
}

// Warnings reports what Probe found that is not an error.
func (s *PostgresSink) Warnings() []string { return append([]string(nil), s.warnings...) }

// Close releases the connection. Safe to call twice.
func (s *PostgresSink) Close() error {
	s.connMu.Lock()
	defer s.connMu.Unlock()
	if s.conn == nil {
		return nil
	}
	conn := s.conn
	s.conn, s.staging = nil, nil
	return conn.Close(context.Background())
}
```

- [ ] **Step 5: The builder, the logger option, the warnings, and coded probe errors**

In `internal/sinks/init.go`:

Add `"go.uber.org/zap"` to the imports. Add `logger *zap.Logger` to `options`. Add after `WithConnLock`:

```go
// WithLogger supplies the logger New reports a sink's startup warnings
// through. Without one they are dropped.
func WithLogger(l *zap.Logger) Option {
	return func(o *options) { o.logger = l }
}

// Warner is implemented by a sink whose probe can find something worth
// saying that is not an error: a target shape that works today and fails on
// a redelivery, say.
type Warner interface {
	Warnings() []string
}
```

In `New`, after the `probe` call succeeds and before `role := o.role`:

```go
	if w, ok := built.(Warner); ok && o.logger != nil {
		for _, msg := range w.Warnings() {
			o.logger.Warn(msg)
		}
	}
```

In `builders`, after the `"clickhouse"` entry:

```go
	"postgres": func(_ context.Context, sink config.Sink, _ adbc.Connection) (core.Sink, error) {
		if sink.Postgres == nil {
			return nil, errs.New(errs.CodeSinkInvalid, "sink: postgres sink needs a postgres block with dsn, table and mode")
		}
		return NewPostgresSink(*sink.Postgres)
	},
```

In `internal/sinks/probe.go`, replace the tail of `probe` from `// A server that answered and refused` to the end with:

```go
	// A probe that already coded its failure said what it found, and the
	// code decides the exit: a missing table is the user's to fix, a refused
	// host may come back. Rewriting it would replace "table t does not exist"
	// with "the destination refused the connection".
	if errs.ClassOf(err) == errs.ClassUser || errs.HasCode(err, errs.CodeSinkUnreachable) {
		return err
	}

	// A server that answered and refused is the user's to fix: a wrong
	// password or a missing database does not resolve on a restart. Reporting
	// it as unreachable would have a supervisor retry a pipeline that cannot
	// start.
	if !isUnreachable(err) {
		return errs.Wrap(errs.CodeSinkInvalid, err,
			"sink: the destination refused the connection at startup")
	}
	return errs.Wrap(errs.CodeSinkUnreachable, err,
		"sink: the destination could not be reached at startup")
}
```

Check `errs.ClassUser` is the exported name: `grep -n "ClassUser" internal/errs/*.go`. If the constant is named differently, use that name.

In `internal/cli/run/root.go`, add a `logger *zap.Logger` parameter to `newPipelineSink` and pass `sinks.WithLogger(logger)`; update every caller (`grep -rn "newPipelineSink(" internal/cli/run/`) including `shared_conn_test.go`, passing `zap.NewNop()` in tests. Add `sinks.WithLogger(logger)` to the DLQ build at `root.go:64` (find the logger variable in scope; if the function has none, add the parameter the same way). In `internal/cli/run/managers.go:118`, add `sinks.WithLogger(l)`.

- [ ] **Step 6: Run the unit tests and the affected packages**

Run: `gofmt -l internal cmd; go vet ./... && go test -short ./internal/sinks ./internal/cli/... ./internal/core ./internal/schema`
Expected: `gofmt` prints nothing, vet clean, sinks PASS. `TestToolingCoverageSinkRegistry_MatchesTheConstructorSwitch` FAILS because `postgres` has no integration file yet, and the schema golden test FAILS because the enum gained `postgres`. Both are fixed in the next steps; everything else passes. The unreachable-flush test takes about 2 seconds for its connect timeout.

- [ ] **Step 7: Regenerate the schema and the golden example**

Run: `make schema`
Expected: `internal/validate/schemas/config.json` gains `postgres` in both sink type enums and a `postgres` properties block; `internal/cli/testdata/config_example.golden` may change. `go test -short ./internal/schema ./internal/cli ./internal/validate` passes.

- [ ] **Step 8: Commit**

```bash
git add internal/core/turbine.go internal/sinks/postgres.go internal/sinks/init.go internal/sinks/probe.go internal/sinks/postgres_test.go internal/cli/run internal/validate/schemas internal/cli/testdata
git commit -m "sinks: a postgres sink on pgx, one transaction per batch, with a probe that checks the key"
```

The registry test stays red until Task 6 lands the integration file. Do not commit a placeholder file to make it green.

---

### Task 6: Registry, the new invariant, and the conformance run

**Files:**
- Modify: `internal/conformance/conformance.go`
- Modify: `internal/conformance/conformance_test.go` (only if a test enumerates verdicts)
- Modify: `docs/coverage/invariants.yml` (after `sink.probe.fails_start`)
- Modify: `docs/coverage/features.yml` (after `sink.clickhouse`)
- Create: `docs/coverage/integrations/sink.postgres.yml`
- Modify: `docs/coverage/integrations/sink.clickhouse.yml`, `sink.iceberg.yml`, `sink.kafka.yml`, `sink.console.yml`, `sink.noop.yml`, `sink.sqlcommand.yml`
- Modify: `internal/sinks/structural_test.go`
- Create: `internal/sinks/conformance_postgres_test.go`

**Interfaces:**
- Consumes: `core.KeyedSink`, `NewPostgresSink`, `conformance.NewProxy(t, nw, "postgres:5432")`.
- Produces: invariant id `sink.flush.idempotent_on_key`; integration id `sink.postgres`; feature `sink.postgres`.

- [ ] **Step 1: Declare the invariant**

In `docs/coverage/invariants.yml`, after the `sink.probe.fails_start` entry:

```yaml
  - id: sink.flush.idempotent_on_key
    family: resilience
    class: safety
    applies_to: sink
    claim: >
      Delivering the same batch twice leaves the destination holding it once.
      The engine promises at-least-once; a sink that identifies rows by key is
      what turns that into exactly-once for the reader. A sink that declares
      no key is exempt, with a proof that it declares none.
    verified_by: harness
    requires: []
```

- [ ] **Step 2: Add the harness step**

In `internal/conformance/conformance.go`:

Add the constant beside the others (find `closeIdempotent = ` and add after it): `idempotentOnKey = "sink.flush.idempotent_on_key"`.

Every early return in `sinkVerdicts` that lists verdicts gains `{invariant: idempotentOnKey, skipped: <the same reason the list uses>}`: the `s.Break == nil` list (`skip`), the `hung` list (`stuck`), the `err == nil` write-through list (`"the rows were delivered on write"`), and the heal-failed list (`"the retry never delivered"`).

Capture the raw sink before wrapping. Change

```go
	sink := core.NewCountingSink(
		s.New(t),
```

to

```go
	raw := s.New(t)
	sink := core.NewCountingSink(
		raw,
```

Before the final `return append([]verdict{empty, buffers, keeps, depth, honours, hollow, order, counted}, startAndStop(t, s)...)`, add:

```go
	// A second delivery of a batch the destination already holds. The engine
	// republishes a bucket whose delete lost a race with its flush, and a
	// sink that identifies rows by key absorbs that rather than doubling it.
	keyed := verdict{invariant: idempotentOnKey}
	k, isKeyed := raw.(core.KeyedSink)
	switch {
	case !isKeyed || len(k.Key()) == 0:
		keyed.skipped = s.Integration + " identifies rows by no key, so a second " +
			"delivery is a second row; exempt it in its docs/coverage/integrations file"
	case keeps.failure != "":
		keyed.skipped = "rows were lost, so there is no second delivery to judge"
	default:
		again := s.Table(t, 1)
		err := sink.WriteTable(ctx, again)
		again.Release()
		if err != nil {
			t.Fatalf("conformance: WriteTable for the second delivery: %v", err)
		}
		if err := sink.Flush(ctx); err != nil {
			keyed.failure = "delivering id=1 a second time failed with " + err.Error() +
				"; a keyed sink replaces the row it already holds"
		} else if got := s.ReadBack(t); len(got) != 3 || countOf(got, int64(1)) != 1 {
			keyed.failure = "after delivering id=1 twice the destination holds " +
				describe(got) + "; want ids 1, 2 and 3 once each"
		}
	}

	return append([]verdict{empty, buffers, keeps, depth, honours, hollow, order, counted, keyed},
		startAndStop(t, s)...)
```

And beside `indexOf`:

```go
func countOf(rows []Row, id int64) int {
	n := 0
	for _, r := range rows {
		if r["id"] == id {
			n++
		}
	}
	return n
}
```

Run: `go test -short ./internal/conformance`
Expected: PASS. If a test in `conformance_test.go` counts verdicts or lists every invariant, add `idempotentOnKey` to it with the skip reason above, because the doubles declare no key.

- [ ] **Step 3: Declare the feature and the integration**

In `docs/coverage/features.yml`, after the `sink.clickhouse` entry:

```yaml
  - id: sink.postgres
    description: Upserts or appends result batches into a Postgres table over a native client.
    requires: [unit, integration]
```

Create `docs/coverage/integrations/sink.postgres.yml`:

```yaml
id: sink.postgres
kind: sink
implements: [Sink, Prober, KeyedSink]
feature: sink.postgres
exempt: []
# Arrow key -> what the sink does with it. Keys come from lattice.yml.
#
# `columns` are the Postgres column types that accept the key, and `expect`
# is what `SELECT v::text` returns on a session with TimeZone UTC. Written
# by hand from internal/sinks/postgres_types.go and the spec's type table,
# never generated from the sink. A row the runner disagrees with is a
# finding: when the stored value is right and only the rendering differs,
# fix the row and say so in the commit; when the value is wrong, fix the
# sink.
types:
  bool:
    outcome: exact
    columns:
      - type: boolean
        expect: "true"
  int8:
    outcome: exact
    columns:
      - type: smallint
        expect: "-128"
  int16:
    outcome: exact
    columns:
      - type: smallint
        expect: "-32768"
  int32:
    outcome: exact
    columns:
      - type: integer
        expect: "-2147483648"
  int64:
    outcome: exact
    columns:
      - type: bigint
        expect: "-9223372036854775808"
  # Postgres has no unsigned integers. Each is sent as the next signed
  # width, and uint64 above the bigint range as numeric.
  uint8:
    outcome: coerced
    rule: sent as smallint
    columns:
      - type: smallint
        expect: "255"
  uint16:
    outcome: coerced
    rule: sent as integer
    columns:
      - type: integer
        expect: "65535"
  uint32:
    outcome: coerced
    rule: sent as bigint
    columns:
      - type: bigint
        expect: "4294967295"
  uint64:
    outcome: coerced
    rule: sent as bigint when it fits, otherwise as numeric
    columns:
      - type: numeric
        expect: "18446744073709551615"
  float32:
    outcome: exact
    columns:
      - type: real
        expect: "3.4028235e+38"
  float64:
    outcome: exact
    columns:
      - type: double precision
        expect: "1.7976931348623157e+308"
  "decimal(*, *)":
    outcome: exact
    columns:
      - type: numeric
        expect: "170141183460469231"
  utf8:
    outcome: exact
    columns:
      - type: text
        expect: "L'Œil 👁 \"quoted\" back\\slash\ttab"
      - type: varchar(100)
        expect: "L'Œil 👁 \"quoted\" back\\slash\ttab"
      # The destinations that parse the text. jsonb normalises spacing.
      - type: jsonb
        value: '{"a":1}'
        expect: '{"a": 1}'
      - type: uuid
        value: "0b7e2c9a-4d31-4f6e-9a02-7c1de3f10b45"
        expect: "0b7e2c9a-4d31-4f6e-9a02-7c1de3f10b45"
      - type: numeric
        value: "1234.56"
        expect: "1234.56"
  binary:
    outcome: exact
    columns:
      - type: bytea
        expect: "\\x00ff7f80"
  date32:
    outcome: exact
    columns:
      - type: date
        expect: "2026-09-08"
  "timestamp[s]":
    outcome: exact
    columns:
      - type: timestamp
        expect: "2026-09-08 12:00:00"
  "timestamp[ms]":
    outcome: exact
    columns:
      - type: timestamp
        expect: "2026-09-08 12:00:00.123"
  "timestamp[us]":
    outcome: exact
    columns:
      - type: timestamp
        expect: "2026-09-08 12:00:00.123456"
      # A naive timestamp into timestamptz is read as a UTC instant.
      - type: timestamptz
        expect: "2026-09-08 12:00:00.123456+00"
  "timestamp[ns]":
    outcome: coerced
    rule: Postgres keeps microseconds; the nanoseconds are truncated
    columns:
      - type: timestamp
        expect: "2026-09-08 12:00:00.123456"
  "timestamp[us, tz=*]":
    outcome: exact
    columns:
      - type: timestamptz
        expect: "2026-09-08 12:00:00.123456+00"
        instant: true
      # The instant's UTC wall clock, into a column with no zone.
      - type: timestamp
        expect: "2026-09-08 12:00:00.123456"
        instant: true
  month_day_nano_interval:
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

  # Containers go as JSON text rendered from the Arrow array, into a json
  # column, which stores the text as sent. Element renderings follow
  # arrow-go's own MarshalJSON: temporals as text, binary as base64.
  "list<bool>":
    outcome: coerced
    rule: rendered as JSON text
    columns:
      - type: json
        expect: "[true,true,true]"
  "list<int8>":
    outcome: coerced
    rule: rendered as JSON text
    columns:
      - type: json
        expect: "[-128,-128,-128]"
  "list<int16>":
    outcome: coerced
    rule: rendered as JSON text
    columns:
      - type: json
        expect: "[-32768,-32768,-32768]"
  "list<int32>":
    outcome: coerced
    rule: rendered as JSON text
    columns:
      - type: json
        expect: "[-2147483648,-2147483648,-2147483648]"
  "list<int64>":
    outcome: coerced
    rule: rendered as JSON text
    columns:
      - type: json
        expect: "[-9223372036854775808,-9223372036854775808,-9223372036854775808]"
  "list<uint8>":
    outcome: coerced
    rule: rendered as JSON text
    columns:
      - type: json
        expect: "[255,255,255]"
  "list<uint16>":
    outcome: coerced
    rule: rendered as JSON text
    columns:
      - type: json
        expect: "[65535,65535,65535]"
  "list<uint32>":
    outcome: coerced
    rule: rendered as JSON text
    columns:
      - type: json
        expect: "[4294967295,4294967295,4294967295]"
  "list<uint64>":
    outcome: coerced
    rule: rendered as JSON text
    columns:
      - type: json
        expect: "[18446744073709551615,18446744073709551615,18446744073709551615]"
  "list<float32>":
    outcome: coerced
    rule: rendered as JSON text
    columns:
      - type: json
        expect: "[3.4028235e+38,3.4028235e+38,3.4028235e+38]"
  "list<float64>":
    outcome: coerced
    rule: rendered as JSON text
    columns:
      - type: json
        expect: "[1.7976931348623157e+308,1.7976931348623157e+308,1.7976931348623157e+308]"
  "list<utf8>":
    outcome: coerced
    rule: rendered as JSON text
    columns:
      - type: json
        expect: '["L''Œil 👁 \"quoted\" back\\slash\ttab","L''Œil 👁 \"quoted\" back\\slash\ttab","L''Œil 👁 \"quoted\" back\\slash\ttab"]'
  "list<binary>":
    outcome: coerced
    rule: rendered as JSON text, each element base64
    columns:
      - type: json
        expect: '["AP9/gA==","AP9/gA==","AP9/gA=="]'
  "list<date32>":
    outcome: coerced
    rule: rendered as JSON text
    columns:
      - type: json
        expect: '["2026-09-08","2026-09-08","2026-09-08"]'
  "list<timestamp[s]>":
    outcome: coerced
    rule: rendered as JSON text
    columns:
      - type: json
        expect: '["2026-09-08 12:00:00","2026-09-08 12:00:00","2026-09-08 12:00:00"]'
  "list<timestamp[ms]>":
    outcome: coerced
    rule: rendered as JSON text
    columns:
      - type: json
        expect: '["2026-09-08 12:00:00.123","2026-09-08 12:00:00.123","2026-09-08 12:00:00.123"]'
  "list<timestamp[us]>":
    outcome: coerced
    rule: rendered as JSON text
    columns:
      - type: json
        expect: '["2026-09-08 12:00:00.123456","2026-09-08 12:00:00.123456","2026-09-08 12:00:00.123456"]'
  "list<timestamp[ns]>":
    outcome: coerced
    rule: rendered as JSON text
    columns:
      - type: json
        expect: '["2026-09-08 12:00:00.123456789","2026-09-08 12:00:00.123456789","2026-09-08 12:00:00.123456789"]'
  "list<timestamp[us, tz=*]>":
    outcome: coerced
    rule: rendered as JSON text in the type's own zone
    columns:
      - type: json
        expect: '["2026-09-08 21:00:00.123456+0900","2026-09-08 21:00:00.123456+0900","2026-09-08 21:00:00.123456+0900"]'
  "list<list<int64>>":
    outcome: coerced
    rule: rendered as JSON text
    columns:
      - type: json
        expect: "[[-9223372036854775808,-9223372036854775808,-9223372036854775808],[-9223372036854775808,-9223372036854775808,-9223372036854775808],[-9223372036854775808,-9223372036854775808,-9223372036854775808]]"
  "list<struct>":
    outcome: coerced
    rule: rendered as JSON text
    columns:
      - type: json
        expect: '[{"a":7},{"a":7},{"a":7}]'
  "fixed_size_list<int32>[3]":
    outcome: coerced
    rule: rendered as JSON text
    columns:
      - type: json
        expect: "[1,2,3]"
  struct:
    outcome: coerced
    rule: rendered as JSON text
    columns:
      - type: json
        expect: '{"a":7}'
  map:
    outcome: coerced
    rule: rendered as JSON text, an array of key and value pairs
    columns:
      - type: json
        expect: '[{"key":"k","value":9}]'
  # A container does not launder an element the sink cannot convert.
  "list<decimal(*, *)>":
    outcome: unsupported
    code: user.sink.type_unsupported
    columns: []
  "list<time64[us]>":
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
# A null is sent as NULL. The test columns are nullable, so it is stored as
# NULL; a NOT NULL column refuses it at the merge with 23502.
nulls:
  default:
    outcome: exact
    rule: stored as NULL; a NOT NULL column refuses it at the merge
  list_element:
    outcome: exact
    rule: a JSON null inside the rendered array
```

Every key in `docs/coverage/lattice.yml` must appear. Run `make coverage-check` locally only for its registry part if it runs without reports; otherwise `uv run --locked pytest tests/tooling -q`, which checks the registries, catches a missing or misspelled key.

- [ ] **Step 4: Exempt the six other sinks with proofs**

Append to `internal/sinks/structural_test.go`, modelled on `TestSinkSqlcommand_ImplementsNoProber` and using each sink's existing construction in that file:

```go
// The six sinks that identify a row by no key. sink.flush.idempotent_on_key
// is exempt for each, and this is the proof: a second delivery is a second
// row because nothing names a key to replace on.
func TestSinkClickhouse_IsNotKeyed(t *testing.T) {
	coverage.Covers(t, "sink.clickhouse")
	built, err := NewClickhouseSink(config.ClickhouseSink{DSN: "clickhouse://localhost:8123/db", Table: "t"})
	assert.NoError(t, err)
	var s core.Sink = built
	_, ok := s.(core.KeyedSink)
	assert.That(t, !ok)
}

func TestSinkIceberg_IsNotKeyed(t *testing.T) {
	coverage.Covers(t, "sink.iceberg")
	catalogName, tableName := newLocalIcebergTable(t)
	built, err := NewIcebergSink(context.Background(), catalogName, tableName)
	assert.NoError(t, err)
	var s core.Sink = built
	_, ok := s.(core.KeyedSink)
	assert.That(t, !ok)
}

func TestSinkKafka_IsNotKeyed(t *testing.T) {
	coverage.Covers(t, "sink.kafka")
	built, err := NewKafkaSink(config.KafkaSink{Brokers: []string{"localhost:1"}, Topic: "t"})
	assert.NoError(t, err)
	var s core.Sink = built
	_, ok := s.(core.KeyedSink)
	assert.That(t, !ok)
}

func TestSinkConsole_IsNotKeyed(t *testing.T) {
	coverage.Covers(t, "sink.console")
	var s core.Sink = NewConsoleSink()
	_, ok := s.(core.KeyedSink)
	assert.That(t, !ok)
}

func TestSinkNoop_IsNotKeyed(t *testing.T) {
	coverage.Covers(t, "sink.noop")
	var s core.Sink = &NoopSink{}
	_, ok := s.(core.KeyedSink)
	assert.That(t, !ok)
}

func TestSinkSqlcommand_IsNotKeyed(t *testing.T) {
	coverage.Covers(t, "sink.sqlcommand")
	conn := newSinkTestConn(t)
	built, err := NewSQLCommandSink(conn, "SELECT 1", nil)
	assert.NoError(t, err)
	var s core.Sink = built
	_, ok := s.(core.KeyedSink)
	assert.That(t, !ok)
}
```

Check `config.KafkaSink`'s field names with `grep -n -A6 "^type KafkaSink struct" internal/config/config.go` and use them. If `NewKafkaSink` dials at construction (read its first twenty lines), build it the way `internal/sinks/kafka_test.go` builds one without a broker.

Add to each of the six integration files under `exempt:` (for `sink.clickhouse.yml` and `sink.kafka.yml`, replace `exempt: []`):

```yaml
  - invariant: sink.flush.idempotent_on_key
    reason: identifies rows by no key; the table engine decides what a second delivery means, and ReplacingMergeTree is a later mode with the same key and mode shape
    proven_by: TestSinkClickhouse_IsNotKeyed
```

```yaml
  - invariant: sink.flush.idempotent_on_key
    reason: appends by design; a second delivery is a second row the reader deduplicates
    proven_by: TestSinkIceberg_IsNotKeyed
```

```yaml
  - invariant: sink.flush.idempotent_on_key
    reason: a topic is a log; a second delivery is a second record, which is at-least-once as Kafka defines it
    proven_by: TestSinkKafka_IsNotKeyed
```

```yaml
  - invariant: sink.flush.idempotent_on_key
    reason: writes lines to an io.Writer; nothing holds a row to identify
    proven_by: TestSinkConsole_IsNotKeyed
```

```yaml
  - invariant: sink.flush.idempotent_on_key
    reason: has no destination; discarding every row is the contract
    proven_by: TestSinkNoop_IsNotKeyed
```

```yaml
  - invariant: sink.flush.idempotent_on_key
    reason: the user's SQL decides what a second delivery means; the sink names no key
    proven_by: TestSinkSqlcommand_IsNotKeyed
```

Run: `go test -short ./internal/sinks ./internal/coverage && uv run --locked pytest tests/tooling -q`
Expected: PASS, including `TestToolingCoverageSinkRegistry_MatchesTheConstructorSwitch`.

- [ ] **Step 5: Write the conformance and type runs**

Create `internal/sinks/conformance_postgres_test.go`:

```go
package sinks

// The Postgres sink under the resilience harness and the type runner,
// against a real server behind the fault proxy.

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/jackc/pgx/v5"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/turbolytics/sql-flow/internal/conformance"
	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/core"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

// postgresImage is pinned so a server upgrade is a commit rather than a
// surprise.
const postgresImage = "postgres:16"

const (
	postgresUser     = "sqlflow"
	postgresPassword = "sqlflow"
	postgresDatabase = "sqlflow"
)

// postgresServer is one container on a network, with a direct connection
// for DDL and read-back that never crosses the fault proxy.
type postgresServer struct {
	container *tcpostgres.PostgresContainer
	network   *testcontainers.DockerNetwork
	direct    *pgx.Conn
}

func startPostgres(t *testing.T) *postgresServer {
	t.Helper()
	ctx := context.Background()

	nw, err := network.New(ctx)
	if err != nil {
		t.Fatalf("docker network: %v", err)
	}
	t.Cleanup(func() { _ = nw.Remove(context.Background()) })

	pg, err := tcpostgres.Run(ctx, postgresImage,
		network.WithNetwork([]string{"postgres"}, nw),
		tcpostgres.WithDatabase(postgresDatabase),
		tcpostgres.WithUsername(postgresUser),
		tcpostgres.WithPassword(postgresPassword),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	t.Cleanup(func() { _ = pg.Terminate(context.Background()) })

	dsn, err := pg.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}
	direct, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = direct.Close(context.Background()) })
	// Every read-back renders on a UTC session, so a timestamptz expect is
	// one string whatever the host's zone.
	_, err = direct.Exec(ctx, "SET TIME ZONE 'UTC'")
	assert.NoError(t, err)

	return &postgresServer{container: pg, network: nw, direct: direct}
}

// proxyDSN addresses the server through the fault proxy.
func (s *postgresServer) proxyDSN(t *testing.T) (string, *conformance.Proxy) {
	proxy := conformance.NewProxy(t, s.network, "postgres:5432")
	return fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=disable",
		postgresUser, postgresPassword, proxy.Addr, postgresDatabase), proxy
}

func idTable(t *testing.T, id int64) arrow.Table {
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)
	b := array.NewRecordBuilder(memory.NewGoAllocator(), schema)
	defer b.Release()
	b.Field(0).(*array.Int64Builder).Append(id)
	rec := b.NewRecord()
	defer rec.Release()
	return array.NewTableFromRecords(schema, []arrow.Record{rec})
}

func TestIntegrationSinkPostgres_Conformance(t *testing.T) {
	coverage.Covers(t, "sink.postgres")
	if testing.Short() {
		t.Skip("integration test: -short runs the unit pass only")
	}
	ctx := context.Background()
	srv := startPostgres(t)
	dsn, proxy := srv.proxyDSN(t)

	table := fmt.Sprintf("conformance_%d", time.Now().UnixNano())
	_, err := srv.direct.Exec(ctx, "CREATE TABLE "+table+" (id bigint PRIMARY KEY)")
	assert.NoError(t, err)

	conformance.Sinks(t, conformance.SinkSubject{
		Integration: "sink.postgres",

		New: func(t *testing.T) core.Sink {
			s, err := NewPostgresSink(config.PostgresSink{DSN: dsn, Table: table, Mode: PostgresModeUpsert, Key: []string{"id"}})
			assert.NoError(t, err)
			t.Cleanup(func() { s.Close() })
			return s
		},

		Break: proxy.Break,
		Heal:  proxy.Heal,

		// ctid order is insertion order on a table nothing has vacuumed,
		// which is what preserves_order has to observe.
		ReadBack: func(t *testing.T) []conformance.Row {
			rows, err := srv.direct.Query(context.Background(), "SELECT id FROM "+table+" ORDER BY ctid")
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

		Table:           idTable,
		OrderedReadBack: true,
	})
}

func TestIntegrationSinkPostgres_Types(t *testing.T) {
	coverage.Covers(t, "sink.postgres")
	if testing.Short() {
		t.Skip("integration test: -short runs the unit pass only")
	}
	srv := startPostgres(t)
	ctx := context.Background()
	dsn, err := srv.container.ConnectionString(ctx, "sslmode=disable")
	assert.NoError(t, err)

	declared, err := coverage.TypesFor("sink.postgres")
	assert.NoError(t, err)
	nulls, err := coverage.NullsFor("sink.postgres")
	assert.NoError(t, err)
	elemNulls, err := coverage.NullElementsFor("sink.postgres")
	assert.NoError(t, err)

	conformance.Types(t, conformance.TypeSubject{
		Integration:      "sink.postgres",
		Declared:         declared,
		Nulls:            nulls,
		ListElementNulls: elemNulls,

		Prepare: func(t *testing.T, key, columnType string) conformance.TypeDestination {
			// An unsupported row fails before any statement is built, so
			// its column type never matters.
			if columnType == "" {
				columnType = "bigint"
			}
			table := fmt.Sprintf("t_%d", time.Now().UnixNano())
			_, err := srv.direct.Exec(context.Background(), fmt.Sprintf("CREATE TABLE %s (v %s)", table, columnType))
			assert.NoError(t, err)

			sink, err := NewPostgresSink(config.PostgresSink{DSN: dsn, Table: table, Mode: PostgresModeAppend})
			assert.NoError(t, err)
			t.Cleanup(func() { sink.Close() })

			return conformance.TypeDestination{
				Sink: sink,
				ReadBack: func(t *testing.T) (any, error) {
					var rendered *string
					err := srv.direct.QueryRow(context.Background(), "SELECT v::text FROM "+table+" LIMIT 1").Scan(&rendered)
					if err != nil {
						return nil, err
					}
					if rendered == nil {
						return nil, nil
					}
					return *rendered, nil
				},
				// The runner writes [value, null, value] into a json column.
				ReadBackNullElement: func(t *testing.T) (bool, error) {
					var isNull bool
					err := srv.direct.QueryRow(context.Background(), "SELECT json_typeof(v -> 1) = 'null' FROM "+table+" LIMIT 1").Scan(&isNull)
					return isNull, err
				},
			}
		},
	})
}
```

- [ ] **Step 6: Run the integration tests locally**

Run: `go test -count=1 -run 'TestIntegrationSinkPostgres_' ./internal/sinks -v 2>&1 | grep -E "^(=== RUN|--- |\s+conformance|ok|FAIL)|failed|declared" | head -80`
Expected: every `sink.*` and `lifecycle.*` subtest PASS or SKIP with the harness's own reason; every `type.*` subtest PASS.

Three outcomes need a decision, in order:

1. A `type.*` row fails on a rendering only, and the stored value is right. Fix the `expect` in `sink.postgres.yml` and record the corrected renderings in the commit message. The temporal element renderings inside JSON (`list<timestamp*>`, `list<date32>`, `map`) are the likely ones.
2. `sink.flush.honours_context` is skipped with "fails a flush before its context expires". Add to `sink.postgres.yml`:

```yaml
  - invariant: sink.flush.honours_context
    reason: >
      a broken proxy resets the connection and pgx fails the flush at once,
      so no deadline is reached to honour; every statement still runs on the
      flush's context
    proven_by: TestIntegrationSinkPostgres_FlushFailsBeforeItsContextExpires
```

  and write that test in `postgres_integration_test.go` (Task 7): break the proxy, flush with a 5 second deadline, assert the error returns within 1 second and is coded `system.sink.unreachable`.
3. Any other failure is a sink defect. Fix the sink, not the harness.

- [ ] **Step 7: Commit**

```bash
git add docs/coverage internal/conformance internal/sinks/structural_test.go internal/sinks/conformance_postgres_test.go
git commit -m "conformance: sink.flush.idempotent_on_key, and the Postgres sink under the harness and the type runner"
```

---

### Task 7: Behaviour against a real server

**Files:**
- Create: `internal/sinks/postgres_integration_test.go`

**Interfaces:**
- Consumes: `startPostgres`, `postgresServer.proxyDSN`, `idTable` from Task 6.

- [ ] **Step 1: Write the tests**

```go
package sinks

// The Postgres sink's own promises, against a real server. The conformance
// harness proves the contract every sink shares; these prove what this sink
// adds: last row in a batch wins, a retry applies buffered batches in order,
// omitted columns take defaults, and the probe reads the catalog.

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/turbolytics/sql-flow/internal/errs"
	"github.com/zeebo/assert"
)

func kvTable(t *testing.T, pairs ...int64) arrow.Table {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "k", Type: arrow.PrimitiveTypes.Int64},
		{Name: "v", Type: arrow.PrimitiveTypes.Int64},
	}, nil)
	b := array.NewRecordBuilder(memory.NewGoAllocator(), schema)
	defer b.Release()
	for i := 0; i < len(pairs); i += 2 {
		b.Field(0).(*array.Int64Builder).Append(pairs[i])
		b.Field(1).(*array.Int64Builder).Append(pairs[i+1])
	}
	rec := b.NewRecord()
	defer rec.Release()
	return array.NewTableFromRecords(schema, []arrow.Record{rec})
}

func readKV(t *testing.T, srv *postgresServer, table string) map[int64]int64 {
	rows, err := srv.direct.Query(context.Background(), "SELECT k, v FROM "+table)
	assert.NoError(t, err)
	defer rows.Close()
	out := map[int64]int64{}
	for rows.Next() {
		var k, v int64
		assert.NoError(t, rows.Scan(&k, &v))
		out[k] = v
	}
	assert.NoError(t, rows.Err())
	return out
}

func newKVTable(t *testing.T, srv *postgresServer, ddl string) string {
	table := fmt.Sprintf("kv_%d", time.Now().UnixNano())
	_, err := srv.direct.Exec(context.Background(), fmt.Sprintf(ddl, table))
	assert.NoError(t, err)
	return table
}

func upsertSink(t *testing.T, dsn, table string) *PostgresSink {
	s, err := NewPostgresSink(config.PostgresSink{DSN: dsn, Table: table, Mode: PostgresModeUpsert, Key: []string{"k"}})
	assert.NoError(t, err)
	t.Cleanup(func() { s.Close() })
	return s
}

// Two rows with one key in a batch: the last one in the batch wins, and
// nothing fails.
func TestIntegrationSinkPostgres_LastRowInABatchWins(t *testing.T) {
	coverage.Covers(t, "sink.postgres")
	if testing.Short() {
		t.Skip("integration test: -short runs the unit pass only")
	}
	srv := startPostgres(t)
	dsn, _ := srv.container.ConnectionString(context.Background(), "sslmode=disable")
	table := newKVTable(t, srv, "CREATE TABLE %s (k bigint PRIMARY KEY, v bigint NOT NULL)")
	s := upsertSink(t, dsn, table)

	batch := kvTable(t, 1, 10, 2, 20, 1, 30)
	defer batch.Release()
	assert.NoError(t, s.WriteTable(context.Background(), batch))
	assert.NoError(t, s.Flush(context.Background()))
	assert.DeepEqual(t, map[int64]int64{1: 30, 2: 20}, readKV(t, srv, table))
}

// A failed flush leaves two batches buffered that share a key. The retry
// applies them one at a time, in order, so the table holds the second
// batch's value rather than failing with 21000.
func TestIntegrationSinkPostgres_RetryAppliesBufferedBatchesInOrder(t *testing.T) {
	coverage.Covers(t, "sink.postgres")
	if testing.Short() {
		t.Skip("integration test: -short runs the unit pass only")
	}
	srv := startPostgres(t)
	dsn, proxy := srv.proxyDSN(t)
	table := newKVTable(t, srv, "CREATE TABLE %s (k bigint PRIMARY KEY, v bigint NOT NULL)")
	s := upsertSink(t, dsn, table)
	ctx := context.Background()

	first := kvTable(t, 1, 1)
	defer first.Release()
	assert.NoError(t, s.WriteTable(ctx, first))
	proxy.Break(t)
	broken, cancel := context.WithTimeout(ctx, 5*time.Second)
	err := s.Flush(broken)
	cancel()
	assert.Error(t, err)
	assert.Equal(t, errs.CodeSinkUnreachable, errs.CodeOf(err))
	assert.Equal(t, 1, s.BufferedRows())

	second := kvTable(t, 1, 2)
	defer second.Release()
	assert.NoError(t, s.WriteTable(ctx, second))
	assert.Equal(t, 2, s.BufferedRows())

	proxy.Heal(t)
	assert.NoError(t, s.Flush(ctx))
	assert.Equal(t, 0, s.BufferedRows())
	assert.DeepEqual(t, map[int64]int64{1: 2}, readKV(t, srv, table))
}

// A broken destination fails the flush at once rather than at the deadline.
// This is the proof behind the honours_context exemption, if the harness
// skipped that claim in Task 6; delete it if the harness passed it.
func TestIntegrationSinkPostgres_FlushFailsBeforeItsContextExpires(t *testing.T) {
	coverage.Covers(t, "sink.postgres")
	if testing.Short() {
		t.Skip("integration test: -short runs the unit pass only")
	}
	srv := startPostgres(t)
	dsn, proxy := srv.proxyDSN(t)
	table := newKVTable(t, srv, "CREATE TABLE %s (k bigint PRIMARY KEY, v bigint NOT NULL)")
	s := upsertSink(t, dsn, table)
	ctx := context.Background()

	warm := kvTable(t, 1, 1)
	defer warm.Release()
	assert.NoError(t, s.WriteTable(ctx, warm))
	assert.NoError(t, s.Flush(ctx))

	proxy.Break(t)
	defer proxy.Heal(t)
	next := kvTable(t, 2, 2)
	defer next.Release()
	assert.NoError(t, s.WriteTable(ctx, next))
	deadline, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	started := time.Now()
	err := s.Flush(deadline)
	assert.Error(t, err)
	assert.That(t, time.Since(started) < 2*time.Second)
	assert.Equal(t, errs.CodeSinkUnreachable, errs.CodeOf(err))
}

// The INSERT names the batch's columns and no others, so a column the batch
// omits takes its default on insert and keeps its value on update.
func TestIntegrationSinkPostgres_OmittedColumnsTakeDefaultsAndKeepValues(t *testing.T) {
	coverage.Covers(t, "sink.postgres")
	if testing.Short() {
		t.Skip("integration test: -short runs the unit pass only")
	}
	srv := startPostgres(t)
	ctx := context.Background()
	dsn, _ := srv.container.ConnectionString(ctx, "sslmode=disable")
	table := newKVTable(t, srv, "CREATE TABLE %s (k bigint PRIMARY KEY, v bigint NOT NULL, stamp timestamptz NOT NULL DEFAULT now())")
	s := upsertSink(t, dsn, table)

	one := kvTable(t, 1, 1)
	defer one.Release()
	assert.NoError(t, s.WriteTable(ctx, one))
	assert.NoError(t, s.Flush(ctx))
	var first time.Time
	assert.NoError(t, srv.direct.QueryRow(ctx, "SELECT stamp FROM "+table+" WHERE k = 1").Scan(&first))

	two := kvTable(t, 1, 2)
	defer two.Release()
	assert.NoError(t, s.WriteTable(ctx, two))
	assert.NoError(t, s.Flush(ctx))
	var second time.Time
	var v int64
	assert.NoError(t, srv.direct.QueryRow(ctx, "SELECT stamp, v FROM "+table+" WHERE k = 1").Scan(&second, &v))
	assert.Equal(t, int64(2), v)
	assert.That(t, second.Equal(first))
}

// The probe reads the catalog: a missing table, a key column the table
// lacks, and a key with no exact unique index are the user's to fix, and
// say so. A partial unique index does not count.
func TestIntegrationSinkPostgres_ProbeChecksTheTarget(t *testing.T) {
	coverage.Covers(t, "sink.postgres")
	if testing.Short() {
		t.Skip("integration test: -short runs the unit pass only")
	}
	srv := startPostgres(t)
	ctx := context.Background()
	dsn, _ := srv.container.ConnectionString(ctx, "sslmode=disable")

	probe := func(table string, key []string) error {
		s, err := NewPostgresSink(config.PostgresSink{DSN: dsn, Table: table, Mode: PostgresModeUpsert, Key: key})
		assert.NoError(t, err)
		defer s.Close()
		return s.Probe(ctx)
	}

	err := probe("no_such_table", []string{"k"})
	assert.Error(t, err)
	assert.Equal(t, errs.CodeSinkInvalid, errs.CodeOf(err))
	assert.Equal(t, errs.ExitUserError, errs.ExitCode(err))

	pk := newKVTable(t, srv, "CREATE TABLE %s (k bigint PRIMARY KEY, v bigint)")
	assert.NoError(t, probe(pk, []string{"k"}))
	err = probe(pk, []string{"nope"})
	assert.Equal(t, errs.CodeSinkInvalid, errs.CodeOf(err))
	err = probe(pk, []string{"v"})
	assert.Equal(t, errs.CodeSinkInvalid, errs.CodeOf(err))

	// A unique index is a conflict target, as it is for native Postgres,
	// and the order of its columns does not matter.
	uix := newKVTable(t, srv, "CREATE TABLE %s (k bigint, v bigint, w bigint); CREATE UNIQUE INDEX ON %[1]s (w, k)")
	assert.NoError(t, probe(uix, []string{"k", "w"}))

	partial := newKVTable(t, srv, "CREATE TABLE %s (k bigint, v bigint); CREATE UNIQUE INDEX ON %[1]s (k) WHERE v > 0")
	err = probe(partial, []string{"k"})
	assert.Equal(t, errs.CodeSinkInvalid, errs.CodeOf(err))

	// append onto a table with a unique index is a warning, not an error.
	a, err := NewPostgresSink(config.PostgresSink{DSN: dsn, Table: pk, Mode: PostgresModeAppend})
	assert.NoError(t, err)
	defer a.Close()
	assert.NoError(t, a.Probe(ctx))
	assert.Equal(t, 1, len(a.Warnings()))
	assert.NoError(t, probe(newKVTable(t, srv, "CREATE TABLE %s (k bigint, v bigint)"), nil) == nil || true)
}

// A value the column refuses is the user's, exits 10, and is never retried;
// the batch stays buffered so the failure is visible in the depth gauge.
func TestIntegrationSinkPostgres_ARefusedValueIsAUserError(t *testing.T) {
	coverage.Covers(t, "sink.postgres")
	if testing.Short() {
		t.Skip("integration test: -short runs the unit pass only")
	}
	srv := startPostgres(t)
	ctx := context.Background()
	dsn, _ := srv.container.ConnectionString(ctx, "sslmode=disable")
	table := newKVTable(t, srv, "CREATE TABLE %s (k bigint PRIMARY KEY, v smallint NOT NULL)")
	s := upsertSink(t, dsn, table)

	big := kvTable(t, 1, 1<<40)
	defer big.Release()
	assert.NoError(t, s.WriteTable(ctx, big))
	err := s.Flush(ctx)
	assert.Error(t, err)
	assert.Equal(t, errs.ExitUserError, errs.ExitCode(err))
	assert.That(t, !retryable(err))
	assert.Equal(t, 1, s.BufferedRows())

	// A NOT NULL the batch omits is refused at the merge, naming the
	// target, not the staging table.
	nn := newKVTable(t, srv, "CREATE TABLE %s (k bigint PRIMARY KEY, v bigint NOT NULL, w bigint NOT NULL)")
	s2 := upsertSink(t, dsn, nn)
	row := kvTable(t, 1, 1)
	defer row.Release()
	assert.NoError(t, s2.WriteTable(ctx, row))
	err = s2.Flush(ctx)
	assert.Error(t, err)
	assert.Equal(t, errs.CodeSinkInvalid, errs.CodeOf(err))
	assert.That(t, !errors.Is(err, context.DeadlineExceeded))
}
```

Remove the last line of `TestIntegrationSinkPostgres_ProbeChecksTheTarget` (the `assert.NoError(... == nil || true)` line) before running; it is a leftover and asserts nothing. Replace it with:

```go
	plain, err := NewPostgresSink(config.PostgresSink{DSN: dsn, Table: newKVTable(t, srv, "CREATE TABLE %s (k bigint, v bigint)"), Mode: PostgresModeAppend})
	assert.NoError(t, err)
	defer plain.Close()
	assert.NoError(t, plain.Probe(ctx))
	assert.Equal(t, 0, len(plain.Warnings()))
```

The `1<<40` into `smallint` case: pgx encodes an int64 into `int2` and refuses it client-side when it does not fit, which is `user.sink.encode_failed` through `postgresCopyError`; if pgx instead sends it and Postgres answers `22003`, the code is the same. The test asserts the exit and the retry policy, which both paths share.

- [ ] **Step 2: Run them**

Run: `go test -count=1 -run 'TestIntegrationSinkPostgres_' ./internal/sinks -v 2>&1 | grep -E "^(--- |ok|FAIL)"`
Expected: every test PASS. The retry test is the one that catches a merge-all-at-once flush: with one transaction for both batches it fails with SQLSTATE 21000.

- [ ] **Step 3: Commit**

```bash
git add internal/sinks/postgres_integration_test.go docs/coverage/integrations/sink.postgres.yml
git commit -m "sinks: the Postgres sink's own promises against a real server: last row wins, retries apply in order, defaults, the probe"
```

---

### Task 8: Validate rules

**Files:**
- Create: `internal/validate/sinks.go`, `internal/validate/sinks_test.go`
- Modify: `internal/validate/window.go:93-98` and `:126-128`
- Modify: `internal/validate/validate.go:47-50`

**Interfaces:**
- Produces: `func checkSinks(rendered []byte, rep *Report)` registering check id `sinks.postgres`; `func appendsOnly(s config.Sink) bool`.

- [ ] **Step 1: Write the failing tests**

`internal/validate/sinks_test.go`:

```go
package validate

import (
	"context"
	"strings"
	"testing"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

const postgresWindowedConfig = `commands:
  - name: attach
    sql: |
      ATTACH 'postgresql://u:p@localhost:5432/db' AS pg (TYPE POSTGRES);
tables:
  sql:
    - name: agg
      sql: CREATE TABLE agg (bucket TIMESTAMPTZ, count INT)
      window:
        time_column: bucket
        size_seconds: 60
        late_rows: %s
        sink:
%s
pipeline:
  batch_size: 1
  source:
    type: kafka
    kafka:
      brokers: ["localhost:9092"]
      group_id: g
      auto_offset_reset: earliest
      topics: ["t"]
  handler:
    type: handlers.InferredMemBatch
    sql: SELECT 1
  sink:
%s
`

const (
	upsertSink = `          type: postgres
          postgres:
            dsn: postgres://u:p@localhost:5432/db
            table: agg
            mode: upsert
            key: [bucket]`
	appendSink = `          type: postgres
          postgres:
            dsn: postgres://u:p@localhost:5432/db
            table: agg
            mode: append`
	conflictSink = `          type: sqlcommand
          sqlcommand:
            sql: INSERT INTO pg.agg SELECT * FROM sqlflow_sink_batch ON CONFLICT (bucket) DO UPDATE SET count = EXCLUDED.count`
	noopSink = `    type: noop`
)

func validateSinks(t *testing.T, lateRows, windowSink, pipelineSink string) Report {
	t.Helper()
	cfg := strings.Replace(postgresWindowedConfig, "%s", lateRows, 1)
	cfg = strings.Replace(cfg, "%s", windowSink, 1)
	cfg = strings.Replace(cfg, "%s", pipelineSink, 1)
	rep, err := Validate(context.Background(), Request{Path: "p.yml", Config: cfg})
	assert.NoError(t, err)
	return rep
}

func sinkDiagnostics(rep Report) []Diagnostic {
	var out []Diagnostic
	for _, d := range rep.Diagnostics {
		if strings.Contains(d.Message, "sink") {
			out = append(out, d)
		}
	}
	return out
}

// upsert with reemit is refused: the sink replaces a bucket's row with what
// it is handed, and a reemit hands it only the late rows.
func TestValidateSchema_PostgresUpsertRefusesReemit(t *testing.T) {
	coverage.Covers(t, "validate.schema")
	rep := validateSinks(t, "reemit", upsertSink, noopSink)
	assert.That(t, !rep.OK)
	diags := sinkDiagnostics(rep)
	assert.Equal(t, 1, len(diags))
	assert.Equal(t, SeverityError, diags[0].Severity)
	assert.That(t, strings.Contains(diags[0].Message, "late_rows is reemit"))

	assert.That(t, validateSinks(t, "drop", upsertSink, noopSink).OK)
	assert.Equal(t, 0, len(sinkDiagnostics(validateSinks(t, "drop", upsertSink, noopSink))))
}

// append with reemit warns, as kafka and iceberg do.
func TestValidateSchema_PostgresAppendWarnsOnReemit(t *testing.T) {
	coverage.Covers(t, "validate.schema")
	rep := validateSinks(t, "reemit", appendSink, noopSink)
	assert.That(t, rep.OK)
	diags := sinkDiagnostics(rep)
	assert.Equal(t, 1, len(diags))
	assert.Equal(t, SeverityWarning, diags[0].Severity)
	assert.That(t, strings.Contains(diags[0].Message, "the postgres sink appends"))
}

// A sqlcommand upsert into an attached Postgres reads the whole target
// table per flush; the warning names the cost and the keyed sink, on the
// window sink and on the pipeline sink alike.
func TestValidateSchema_SqlcommandUpsertIntoPostgresWarns(t *testing.T) {
	coverage.Covers(t, "validate.schema")
	rep := validateSinks(t, "drop", conflictSink, noopSink)
	assert.That(t, rep.OK)
	diags := sinkDiagnostics(rep)
	assert.Equal(t, 1, len(diags))
	assert.Equal(t, SeverityWarning, diags[0].Severity)
	assert.That(t, strings.Contains(diags[0].Message, "whole target table"))

	pipelineConflict := strings.ReplaceAll(conflictSink, "          ", "    ")
	rep = validateSinks(t, "drop", strings.ReplaceAll(noopSink, "    ", "          "), pipelineConflict)
	assert.Equal(t, 1, len(sinkDiagnostics(rep)))

	// Without an attached Postgres the statement is DuckDB's own, and the
	// extension's cost does not apply.
	rep, err := Validate(context.Background(), Request{Path: "p.yml", Config: strings.Replace(
		strings.Replace(strings.Replace(postgresWindowedConfig, "%s", "drop", 1), "%s", conflictSink, 1), "%s", noopSink, 1),
	})
	assert.NoError(t, err)
	_ = rep
	local := strings.Replace(strings.Replace(strings.Replace(postgresWindowedConfig, "%s", "drop", 1), "%s", conflictSink, 1), "%s", noopSink, 1)
	local = strings.Replace(local, "(TYPE POSTGRES)", "(TYPE DUCKDB)", 1)
	rep, err = Validate(context.Background(), Request{Path: "p.yml", Config: local})
	assert.NoError(t, err)
	assert.Equal(t, 0, len(sinkDiagnostics(rep)))
}

// The block's own shape: mode is required, upsert needs a key, append
// refuses one.
func TestValidateSchema_PostgresBlockShape(t *testing.T) {
	coverage.Covers(t, "validate.schema")
	for _, c := range []struct {
		block string
		want  string
	}{
		{"          type: postgres\n          postgres:\n            dsn: x\n            table: t", "mode is required"},
		{"          type: postgres\n          postgres:\n            dsn: x\n            table: t\n            mode: upsert", "needs key"},
		{"          type: postgres\n          postgres:\n            dsn: x\n            table: t\n            mode: append\n            key: [a]", "takes no key"},
	} {
		rep := validateSinks(t, "drop", c.block, noopSink)
		assert.That(t, !rep.OK)
		found := false
		for _, d := range sinkDiagnostics(rep) {
			if strings.Contains(d.Message, c.want) {
				found = true
			}
		}
		assert.That(t, found)
	}
}
```

- [ ] **Step 2: Run them to see them fail**

Run: `go test ./internal/validate -run 'TestValidateSchema_Postgres|TestValidateSchema_Sqlcommand' 2>&1 | tail -5`
Expected: FAIL. The reemit case reports the wrong severity, and the others report no diagnostic.

- [ ] **Step 3: Write the rules**

`internal/validate/sinks.go`:

```go
package validate

import (
	"fmt"
	"regexp"

	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/errs"
	"gopkg.in/yaml.v3"
)

// checkSinks checks every sink block, the pipeline's and each window's,
// without running anything.
//
//   - A postgres block missing mode, an upsert without a key, or an append
//     with one. The sink refuses these at start; validate says so first.
//   - A window whose postgres sink upserts and whose late_rows is reemit.
//     The sink replaces a bucket's row with what it is handed, and a reemit
//     hands it emit_sql over the late rows alone. Refused.
//   - A sqlcommand sink whose SQL carries ON CONFLICT while a command
//     attaches a Postgres. The DuckDB postgres extension runs that upsert by
//     copying every row's key from the target into DuckDB, so a flush costs
//     the table, not the batch. A warning that names the postgres sink.
func checkSinks(rendered []byte, rep *Report) {
	var root yaml.Node
	if err := yaml.Unmarshal(rendered, &root); err != nil {
		rep.SetCheck("sinks.postgres", StatusSkipped, "the config did not parse, so there were no sinks to check")
		return
	}
	var conf config.Conf
	if err := root.Decode(&conf); err != nil {
		rep.SetCheck("sinks.postgres", StatusSkipped, "the config did not decode, so there were no sinks to check")
		return
	}

	status := StatusPass
	fail := func(msg string, pos *Position) {
		status = StatusFail
		rep.Add(diagnostic(errs.CodeConfigInvalid, SeverityError, msg, pos))
	}
	warn := func(msg string, pos *Position) {
		rep.Add(diagnostic(errs.CodeConfigInvalid, SeverityWarning, msg, pos))
	}

	attached := attachesPostgres(conf)

	doc := &root
	if doc.Kind == yaml.DocumentNode && len(doc.Content) > 0 {
		doc = doc.Content[0]
	}
	pipelineSink := mappingValue(mappingValue(doc, "pipeline"), "sink")
	checkSink("pipeline.sink", conf.Pipeline.Sink, pipelineSink, attached, fail, warn)

	if conf.Tables != nil {
		for i, table := range conf.Tables.SQL {
			if table.Window == nil {
				continue
			}
			node := windowNode(&root, i)
			where := fmt.Sprintf("tables.sql[%d] window sink", i)
			checkSink(where, table.Window.Sink, mappingValue(node, "sink"), attached, fail, warn)

			if table.Window.Sink.Type == "postgres" && table.Window.Sink.Postgres != nil &&
				table.Window.Sink.Postgres.Mode == "upsert" && table.Window.LateRows == "reemit" {
				fail(fmt.Sprintf("tables.sql[%d] window: late_rows is reemit and the postgres sink upserts. "+
					"A reemit publishes emit_sql over the late rows alone, and the sink replaces the "+
					"bucket's row with that. Use drop", i), position(mappingKey(node, "late_rows")))
			}
		}
	}

	rep.SetCheck("sinks.postgres", status, "")
}

// checkSink holds one sink block to the rules that need no window.
func checkSink(where string, s config.Sink, node *yaml.Node, attached bool, fail, warn func(string, *Position)) {
	switch s.Type {
	case "postgres":
		if s.Postgres == nil {
			fail(where+": type postgres needs a postgres block with dsn, table and mode", position(node))
			return
		}
		pos := position(mappingValue(node, "postgres"))
		switch s.Postgres.Mode {
		case "":
			fail(where+": postgres mode is required: upsert or append", pos)
		case "upsert":
			if len(s.Postgres.Key) == 0 {
				fail(where+": postgres mode upsert needs key, the columns a row is identified by", pos)
			}
		case "append":
			if len(s.Postgres.Key) > 0 {
				fail(where+": postgres mode append takes no key; every row is inserted as it is", pos)
			}
		}
	case "sqlcommand":
		if attached && s.SQLCommand != nil && onConflict.MatchString(s.SQLCommand.SQL) {
			warn(where+": ON CONFLICT through the DuckDB postgres extension copies every row's key "+
				"from the whole target table into DuckDB on every flush, so a flush costs the table, not "+
				"the batch. The postgres sink does the same write at the cost of the batch: "+
				"type: postgres with table, mode: upsert and key", position(mappingValue(node, "sqlcommand")))
		}
	}
}

var (
	onConflict     = regexp.MustCompile(`(?i)\bON\s+CONFLICT\b`)
	attachPostgres = regexp.MustCompile(`(?is)\bATTACH\b.*\bTYPE\s+POSTGRES\b`)
)

// attachesPostgres reports whether any command attaches a Postgres.
func attachesPostgres(conf config.Conf) bool {
	for _, c := range conf.Commands {
		if attachPostgres.MatchString(c.SQL) {
			return true
		}
	}
	return false
}
```

Check the command struct's field names: `grep -n -A6 "^type Command struct" internal/config/config.go`. If the list is not `conf.Commands` with a `SQL` field, use the names it has.

In `internal/validate/window.go`, change `appendsOnly(w.Sink.Type)` to `appendsOnly(w.Sink)` and the message's `%s sink appends` argument from `w.Sink.Type` stays. Replace the function:

```go
// appendsOnly reports a sink that cannot replace a row it already holds.
// Iceberg appends by design, a Kafka topic is a log, and the postgres sink
// in append mode inserts every row. The sqlcommand and ClickHouse sinks
// depend on the SQL or the table engine, so they are the user's call.
func appendsOnly(s config.Sink) bool {
	switch s.Type {
	case "iceberg", "kafka":
		return true
	case "postgres":
		return s.Postgres != nil && s.Postgres.Mode == "append"
	}
	return false
}
```

In `internal/validate/validate.go`, after `checkWindows(rendered, &rep)` add `checkSinks(rendered, &rep)`.

- [ ] **Step 4: Run the package**

Run: `go test ./internal/validate`
Expected: PASS, including the existing window tests. If `TestValidateSchema_ReemitOnAnAppendOnlySinkWarns` asserts an exact diagnostic count, the new check adds none for a kafka sink, so it still passes. If the `sinks.postgres` check appears in a golden report somewhere (`grep -rn "tables.window" internal/validate/testdata internal/cli/testdata`), add the new check to that golden the same way.

- [ ] **Step 5: Commit**

```bash
git add internal/validate
git commit -m "validate: refuse upsert with reemit, warn on append with reemit, and name the cost of ON CONFLICT through the extension"
```

---

### Task 9: Examples, the window loop through the sink, and the CHANGELOG

**Files:**
- Modify: `dev/config/examples/bluesky/bluesky.postgres.windowed.yml`
- Modify: `dev/config/examples/kafka.postgres.sink.yml`
- Modify: `internal/managers/window_leakloop_test.go`
- Modify: `CHANGELOG.md`

- [ ] **Step 1: Move the windowed example to the sink**

In `dev/config/examples/bluesky/bluesky.postgres.windowed.yml`:

Delete the two commands `load postgres extension` and `attach postgres` (lines 4-11), leaving `declare the post schema`. Replace the window's sink block (from `        sink:` through the end of the `sql: |` statement) with:

```yaml
        sink:
          # A keyed sink replaces the row a (bucket, lang) already identifies,
          # so a bucket republished after a crash lands harmlessly. The write
          # costs the batch: a COPY into a staging table and a server-side
          # merge, in one transaction. The target needs PRIMARY KEY (bucket,
          # lang) or a unique index on the pair; the sink checks at startup.
          type: postgres
          postgres:
            dsn: "{{ SQLFLOW_POSTGRES_URI|default('postgresql://postgres:postgres@sqlflow-postgres:5432/postgres') }}"
            table: posts_per_minute_by_lang
            mode: upsert
            key: [bucket, lang]
```

Update the `late_rows: drop` comment's last sentence from `and ON CONFLICT would replace the bucket's count with theirs` to `and the upsert would replace the bucket's count with theirs`. Update the file's header comment: `writes each closed window to Postgres` stays true.

In `dev/config/examples/kafka.postgres.sink.yml`, delete the two commands and the `commands:` key, and replace the sink block with:

```yaml
  sink:
    type: postgres
    postgres:
      dsn: "{{ SQLFLOW_POSTGRES_USERS_URI|default('postgresql://postgres:postgres@localhost:5432/testdb') }}"
      table: user_action
      mode: append
```

Run: `go test -short ./internal/cli -run TestConfigValidation_ExampleConfigsBuildRealComponents -v 2>&1 | grep -E "postgres|--- (PASS|FAIL|SKIP)" | head`
Expected: both examples SKIP with "needs a resource this test cannot provide", never FAIL. Then `go run ./cmd/sqlflow validate dev/config/examples/bluesky/bluesky.postgres.windowed.yml` prints no error.

- [ ] **Step 2: Run the windowed example end to end**

The local dev Postgres is `dev_postgres_1` on 127.0.0.1:5432, user and password `postgres`, and `dev/bench/bluesky/postgres.sql` creates the demo's table. In one shell, serve the capture: `go run ./dev/bench/replay -file /private/tmp/claude-501/-Users-danielmican-code-github-com-turbolytics-turbolytics-io/3e1b0f27-9432-4625-bbed-e4a23a9a64a6/scratchpad/bench/jetstream-posts-2m.ndjson.gz -speed 50 -limit 200000`. In another:

```bash
PGPASSWORD=postgres psql -h 127.0.0.1 -U postgres -c "DROP DATABASE IF EXISTS example_bluesky" -c "CREATE DATABASE example_bluesky"
PGPASSWORD=postgres psql -h 127.0.0.1 -U postgres -d example_bluesky -f dev/bench/bluesky/postgres.sql
SQLFLOW_POSTGRES_URI='postgresql://postgres:postgres@127.0.0.1:5432/example_bluesky' \
  go run ./cmd/sqlflow run dev/config/examples/bluesky/bluesky.postgres.windowed.yml --max-msgs 200000
PGPASSWORD=postgres psql -h 127.0.0.1 -U postgres -d example_bluesky -Atc "SELECT count(*), min(bucket), max(bucket), sum(posts) FROM posts_per_minute_by_lang"
```

Set the websocket `uri` to `ws://127.0.0.1:8765/subscribe` for the run by exporting the template variable the example uses, or by a copy of the file in the scratchpad with the uri replaced; do not commit that change. Expected: the pipeline exits 0, the count is in the hundreds, and `sum(posts)` is within a few percent of 200,000 times the capture's create fraction (0.93). Paste the psql line into the commit message.

Then stop Postgres under it: start the same run without `--max-msgs`, `docker stop dev_postgres_1` after a minute, and read the exit code: expected 12, with `system.sink.unreachable` in the last log line. `docker start dev_postgres_1` afterwards.

- [ ] **Step 3: The window loop through the sink**

In `internal/managers/window_leakloop_test.go`, add after `TestWindowDemo__PostgresUpsertPerMessage`:

```go
// TestWindowDemo__PostgresSinkPerMessage publishes every closed minute
// through the keyed postgres sink into SQLFLOW_LEAK_POSTGRES: the write the
// demo moves to. Expected at the plain-insert rate.
func TestWindowDemo__PostgresSinkPerMessage(t *testing.T) {
	dsn := os.Getenv("SQLFLOW_LEAK_POSTGRES")
	if dsn == "" {
		t.Fatal("SQLFLOW_LEAK_POSTGRES is required: a Postgres connection string as this process sees it")
	}
	conn, err := pgx.Connect(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	for _, sql := range []string{
		`DROP TABLE IF EXISTS leakloop_sink`,
		`CREATE TABLE leakloop_sink (bucket TIMESTAMPTZ NOT NULL, lang TEXT NOT NULL, posts INTEGER NOT NULL,
			updated_at TIMESTAMPTZ NOT NULL DEFAULT now(), PRIMARY KEY (bucket, lang))`,
	} {
		if _, err := conn.Exec(context.Background(), sql); err != nil {
			t.Fatal(err)
		}
	}
	conn.Close(context.Background())

	s, err := sinks.NewPostgresSink(config.PostgresSink{DSN: dsn, Table: "leakloop_sink", Mode: sinks.PostgresModeUpsert, Key: []string{"bucket", "lang"}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	runDemoWindow(t, "demo window, postgres sink", s, nil)
}
```

Add `"github.com/jackc/pgx/v5"` to the imports. Run on Linux, one process per loop:

```bash
B=/private/tmp/claude-501/-Users-danielmican-code-github-com-turbolytics-turbolytics-io/3e1b0f27-9432-4625-bbed-e4a23a9a64a6/scratchpad/bench
docker image rm sqlflow-leakloop 2>/dev/null; SQLFLOW_LEAK_SCALE=40 SQLFLOW_LEAK_JETSTREAM=$B/jetstream-posts-2m.ndjson.gz \
SQLFLOW_LEAK_POSTGRES=postgresql://postgres:postgres@dev_postgres_1:5432/bench_bluesky \
  dev/bench/leakloops.sh $B/window-loops-sink 'TestWindowDemo__' ./internal/managers
```

The image is rebuilt because `go.mod` changed. Expected: three lines with `per message after warm-up`; the postgres sink row's native rate is within a few bytes a message of the counting sink's, against the sqlcommand upsert's roughly 11 bytes above it. Copy the three lines for the PR description.

- [ ] **Step 4: The CHANGELOG**

Under `## Unreleased`, `### Added`, prepend:

```markdown
- A `postgres` sink. `type: postgres` with `dsn`, `table`, `mode: upsert | append`
  and, for upsert, `key`. Each batch is a `COPY` into a staging table and a
  server-side `INSERT ... ON CONFLICT`, in one transaction, at the cost of the
  batch rather than the table. Two rows with one key in a batch: the last one
  in the batch wins. The probe checks the table exists and that a unique index
  or constraint covers the key. Failures classify from SQLSTATE: a refused or
  lost connection exits 12 and retries; a refused value or a missing column
  exits 10.
- `sqlflow validate` refuses `late_rows: reemit` with a postgres sink in
  upsert mode, warns on it in append mode, and warns on a `sqlcommand` sink
  whose SQL carries `ON CONFLICT` into an attached Postgres, naming the cost:
  the DuckDB postgres extension copies every row's key from the target table
  on every flush.
- The image sets `MALLOC_ARENA_MAX=2`.
- `sink.flush.idempotent_on_key` in the conformance registry: delivering the
  same batch twice leaves a keyed sink's destination holding it once.
```

And under a `### Changed` heading (create it after `### Added` if absent):

```markdown
- `bluesky.postgres.windowed.yml` and `kafka.postgres.sink.yml` write through
  the `postgres` sink. Neither attaches Postgres through DuckDB any more.
```

- [ ] **Step 5: Commit**

```bash
git add dev/config/examples CHANGELOG.md internal/managers/window_leakloop_test.go
git commit -m "examples: the Postgres examples write through the keyed sink; the demo's window loop through it"
```

Put the psql read-back line and the exit-12 log line in the commit body.

---

### Task 10: Verification, the PR description, and the push

**Files:**
- Modify: the PR description of #290 via `gh pr edit 290 --body-file`, from `scratchpad/bench/pr-body.md`

- [ ] **Step 1: The full unit pass**

```bash
gofmt -l internal cmd dev
go vet ./...
go test -short -race -count=1 ./...
uv run --locked pytest tests/tooling -q
make coverage-page && git status --short docs/coverage
```

Expected: `gofmt` prints nothing, vet clean, every package ok, 205 or more tooling tests pass, and `docs/coverage` shows only `matrix.md` and the new page content changed by the registry additions, if anything. Do not edit `docs/coverage/status/`.

- [ ] **Step 2: The integration pass**

```bash
go test -count=1 -run '^TestIntegrationSinkPostgres' ./internal/sinks -v 2>&1 | grep -E "^(--- |ok|FAIL)"
go test -count=1 -run '^TestIntegration' ./internal/conformance ./internal/sinks 2>&1 | tail -5
```

Expected: every Postgres test PASS, and the existing ClickHouse, Kafka and Iceberg integration tests unchanged.

- [ ] **Step 3: The release suite, because the image changed**

The Dockerfile gained an environment variable and the binary gained a sink, so the release suite runs:

```bash
make sqlflow-image 2>&1 | tail -2
SQLFLOW_IMAGE=$(docker images --format '{{.Repository}}:{{.Tag}}' | grep -m1 'sql-flow') uv run --locked pytest tests/release -q 2>&1 | tail -3
```

Find the tag `make sqlflow-image` produced from its output and use it. Expected: 19 or more passed. Never run `make release-image`.

- [ ] **Step 4: The soak**

`make soak` is required by the PR template when a sink changes. Run it from a copy of the harness, as the memory notes say, with the image from Step 3:

```bash
SQLFLOW_IMAGE=<the tag from step 3> scripts/soak.sh 10 postgres-sink 2>&1 | tail -12
```

Paste the verdict block into the PR description's "Soak verdict" section. Expected: PASS at under 0.1 B/msg, the same as the two soaks already in the description.

- [ ] **Step 5: Update the PR description**

Edit `scratchpad/bench/pr-body.md`:

- Change the first paragraph of "What this changes" to say the branch carries the fix: the sink, the validate rules, the invariant, the examples, and the image setting, and link the spec and this plan by path.
- Add to the loop table in finding 3 the three lines from Task 9 Step 3 as an "after" row.
- Replace "The path forward" with "What landed": one paragraph per bullet of the spec's "Done when", each with the test or command that proves it.
- Fill "Verification" with the outputs of Steps 1 to 4.

Then:

```bash
git push origin bench/component-leak-loops
gh pr edit 290 --body-file /private/tmp/claude-501/-Users-danielmican-code-github-com-turbolytics-turbolytics-io/3e1b0f27-9432-4625-bbed-e4a23a9a64a6/scratchpad/bench/pr-body.md
gh pr ready 290
```

Expected: CI green on Go, Integration, Tooling, Image and Coverage. If Coverage fails on `sink.postgres` requiring `integration` evidence, read its log: the Integration job must have run `TestIntegrationSinkPostgres_Conformance`, and a Docker permission problem there is CI's, not the branch's.

---

## Self-review

**Spec coverage.** Section 1 (config): Task 1 and Task 5's constructor. Section 2 (one flush, per batch, staging, DISTINCT ON, defaults on omitted columns, context): Task 2's builders, Task 5's `send` and `Flush`, Task 7's tests. Section 3 (startup checks, partial index, append warning): Task 5's `Probe`, Task 7's probe test, deviation 3 for the logger. Section 4 (SQLSTATE table): Task 4. Section 5 (types): Task 3 and the integration file in Task 6. Section 6 (validate): Task 8. Section 7 (conformance, invariant, exemptions, feature): Task 6. Section 8 (examples): Task 9. Section 9 (image): already on the branch. Client library: Task 1. Done-when: Tasks 7, 9 and 10 carry each bullet, including the retry-with-shared-key test and the two-rows-one-key test.

**Placeholders.** None: every step carries its code or its command. Two conditional branches are stated with their exact resolution (a rendering mismatch in the type table; the honours_context skip).

**Type consistency.** `NewPostgresSink(config.PostgresSink)` in Tasks 5, 6, 7, 9. `PostgresModeUpsert`, `PostgresModeAppend` from Task 2 used in 5, 6, 7, 9. `postgresRows`, `postgresValue` from Task 3 used in 5. `postgresError`, `postgresCopyError` from Task 4 used in 5. `core.KeyedSink.Key() []string` from Task 5 used in 6. `startPostgres`, `proxyDSN`, `idTable` from Task 6 used in 7. `appendsOnly(config.Sink)` in Task 8 matches its one call site.
