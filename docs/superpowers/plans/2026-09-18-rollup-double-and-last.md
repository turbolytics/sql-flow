# Rollup Double Sums and `last` Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let `sqlflow rollup` keep a sum's fraction and keep a bucket's most recent value, so a metrics table with gauges can use the generated ladder.

**Architecture:** A `sum` measure gains `numeric: integer|double`, which picks the cast the generator writes. A new `last` measure generates an ordered `array_agg` over the finer rows of a bucket, newest first. Tables are created with `CREATE TABLE … AS SELECT … WITH NO DATA`, so a column's type is the type of the expression that fills it: no DDL type list changes. The check refuses the two places `last` has no meaning: a dimension set that drops a source dimension, and a folded serve dataset.

**Tech Stack:** Go, `github.com/zeebo/assert`, `coverage.Covers`, testcontainers Postgres 18 for the integration pass, `make schema`.

**Spec:** `docs/superpowers/specs/2026-09-18-rollup-double-and-last-design.md`

## Global Constraints

- A declaration that uses neither addition generates byte-identical output. `internal/rollup/testdata/bluesky.postgres.sql` and `bluesky.serve.yml` must not change. If either changes, the task is wrong.
- `numeric` default is `integer`, today's `::bigint`.
- `avg`, `gauge` and `histogram` stay reserved and refused.
- Every test calls `coverage.Covers(t, "cli.rollup")` first. Unit test names start `TestCliRollup_`; integration test names start `TestIntegrationRollup_` and skip under `testing.Short()`.
- All prose follows `CLAUDE.md`. Comments explain why.
- Commit messages name the defect, the fix, and the evidence.
- Branch from `main`: `feat/rollup-double-and-last`.

## File Structure

| File | Change |
|---|---|
| `internal/config/rollup.go` | `last` in the type enum; `RollupMeasure.Numeric`. |
| `internal/config/rollup_check.go` | Admit `last`; rules for `numeric`, for `last` with dropped dimensions, for `last` under a fold. |
| `internal/config/rollup_check_test.go` | The new rules, from their own fixture. |
| `internal/validate/schemas/rollups.json` | Regenerated. |
| `internal/rollup/sql.go` | `sumCast`, `lastExpr`, the `sum` and `last` arms of `sourceExpr` and `mergeExpr`. |
| `internal/rollup/serve.go` | The `sum` arm of the folded outer select casts by `numeric`. |
| `dev/config/rollups/metrics.yml` | A second example declaration: a set of three dimensions and five measures, a set that drops one dimension and has no `last`, no `serve:`. |
| `internal/rollup/testdata/metrics.postgres.sql` | Its golden. |
| `internal/rollup/postgres_test.go`, `serve_test.go` | Golden and expression tests. |
| `internal/rollup/metrics_integration_test.go` | The measures against a real Postgres. |
| `README.md`, `CHANGELOG.md` | The measures paragraph, and an Unreleased entry. |

---

### Task 1: the config admits `last` and `numeric`, and refuses their misuse

**Files:**
- Modify: `internal/config/rollup.go:68-76`
- Modify: `internal/config/rollup_check.go` (`rollupMeasureTypes` line 15, `checkMeasure` near line 178, `checkRollupDataset` near line 257)
- Modify: `internal/config/rollup_check_test.go`
- Regenerate: `internal/validate/schemas/rollups.json`

**Interfaces:**
- Consumes: nothing new.
- Produces: `RollupMeasure.Numeric string` (`""`, `"integer"` or `"double"`), and the measure type `"last"`. Tasks 2 and 3 switch on both.

- [ ] **Step 1: Write the failing tests**

Append to `internal/config/rollup_check_test.go`:

```go
// lastRollups is the metrics template's declaration: every source dimension
// kept, a double sum, and a last. It has no serve block, because a served
// dataset takes at most one dimension.
const lastRollups = `
rollups:
  - name: metrics
    source:
      table: metrics_1m
      time_column: bucket
      grain: 1m
      dimensions: [name, type, dimensions_key]
    grains:
      5m: {from: 1m}
      1h: {from: 5m}
    dimension_sets:
      - name: metrics
        dimensions: [name, type, dimensions_key]
        measures:
          value_sum: {type: sum, column: value_sum, numeric: double}
          value_count: {type: sum, column: value_count}
          value_last: {type: last, column: value_last}
`

func TestCliRollup_CheckAcceptsLastAndADoubleSum(t *testing.T) {
	coverage.Covers(t, "cli.rollup")
	assert.Equal(t, 0, len(parseRollups(t, lastRollups).Check()))
}

func TestCliRollup_CheckReportsEachLastAndNumericRule(t *testing.T) {
	coverage.Covers(t, "cli.rollup")

	for _, tt := range []struct {
		name     string
		from, to string
		path     string
		message  string
	}{
		{"last without a column", "{type: last, column: value_last}", "{type: last}",
			"rollups.0.dimension_sets.0.measures.value_last.column", "needs column"},
		{"numeric on a last", "{type: last, column: value_last}", "{type: last, column: value_last, numeric: double}",
			"rollups.0.dimension_sets.0.measures.value_last.numeric", "only a sum takes numeric"},
		{"numeric not a kind", "numeric: double", "numeric: decimal",
			"rollups.0.dimension_sets.0.measures.value_sum.numeric", "use integer or double"},
		// Two series share a bucket once a dimension is dropped, and neither
		// is the later one.
		{"last in a set that drops a dimension", "        dimensions: [name, type, dimensions_key]", "        dimensions: [name, type]",
			"rollups.0.dimension_sets.0.measures.value_last.type", "keeps every source dimension"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			text := strings.Replace(lastRollups, tt.from, tt.to, 1)
			assert.That(t, text != lastRollups)
			violations := parseRollups(t, text).Check()
			if len(violations) != 1 {
				t.Fatalf("want 1 violation, got %d: %+v", len(violations), violations)
			}
			assert.Equal(t, errs.CodeConfigRollup, violations[0].Code)
			assert.Equal(t, tt.path, strings.Join(violations[0].Path, "."))
			if !strings.Contains(violations[0].Message, tt.message) {
				t.Fatalf("message %q does not contain %q", violations[0].Message, tt.message)
			}
		})
	}
}

// A folded dataset sums the values past the top into "other", and the last
// value of many series is no series' value.
func TestCliRollup_CheckRefusesLastUnderAFold(t *testing.T) {
	coverage.Covers(t, "cli.rollup")

	text := strings.Replace(validRollups,
		"          posts: {type: sum, column: posts}\n      - name: posts_total",
		"          posts: {type: sum, column: posts}\n          latest: {type: last, column: posts}\n      - name: posts_total", 1)
	assert.That(t, text != validRollups)
	violations := parseRollups(t, text).Check()
	if len(violations) != 1 {
		t.Fatalf("want 1 violation, got %d: %+v", len(violations), violations)
	}
	assert.Equal(t, "rollups.0.serve.datasets.0.dimension_set", strings.Join(violations[0].Path, "."))
	assert.That(t, strings.Contains(violations[0].Message, "latest is last"))
}
```

The "dimension set drops a dimension" edit replaces the first match of the eight-space-indented `dimensions:` line, which is the dimension set's. The source's line is indented six spaces and does not match.

In the existing table in `TestCliRollup_CheckReportsEachRuleAtItsPath`, the `"unknown measure type"` case expects the message `"use sum, min, max or count_buckets"`. Change that expectation to `"use sum, min, max, last or count_buckets"`.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test ./internal/config/ -run 'TestCliRollup_Check'`
Expected: FAIL. `TestCliRollup_CheckAcceptsLastAndADoubleSum` fails at parse: strict decoding refuses the unknown field `numeric`.

- [ ] **Step 3: Implement the type**

In `internal/config/rollup.go`, replace the `RollupMeasure` struct:

```go
// RollupMeasure is one kept value and how it merges.
type RollupMeasure struct {
	// sum, min, max, last or count_buckets. last keeps the value of the
	// latest finer bucket. avg, gauge and histogram are reserved and refused
	// until a later version generates them.
	Type string `yaml:"type" jsonschema:"enum=sum,enum=min,enum=max,enum=last,enum=count_buckets,enum=avg,enum=gauge,enum=histogram"`
	// The source column the measure reads. Required for sum, min, max and
	// last; refused for count_buckets, which counts source buckets.
	Column string `yaml:"column,omitempty"`
	// How a sum is stored: integer, the default, is a bigint, and double is
	// a double precision, which keeps a fraction. Refused on any other type:
	// min, max and last take the source column's type.
	Numeric string `yaml:"numeric,omitempty" jsonschema:"enum=integer,enum=double"`
}
```

- [ ] **Step 4: Implement the rules**

In `internal/config/rollup_check.go`, admit `last`:

```go
	rollupMeasureTypes         = map[string]bool{"sum": true, "min": true, "max": true, "last": true, "count_buckets": true}
```

In `checkMeasure`, change both messages that read `use sum, min, max or count_buckets` to `use sum, min, max, last or count_buckets`. Then append two switches to the end of the function:

```go
	switch {
	case m.Numeric == "":
	case m.Type != "sum":
		add(at(path, "numeric"), "rollup %s dimension set %s: measure %s is %s, and only a sum takes numeric; the others take the source column's type",
			r.Name, set.Name, name, m.Type)
	case m.Numeric != "integer" && m.Numeric != "double":
		add(at(path, "numeric"), "rollup %s dimension set %s: measure %s has numeric %q; use integer or double",
			r.Name, set.Name, name, m.Numeric)
	}

	// The latest finer bucket is one row only while the set's key is the
	// source's key. Drop a dimension and two series share a bucket.
	if m.Type == "last" && len(set.Dimensions) != len(r.Source.Dimensions) {
		add(at(path, "type"), "rollup %s dimension set %s: measure %s is last, which needs a set that keeps every source dimension; this one keeps %d of %d",
			r.Name, set.Name, name, len(set.Dimensions), len(r.Source.Dimensions))
	}
```

In `checkRollupDataset`, inside `if len(set.Dimensions) == 1 {`, extend the loop that refuses `count_buckets`:

```go
		for _, name := range set.MeasureNames() {
			switch set.Measures[name].Type {
			case "count_buckets":
				add(at(path, "dimension_set"), "rollup %s dataset %s: measure %s is count_buckets, which cannot be summed across %s values into other",
					r.Name, ds.Name, name, dim)
			case "last":
				add(at(path, "dimension_set"), "rollup %s dataset %s: measure %s is last, and the last value of the %s values folded into other is no series' value; write the dataset in serve.yml",
					r.Name, ds.Name, name, dim)
			}
		}
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `go test ./internal/config/ -run 'TestCliRollup_'`
Expected: PASS.

If `TestCliRollup_CheckRefusesLastUnderAFold` reports two violations, the second is the dropped-dimension rule: `validRollups` has one source dimension and `posts_by_lang` keeps it, so it must not fire. Re-read the `len` comparison.

- [ ] **Step 6: Regenerate the schema**

Run: `make schema`
Run: `git diff --stat internal/validate/schemas internal/cli/testdata`
Expected: only `rollups.json` changes.

- [ ] **Step 7: Commit**

```bash
git add internal/config/rollup.go internal/config/rollup_check.go internal/config/rollup_check_test.go internal/validate/schemas/rollups.json
git commit -m "rollup: the declaration admits a last measure and a double sum

A rollup declaration could not say that a sum keeps its fraction, or
that a measure is the bucket's most recent value. A metrics table with
gauges needs both.

A sum takes numeric: integer or double. last joins the measure types.
The check refuses numeric on any other type, a last in a set that drops
a source dimension, where two series share a bucket and neither is
later, and a last under a fold, where other has no last value.

The generator does not emit either yet: PostgresDDL panics on last until
the next commit."
```

---

### Task 2: the generator writes the double cast and the `last` expression

**Files:**
- Modify: `internal/rollup/sql.go:63-90`
- Modify: `internal/rollup/serve.go:128-137`
- Create: `dev/config/rollups/metrics.yml`
- Create: `internal/rollup/testdata/metrics.postgres.sql` (generated)
- Test: `internal/rollup/postgres_test.go`, `internal/rollup/serve_test.go`

**Interfaces:**
- Consumes: `config.RollupMeasure.Numeric` and type `"last"` from Task 1.
- Produces: `func mergeExpr(r config.Rollup, name string, m config.RollupMeasure) string`. The signature gains `r`; `selectList` is its only caller. `dev/config/rollups/metrics.yml` and `const metricsPath` for Task 3.

- [ ] **Step 1: Add the example declaration**

Create `dev/config/rollups/metrics.yml`:

```yaml
# A metrics table with counts and gauges: one row per minute per series, a
# series being a name, a type and a set of dimensions. It is the declaration
# the Deploy to Render template uses.
#
#   sqlflow rollup ddl -c dev/config/rollups/metrics.yml
#
# It has no serve block. A generated dataset takes at most one dimension and
# folds it, and a series here is three columns that cannot fold. The template
# writes its dataset's grains in serve.yml.
rollups:
  - name: metrics
    source:
      table: metrics_1m
      time_column: bucket
      grain: 1m
      dimensions: [name, type, dimensions_key]

    grains:
      5m: {from: 1m}
      15m: {from: 5m}
      1h: {from: 15m}
      6h: {from: 1h}
      1d: {from: 6h}

    dimension_sets:
      - name: metrics
        dimensions: [name, type, dimensions_key]
        measures:
          # Summed as an integer, a gauge of 0.73 is 1 and one of 0.25 is 0.
          value_sum: {type: sum, column: value_sum, numeric: double}
          value_count: {type: sum, column: value_count}
          value_min: {type: min, column: value_min}
          value_max: {type: max, column: value_max}
          # A gauge's headline value: queue depth now, not its sum.
          value_last: {type: last, column: value_last}
      # A name across every set of dimensions. No last: two series share a
      # bucket here, and neither is the later one.
      - name: metrics_total
        dimensions: [name, type]
        measures:
          value_sum: {type: sum, column: value_sum, numeric: double}
          value_count: {type: sum, column: value_count}
          value_min: {type: min, column: value_min}
          value_max: {type: max, column: value_max}
```

- [ ] **Step 2: Write the failing tests**

Append to `internal/rollup/postgres_test.go`:

```go
// metricsPath declares a double sum and a last over three dimensions.
const metricsPath = "../../dev/config/rollups/metrics.yml"

func loadMetrics(t *testing.T) *config.RollupsConf {
	t.Helper()
	conf, err := config.LoadRollups(metricsPath)
	assert.NoError(t, err)
	return conf
}

func TestCliRollup_MetricsDDLMatchesTheGolden(t *testing.T) {
	coverage.Covers(t, "cli.rollup")

	got, err := PostgresDDL(loadMetrics(t))
	assert.NoError(t, err)
	golden(t, "testdata/metrics.postgres.sql", got)
}

// The cast is the column's type, because the tables are created from the
// query that fills them. last is ordered newest first with nulls after every
// value, so a bucket whose latest minute is null keeps the one before it.
func TestCliRollup_DoubleSumAndLastExpressions(t *testing.T) {
	coverage.Covers(t, "cli.rollup")

	r := loadMetrics(t).Rollups[0]
	set := r.DimensionSets[0]

	assert.Equal(t, `sum(f."value_sum")::double precision`, sourceExpr(r, set.Measures["value_sum"]))
	assert.Equal(t, `sum(f."value_sum")::double precision`, mergeExpr(r, "value_sum", set.Measures["value_sum"]))
	assert.Equal(t, `sum(f."value_count")::bigint`, sourceExpr(r, set.Measures["value_count"]))
	assert.Equal(t, `sum(f."value_count")::bigint`, mergeExpr(r, "value_count", set.Measures["value_count"]))

	last := `(array_agg(f."value_last" ORDER BY (f."value_last" IS NULL), f."bucket" DESC))[1]`
	assert.Equal(t, last, sourceExpr(r, set.Measures["value_last"]))
	assert.Equal(t, last, mergeExpr(r, "value_last", set.Measures["value_last"]))

	// A measure named differently from its column merges by its own name.
	renamed := config.RollupMeasure{Type: "last", Column: "v"}
	assert.Equal(t, `(array_agg(f."v" ORDER BY (f."v" IS NULL), f."bucket" DESC))[1]`, sourceExpr(r, renamed))
	assert.Equal(t, `(array_agg(f."latest" ORDER BY (f."latest" IS NULL), f."bucket" DESC))[1]`, mergeExpr(r, "latest", renamed))

	// count_buckets merges as an integer whatever else the set declares.
	assert.Equal(t, `sum(f."minutes")::bigint`, mergeExpr(r, "minutes", config.RollupMeasure{Type: "count_buckets"}))
}
```

Append to `internal/rollup/serve_test.go`:

```go
// A folded dataset sums a measure across the values folded into other. A
// double sum cast to BIGINT there would lose the fraction the tables kept.
func TestCliRollup_ServeKeepsADoubleSumsFraction(t *testing.T) {
	coverage.Covers(t, "cli.rollup")

	conf := loadExample(t)
	set := conf.Rollups[0].DimensionSets[0]
	m := set.Measures["posts"]
	m.Numeric = "double"
	set.Measures["posts"] = m

	datasets, err := ServeDatasets(conf)
	assert.NoError(t, err)
	for grain, g := range datasets[0].Grains {
		if !strings.Contains(g.SQL, "sum(posts)::DOUBLE AS posts") {
			t.Fatalf("grain %s still casts the sum to an integer:\n%s", grain, g.SQL)
		}
	}
}
```

Check the imports of `serve_test.go` include `strings`; add it if not. If `datasets[0].Grains` is a slice and not a map in `config.ServeDataset`, range over it as `for _, g := range` and print `g.Bucket`.

- [ ] **Step 3: Run the tests to verify they fail**

Run: `go test -short ./internal/rollup/`
Expected: build failure, `too many arguments in call to mergeExpr`.

- [ ] **Step 4: Implement `sql.go`**

In `internal/rollup/sql.go`, replace `sourceExpr` and `mergeExpr`, and add the two helpers above them:

```go
// sumCast is the type a sum is stored as. The tables are created from the
// query that fills them, so the cast is also the column's type.
func sumCast(m config.RollupMeasure) string {
	if m.Numeric == "double" {
		return "double precision"
	}
	return "bigint"
}

// lastExpr is the value of column in the latest of the finer rows aliased f.
// Nulls sort after every value, so a bucket whose latest row is null keeps
// the one before it, and a bucket of nulls stores null. Check admits last
// only in a set that keeps every source dimension, so a group holds one row
// per finer bucket and the order has no ties.
func lastExpr(r config.Rollup, column string) string {
	c := "f." + quote(column)
	return "(array_agg(" + c + " ORDER BY (" + c + " IS NULL), f." + quote(r.Source.TimeColumn) + " DESC))[1]"
}

// sourceExpr builds a measure's stored form from source rows aliased f.
func sourceExpr(r config.Rollup, m config.RollupMeasure) string {
	switch m.Type {
	case "sum":
		return "sum(f." + quote(m.Column) + ")::" + sumCast(m)
	case "min":
		return "min(f." + quote(m.Column) + ")"
	case "max":
		return "max(f." + quote(m.Column) + ")"
	case "last":
		return lastExpr(r, m.Column)
	case "count_buckets":
		return "count(DISTINCT f." + quote(r.Source.TimeColumn) + ")"
	}
	panic(fmt.Sprintf("rollup: no source expression for measure type %q; Check admits only generated types", m.Type))
}

// mergeExpr merges a measure's stored form from finer rollup rows aliased f.
// count_buckets sums, because source buckets never overlap in time. Every
// table of a set names its time column as the source does, so last orders by
// the same column at every grain.
func mergeExpr(r config.Rollup, name string, m config.RollupMeasure) string {
	switch m.Type {
	case "sum":
		return "sum(f." + quote(name) + ")::" + sumCast(m)
	case "count_buckets":
		return "sum(f." + quote(name) + ")::bigint"
	case "min":
		return "min(f." + quote(name) + ")"
	case "max":
		return "max(f." + quote(name) + ")"
	case "last":
		return lastExpr(r, name)
	}
	panic(fmt.Sprintf("rollup: no merge expression for measure type %q; Check admits only generated types", m.Type))
}
```

In `selectList`, change the call `mergeExpr(name, m)` to `mergeExpr(r, name, m)`.

- [ ] **Step 5: Implement `serve.go`**

In `internal/rollup/serve.go`, in `grainSQL`, replace the `"sum"` arm of the `outer` switch:

```go
		case "sum":
			cast := "BIGINT"
			if set.Measures[name].Numeric == "double" {
				cast = "DOUBLE"
			}
			outer[i] = "sum(" + name + ")::" + cast + " AS " + name
```

The switch needs no `last` arm. Check refuses a `last` in a set with a dimension that a dataset reads, and a set with no dimensions returns before this switch, selecting each measure as stored.

- [ ] **Step 6: Write the golden and read it**

Run: `UPDATE_GOLDEN=1 go test -short ./internal/rollup/ -run TestCliRollup_MetricsDDLMatchesTheGolden`
Run: `git status --short internal/rollup/testdata`
Expected: `?? internal/rollup/testdata/metrics.postgres.sql` and nothing else. If `bluesky.postgres.sql` or `bluesky.serve.yml` shows as modified, stop: the byte-identical constraint is broken.

Read `internal/rollup/testdata/metrics.postgres.sql`. Confirm by eye:
- `CREATE TABLE IF NOT EXISTS "metrics_5m"` selects `sum(f."value_sum")::double precision AS "value_sum"` and `sum(f."value_count")::bigint AS "value_count"`.
- Its unique index is on `("bucket", "name", "type", "dimensions_key")`.
- `GROUP BY 1, 2, 3, 4`.
- The `"sqlflow_rollup_metrics_15m"` function reads `FROM "metrics_5m" AS f` and orders `value_last` by `f."bucket" DESC`.
- `"metrics_total_5m"` has no `value_last`, is keyed on `("bucket", "name", "type")`, and groups by `1, 2, 3`.

- [ ] **Step 7: Run the unit pass**

Run: `go test -short -race ./internal/rollup/ ./internal/config/ ./internal/schema/ ./internal/validate/ ./internal/cli/...`
Expected: PASS. `internal/schema` globs `dev/config/rollups/*.yml` and holds `metrics.yml` to the schema Task 1 regenerated.

- [ ] **Step 8: Commit**

```bash
git add internal/rollup/sql.go internal/rollup/serve.go internal/rollup/postgres_test.go internal/rollup/serve_test.go internal/rollup/testdata/metrics.postgres.sql dev/config/rollups/metrics.yml
git commit -m "rollup: generate a double sum and a last measure

The generator cast every sum to bigint in the source expression, the
merge expression and the folded serve query, so a gauge of 0.73 summed
to 0 at every grain. It had no expression for last and panicked on one.

A sum casts to double precision under numeric: double. The tables are
created from the query that fills them, so the cast is the column's
type. last is array_agg over the finer rows, newest first, nulls after
every value. mergeExpr takes the rollup for its time column.

dev/config/rollups/metrics.yml declares both over three dimensions, and
its migration is a golden. The bluesky goldens are byte for byte what
they were."
```

---

### Task 3: the measures against a real Postgres

**Files:**
- Create: `internal/rollup/metrics_integration_test.go`

**Interfaces:**
- Consumes: `loadMetrics(t)` from Task 2; `startRollupPostgres`, `execSQL`, `applyDDL`, `rollupServer` from `postgres_integration_test.go`, same package.
- Produces: nothing.

`startRollupPostgres` creates the Bluesky source table. It is unused here and harmless: the metrics DDL never names it.

- [ ] **Step 1: Write the tests**

Create `internal/rollup/metrics_integration_test.go`:

```go
package rollup

// A double sum and a last through the generated triggers: the fraction
// survives every grain, and last follows the latest minute through a
// rewrite, a delete and a null.

import (
	"context"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

// metricsSourceDDL is the Deploy to Render template's minute table, without
// the columns no rollup reads.
const metricsSourceDDL = `CREATE TABLE metrics_1m (
  bucket         TIMESTAMPTZ      NOT NULL,
  name           TEXT             NOT NULL,
  type           TEXT             NOT NULL,
  dimensions_key TEXT             NOT NULL,
  value_sum      DOUBLE PRECISION NOT NULL,
  value_count    BIGINT           NOT NULL,
  value_min      DOUBLE PRECISION,
  value_max      DOUBLE PRECISION,
  value_last     DOUBLE PRECISION,
  PRIMARY KEY (bucket, name, type, dimensions_key)
)`

var metricsGrains = []string{"5m", "15m", "1h", "6h", "1d"}

func startMetricsPostgres(t *testing.T) *rollupServer {
	t.Helper()
	srv := startRollupPostgres(t)
	execSQL(t, srv.conn, metricsSourceDDL)
	script, err := PostgresDDL(loadMetrics(t))
	assert.NoError(t, err)
	applyDDL(t, srv.conn, script)
	return srv
}

// putGauge upserts one minute of the series cpu, as the pipeline's sink does:
// a second write of a minute replaces the first. A nil value stores null.
// The casts are not decoration: beside the literal 0, Postgres infers $2 as
// an integer, and 0.25 arrives as 0.
func putGauge(t *testing.T, conn *pgx.Conn, minute string, value *float64) {
	t.Helper()
	_, err := conn.Exec(context.Background(), `
INSERT INTO metrics_1m (bucket, name, type, dimensions_key, value_sum, value_count, value_min, value_max, value_last)
VALUES ($1::timestamptz, 'cpu', 'gauge', '{}', coalesce($2::double precision, 0), 1, $2::double precision, $2::double precision, $2::double precision)
ON CONFLICT (bucket, name, type, dimensions_key) DO UPDATE SET
  value_sum = excluded.value_sum, value_count = excluded.value_count,
  value_min = excluded.value_min, value_max = excluded.value_max, value_last = excluded.value_last`,
		"2026-09-15 "+minute+":00+00", value)
	assert.NoError(t, err)
}

func f(v float64) *float64 { return &v }

// column reads one measure of the one row each grain holds for the day.
func column(t *testing.T, conn *pgx.Conn, grain, col string) *float64 {
	t.Helper()
	var v *float64
	q := fmt.Sprintf("SELECT %s::double precision FROM metrics_%s WHERE name = 'cpu' AND dimensions_key = '{}'", col, grain)
	assert.NoError(t, conn.QueryRow(context.Background(), q).Scan(&v))
	return v
}

func assertEveryGrain(t *testing.T, conn *pgx.Conn, col string, want float64) {
	t.Helper()
	for _, g := range metricsGrains {
		got := column(t, conn, g, col)
		if got == nil {
			t.Fatalf("metrics_%s.%s is null, want %v", g, col, want)
		}
		if *got != want {
			t.Fatalf("metrics_%s.%s is %v, want %v", g, col, *got, want)
		}
	}
}

// 0.25 and 0.5 are exact in binary, so equality is exact. Under the bigint
// cast the sum is 1: Postgres rounds.
func TestIntegrationRollup_ADoubleSumKeepsItsFraction(t *testing.T) {
	coverage.Covers(t, "cli.rollup")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	srv := startMetricsPostgres(t)

	putGauge(t, srv.conn, "10:00", f(0.25))
	putGauge(t, srv.conn, "10:01", f(0.5))

	assertEveryGrain(t, srv.conn, "value_sum", 0.75)
	assertEveryGrain(t, srv.conn, "value_count", 2)
	assertEveryGrain(t, srv.conn, "value_min", 0.25)
	assertEveryGrain(t, srv.conn, "value_max", 0.5)

	var typ string
	assert.NoError(t, srv.conn.QueryRow(context.Background(),
		"SELECT data_type FROM information_schema.columns WHERE table_name = 'metrics_1d' AND column_name = 'value_sum'").Scan(&typ))
	assert.Equal(t, "double precision", typ)
	assert.NoError(t, srv.conn.QueryRow(context.Background(),
		"SELECT data_type FROM information_schema.columns WHERE table_name = 'metrics_1d' AND column_name = 'value_count'").Scan(&typ))
	assert.Equal(t, "bigint", typ)
}

// Two series of one name sum into metrics_total at every grain, fraction kept.
func TestIntegrationRollup_ATotalSumsAcrossDimensions(t *testing.T) {
	coverage.Covers(t, "cli.rollup")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	srv := startMetricsPostgres(t)

	putGauge(t, srv.conn, "10:00", f(0.25))
	execSQL(t, srv.conn, `INSERT INTO metrics_1m (bucket, name, type, dimensions_key, value_sum, value_count, value_min, value_max, value_last)
VALUES ('2026-09-15 10:00:00+00', 'cpu', 'gauge', '{"host":"b"}', 0.5, 3, 0.125, 0.25, 0.125)`)

	for _, g := range metricsGrains {
		var sum, min, max float64
		var n int64
		q := fmt.Sprintf("SELECT value_sum, value_count, value_min, value_max FROM metrics_total_%s WHERE name = 'cpu'", g)
		assert.NoError(t, srv.conn.QueryRow(context.Background(), q).Scan(&sum, &n, &min, &max))
		assert.Equal(t, 0.75, sum)
		assert.Equal(t, int64(4), n)
		assert.Equal(t, 0.125, min)
		assert.Equal(t, 0.25, max)
	}
}

// The minutes are written out of order, so last cannot be the last written.
func TestIntegrationRollup_LastIsTheLatestMinute(t *testing.T) {
	coverage.Covers(t, "cli.rollup")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	srv := startMetricsPostgres(t)

	putGauge(t, srv.conn, "10:04", f(4))
	putGauge(t, srv.conn, "10:00", f(3))
	putGauge(t, srv.conn, "10:01", f(9))
	assertEveryGrain(t, srv.conn, "value_last", 4)

	// A later bucket of a coarser grain: 10:20 is another 5m and 15m bucket,
	// the same hour. The hour and up follow it; 10:00's 5m bucket does not.
	putGauge(t, srv.conn, "10:20", f(6))
	for _, g := range []string{"1h", "6h", "1d"} {
		assert.Equal(t, 6.0, *column(t, srv.conn, g, "value_last"))
	}
	var first float64
	assert.NoError(t, srv.conn.QueryRow(context.Background(),
		"SELECT value_last FROM metrics_5m WHERE bucket = '2026-09-15 10:00:00+00'").Scan(&first))
	assert.Equal(t, 4.0, first)
}

func TestIntegrationRollup_LastFollowsARewriteOfTheLatestMinuteOnly(t *testing.T) {
	coverage.Covers(t, "cli.rollup")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	srv := startMetricsPostgres(t)

	putGauge(t, srv.conn, "10:00", f(3))
	putGauge(t, srv.conn, "10:01", f(9))
	putGauge(t, srv.conn, "10:04", f(4))

	putGauge(t, srv.conn, "10:04", f(7))
	assertEveryGrain(t, srv.conn, "value_last", 7)

	putGauge(t, srv.conn, "10:01", f(100))
	assertEveryGrain(t, srv.conn, "value_last", 7)
	assertEveryGrain(t, srv.conn, "value_max", 100)
}

// As for every measure: retention on the minute table leaves history alone.
func TestIntegrationRollup_LastOutlivesADelete(t *testing.T) {
	coverage.Covers(t, "cli.rollup")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	srv := startMetricsPostgres(t)

	putGauge(t, srv.conn, "10:01", f(9))
	putGauge(t, srv.conn, "10:04", f(4))
	execSQL(t, srv.conn, "DELETE FROM metrics_1m WHERE bucket = '2026-09-15 10:04:00+00'")

	assertEveryGrain(t, srv.conn, "value_last", 4)
}

func TestIntegrationRollup_LastSkipsANullAndStoresOneWhenNothingElseExists(t *testing.T) {
	coverage.Covers(t, "cli.rollup")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	srv := startMetricsPostgres(t)

	putGauge(t, srv.conn, "10:04", nil)
	for _, g := range metricsGrains {
		assert.Nil(t, column(t, srv.conn, g, "value_last"))
	}

	putGauge(t, srv.conn, "10:01", f(9))
	assertEveryGrain(t, srv.conn, "value_last", 9)
}
```

- [ ] **Step 2: Run the tests**

Run: `go test ./internal/rollup/ -run 'TestIntegrationRollup_(ADoubleSum|Last)' -v`
Expected: PASS, six tests. Docker must be running.

These tests are written after the generator, so they cannot be watched failing on it. Prove each can fail before trusting it:

- [ ] **Step 3: Prove the tests can fail**

In `internal/rollup/sql.go`, make `sumCast` return `"bigint"` always.
Run: `go test ./internal/rollup/ -run TestIntegrationRollup_ADoubleSumKeepsItsFraction`
Expected: FAIL, `metrics_5m.value_sum is 1, want 0.75`. Restore `sumCast`.

In `lastExpr`, remove ` DESC`.
Run: `go test ./internal/rollup/ -run 'TestIntegrationRollup_Last'`
Expected: `LastIsTheLatestMinute` FAILS with 3 where it wants 4. Restore ` DESC`.

In `lastExpr`, remove the `(c IS NULL), ` ordering term.
Run: `go test ./internal/rollup/ -run TestIntegrationRollup_LastSkipsANull`
Expected: FAIL. Postgres sorts nulls first under `DESC`, so `value_last` is null after 10:01 is written. Restore the term.

Run: `git diff internal/rollup/sql.go`
Expected: empty.

- [ ] **Step 4: Commit**

```bash
git add internal/rollup/metrics_integration_test.go
git commit -m "rollup: a double sum and a last, held against a real Postgres

The generator's expressions were tested as text. Nothing ran them.

Six cases run the metrics migration in Postgres 18: 0.25 + 0.5 is 0.75
at every grain and the column is double precision; two series sum into
the set that drops their dimension; last is the latest
minute when minutes arrive out of order; a rewrite moves last only when
it rewrites the latest minute; a delete changes nothing; a null latest
minute is skipped, and a bucket of nulls stores null. Each case was
watched failing against the defect it names."
```

---

### Task 4: the README and the changelog

**Files:**
- Modify: `README.md` (the rollup section's measures paragraph, near line 542)
- Modify: `CHANGELOG.md`

- [ ] **Step 1: Update the README**

Replace:

```markdown
Measures are `sum`, `min`, `max` and `count_buckets`, which counts the source
buckets present, such as minutes observed. `avg`, `gauge` and `histogram` are
reserved.
```

with:

```markdown
Measures are `sum`, `min`, `max`, `last` and `count_buckets`, which counts the
source buckets present, such as minutes observed. `avg`, `gauge` and
`histogram` are reserved.

A `sum` is stored as a `bigint`. Add `numeric: double` to store a
`double precision`, which keeps a fraction: a gauge of `0.73` summed as an
integer is `1`, and `0.25` is `0`. For an average, keep a sum and a count and divide when you
read. That is exact at every grain, and an average of averages is not.

`last` keeps the value of a bucket's latest finer bucket, which is what a
gauge reads as: queue depth now. It takes the source column's type. A `last`
needs a dimension set that keeps every source dimension, and a generated
dataset cannot serve it, because the last value of the series folded into
`other` belongs to none of them. Write that dataset in `serve.yml`.
[`dev/config/rollups/metrics.yml`](dev/config/rollups/metrics.yml) declares
both.
```

- [ ] **Step 2: Update the changelog**

Under `## Unreleased`, as the first entry of the existing `### Added` section. Unreleased already has one, below `### Fixed`; do not create a second:

```markdown
- `sqlflow rollup` measures: `numeric: double` on a `sum`, and a `last` type.
  Every sum was cast to `bigint`, so a fractional value was rounded to an
  integer at every grain. `last` keeps the value of the bucket's latest finer
  bucket. A declaration that uses neither generates the migration and the
  serve datasets it did before, byte for byte.
```

- [ ] **Step 3: Run everything**

Run: `make test-go`
Expected: PASS. This includes the integration pass and needs Docker.

- [ ] **Step 4: Commit**

```bash
git add README.md CHANGELOG.md
git commit -m "docs: numeric: double and last in the rollup section

The README listed four measure types and said nothing of how a sum is
stored. It now names last, says when to ask for a double, and says why
an average is a sum and a count."
```

---

## Verification

- [ ] `make test-go` passes with Docker running.
- [ ] `git diff main -- internal/rollup/testdata/bluesky.postgres.sql internal/rollup/testdata/bluesky.serve.yml` is empty.
- [ ] In a checkout of `sql-flow-bluesky-demo`, with the new binary on `PATH`, `make validate` passes without regenerating anything.
