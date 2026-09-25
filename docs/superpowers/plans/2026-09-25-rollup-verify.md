# `sqlflow rollup verify` Implementation Plan (Plan 2b)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** SQLFlow checks every rollup table against the table it is built from, in the daemon every minute and on demand with `sqlflow rollup verify`, reports drift through `/healthz` as `degraded`, and measures each table's newest bucket through a new `internal/freshness` package that also names the store the same way for every reporter.

**Architecture:** `internal/rollup/verify.go` recomputes a table's rows over a range of buckets from the table it is built from, with the generator's own select list, and compares them with the stored rows in one statement. `VerifyTargets` lists what to check: declared and retained tables, minus any table still filling. `internal/freshness` reads a table's newest bucket and names a store by its system identifier. The daemon runs an observe pass every interval and a verify pass every fourth, and `/healthz` reports `degraded` while the last pass found drift. `sqlflow rollup verify` checks every bucket since `--since` and exits with `system.rollup.drift`.

**Tech Stack:** Go 1.26, pgx/v5, cobra, zap, OpenTelemetry metric SDK v1.46 with its Prometheus exporter, testcontainers-go Postgres module, zeebo/assert.

**Spec:** `docs/superpowers/specs/2026-09-24-rollup-daemon-design.md`

## Where this plan sits

| Plan | Scope | State |
|---|---|---|
| 1 | Config blocks, planner, state table, `sqlflow rollup install` | Merged, #380 |
| 2a | Chunked backfill, `sqlflow rollup run`, the leader lock, `/healthz` and metrics, the rollup lock | Merged, #382 |
| **2b, this one** | The verify pass and `sqlflow rollup verify`, `internal/freshness` and the store identity, `degraded` in `/healthz` | |
| 3 | The TurboStats `freshness` list and `rollup` section, completeness, trigger cost, and the release test | |
| 4 | `sqlflow rollup test` | |
| 5 | A release, then the Bluesky demo, the Render template, and the parity proof | |

Completeness and trigger cost are reported only through TurboStats, so they move to Plan 3 with it. This plan's observe pass records each table's newest bucket as a metric.

## Global Constraints

- One branch, `feat/rollup-verify`, from main at `1c7d1c6`, carrying this plan, the spec amendments and the code, in one PR against main. Its upstream is `origin/main`, so push with `git push -u origin feat/rollup-verify`. Once the PR exists, gate every push: `[ "$(gh pr view N --repo turbolytics/sql-flow --json state --jq .state)" = OPEN ]`.
- Postgres 15 or later. Integration tests run `postgres:18`.
- One new error code, `system.rollup.drift` (`errs.CodeRollupDrift`). Its class default exits 1. A database error stays `system.rollup.internal`, a failed connect `system.rollup.unreachable`, a bad flag or empty DSN `user.config.invalid`.
- Verify only reads. It takes no advisory lock, sets no `lock_timeout` and writes nothing. One range is one statement, so one snapshot.
- The comparison: `IS DISTINCT FROM` for every measure except a `sum` with `numeric: double`, which matches within a relative `1e-9`. Rows pair on the table's key with `NULL` equal to `NULL`, as the unique index `NULLS NOT DISTINCT` does.
- Intervals: observe every daemon interval (15 s by default), verify every `verifyEvery` = 4 intervals (60 s). The loop checks each table's newest two buckets.
- Store identity, exact: `pg:` and the first 16 hex characters of `sha256(<system_identifier> || '/' || current_database())`, kind `system`. When `pg_control_system()` raises `42501`, `sha256(<host> || ':' || <port> || '/' || current_database())` with `coalesce(host(inet_server_addr()), 'local')` and `coalesce(inet_server_port(), 0)`, kind `address`.
- Metric instruments, exact names: `rollup_verify_buckets`, `rollup_drift_buckets`, `rollup_verify_duration` (unit `s`), `rollup_newest_bucket_timestamp` (gauge, unit `s`), and `rollup_errors` gains phases `verify` and `observe`. Attributes `rollup` and `table`.
- `/healthz` rule order, from the spec: `failed`, `degraded`, `standby`, `backfilling`, `starting`, `healthy`.
- Unit tests are `TestCliRollupRun_*`; integration tests `TestIntegrationRollupRun_*`, skip under `-short`, start their own container. Every one calls `coverage.Covers(t, "cli.rollup_run")`, whose comment already names verify.
- Run `go`, `make`, `docker` and `gh` outside the sandbox. One heredoc per shell command; commit messages go in a file and `git commit -F`. `$TMPDIR` differs inside and outside the sandbox.
- Prose follows the repo's `CLAUDE.md`. Comments explain why.

## Review Focus

1. **Verify beside live writers** never reports drift on correct rows, including the open bucket. Task 2, `TestIntegrationRollupRun_VerifyUnderConcurrentWritesReportsNoDrift`.
2. **A `NULL` dimension value and a double sum** compare equal when they are correct. Task 1, `TestIntegrationRollupRun_ANullDimensionAndADoubleSumVerifyClean`.
3. **A table still filling**, and every table its upserts reach, is skipped rather than reported as drift, and a grain added later skips only itself. Task 2, `TestIntegrationRollupRun_VerifySkipsTablesStillFilling`.
4. **A database made by `rollup ddl`'s migration**, which has no state table, verifies. Task 2, `TestIntegrationRollupRun_VerifyNeedsNoStateTable`, and Task 5, `TestIntegrationRollupRun_VerifyCommandPassesACleanDatabase`.
5. **Fixed drift clears `degraded`** on the next pass with no restart. Task 4, `TestIntegrationRollupRun_DriftIsReportedAndClears`.

## File Structure

```
internal/rollup/verify.go                      Target, Verified, DriftRow, verifySQL, VerifyRange, VerifyTargets, VerifyNewest, VerifySince
internal/rollup/verify_test.go                 the SQL's shape, the span
internal/rollup/verify_integration_test.go     against Postgres
internal/rollup/testdata/verify.posts_by_lang_15m.sql   golden
internal/freshness/freshness.go                Observation, Observe, Store, StoreOf
internal/freshness/freshness_test.go           Age, the two id forms
internal/freshness/freshness_integration_test.go
internal/rollup/daemon/health.go               drift in the snapshot, the degraded rule
internal/rollup/daemon/health_test.go          three rows
internal/rollup/daemon/metrics.go              four instruments
internal/rollup/daemon/daemon.go               observe and verify passes, the store log line
internal/rollup/daemon/daemon_integration_test.go       DriftIsReportedAndClears
internal/errs/registry.go, testdata/codes.golden        system.rollup.drift
internal/cli/rollup/verify.go                  sqlflow rollup verify
internal/cli/rollup/rollup.go                  register verify
internal/cli/rollup/verify_test.go
internal/cli/rollup/verify_integration_test.go
README.md, CHANGELOG.md                        docs
docs/superpowers/specs/2026-09-24-rollup-daemon-design.md   amendments
```

---

### Task 1: The comparison

**Files:**
- Create: `internal/rollup/verify.go`
- Create: `internal/rollup/verify_test.go`
- Create: `internal/rollup/verify_integration_test.go`
- Create: `internal/rollup/testdata/verify.posts_by_lang_15m.sql` (golden, written by the test)

**Interfaces:**
- Consumes: `edge`, `edges`, `findEdge`, `selectList`, `keyColumns`, `groupBy`, `quote`, `quoteList` (internal/rollup/sql.go); `golden` (helpers_test.go); `exampleRollup` (backfill_test.go); `startRollupPostgres`, `history`, `mustInstall`, `fillAll`, `loadExample`, `execSQL`, `at` (integration helpers).
- Produces: `type Target struct { Rollup config.Rollup; Table, BuiltFrom string; Retained bool; Skip string; e edge }`; `func newTarget(r config.Rollup, e edge) Target`; `type DriftRow struct { Bucket time.Time; Key, Kind, Measure, Stored, Recomputed string }`; `type Verified struct { Rollup, Table, BuiltFrom string; From, To time.Time; Buckets, DriftBuckets int64; Sample []DriftRow }`; `func VerifyRange(ctx context.Context, conn *pgx.Conn, tg Target, lo, hi time.Time) (Verified, error)`; `const verifyTolerance = 1e-9`; `const maxDriftSample = 10`.

- [ ] **Step 1: Write the failing unit tests**

Create `internal/rollup/verify_test.go`:

```go
package rollup

import (
	"strings"
	"testing"

	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

// The statement recomputes 15m from 5m with the generator's own select
// list, so a change to how triggers merge changes what verify expects.
func TestCliRollupRun_VerifySQLRecomputesFromTheFinerTable(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")

	r := exampleRollup(t)
	e, ok := findEdge(r, "posts_by_lang_15m")
	assert.True(t, ok)
	golden(t, "testdata/verify.posts_by_lang_15m.sql", verifySQL(r, e))
}

// Only a double sum is compared within a tolerance: Postgres sums floats in
// no fixed order. Every other measure is exact.
func TestCliRollupRun_OnlyADoubleSumMatchesWithinATolerance(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")

	r := config.Rollup{
		Name:   "m",
		Source: config.RollupSource{Table: "m_1m", TimeColumn: "minute", Grain: "1m"},
		Grains: map[string]config.RollupGrain{"1h": {From: "1m"}},
		DimensionSets: []config.RollupDimensionSet{{
			Name: "m", Dimensions: []string{},
			Measures: map[string]config.RollupMeasure{
				"a": {Type: "sum", Column: "a", Numeric: "double"},
				"b": {Type: "sum", Column: "b"},
				"c": {Type: "max", Column: "c"},
			},
		}},
	}
	sql := verifySQL(r, edges(r)[0])
	// The double's expression appears four times: in the WHERE, and in the
	// measure, stored and recomputed CASE arms.
	assert.Equal(t, 4, strings.Count(sql, "1e-09"))
	assert.That(t, strings.Contains(sql, `"w:b" IS DISTINCT FROM "g:b"`))
	assert.That(t, strings.Contains(sql, `"w:c" IS DISTINCT FROM "g:c"`))
	assert.False(t, strings.Contains(sql, `"w:a" IS DISTINCT FROM`))
}
```

`exampleRollup(t)` is the Plan 2a helper in `backfill_test.go`.

- [ ] **Step 2: Run them to verify they fail**

Run: `go test -count=1 -run 'TestCliRollupRun_VerifySQL|TestCliRollupRun_OnlyADoubleSum' ./internal/rollup/`
Expected: FAIL to compile, `undefined: verifySQL`.

- [ ] **Step 3: Write the comparison**

Create `internal/rollup/verify.go`:

```go
package rollup

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/errs"
)

// verifyTolerance is how far apart two double sums may be, relative to the
// larger, and still match. Postgres sums floats in no fixed order, so a
// trigger's re-merge and verify's recompute can differ in the last bits of
// a correct sum.
const verifyTolerance = 1e-9

// maxDriftSample bounds the drifted rows one check returns, and so the
// daemon's log to 10 lines per table per pass.
const maxDriftSample = 10

// Target is one table verify checks against the table it is built from.
type Target struct {
	Rollup    config.Rollup
	Table     string
	BuiltFrom string
	// Retained is a table a later declaration removed. Its triggers still
	// run, so it is still checked.
	Retained bool
	// Skip says why verify leaves the table alone. Empty means check it.
	Skip string
	e    edge
}

func newTarget(r config.Rollup, e edge) Target {
	return Target{Rollup: r, Table: e.Table, BuiltFrom: e.From, e: e}
}

// DriftRow is one row where a table differs from what its from table makes.
type DriftRow struct {
	Bucket time.Time `json:"bucket_at"`
	// Key is the row's dimension values, such as "lang=en", and empty for a
	// set with none.
	Key string `json:"key"`
	// Kind is "missing", a row the from table makes that the table lacks;
	// "extra", a stored row nothing makes; or "differs".
	Kind string `json:"kind"`
	// Measure is the first measure that differs, and Stored and Recomputed
	// are its two values as text. A side the row lacks is empty.
	Measure    string `json:"measure"`
	Stored     string `json:"stored"`
	Recomputed string `json:"recomputed"`
}

// Verified is one table's check over the buckets in [From, To).
type Verified struct {
	Rollup    string
	Table     string
	BuiltFrom string
	From, To  time.Time
	// Buckets is how many buckets either side holds, and DriftBuckets how
	// many of them differ.
	Buckets      int64
	DriftBuckets int64
	// Sample is the first drifted rows, oldest first, at most
	// maxDriftSample.
	Sample []DriftRow
}

// measureDiffers is true when a measure's recomputed value w and stored
// value g differ.
func measureDiffers(m config.RollupMeasure, w, g string) string {
	if m.Type == "sum" && m.Numeric == "double" {
		return fmt.Sprintf("((%[1]s IS NULL) <> (%[2]s IS NULL) OR coalesce(abs(%[1]s - %[2]s) > %[3]g * greatest(abs(%[1]s), abs(%[2]s)), false))",
			w, g, verifyTolerance)
	}
	return w + " IS DISTINCT FROM " + g
}

// verifySQL is one statement that recomputes e's rows over the buckets in
// [$1, $2) from the table e is built from, pairs them with the stored rows
// on the table's key, and returns the buckets either side holds, the
// buckets that differ, and the first differing rows as JSON.
//
// One statement reads one snapshot, and a writer writes the source and
// every grain in one transaction, so a correct table never differs, even in
// its open bucket. Rows pair through GROUP BY rather than a full join:
// Postgres refuses a full join on IS NOT DISTINCT FROM, and grouping treats
// NULL dimension values as equal, as the unique index does.
func verifySQL(r config.Rollup, e edge) string {
	t := quote(r.Source.TimeColumn)
	keys := keyColumns(r, e.Set)
	measures := e.Set.MeasureNames()
	names := append(append([]string{}, keys...), measures...)

	cols := selectList(r, e.Set, e.Grain)
	for i := range cols {
		cols[i] += " AS " + quote(names[i])
	}
	pairs := []string{quoteList(keys), "bool_or(side = 'w') AS in_want", "bool_or(side = 'g') AS in_got"}
	var differs, measureArms, storedArms, recomputedArms []string
	for _, m := range measures {
		w, g := quote("w:"+m), quote("g:"+m)
		pairs = append(pairs,
			fmt.Sprintf("(array_agg(%s) FILTER (WHERE side = 'w'))[1] AS %s", quote(m), w),
			fmt.Sprintf("(array_agg(%s) FILTER (WHERE side = 'g'))[1] AS %s", quote(m), g))
		d := measureDiffers(e.Set.Measures[m], w, g)
		differs = append(differs, d)
		measureArms = append(measureArms, fmt.Sprintf("WHEN %s THEN '%s'", d, m))
		storedArms = append(storedArms, fmt.Sprintf("WHEN %s THEN %s::text", d, g))
		recomputedArms = append(recomputedArms, fmt.Sprintf("WHEN %s THEN %s::text", d, w))
	}
	key := "''"
	if len(e.Set.Dimensions) > 0 {
		parts := make([]string, len(e.Set.Dimensions))
		for i, d := range e.Set.Dimensions {
			parts[i] = fmt.Sprintf("'%s=' || coalesce(%s::text, 'null')", d, quote(d))
		}
		key = "concat_ws(', ', " + strings.Join(parts, ", ") + ")"
	}

	return fmt.Sprintf(`WITH want AS (
  SELECT %[1]s
  FROM %[2]s AS f
  WHERE f.%[3]s >= $1 AND f.%[3]s < $2
  GROUP BY %[4]s
), got AS (
  SELECT %[5]s FROM %[6]s WHERE %[3]s >= $1 AND %[3]s < $2
), pairs AS (
  SELECT %[7]s
  FROM (SELECT 'w' AS side, %[5]s FROM want UNION ALL SELECT 'g', %[5]s FROM got) AS u
  GROUP BY %[8]s
), diff AS (
  SELECT %[3]s AS bucket_at, %[9]s AS key,
         CASE WHEN NOT in_got THEN 'missing' WHEN NOT in_want THEN 'extra' ELSE 'differs' END AS kind,
         CASE %[10]s END AS measure,
         CASE %[11]s END AS stored,
         CASE %[12]s END AS recomputed
  FROM pairs
  WHERE NOT in_want OR NOT in_got OR %[13]s
)
SELECT (SELECT count(DISTINCT %[3]s) FROM pairs),
       (SELECT count(DISTINCT bucket_at) FROM diff),
       coalesce((SELECT jsonb_agg(s ORDER BY s.bucket_at, s.key)
                 FROM (SELECT * FROM diff ORDER BY bucket_at, key LIMIT %[14]d) AS s), '[]'::jsonb)
`,
		strings.Join(cols, ", "), quote(e.From), t, groupBy(len(keys)),
		quoteList(names), quote(e.Table),
		strings.Join(pairs, ", "), quoteList(keys), key,
		strings.Join(measureArms, " "), strings.Join(storedArms, " "), strings.Join(recomputedArms, " "),
		strings.Join(differs, " OR "), maxDriftSample)
}

// VerifyRange checks tg's table over the buckets in [lo, hi). lo and hi lie
// on the table's grain boundaries, so every bucket is whole.
func VerifyRange(ctx context.Context, conn *pgx.Conn, tg Target, lo, hi time.Time) (Verified, error) {
	v := Verified{Rollup: tg.Rollup.Name, Table: tg.Table, BuiltFrom: tg.BuiltFrom, From: lo, To: hi}
	var sample []byte
	if err := conn.QueryRow(ctx, verifySQL(tg.Rollup, tg.e), lo, hi).Scan(&v.Buckets, &v.DriftBuckets, &sample); err != nil {
		return v, verifyError(err, tg.Table)
	}
	if err := json.Unmarshal(sample, &v.Sample); err != nil {
		return v, verifyError(err, tg.Table)
	}
	return v, nil
}

func verifyError(err error, table string) error {
	return errs.Wrap(errs.CodeRollupInternal, err, "rollup verify %s", table)
}
```

- [ ] **Step 4: Write the golden and run the unit tests**

Run: `UPDATE_GOLDEN=1 go test -count=1 -run 'TestCliRollupRun_VerifySQL' ./internal/rollup/ && go test -count=1 -run 'TestCliRollupRun_VerifySQL|TestCliRollupRun_OnlyADoubleSum' ./internal/rollup/`
Expected: PASS. Read `testdata/verify.posts_by_lang_15m.sql`: `want` reads `"posts_by_lang_5m" AS f` with `sum(f."posts")::bigint`, and the key reads `'lang=' || coalesce("lang"::text, 'null')`.

- [ ] **Step 5: Write the integration tests**

Create `internal/rollup/verify_integration_test.go`:

```go
package rollup

import (
	"context"
	"testing"
	"time"

	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

// targetFor is the verify target for a declared table.
func targetFor(t *testing.T, r config.Rollup, table string) Target {
	t.Helper()
	e, ok := findEdge(r, table)
	assert.True(t, ok)
	return newTarget(r, e)
}

func TestIntegrationRollupRun_VerifyFindsNothingInACorrectTable(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	srv := startRollupPostgres(t)
	history(t, srv.conn, "2026-09-12T00:00:00Z", "2026-09-12T23:59:00Z")
	mustInstall(t, srv.conn, loadExample(t))
	fillAll(t, srv.conn, loadExample(t))

	r := loadExample(t).Rollups[0]
	for _, e := range edges(r) {
		v, err := VerifyRange(context.Background(), srv.conn, newTarget(r, e), at("2026-09-12T00:00:00Z"), at("2026-09-13T00:00:00Z"))
		assert.NoError(t, err)
		assert.Equal(t, int64(0), v.DriftBuckets)
		assert.Equal(t, int64(24*time.Hour/e.Grain.Width), v.Buckets)
	}
}

// Stopping the triggers from 5m to 15m and then writing breaks one edge.
// Verify reports it at 15m, the three ways a row can differ, and not at 1h:
// 1h still equals the 15m rows it is built from.
func TestIntegrationRollupRun_ADisabledTriggerDriftsOneEdge(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	srv := startRollupPostgres(t)
	ctx := context.Background()
	history(t, srv.conn, "2026-09-12T00:00:00Z", "2026-09-12T23:59:00Z")
	mustInstall(t, srv.conn, loadExample(t))
	fillAll(t, srv.conn, loadExample(t))

	execSQL(t, srv.conn, `ALTER TABLE posts_by_lang_5m DISABLE TRIGGER sqlflow_rollup_posts_by_lang_15m_ins`)
	execSQL(t, srv.conn, `ALTER TABLE posts_by_lang_5m DISABLE TRIGGER sqlflow_rollup_posts_by_lang_15m_upd`)
	execSQL(t, srv.conn, `INSERT INTO posts_per_minute_by_lang (bucket, lang, posts) VALUES ('2026-09-12T23:58:00Z', 'pt', 9)`)
	execSQL(t, srv.conn, `UPDATE posts_per_minute_by_lang SET posts = 99 WHERE bucket = '2026-09-12T23:50:00Z' AND lang = 'en'`)
	execSQL(t, srv.conn, `INSERT INTO posts_by_lang_15m (bucket, lang, posts) VALUES ('2026-09-12T23:45:00Z', 'xx', 1)`)

	r := loadExample(t).Rollups[0]
	v, err := VerifyRange(ctx, srv.conn, targetFor(t, r, "posts_by_lang_15m"), at("2026-09-12T23:00:00Z"), at("2026-09-13T00:00:00Z"))
	assert.NoError(t, err)
	assert.Equal(t, int64(4), v.Buckets)
	assert.Equal(t, int64(1), v.DriftBuckets)
	kinds := map[string]DriftRow{}
	for _, row := range v.Sample {
		assert.That(t, row.Bucket.Equal(at("2026-09-12T23:45:00Z")))
		kinds[row.Kind+" "+row.Key] = row
	}
	assert.Equal(t, 3, len(kinds))
	assert.Equal(t, "9", kinds["missing lang=pt"].Recomputed)
	assert.Equal(t, "", kinds["missing lang=pt"].Stored)
	assert.Equal(t, "1", kinds["extra lang=xx"].Stored)
	differs := kinds["differs lang=en"]
	assert.Equal(t, "posts", differs.Measure)
	assert.That(t, differs.Stored != differs.Recomputed)

	v, err = VerifyRange(ctx, srv.conn, targetFor(t, r, "posts_by_lang_1h"), at("2026-09-12T00:00:00Z"), at("2026-09-13T00:00:00Z"))
	assert.NoError(t, err)
	assert.Equal(t, int64(0), v.DriftBuckets)
}

// A NULL dimension value pairs with its stored row, and a double sum stored
// with a last-bit difference still matches. A difference past the
// tolerance does not.
func TestIntegrationRollupRun_ANullDimensionAndADoubleSumVerifyClean(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	srv := startRollupPostgres(t)
	ctx := context.Background()
	execSQL(t, srv.conn, `CREATE TABLE region_minutes (bucket TIMESTAMPTZ NOT NULL, region TEXT, cost DOUBLE PRECISION NOT NULL)`)
	execSQL(t, srv.conn, `CREATE UNIQUE INDEX ON region_minutes (bucket, region) NULLS NOT DISTINCT`)
	conf := &config.RollupsConf{Rollups: []config.Rollup{{
		Name:   "costs",
		Source: config.RollupSource{Table: "region_minutes", TimeColumn: "bucket", Grain: "1m", Dimensions: []string{"region"}},
		Grains: map[string]config.RollupGrain{"5m": {From: "1m"}},
		DimensionSets: []config.RollupDimensionSet{{
			Name: "cost_by_region", Dimensions: []string{"region"},
			Measures: map[string]config.RollupMeasure{"cost": {Type: "sum", Column: "cost", Numeric: "double"}},
		}},
	}}}
	mustInstall(t, srv.conn, conf)
	fillAll(t, srv.conn, conf)
	execSQL(t, srv.conn, `INSERT INTO region_minutes (bucket, region, cost)
SELECT g, r, 0.1 * extract(minute FROM g) FROM generate_series('2026-09-12T10:00:00Z'::timestamptz, '2026-09-12T10:59:00Z', interval '1 minute') AS g,
       unnest(ARRAY['eu', NULL]) AS r`)

	tg := targetFor(t, conf.Rollups[0], "cost_by_region_5m")
	lo, hi := at("2026-09-12T10:00:00Z"), at("2026-09-12T11:00:00Z")
	v, err := VerifyRange(ctx, srv.conn, tg, lo, hi)
	assert.NoError(t, err)
	assert.Equal(t, int64(12), v.Buckets)
	assert.Equal(t, int64(0), v.DriftBuckets)

	execSQL(t, srv.conn, `UPDATE cost_by_region_5m SET cost = cost * (1 + 1e-12)`)
	v, err = VerifyRange(ctx, srv.conn, tg, lo, hi)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), v.DriftBuckets)

	execSQL(t, srv.conn, `UPDATE cost_by_region_5m SET cost = cost * (1 + 1e-6) WHERE region IS NULL AND cost > 0`)
	v, err = VerifyRange(ctx, srv.conn, tg, lo, hi)
	assert.NoError(t, err)
	assert.Equal(t, int64(12), v.DriftBuckets)
	assert.Equal(t, "region=null", v.Sample[0].Key)
	assert.Equal(t, "differs", v.Sample[0].Kind)
}
```

The first 5m bucket of 10:00 sums `0.1 × (0..4)` for `NULL`, which is positive, so `cost > 0` selects all 12 `NULL` rows. If `mustInstall` refuses the inline declaration for a reason the file loader supplies, such as an unset default, build the declaration through `config.LoadRollups` from a file written to `t.TempDir()` instead, and ledger a ruling.

- [ ] **Step 6: Run them**

Run: `go test -count=1 -run 'TestIntegrationRollupRun_(VerifyFindsNothingInACorrectTable|ADisabledTriggerDriftsOneEdge|ANullDimensionAndADoubleSumVerifyClean)$' ./internal/rollup/`
Expected: PASS. These tests arrive after the code they test, so break the code once to watch each guard fail, then restore it:
- Make `measureDiffers` return `w + " IS DISTINCT FROM " + g` for every measure. `ANullDimensionAndADoubleSumVerifyClean` FAILS at the `1e-12` step.
- The `NULL` pairing has no one-line break: `GROUP BY` always treats `NULL`s as equal, and that is why the pairing uses it. The test pins it: a pairing that split `NULL`s would report every `NULL` row as missing and extra.
- Make `ADisabledTriggerDriftsOneEdge` check `posts_by_lang_1h` against `posts_by_lang_5m` by editing its target's `e.From`. It FAILS, which shows the edge check is what keeps 1h clean.

- [ ] **Step 7: Commit**

```bash
git add internal/rollup/verify.go internal/rollup/verify_test.go internal/rollup/verify_integration_test.go internal/rollup/testdata/verify.posts_by_lang_15m.sql
git commit -F <message file>
```

Message: `rollup: recompute a table from what it is built from and compare, in one statement`, with a body that names the GROUP BY pairing and the double tolerance.

---

### Task 2: What to check, and where

**Files:**
- Modify: `internal/rollup/verify.go`
- Modify: `internal/rollup/verify_test.go`
- Modify: `internal/rollup/verify_integration_test.go`

**Interfaces:**
- Consumes: Task 1's `Target`, `newTarget`, `VerifyRange`, `Verified`; `readAllStates`, `State`, `RetainedTable`, `AppliedSet` (Plan 1); `findEdge`, `cascadeLevels`, `floorTo` (Plan 2a); `flushMinutes`, `randomMinutes` (integration helpers).
- Produces: `func VerifyTargets(ctx context.Context, conn *pgx.Conn, conf *config.RollupsConf) ([]Target, error)`; `func VerifyNewest(ctx context.Context, conn *pgx.Conn, tg Target) (Verified, bool, error)`; `func VerifySince(ctx context.Context, conn *pgx.Conn, tg Target, since *time.Time) (Verified, error)`; `func verifySpan(width time.Duration) time.Duration`; `func retainedEdge(r config.Rollup, rt RetainedTable) (edge, error)`.

- [ ] **Step 1: Write the failing unit test**

Append to `internal/rollup/verify_test.go`:

```go
// A span holds whole buckets, about a day of them, so a width that does not
// divide a day still never splits a bucket between two statements.
func TestCliRollupRun_AVerifySpanHoldsWholeBuckets(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")

	assert.Equal(t, 24*time.Hour, verifySpan(5*time.Minute))
	assert.Equal(t, 25*time.Hour, verifySpan(5*time.Hour))
	assert.Equal(t, 7*24*time.Hour, verifySpan(7*24*time.Hour))
}
```

Add `"time"` to the imports.

- [ ] **Step 2: Run it to verify it fails**

Run: `go test -count=1 -run 'TestCliRollupRun_AVerifySpan' ./internal/rollup/`
Expected: FAIL to compile, `undefined: verifySpan`.

- [ ] **Step 3: Write targets and ranges**

Append to `internal/rollup/verify.go`:

```go
// VerifyTargets lists every table of conf's rollups, declared then
// retained. A table still filling, and every table its upserts reach,
// differs from its from table until the fill completes, so it is skipped.
// A database made by `rollup ddl`'s migration has no state table, and so
// nothing retained and nothing filling.
func VerifyTargets(ctx context.Context, conn *pgx.Conn, conf *config.RollupsConf) ([]Target, error) {
	states := map[string]State{}
	var exists bool
	if err := conn.QueryRow(ctx, "SELECT to_regclass('sqlflow_rollup_state') IS NOT NULL").Scan(&exists); err != nil {
		return nil, verifyError(err, "sqlflow_rollup_state")
	}
	if exists {
		all, err := readAllStates(ctx, conn)
		if err != nil {
			return nil, verifyError(err, "sqlflow_rollup_state")
		}
		for _, s := range all {
			states[s.Rollup] = s
		}
	}

	var out []Target
	for _, r := range conf.Rollups {
		s := states[r.Name]
		filling := map[string]bool{}
		for table := range s.Backfill {
			filling[table] = true
			if e, ok := findEdge(r, table); ok {
				for _, g := range cascadeLevels(r, e) {
					filling[Table(e.Set, g.Name)] = true
				}
			}
		}
		for _, e := range edges(r) {
			tg := newTarget(r, e)
			if filling[e.Table] {
				tg.Skip = "backfill pending"
			}
			out = append(out, tg)
		}
		for _, rt := range s.Retained {
			e, err := retainedEdge(r, rt)
			if err != nil {
				return nil, err
			}
			tg := newTarget(r, e)
			tg.Retained = true
			if filling[rt.Table] {
				tg.Skip = "backfill pending"
			}
			out = append(out, tg)
		}
	}
	return out, nil
}

// retainedEdge rebuilds the edge a retained table was built on, from the
// shape install recorded when a declaration removed it.
func retainedEdge(r config.Rollup, rt RetainedTable) (edge, error) {
	w, err := config.ParseServeDuration(rt.Grain)
	if err != nil {
		return edge{}, errs.Wrap(errs.CodeRollupInternal, err, "rollup verify %s: retained grain %q", rt.Table, rt.Grain)
	}
	set := config.RollupDimensionSet{Name: rt.Set, Dimensions: rt.Shape.Dimensions, Measures: map[string]config.RollupMeasure{}}
	for name, m := range rt.Shape.Measures {
		set.Measures[name] = config.RollupMeasure{Type: m.Type, Column: m.Column, Numeric: m.Numeric}
	}
	from := r.Source.Table
	if rt.From != r.Source.Grain {
		from = Table(set, rt.From)
	}
	return edge{Set: set, Grain: config.RollupLevel{Name: rt.Grain, Width: w, From: rt.From}, Table: rt.Table, From: from}, nil
}

// verifySpan is how much of a table VerifySince checks in one statement:
// about a day, in whole buckets.
func verifySpan(width time.Duration) time.Duration {
	n := (24*time.Hour + width - 1) / width
	return n * width
}

// bounds reads the oldest and newest bucket of tg's table and of the
// buckets its from table makes, on the table's grain. Both are nil when
// both tables are empty.
func bounds(ctx context.Context, conn *pgx.Conn, tg Target) (oldest, newest *time.Time, err error) {
	t := quote(tg.Rollup.Source.TimeColumn)
	w := tg.e.Grain.Width
	err = conn.QueryRow(ctx, fmt.Sprintf(`SELECT least((SELECT min(%[1]s) FROM %[2]s), (SELECT %[3]s FROM %[5]s AS f)),
       greatest((SELECT max(%[1]s) FROM %[2]s), (SELECT %[4]s FROM %[5]s AS f))`,
		t, quote(tg.Table), bin(w, "min(f."+t+")"), bin(w, "max(f."+t+")"), quote(tg.BuiltFrom))).Scan(&oldest, &newest)
	if err != nil {
		return nil, nil, verifyError(err, tg.Table)
	}
	return oldest, newest, nil
}

// VerifyNewest checks tg's newest two buckets: the open one and the one
// that closed before it. The newest is the later of the table's newest and
// the newest its from table makes, so a table that trails is checked where
// it trails. ok is false when both tables are empty.
func VerifyNewest(ctx context.Context, conn *pgx.Conn, tg Target) (Verified, bool, error) {
	_, newest, err := bounds(ctx, conn, tg)
	if err != nil || newest == nil {
		return Verified{Rollup: tg.Rollup.Name, Table: tg.Table, BuiltFrom: tg.BuiltFrom}, false, err
	}
	w := tg.e.Grain.Width
	v, err := VerifyRange(ctx, conn, tg, newest.Add(-w), newest.Add(w))
	return v, err == nil, err
}

// VerifySince checks every bucket of tg's table from since, or from its
// oldest when since is nil, one span per statement. The sample keeps the
// first maxDriftSample drifted rows across spans.
func VerifySince(ctx context.Context, conn *pgx.Conn, tg Target, since *time.Time) (Verified, error) {
	all := Verified{Rollup: tg.Rollup.Name, Table: tg.Table, BuiltFrom: tg.BuiltFrom}
	oldest, newest, err := bounds(ctx, conn, tg)
	if err != nil || newest == nil {
		return all, err
	}
	w := tg.e.Grain.Width
	lo, hi := floorTo(*oldest, w), newest.Add(w)
	if since != nil && floorTo(*since, w).After(lo) {
		lo = floorTo(*since, w)
	}
	all.From, all.To = lo, hi
	span := verifySpan(w)
	for from := lo; from.Before(hi); from = from.Add(span) {
		to := from.Add(span)
		if to.After(hi) {
			to = hi
		}
		v, err := VerifyRange(ctx, conn, tg, from, to)
		if err != nil {
			return all, err
		}
		all.Buckets += v.Buckets
		all.DriftBuckets += v.DriftBuckets
		for _, row := range v.Sample {
			if len(all.Sample) < maxDriftSample {
				all.Sample = append(all.Sample, row)
			}
		}
	}
	return all, nil
}
```

`newest` from `bounds` is already a bucket start: a stored bucket, or `bin` of the from table's newest. `floorTo` bins from `2000-01-01 UTC`, the generator's origin, so its boundaries are the SQL's.

- [ ] **Step 4: Run the unit test**

Run: `go test -count=1 -run 'TestCliRollupRun_AVerifySpan' ./internal/rollup/`
Expected: PASS.

- [ ] **Step 5: Write the integration tests**

Append to `internal/rollup/verify_integration_test.go`, adding `"math/rand"`, `"sync"` and `"strings"` to its imports as the tests need:

```go
// targetNamed finds a table among the targets.
func targetNamed(t *testing.T, targets []Target, table string) Target {
	t.Helper()
	for _, tg := range targets {
		if tg.Table == table {
			return tg
		}
	}
	t.Fatalf("no target %s", table)
	return Target{}
}

// A table still filling differs from its source by design, and so does
// every grain its chunks' upserts reach. A grain added later skips only
// itself: the tables it is built from are full.
func TestIntegrationRollupRun_VerifySkipsTablesStillFilling(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	srv := startRollupPostgres(t)
	ctx := context.Background()
	history(t, srv.conn, "2026-09-11T00:00:00Z", "2026-09-12T23:59:00Z")
	mustInstall(t, srv.conn, loadExample(t))

	targets, err := VerifyTargets(ctx, srv.conn, loadExample(t))
	assert.NoError(t, err)
	assert.Equal(t, 10, len(targets))
	for _, tg := range targets {
		assert.Equal(t, "backfill pending", tg.Skip)
	}

	fillAll(t, srv.conn, loadExample(t))
	withWeek := loadExample(t)
	withWeek.Rollups[0].Grains["7d"] = config.RollupGrain{From: "1d"}
	mustInstall(t, srv.conn, withWeek)
	targets, err = VerifyTargets(ctx, srv.conn, withWeek)
	assert.NoError(t, err)
	for _, tg := range targets {
		if strings.HasSuffix(tg.Table, "_7d") {
			assert.Equal(t, "backfill pending", tg.Skip)
			continue
		}
		assert.Equal(t, "", tg.Skip)
		v, err := VerifySince(ctx, srv.conn, tg, nil)
		assert.NoError(t, err)
		assert.Equal(t, int64(0), v.DriftBuckets)
	}
}

// A removed grain keeps its table and its trigger, so verify still checks
// it, against the shape install recorded.
func TestIntegrationRollupRun_ARetainedTableIsStillVerified(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	srv := startRollupPostgres(t)
	ctx := context.Background()
	history(t, srv.conn, "2026-09-12T00:00:00Z", "2026-09-12T23:59:00Z")
	withWeek := loadExample(t)
	withWeek.Rollups[0].Grains["7d"] = config.RollupGrain{From: "1d"}
	mustInstall(t, srv.conn, withWeek)
	fillAll(t, srv.conn, withWeek)
	mustInstall(t, srv.conn, loadExample(t))

	targets, err := VerifyTargets(ctx, srv.conn, loadExample(t))
	assert.NoError(t, err)
	week := targetNamed(t, targets, "posts_by_lang_7d")
	assert.True(t, week.Retained)
	assert.Equal(t, "posts_by_lang_1d", week.BuiltFrom)
	v, err := VerifySince(ctx, srv.conn, week, nil)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), v.DriftBuckets)

	execSQL(t, srv.conn, `UPDATE posts_by_lang_7d SET posts = posts + 1 WHERE lang = 'en'`)
	v, err = VerifySince(ctx, srv.conn, week, nil)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), v.DriftBuckets)
}

// `rollup ddl`'s migration creates the tables and fills them itself, and
// creates no state table. Verify checks every table there.
func TestIntegrationRollupRun_VerifyNeedsNoStateTable(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	srv := startRollupPostgres(t)
	ctx := context.Background()
	history(t, srv.conn, "2026-09-12T00:00:00Z", "2026-09-12T23:59:00Z")
	applyDDL(t, srv.conn, exampleDDL(t))

	targets, err := VerifyTargets(ctx, srv.conn, loadExample(t))
	assert.NoError(t, err)
	assert.Equal(t, 10, len(targets))
	for _, tg := range targets {
		assert.Equal(t, "", tg.Skip)
		v, err := VerifySince(ctx, srv.conn, tg, nil)
		assert.NoError(t, err)
		assert.That(t, v.Buckets > 0)
		assert.Equal(t, int64(0), v.DriftBuckets)
	}
}

// A table whose trigger stopped trails the table it is built from. Its
// newest stored bucket is older than the newest its from table makes, and
// the check covers the newer one, where the rows are missing.
func TestIntegrationRollupRun_VerifyNewestChecksWhereATableTrails(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	srv := startRollupPostgres(t)
	ctx := context.Background()
	history(t, srv.conn, "2026-09-12T00:00:00Z", "2026-09-12T23:44:00Z")
	mustInstall(t, srv.conn, loadExample(t))
	fillAll(t, srv.conn, loadExample(t))

	r := loadExample(t).Rollups[0]
	v, ok, err := VerifyNewest(ctx, srv.conn, targetFor(t, r, "posts_by_lang_1h"))
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.That(t, v.From.Equal(at("2026-09-12T22:00:00Z")) && v.To.Equal(at("2026-09-13T00:00:00Z")))
	assert.Equal(t, int64(2), v.Buckets)

	execSQL(t, srv.conn, `ALTER TABLE posts_by_lang_5m DISABLE TRIGGER sqlflow_rollup_posts_by_lang_15m_ins`)
	execSQL(t, srv.conn, `INSERT INTO posts_per_minute_by_lang (bucket, lang, posts) VALUES ('2026-09-12T23:50:00Z', 'en', 3)`)
	v, ok, err = VerifyNewest(ctx, srv.conn, targetFor(t, r, "posts_by_lang_15m"))
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.That(t, v.To.Equal(at("2026-09-13T00:00:00Z")))
	assert.Equal(t, int64(1), v.DriftBuckets)
	assert.Equal(t, "missing", v.Sample[0].Kind)
}

// One statement reads one snapshot, and a writer writes the source and
// every grain in one transaction, so verify beside live writers never sees
// half a write. The spec asks for 60 seconds; 20 keep CI short and still
// run hundreds of checks against about a thousand flushes.
func TestIntegrationRollupRun_VerifyUnderConcurrentWritesReportsNoDrift(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	srv := startRollupPostgres(t)
	ctx := context.Background()
	history(t, srv.conn, "2026-09-10T00:00:00Z", "2026-09-12T23:59:00Z")
	mustInstall(t, srv.conn, loadExample(t))
	fillAll(t, srv.conn, loadExample(t))
	targets, err := VerifyTargets(ctx, srv.conn, loadExample(t))
	assert.NoError(t, err)

	start := at("2026-09-10T00:00:00Z")
	stop := make(chan struct{})
	var wg sync.WaitGroup
	var flushes, failed int64
	var mu sync.Mutex
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed))
			for {
				select {
				case <-stop:
					return
				default:
				}
				err := flushMinutes(srv.dsn, randomMinutes(rng, start))
				mu.Lock()
				flushes++
				if err != nil {
					failed++
				}
				mu.Unlock()
			}
		}(int64(w + 1))
	}

	checks := 0
	for deadline := time.Now().Add(20 * time.Second); time.Now().Before(deadline); {
		for _, tg := range targets {
			v, err := VerifySince(ctx, srv.conn, tg, nil)
			assert.NoError(t, err)
			if v.DriftBuckets != 0 {
				close(stop)
				wg.Wait()
				t.Fatalf("%s drifted beside live writers: %+v", tg.Table, v.Sample)
			}
			checks++
		}
	}
	close(stop)
	wg.Wait()
	t.Logf("%d checks beside %d flushes", checks, flushes)
	assert.Equal(t, int64(0), failed)
	assert.That(t, checks > 100 && flushes > 100)
}
```

`applyDDL` and `exampleDDL` are the 2026-09-15 helpers in `postgres_integration_test.go`. The example declares 5 grains in 2 sets, so 10 targets.

- [ ] **Step 6: Run them**

Run: `go test -count=1 -run 'TestIntegrationRollupRun_(VerifySkipsTablesStillFilling|ARetainedTableIsStillVerified|VerifyNeedsNoStateTable|VerifyNewestChecksWhereATableTrails|VerifyUnderConcurrentWritesReportsNoDrift)$' ./internal/rollup/`
Expected: PASS. For a failing run of the two tests that guard a decision, break it once and restore it: without the `to_regclass` check, `VerifyNeedsNoStateTable` FAILS with `42P01`; with `filling` left empty, `VerifySkipsTablesStillFilling` FAILS on the first `Skip` assertion.

- [ ] **Step 7: Commit**

Message: `rollup: verify every declared and retained table, skipping any still filling`.

---

### Task 3: `internal/freshness` and the store identity

**Files:**
- Create: `internal/freshness/freshness.go`
- Create: `internal/freshness/freshness_test.go`
- Create: `internal/freshness/freshness_integration_test.go`

**Interfaces:**
- Consumes: pgx/v5, pgconn.
- Produces: `type Querier interface { QueryRow(ctx context.Context, sql string, args ...any) pgx.Row }`; `type Observation struct { Table, TimeColumn string; Grain time.Duration; NewestBucketAt *time.Time; ObservedAt time.Time }`; `func (o Observation) Age() (time.Duration, bool)`; `func Observe(ctx context.Context, q Querier, table, timeColumn string, grain time.Duration) (Observation, error)`; `type Store struct { ID, Kind string }`; `const KindSystem = "system"`, `const KindAddress = "address"`; `func StoreOf(ctx context.Context, q Querier) (Store, error)`.

- [ ] **Step 1: Write the failing unit tests**

Create `internal/freshness/freshness_test.go`:

```go
package freshness

import (
	"testing"
	"time"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

// A bucket's data is as old as the bucket's end: an hour bucket that
// started at 18:00 is complete at 19:00.
func TestCliRollupRun_AgeRunsFromTheNewestBucketsEnd(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")

	newest := time.Date(2026, 9, 24, 18, 0, 0, 0, time.UTC)
	o := Observation{Grain: time.Hour, NewestBucketAt: &newest, ObservedAt: newest.Add(time.Hour + 41*time.Minute)}
	age, ok := o.Age()
	assert.True(t, ok)
	assert.Equal(t, 41*time.Minute, age)

	_, ok = Observation{Grain: time.Hour}.Age()
	assert.False(t, ok)
}

// The two forms of a store's id, fixed so a reporter built from another
// version names the same store the same way.
func TestCliRollupRun_AStoreIDHasTwoForms(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")

	assert.Equal(t, "pg:71b9f0d711eccc11", systemID(7412345678901234567, "rollup"))
	assert.Equal(t, "pg:f431cc3d5ad5f18e", addressID("172.17.0.2", 5432, "rollup"))
}
```

The expected values are the first 16 hex characters of `sha256("7412345678901234567/rollup")` and `sha256("172.17.0.2:5432/rollup")`, computed with `shasum -a 256` on 2026-09-25.

- [ ] **Step 2: Run them to verify they fail**

Run: `go test -count=1 ./internal/freshness/`
Expected: FAIL to compile, `undefined: Observation`.

- [ ] **Step 3: Write the package**

Create `internal/freshness/freshness.go`:

```go
// Package freshness measures how recent the data in a table is, and names
// the store it lives in the same way for every reporter. It knows tables,
// time columns and grains, and nothing about rollups or pipelines, so any
// process that reaches a store reports the same numbers: the rollup daemon
// first, and `sqlflow monitor` later.
package freshness

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Querier is the one method this package needs; *pgx.Conn has it.
type Querier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Observation is a table's newest bucket as one reporter read it.
type Observation struct {
	// Table is qualified by its schema, so two reporters whose search paths
	// differ name one table the same way.
	Table      string
	TimeColumn string
	Grain      time.Duration
	// NewestBucketAt is the start of the newest bucket, nil for an empty
	// table.
	NewestBucketAt *time.Time
	// ObservedAt is the database's clock at the read, so the age never
	// depends on the reporter's clock.
	ObservedAt time.Time
}

// Age is the time from the end of the newest bucket to ObservedAt. A
// bucket ends at its start plus the grain. ok is false for an empty table.
func (o Observation) Age() (age time.Duration, ok bool) {
	if o.NewestBucketAt == nil {
		return 0, false
	}
	return o.ObservedAt.Sub(o.NewestBucketAt.Add(o.Grain)), true
}

func quote(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// Observe reads table's newest bucket. timeColumn holds bucket starts, and
// an index that leads with it makes the read one index probe. table is
// unqualified and resolves through the connection's search_path, as the
// writers' statements do.
func Observe(ctx context.Context, q Querier, table, timeColumn string, grain time.Duration) (Observation, error) {
	o := Observation{Table: table, TimeColumn: timeColumn, Grain: grain}
	var schema *string
	err := q.QueryRow(ctx, fmt.Sprintf(
		"SELECT (SELECT relnamespace::regnamespace::text FROM pg_class WHERE oid = to_regclass($1)), max(%s), now() FROM %s",
		quote(timeColumn), quote(table)), quote(table)).Scan(&schema, &o.NewestBucketAt, &o.ObservedAt)
	if err != nil {
		return o, fmt.Errorf("freshness %s: %w", table, err)
	}
	if schema != nil {
		o.Table = *schema + "." + table
	}
	return o, nil
}

// Store names the database a table lives in.
type Store struct {
	// ID is "pg:" and 16 hex characters. It carries no host and no
	// credentials.
	ID string
	// Kind is KindSystem or KindAddress.
	Kind string
}

const (
	// KindSystem is an id from the server's system identifier, which
	// belongs to its data directory: every reporter derives the same id,
	// whatever hostname it dialed.
	KindSystem = "system"
	// KindAddress is an id from the address the server answered on, used
	// when the server refuses pg_control_system(). Another reporter may
	// see another address, so the id may not match its.
	KindAddress = "address"
)

// StoreOf names the database q reaches.
func StoreOf(ctx context.Context, q Querier) (Store, error) {
	var sysid int64
	var db string
	err := q.QueryRow(ctx, "SELECT system_identifier, current_database() FROM pg_control_system()").Scan(&sysid, &db)
	if err == nil {
		return Store{ID: systemID(sysid, db), Kind: KindSystem}, nil
	}
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "42501" {
		return Store{}, fmt.Errorf("store id: %w", err)
	}
	var host string
	var port int
	if err := q.QueryRow(ctx,
		"SELECT coalesce(host(inet_server_addr()), 'local'), coalesce(inet_server_port(), 0), current_database()").Scan(&host, &port, &db); err != nil {
		return Store{}, fmt.Errorf("store id: %w", err)
	}
	return Store{ID: addressID(host, port, db), Kind: KindAddress}, nil
}

func systemID(sysid int64, db string) string {
	return hashID(fmt.Sprintf("%d/%s", sysid, db))
}

func addressID(host string, port int, db string) string {
	return hashID(fmt.Sprintf("%s:%d/%s", host, port, db))
}

func hashID(s string) string {
	sum := sha256.Sum256([]byte(s))
	return "pg:" + hex.EncodeToString(sum[:])[:16]
}
```

- [ ] **Step 4: Run the unit tests**

Run: `go test -count=1 -short ./internal/freshness/`
Expected: PASS.

- [ ] **Step 5: Write the integration tests**

Create `internal/freshness/freshness_integration_test.go`:

```go
package freshness

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

func startPostgres(t *testing.T) (string, *pgx.Conn) {
	t.Helper()
	ctx := context.Background()
	pg, err := tcpostgres.Run(ctx, "postgres:18",
		tcpostgres.WithDatabase("rollup"),
		tcpostgres.WithUsername("rollup"),
		tcpostgres.WithPassword("rollup"),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	t.Cleanup(func() { _ = pg.Terminate(context.Background()) })
	dsn, err := pg.ConnectionString(ctx, "sslmode=disable")
	assert.NoError(t, err)
	return dsn, connect(t, dsn)
}

func connect(t *testing.T, dsn string) *pgx.Conn {
	t.Helper()
	conn, err := pgx.Connect(context.Background(), dsn)
	assert.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	return conn
}

func exec(t *testing.T, conn *pgx.Conn, sql string) {
	t.Helper()
	_, err := conn.Exec(context.Background(), sql)
	assert.NoError(t, err)
}

func TestIntegrationRollupRun_ObserveReadsTheNewestBucket(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	_, conn := startPostgres(t)
	ctx := context.Background()
	exec(t, conn, "CREATE TABLE posts_1h (bucket TIMESTAMPTZ PRIMARY KEY, posts BIGINT)")

	o, err := Observe(ctx, conn, "posts_1h", "bucket", time.Hour)
	assert.NoError(t, err)
	assert.Equal(t, "public.posts_1h", o.Table)
	assert.That(t, o.NewestBucketAt == nil)
	_, ok := o.Age()
	assert.False(t, ok)

	exec(t, conn, "INSERT INTO posts_1h VALUES (date_trunc('hour', now()) - interval '2 hours', 1), (date_trunc('hour', now()) - interval '1 hour', 2)")
	o, err = Observe(ctx, conn, "posts_1h", "bucket", time.Hour)
	assert.NoError(t, err)
	age, ok := o.Age()
	assert.True(t, ok)
	assert.That(t, age >= 0 && age < time.Hour)
}

// Two hostnames for one server name one store. Another database on the
// same server is another store.
func TestIntegrationRollupRun_OneStoreIDForTwoHostnames(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	dsn, conn := startPostgres(t)
	ctx := context.Background()
	assert.That(t, strings.Contains(dsn, "localhost"))
	byIP := connect(t, strings.Replace(dsn, "localhost", "127.0.0.1", 1))

	a, err := StoreOf(ctx, conn)
	assert.NoError(t, err)
	b, err := StoreOf(ctx, byIP)
	assert.NoError(t, err)
	assert.Equal(t, KindSystem, a.Kind)
	assert.Equal(t, a, b)

	exec(t, conn, "CREATE DATABASE other")
	other, err := StoreOf(ctx, connect(t, strings.Replace(dsn, "/rollup?", "/other?", 1)))
	assert.NoError(t, err)
	assert.That(t, other.ID != a.ID)
}

// A server that refuses pg_control_system() still gets an id, marked as
// one another reporter may not share.
func TestIntegrationRollupRun_AStoreThatRefusesTheControlFunctionIsNamedByAddress(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	dsn, conn := startPostgres(t)
	exec(t, conn, "REVOKE EXECUTE ON FUNCTION pg_control_system() FROM PUBLIC")
	exec(t, conn, "CREATE ROLE reader LOGIN PASSWORD 'reader'")
	reader := connect(t, strings.Replace(dsn, "rollup:rollup@", "reader:reader@", 1))

	s, err := StoreOf(context.Background(), reader)
	assert.NoError(t, err)
	assert.Equal(t, KindAddress, s.Kind)
	assert.That(t, strings.HasPrefix(s.ID, "pg:") && len(s.ID) == 19)
}
```

If the container's DSN names `127.0.0.1` rather than `localhost`, swap the two in `OneStoreIDForTwoHostnames` and ledger it.

- [ ] **Step 6: Run them**

Run: `go test -count=1 ./internal/freshness/`
Expected: PASS. For the fallback's failing run: make `StoreOf` return the first error as is, run `AStoreThatRefusesTheControlFunctionIsNamedByAddress`, see it FAIL with `42501`, and restore it.

- [ ] **Step 7: Commit**

Message: `freshness: a table's newest bucket, and one store id for every reporter`.

---

### Task 4: The daemon verifies, observes and reports `degraded`

**Files:**
- Modify: `internal/rollup/daemon/health.go`
- Modify: `internal/rollup/daemon/health_test.go`
- Modify: `internal/rollup/daemon/metrics.go`
- Modify: `internal/rollup/daemon/daemon.go`
- Modify: `internal/rollup/daemon/daemon_integration_test.go`

**Interfaces:**
- Consumes: `rollup.VerifyTargets`, `rollup.VerifyNewest`, `rollup.Verified`, `rollup.Table` (Tasks 1 and 2); `freshness.Observe`, `freshness.StoreOf` (Task 3); `config.ParseServeDuration`, `config.Rollup.Ladder()`.
- Produces: `snapshot.DriftTables []string`, `snapshot.DriftBuckets int64`; `func (h *healthState) setDrift(tables []string, buckets int64)`; `const verifyEvery = 4`; `func (d *Daemon) observe(ctx context.Context, work *pgx.Conn)`; `func (d *Daemon) verify(ctx context.Context, work *pgx.Conn)`.

- [ ] **Step 1: Write the failing health rows**

In `internal/rollup/daemon/health_test.go`, add three rows to `TestCliRollupRun_HealthzRulesInOrder`'s table, after "failed beats standby":

```go
		{"failed beats drift", snapshot{Role: roleLeader, LastContact: now.Add(-time.Hour), DriftTables: []string{"posts_by_lang_15m"}, DriftBuckets: 1},
			"failed", http.StatusServiceUnavailable, "no database round trip for 3600s"},
		{"drift", snapshot{Role: roleLeader, LastContact: now, DriftTables: []string{"posts_by_lang_15m", "posts_total_15m"}, DriftBuckets: 3},
			"degraded", http.StatusOK, "3 buckets differ from the tables they are built from, in posts_by_lang_15m, posts_total_15m"},
		{"drift beats backfilling", snapshot{Role: roleLeader, LastContact: now, PendingTables: 1, DriftTables: []string{"posts_by_lang_15m"}, DriftBuckets: 1},
			"degraded", http.StatusOK, "1 buckets differ from the tables they are built from, in posts_by_lang_15m"},
```

- [ ] **Step 2: Run it to verify it fails**

Run: `go test -count=1 -short -run 'TestCliRollupRun_HealthzRulesInOrder' ./internal/rollup/daemon/`
Expected: FAIL to compile, `unknown field DriftTables in struct literal`.

- [ ] **Step 3: Add drift to health**

In `internal/rollup/daemon/health.go`, add `"strings"` to the imports, add to `snapshot` after `Remaining`:

```go
	// DriftTables are the tables the last verify pass found drifted, and
	// DriftBuckets how many of their buckets differ. A standby verifies
	// nothing, so it holds none.
	DriftTables  []string
	DriftBuckets int64
```

add the setter:

```go
func (h *healthState) setDrift(tables []string, buckets int64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.s.DriftTables, h.s.DriftBuckets = tables, buckets
}
```

and in `healthStatus`, after the `failed` rule:

```go
	if len(s.DriftTables) > 0 {
		return "degraded", fmt.Sprintf("%d buckets differ from the tables they are built from, in %s",
			s.DriftBuckets, strings.Join(s.DriftTables, ", ")), http.StatusOK
	}
```

Change the function's comment's last sentence to: `The first matching rule wins. degraded is 200: a restart cannot fix drift.` In `keepLead`, call `d.health.setDrift(nil, 0)` beside both `d.health.setRole(roleStandby)` calls.

- [ ] **Step 4: Run the health test**

Run: `go test -count=1 -short -run 'TestCliRollupRun_HealthzRulesInOrder' ./internal/rollup/daemon/`
Expected: PASS.

- [ ] **Step 5: Write the failing daemon test**

Append to `internal/rollup/daemon/daemon_integration_test.go`:

```go
// Stopping the triggers from 5m to 15m and then writing drifts 15m. The
// next verify pass reports degraded and names the table, the instruments
// record it, and after the trigger returns and the minute is rewritten the
// next pass is healthy again, with no restart.
func TestIntegrationRollupRun_DriftIsReportedAndClears(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	dsn, conn := startPostgres(t)
	history(t, conn, "2026-09-12T00:00:00Z", "2026-09-12T23:59:00Z")
	addrc := make(chan net.Addr, 1)
	d, _ := running(t, dsn, Options{Metrics: "prometheus", Addr: "127.0.0.1:0", OnListen: func(a net.Addr) { addrc <- a }})
	addr := (<-addrc).String()
	waitFor(t, "healthy", func() bool { return status(d) == "healthy" })

	exec(t, conn, `ALTER TABLE posts_by_lang_5m DISABLE TRIGGER sqlflow_rollup_posts_by_lang_15m_ins`)
	exec(t, conn, `ALTER TABLE posts_by_lang_5m DISABLE TRIGGER sqlflow_rollup_posts_by_lang_15m_upd`)
	exec(t, conn, `INSERT INTO posts_per_minute_by_lang (bucket, lang, posts) VALUES ('2026-09-12T23:58:00Z', 'pt', 9)`)
	waitFor(t, "degraded naming posts_by_lang_15m", func() bool {
		s, reason := d.Health()
		return s == "degraded" && strings.Contains(reason, "posts_by_lang_15m")
	})

	resp, err := http.Get("http://" + addr + "/metrics")
	assert.NoError(t, err)
	text, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	assert.NoError(t, err)
	for _, series := range []string{"rollup_verify_buckets_total", "rollup_drift_buckets_total",
		"rollup_verify_duration_seconds", "rollup_newest_bucket_timestamp_seconds"} {
		assert.That(t, strings.Contains(string(text), series))
	}

	exec(t, conn, `ALTER TABLE posts_by_lang_5m ENABLE TRIGGER sqlflow_rollup_posts_by_lang_15m_ins`)
	exec(t, conn, `ALTER TABLE posts_by_lang_5m ENABLE TRIGGER sqlflow_rollup_posts_by_lang_15m_upd`)
	exec(t, conn, `UPDATE posts_per_minute_by_lang SET posts = posts WHERE bucket = '2026-09-12T23:58:00Z'`)
	waitFor(t, "healthy again", func() bool { return status(d) == "healthy" })
}
```

- [ ] **Step 6: Run it to verify it fails**

Run: `go test -count=1 -run 'TestIntegrationRollupRun_DriftIsReportedAndClears$' ./internal/rollup/daemon/`
Expected: FAIL after 60 s, `waited 60s for degraded naming posts_by_lang_15m`.

- [ ] **Step 7: Add the instruments**

In `internal/rollup/daemon/metrics.go`, add to `instruments`:

```go
	verifyBuckets  metric.Int64Counter
	driftBuckets   metric.Int64Counter
	verifyDuration metric.Float64Histogram
	newestBucket   metric.Float64Gauge
```

create them in `newInstruments` before `errors`:

```go
	if in.verifyBuckets, err = m.Int64Counter("rollup_verify_buckets",
		metric.WithDescription("Buckets recomputed and compared")); err != nil {
		return nil, err
	}
	if in.driftBuckets, err = m.Int64Counter("rollup_drift_buckets",
		metric.WithDescription("Buckets that differed from the table they are built from")); err != nil {
		return nil, err
	}
	if in.verifyDuration, err = m.Float64Histogram("rollup_verify_duration",
		metric.WithDescription("One verify pass"), metric.WithUnit("s")); err != nil {
		return nil, err
	}
	if in.newestBucket, err = m.Float64Gauge("rollup_newest_bucket_timestamp",
		metric.WithDescription("The start of the newest bucket of each table and of the source, in Unix seconds"),
		metric.WithUnit("s")); err != nil {
		return nil, err
	}
```

and change the `rollup_errors` description to `Errors by phase: install, backfill, verify, observe or lock`.

- [ ] **Step 8: Add the passes**

In `internal/rollup/daemon/daemon.go`, add imports `"github.com/turbolytics/sql-flow/internal/config"` (already present) and `"github.com/turbolytics/sql-flow/internal/freshness"`, and add:

```go
// verifyEvery is how many intervals apart verify passes run: 60 seconds at
// the default interval. An observe pass runs every interval.
const verifyEvery = 4
```

In `Run`, after `d.logInstall(report)`, add `d.logStore(ctx, work)`. Replace the loop's body up to `if ctx.Err() != nil` with:

```go
	lead := &session{}
	defer lead.close()
	var checked, observed, verified time.Time
	for {
		if time.Since(checked) >= d.interval {
			d.keepLead(ctx, lead)
			checked = time.Now()
		}
		more := false
		if lead.leading {
			work = d.keepWork(ctx, work)
			if work != nil {
				if time.Since(observed) >= d.interval {
					d.observe(ctx, work)
					observed = time.Now()
				}
				if time.Since(verified) >= verifyEvery*d.interval {
					d.verify(ctx, work)
					verified = time.Now()
				}
				more = d.fillOne(ctx, work)
			}
		}
```

and add the methods:

```go
// logStore logs the store's id once, so a log line names the database the
// daemon manages without its host or credentials.
func (d *Daemon) logStore(ctx context.Context, work *pgx.Conn) {
	s, err := freshness.StoreOf(ctx, work)
	if err != nil {
		d.log.Warn("naming the store", zap.Error(err))
		return
	}
	d.log.Info("store", zap.String("store_id", s.ID), zap.String("store_id_kind", s.Kind))
}

// observe records the newest bucket of every declared table and of each
// source. A table it cannot read counts an error and the pass moves on.
func (d *Daemon) observe(ctx context.Context, work *pgx.Conn) {
	for _, r := range d.conf.Rollups {
		if w, err := config.ParseServeDuration(r.Source.Grain); err == nil {
			d.observeTable(ctx, work, r.Name, r.Source.Table, r.Source.TimeColumn, w)
		}
		for _, set := range r.DimensionSets {
			for _, g := range r.Ladder() {
				d.observeTable(ctx, work, r.Name, rollup.Table(set, g.Name), r.Source.TimeColumn, g.Width)
			}
		}
	}
}

func (d *Daemon) observeTable(ctx context.Context, work *pgx.Conn, rollupName, table, timeColumn string, grain time.Duration) {
	o, err := freshness.Observe(ctx, work, table, timeColumn, grain)
	if err != nil {
		if ctx.Err() == nil {
			d.log.Warn("observing a table", zap.String("table", table), zap.Error(err))
			d.m.errors.Add(ctx, 1, phase("observe"))
		}
		return
	}
	d.health.touch(time.Now())
	if o.NewestBucketAt != nil {
		d.m.newestBucket.Record(ctx, float64(o.NewestBucketAt.Unix()),
			metric.WithAttributes(attribute.String("rollup", rollupName), attribute.String("table", table)))
	}
}

// verify checks the newest two buckets of every table not still filling.
// Drift sets degraded until a pass finds none: a restart cannot fix it.
// Each drifted table logs its first rows, at most 10.
func (d *Daemon) verify(ctx context.Context, work *pgx.Conn) {
	start := time.Now()
	targets, err := rollup.VerifyTargets(ctx, work, d.conf)
	if err != nil {
		if ctx.Err() == nil {
			d.log.Warn("listing tables to verify", zap.Error(err))
			d.m.errors.Add(ctx, 1, phase("verify"))
		}
		return
	}
	var drifted []string
	var buckets int64
	for _, tg := range targets {
		if tg.Skip != "" {
			continue
		}
		v, ok, err := rollup.VerifyNewest(ctx, work, tg)
		if err != nil {
			if ctx.Err() == nil {
				d.log.Warn("verifying a table", zap.String("table", tg.Table), zap.Error(err))
				d.m.errors.Add(ctx, 1, phase("verify"))
			}
			continue
		}
		if !ok {
			continue
		}
		attrs := metric.WithAttributes(attribute.String("rollup", v.Rollup), attribute.String("table", v.Table))
		d.m.verifyBuckets.Add(ctx, v.Buckets, attrs)
		if v.DriftBuckets == 0 {
			continue
		}
		d.m.driftBuckets.Add(ctx, v.DriftBuckets, attrs)
		drifted = append(drifted, v.Table)
		buckets += v.DriftBuckets
		for _, row := range v.Sample {
			d.log.Warn("drift", zap.String("rollup", v.Rollup), zap.String("table", v.Table),
				zap.String("built_from", v.BuiltFrom), zap.Time("bucket", row.Bucket), zap.String("key", row.Key),
				zap.String("kind", row.Kind), zap.String("measure", row.Measure),
				zap.String("stored", row.Stored), zap.String("recomputed", row.Recomputed))
		}
	}
	if ctx.Err() != nil {
		return
	}
	d.health.touch(time.Now())
	d.health.setDrift(drifted, buckets)
	d.m.verifyDuration.Record(ctx, time.Since(start).Seconds())
}
```

- [ ] **Step 9: Run the daemon tests**

Run: `go test -count=1 ./internal/rollup/daemon/`
Expected: PASS, including every Plan 2a daemon test.

- [ ] **Step 10: Commit**

Message: `daemon: verify every minute, observe every interval, and report drift as degraded`.

---

### Task 5: `sqlflow rollup verify`

**Files:**
- Modify: `internal/errs/registry.go`, `internal/errs/testdata/codes.golden`
- Create: `internal/cli/rollup/verify.go`
- Modify: `internal/cli/rollup/rollup.go`
- Create: `internal/cli/rollup/verify_test.go`
- Create: `internal/cli/rollup/verify_integration_test.go`

**Interfaces:**
- Consumes: `gen.VerifyTargets`, `gen.VerifySince`, `gen.Verified`, `gen.Connect`, `gen.PostgresDDL` (internal/rollup, imported as `gen`); `withStore`, `run`, `readFile`, `example` (CLI test helpers).
- Produces: `errs.CodeRollupDrift`; `func newVerifyCommand() *cobra.Command`; `func parseSince(s string, now time.Time) (*time.Time, error)`.

- [ ] **Step 1: Write the failing unit tests**

Create `internal/cli/rollup/verify_test.go`:

```go
package rollup

import (
	"strings"
	"testing"
	"time"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/turbolytics/sql-flow/internal/errs"
	"github.com/zeebo/assert"
)

func TestCliRollupRun_VerifyIsListed(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")

	out, _, err := run(t, "--help")
	assert.NoError(t, err)
	assert.That(t, strings.Contains(out, "\n  verify "))
}

func TestCliRollupRun_VerifyRefusesAnEmptyDSN(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")

	_, _, err := run(t, "verify", "-c", withStore(t, ""))
	assert.Error(t, err)
	assert.Equal(t, errs.CodeConfigInvalid, errs.CodeOf(err))
}

// A bad --since fails before anything connects: the DSN names a port
// nothing listens on.
func TestCliRollupRun_VerifyRefusesABadSince(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")

	_, _, err := run(t, "verify", "-c", withStore(t, "postgres://rollup@127.0.0.1:1/rollup"), "--since", "yesterday")
	assert.Error(t, err)
	assert.Equal(t, errs.CodeConfigInvalid, errs.CodeOf(err))
}

func TestCliRollupRun_SinceTakesATimeOrADuration(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")

	now := time.Date(2026, 9, 25, 12, 0, 0, 0, time.UTC)
	got, err := parseSince("", now)
	assert.NoError(t, err)
	assert.That(t, got == nil)
	got, err = parseSince("2026-09-01T00:00:00Z", now)
	assert.NoError(t, err)
	assert.That(t, got.Equal(time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)))
	got, err = parseSince("36h", now)
	assert.NoError(t, err)
	assert.That(t, got.Equal(now.Add(-36*time.Hour)))
	got, err = parseSince("7d", now)
	assert.NoError(t, err)
	assert.That(t, got.Equal(now.Add(-7*24*time.Hour)))
}

// Drift is ours to report, not the user's config: a restart cannot fix
// it, but it is not a user error either.
func TestCliRollupRun_DriftIsASystemCode(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")

	err := errs.New(errs.CodeRollupDrift, "3 buckets differ")
	assert.Equal(t, errs.ExitInternal, errs.ExitCode(err))
	_, ok := errs.Lookup(errs.CodeRollupDrift)
	assert.True(t, ok)
}
```

If `errs.Lookup` has another name in `internal/errs/registry.go`, use that and ledger the ruling.

- [ ] **Step 2: Run them to verify they fail**

Run: `go test -count=1 -short -run 'TestCliRollupRun_(Verify|Since|DriftIsASystemCode)' ./internal/cli/rollup/`
Expected: FAIL to compile, `undefined: parseSince` and `undefined: errs.CodeRollupDrift`.

- [ ] **Step 3: Add the code**

In `internal/errs/registry.go`, after `CodeRollupUnreachable`:

```go
	// verify found stored buckets that differ from the table they are built
	// from. A restart cannot fix it: something wrote around the triggers.
	CodeRollupDrift Code = "system.rollup.drift"
```

and its definition beside `CodeRollupUnreachable`'s:

```go
	CodeRollupDrift: {
		CodeRollupDrift,
		"Stored rollup buckets differ from what the table they are built from makes.",
		"Read the rows verify printed: the table, the bucket, and the first measure that differs. A disabled trigger, a hand edit, or a write that bypassed the triggers causes it. Fix the cause, then rewrite the bucket's source rows, for example UPDATE <source> SET <column> = <column> over the bucket's range, which re-merges every grain above it, and run verify again.",
	},
```

Run `UPDATE_GOLDEN=1 go test -count=1 -run TestErrorTaxonomy_RegistryIsAppendOnly ./internal/errs/` and check `git diff internal/errs/testdata/codes.golden` adds exactly `system.rollup.drift`.

- [ ] **Step 4: Write the command**

Create `internal/cli/rollup/verify.go`:

```go
package rollup

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/errs"
	gen "github.com/turbolytics/sql-flow/internal/rollup"
)

func newVerifyCommand() *cobra.Command {
	var configPath, since string
	cmd := &cobra.Command{
		Use:   "verify",
		Short: "Check every rollup table against the table it is built from, then exit",
		Long: "Recompute every rollup table's buckets from the table it is built from and compare " +
			"them with the stored rows. Exits non-zero with system.rollup.drift when a bucket " +
			"differs. verify only reads: it takes no lock and changes nothing, so it runs beside " +
			"`sqlflow rollup run` or on a database no daemon manages. A table still filling is " +
			"skipped. --since bounds the buckets checked, because a retention job makes old " +
			"buckets differ from their source by design.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			conf, err := config.LoadRollups(configPath)
			if err != nil {
				return err
			}
			if err := conf.CheckError(); err != nil {
				return err
			}
			from, err := parseSince(since, time.Now())
			if err != nil {
				return err
			}
			dsn, err := conf.PostgresDSN()
			if err != nil {
				return err
			}
			ctx := cmd.Context()
			if ctx == nil {
				ctx = context.Background()
			}
			conn, err := gen.Connect(ctx, dsn)
			if err != nil {
				return err
			}
			defer conn.Close(context.Background())

			targets, err := gen.VerifyTargets(ctx, conn, conf)
			if err != nil {
				return err
			}
			out := cmd.OutOrStdout()
			var drifted []string
			var buckets int64
			for _, tg := range targets {
				if tg.Skip != "" {
					fmt.Fprintf(out, "%s: skipped, %s\n", tg.Table, tg.Skip)
					continue
				}
				v, err := gen.VerifySince(ctx, conn, tg, from)
				if err != nil {
					return err
				}
				printVerified(out, v)
				if v.DriftBuckets > 0 {
					drifted = append(drifted, v.Table)
					buckets += v.DriftBuckets
				}
			}
			if len(drifted) > 0 {
				return errs.New(errs.CodeRollupDrift, "%d buckets differ from the tables they are built from, in %s",
					buckets, strings.Join(drifted, ", "))
			}
			return nil
		},
	}
	cmd.Flags().StringVarP(&configPath, "config", "c", "", "Path to the rollups file")
	cmd.Flags().StringVar(&since, "since", "", "Check buckets from this RFC 3339 time, or this long ago, such as 36h or 7d; default all")
	_ = cmd.MarkFlagRequired("config")
	return cmd
}

// parseSince reads --since: an RFC 3339 time, or a duration back from now.
// Empty checks every bucket.
func parseSince(s string, now time.Time) (*time.Time, error) {
	if s == "" {
		return nil, nil
	}
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return &t, nil
	}
	if d, err := config.ParseServeDuration(s); err == nil {
		t := now.Add(-d)
		return &t, nil
	}
	return nil, errs.New(errs.CodeConfigInvalid, "--since %q is neither an RFC 3339 time nor a duration such as 36h or 7d", s)
}

func printVerified(w io.Writer, v gen.Verified) {
	fmt.Fprintf(w, "%s: %d buckets checked against %s, %d differ\n", v.Table, v.Buckets, v.BuiltFrom, v.DriftBuckets)
	for _, row := range v.Sample {
		line := fmt.Sprintf("  %s %s", row.Bucket.UTC().Format(time.RFC3339), row.Kind)
		if row.Key != "" {
			line += " " + row.Key
		}
		if row.Measure != "" {
			line += fmt.Sprintf(": %s stored %s, recomputed %s", row.Measure, orNone(row.Stored), orNone(row.Recomputed))
		}
		fmt.Fprintln(w, line)
	}
}

func orNone(s string) string {
	if s == "" {
		return "none"
	}
	return s
}
```

In `internal/cli/rollup/rollup.go`, register `newVerifyCommand()` beside `newRunCommand()`, and add verify to the command's `Long` where it lists install and run.

- [ ] **Step 5: Run the unit tests**

Run: `go test -count=1 -short ./internal/cli/rollup/ ./internal/errs/`
Expected: PASS.

- [ ] **Step 6: Write the integration tests**

Create `internal/cli/rollup/verify_integration_test.go`:

```go
package rollup

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/turbolytics/sql-flow/internal/errs"
	gen "github.com/turbolytics/sql-flow/internal/rollup"
	"github.com/zeebo/assert"
)

// migrated starts a Postgres, writes two days of history, and applies
// `rollup ddl`'s migration, which fills every table and creates no state
// table.
func migrated(t *testing.T) (string, *pgx.Conn) {
	t.Helper()
	ctx := context.Background()
	pg, err := tcpostgres.Run(ctx, "postgres:18",
		tcpostgres.WithDatabase("rollup"),
		tcpostgres.WithUsername("rollup"),
		tcpostgres.WithPassword("rollup"),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	t.Cleanup(func() { _ = pg.Terminate(context.Background()) })
	dsn, err := pg.ConnectionString(ctx, "sslmode=disable")
	assert.NoError(t, err)
	conn, err := pgx.Connect(ctx, dsn)
	assert.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	conf, err := config.LoadRollups(example)
	assert.NoError(t, err)
	script, err := gen.PostgresDDL(conf)
	assert.NoError(t, err)
	for _, sql := range []string{
		`CREATE TABLE posts_per_minute_by_lang (
  bucket TIMESTAMPTZ NOT NULL, lang TEXT NOT NULL, posts INTEGER NOT NULL,
  PRIMARY KEY (bucket, lang))`,
		`INSERT INTO posts_per_minute_by_lang (bucket, lang, posts)
SELECT g, l, 1 FROM generate_series('2026-09-11T00:00:00Z'::timestamptz, '2026-09-12T23:59:00Z', interval '1 minute') AS g,
       unnest(ARRAY['en', 'ja']) AS l`,
		script,
	} {
		_, err := conn.Exec(ctx, sql)
		assert.NoError(t, err)
	}
	return dsn, conn
}

func sqlExec(t *testing.T, conn *pgx.Conn, sql string) {
	t.Helper()
	_, err := conn.Exec(context.Background(), sql)
	assert.NoError(t, err)
}

// verify reads a database no daemon manages, and takes no lock: an install
// held open elsewhere does not stop it.
func TestIntegrationRollupRun_VerifyCommandPassesACleanDatabase(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	dsn, _ := migrated(t)
	holder, err := pgx.Connect(context.Background(), dsn)
	assert.NoError(t, err)
	t.Cleanup(func() { _ = holder.Close(context.Background()) })
	sqlExec(t, holder, "SELECT pg_advisory_lock(hashtextextended('sqlflow_rollup_install', 0))")

	out, _, err := run(t, "verify", "-c", withStore(t, dsn))
	assert.NoError(t, err)
	assert.Equal(t, 10, strings.Count(out, ", 0 differ"))
	assert.That(t, strings.Contains(out, "posts_by_lang_15m: 192 buckets checked against posts_by_lang_5m, 0 differ"))
}

func TestIntegrationRollupRun_VerifyCommandFailsOnDriftAndNamesTheBucket(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	dsn, conn := migrated(t)
	sqlExec(t, conn, `ALTER TABLE posts_by_lang_5m DISABLE TRIGGER sqlflow_rollup_posts_by_lang_15m_ins`)
	sqlExec(t, conn, `ALTER TABLE posts_by_lang_5m DISABLE TRIGGER sqlflow_rollup_posts_by_lang_15m_upd`)
	sqlExec(t, conn, `UPDATE posts_per_minute_by_lang SET posts = 7 WHERE bucket = '2026-09-11T10:02:00Z' AND lang = 'en'`)

	out, _, err := run(t, "verify", "-c", withStore(t, dsn))
	assert.Error(t, err)
	assert.Equal(t, errs.CodeRollupDrift, errs.CodeOf(err))
	assert.That(t, strings.Contains(out, "posts_by_lang_15m: 192 buckets checked against posts_by_lang_5m, 1 differ"))
	assert.That(t, strings.Contains(out, "  2026-09-11T10:00:00Z differs lang=en: posts stored 15, recomputed 21"))

	// The drift is on 2026-09-11, so a check from the next day passes.
	out, _, err = run(t, "verify", "-c", withStore(t, dsn), "--since", "2026-09-12T00:00:00Z")
	assert.NoError(t, err)
	assert.That(t, strings.Contains(out, "posts_by_lang_15m: 96 buckets checked against posts_by_lang_5m, 0 differ"))
}
```

Two days of 15m buckets is 192; one day is 96. The 15m bucket at 10:00 holds 15 minutes of `posts = 1` for `en`, so 15 stored; the rewritten minute adds 6, so 21 recomputed.

- [ ] **Step 7: Run them**

Run: `go test -count=1 -run 'TestIntegrationRollupRun_VerifyCommand' ./internal/cli/rollup/`
Expected: PASS. For a failing run: remove the `--since` bound from `VerifySince`'s `lo`, run `VerifyCommandFailsOnDriftAndNamesTheBucket`, see the second `run` FAIL with drift, and restore it.

- [ ] **Step 8: Commit**

Message: `cli: sqlflow rollup verify, which exits system.rollup.drift when a bucket differs`.

---

### Task 6: Docs, the spec, and the PR

**Files:**
- Modify: `README.md`, `CHANGELOG.md`
- Modify: `docs/superpowers/specs/2026-09-24-rollup-daemon-design.md`
- Modify: this plan, its "After execution" section

- [ ] **Step 1: README**

In the rollup commands table beside `install` and `run`, add a `verify` row: `Recomputes every table from the table it is built from and exits system.rollup.drift when a bucket differs. It only reads. --since bounds the buckets checked.` After the paragraph about `run` filling history, add one sentence: `run also checks each table's newest two buckets every minute; drift turns /healthz to degraded, which returns 200 because a restart cannot fix it.`

- [ ] **Step 2: CHANGELOG**

Under `## Unreleased`, `### Added`, one entry: `sqlflow rollup verify` checks every rollup table against the table it is built from, and `sqlflow rollup run` does the same for each table's newest two buckets every minute, reporting drift as `degraded` on `/healthz`. New metrics: `rollup_verify_buckets`, `rollup_drift_buckets`, `rollup_verify_duration`, `rollup_newest_bucket_timestamp`. New error code: `system.rollup.drift`.

- [ ] **Step 3: Spec amendments**

In the spec:
- "Verify": add that rows pair through `GROUP BY`, because Postgres refuses a full join on `IS NOT DISTINCT FROM`; that the newest bucket is the later of the table's newest and the newest its from table makes; and that a table still filling, and every table its upserts reach, is skipped, in the loop and in `sqlflow rollup verify`.
- "The loop": verify runs every fourth observe interval.
- "Tests": `VerifyUnderConcurrentWritesReportsNoDrift` runs 20 seconds.
- "What breaks if this is wrong": rows for a full join on `=` (a `NULL` dimension reads as drift, `ANullDimensionAndADoubleSumVerifyClean`), and for verifying a table still filling (`degraded` during every backfill, `VerifySkipsTablesStillFilling`). The row "Verify compares doubles exactly" names `ANullDimensionAndADoubleSumVerifyClean` and `OnlyADoubleSumMatchesWithinATolerance` in place of "a unit test that sums doubles in two orders": Postgres picks the summation order, so a test cannot choose two orders, and the integration test stores a last-bit difference instead.

- [ ] **Step 4: The whole suite**

Run: `go build ./... && go test -count=1 -short ./... && go test -count=1 ./internal/rollup/... ./internal/freshness/ ./internal/cli/rollup/ && uv run --locked pytest tests/tooling -q`
Expected: every package ok, and tooling passes.

- [ ] **Step 5: Commit, push, and open the PR**

Commit the docs with message `docs: sqlflow rollup verify and degraded`. Write the PR body to a file:

```
Plan 2b of `docs/superpowers/specs/2026-09-24-rollup-daemon-design.md`: SQLFlow checks the rollups the triggers keep.

- `sqlflow rollup verify` recomputes every table from the table it is built from and exits `system.rollup.drift` when a bucket differs. It only reads, so it runs beside the daemon or on a database made by `rollup ddl`'s migration.
- `sqlflow rollup run` checks each table's newest two buckets every minute. Drift turns `/healthz` to `degraded`, logs the first rows that differ, and clears on the next clean pass.
- Each edge is checked alone, so a report names the broken one. A table still filling is skipped. A double sum matches within a relative 1e-9.
- `internal/freshness` reads a table's newest bucket and names the store by its system identifier, the same for every reporter. The daemon records `rollup_newest_bucket_timestamp` and logs the store's id; TurboStats reporting is Plan 3.

This PR carries the plan (`docs/superpowers/plans/2026-09-25-rollup-verify.md`) and the spec amendments with the code.
```

Paste the title and body into the chat and wait for the maintainer's go. Then:

Run: `git push -u origin feat/rollup-verify && gh pr create --repo turbolytics/sql-flow --base main --head feat/rollup-verify --title "rollup: sqlflow rollup verify, and drift as degraded" --body-file <body file>`
Expected: a PR URL. Then `gh pr view feat/rollup-verify --repo turbolytics/sql-flow --json state,baseRefName --jq '.state, .baseRefName'` prints `OPEN` and `main`.

---

## After execution

Filled in once the plan has run: where the code differs from the text above, and why.
