package rollup

// The rollups test harness against a real Postgres: the sandbox, the
// invariants, the generated workload and the fixture cases.

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/turbolytics/sql-flow/internal/errs"
	"github.com/zeebo/assert"
)

func exists(t *testing.T, conn *pgx.Conn, qualified string) bool {
	t.Helper()
	var ok bool
	assert.NoError(t, conn.QueryRow(context.Background(), "SELECT to_regclass($1) IS NOT NULL", qualified).Scan(&ok))
	return ok
}

func constraints(t *testing.T, conn *pgx.Conn, qualified, kind string) int64 {
	t.Helper()
	var n int64
	assert.NoError(t, conn.QueryRow(context.Background(),
		"SELECT count(*) FROM pg_constraint WHERE conrelid = to_regclass($1) AND contype = $2", qualified, kind).Scan(&n))
	return n
}

// The sandbox clones the source without its CHECK constraint, keeps its
// primary key, installs the rollups beside the clone, and leaves the team's
// schema alone. Closing it drops it, unless it is kept.
func TestIntegrationRollupTest_TheSandboxClonesTheSourceAndInstallsThere(t *testing.T) {
	coverage.Covers(t, "cli.rollup_test")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	srv := startRollupPostgres(t)
	ctx := context.Background()
	execSQL(t, srv.conn, "ALTER TABLE posts_per_minute_by_lang ADD CONSTRAINT posts_nonnegative CHECK (posts >= 0)")

	sb, err := OpenSandbox(ctx, srv.conn, loadExample(t), "test")
	assert.NoError(t, err)
	assert.That(t, strings.HasPrefix(sb.Schema, "sqlflow_test_") && len(sb.Schema) == len("sqlflow_test_")+16)
	clone := sb.Schema + ".posts_per_minute_by_lang"
	assert.Equal(t, int64(0), constraints(t, srv.conn, clone, "c"))
	assert.Equal(t, int64(1), constraints(t, srv.conn, clone, "p"))
	assert.That(t, exists(t, srv.conn, sb.Schema+".sqlflow_rollup_state"))
	assert.That(t, exists(t, srv.conn, sb.Schema+".posts_by_lang_5m"))
	assert.False(t, exists(t, srv.conn, "public.posts_by_lang_5m"))

	// The session writes to the clone, and its triggers fill the sandbox.
	execSQL(t, srv.conn, `INSERT INTO posts_per_minute_by_lang (bucket, lang, posts) VALUES ('2026-09-12T12:07:00Z', 'en', -3)`)
	assert.Equal(t, int64(1), count(t, srv.conn, "SELECT count(*) FROM "+sb.Schema+".posts_by_lang_5m"))
	assert.Equal(t, int64(0), count(t, srv.conn, "SELECT count(*) FROM public.posts_per_minute_by_lang"))

	assert.NoError(t, sb.Close(ctx, srv.conn, false))
	assert.Equal(t, int64(0), count(t, srv.conn, "SELECT count(*) FROM pg_namespace WHERE nspname = '"+sb.Schema+"'"))

	kept, err := OpenSandbox(ctx, srv.conn, loadExample(t), "test")
	assert.NoError(t, err)
	assert.NoError(t, kept.Close(ctx, srv.conn, true))
	assert.Equal(t, int64(1), count(t, srv.conn, "SELECT count(*) FROM pg_namespace WHERE nspname = '"+kept.Schema+"'"))
}

// A source the database lacks stops the sandbox with its name, before any
// schema is created.
func TestIntegrationRollupTest_AMissingSourceStopsTheSandbox(t *testing.T) {
	coverage.Covers(t, "cli.rollup_test")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	srv := startRollupPostgres(t)
	execSQL(t, srv.conn, "DROP TABLE posts_per_minute_by_lang")
	_, err := OpenSandbox(context.Background(), srv.conn, loadExample(t), "test")
	assert.Error(t, err)
	assert.That(t, strings.Contains(err.Error(), "posts_per_minute_by_lang"))
	assert.Equal(t, errs.CodeConfigRollup, errs.CodeOf(err))
	assert.That(t, strings.Contains(err.Error(), "does not exist"))
	assert.Equal(t, int64(0), count(t, srv.conn, "SELECT count(*) FROM pg_namespace WHERE nspname LIKE 'sqlflow_test_%'"))
}

// Writes through the triggers keep both invariants. A hand edit breaks the
// first at the edited table, and a bucket off its grain's boundary breaks
// the second.
func TestIntegrationRollupTest_TheInvariantsFindAHandEditAndAnOffBoundaryBucket(t *testing.T) {
	coverage.Covers(t, "cli.rollup_test")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	srv := startRollupPostgres(t)
	ctx := context.Background()
	conf := loadExample(t)
	sb, err := OpenSandbox(ctx, srv.conn, conf, "test")
	assert.NoError(t, err)
	defer func() { _ = sb.Close(ctx, srv.conn, false) }()
	history(t, srv.conn, "2026-09-12T00:00:00Z", "2026-09-12T23:59:00Z")

	failures, err := CheckInvariants(ctx, srv.conn, conf)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(failures))

	execSQL(t, srv.conn, `UPDATE posts_by_lang_1h SET posts = posts + 1 WHERE lang = 'en' AND bucket = '2026-09-12T12:00:00Z'`)
	execSQL(t, srv.conn, `INSERT INTO posts_total_5m (bucket, posts, minutes) VALUES ('2026-09-12T12:07:00Z', 1, 1)`)
	failures, err = CheckInvariants(ctx, srv.conn, conf)
	assert.NoError(t, err)
	found := map[string]bool{}
	for _, f := range failures {
		found[f.Table+" "+f.Invariant] = true
	}
	assert.That(t, found["posts_by_lang_1h "+InvariantEqualsSource])
	assert.That(t, found["posts_total_5m "+InvariantOnBoundary])
}

// workload opens a sandbox for conf, runs the workload with seed, and
// returns what it wrote. The sandbox closes when the test ends.
func workload(t *testing.T, conn *pgx.Conn, conf *config.RollupsConf, seed int64) (*Sandbox, []WorkloadReport) {
	t.Helper()
	ctx := context.Background()
	sb, err := OpenSandbox(ctx, conn, conf, "test")
	assert.NoError(t, err)
	t.Cleanup(func() { _ = sb.Close(ctx, conn, false) })
	reports, err := RunWorkload(ctx, conn, conf, seed)
	assert.NoError(t, err)
	return sb, reports
}

func noFailures(t *testing.T, conn *pgx.Conn, conf *config.RollupsConf) {
	t.Helper()
	failures, err := CheckInvariants(context.Background(), conn, conf)
	assert.NoError(t, err)
	for _, f := range failures {
		t.Errorf("%s %s: %s: %d buckets %v", f.Rollup, f.Table, f.Invariant, f.Buckets, f.Sample)
	}
}

// The demo's declaration keeps both invariants through 20 batches that
// cross the daylight saving change, in three session zones.
func TestIntegrationRollupTest_TheDemoPassesTheWorkload(t *testing.T) {
	coverage.Covers(t, "cli.rollup_test")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	srv := startRollupPostgres(t)
	conf := loadExample(t)
	_, reports := workload(t, srv.conn, conf, 42)
	assert.Equal(t, 1, len(reports))
	assert.Equal(t, "posts", reports[0].Rollup)
	assert.Equal(t, 20, reports[0].Batches)
	assert.That(t, reports[0].Rows >= 20 && reports[0].Rows <= 1000)
	// The workload's days hold the change: 2026-03-08T07:00Z in New York.
	assert.That(t, count(t, srv.conn, "SELECT count(*) FROM posts_by_lang_1d") >= 2)
	noFailures(t, srv.conn, conf)
}

// The Render template's minute table passes: its CHECK constraint is left
// out of the clone, and its defaulted updated_at is not written.
func TestIntegrationRollupTest_TheRenderTemplatePasses(t *testing.T) {
	coverage.Covers(t, "cli.rollup_test")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	srv := startRollupPostgres(t)
	migration, err := os.ReadFile("../../render/migrations/0001_metrics_1m.sql")
	assert.NoError(t, err)
	execSQL(t, srv.conn, string(migration))
	conf, err := config.LoadRollups("../../render/rollups.yml")
	assert.NoError(t, err)
	_, reports := workload(t, srv.conn, conf, 42)
	assert.Equal(t, 20, reports[0].Batches)
	assert.That(t, count(t, srv.conn, "SELECT count(*) FROM metrics_1m WHERE updated_at IS NULL") == 0)
	noFailures(t, srv.conn, conf)
}

const nullableRollup = `rollups:
  - name: events
    source:
      table: events_1m
      time_column: bucket
      grain: 1m
      dimensions: [region]
    grains:
      1h: {from: 1m}
    dimension_sets:
      - name: events_by_region
        dimensions: [region]
        measures:
          n: {type: sum, column: n}
      - name: events_total
        dimensions: []
        measures:
          n: {type: sum, column: n}
`

// A nullable dimension under a NULLS NOT DISTINCT index gets NULL keys, and
// a NULL key upserts and pairs like any other.
func TestIntegrationRollupTest_ANullableDimensionPasses(t *testing.T) {
	coverage.Covers(t, "cli.rollup_test")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	srv := startRollupPostgres(t)
	execSQL(t, srv.conn, `CREATE TABLE events_1m (bucket TIMESTAMPTZ NOT NULL, region TEXT, n BIGINT NOT NULL)`)
	execSQL(t, srv.conn, `CREATE UNIQUE INDEX events_1m_key ON events_1m (bucket, region) NULLS NOT DISTINCT`)
	conf, err := config.ParseRollups([]byte(nullableRollup))
	assert.NoError(t, err)
	workload(t, srv.conn, conf, 42)
	assert.That(t, count(t, srv.conn, "SELECT count(*) FROM events_1m WHERE region IS NULL") > 0)
	assert.That(t, count(t, srv.conn, "SELECT count(*) FROM events_by_region_1h WHERE region IS NULL") > 0)
	noFailures(t, srv.conn, conf)
}

// A seed writes the same rows every time, and another seed other rows.
func TestIntegrationRollupTest_ASeedReproducesTheWorkload(t *testing.T) {
	coverage.Covers(t, "cli.rollup_test")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	srv := startRollupPostgres(t)
	conf := loadExample(t)
	digest := func(seed int64) WorkloadReport {
		ctx := context.Background()
		sb, err := OpenSandbox(ctx, srv.conn, conf, "test")
		assert.NoError(t, err)
		defer func() { assert.NoError(t, sb.Close(ctx, srv.conn, false)) }()
		reports, err := RunWorkload(ctx, srv.conn, conf, seed)
		assert.NoError(t, err)
		return reports[0]
	}
	first, again, other := digest(7), digest(7), digest(8)
	assert.Equal(t, first, again)
	assert.NotEqual(t, first.Digest, other.Digest)
}

// A source whose key has no unique index stops the workload with the key
// the sink's upsert needs.
func TestIntegrationRollupTest_ASourceWithoutAKeyIndexNamesTheKey(t *testing.T) {
	coverage.Covers(t, "cli.rollup_test")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	srv := startRollupPostgres(t)
	execSQL(t, srv.conn, `CREATE TABLE events_1m (bucket TIMESTAMPTZ NOT NULL, region TEXT, n BIGINT NOT NULL)`)
	execSQL(t, srv.conn, `CREATE INDEX events_1m_bucket ON events_1m (bucket)`)
	conf, err := config.ParseRollups([]byte(nullableRollup))
	assert.NoError(t, err)
	ctx := context.Background()
	sb, err := OpenSandbox(ctx, srv.conn, conf, "test")
	assert.NoError(t, err)
	defer func() { _ = sb.Close(ctx, srv.conn, false) }()
	_, err = RunWorkload(ctx, srv.conn, conf, 42)
	assert.Error(t, err)
	assert.That(t, strings.Contains(err.Error(), "unique index on (bucket, region)"))
}

// runCase runs one case in a sandbox for conf, after the workload, as the
// command does.
func runCase(t *testing.T, conn *pgx.Conn, conf *config.RollupsConf, tc config.RollupTestCase) ([]CaseFailure, error) {
	t.Helper()
	workload(t, conn, conf, 42)
	return RunCase(context.Background(), conn, conf, 0, tc)
}

func write(rows ...map[string]any) []map[string]any { return rows }

// The demo's fixture file passes, each case from empty tables after the
// workload filled them.
func TestIntegrationRollupTest_TheDemosCasesPass(t *testing.T) {
	coverage.Covers(t, "cli.rollup_test")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	srv := startRollupPostgres(t)
	conf := loadExample(t)
	tests, err := config.LoadRollupTests("../../dev/config/rollups/bluesky.test.yml")
	assert.NoError(t, err)
	workload(t, srv.conn, conf, 42)
	for i, tc := range tests.Tests {
		failures, err := RunCase(context.Background(), srv.conn, conf, i, tc)
		assert.NoError(t, err)
		for _, f := range failures {
			t.Errorf("%s: %s %s %s: expected %s, got %s", tc.Name, f.Table, f.Kind, f.Key, f.Expected, f.Actual)
		}
	}
}

// An expectation the writes do not make fails with the table, the key and
// both rows.
func TestIntegrationRollupTest_AWrongExpectationFailsWithADiff(t *testing.T) {
	coverage.Covers(t, "cli.rollup_test")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	srv := startRollupPostgres(t)
	failures, err := runCase(t, srv.conn, loadExample(t), config.RollupTestCase{
		Name: "wrong", Rollup: "posts",
		Writes: [][]map[string]any{write(map[string]any{"bucket": "2026-09-24T12:07:00Z", "lang": "en", "posts": 9})},
		Expect: map[string][]map[string]any{
			"posts_by_lang_5m": {{"bucket": "2026-09-24T12:05:00Z", "lang": "en", "posts": 8}},
		},
	})
	assert.NoError(t, err)
	assert.Equal(t, []CaseFailure{{
		Table: "posts_by_lang_5m", Key: "bucket=2026-09-24T12:05:00Z, lang=en", Kind: "differs",
		Expected: "bucket=2026-09-24T12:05:00Z, lang=en, posts=8",
		Actual:   "bucket=2026-09-24T12:05:00Z, lang=en, posts=9",
	}}, failures)
}

// A row the table lacks and a row it holds unexpected each fail by key.
func TestIntegrationRollupTest_AMissingRowAndAnExtraRowFailByKey(t *testing.T) {
	coverage.Covers(t, "cli.rollup_test")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	srv := startRollupPostgres(t)
	failures, err := runCase(t, srv.conn, loadExample(t), config.RollupTestCase{
		Name: "rows", Rollup: "posts",
		Writes: [][]map[string]any{write(
			map[string]any{"bucket": "2026-09-24T12:07:00Z", "lang": "en", "posts": 9},
			map[string]any{"bucket": "2026-09-24T12:07:00Z", "lang": "ja", "posts": 2},
		)},
		Expect: map[string][]map[string]any{
			"posts_by_lang_5m": {
				{"bucket": "2026-09-24T12:05:00Z", "lang": "en", "posts": 9},
				{"bucket": "2026-09-24T12:05:00Z", "lang": "de", "posts": 1},
			},
		},
	})
	assert.NoError(t, err)
	assert.Equal(t, []CaseFailure{
		{Table: "posts_by_lang_5m", Key: "bucket=2026-09-24T12:05:00Z, lang=de", Kind: "missing",
			Expected: "bucket=2026-09-24T12:05:00Z, lang=de, posts=1"},
		{Table: "posts_by_lang_5m", Key: "bucket=2026-09-24T12:05:00Z, lang=ja", Kind: "extra",
			Actual: "bucket=2026-09-24T12:05:00Z, lang=ja, posts=2"},
	}, failures)
}

// A double sum matches its expectation within verify's tolerance, so 0.1
// and 0.2 sum to 0.3.
func TestIntegrationRollupTest_ADoubleSumMatchesWithinTolerance(t *testing.T) {
	coverage.Covers(t, "cli.rollup_test")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	srv := startRollupPostgres(t)
	execSQL(t, srv.conn, `CREATE TABLE gauge_1m (bucket TIMESTAMPTZ NOT NULL, v DOUBLE PRECISION NOT NULL, PRIMARY KEY (bucket))`)
	conf, err := config.ParseRollups([]byte(`rollups:
  - name: gauge
    source: {table: gauge_1m, time_column: bucket, grain: 1m}
    grains:
      5m: {from: 1m}
    dimension_sets:
      - name: gauge
        dimensions: []
        measures:
          v: {type: sum, column: v, numeric: double}
`))
	assert.NoError(t, err)
	failures, err := runCase(t, srv.conn, conf, config.RollupTestCase{
		Name: "tolerance", Rollup: "gauge",
		Writes: [][]map[string]any{write(
			map[string]any{"bucket": "2026-09-24T12:00:00Z", "v": 0.1},
			map[string]any{"bucket": "2026-09-24T12:01:00Z", "v": 0.2},
		)},
		Expect: map[string][]map[string]any{"gauge_5m": {{"bucket": "2026-09-24T12:00:00Z", "v": 0.3}}},
	})
	assert.NoError(t, err)
	assert.Equal(t, 0, len(failures))
}

// A write naming a column the source lacks is refused at its YAML path,
// before the case writes anything.
func TestIntegrationRollupTest_AWriteToAColumnTheSourceLacksIsRefusedAtItsPath(t *testing.T) {
	coverage.Covers(t, "cli.rollup_test")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	srv := startRollupPostgres(t)
	_, err := runCase(t, srv.conn, loadExample(t), config.RollupTestCase{
		Name: "nope", Rollup: "posts",
		Writes: [][]map[string]any{write(map[string]any{"bucket": "2026-09-24T12:07:00Z", "lang": "en", "posts": 9, "nope": 1})},
	})
	assert.Error(t, err)
	assert.That(t, strings.Contains(err.Error(), "tests.0.writes.0.0.nope"))
	assert.Equal(t, int64(0), count(t, srv.conn, "SELECT count(*) FROM posts_per_minute_by_lang WHERE bucket = '2026-09-24T12:07:00Z'"))
}
