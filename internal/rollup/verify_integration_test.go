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
