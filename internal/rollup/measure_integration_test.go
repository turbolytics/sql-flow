package rollup

import (
	"context"
	"testing"
	"time"

	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

// posts_total counts minutes. With three minutes of 22:00 missing, its
// newest closed hour holds 57 of 60, the least complete of its newest
// closed buckets. The table is qualified by its schema, as freshness names
// it, so a receiver joins the two.
//
// posts_by_lang counts minutes too, per language, and German is quiet from
// 23:51 to 23:54. That is one language's sparsity, not a gap in the source:
// the other languages cover those minutes. A set with dimensions counts each
// key's buckets, so it never stands for the source's completeness, and a
// rollup of it alone reports none.
func TestIntegrationRollupRun_TheLeastCompleteClosedBucketIsReported(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	t.Parallel()
	srv := startRollupPostgres(t)
	ctx := context.Background()
	history(t, srv.conn, "2026-09-12T00:00:00Z", "2026-09-12T23:59:00Z")
	execSQL(t, srv.conn, `DELETE FROM posts_per_minute_by_lang WHERE bucket >= '2026-09-12T22:10:00Z' AND bucket < '2026-09-12T22:13:00Z'`)
	execSQL(t, srv.conn, `DELETE FROM posts_per_minute_by_lang WHERE lang = 'de' AND bucket >= '2026-09-12T23:51:00Z' AND bucket < '2026-09-12T23:55:00Z'`)
	conf := loadExample(t)
	conf.Rollups[0].DimensionSets[0].Measures["minutes"] = config.RollupMeasure{Type: "count_buckets"}
	// The demo's dataset folds languages into "other", which count_buckets
	// cannot be summed across; this test serves nothing.
	conf.Rollups[0].Serve = nil
	mustInstall(t, srv.conn, conf)
	fillAll(t, srv.conn, conf)

	c, ok, err := LeastComplete(ctx, srv.conn, conf.Rollups[0])
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, "public.posts_total_1h", c.Table)
	assert.That(t, c.BucketAt.Equal(at("2026-09-12T22:00:00Z")))
	assert.Equal(t, int64(57), c.SourceBuckets)
	assert.Equal(t, int64(60), c.ExpectedBuckets)

	byLang := conf.Rollups[0]
	byLang.DimensionSets = byLang.DimensionSets[:1]
	_, ok, err = LeastComplete(ctx, srv.conn, byLang)
	assert.NoError(t, err)
	assert.False(t, ok)
}

// track_functions is off by default, and pg_stat_user_functions then holds
// no rows for the triggers: the cost is unknown, not zero. A session that
// sets it to pl counts its writes' trigger calls.
func TestIntegrationRollupRun_TriggerCostIsAbsentUntilTheServerTracksFunctions(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	t.Parallel()
	srv := startRollupPostgres(t)
	ctx := context.Background()
	mustInstall(t, srv.conn, loadExample(t))
	fillAll(t, srv.conn, loadExample(t))
	r := loadExample(t).Rollups[0]

	history(t, srv.conn, "2026-09-12T00:00:00Z", "2026-09-12T00:04:00Z")
	_, _, ok, err := TriggerCost(ctx, srv.conn, r)
	assert.NoError(t, err)
	assert.False(t, ok)

	w := connectIn(t, srv.dsn, "UTC")
	execSQL(t, w, "SET track_functions = 'pl'")
	execSQL(t, w, `INSERT INTO posts_per_minute_by_lang (bucket, lang, posts) VALUES ('2026-09-12T00:05:00Z', 'en', 1)`)
	// The writer's session flushes its counts when it goes idle, and the
	// server can show a function's row before its calls, so the test waits
	// for the calls.
	deadline := time.Now().Add(20 * time.Second)
	for {
		calls, seconds, ok, err := TriggerCost(ctx, srv.conn, r)
		assert.NoError(t, err)
		if ok && calls > 0 {
			assert.That(t, seconds >= 0)
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("no trigger calls counted 20s after a tracked write")
		}
		time.Sleep(200 * time.Millisecond)
	}
}
