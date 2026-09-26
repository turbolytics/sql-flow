package rollup

import (
	"context"
	"testing"
	"time"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

// posts_total counts minutes. With three minutes of 22:00 missing, its
// newest closed hour holds 57 of 60, the least complete of its newest
// closed buckets. posts_by_lang counts nothing, so a rollup of it alone
// reports no completeness.
func TestIntegrationRollupRun_TheLeastCompleteClosedBucketIsReported(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	srv := startRollupPostgres(t)
	ctx := context.Background()
	history(t, srv.conn, "2026-09-12T00:00:00Z", "2026-09-12T23:59:00Z")
	execSQL(t, srv.conn, `DELETE FROM posts_per_minute_by_lang WHERE bucket >= '2026-09-12T22:10:00Z' AND bucket < '2026-09-12T22:13:00Z'`)
	mustInstall(t, srv.conn, loadExample(t))
	fillAll(t, srv.conn, loadExample(t))

	c, ok, err := LeastComplete(ctx, srv.conn, loadExample(t).Rollups[0])
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, "posts_total_1h", c.Table)
	assert.That(t, c.BucketAt.Equal(at("2026-09-12T22:00:00Z")))
	assert.Equal(t, int64(57), c.SourceBuckets)
	assert.Equal(t, int64(60), c.ExpectedBuckets)

	byLang := loadExample(t).Rollups[0]
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
