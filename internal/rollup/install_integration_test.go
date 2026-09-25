package rollup

// sqlflow rollup install against a real Postgres: the state row, the checks
// on the source and on existing tables, and the install itself.

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/turbolytics/sql-flow/internal/errs"
	"github.com/zeebo/assert"
)

func TestIntegrationRollupRun_TheStateRowRoundTrips(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	srv := startRollupPostgres(t)
	ctx := context.Background()
	execSQL(t, srv.conn, stateDDL)

	none, err := readState(ctx, srv.conn, "posts")
	assert.NoError(t, err)
	assert.That(t, none == nil)

	day := at("2026-09-20T00:00:00Z")
	want := State{
		Rollup: "posts", Declaration: AppliedFrom(exampleRollup(t)), Version: "test",
		Backfill: map[string]*time.Time{"posts_by_lang_5m": nil, "posts_total_5m": &day},
		Retained: []string{"posts_by_lang_7d"},
	}
	assert.NoError(t, writeState(ctx, srv.conn, want))

	got, err := readState(ctx, srv.conn, "posts")
	assert.NoError(t, err)
	assert.DeepEqual(t, want.Declaration, got.Declaration)
	assert.DeepEqual(t, want.Retained, got.Retained)
	assert.Equal(t, "test", got.Version)
	pending, ok := got.Backfill["posts_by_lang_5m"]
	assert.True(t, ok)
	assert.That(t, pending == nil)
	assert.That(t, got.Backfill["posts_total_5m"].Equal(day))

	all, err := readAllStates(ctx, srv.conn)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(all))
}

func TestIntegrationRollupRun_SourceChecksNameWhatTheTriggersNeed(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	srv := startRollupPostgres(t)
	check := func(r config.Rollup) []config.Violation {
		t.Helper()
		v, err := checkSource(context.Background(), srv.conn, r, pathInFile)
		assert.NoError(t, err)
		return v
	}

	assert.Equal(t, 0, len(check(exampleRollup(t))))

	gone := exampleRollup(t)
	gone.Source.Table = "posts_nowhere"
	v := check(gone)
	assert.Equal(t, 1, len(v))
	assert.Equal(t, errs.CodeConfigRollup, v[0].Code)
	assert.That(t, strings.Contains(v[0].Message, "does not exist"))

	unread := exampleRollup(t)
	unread.DimensionSets[0].Measures["likes"] = config.RollupMeasure{Type: "sum", Column: "likes"}
	v = check(unread)
	assert.Equal(t, 1, len(v))
	assert.That(t, strings.Contains(v[0].Message, "has no column likes"))

	// A key led by lang serves no range on bucket.
	execSQL(t, srv.conn, "ALTER TABLE posts_per_minute_by_lang DROP CONSTRAINT posts_per_minute_by_lang_pkey")
	execSQL(t, srv.conn, "ALTER TABLE posts_per_minute_by_lang ADD PRIMARY KEY (lang, bucket)")
	v = check(exampleRollup(t))
	assert.Equal(t, 1, len(v))
	assert.Equal(t, "rollups.0.source.time_column", strings.Join(v[0].Path, "."))
	assert.That(t, strings.Contains(v[0].Message, `CREATE INDEX ON "posts_per_minute_by_lang" ("bucket")`))
}

func TestIntegrationRollupRun_CompareTableNamesEachColumnThatDiffers(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	srv := startRollupPostgres(t)
	ctx := context.Background()
	r := exampleRollup(t)
	e := edges(r)[0]
	compare := func() []config.Violation {
		t.Helper()
		tx, err := srv.conn.Begin(ctx)
		assert.NoError(t, err)
		defer func() { _ = tx.Rollback(ctx) }()
		v, err := compareTable(ctx, tx, r, e, quote(e.Table), pathInFile)
		assert.NoError(t, err)
		return v
	}

	execSQL(t, srv.conn, "CREATE TABLE posts_by_lang_5m (bucket timestamptz, lang text, posts text, extra int)")
	v := compare()
	assert.Equal(t, 1, len(v))
	assert.Equal(t, errs.CodeConfigRollupChange, v[0].Code)
	assert.Equal(t, "rollups.0.dimension_sets.0", strings.Join(v[0].Path, "."))
	assert.That(t, strings.Contains(v[0].Message, "posts is text, not bigint"))
	assert.That(t, strings.Contains(v[0].Message, "extra is not declared"))

	execSQL(t, srv.conn, "DROP TABLE posts_by_lang_5m")
	objects, err := PostgresObjects(r)
	assert.NoError(t, err)
	execSQL(t, srv.conn, objects)
	assert.Equal(t, 0, len(compare()))
}
