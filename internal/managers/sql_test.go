package managers

import (
	"testing"
	"time"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

// Every statement the manager runs, byte for byte, for one declaration.
// The collect and the delete share one predicate, and no statement reads
// now(): the watermark is the only clock.
func TestManagerWindow_GeneratedSQL(t *testing.T) {
	coverage.Covers(t, "manager.window")
	d := Declaration{
		Table:      "posts_per_minute",
		TimeColumn: "bucket",
		Size:       time.Minute,
		Grace:      30 * time.Second,
		Late:       LateDrop,
	}
	at := time.Date(2026, 9, 13, 10, 5, 0, 0, time.UTC)

	assert.Equal(t,
		`"bucket" + INTERVAL '60' SECOND <= TIMESTAMPTZ '2026-09-13 10:05:00+00:00'`,
		d.closedBefore(at))
	assert.Equal(t,
		`SELECT epoch_us(max("bucket")) FROM "posts_per_minute"`,
		d.newestSQL())
	assert.Equal(t,
		`SELECT epoch_us(last_arrival) FROM sqlflow_progress`,
		lastArrivalSQL())
	assert.Equal(t,
		`SELECT count(*)::BIGINT FROM "posts_per_minute" WHERE "bucket" + INTERVAL '60' SECOND <= TIMESTAMPTZ '2026-09-13 10:05:00+00:00'`,
		d.countClosedSQL(at))
	assert.Equal(t,
		`DELETE FROM "posts_per_minute" WHERE "bucket" + INTERVAL '60' SECOND <= TIMESTAMPTZ '2026-09-13 10:05:00+00:00'`,
		d.deleteClosedSQL(at))
	closed := `WITH closed AS (SELECT * FROM "posts_per_minute" WHERE "bucket" + INTERVAL '60' SECOND <= TIMESTAMPTZ '2026-09-13 10:05:00+00:00')`
	assert.Equal(t, closed+` SELECT * FROM closed`, d.collectSQL(at))

	d.EmitSQL = "  SELECT bucket, sum(n) FROM closed GROUP BY ALL  "
	assert.Equal(t, closed+` SELECT bucket, sum(n) FROM closed GROUP BY ALL`, d.collectSQL(at))

	// An emit_sql with its own WITH keeps it: closed joins its list.
	d.EmitSQL = "WITH totals AS (SELECT sum(n) AS n FROM closed)\nSELECT n FROM totals"
	assert.Equal(t, closed+`, totals AS (SELECT sum(n) AS n FROM closed)
SELECT n FROM totals`, d.collectSQL(at))
	d.EmitSQL = "with t as (select 1) select * from t, closed"
	assert.Equal(t, closed+`, t as (select 1) select * from t, closed`, d.collectSQL(at))
}

// An identifier with a quote in it is quoted, not injected.
func TestManagerWindow_IdentifiersAreQuoted(t *testing.T) {
	coverage.Covers(t, "manager.window")
	d := Declaration{Table: `odd"name`, TimeColumn: "when", Size: time.Second, Late: LateDrop}
	assert.Equal(t, `SELECT epoch_us(max("when")) FROM "odd""name"`, d.newestSQL())
}

// A declaration the engine cannot run is refused when the manager is built,
// with a config code, not at the first poll.
func TestManagerWindow_DeclarationIsValidated(t *testing.T) {
	coverage.Covers(t, "manager.window")
	cases := map[string]Declaration{
		"no table":        {TimeColumn: "b", Size: time.Second, Late: LateDrop},
		"no time column":  {Table: "t", Size: time.Second, Late: LateDrop},
		"zero size":       {Table: "t", TimeColumn: "b", Late: LateDrop},
		"negative grace":  {Table: "t", TimeColumn: "b", Size: time.Second, Grace: -1, Late: LateDrop},
		"negative idle":   {Table: "t", TimeColumn: "b", Size: time.Second, IdleClose: -1, Late: LateDrop},
		"bad late policy": {Table: "t", TimeColumn: "b", Size: time.Second, Late: "keep"},
		"no late policy":  {Table: "t", TimeColumn: "b", Size: time.Second},
		"engine table":    {Table: "sqlflow_offsets", TimeColumn: "b", Size: time.Second, Late: LateDrop},
	}
	for name, d := range cases {
		t.Run(name, func(t *testing.T) {
			coverage.Covers(t, "manager.window")
			assert.Error(t, d.validate())
		})
	}
	assert.NoError(t, Declaration{Table: "t", TimeColumn: "b", Size: time.Second, Late: LateReemit}.validate())
}
