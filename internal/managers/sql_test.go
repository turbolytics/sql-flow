package managers

import (
	"testing"
	"time"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

// Every statement the manager runs, byte for byte, for one declaration.
// The collect and the delete share one predicate builder, and no statement
// reads now(): the watermark is the only clock.
func TestManagerWindow_GeneratedSQL(t *testing.T) {
	coverage.Covers(t, "manager.window")
	d := Declaration{
		Table:      "posts_per_minute",
		TimeColumn: "bucket",
		Size:       time.Minute,
		Grace:      30 * time.Second,
		Lateness:   5 * time.Minute,
	}
	closed := time.Date(2026, 9, 13, 10, 0, 0, 0, time.UTC)
	at := time.Date(2026, 9, 13, 10, 5, 0, 0, time.UTC)

	assert.Equal(t,
		`"bucket" + INTERVAL '60' SECOND <= TIMESTAMPTZ '2026-09-13 10:05:00+00:00'`,
		d.closedBefore(at))
	assert.Equal(t,
		`SELECT epoch_us(max("bucket")) FROM "posts_per_minute"`,
		d.newestSQL())
	assert.Equal(t,
		`SELECT epoch_us(min("bucket")) FROM "posts_per_minute"`,
		d.oldestSQL())

	// Due: after the closed watermark, at or before the asserted one. Before
	// the first close, everything at or before the assertion.
	assert.Equal(t,
		`"bucket" + INTERVAL '60' SECOND > TIMESTAMPTZ '2026-09-13 10:00:00+00:00' AND "bucket" + INTERVAL '60' SECOND <= TIMESTAMPTZ '2026-09-13 10:05:00+00:00'`,
		d.dueBetween(closed, true, at))
	assert.Equal(t, d.closedBefore(at), d.dueBetween(time.Time{}, false, at))
	// One bucket, for a recompute.
	assert.Equal(t, `"bucket" = TIMESTAMPTZ '2026-09-13 10:00:00+00:00'`, d.bucketIs(closed))
	// Expired: ended at or before asserted - lateness. With no lateness, at
	// or before the assertion itself.
	assert.Equal(t,
		`"bucket" + INTERVAL '60' SECOND <= TIMESTAMPTZ '2026-09-13 10:00:00+00:00'`,
		d.expiredBefore(at))
	none := d
	none.Lateness = 0
	assert.Equal(t, d.closedBefore(at), none.expiredBefore(at))
	// Retained: closed in an earlier pass, ended after asserted - lateness.
	assert.Equal(t,
		`"bucket" + INTERVAL '60' SECOND > TIMESTAMPTZ '2026-09-13 10:00:00+00:00' AND "bucket" + INTERVAL '60' SECOND <= TIMESTAMPTZ '2026-09-13 10:00:00+00:00'`,
		d.retainedBetween(closed, at))

	where := d.closedBefore(at)
	assert.Equal(t,
		`SELECT count(*)::BIGINT FROM "posts_per_minute" WHERE `+where,
		d.countSQL(where))
	assert.Equal(t,
		`DELETE FROM "posts_per_minute" WHERE `+where,
		d.deleteSQL(where))
	cte := `WITH closed AS (SELECT * FROM "posts_per_minute" WHERE ` + where + `)`
	assert.Equal(t, cte+` SELECT * FROM closed`, d.collectSQL(where))

	d.EmitSQL = "  SELECT bucket, sum(n) FROM closed GROUP BY ALL  "
	assert.Equal(t, cte+` SELECT bucket, sum(n) FROM closed GROUP BY ALL`, d.collectSQL(where))

	// An emit_sql with its own WITH keeps it: closed joins its list.
	d.EmitSQL = "WITH totals AS (SELECT sum(n) AS n FROM closed)\nSELECT n FROM totals"
	assert.Equal(t, cte+`, totals AS (SELECT sum(n) AS n FROM closed)
SELECT n FROM totals`, d.collectSQL(where))
	d.EmitSQL = "with t as (select 1) select * from t, closed"
	assert.Equal(t, cte+`, t as (select 1) select * from t, closed`, d.collectSQL(where))
}

// An identifier with a quote in it is quoted, not injected.
func TestManagerWindow_IdentifiersAreQuoted(t *testing.T) {
	coverage.Covers(t, "manager.window")
	d := Declaration{Table: `odd"name`, TimeColumn: "when", Size: time.Second}
	assert.Equal(t, `SELECT epoch_us(max("when")) FROM "odd""name"`, d.newestSQL())
}

// A declaration the engine cannot run is refused when the manager is built,
// with a config code, not at the first pass.
func TestManagerWindow_DeclarationIsValidated(t *testing.T) {
	coverage.Covers(t, "manager.window")
	cases := map[string]Declaration{
		"no table":          {TimeColumn: "b", Size: time.Second},
		"no time column":    {Table: "t", Size: time.Second},
		"zero size":         {Table: "t", TimeColumn: "b"},
		"negative grace":    {Table: "t", TimeColumn: "b", Size: time.Second, Grace: -1},
		"negative idle":     {Table: "t", TimeColumn: "b", Size: time.Second, IdleClose: -1},
		"negative lateness": {Table: "t", TimeColumn: "b", Size: time.Second, Lateness: -1},
		"engine table":      {Table: "sqlflow_offsets", TimeColumn: "b", Size: time.Second},
	}
	for name, d := range cases {
		t.Run(name, func(t *testing.T) {
			coverage.Covers(t, "manager.window")
			assert.Error(t, d.validate())
		})
	}
	assert.NoError(t, Declaration{Table: "t", TimeColumn: "b", Size: time.Second}.validate())
	assert.NoError(t, Declaration{Table: "t", TimeColumn: "b", Size: time.Second, Lateness: time.Hour}.validate())
}
