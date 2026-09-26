package rollup

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/turbolytics/sql-flow/internal/config"
)

// Completeness is how many source buckets one closed bucket holds, against
// how many its width holds.
type Completeness struct {
	Table           string
	BucketAt        time.Time
	SourceBuckets   int64
	ExpectedBuckets int64
}

// LeastComplete reads the newest closed bucket of every table of r whose
// set counts source buckets, and returns the least complete. A bucket is
// closed once the table holds a newer one, so the open bucket, still
// filling, never reads as a gap. In a set with dimensions the bucket's
// fewest counted buckets stand for it. ok is false for a rollup with no
// count_buckets measure, and before any such table has closed a bucket.
func LeastComplete(ctx context.Context, conn *pgx.Conn, r config.Rollup) (Completeness, bool, error) {
	source, err := config.ParseServeDuration(r.Source.Grain)
	if err != nil {
		return Completeness{}, false, err
	}
	t := quote(r.Source.TimeColumn)
	var worst Completeness
	found := false
	for _, e := range edges(r) {
		measure := ""
		for _, name := range e.Set.MeasureNames() {
			if e.Set.Measures[name].Type == "count_buckets" {
				measure = name
				break
			}
		}
		if measure == "" {
			continue
		}
		var bucket *time.Time
		var present *int64
		err := conn.QueryRow(ctx, fmt.Sprintf(`WITH closed AS (
  SELECT max(%[1]s) AS b FROM %[2]s WHERE %[1]s < (SELECT max(%[1]s) FROM %[2]s))
SELECT closed.b, (SELECT min(%[3]s) FROM %[2]s WHERE %[1]s = closed.b) FROM closed`,
			t, quote(e.Table), quote(measure))).Scan(&bucket, &present)
		if err != nil {
			return Completeness{}, false, fmt.Errorf("rollup completeness %s: %w", e.Table, err)
		}
		if bucket == nil || present == nil {
			continue
		}
		c := Completeness{Table: e.Table, BucketAt: *bucket, SourceBuckets: *present,
			ExpectedBuckets: int64(e.Grain.Width / source)}
		// Compare the fractions by cross-multiplying, so no float rounds two
		// equal ratios apart.
		if !found || c.SourceBuckets*worst.ExpectedBuckets < worst.SourceBuckets*c.ExpectedBuckets {
			worst, found = c, true
		}
	}
	return worst, found, nil
}

// TriggerCost sums the calls and time of r's trigger functions from
// pg_stat_user_functions. The server counts them only in sessions whose
// track_functions is pl or all, and holds no row otherwise, so ok false
// means unknown, not free.
func TriggerCost(ctx context.Context, conn *pgx.Conn, r config.Rollup) (calls int64, seconds float64, ok bool, err error) {
	var names []string
	for _, e := range edges(r) {
		names = append(names, "sqlflow_rollup_"+e.Table)
	}
	var c *int64
	var s *float64
	if err := conn.QueryRow(ctx, `SELECT sum(calls)::bigint, sum(total_time) / 1000
FROM pg_stat_user_functions WHERE funcname = ANY($1) AND schemaname = current_schema()`, names).Scan(&c, &s); err != nil {
		return 0, 0, false, fmt.Errorf("rollup trigger cost: %w", err)
	}
	if c == nil || s == nil {
		return 0, 0, false, nil
	}
	return *c, *s, true, nil
}
