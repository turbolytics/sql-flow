package core

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/turbolytics/sql-flow/internal/duckdb"
	"github.com/zeebo/assert"
)

// The engine decides a record's lateness from its bucket, and the handler's
// SQL computes time_column with time_bucket. If the two disagree, lateness
// is decided against the wrong bucket. This proves BucketStart is
// time_bucket, over every size a window is likely to declare and the ones
// where a naive truncation is wrong: 25 hours and 3 days do not divide the
// span from Go's zero time to DuckDB's origin, and time.Truncate gets them
// wrong by hours.
func TestWindowBucket_AgreesWithDuckDBTimeBucket(t *testing.T) {
	coverage.Covers(t, "manager.window")
	ctx := context.Background()
	db, err := duckdb.OpenPath(ctx, "")
	assert.NoError(t, err)
	defer db.Close()
	conn, err := db.Connect(ctx)
	assert.NoError(t, err)
	defer conn.Close()

	instants := []time.Time{
		time.Date(2026, 9, 26, 12, 34, 56, 789000000, time.UTC),
		time.Date(2026, 9, 26, 0, 0, 0, 0, time.UTC),
		time.Date(2026, 9, 26, 23, 59, 59, 999999000, time.UTC),
		time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		EventTimeFloor,
		time.Date(2026, 9, 26, 12, 35, 0, 0, time.UTC), // exactly on a minute
		time.Date(2026, 9, 26, 12, 34, 59, 999999000, time.UTC),
	}
	sizes := []int{1, 7, 60, 90, 300, 420, 3600, 5400, 7200, 86400, 90000, 172800, 259200, 604800}
	for _, size := range sizes {
		for _, at := range instants {
			q := fmt.Sprintf(`SELECT epoch_us(time_bucket(INTERVAL '%d seconds', TIMESTAMPTZ '%s'))`,
				size, at.Format("2006-01-02 15:04:05.999999-07:00"))
			stmt, err := conn.NewStatement()
			assert.NoError(t, err)
			assert.NoError(t, stmt.SetSqlQuery(q))
			rdr, _, err := stmt.ExecuteQuery(ctx)
			assert.NoError(t, err)
			var micros int64
			for rdr.Next() {
				micros = rdr.Record().Column(0).(*array.Int64).Value(0)
			}
			rdr.Release()
			stmt.Close()

			want := time.UnixMicro(micros).UTC()
			d := time.Duration(size) * time.Second
			if got := BucketStart(at, d); !got.Equal(want) {
				t.Fatalf("size %ds at %s: BucketStart %s, time_bucket %s", size, at, got, want)
			}
			assert.Equal(t, want.Add(d), BucketEnd(at, d))
		}
	}
}

// And the case the origin exists for, pinned on its own so the grid above
// cannot quietly lose it: a naive truncation from Go's zero time is wrong
// for a 25-hour bucket.
func TestWindowBucket_ANaiveTruncationWouldBeWrong(t *testing.T) {
	coverage.Covers(t, "manager.window")
	at := time.Date(2026, 9, 26, 12, 34, 56, 0, time.UTC)
	size := 25 * time.Hour
	naive := at.Truncate(size)
	got := BucketStart(at, size)
	assert.That(t, !got.Equal(naive))
	assert.Equal(t, time.Date(2026, 9, 25, 12, 0, 0, 0, time.UTC), got) // what DuckDB says
}
