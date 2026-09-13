package managers

// The watermark manager under the conformance harness.
//
// The subject supplies five things: build the manager on the sink the
// harness hands it, put closed buckets in the table, put late rows in a
// bucket that closed, count what is left, and hold rows open in the
// pipeline's transaction. The harness owns the sink, the faults and the
// verdicts.

import (
	"context"
	"testing"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/turbolytics/sql-flow/internal/conformance"
	"github.com/turbolytics/sql-flow/internal/core"
	"github.com/turbolytics/sql-flow/internal/coverage"
)

func TestManagerWatermark_Conformance(t *testing.T) {
	coverage.Covers(t, "manager.window")

	d := newTestDB(t, "")
	createWindowTable(t, d.pipeline)

	// The idle rule closes everything: the stream went quiet an hour ago.
	now := func() time.Time { return t0.Add(time.Hour) }
	arrivedAt(t, d.pipeline, t0)

	// Each New builds on a fresh connection, so a manager whose connection
	// holds an open transaction from a failed poll does not block the next.
	subject := conformance.ManagerSubject{
		Integration: "manager.watermark",

		New: func(t *testing.T, sink core.Sink, poll time.Duration, budget *core.DrainBudget, late string) conformance.Manager {
			conn := managerConn(t, d.db)
			t.Cleanup(func() { conn.Close() })
			decl := testDecl()
			decl.Late = LatePolicy(late)
			w, err := NewWatermark(conn, decl, poll, sink,
				WithDrainBudget(budget), WithClock(now))
			if err != nil {
				t.Fatal(err)
			}
			return w
		},

		// n buckets, one row each, and the watermark forgotten, so every
		// check starts from a table that has never closed anything.
		Seed: func(t *testing.T, n int) {
			exec(t, d.pipeline, `DELETE FROM agg_cities_count`)
			exec(t, d.pipeline, `DELETE FROM sqlflow_windows`)
			for i := 0; i < n; i++ {
				insertBucket(t, d.pipeline, i, "city", i+1)
			}
		},

		// The first bucket closed with Seed's, so rows for it are late.
		SeedLate: func(t *testing.T, n int) {
			for i := 0; i < n; i++ {
				insertBucket(t, d.pipeline, 0, "late", 1)
			}
		},

		Remaining: func(t *testing.T) int64 {
			return countRows(t, d.pipeline, testTable)
		},

		// A transaction on a third connection, left open until released:
		// rows the pipeline has written and not committed.
		Uncommitted: func(t *testing.T, n int) func() {
			conn := managerConn(t, d.db)
			for i := 0; i < n; i++ {
				insertBucket(t, conn, 0, "uncommitted", 1)
			}
			return func() {
				_ = conn.(interface{ Rollback(context.Context) error }).Rollback(context.Background())
				conn.Close()
			}
		},
	}
	conformance.Managers(t, subject)
}

var _ adbc.Connection
