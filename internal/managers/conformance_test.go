package managers

// The watermark manager under the conformance harness.
//
// The subject supplies what the harness drives: build the manager on the
// sink the harness hands it, put closed buckets in the table, count what is
// left, hold rows open in the pipeline's transaction, run a batch through the
// structured handler on the pipeline's connection, and build a manager whose
// commit waits. The harness owns the sink, the faults and the verdicts.
//
// The engine's assertion is written by hand where a check changes the
// table: Seed asserts exactly its buckets' end. Everything closes on that
// and on nothing else; the manager takes no clock and no signal here,
// because every check drives Pass itself.

import (
	"context"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/turbolytics/sql-flow/internal/conformance"
	"github.com/turbolytics/sql-flow/internal/core"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/turbolytics/sql-flow/internal/handlers"
)

// heldConn is a manager's connection whose commit calls hold first, so a
// test can stop a pass with its writes made and not committed.
type heldConn struct {
	adbc.Connection
	hold func()
}

func (c heldConn) Commit(ctx context.Context) error {
	c.hold()
	return c.Connection.(transaction).Commit(ctx)
}

func (c heldConn) Rollback(ctx context.Context) error {
	return c.Connection.(transaction).Rollback(ctx)
}

func TestManagerWatermark_Conformance(t *testing.T) {
	coverage.Covers(t, "manager.window")

	d := newTestDB(t, "")
	createWindowTable(t, d.pipeline)

	// The handler the bluesky demo runs, on the pipeline's connection. It
	// checkpoints each time it re-initialises, which is the statement a
	// window's uncommitted write refused.
	exec(t, d.pipeline, `CREATE TABLE posts (id BIGINT, lang VARCHAR)`)
	handler, err := handlers.NewStructuredBatchHandler(d.pipeline,
		`SELECT lang, count(*) AS n FROM posts GROUP BY lang`, "posts",
		arrow.NewSchema([]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "lang", Type: arrow.BinaryTypes.String},
		}, nil))
	if err != nil {
		t.Fatal(err)
	}
	if err := handler.Init(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Each New builds on a fresh connection, so a manager whose connection
	// holds an open transaction from a failed pass does not block the next.
	subject := conformance.ManagerSubject{
		Integration: "manager.watermark",

		New: func(t *testing.T, sink core.Sink, budget *core.DrainBudget) conformance.Manager {
			conn := managerConn(t, d.db)
			t.Cleanup(func() { conn.Close() })
			w, err := NewWatermark(conn, testDecl(), sink, nil, WithDrainBudget(budget))
			if err != nil {
				t.Fatal(err)
			}
			return w
		},

		// n buckets, one row each, the watermark forgotten so every check
		// starts from a table that has never closed anything, and the engine
		// having asserted the last seeded bucket's end.
		Seed: func(t *testing.T, n int) {
			exec(t, d.pipeline, `DELETE FROM agg_cities_count`)
			exec(t, d.pipeline, `DELETE FROM sqlflow_windows`)
			for i := 0; i < n; i++ {
				insertBucket(t, d.pipeline, i, "city", i+1)
			}
			assertAt(t, d.pipeline, bucket(n))
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

		// One batch the way the consume loop runs it: write, invoke,
		// release, re-initialise.
		Batch: func(t *testing.T) error {
			ctx := context.Background()
			for i := 0; i < 100; i++ {
				if err := handler.Write([]byte(`{"id":1,"lang":"en"}`)); err != nil {
					return err
				}
			}
			table, err := handler.Invoke(ctx)
			if err != nil {
				return err
			}
			if table != nil {
				table.Release()
			}
			return handler.Init(ctx)
		},

		HoldCommit: func(t *testing.T, sink core.Sink, budget *core.DrainBudget, hold func()) conformance.Manager {
			conn := managerConn(t, d.db)
			t.Cleanup(func() { conn.Close() })
			w, err := NewWatermark(heldConn{Connection: conn, hold: hold}, testDecl(), sink, nil,
				WithDrainBudget(budget))
			if err != nil {
				t.Fatal(err)
			}
			return w
		},

		// A row for the first seeded bucket, which a manager has closed: what
		// a window with lateness retains, written by hand.
		SeedLate: func(t *testing.T, n int) {
			for i := 0; i < n; i++ {
				insertBucket(t, d.pipeline, 0, "late", 1)
			}
		},

		// A bucket newer than every seeded one, and the engine asserted past
		// its end, so the next pass publishes it, deletes it, and updates the
		// window's row.
		SeedNewer: func(t *testing.T) {
			insertBucket(t, d.pipeline, seededBuckets+1, "newer", 1)
			assertAt(t, d.pipeline, bucket(seededBuckets+2))
		},
	}
	conformance.Managers(t, subject)
}

// seededBuckets is what the harness seeds, so SeedNewer lands past it.
const seededBuckets = 2
