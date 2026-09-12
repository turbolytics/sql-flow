package managers

// The tumbling window manager under the conformance harness.
//
// The subject supplies three things: build the manager on the sink the
// harness hands it, put closed windows in the state table, and count what
// is left. The harness owns the sink, the faults and the verdicts, so a
// second manager kind supplies the same three things and inherits every
// manager invariant.

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/turbolytics/sql-flow/internal/conformance"
	"github.com/turbolytics/sql-flow/internal/core"
	"github.com/turbolytics/sql-flow/internal/coverage"
)

func TestManagerTumblingWindow_Conformance(t *testing.T) {
	coverage.Covers(t, "manager.tumbling_window")

	conn, cleanup := newTestConn(t)
	defer cleanup()
	exec(t, conn, `CREATE TABLE agg_cities_count (bucket TIMESTAMPTZ, city VARCHAR, count INT);`)

	conformance.Managers(t, conformance.ManagerSubject{
		Integration: "manager.tumbling_window",

		New: func(t *testing.T, sink core.Sink, poll time.Duration) conformance.Manager {
			return NewTumbling(conn, collectSQL, deleteSQL, poll, sink, &sync.Mutex{})
		},

		// Every seeded row is ten minutes old, so the close predicate the
		// package's other tests use selects all of them.
		Seed: func(t *testing.T, n int) {
			exec(t, conn, `DELETE FROM agg_cities_count`)
			for i := 0; i < n; i++ {
				exec(t, conn, fmt.Sprintf(
					`INSERT INTO agg_cities_count VALUES
						(now()::timestamptz - INTERVAL '600' SECOND, 'city-%d', %d)`,
					i, i+1))
			}
		},

		Remaining: func(t *testing.T) int64 {
			return countRows(t, conn, "agg_cities_count")
		},
	})
}
