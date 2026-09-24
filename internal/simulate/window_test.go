package simulate

import (
	"testing"
	"time"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/turbolytics/sql-flow/internal/managers"
	"github.com/zeebo/assert"
)

func windowDecl() managers.Declaration {
	return managers.Declaration{
		Table:      windowTable,
		TimeColumn: "bucket",
		Size:       time.Minute,
		Grace:      time.Minute,
		IdleClose:  10 * time.Second,
		Late:       managers.LateDrop,
	}
}

// The stream moving past a bucket by the grace closes it, and every row the
// loop wrote is either published or still open. Nothing evaporates between
// the two halves.
func TestSimulate_AWindowedRunAccountsForEveryRow(t *testing.T) {
	coverage.Covers(t, "manager.window")
	r := RunWindowed(t, []int32{0}, windowDecl(), []Step{
		Produce{Partition: 0, Rows: 4},
		Elapse{By: time.Minute},
		Produce{Partition: 0, Rows: 6},
		Elapse{By: time.Minute},
		Produce{Partition: 0, Rows: 5},
		Poll{},
	})

	assert.Equal(t, 15, r.Produced)
	assert.That(t, r.Published > 0)
	assert.Equal(t, int64(15), r.Published+r.StillOpen)
	assert.Equal(t, 0, r.Republished)
}

// The idle close needs the engine to confirm the quiet: the loop's idle tick
// writes the row the manager reads, and only then does the bucket close.
func TestSimulate_AnIdleCloseNeedsTheLoopToConfirmTheQuiet(t *testing.T) {
	coverage.Covers(t, "manager.window")
	r := RunWindowed(t, []int32{0}, windowDecl(), []Step{
		Produce{Partition: 0, Rows: 7},
		// A poll before any quiet is confirmed closes nothing: one bucket,
		// no grace passed, no idle bound reached.
		Poll{},
		Elapse{By: 30 * time.Second},
		IdleTick{},
		Poll{},
	})

	assert.Equal(t, 7, r.Produced)
	assert.Equal(t, int64(7), r.Published)
	assert.Equal(t, int64(0), r.StillOpen)
}

// The interaction this PR is about, end to end: the loop tells the row the
// source holds nothing, and the manager refuses to read that silence as a
// quiet stream. Before the row carried it, this bucket closed early and the
// backlog after the reassignment was late.
func TestSimulate_ASourceThatCannotDeliverStopsTheIdleClose(t *testing.T) {
	coverage.Covers(t, "manager.window")
	r := RunWindowed(t, []int32{0}, windowDecl(), []Step{
		Produce{Partition: 0, Rows: 9},
		Revoke{Partition: 0},
		// Far past the idle bound, with the loop committing all the while.
		Elapse{By: 5 * time.Minute},
		IdleTick{},
		Poll{},
		IdleTick{},
		Poll{},
	})

	assert.Equal(t, 9, r.Produced)
	assert.Equal(t, int64(0), r.Published)
	assert.Equal(t, int64(9), r.StillOpen)
}

// And the same silence closes once the partition is back, because the row
// then says the source could have delivered through it.
func TestSimulate_TheIdleCloseResumesWhenTheSourceIsBack(t *testing.T) {
	coverage.Covers(t, "manager.window")
	r := RunWindowed(t, []int32{0}, windowDecl(), []Step{
		Produce{Partition: 0, Rows: 9},
		Revoke{Partition: 0},
		Elapse{By: 5 * time.Minute},
		IdleTick{},
		Poll{},
		Assign{Partition: 0},
		Elapse{By: time.Minute},
		IdleTick{},
		Poll{},
	})

	assert.Equal(t, 9, r.Produced)
	assert.Equal(t, int64(9), r.Published)
	assert.Equal(t, int64(0), r.StillOpen)
}
