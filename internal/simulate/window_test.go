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
//
// Event time comes from the engine clock here, because the script sets no
// At: Elapse is what moves the stream on. See event_time_test.go for the
// notation and for rows that carry their own time.
//
//	step         bucket   window table                  watermark
//	--------------------------------------------------------------------
//	Produce 4    12:00    12:00: 4                      -
//	Elapse 1m    -        (same)                        -
//	Produce 6    12:01    12:00: 4, 12:01: 6            -
//	Elapse 1m    -        (same)                        -
//	Produce 5    12:02    12:00: 4, 12:01: 6, 12:02: 5  -
//	Poll         -        12:02: 5                      12:01
//	               ^ grace close: newest 12:02 - 1m = 12:01, so 12:00 (ending
//	                 12:01) publishes. 12:01 ends at 12:02 and stays open.
//	                 Published + still open = 15, the whole run.
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
//
//	step         window table   watermark   why
//	--------------------------------------------------------------------
//	Produce 7    12:00: 7       -           one bucket, nothing past it
//	Poll         12:00: 7       -           held: no grace passed, and the
//	                                        engine has confirmed no quiet
//	Elapse 30s   12:00: 7       -           time passes, but the row does
//	                                        not move on its own
//	IdleTick     12:00: 7       -           now the loop commits, and the
//	                                        row says 30s of silence > 10s
//	Poll         empty          12:01       idle close: newest 12:00 + 1m
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

// The loop holds the quiet clock while the source can deliver nothing, so
// the manager never reads that silence as a quiet stream. Without the hold
// this bucket closes early and the backlog after the reassignment is late.
//
//	step          source      window table   quiet the row shows   watermark
//	-----------------------------------------------------------------------
//	Produce 9     owns p0     12:00: 9       ~0                    -
//	Revoke p0     owns none   12:00: 9       ~0                    -
//	Elapse 5m     owns none   12:00: 9       (still ~0: the loop   -
//	                                          resets the clock on
//	                                          every idle commit
//	                                          while it holds
//	                                          nothing)
//	IdleTick      owns none   12:00: 9       ~0                    -
//	Poll          owns none   12:00: 9       ~0 < 10s: held        -
//	IdleTick      owns none   12:00: 9       ~0                    -
//	Poll          owns none   12:00: 9       ~0 < 10s: held        -
//
//	Five minutes of wall time, and not one second of it counts as the
//	stream being quiet, because the stream was never asked.
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

// A source that is back but has not been back long is the case the bound
// exists for, and the only scenario where the bound is what decides. The
// others elapse a minute after the reassignment, so the quiet the row shows
// and the quiet the source could have filled are both past the idle close
// and either one would close the bucket. Here they disagree.
//
//	step          source     silence so far   what the engine may confirm
//	--------------------------------------------------------------------
//	Produce 9     owns p0    -                -
//	Revoke p0     owns none  -                -
//	Elapse 5m     owns none  5m of wall time  none of it: it held nothing
//	IdleTick      owns none  5m               none
//	Poll          owns none  5m               held
//	Assign p0     owns p0    5m               none yet: back for 0s
//	Elapse 3s     owns p0    5m               3s
//	IdleTick      owns p0    5m               ~5s, bounded by the resumption
//	Poll          owns p0    5m               5s < 10s: still held
//
//	The row would say five minutes. The source has been back five seconds.
//	Five seconds is the honest number, and it is not enough to close.
func TestSimulate_TheResumptionBoundsTheQuiet(t *testing.T) {
	coverage.Covers(t, "manager.window")
	r := RunWindowed(t, []int32{0}, windowDecl(), []Step{
		Produce{Partition: 0, Rows: 9},
		Revoke{Partition: 0},
		Elapse{By: 5 * time.Minute},
		IdleTick{},
		Poll{},
		Assign{Partition: 0},
		// Three seconds, plus the second each step takes: under the ten the
		// declaration closes on.
		Elapse{By: 3 * time.Second},
		IdleTick{},
		Poll{},
	})

	assert.Equal(t, 9, r.Produced)
	assert.Equal(t, int64(0), r.Published)
	assert.Equal(t, int64(9), r.StillOpen)
}

// And the same silence closes once the source has been back longer than the
// bound, because by then it has had the chance to deliver through it.
//
//	step          source     what the engine may confirm   watermark
//	--------------------------------------------------------------------
//	Produce 9     owns p0    -                             -
//	Revoke p0     owns none  none                          -
//	Elapse 5m     owns none  none                          -
//	IdleTick/Poll owns none  none: held                    -
//	Assign p0     owns p0    back for 0s                   -
//	Elapse 1m     owns p0    1m                            -
//	IdleTick      owns p0    1m, bounded by the resumption -
//	Poll          owns p0    1m > 10s: close               12:01
//
//	Same five minutes of silence as the scenario above. What changed is that
//	the source has now had a minute in which it could have delivered and did
//	not, which is what the idle close is entitled to act on.
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
