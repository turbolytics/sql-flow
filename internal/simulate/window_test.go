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
// notation and for rows that carry their own time. The asserted column is
// the engine's watermark after the step's commit: the newest event time it
// has placed, less the one-minute grace.
//
//	step         event time   window table                  asserted   closed
//	----------------------------------------------------------------------------
//	Produce 4    12:00:01     12:00: 4                      11:59:01   -
//	Elapse 1m
//	Produce 6    12:01:02     12:00: 4, 12:01: 6            12:00:02   -
//	Elapse 1m
//	Produce 5    12:02:03     12:00: 4, 12:01: 6, 12:02: 5  12:01:03   -
//	Poll                      12:01: 6, 12:02: 5            12:01:03   12:01:03
//	               ^ 12:00 ends at 12:01, at or before the assertion: it
//	                 publishes. 12:01 ends at 12:02 and stays open.
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
	assert.Equal(t, int64(4), r.Published)
	assert.Equal(t, int64(11), r.StillOpen)
	assert.Equal(t, 0, r.Republished)
}

// The idle close needs the loop to assert it: time passing moves nothing
// until a commit finds the partition silent for the bound, and only then
// does the bucket close. The manager has no clock of its own to grow
// impatient on.
//
//	step         engine clock  window table   asserted   why
//	--------------------------------------------------------------------------
//	Produce 7    12:00:01      12:00: 7       11:59:01   grace: 12:00:01 - 1m
//	Poll         12:00:02      12:00: 7       11:59:01   held: 12:00 ends 12:01
//	Elapse 30s   12:00:33      12:00: 7       11:59:01   time passed; no commit,
//	                                                     so the row did not move
//	IdleTick     12:00:34      12:00: 7       12:01:01   the partition has been
//	                                                     silent 33s > 10s: idle.
//	                                                     Nothing in the minimum:
//	                                                     newest 12:00:01 + 1m
//	Poll         12:00:35      empty          12:01:01   12:00 ends 12:01: closed
func TestSimulate_AnIdleCloseNeedsTheLoopToAssertIt(t *testing.T) {
	coverage.Covers(t, "manager.window")
	r := RunWindowed(t, []int32{0}, windowDecl(), []Step{
		Produce{Partition: 0, Rows: 7},
		// A poll before any tick closes nothing: the assertion is a minute
		// behind the only bucket, and nothing has said the stream stopped.
		Poll{},
		Elapse{By: 30 * time.Second},
		IdleTick{},
		Poll{},
	})

	assert.Equal(t, 7, r.Produced)
	assert.Equal(t, int64(7), r.Published)
	assert.Equal(t, int64(0), r.StillOpen)
}

// A lost partition holds the window. The session failed and the partition
// may come back with a backlog for the buckets this process holds, so the
// engine keeps it in the minimum at its last position, never idle. Five
// minutes of wall time, and nothing closes.
//
//	step          source        window table   asserted   why
//	--------------------------------------------------------------------------
//	Produce 9     holds p0      12:00: 9       11:59:01   grace
//	Lose p0       p0 lost       12:00: 9       11:59:01   holds at 12:00:01 - 1m
//	Elapse 5m
//	IdleTick      p0 lost       12:00: 9       11:59:01   lost is not idle
//	Poll          p0 lost       12:00: 9       11:59:01   held
//	IdleTick      p0 lost       12:00: 9       11:59:01   still
//	Poll          p0 lost       12:00: 9       11:59:01   held
func TestSimulate_ALostPartitionHoldsTheWindow(t *testing.T) {
	coverage.Covers(t, "manager.window")
	r := RunWindowed(t, []int32{0}, windowDecl(), []Step{
		Produce{Partition: 0, Rows: 9},
		Lose{Partition: 0},
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

// A partition that is back but has not been back long is the case the bound
// exists for: idleness is measured from the assignment, so five minutes of
// outage count for nothing and five seconds of silence since the return is
// not ten.
//
//	step          source     silent since   idle?          asserted
//	--------------------------------------------------------------------
//	Produce 9     holds p0   12:00:01        -              11:59:01
//	Lose p0       lost       -               lost: never    11:59:01
//	Elapse 5m
//	IdleTick      lost       -               no             11:59:01
//	Poll          lost                                      held
//	Assign p0     holds p0   12:05:05        0s: no         11:59:01
//	Elapse 3s
//	IdleTick      holds p0   12:05:05        5s < 10s: no   11:59:01
//	Poll                                                    held
func TestSimulate_TheAssignmentBoundsTheIdleness(t *testing.T) {
	coverage.Covers(t, "manager.window")
	r := RunWindowed(t, []int32{0}, windowDecl(), []Step{
		Produce{Partition: 0, Rows: 9},
		Lose{Partition: 0},
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

// And the same silence closes once the partition has been back longer than
// the bound, because by then it has had the chance to deliver through it.
//
//	step          source     silent since   idle?           asserted
//	----------------------------------------------------------------------
//	Produce 9     holds p0   12:00:01        -               11:59:01
//	Lose p0       lost                       lost: never     11:59:01
//	Elapse 5m, IdleTick, Poll                                held
//	Assign p0     holds p0   12:05:05        0s              11:59:01
//	Elapse 1m
//	IdleTick      holds p0   12:05:05        1m > 10s: yes   12:01:01  = 12:00:01 + 1m
//	Poll                                                     12:00 closes
func TestSimulate_TheIdleCloseResumesWhenThePartitionIsBack(t *testing.T) {
	coverage.Covers(t, "manager.window")
	r := RunWindowed(t, []int32{0}, windowDecl(), []Step{
		Produce{Partition: 0, Rows: 9},
		Lose{Partition: 0},
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

// A revoked partition is another worker's now, and it leaves the minimum:
// what it contributed to this worker's table closes by the remaining
// partition's progress.
//
// Holding for it instead -- which is what "revoked is not idle" would mean --
// pins the minimum at the partition's last position for the life of the
// process: it never delivers here again and never goes idle, so no window on
// this worker ever closes again and its table grows without bound. See
// core.Watermarks for the whole argument, and Lose for the case where holding
// is right.
//
//	step            part  event time   window table        asserted        why
//	------------------------------------------------------------------------------
//	Produce 3       p0    12:00:30     12:00: 3            -               p1 at -inf holds
//	Produce 3       p1    12:00:30     12:00: 6            11:59:30        min(p0, p1) - 1m
//	Revoke p1                          12:00: 6            11:59:30        p1 gone
//	Produce 4       p0    12:05:00     12:00: 6, 12:05: 4  12:04           p0 alone
//	Poll                               12:05: 4            closed 12:04    12:00 publishes 6
//	Produce 5       p1    12:00:45     (not ours)                          another worker's rows
func TestSimulate_ARevokedPartitionLeavesTheMinimum(t *testing.T) {
	coverage.Covers(t, "manager.window")
	r := RunWindowed(t, []int32{0, 1}, windowDecl(), []Step{
		Produce{Partition: 0, Rows: 3, At: base.Add(30 * time.Second)},
		Produce{Partition: 1, Rows: 3, At: base.Add(30 * time.Second)},
		Revoke{Partition: 1},
		Produce{Partition: 0, Rows: 4, At: base.Add(5 * time.Minute)},
		Poll{},
		// Produced against the other worker, not this one: nothing arrives
		// here, and it is not a loss.
		Produce{Partition: 1, Rows: 5, At: base.Add(45 * time.Second)},
	})

	assert.Equal(t, 10, r.Produced)
	assert.Equal(t, int64(6), r.Published)
	assert.Equal(t, int64(4), r.StillOpen)
	assert.Equal(t, int64(0), r.LateDropped)
}

// The rows a revoked partition left behind still close, even after every
// remaining partition has gone quiet. This is the liveness half of the rule
// above: leaving the minimum is what lets the window close, and the all-idle
// close is measured over everything this process ever placed -- not only what
// it still holds -- so the revoked partition's rows have a closer even when
// nothing is left to carry the watermark.
//
// Without that, a scale-out would strand those rows in this worker's table
// for the life of the process, which is unbounded growth rather than a late
// close.
//
//	step            part  event time   window table        asserted   why
//	--------------------------------------------------------------------------
//	Produce 3       p0    12:00:30     12:00: 3            -          p1 at -inf
//	Produce 4       p1    12:02:00     12:00: 3, 12:02: 4  11:59:30   min is p0
//	Revoke p1                          (same)              11:59:30   p1 gone
//	Elapse 30s
//	IdleTick              -            (same)              12:03      p0 idle too, so
//	                                                                  nothing is in the
//	                                                                  minimum: newest
//	                                                                  12:02 + 1m, and the
//	                                                                  newest is p1's
//	Poll                               empty               12:03      both buckets close
func TestSimulate_ARevokedPartitionsRowsStillCloseOnIdleness(t *testing.T) {
	coverage.Covers(t, "manager.window")
	r := RunWindowed(t, []int32{0, 1}, windowDecl(), []Step{
		Produce{Partition: 0, Rows: 3, At: base.Add(30 * time.Second)},
		Produce{Partition: 1, Rows: 4, At: base.Add(2 * time.Minute)},
		Revoke{Partition: 1},
		Elapse{By: 30 * time.Second},
		IdleTick{},
		Poll{},
	})

	assert.Equal(t, 7, r.Produced)
	assert.Equal(t, int64(7), r.Published)
	assert.Equal(t, int64(0), r.StillOpen)
	assert.Equal(t, int64(0), r.LateDropped)
}

// And a worker that loses its whole assignment closes what it holds. Nothing
// more will ever arrive for those buckets here, so holding them would strand
// them: a worker scaled down to nothing publishes its table and stops.
func TestSimulate_AWorkerHoldingNothingClosesWhatItHas(t *testing.T) {
	coverage.Covers(t, "manager.window")
	r := RunWindowed(t, []int32{0, 1}, windowDecl(), []Step{
		Produce{Partition: 0, Rows: 3, At: base.Add(30 * time.Second)},
		Produce{Partition: 1, Rows: 4, At: base.Add(2 * time.Minute)},
		Revoke{Partition: 0},
		Revoke{Partition: 1},
		Elapse{By: 30 * time.Second},
		IdleTick{},
		Poll{},
	})

	assert.Equal(t, 7, r.Produced)
	assert.Equal(t, int64(7), r.Published)
	assert.Equal(t, int64(0), r.StillOpen)
}

// A restart loses nothing the table and the row hold, and the idle close
// still finds the newest bucket: the new process has seen no rows, so it
// measures the close from what its table holds instead.
//
//	step          window table   asserted   why
//	--------------------------------------------------------------------
//	Produce 7     12:00: 7       11:59:01   grace
//	Restart       12:00: 7       11:59:01   a new tracker: nothing seen, the
//	                                        table's newest bucket restored
//	Elapse 30s
//	IdleTick      12:00: 7       12:01      idle with nothing seen: the
//	                                        newest bucket's end
//	Poll          empty          12:01      12:00 closes
func TestSimulate_ARestartStillClosesByIdleness(t *testing.T) {
	coverage.Covers(t, "manager.window")
	r := RunWindowed(t, []int32{0}, windowDecl(), []Step{
		Produce{Partition: 0, Rows: 7},
		Restart{},
		Elapse{By: 30 * time.Second},
		IdleTick{},
		Poll{},
	})

	assert.Equal(t, 7, r.Produced)
	assert.Equal(t, int64(7), r.Published)
	assert.Equal(t, int64(0), r.StillOpen)
}
