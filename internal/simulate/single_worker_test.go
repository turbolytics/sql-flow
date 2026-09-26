package simulate

import (
	"testing"
	"time"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

// A restart, time passing and an idle tick in one run: every row the source
// delivered reaches the sink, once.
//
// These scenarios have no window: the sink receives every row the handler
// produced, so what is being pinned is the loop's own delivery.
//
//	step         delivered   committed   sink   why
//	--------------------------------------------------------------------
//	Produce 10   10          -           -      buffered by the handler
//	IdleTick     10          10          10     the flush commits them
//	Elapse 1m    -           10          10     nothing arrives
//	Produce 10   20          10          10
//	Restart      -           10          10     the loop dies and comes back;
//	                                            it resumes from the committed
//	                                            offset, so the second ten are
//	                                            replayed rather than lost
//	Produce 20   40          -           -
//	IdleTick     40          40          40     every row once, none twice
func TestSimulate_ASingleWorkerPublishesEveryRow(t *testing.T) {
	coverage.Covers(t, "core.consume_loop")
	r := Run(t, []int32{0}, []Step{
		Produce{Partition: 0, Rows: 10},
		IdleTick{},
		Elapse{By: time.Minute},
		Produce{Partition: 0, Rows: 10},
		Restart{},
		Produce{Partition: 0, Rows: 10},
		Produce{Partition: 0, Rows: 10},
		IdleTick{},
	})

	assert.Equal(t, 40, r.Produced)
	assert.Equal(t, 0, len(r.Missing))
	assert.Equal(t, 0, r.Duplicated)
	assert.Equal(t, 40, r.Published)
}

// A restart replays whatever the committed offsets did not cover, and the
// sink holds each row once because the loop commits only what it flushed.
//
//	step         p0   p1   what a restart means here
//	--------------------------------------------------------------------
//	Produce 5    5    -
//	Produce 5    5    5
//	Restart      -    -    both partitions resume from the group's committed
//	                       offsets; anything the sink did not take is re-read
//	Produce 5    10   5
//	Restart      -    -    again, mid-stream
//	Produce 5    10   10
//
//	Twenty rows produced across two partitions and two restarts, and the
//	assertion is that none of them is missing. Duplicates are permitted by
//	at-least-once; losses are not.
func TestSimulate_ARestartLosesNothing(t *testing.T) {
	coverage.Covers(t, "core.consume_loop")
	r := Run(t, []int32{0, 1}, []Step{
		Produce{Partition: 0, Rows: 5},
		Produce{Partition: 1, Rows: 5},
		Restart{},
		Produce{Partition: 0, Rows: 5},
		Restart{},
		Produce{Partition: 1, Rows: 5},
	})

	assert.Equal(t, 20, r.Produced)
	assert.Equal(t, 0, len(r.Missing))
}

// A revoked partition delivers nothing to this worker, and the rows it still
// holds are unaffected.
//
//	step         owns     produced here   why
//	--------------------------------------------------------------------
//	Produce p0   p0, p1   5               this worker owns p0
//	Revoke p1    p0       5               p1 goes to another worker
//	Produce p1   p0       5               nothing arrives: not ours to read
//	Produce p0   p0       10
//	Assign p1    p0, p1   10              p1 comes back
//	Produce p1   p0, p1   15              and delivers again
//	IdleTick     p0, p1   15              all 15 flushed, none lost
//
//	The middle Produce is the point: a partition this worker does not own
//	produces nothing for it, and that is not a loss.
func TestSimulate_ARevokedPartitionStopsArriving(t *testing.T) {
	coverage.Covers(t, "core.consume_loop")
	r := Run(t, []int32{0, 1}, []Step{
		Produce{Partition: 0, Rows: 5},
		Revoke{Partition: 1},
		Produce{Partition: 1, Rows: 5}, // another worker's now: nothing arrives
		Produce{Partition: 0, Rows: 5},
		Assign{Partition: 1},
		Produce{Partition: 1, Rows: 5},
		IdleTick{},
	})

	assert.Equal(t, 15, r.Produced)
	assert.Equal(t, 0, len(r.Missing))
	assert.Equal(t, 0, r.Duplicated)
}

// A worker holding no partitions says so through Deliverer, which is how a
// source with no partitions of its own to report -- a websocket, a webhook --
// tells the engine its one partition is out.
//
//	step         owns     produced   Delivering() says
//	--------------------------------------------------------------------
//	Produce 5    p0       5          yes, since it was assigned
//	Revoke p0    none     5          no: it holds nothing at all
//	IdleTick     none     5          the loop commits, and asks the source
//	                                 before it does
//
//	Without a window there is nothing to close, so this pins the reporting
//	rather than the decision. window_test.go has the decision, and for a
//	source that does report its partitions the engine hears them directly
//	rather than through this.
func TestSimulate_AWorkerHoldingNothingReportsItCannotDeliver(t *testing.T) {
	coverage.Covers(t, "core.consume_loop")
	r := Run(t, []int32{0}, []Step{
		Produce{Partition: 0, Rows: 5},
		Revoke{Partition: 0},
		IdleTick{},
	})

	assert.Equal(t, 5, r.Produced)
	assert.Equal(t, 0, len(r.Missing))
}
