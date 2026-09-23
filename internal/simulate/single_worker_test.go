package simulate

import (
	"testing"
	"time"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

// A restart, a clock step in each direction and an idle tick in one run: every
// row the source delivered reaches the sink, once.
func TestSimulate_ASingleWorkerPublishesEveryRow(t *testing.T) {
	coverage.Covers(t, "core.consume_loop")
	r := Run(t, []int32{0}, []Step{
		Produce{Partition: 0, Rows: 10},
		IdleTick{},
		StepClock{By: time.Minute},
		Produce{Partition: 0, Rows: 10},
		Restart{},
		Produce{Partition: 0, Rows: 10},
		StepClock{By: -2 * time.Minute},
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

// A worker holding no partitions says so through Deliverer, which is what
// stops a window closing on a silence the source could not have filled.
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
