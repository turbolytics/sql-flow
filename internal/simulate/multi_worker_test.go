package simulate

import (
	"testing"

	"github.com/turbolytics/sql-flow/internal/coverage"
)

// Two workers over two partitions, which is where a bucket's rows split and
// both publish a partial count for the same key. The scripts are written; the
// runner drives one worker, so turning these on means teaching it a second
// and giving each its own window state.
//
// Skipped rather than deleted: the ledger carries
// pipeline.window.counts_every_row as declared-unenforced against #183, and
// these are the scenarios that would enforce it.
func TestSimulate_ScaleOutKeepsEveryRow(t *testing.T) {
	coverage.Covers(t, "core.consume_loop")
	t.Skip("tracked by #183: a bucket split across workers publishes one worker's share")

	// The script this would run:
	//   Produce{Partition: 0, Rows: 100}, Produce{Partition: 1, Rows: 100},
	//   StartWorker{Owns: []int32{1}},   // revokes partition 1 from worker A
	//   Produce{Partition: 0, Rows: 100}, Produce{Partition: 1, Rows: 100},
	//   IdleTick{}, Drain{},
	// and assert every row reaches the sink once, across both workers.
}

// A worker killed mid-batch leaves its window state behind while the group's
// offsets move past the rows that produced it, so nobody publishes them. The
// fix is partition-keyed state or additive publication, neither of which
// exists yet.
func TestSimulate_AKilledWorkerStrandsNoState(t *testing.T) {
	coverage.Covers(t, "core.consume_loop")
	t.Skip("tracked by #183: a dead worker's committed-but-unpublished rows are stranded")
}
