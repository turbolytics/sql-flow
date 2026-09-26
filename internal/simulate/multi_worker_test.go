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
//	                     worker A (p0)          worker B (p1)
//	                     own DuckDB             own DuckDB
//	                     own window table       own window table
//	                     own watermark          own watermark
//	                          |                      |
//	   bucket 12:00, key K    | 100 rows             | 100 rows
//	                          v                      v
//	                     publishes 100          publishes 100
//	                              \                /
//	                               v              v
//	                            the destination sees two writes
//	                            for the same (bucket, key)
//
//	Keyed on (bucket, key) and upserting: the last writer wins and 100 rows
//	are lost. That is the 12% loss reproduced under #183.
//
//	The per-partition watermark does not touch this. It fixes the ordering
//	half -- a partition just assigned holds the minimum until it delivers, so
//	the survivor cannot close buckets its new partitions still have data for
//	-- and leaves the splitting half exactly where it was.
//
//	Two ways out, and the engine has to declare which:
//	  co-partition        the window key is the message key, so a key never
//	                      spans partitions and is never split
//	  merge per process   each writes (bucket, key, process) and the
//	                      destination sums across processes, which is what
//	                      pipeline.writers.merge_exactly already proves for
//	                      the Render template
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
// offsets move past the rows that produced it, so nobody publishes them.
//
//	worker A                                      the group
//	--------------------------------------------------------------------
//	reads p0 offsets 100..199                     committed: 99
//	writes them into its window table   ---+
//	commits offsets and state together     |      committed: 199
//	                                       |      (atomic, and correct)
//	*** dies before the bucket closes ***  |
//	                                       v
//	                            those rows exist only in A's state file
//
//	worker B takes p0                             committed: 199
//	resumes from 199, so 100..199 are never re-read.
//	Nobody ever publishes them.
//
//	Offsets and state commit atomically per process, but recovery happens
//	per group. Flink makes those one unit: a checkpoint advances both and a
//	failure rewinds both. Our rewind unit is the group's committed offset;
//	our state unit is one machine's disk. A per-process key at the
//	destination does not help, because the partial was never written at all.
func TestSimulate_AKilledWorkerStrandsNoState(t *testing.T) {
	coverage.Covers(t, "core.consume_loop")
	t.Skip("tracked by #183: a dead worker's committed-but-unpublished rows are stranded")
}
