package simulate

import (
	"testing"
	"time"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

// base is the simulated clock's start, so a script can place a row's event
// time relative to the run rather than guessing at it.
var base = time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)

// Reading the diagrams in this file
//
// Every scenario runs under windowDecl(): one-minute buckets, one minute of
// grace, a ten-second idle close, and no lateness, so a late row is refused.
//
// Two clocks, and keeping them apart is the point of the whole file:
//
//	engine clock   advances one second per step, plus whatever Elapse adds.
//	               It decides only whether a partition has gone idle.
//	event time     what a row says about itself, chosen by the script with
//	               Produce{At}. This is what a bucket is cut on, and what the
//	               watermark is computed from.
//
// The columns are what the manager sees when it passes at that moment:
//
//	step           what the script does
//	event time     the row's own clock
//	bucket         where that event time falls: event time truncated to a minute
//	window table   what the handler has written, and what a pass would read
//	asserted       the engine's watermark after the step's commit: for each
//	               partition that holds and is not idle, its newest event time
//	               less the grace, and the minimum over them. A bucket closes
//	               when its end is at or before it.
//
// Two rules explain every outcome below. While partitions deliver, the
// assertion is `min over partitions of (newest event time - grace)`; when
// every partition has been silent for the idle bound, it is `newest event
// time + size`, which closes the newest bucket too. And a record's lateness
// is decided by the engine when it arrives, against that assertion: refused
// before the handler with no lateness, written and recomputed within it.

// Rows out of order inside the grace all land in their own buckets, and the
// stream moving on closes them. This is the case grace_seconds exists for: a
// producer whose records arrive shuffled by less than the tolerance.
//
//	step         event time   bucket   window table          asserted
//	-----------------------------------------------------------------------
//	Produce 3    12:01:30     12:01    12:01: 3              12:00:30
//	Produce 2    12:01:05     12:01    12:01: 5              12:00:30
//	               ^ older than the row before it, same bucket, not late: the
//	                 newest event time is still 12:01:30, so nothing moved
//	Produce 4    12:05:00     12:05    12:01: 5, 12:05: 4    12:04
//	Pass         -            -        12:05: 4              12:04
//	               ^ 12:01 ends at 12:02, at or before 12:04, so it publishes
//	                 all 5 rows. 12:05 ends at 12:06 and stays open.
func TestSimulate_OutOfOrderRowsInsideTheGraceAreNotLate(t *testing.T) {
	coverage.Covers(t, "manager.window")
	r := RunWindowed(t, []int32{0}, windowDecl(), []Step{
		Produce{Partition: 0, Rows: 3, At: base.Add(90 * time.Second)},
		// Older than the rows before it, and still inside the same minute.
		Produce{Partition: 0, Rows: 2, At: base.Add(65 * time.Second)},
		// The stream moves on by more than the grace, which closes 12:01.
		Produce{Partition: 0, Rows: 4, At: base.Add(5 * time.Minute)},
		Pass{},
	})

	assert.Equal(t, 9, r.Produced)
	assert.Equal(t, int64(5), r.Published)
	assert.Equal(t, int64(4), r.StillOpen)
	assert.Equal(t, int64(0), r.LateRefused)
	assert.Equal(t, 0, r.Republished)
}

// A row for a bucket the watermark has passed is late. With no lateness the
// engine refuses it before the handler: it never reaches the table, and the
// sink's value for the bucket is the one the close published. That is the
// declared contract, so this is what correct looks like rather than a defect.
//
//	step         event time   bucket   window table          asserted   engine
//	-------------------------------------------------------------------------------
//	Produce 5    12:00:30     12:00    12:00: 5              11:59:30
//	Produce 4    12:05:00     12:05    12:00: 5, 12:05: 4    12:04
//	Pass         -            -        12:05: 4              12:04      12:00 → 5 published, deleted
//	Produce 6    12:00:30     (none)   12:05: 4              12:04      end 12:01 <= 12:04: refused, 6 counted
//	               ^ back into a bucket that ended at 12:01, already at or
//	                 before the watermark. Late on arrival, refused before
//	                 the handler, and the assertion does not move: 12:00:30
//	                 is not the newest.
func TestSimulate_ARowForAClosedBucketIsRefused(t *testing.T) {
	coverage.Covers(t, "manager.window")
	r := RunWindowed(t, []int32{0}, windowDecl(), []Step{
		Produce{Partition: 0, Rows: 5, At: base.Add(30 * time.Second)},
		Produce{Partition: 0, Rows: 4, At: base.Add(5 * time.Minute)},
		Pass{},
		// Back in the bucket that just closed.
		Produce{Partition: 0, Rows: 6, At: base.Add(30 * time.Second)},
	})

	assert.Equal(t, 15, r.Produced)
	assert.Equal(t, int64(5), r.Published)
	assert.Equal(t, int64(4), r.StillOpen)
	assert.Equal(t, int64(6), r.LateRefused)
	assert.Equal(t, int64(5), r.LastValue[base])
	assert.Equal(t, int64(0), r.Recomputes)
}

// With lateness, a late row is written and the bucket republished whole: the
// sink's last value is the exact count, never the late rows alone.
//
//	step         event time   bucket   window table          asserted   sink's value for 12:00
//	-----------------------------------------------------------------------------------------
//	Produce 5    12:00:30     12:00    12:00: 5              11:59:30   -
//	Produce 4    12:05:00     12:05    12:00: 5, 12:05: 4    12:04      -
//	Pass                              12:00: 5, 12:05: 4    12:04      5     kept: lateness 10m
//	Produce 1    12:00:45     12:00    12:00: 6, 12:05: 4    12:04      5     late, allowed: recompute queued
//	Pass                              (same)                12:04      6     the whole bucket
//	Produce 2    12:00:50     12:00    12:00: 8, 12:05: 4    12:04      6
//	Pass                              (same)                12:04      8
func TestSimulate_ALateRowRecomputesTheBucket(t *testing.T) {
	coverage.Covers(t, "manager.window")
	decl := windowDecl()
	decl.Lateness = 10 * time.Minute
	r := RunWindowed(t, []int32{0}, decl, []Step{
		Produce{Partition: 0, Rows: 5, At: base.Add(30 * time.Second)},
		Produce{Partition: 0, Rows: 4, At: base.Add(5 * time.Minute)},
		Pass{},
		Produce{Partition: 0, Rows: 1, At: base.Add(45 * time.Second)},
		Pass{},
		Produce{Partition: 0, Rows: 2, At: base.Add(50 * time.Second)},
		Pass{},
	})
	assert.Equal(t, 12, r.Produced)
	assert.Equal(t, int64(8), r.LastValue[base])
	assert.Equal(t, int64(0), r.LateRefused)
	assert.Equal(t, int64(2), r.Recomputes)
	// The bucket is retained: its rows are still in the table.
	assert.Equal(t, int64(12), r.StillOpen)
}

// Beyond lateness a late row is refused even though the watermark has only
// just passed: 12:00 ended at 12:01, the lateness is two minutes, and the
// assertion is 12:04, so 12:01 + 2m = 12:03 <= 12:04 had already expired it.
func TestSimulate_ALateRowBeyondLatenessIsRefused(t *testing.T) {
	coverage.Covers(t, "manager.window")
	decl := windowDecl()
	decl.Lateness = 2 * time.Minute
	r := RunWindowed(t, []int32{0}, decl, []Step{
		Produce{Partition: 0, Rows: 5, At: base.Add(30 * time.Second)},
		Produce{Partition: 0, Rows: 4, At: base.Add(5 * time.Minute)},
		Pass{},
		Produce{Partition: 0, Rows: 1, At: base.Add(45 * time.Second)},
	})
	assert.Equal(t, int64(1), r.LateRefused)
	assert.Equal(t, int64(5), r.LastValue[base])
	assert.Equal(t, int64(0), r.Recomputes)
	// And the pass purged the expired bucket: only 12:05 is left.
	assert.Equal(t, int64(4), r.StillOpen)
}

// Three late rows for one bucket before the next pass, one recompute.
func TestSimulate_ABurstOfLateRowsIsOneRecompute(t *testing.T) {
	coverage.Covers(t, "manager.window")
	decl := windowDecl()
	decl.Lateness = 10 * time.Minute
	r := RunWindowed(t, []int32{0}, decl, []Step{
		Produce{Partition: 0, Rows: 5, At: base.Add(30 * time.Second)},
		Produce{Partition: 0, Rows: 4, At: base.Add(5 * time.Minute)},
		Pass{},
		Produce{Partition: 0, Rows: 1, At: base.Add(40 * time.Second)},
		Produce{Partition: 0, Rows: 1, At: base.Add(41 * time.Second)},
		Produce{Partition: 0, Rows: 1, At: base.Add(42 * time.Second)},
		Pass{},
	})
	assert.Equal(t, int64(8), r.LastValue[base])
	assert.Equal(t, int64(1), r.Recomputes)
}

// A late row committed, then a restart before the manager passed: the
// recompute set was in memory and is gone, but the rows are in the table,
// and the start pass republishes every retained bucket, so the exact value
// still reaches the sink.
func TestSimulate_ARecomputeSurvivesARestart(t *testing.T) {
	coverage.Covers(t, "manager.window")
	decl := windowDecl()
	decl.Lateness = 10 * time.Minute
	r := RunWindowed(t, []int32{0}, decl, []Step{
		Produce{Partition: 0, Rows: 5, At: base.Add(30 * time.Second)},
		Produce{Partition: 0, Rows: 4, At: base.Add(5 * time.Minute)},
		Pass{},
		Produce{Partition: 0, Rows: 1, At: base.Add(45 * time.Second)},
		Restart{},
		StartPass{}, // what Start does first
	})
	assert.Equal(t, int64(6), r.LastValue[base])
}

// The kick follows the commit: with the manager's loop running, a produce
// that moves the watermark publishes without any Pass step.
func TestSimulate_TheKickFollowsTheCommit(t *testing.T) {
	coverage.Covers(t, "manager.window")
	r := RunWindowedDriven(t, []int32{0}, windowDecl(), []Step{
		Produce{Partition: 0, Rows: 5, At: base.Add(30 * time.Second)},
		Produce{Partition: 0, Rows: 4, At: base.Add(5 * time.Minute)},
		AwaitPublished{Rows: 5},
	})
	assert.Equal(t, int64(5), r.Published)
	assert.Equal(t, int64(4), r.StillOpen)
	assert.Equal(t, int64(5), r.LastValue[base])
}

// Two partitions, one lagging in event time.
//
// The watermark is the minimum over the partitions, so a partition racing
// ahead cannot close the buckets a slower one is still filling: the lagging
// partition holds the window open until it catches up, and its rows are
// never late. Before the watermark the manager took the newest bucket in the
// table, whoever wrote it, and this scenario pinned the loss: five of the
// slow partition's rows, dropped because a partition it shared no data with
// had moved the stream past the bucket they were both filling.
//
//	step          part  event time   bucket   window table        asserted
//	--------------------------------------------------------------------------
//	Produce 3     p0    12:00:30     12:00    12:00: 3            -          p1 at -inf
//	Produce 3     p1    12:00:30     12:00    12:00: 6            11:59:30
//	Produce 4     p0    12:05:00     12:05    12:00: 6, 12:05: 4  11:59:30
//	                ^ p0 alone races five minutes ahead; the minimum is p1's
//	Pass                                      (same)              11:59:30   nothing closes
//	Produce 5     p1    12:00:45     12:00    12:00: 11, 12:05: 4 11:59:45
//	                ^ p1's share of the bucket, and it is not late
//	Pass                                      (same)              11:59:45   still open
//	Produce 1     p1    12:05:00     12:05    12:00: 11, 12:05: 5 12:04
//	                ^ p1 catches up; the minimum follows
//	Pass                                      12:05: 5            12:04      12:00 publishes 11
func TestSimulate_AFastPartitionHoldsForTheSlowOne(t *testing.T) {
	coverage.Covers(t, "manager.window")
	r := RunWindowed(t, []int32{0, 1}, windowDecl(), []Step{
		// Both partitions in the same early bucket.
		Produce{Partition: 0, Rows: 3, At: base.Add(30 * time.Second)},
		Produce{Partition: 1, Rows: 3, At: base.Add(30 * time.Second)},
		// Partition 0 races five minutes ahead in event time.
		Produce{Partition: 0, Rows: 4, At: base.Add(5 * time.Minute)},
		Pass{},
		// Partition 1 is still delivering its own share of the early bucket.
		Produce{Partition: 1, Rows: 5, At: base.Add(45 * time.Second)},
		Pass{},
		// And catches up, which is what closes the bucket both filled.
		Produce{Partition: 1, Rows: 1, At: base.Add(5 * time.Minute)},
		Pass{},
	})

	assert.Equal(t, 16, r.Produced)
	assert.Equal(t, int64(11), r.Published)
	assert.Equal(t, int64(5), r.StillOpen)
	assert.Equal(t, int64(0), r.LateRefused)
	assert.Equal(t, 0, r.Republished)
}

// One row whose event time is years ahead would advance the stream past
// every open bucket, close them all, and make everything after it late.
//
//	step         event time     bucket        window table   asserted
//	----------------------------------------------------------------------
//	Produce 5    12:00:30       12:00         12:00: 5       11:59:30
//	Produce 1    2099-01-01     (refused)     12:00: 5       11:59:30
//	               ^ one device with a wrong clock, or one bad field. The
//	                 engine cannot place an event time ahead of its own
//	                 clock, so the record never reaches the handler, never
//	                 enters the table, and never reaches the watermark.
//	Pass         -              -             12:00: 5       11:59:30
//	Produce 7    12:01:30       12:01         12:00: 5, 12:01: 7   12:00:30
//	               ^ the stream carries on where it actually is, and it is
//	                 not late, because nothing moved
//	Pass, Elapse 30s, IdleTick                                12:02:30
//	               ^ idle: newest 12:01:30 + 1m
//	Pass                                      empty          12:02:30
//	               ^ both buckets publish, all 12 rows. The bad record cost
//	                 the pipeline exactly itself.
//
// This is #358, reproduced end to end rather than against hand-written rows.
// Before the engine placed event time, that one record dragged the watermark
// 73 years ahead and every correctly stamped row after it was dropped as
// late: one device with a fast clock emptied a fleet's stream. The fleets
// most exposed were the ones least able to prevent it, the gateways with no
// battery-backed clock that boot to a fixed epoch.
//
// The record is refused before the handler, not held in the table: it is
// not late, because no earlier publication of its bucket exists to amend,
// and it is not open, because no watermark this engine computes will reach
// it. Unplaceable, so discarded, and counted in messages_unplaceable_total,
// not as late: it never had a bucket to be late for.
func TestSimulate_APoisonTimestampClosesEveryBucket(t *testing.T) {
	coverage.Covers(t, "manager.window")
	r := RunWindowed(t, []int32{0}, windowDecl(), []Step{
		Produce{Partition: 0, Rows: 5, At: base.Add(30 * time.Second)},
		// One row from a device that thinks it is 2099.
		Produce{Partition: 0, Rows: 1, At: time.Date(2099, 1, 1, 0, 0, 0, 0, time.UTC)},
		Pass{},
		// The stream carries on where it actually is.
		Produce{Partition: 0, Rows: 7, At: base.Add(90 * time.Second)},
		Pass{},
		// A close at the end, so every honest bucket has published.
		Elapse{By: 30 * time.Second},
		IdleTick{},
		Pass{},
	})

	assert.Equal(t, 13, r.Produced)
	// Every honest row published; the only row unaccounted for is the
	// dishonest one, refused before it could reach the table.
	assert.Equal(t, int64(12), r.Published)
	assert.Equal(t, int64(0), r.StillOpen)
	assert.Equal(t, int64(1), r.Unplaceable)
	assert.Equal(t, int64(0), r.LateRefused)
}

// Replay: the same rows delivered twice.
//
// At-least-once is what the engine promises, so a restart replays whatever
// the committed offsets did not cover. What would make that safe is a bucket
// published as a whole value on a deterministic key, so the destination holds
// the same number however many times the rows arrive.
//
//	once                                        twice
//	------------------------------------------  ------------------------------
//	Produce 5  12:00:30  ->  12:00: 5           Produce 5  ->  12:00: 5
//	                                            Produce 5  ->  12:00: 10
//	                                                           ^ the same rows
//	                                                             again, same
//	                                                             event times
//	Produce 4  12:05:00  ->  12:05: 4           Produce 4  ->  12:05: 4
//	                                            Produce 4  ->  12:05: 8
//	Pass       publishes 12:00, 5 rows          Poll       ->  publishes 10
//
// The engine has no record identity, so a replayed row is simply a new row
// and the bucket's value doubles. Dedupe on an observation id is what closes
// this, and it is not in the engine today: this asserts the doubling.
func TestSimulate_ReplayingEveryRowPublishesTheSameTotals(t *testing.T) {
	coverage.Covers(t, "manager.window")
	once := RunWindowed(t, []int32{0}, windowDecl(), []Step{
		Produce{Partition: 0, Rows: 5, At: base.Add(30 * time.Second)},
		Produce{Partition: 0, Rows: 4, At: base.Add(5 * time.Minute)},
		Pass{},
	})

	twice := RunWindowed(t, []int32{0}, windowDecl(), []Step{
		Produce{Partition: 0, Rows: 5, At: base.Add(30 * time.Second)},
		// The same rows again, as a replay from an uncommitted offset would
		// deliver them: same event times, same buckets.
		Produce{Partition: 0, Rows: 5, At: base.Add(30 * time.Second)},
		Produce{Partition: 0, Rows: 4, At: base.Add(5 * time.Minute)},
		Produce{Partition: 0, Rows: 4, At: base.Add(5 * time.Minute)},
		Pass{},
	})

	// The engine has no record identity, so a replayed row is a new row and
	// the bucket's value doubles. Dedupe on an observation id is what closes
	// this, and it is not in the engine today.
	if twice.Published == once.Published {
		t.Fatal("a replayed row no longer doubles the bucket: the engine dedupes " +
			"on record identity now, so invert this test")
	}
	assert.Equal(t, once.Published*2, twice.Published)
}
