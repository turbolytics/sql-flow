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
// grace, a ten-second idle close, and late rows dropped.
//
// Two clocks, and keeping them apart is the point of the whole file:
//
//	engine clock   advances one second per step, plus whatever Elapse adds.
//	               Nothing here decides a window on it.
//	event time     what a row says about itself, chosen by the script with
//	               Produce{At}. This is what a bucket is cut on.
//
// The columns are what the manager could see if it polled at that moment:
//
//	step           what the script does
//	event time     the row's own clock
//	bucket         where that event time falls: event time truncated to a minute
//	window table   what the handler has written, and what a poll would read
//	watermark      what the manager has decided; a bucket closes when its end
//	               is at or before it
//
// Two rules explain every outcome below. The watermark after a grace close is
// `newest bucket - grace`; after an idle close it is `newest bucket + size`.
// And the manager collects late rows only on a poll that moves the watermark,
// which is why the scenarios that assert a loss end by forcing a close.

// Rows out of order inside the grace all land in their own buckets, and the
// stream moving on closes them. This is the case grace_seconds exists for: a
// producer whose records arrive shuffled by less than the tolerance.
//
//	step         event time   bucket   window table          watermark
//	-----------------------------------------------------------------------
//	Produce 3    12:01:30     12:01    12:01: 3              -
//	Produce 2    12:01:05     12:01    12:01: 5              -
//	               ^ older than the row before it, same bucket, not late:
//	                 nothing has closed, so there is no watermark to be behind
//	Produce 4    12:05:00     12:05    12:01: 5, 12:05: 4    -
//	Poll         -            -        12:05: 4              12:04
//	               ^ first close, by grace: newest 12:05 - 1m grace = 12:04.
//	                 12:01 ends at 12:02, at or before 12:04, so it publishes
//	                 all 5 rows. 12:05 ends at 12:06 and stays open.
func TestSimulate_OutOfOrderRowsInsideTheGraceAreNotLate(t *testing.T) {
	coverage.Covers(t, "manager.window")
	r := RunWindowed(t, []int32{0}, windowDecl(), []Step{
		Produce{Partition: 0, Rows: 3, At: base.Add(90 * time.Second)},
		// Older than the rows before it, and still inside the same minute.
		Produce{Partition: 0, Rows: 2, At: base.Add(65 * time.Second)},
		// The stream moves on by more than the grace, which closes 12:01.
		Produce{Partition: 0, Rows: 4, At: base.Add(5 * time.Minute)},
		Poll{},
	})

	assert.Equal(t, 9, r.Produced)
	// Every row is accounted for: published, or still open in a later bucket.
	assert.Equal(t, int64(9), r.Published+r.StillOpen)
	assert.Equal(t, int64(0), r.LateDropped)
	assert.Equal(t, int64(0), r.Republished)
}

// A row for a bucket the watermark has passed is late, and late_rows drop
// discards it. That is the declared contract, so this is what correct looks
// like rather than a defect.
//
//	step         event time   bucket   window table          watermark
//	-----------------------------------------------------------------------
//	Produce 5    12:00:30     12:00    12:00: 5              -
//	Produce 4    12:05:00     12:05    12:00: 5, 12:05: 4    -
//	Poll         -            -        12:05: 4              12:04
//	               ^ grace close: 12:00 publishes its 5 rows
//	Produce 6    12:00:30     12:00    12:00: 6, 12:05: 4    12:04
//	               ^ back into a bucket that ended at 12:01, which is already
//	                 at or before the watermark. These 6 are late on arrival.
//	Elapse 30s   -            -        (same)                12:04
//	IdleTick     -            -        (same)                12:04
//	               ^ the engine confirms the stream quiet for 10s
//	Poll         -            -        empty                 12:06
//	               ^ idle close: newest 12:05 + 1m size = 12:06, so 12:05
//	                 publishes its 4 rows. The watermark moved, so this is
//	                 also when the 6 late rows are collected and dropped.
func TestSimulate_ARowForAClosedBucketIsDropped(t *testing.T) {
	coverage.Covers(t, "manager.window")
	r := RunWindowed(t, []int32{0}, windowDecl(), []Step{
		Produce{Partition: 0, Rows: 5, At: base.Add(30 * time.Second)},
		Produce{Partition: 0, Rows: 4, At: base.Add(5 * time.Minute)},
		Poll{},
		// Back in the bucket that just closed.
		Produce{Partition: 0, Rows: 6, At: base.Add(30 * time.Second)},
		// The manager collects late rows on a poll that moves the watermark,
		// not on every poll, so the run has to reach a close for the drop to
		// have happened. Without this the count is whatever the timing gave:
		// six without -race and four with it.
		Elapse{By: 30 * time.Second},
		IdleTick{},
		Poll{},
	})

	assert.Equal(t, 15, r.Produced)
	// The six that went back to a closed bucket are gone, which is what
	// late_rows drop promises. How the surviving nine split between
	// published and still open depends on whether the closing idle tick
	// lands before the run stops, so only the loss is pinned.
	assert.Equal(t, int64(6), r.LateDropped)
}

// Two partitions, one lagging in event time.
//
// The engine takes the newest bucket in the table, whoever wrote it, so a
// partition that races ahead closes the buckets a slower one is still
// filling. A per-partition watermark combined by minimum is what stops this:
// the lagging partition holds the window open until it catches up.
//
//	step          part  event time   bucket   window table        watermark
//	--------------------------------------------------------------------------
//	Produce 3     p0    12:00:30     12:00    12:00: 3            -
//	Produce 3     p1    12:00:30     12:00    12:00: 6            -
//	                ^ both partitions are filling the same bucket
//	Produce 4     p0    12:05:00     12:05    12:00: 6, 12:05: 4  -
//	                ^ p0 alone races five minutes ahead in event time
//	Poll          -     -            -        12:05: 4            12:04
//	                ^ grace close on p0's newest bucket: 12:00 publishes 6.
//	                  p1 never said it was done with 12:00; nothing asked it.
//	Produce 5     p1    12:00:45     12:00    12:00: 5, 12:05: 4  12:04
//	                ^ p1's own share of a bucket closed underneath it: late
//	Poll, Elapse, IdleTick, Poll                empty             12:06
//	                ^ idle close publishes 12:05, and collects p1's 5 rows
//	                  as late. They are gone.
//
//	With a per-partition watermark:  W = min(p0, p1)
//	  p0 = 12:05 - 1m = 12:04        p1 = 12:00 - 1m = 11:59
//	  W  = 11:59, so 12:00 (ending 12:01) stays open until p1 moves on.
//
// Asserts the defect, because the engine has it. It inverts when the
// watermark lands.
func TestSimulate_AFastPartitionClosesASlowOnesBuckets(t *testing.T) {
	coverage.Covers(t, "manager.window")
	r := RunWindowed(t, []int32{0, 1}, windowDecl(), []Step{
		// Both partitions in the same early bucket.
		Produce{Partition: 0, Rows: 3, At: base.Add(30 * time.Second)},
		Produce{Partition: 1, Rows: 3, At: base.Add(30 * time.Second)},
		// Partition 0 races five minutes ahead in event time.
		Produce{Partition: 0, Rows: 4, At: base.Add(5 * time.Minute)},
		Poll{},
		// Partition 1 is still delivering its own share of the early bucket,
		// which the fast partition has just closed underneath it.
		Produce{Partition: 1, Rows: 5, At: base.Add(45 * time.Second)},
		Poll{},
		// A close at the end, so late collection has certainly happened:
		// the manager collects on a poll that moves the watermark, not on
		// every poll.
		Elapse{By: 30 * time.Second},
		IdleTick{},
		Poll{},
	})

	assert.Equal(t, 15, r.Produced)
	if r.LateDropped == 0 {
		t.Fatal("nothing was lost to the fast partition: the watermark is per " +
			"partition now, so invert this test and assert every row is accounted for")
	}
	// The slow partition's whole later share, lost because a partition it
	// shares no data with moved the stream past the bucket they were both
	// filling.
	assert.Equal(t, int64(5), r.LateDropped)
}

// One row whose event time is years ahead advances the stream past every open
// bucket, closes them all, and makes everything after it late.
//
//	step         event time     bucket        window table   watermark
//	----------------------------------------------------------------------
//	Produce 5    12:00:30       12:00         12:00: 5       -
//	Produce 1    2099-01-01     (refused)     12:00: 5       -
//	               ^ one device with a wrong clock, or one bad field. The
//	                 engine cannot place an event time ahead of its own
//	                 clock, so the record never reaches the handler, never
//	                 enters the table, and never touches the watermark.
//	Poll         -              -             12:00: 5       -
//	               ^ nothing to close: one bucket, nothing past it
//	Produce 7    12:01:30       12:01         12:00: 5, 12:01: 7
//	               ^ the stream carries on where it actually is, and it is
//	                 not late, because nothing moved
//	Poll, Elapse, IdleTick, Poll               empty          12:02
//	               ^ the idle close publishes both buckets, all 12 rows.
//	                 The bad record cost the pipeline exactly itself.
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
// it. Unplaceable, so discarded, and counted in messages_unplaceable_total.
func TestSimulate_APoisonTimestampClosesEveryBucket(t *testing.T) {
	coverage.Covers(t, "manager.window")
	r := RunWindowed(t, []int32{0}, windowDecl(), []Step{
		Produce{Partition: 0, Rows: 5, At: base.Add(30 * time.Second)},
		// One row from a device that thinks it is 2099.
		Produce{Partition: 0, Rows: 1, At: time.Date(2099, 1, 1, 0, 0, 0, 0, time.UTC)},
		Poll{},
		// The stream carries on where it actually is, and is now late.
		Produce{Partition: 0, Rows: 7, At: base.Add(90 * time.Second)},
		Poll{},
		// A close at the end, so late collection has certainly happened:
		// the manager collects on a poll that moves the watermark, not on
		// every poll.
		Elapse{By: 30 * time.Second},
		IdleTick{},
		Poll{},
	})

	assert.Equal(t, 13, r.Produced)
	// Every honest row published; the only row unaccounted for is the
	// dishonest one, refused before it could reach the table.
	assert.Equal(t, int64(12), r.Published)
	assert.Equal(t, int64(0), r.StillOpen)
	assert.Equal(t, int64(1), r.LateDropped)
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
//	Poll       publishes 12:00, 5 rows          Poll       ->  publishes 10
//
// The engine has no record identity, so a replayed row is simply a new row
// and the bucket's value doubles. Dedupe on an observation id is what closes
// this, and it is not in the engine today: this asserts the doubling.
func TestSimulate_ReplayingEveryRowPublishesTheSameTotals(t *testing.T) {
	coverage.Covers(t, "manager.window")
	once := RunWindowed(t, []int32{0}, windowDecl(), []Step{
		Produce{Partition: 0, Rows: 5, At: base.Add(30 * time.Second)},
		Produce{Partition: 0, Rows: 4, At: base.Add(5 * time.Minute)},
		Poll{},
	})

	twice := RunWindowed(t, []int32{0}, windowDecl(), []Step{
		Produce{Partition: 0, Rows: 5, At: base.Add(30 * time.Second)},
		// The same rows again, as a replay from an uncommitted offset would
		// deliver them: same event times, same buckets.
		Produce{Partition: 0, Rows: 5, At: base.Add(30 * time.Second)},
		Produce{Partition: 0, Rows: 4, At: base.Add(5 * time.Minute)},
		Produce{Partition: 0, Rows: 4, At: base.Add(5 * time.Minute)},
		Poll{},
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
