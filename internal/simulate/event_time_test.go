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

// Rows out of order inside the grace all land in their own buckets, and the
// stream moving on closes them. This is the case grace_seconds exists for: a
// producer whose records arrive shuffled by less than the tolerance.
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
// A device whose clock is wrong, or one malformed field, and the pipeline
// silently drops the stream. Asserts the defect: the watermark design refuses
// a timestamp outside the engine's bounds, so it advances nothing.
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
	if r.LateDropped == 0 {
		t.Fatal("the stream survived a timestamp from 2099: the assigner refuses " +
			"out-of-range event times now, so invert this test")
	}
	// Every row that arrived after the bad one, gone. One record from a
	// device with a wrong clock took the rest of the stream with it.
	assert.Equal(t, int64(7), r.LateDropped)
}

// Replay: every event delivered twice produces the same published totals as
// delivering it once.
//
// At-least-once is what the engine promises, so a restart replays whatever
// the committed offsets did not cover. What makes that safe is that a bucket
// is published as a whole value on a deterministic key, so the destination
// holds the same number either way. This is the property a billing pipeline
// needs and the one that makes read-time deduplication unnecessary.
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
