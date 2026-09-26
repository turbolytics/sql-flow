package core

import (
	"context"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/turbolytics/sql-flow/internal/duckdb"
	"github.com/zeebo/assert"
)

// The computation, one line at a time, with no engine and no database. The
// clock is a value the test moves; event times are what the test says they
// are. Under the spec's shape D unless a test says otherwise: one-minute
// buckets, one minute of grace, ten seconds of idle close.
//
// Notation in the diagrams: W is the window's watermark after Advance; a
// dash is "did not move"; p0, p1 are partitions of topic t.

var (
	wmT0   = time.Date(2026, 9, 25, 12, 0, 0, 0, time.UTC)
	wmSpec = WindowSpec{Name: "w", Size: time.Minute, Grace: time.Minute, IdleClose: 10 * time.Second}
)

type wmClock struct{ at time.Time }

func (c *wmClock) now() time.Time { return c.at }

func (c *wmClock) tick(d time.Duration) { c.at = c.at.Add(d) }

func parts(ps ...int32) map[string][]int32 { return map[string][]int32{"t": ps} }

func at(d time.Duration) int64 { return wmT0.Add(d).UnixNano() }

// A held partition that has delivered is its newest event time less the
// grace, and the assertion is that and nothing to do with the clock.
//
//	step                    seen p0    W
//	----------------------------------------------
//	assign p0               -          -      -inf: nothing seen
//	observe 12:05:00        12:05:00   12:04
//	observe 12:03:00        12:05:00   -      older: seen is a max
//	observe 12:07:30        12:07:30   12:06:30
func TestWindowWatermark_AHeldPartitionIsItsNewestLessTheGrace(t *testing.T) {
	coverage.Covers(t, "manager.window")
	c := &wmClock{at: wmT0}
	w := NewWatermarks([]WindowSpec{wmSpec}, c.now)
	w.Assigned(parts(0))
	assert.Equal(t, 0, len(w.Advance()))

	w.Observe("t", 0, at(5*time.Minute))
	moved := w.Advance()
	assert.Equal(t, wmT0.Add(4*time.Minute), moved["w"])

	w.Observe("t", 0, at(3*time.Minute))
	assert.Equal(t, 0, len(w.Advance()))

	w.Observe("t", 0, at(7*time.Minute+30*time.Second))
	moved = w.Advance()
	assert.Equal(t, wmT0.Add(6*time.Minute+30*time.Second), moved["w"])
}

// Two partitions: the minimum. A partition racing ahead in event time
// cannot close the buckets a slower one is still filling, which is the
// defect TestSimulate_AFastPartitionClosesASlowOnesBuckets pinned before
// the watermark and inverts after it.
//
//	step                    seen p0    seen p1    W
//	--------------------------------------------------------
//	assign p0, p1           -          -          -
//	observe p0 12:00:30     12:00:30   -          -      p1 at -inf holds
//	observe p1 12:00:30     12:00:30   12:00:30   11:59:30
//	observe p0 12:05:00     12:05:00   12:00:30   -      min is p1: 11:59:30
//	observe p1 12:02:00     12:05:00   12:02:00   12:01
func TestWindowWatermark_TheMinimumOverPartitionsHoldsForTheSlowOne(t *testing.T) {
	coverage.Covers(t, "manager.window")
	c := &wmClock{at: wmT0}
	w := NewWatermarks([]WindowSpec{wmSpec}, c.now)
	w.Assigned(parts(0, 1))

	w.Observe("t", 0, at(30*time.Second))
	assert.Equal(t, 0, len(w.Advance()))

	w.Observe("t", 1, at(30*time.Second))
	assert.Equal(t, wmT0.Add(-30*time.Second), w.Advance()["w"])

	w.Observe("t", 0, at(5*time.Minute))
	assert.Equal(t, 0, len(w.Advance()))

	w.Observe("t", 1, at(2*time.Minute))
	assert.Equal(t, wmT0.Add(time.Minute), w.Advance()["w"])
}

// An idle partition leaves the minimum, so a quiet partition does not pin
// the window open for the busy ones; and when every partition is idle the
// stream is done with what it has, and the window closes through the
// newest row's bucket: max(seen) + size, no grace.
//
//	step                        clock    p0            p1            W
//	---------------------------------------------------------------------
//	assign p0, p1               12:00
//	observe p0 12:00:30                  seen 12:00:30
//	observe p1 12:00:30                  ...           seen 12:00:30  11:59:30
//	observe p0 12:05:00                  seen 12:05    (12:00:30)     -   p1 holds
//	tick 11s, observe p0 12:06  12:00:11 seen 12:06    idle           12:05  p1 left
//	tick 11s                    12:00:22 idle          idle           12:07  = 12:06 + 1m
func TestWindowWatermark_AnIdlePartitionLeavesAndAllIdleClosesThroughTheNewestBucket(t *testing.T) {
	coverage.Covers(t, "manager.window")
	c := &wmClock{at: wmT0}
	w := NewWatermarks([]WindowSpec{wmSpec}, c.now)
	w.Assigned(parts(0, 1))
	w.Observe("t", 0, at(30*time.Second))
	w.Observe("t", 1, at(30*time.Second))
	assert.Equal(t, wmT0.Add(-30*time.Second), w.Advance()["w"])

	w.Observe("t", 0, at(5*time.Minute))
	assert.Equal(t, 0, len(w.Advance()))

	c.tick(11 * time.Second)
	w.Observe("t", 0, at(6*time.Minute))
	assert.Equal(t, wmT0.Add(5*time.Minute), w.Advance()["w"])

	c.tick(11 * time.Second)
	assert.Equal(t, wmT0.Add(7*time.Minute), w.Advance()["w"])
	// And asserting again with nothing new asserts nothing new: an idle
	// pipeline writes nothing.
	c.tick(time.Hour)
	assert.Equal(t, 0, len(w.Advance()))
}

// Without an idle bound nothing is ever idle, so a stream that stops never
// publishes its last bucket. That is shapes A and B's documented failure
// mode, and it is a rule here rather than an accident.
func TestWindowWatermark_WithoutAnIdleBoundAStoppedStreamHolds(t *testing.T) {
	coverage.Covers(t, "manager.window")
	c := &wmClock{at: wmT0}
	spec := wmSpec
	spec.IdleClose = 0
	w := NewWatermarks([]WindowSpec{spec}, c.now)
	w.Assigned(parts(0))
	w.Observe("t", 0, at(5*time.Minute))
	assert.Equal(t, wmT0.Add(4*time.Minute), w.Advance()["w"])
	c.tick(24 * time.Hour)
	assert.Equal(t, 0, len(w.Advance()))
}

// A partition just assigned holds at -inf until it delivers: the group may
// have handed this process a backlog older than anything it has seen, and
// closing over that backlog would make all of it late.
func TestWindowWatermark_AnAssignedPartitionHoldsUntilItDelivers(t *testing.T) {
	coverage.Covers(t, "manager.window")
	c := &wmClock{at: wmT0}
	w := NewWatermarks([]WindowSpec{wmSpec}, c.now)
	w.Assigned(parts(0))
	w.Observe("t", 0, at(5*time.Minute))
	assert.Equal(t, wmT0.Add(4*time.Minute), w.Advance()["w"])

	w.Assigned(parts(1))
	w.Observe("t", 0, at(9*time.Minute))
	assert.Equal(t, 0, len(w.Advance()))

	// Nor does the new partition's silence count as idleness yet: it is
	// measured from the assignment, and eleven seconds after it the
	// partition leaves the minimum like any other idle one, while p0, still
	// delivering, is the minimum on its own.
	c.tick(11 * time.Second)
	w.Observe("t", 0, at(9*time.Minute))
	assert.Equal(t, wmT0.Add(8*time.Minute), w.Advance()["w"])
}

// Revoked and lost are different facts. A revoked partition is another
// member's now: it leaves the minimum, and what it contributed here closes
// by the others' progress. A lost partition may come back with a backlog:
// it holds at its last position, is never idle, and resumes from there when
// it is assigned again.
//
//	step                     p0             p1            W
//	---------------------------------------------------------------
//	assign p0, p1
//	observe p0 12:05         12:05          -inf          -
//	observe p1 12:02         12:05          12:02         12:01
//	lose p1                  12:05          lost @12:02   -       holds
//	observe p0 12:09, +1m    12:09          lost @12:02   -       still: never idle
//	assign p1                12:09          12:02 held    -       resumes, not -inf
//	observe p1 12:06         12:09          12:06         12:05
//	revoke p1                12:09          gone          12:08   p0 alone
func TestWindowWatermark_ARevokedPartitionLeavesAndALostOneHolds(t *testing.T) {
	coverage.Covers(t, "manager.window")
	c := &wmClock{at: wmT0}
	w := NewWatermarks([]WindowSpec{wmSpec}, c.now)
	w.Assigned(parts(0, 1))
	w.Observe("t", 0, at(5*time.Minute))
	assert.Equal(t, 0, len(w.Advance()))
	w.Observe("t", 1, at(2*time.Minute))
	assert.Equal(t, wmT0.Add(time.Minute), w.Advance()["w"])

	w.Lost(parts(1))
	assert.Equal(t, 0, len(w.Advance()))
	c.tick(time.Minute)
	w.Observe("t", 0, at(9*time.Minute))
	assert.Equal(t, 0, len(w.Advance()))

	w.Assigned(parts(1))
	assert.Equal(t, 0, len(w.Advance()))
	w.Observe("t", 1, at(6*time.Minute))
	assert.Equal(t, wmT0.Add(5*time.Minute), w.Advance()["w"])

	w.Released(parts(1))
	assert.Equal(t, wmT0.Add(8*time.Minute), w.Advance()["w"])
}

// A partition lost before it ever delivered holds at -inf, the same as one
// just assigned: nothing is known about what it will bring.
func TestWindowWatermark_APartitionLostBeforeDeliveringHoldsEverything(t *testing.T) {
	coverage.Covers(t, "manager.window")
	c := &wmClock{at: wmT0}
	w := NewWatermarks([]WindowSpec{wmSpec}, c.now)
	w.Assigned(parts(0, 1))
	w.Observe("t", 0, at(5*time.Minute))
	w.Lost(parts(1))
	c.tick(time.Hour)
	assert.Equal(t, 0, len(w.Advance()))
}

// The watermark never moves backwards, by construction: a partition
// assigned later with older data lowers the minimum, and the stored value
// stays where it was.
func TestWindowWatermark_NeverBackwards(t *testing.T) {
	coverage.Covers(t, "manager.window")
	c := &wmClock{at: wmT0}
	w := NewWatermarks([]WindowSpec{wmSpec}, c.now)
	w.Assigned(parts(0))
	w.Observe("t", 0, at(10*time.Minute))
	assert.Equal(t, wmT0.Add(9*time.Minute), w.Advance()["w"])

	w.Assigned(parts(1))
	w.Observe("t", 1, at(2*time.Minute))
	assert.Equal(t, 0, len(w.Advance()))
	got, ok := w.Asserted("w")
	assert.That(t, ok)
	assert.Equal(t, wmT0.Add(9*time.Minute), got)

	// And once p1 catches up, the minimum is p0's 12:09 again, which is where
	// the stored value already is: no move. Then p0 moves on and it follows.
	w.Observe("t", 1, at(11*time.Minute))
	assert.Equal(t, 0, len(w.Advance()))
	w.Observe("t", 0, at(12*time.Minute))
	assert.Equal(t, wmT0.Add(10*time.Minute), w.Advance()["w"])
}

// A record that stamps nothing moves nothing. The engine's placement rule
// keeps unplaceable records from ever reaching Observe; this pins the one
// value that does reach it and must still be ignored.
func TestWindowWatermark_AnUnstampedRecordMovesNothing(t *testing.T) {
	coverage.Covers(t, "manager.window")
	c := &wmClock{at: wmT0}
	w := NewWatermarks([]WindowSpec{wmSpec}, c.now)
	w.Assigned(parts(0))
	w.Observe("t", 0, 0)
	w.Observe("t", 0, EventTimeMissing)
	assert.Equal(t, 0, len(w.Advance()))
	// Such a pipeline still closes by idleness -- from the floor, if the
	// table has one, since nothing was seen.
	w.Restore("w", wmT0.Add(3*time.Minute), time.Time{})
	c.tick(11 * time.Second)
	assert.Equal(t, wmT0.Add(4*time.Minute), w.Advance()["w"])
}

// After a restart the process has seen nothing, and the idle close is
// measured from what the table holds instead, so a quiet stream's last
// buckets still close. The stored assertion is restored too, so the first
// Advance cannot land below it.
func TestWindowWatermark_TheRestartFloorClosesByIdleness(t *testing.T) {
	coverage.Covers(t, "manager.window")
	c := &wmClock{at: wmT0}
	w := NewWatermarks([]WindowSpec{wmSpec}, c.now)
	w.Restore("w", wmT0.Add(7*time.Minute), wmT0.Add(6*time.Minute))
	w.Assigned(parts(0))
	// Held, nothing seen: holds. The backlog may be older than the floor.
	assert.Equal(t, 0, len(w.Advance()))
	got, ok := w.Asserted("w")
	assert.That(t, ok)
	assert.Equal(t, wmT0.Add(6*time.Minute), got)
	// Idle for the bound with nothing delivered: the stream is done, and the
	// newest bucket in the table closes.
	c.tick(11 * time.Second)
	assert.Equal(t, wmT0.Add(8*time.Minute), w.Advance()["w"])
}

// A source with no partitions to report is one partition, held while it
// delivers and lost while it cannot. A reconnect longer than the idle bound
// is not a quiet stream.
//
//	step                         delivering   W
//	----------------------------------------------------
//	delivering, observe 12:05    yes          12:04
//	not delivering, +5m          no           -       lost: holds, not idle
//	delivering, +3s              yes          -       back 3s: not idle yet
//	+11s                         yes          12:06   idle from the resumption
func TestWindowWatermark_APartitionlessSourceIsLostWhileNotDelivering(t *testing.T) {
	coverage.Covers(t, "manager.window")
	c := &wmClock{at: wmT0}
	w := NewWatermarks([]WindowSpec{wmSpec}, c.now)
	w.SetDelivering(true)
	w.Observe("", 0, at(5*time.Minute))
	assert.Equal(t, wmT0.Add(4*time.Minute), w.Advance()["w"])

	w.SetDelivering(false)
	c.tick(5 * time.Minute)
	assert.Equal(t, 0, len(w.Advance()))

	w.SetDelivering(true)
	c.tick(3 * time.Second)
	assert.Equal(t, 0, len(w.Advance()))

	c.tick(11 * time.Second)
	assert.Equal(t, wmT0.Add(6*time.Minute), w.Advance()["w"])
}

// Two windows on one pipeline share the partitions and differ in their
// durations: each gets its own value from the same facts.
func TestWindowWatermark_EachWindowIsComputedFromTheSameFacts(t *testing.T) {
	coverage.Covers(t, "manager.window")
	c := &wmClock{at: wmT0}
	tight := WindowSpec{Name: "tight", Size: time.Minute, Grace: 0, IdleClose: 10 * time.Second}
	loose := WindowSpec{Name: "loose", Size: 5 * time.Minute, Grace: 2 * time.Minute}
	w := NewWatermarks([]WindowSpec{tight, loose}, c.now)
	w.Assigned(parts(0))
	w.Observe("t", 0, at(10*time.Minute))
	moved := w.Advance()
	assert.Equal(t, wmT0.Add(10*time.Minute), moved["tight"])
	assert.Equal(t, wmT0.Add(8*time.Minute), moved["loose"])

	c.tick(11 * time.Second)
	moved = w.Advance()
	assert.Equal(t, wmT0.Add(11*time.Minute), moved["tight"])
	_, hasLoose := moved["loose"]
	assert.That(t, !hasLoose)
}

// The store: one row per window, updated in place, read back on any
// connection; and a table with nothing asserted reads as nothing, not zero.
func TestStateDurability_TheWatermarkStoreKeepsOneRowPerWindow(t *testing.T) {
	coverage.Covers(t, "state.durability")
	ctx := context.Background()
	db, err := duckdb.OpenPath(ctx, "")
	assert.NoError(t, err)
	defer db.Close()
	conn, err := db.Connect(ctx)
	assert.NoError(t, err)
	defer conn.Close()
	other, err := db.Connect(ctx)
	assert.NoError(t, err)
	defer other.Close()

	s := NewWatermarkStore(conn)
	assert.NoError(t, s.Init(ctx))
	assert.NoError(t, s.Init(ctx))

	_, ok, err := s.Load(ctx, "w")
	assert.NoError(t, err)
	assert.That(t, !ok)

	assert.NoError(t, s.Save(ctx, "w", wmT0))
	assert.NoError(t, s.Save(ctx, "w", wmT0.Add(time.Minute)))
	assert.NoError(t, s.Save(ctx, "it's", wmT0))

	got, ok, err := LoadWatermark(ctx, other, "w")
	assert.NoError(t, err)
	assert.That(t, ok)
	assert.Equal(t, wmT0.Add(time.Minute), got)
	got, ok, err = LoadWatermark(ctx, other, "it's")
	assert.NoError(t, err)
	assert.That(t, ok)
	assert.Equal(t, wmT0, got)

	var rows int64
	assert.NoError(t, scalar(ctx, other, `SELECT count(*)::BIGINT FROM `+WatermarksTable, func(rec arrow.Record) error {
		rows = rec.Column(0).(*array.Int64).Value(0)
		return nil
	}))
	assert.Equal(t, int64(2), rows)

	// And the newest-bucket read the restart floor comes from.
	stmt, err := conn.NewStatement()
	assert.NoError(t, err)
	assert.NoError(t, stmt.SetSqlQuery(`CREATE TABLE "win dow" ("bucket" TIMESTAMPTZ, n BIGINT)`))
	_, err = stmt.ExecuteUpdate(ctx)
	assert.NoError(t, err)
	stmt.Close()
	_, ok, err = NewestBucketStart(ctx, conn, "win dow", "bucket")
	assert.NoError(t, err)
	assert.That(t, !ok)
	stmt, err = conn.NewStatement()
	assert.NoError(t, err)
	assert.NoError(t, stmt.SetSqlQuery(`INSERT INTO "win dow" VALUES (TIMESTAMPTZ '2026-09-25 12:03:00+00:00', 1), (TIMESTAMPTZ '2026-09-25 12:01:00+00:00', 1)`))
	_, err = stmt.ExecuteUpdate(ctx)
	assert.NoError(t, err)
	stmt.Close()
	newest, ok, err := NewestBucketStart(ctx, conn, "win dow", "bucket")
	assert.NoError(t, err)
	assert.That(t, ok)
	assert.Equal(t, wmT0.Add(3*time.Minute), newest)
}
