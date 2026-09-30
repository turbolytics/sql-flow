package core

import (
	"context"
	"testing"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/turbolytics/sql-flow/internal/duckdb"
	"github.com/zeebo/assert"
)

var minuteSpec = WindowSpec{Name: "w", Size: time.Minute, Lateness: time.Minute}

func msgAt(p int32, off int64, at time.Time) Message {
	return Message{Topic: "t", Partition: p, Offset: off, LeaderEpoch: 3, EventAtNanos: at.UnixNano()}
}

var woT0 = time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC)

// The commit for a partition is the one before the lowest offset still
// feeding a retained bucket, so a worker that starts without the rows
// replays them.
func TestWindowOffsets_LowIsBeforeTheOldestRetainedRecord(t *testing.T) {
	o := NewWindowOffsets([]WindowSpec{minuteSpec})
	o.Note(msgAt(0, 100, woT0.Add(10*time.Second)))
	o.Note(msgAt(0, 101, woT0.Add(70*time.Second)))
	o.Note(msgAt(0, 102, woT0.Add(20*time.Second))) // out of order, same first bucket
	o.Merge()
	processed := NewMarks()
	processed.Advance("t", 0, Mark{Offset: 102})
	low := o.Low(processed)
	m, _ := low.Get("t", 0)
	assert.Equal(t, int64(99), m.Offset)
	assert.Equal(t, int32(3), m.LeaderEpoch)
}

// A bucket past its lateness against the manager's closed watermark drops
// its record, and the commit moves to the next retained bucket.
func TestWindowOffsets_ExpireMovesLowForward(t *testing.T) {
	o := NewWindowOffsets([]WindowSpec{minuteSpec})
	o.Note(msgAt(0, 100, woT0.Add(10*time.Second)))
	o.Note(msgAt(0, 150, woT0.Add(70*time.Second)))
	o.Merge()
	// Bucket 12:00 ends 12:01; with 60 s of lateness it expires at closed 12:02.
	o.Expire("w", woT0.Add(2*time.Minute))
	processed := NewMarks()
	processed.Advance("t", 0, Mark{Offset: 150})
	m, _ := o.Low(processed).Get("t", 0)
	assert.Equal(t, int64(149), m.Offset)
}

// One second short of expiry keeps the record: the predicate is the
// manager's, bucket_end + lateness <= closed.
func TestWindowOffsets_ExpireIsTheManagersPredicate(t *testing.T) {
	o := NewWindowOffsets([]WindowSpec{minuteSpec})
	o.Note(msgAt(0, 100, woT0.Add(10*time.Second)))
	o.Merge()
	o.Expire("w", woT0.Add(2*time.Minute-time.Second))
	processed := NewMarks()
	processed.Advance("t", 0, Mark{Offset: 100})
	m, _ := o.Low(processed).Get("t", 0)
	assert.Equal(t, int64(99), m.Offset)
}

// A partition with nothing retained commits what it processed.
func TestWindowOffsets_NothingRetainedCommitsProcessed(t *testing.T) {
	o := NewWindowOffsets([]WindowSpec{minuteSpec})
	processed := NewMarks()
	processed.Advance("t", 4, Mark{Offset: 7, LeaderEpoch: 2})
	m, _ := o.Low(processed).Get("t", 4)
	assert.Equal(t, Mark{Offset: 7, LeaderEpoch: 2}, m)
}

// Attribution is the record's own partition and its own bucket.
func TestWindowOffsets_PartitionsAreIndependent(t *testing.T) {
	o := NewWindowOffsets([]WindowSpec{minuteSpec})
	o.Note(msgAt(0, 5, woT0))
	o.Note(msgAt(1, 900, woT0))
	o.Merge()
	processed := NewMarks()
	processed.Advance("t", 0, Mark{Offset: 10})
	processed.Advance("t", 1, Mark{Offset: 950})
	low := o.Low(processed)
	m0, _ := low.Get("t", 0)
	m1, _ := low.Get("t", 1)
	assert.Equal(t, int64(4), m0.Offset)
	assert.Equal(t, int64(899), m1.Offset)
}

// A batch that rolls back leaves its records pending: the replay notes the
// same records, and the next successful commit writes them once.
func TestWindowOffsets_RollbackKeepsPendingRecords(t *testing.T) {
	o := NewWindowOffsets([]WindowSpec{minuteSpec})
	o.Note(msgAt(0, 5, woT0))
	o.Merge()
	assert.Equal(t, 1, len(o.Pending().Added))
	// Rolled back: Saved is not called. The replay notes the same record.
	o.Note(msgAt(0, 5, woT0))
	o.Merge()
	assert.Equal(t, 1, len(o.Pending().Added))
	o.Saved()
	assert.Equal(t, 0, len(o.Pending().Added))
}

// A record with no event time has no bucket and no record.
func TestWindowOffsets_IgnoresRecordsWithoutEventTimeOrPosition(t *testing.T) {
	o := NewWindowOffsets([]WindowSpec{minuteSpec})
	o.Note(Message{Topic: "t", Partition: 0, Offset: 1})
	o.Note(Message{EventAtNanos: woT0.UnixNano()})
	o.Merge()
	assert.Equal(t, 0, len(o.Pending().Added))
}

// Drop forgets a partition's records and says so for the store.
func TestWindowOffsets_Drop(t *testing.T) {
	o := NewWindowOffsets([]WindowSpec{minuteSpec})
	o.Note(msgAt(2, 5, woT0))
	o.Merge()
	o.Saved()
	o.Drop("t", []int32{2})
	processed := NewMarks()
	processed.Advance("t", 2, Mark{Offset: 9})
	m, _ := o.Low(processed).Get("t", 2)
	assert.Equal(t, int64(9), m.Offset)
	assert.Equal(t, []int32{2}, o.Pending().Dropped["t"])
}

// memConn is an in-memory DuckDB connection, as the watermark store's own
// test opens one.
func memConn(t *testing.T) adbc.Connection {
	t.Helper()
	ctx := context.Background()
	db, err := duckdb.OpenPath(ctx, "")
	assert.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	conn, err := db.Connect(ctx)
	assert.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	return conn
}

// The store round-trips records, deletes expired buckets with the tracker's
// predicate, and deletes a dropped partition.
func TestWindowOffsetStore_RoundTrip(t *testing.T) {
	coverage.Covers(t, "state.durability")
	conn := memConn(t)
	ctx := context.Background()
	store := NewWindowOffsetStore(conn, []WindowSpec{minuteSpec})
	assert.NoError(t, store.Init(ctx))

	o := NewWindowOffsets([]WindowSpec{minuteSpec})
	o.Note(msgAt(0, 100, woT0))
	o.Note(msgAt(0, 150, woT0.Add(time.Minute)))
	o.Note(msgAt(1, 7, woT0.Add(time.Minute)))
	o.Merge()
	assert.NoError(t, store.Save(ctx, o.Pending()))
	o.Saved()

	o.Expire("w", woT0.Add(2*time.Minute)) // drops bucket 12:00
	o.Drop("t", []int32{1})
	assert.NoError(t, store.Save(ctx, o.Pending()))

	got, err := store.Load(ctx)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(got))
	assert.Equal(t, int64(150), got[0].Mark.Offset)
	assert.Equal(t, int32(0), got[0].Partition)
	assert.That(t, got[0].Bucket.Equal(woT0.Add(time.Minute)))
}

func TestReplayFloor_RoundTrip(t *testing.T) {
	in := map[string]time.Time{"w": woT0, "v": woT0.Add(time.Minute)}
	out, ok := DecodeReplayFloor(EncodeReplayFloor(in))
	assert.That(t, ok)
	assert.That(t, out["w"].Equal(woT0))
	assert.That(t, out["v"].Equal(woT0.Add(time.Minute)))
	_, ok = DecodeReplayFloor("")
	assert.That(t, !ok)
	_, ok = DecodeReplayFloor(`{"closed":{}}`) // someone else's metadata
	assert.That(t, !ok)
}

// Note runs per record on the consume loop. A fetch's records share a
// partition and mostly a bucket, so the common case is one comparison.
func BenchmarkWindowOffsetsNote(b *testing.B) {
	o := NewWindowOffsets([]WindowSpec{minuteSpec})
	msgs := make([]Message, 500)
	for i := range msgs {
		msgs[i] = msgAt(int32(i/100), int64(i), woT0.Add(time.Duration(i)*100*time.Millisecond))
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		o.Note(msgs[i%len(msgs)])
		if i%len(msgs) == len(msgs)-1 {
			o.Merge()
			o.Saved()
		}
	}
}
