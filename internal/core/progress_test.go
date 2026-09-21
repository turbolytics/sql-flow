package core

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/turbolytics/sql-flow/internal/duckdb"
	"github.com/zeebo/assert"
)

// progressRecorder is the test double for the store: it keeps every record.
type progressRecorder struct {
	mu   sync.Mutex
	recs []Progress
}

func (r *progressRecorder) Record(_ context.Context, p Progress) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.recs = append(r.recs, p)
	return nil
}

func (r *progressRecorder) last() (Progress, int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.recs) == 0 {
		return Progress{}, 0
	}
	return r.recs[len(r.recs)-1], len(r.recs)
}

// A batch moves both clocks and the count. An idle tick moves the commit
// clock only. That distinction is what lets a predicate tell "quiet" from
// "stuck", so it is the first thing pinned.
func TestCoreConsumeLoop_ProgressRecordsArrivalOnBatchAndCommitOnIdle(t *testing.T) {
	coverage.Covers(t, "core.consume_loop")
	rec := &progressRecorder{}
	src := newBlockingSource(messages(3))
	// Zero interval: this test is about what a commit records, not about how
	// often the table is written, and the production throttle would pace it
	// at one record a second against a five second deadline.
	tb := NewTurbine(src, &fakeHandler{}, &fakeSink{}, 1000, 30*time.Millisecond,
		&sync.Mutex{}, PipelineErrorPolicies{},
		WithProgressStore(rec), WithProgressWriteInterval(0))
	done := make(chan struct{})
	go func() { _, _ = tb.ConsumeLoop(context.Background(), 0); close(done) }()

	deadline := time.After(5 * time.Second)
	var afterBatch Progress
	for {
		p, n := rec.last()
		if n >= 1 && p.Messages == 3 {
			afterBatch = p
			break
		}
		select {
		case <-deadline:
			t.Fatalf("no progress record with 3 messages; records=%d", n)
		default:
			time.Sleep(5 * time.Millisecond)
		}
	}
	assert.That(t, !afterBatch.LastArrival.IsZero())
	assert.That(t, !afterBatch.LastCommit.Before(afterBatch.LastArrival))

	// Now the source is silent. Idle ticks keep recording, and each one
	// carries the same arrival the batch had, unchanged: the record is
	// always the truth about both clocks rather than a delta, which is what
	// lets a throttled write be retried without losing the arrival.
	deadline = time.After(5 * time.Second)
	for {
		p, n := rec.last()
		if n >= 4 {
			assert.Equal(t, afterBatch.LastArrival, p.LastArrival)
			assert.That(t, p.LastCommit.After(afterBatch.LastCommit))
			assert.Equal(t, int64(3), p.Messages)
			break
		}
		select {
		case <-deadline:
			t.Fatalf("idle ticks did not record progress; records=%d", n)
		default:
			time.Sleep(5 * time.Millisecond)
		}
	}

	// The snapshot is the reader's view, and it keeps the arrival clock
	// through every idle tick. That is what /healthz and /stats report.
	snap := tb.Progress()
	assert.Equal(t, int64(3), snap.Messages)
	assert.Equal(t, afterBatch.LastArrival, snap.LastArrival)
	assert.That(t, snap.LastCommit.After(afterBatch.LastCommit))
	close(src.release)
	<-done
}

// The store writes one row and rewrites it; it never grows. Read back
// through a second statement, the way the window manager will.
func TestStateDurability_ProgressStoreKeepsOneRow(t *testing.T) {
	coverage.Covers(t, "state.durability")
	ctx := context.Background()
	db, err := duckdb.OpenPath(ctx, "")
	assert.NoError(t, err)
	defer db.Close()
	conn, err := db.Connect(ctx)
	assert.NoError(t, err)
	defer conn.Close()

	store := NewProgressStore(conn)
	assert.NoError(t, store.Init(ctx))
	assert.NoError(t, store.Init(ctx)) // idempotent, like the offsets table

	t0 := time.Date(2026, 9, 10, 12, 0, 0, 0, time.UTC)
	assert.NoError(t, store.Record(ctx, Progress{LastArrival: t0, LastCommit: t0, Messages: 1}))
	// An idle tick: no arrival, the commit clock moves.
	assert.NoError(t, store.Record(ctx, Progress{LastCommit: t0.Add(time.Minute), Messages: 1}))

	stmt, err := conn.NewStatement()
	assert.NoError(t, err)
	defer stmt.Close()
	assert.NoError(t, stmt.SetSqlQuery(`SELECT count(*), max(messages), epoch(max(last_commit) - max(last_arrival))::BIGINT FROM sqlflow_progress`))
	reader, _, err := stmt.ExecuteQuery(ctx)
	assert.NoError(t, err)
	defer reader.Release()
	assert.That(t, reader.Next())
	rec := reader.Record()
	assert.Equal(t, int64(1), rec.Column(0).(*array.Int64).Value(0))
	assert.Equal(t, int64(1), rec.Column(1).(*array.Int64).Value(0))
	assert.Equal(t, int64(60), rec.Column(2).(*array.Int64).Value(0))
}

// Twenty idle ticks against a real state file. The commit clock moves on
// every tick, the arrival clock never does, and the file does not grow. An
// empty commit that costs bytes would make a quiet pipeline expensive to
// leave running, which is exactly what a slow stream does.
func TestStateDurability_IdleTicksDoNotGrowTheStateFile(t *testing.T) {
	coverage.Covers(t, "state.durability")
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state.db")
	db, err := duckdb.OpenPath(ctx, path)
	assert.NoError(t, err)
	defer db.Close()
	conn, err := db.Connect(ctx)
	assert.NoError(t, err)
	defer conn.Close()

	offsets := NewOffsetStore(conn)
	assert.NoError(t, offsets.Init(ctx))
	progress := NewProgressStore(conn)
	assert.NoError(t, progress.Init(ctx))

	// Every batch is one transaction from here on, exactly as root.go does
	// it: the tables are created under autocommit, then it goes off and the
	// connection's own Commit is the boundary.
	po, ok := conn.(adbc.PostInitOptions)
	assert.That(t, ok)
	assert.NoError(t, po.SetOption(adbc.OptionKeyAutoCommit, adbc.OptionValueDisabled))

	// The same connection the offsets ride, so the progress row commits with
	// them. This is what root.go builds for a pipeline with a state file.
	stateConn, ok := conn.(interface {
		Commit(context.Context) error
		Rollback(context.Context) error
	})
	assert.That(t, ok)

	src := newIdleSource()
	tb := NewTurbine(src, &fakeHandler{}, &fakeSink{}, 1000, 20*time.Millisecond,
		&sync.Mutex{}, PipelineErrorPolicies{},
		WithStateStore(offsets, stateConn), WithProgressStore(progress))
	done := make(chan struct{})
	go func() { _, _ = tb.ConsumeLoop(ctx, 0); close(done) }()

	sizeAfter := func(ticks int64) int64 {
		waitFor(t, fmt.Sprintf("%d idle commits", ticks), 10*time.Second, func() bool {
			return tb.commitCount() >= ticks
		})
		fi, err := os.Stat(path)
		assert.NoError(t, err)
		return fi.Size()
	}
	s1 := sizeAfter(1)
	s20 := sizeAfter(20)

	// Nothing ever arrived, so the arrival clock is still unset while the
	// commit clock has moved twenty times.
	snap := tb.Progress()
	assert.That(t, snap.LastArrival.IsZero())
	assert.That(t, !snap.LastCommit.IsZero())
	assert.Equal(t, int64(0), snap.Messages)

	// One checkpoint of slack: DuckDB may write a WAL frame on the first
	// commit. Nineteen more empty commits must not add another.
	if s20 > s1+256*1024 {
		t.Fatalf("state file grew across idle commits: %d bytes after one, %d after twenty", s1, s20)
	}
	close(src.release)
	<-done
}

// The invariant the throttle broke, and the reason this file exists.
//
// The window predicate closes a bucket when now() - last_arrival exceeds the
// grace. If the table's last_arrival is older than the newest arrival the
// pipeline actually took, that difference is too large and the window closes
// EARLY, while rows for it may still be coming. Early closing is the
// window-splitting defect the stream clock was introduced to remove, so a
// throttle that drops an arrival walks straight back into it.
//
// The dangerous shape is a batch followed by silence: the batch is the last
// thing that will ever arrive, and if its write is skipped for being inside
// the throttle interval, nothing afterwards carries it.
func TestStateDurability_ProgressNeverReportsAnArrivalOlderThanTheNewest(t *testing.T) {
	coverage.Covers(t, "state.durability")
	rec := &progressRecorder{}

	// The ordering is the whole test. An idle tick has to write first, so
	// that the batch's own commit falls inside the throttle interval and a
	// clock-only rule would skip it. A source that delivers immediately
	// makes the batch the first write of all, which is always due, and the
	// bug hides: this test passed against the broken version until the
	// source was paced.
	src := newPacedSource(1, 150*time.Millisecond)

	// An interval far longer than the test, so nothing after the first
	// write is ever due on the clock alone.
	tb := NewTurbine(src, &fakeHandler{}, &fakeSink{}, 1000, 20*time.Millisecond,
		&sync.Mutex{}, PipelineErrorPolicies{},
		WithProgressStore(rec), WithProgressWriteInterval(time.Hour))
	done := make(chan struct{})
	go func() { _, _ = tb.ConsumeLoop(context.Background(), 0); close(done) }()

	// Wait for the batch, then for several idle ticks after it. The commit
	// counter only moves on a state commit and this pipeline has no state
	// database, so the snapshot's own commit clock is the signal.
	waitFor(t, "the batch to be consumed", 5*time.Second, func() bool {
		return tb.Progress().Messages == 1
	})
	afterBatch := tb.Progress().LastCommit
	waitFor(t, "idle ticks after the batch", 5*time.Second, func() bool {
		return tb.Progress().LastCommit.Sub(afterBatch) > 60*time.Millisecond
	})

	// The snapshot is the truth about the newest arrival.
	newest := tb.Progress().LastArrival
	assert.That(t, !newest.IsZero())

	// Every record the table ever received must agree with it or predate
	// it, and the most recent one must equal it. A record carrying an
	// arrival older than the newest is what closes a window early.
	last, n := rec.last()
	assert.That(t, n > 0)
	if !last.LastArrival.Equal(newest) {
		t.Fatalf("the table holds arrival %v while the newest is %v: a window "+
			"reading this closes %v early", last.LastArrival, newest,
			newest.Sub(last.LastArrival))
	}

	rec.mu.Lock()
	defer rec.mu.Unlock()
	for i, r := range rec.recs {
		if r.LastArrival.After(newest) {
			t.Fatalf("record %d reports an arrival from the future: %v > %v", i, r.LastArrival, newest)
		}
	}
	close(src.release)
	<-done
}

// The health endpoint calls a pipeline degraded when it recorded an error
// inside the last interval, so the snapshot has to carry when that was and
// how many there have been.
func TestCoreConsumeLoop_ProgressRecordsTheLastError(t *testing.T) {
	coverage.Covers(t, "core.consume_loop")
	src := &fakeSource{batches: [][]Message{{{Value: []byte("bad")}, {Value: []byte("ok")}}}}
	h := &failingHandler{failWriteOn: "bad"}
	tb := NewTurbine(src, h, &fakeSink{}, 2, time.Second, &sync.Mutex{},
		PipelineErrorPolicies{Policy: PolicyIgnore})

	assert.Equal(t, int64(0), tb.Progress().Errors)
	assert.That(t, tb.Progress().LastError.IsZero())

	before := time.Now().UTC()
	_, err := tb.ConsumeLoop(context.Background(), 0)
	assert.NoError(t, err)

	p := tb.Progress()
	assert.Equal(t, int64(1), p.Errors)
	assert.That(t, !p.LastError.Before(before))
	assert.That(t, !p.LastError.After(time.Now().UTC()))
}

// The engine reads last_arrival in one place, the watermark predicate's
// idle-close branch. Where that reader exists every arrival is owed a write on
// the commit that sees it; where it does not, the write interval governs.
//
// That per-commit UPDATE is what this buys back. Measured on the container
// benchmark against v1.1.0, which had no progress row: about 8 to 9 percent of
// throughput at batch 5000 and about a quarter at batch 500, because the cost
// is per commit and a smaller batch commits more often per message.
//
// Three cases, none of which depends on how fast the machine is. A long
// interval can never come due, so any write past the first is owed. A one
// millisecond interval is always due against a source paced at ten, so every
// commit writes on the clock alone.
func TestCoreConsumeLoop_ArrivalForcesAProgressWriteOnlyForItsReader(t *testing.T) {
	coverage.Covers(t, "core.consume_loop")

	const batches = 6

	run := func(t *testing.T, interval time.Duration, opts ...TurbineOption) (*Turbine, *progressRecorder) {
		t.Helper()
		rec := &progressRecorder{}
		src := newPacedSource(batches, 10*time.Millisecond)
		t.Cleanup(func() { close(src.release) })
		opts = append([]TurbineOption{WithProgressStore(rec), WithProgressWriteInterval(interval)}, opts...)
		tb := NewTurbine(src, &fakeHandler{}, &fakeSink{}, 1, 5*time.Millisecond,
			&sync.Mutex{}, PipelineErrorPolicies{}, opts...)
		done := make(chan struct{})
		go func() { _, _ = tb.ConsumeLoop(context.Background(), batches); close(done) }()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Fatal("the consume loop did not finish")
		}
		return tb, rec
	}

	t.Run("a reader is owed every arrival", func(t *testing.T) {
		tb, rec := run(t, time.Hour)
		last, n := rec.last()
		// The interval never comes due, so every write after the first was
		// forced by an arrival: one per batch at the least.
		if n < batches {
			t.Fatalf("%d batches produced %d writes; an arrival did not force one", batches, n)
		}
		assert.That(t, last.LastArrival.Equal(tb.Progress().LastArrival))
	})

	t.Run("without a reader an arrival forces nothing", func(t *testing.T) {
		_, rec := run(t, time.Hour, WithProgressReadersAbsent())
		// The first commit is always due. Nothing after it can be: the
		// interval is an hour and no arrival is owed.
		if _, n := rec.last(); n != 1 {
			t.Fatalf("opting out wrote %d times inside one interval, want exactly 1", n)
		}
	})

	t.Run("and the interval still keeps the row current", func(t *testing.T) {
		tb, rec := run(t, time.Millisecond, WithProgressReadersAbsent())
		// Opting out is not abandoning the table: the clock alone keeps
		// writing it, well past the first commit.
		if _, n := rec.last(); n < 2 {
			t.Fatalf("a 1ms interval over six paced batches produced %d writes", n)
		}
		// What it does not promise is that the newest arrival is there the
		// instant the loop ends: a commit inside the interval of the write
		// before it is skipped, and nothing is owed. That is the staleness
		// bound, and the drain is what closes it on the way out.
		assert.NoError(t, tb.SyncState(context.Background()))
		last, _ := rec.last()
		assert.That(t, last.LastArrival.Equal(tb.Progress().LastArrival))
	})
}

// flakyProgress fails its first failFirst writes, then records the rest.
type flakyProgress struct {
	mu        sync.Mutex
	failFirst int
	attempts  int
	recs      []Progress
}

func (f *flakyProgress) Record(_ context.Context, p Progress) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.attempts++
	if f.attempts <= f.failFirst {
		return fmt.Errorf("progress store is down")
	}
	f.recs = append(f.recs, p)
	return nil
}

func (f *flakyProgress) state() (attempts int, recs []Progress) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.attempts, append([]Progress(nil), f.recs...)
}

// A store that keeps failing is retried on the write interval, not on every
// commit. Record runs under the connection lock the window managers poll
// under, so a slow failure retried per commit starves the polls and the
// pipeline stops publishing. The reader is present here on purpose: that is
// the arm where an owed arrival would otherwise keep forcing the retry.
func TestStateDurability_AFailingProgressStoreIsRetriedOnTheIntervalNotEveryCommit(t *testing.T) {
	coverage.Covers(t, "state.durability")
	store := &flakyProgress{failFirst: 1 << 30}
	tb := NewTurbine(newIdleSource(), &fakeHandler{}, &fakeSink{}, 1000, time.Hour,
		&sync.Mutex{}, PipelineErrorPolicies{},
		WithProgressStore(store), WithProgressWriteInterval(time.Hour))

	ctx := context.Background()
	for i := 0; i < 50; i++ {
		// Every commit carries a newer arrival, which is what makes a write
		// owed and what a live pipeline always looks like.
		tb.arrivedAt = time.Now().UTC().Add(time.Duration(i) * time.Millisecond)
		tb.recordProgress(ctx, false)
	}
	if n, _ := store.state(); n != 1 {
		t.Fatalf("a failing store was attempted %d times in 50 commits inside one interval", n)
	}
	// And the failure is a recorded error, not only a log line.
	assert.Equal(t, int64(1), tb.Progress().Errors)
}

// A batch, a failed write, then silence and shutdown: no newer arrival is
// coming, so nothing on the commit path will ever carry the one the failed
// write had. The drain forces the write so the managers' final poll reads a
// current arrival clock instead of closing open buckets early on the way out.
func TestStateDurability_TheDrainWritesTheArrivalAFailedWriteLeftBehind(t *testing.T) {
	coverage.Covers(t, "state.durability")
	store := &flakyProgress{failFirst: 1}
	tb := NewTurbine(newIdleSource(), &fakeHandler{}, &fakeSink{}, 1000, time.Hour,
		&sync.Mutex{}, PipelineErrorPolicies{},
		WithProgressStore(store), WithProgressWriteInterval(time.Hour))

	ctx := context.Background()
	arrival := time.Now().UTC()
	tb.arrivedAt = arrival
	tb.recordProgress(ctx, false) // due, and fails

	// Inside the interval a commit must not retry ...
	tb.recordProgress(ctx, false)
	if n, _ := store.state(); n != 1 {
		t.Fatalf("the commit path retried inside the interval: %d attempts", n)
	}

	// ... but the drain must, and what it writes is the arrival left behind.
	assert.NoError(t, tb.SyncState(ctx))
	n, recs := store.state()
	assert.Equal(t, 2, n)
	assert.Equal(t, 1, len(recs))
	assert.That(t, recs[0].LastArrival.Equal(arrival))
}
