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
	// carries a zero arrival, which is how Record says "leave the arrival
	// clock where it was". The commit clock moves on every one of them.
	deadline = time.After(5 * time.Second)
	for {
		p, n := rec.last()
		if n >= 4 {
			assert.That(t, p.LastArrival.IsZero())
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
