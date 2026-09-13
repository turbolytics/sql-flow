package core

// The consume loop returns, and then run syncs state twice on the way out:
// once before the table managers' final poll and once after. Those syncs save
// whatever positions the turbine holds in memory.
//
// Positions advance when a message reaches the handler, before its batch is
// flushed. So a batch whose flush failed left positions in memory for rows
// the sink never took, and the shutdown syncs made them durable. The DuckDB
// driver ignores the context on commit, so no deadline or cancellation
// stopped it. A restart then resumed past rows nothing had written.
//
// These tests drive the loop and then the shutdown exactly as run does, and
// read the durable offsets back from a reopened state file.

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/turbolytics/sql-flow/internal/duckdb"
	"github.com/zeebo/assert"
)

// stateFile is a file-backed state database set up the way run sets one up:
// the tables created under autocommit, then autocommit off so each batch is
// one transaction.
type stateFile struct {
	path    string
	db      *duckdb.DB
	conn    adbc.Connection
	offsets *OffsetStore
	tx      stateTx
}

func openStateFile(t *testing.T) *stateFile {
	t.Helper()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state.db")
	db, err := duckdb.OpenPath(ctx, path)
	assert.NoError(t, err)
	conn, err := db.Connect(ctx)
	assert.NoError(t, err)

	offsets := NewOffsetStore(conn)
	assert.NoError(t, offsets.Init(ctx))

	po, ok := conn.(adbc.PostInitOptions)
	assert.That(t, ok)
	assert.NoError(t, po.SetOption(adbc.OptionKeyAutoCommit, adbc.OptionValueDisabled))
	tx, ok := conn.(stateTx)
	assert.That(t, ok)

	return &stateFile{path: path, db: db, conn: conn, offsets: offsets, tx: tx}
}

// durable closes the database and reads the offsets a restart would resume
// from. Anything not committed is gone once the connection closes.
func (s *stateFile) durable(t *testing.T) *Marks {
	t.Helper()
	ctx := context.Background()
	assert.NoError(t, s.conn.Close())
	assert.NoError(t, s.db.Close())

	db, err := duckdb.OpenPath(ctx, s.path)
	assert.NoError(t, err)
	defer db.Close()
	conn, err := db.Connect(ctx)
	assert.NoError(t, err)
	defer conn.Close()

	marks, err := NewOffsetStore(conn).Load(ctx)
	assert.NoError(t, err)
	return marks
}

// shutdown is run's deferred sequence after the loop returns: sync before the
// managers' final poll, sync after it.
func shutdown(t *testing.T, tb *Turbine) {
	t.Helper()
	_ = tb.SyncState(context.Background())
	_ = tb.SyncState(context.Background())
}

// failingAfterSink accepts the first `ok` flushes and refuses every one after.
type failingAfterSink struct {
	mu      sync.Mutex
	ok      int
	flushes int
	rows    int64
	pending int64
}

func (s *failingAfterSink) WriteTable(_ context.Context, batch arrow.Table) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if batch != nil {
		s.pending += batch.NumRows()
	}
	return nil
}

func (s *failingAfterSink) Flush(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.flushes++
	if s.flushes > s.ok {
		return errors.New("connection reset by peer")
	}
	s.rows += s.pending
	s.pending = 0
	return nil
}

// A batch the sink refused leaves the durable offset at the last batch the
// sink took, through the shutdown syncs. Before the fix the second sync saved
// offset 9 for rows the sink never received.
func TestStateOffsets_AFailedBatchIsNotCommittedByTheShutdown(t *testing.T) {
	coverage.Covers(t, "state.offsets")
	st := openStateFile(t)

	src := &fakeSource{batches: [][]Message{
		kafkaMessages("events", 0, 0, 5),
		kafkaMessages("events", 0, 5, 5),
	}}
	sink := &failingAfterSink{ok: 1}
	tb := NewTurbine(src, &fakeHandler{}, sink, 5, time.Hour, &sync.Mutex{},
		PipelineErrorPolicies{}, WithStateStore(st.offsets, st.tx))

	_, err := tb.ConsumeLoop(context.Background(), 0)
	assert.Error(t, err)
	shutdown(t, tb)

	mark, ok := st.durable(t).Get("events", 0)
	assert.That(t, ok)
	assert.Equal(t, int64(4), mark.Offset)
	assert.Equal(t, int64(5), sink.rows)
}

// The first batch failing has no earlier position to fall back to, and the
// shutdown must not invent one.
func TestStateOffsets_AFirstBatchThatFailsCommitsNoOffsetAtAll(t *testing.T) {
	coverage.Covers(t, "state.offsets")
	st := openStateFile(t)

	src := &fakeSource{batches: [][]Message{kafkaMessages("events", 0, 0, 5)}}
	tb := NewTurbine(src, &fakeHandler{}, &failingAfterSink{ok: 0}, 5, time.Hour,
		&sync.Mutex{}, PipelineErrorPolicies{}, WithStateStore(st.offsets, st.tx))

	_, err := tb.ConsumeLoop(context.Background(), 0)
	assert.Error(t, err)
	shutdown(t, tb)

	_, ok := st.durable(t).Get("events", 0)
	assert.That(t, !ok)
}

// cancellingHandler cancels the run when it receives its nth message, which
// puts the cancel exactly on a batch boundary.
type cancellingHandler struct {
	fakeHandler
	at     int
	seen   int
	cancel context.CancelFunc
}

func (h *cancellingHandler) Write(msg []byte) error {
	h.seen++
	if h.seen == h.at {
		h.cancel()
	}
	return h.fakeHandler.Write(msg)
}

// A cancel that lands as a batch fills used to return without writing the
// batch, report a clean stop, and leave its positions for the shutdown to
// commit. The batch is drained now: the sink takes it, then its offsets
// commit.
func TestLifecycleDrain_ACancelOnABatchBoundaryDrainsTheBatch(t *testing.T) {
	coverage.Covers(t, "lifecycle.drain")
	st := openStateFile(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	src := &fakeSource{batches: [][]Message{kafkaMessages("events", 0, 0, 5)}}
	sink := &failingAfterSink{ok: 1}
	h := &cancellingHandler{at: 5, cancel: cancel}
	tb := NewTurbine(src, h, sink, 5, time.Hour, &sync.Mutex{},
		PipelineErrorPolicies{}, WithStateStore(st.offsets, st.tx))

	_, err := tb.ConsumeLoop(ctx, 0)
	assert.NoError(t, err)
	shutdown(t, tb)

	assert.Equal(t, int64(5), sink.rows)
	mark, ok := st.durable(t).Get("events", 0)
	assert.That(t, ok)
	assert.Equal(t, int64(4), mark.Offset)
}

// The same boundary cancel against a sink that refuses the drain: the loop
// reports the failure, and nothing the sink did not take becomes durable.
func TestLifecycleDrain_ABoundaryDrainThatFailsCommitsNothing(t *testing.T) {
	coverage.Covers(t, "lifecycle.drain")
	st := openStateFile(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	src := &fakeSource{batches: [][]Message{kafkaMessages("events", 0, 0, 5)}}
	h := &cancellingHandler{at: 5, cancel: cancel}
	tb := NewTurbine(src, h, &failingAfterSink{ok: 0}, 5, time.Hour, &sync.Mutex{},
		PipelineErrorPolicies{}, WithStateStore(st.offsets, st.tx))

	_, err := tb.ConsumeLoop(ctx, 0)
	assert.Error(t, err)
	shutdown(t, tb)

	_, ok := st.durable(t).Get("events", 0)
	assert.That(t, !ok)
}
