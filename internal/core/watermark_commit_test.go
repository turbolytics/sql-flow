package core

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/turbolytics/sql-flow/internal/duckdb"
	"github.com/turbolytics/sql-flow/internal/errs"
	"github.com/zeebo/assert"
)

// How the engine asserts the watermark: in the commit, beside the rows it
// describes, and nowhere else. These drive commitState the way the loop
// does, with the tracker's clock and the turbine's the same fake.

// windowedState is a state connection with the engine's tables and one
// window table, autocommit off, plus a reader on another connection that
// sees only what has committed.
type windowedState struct {
	db     *duckdb.DB
	conn   adbc.Connection
	reader adbc.Connection
	tx     stateTx
	offs   *OffsetStore
}

func openWindowedState(t *testing.T) *windowedState {
	t.Helper()
	ctx := context.Background()
	db, err := duckdb.OpenPath(ctx, "")
	assert.NoError(t, err)
	conn, err := db.Connect(ctx)
	assert.NoError(t, err)
	reader, err := db.Connect(ctx)
	assert.NoError(t, err)
	t.Cleanup(func() { reader.Close(); conn.Close(); db.Close() })

	// run's order: the engine's tables and the handler's under autocommit,
	// then autocommit off so each batch is one transaction.
	offs := NewOffsetStore(conn)
	assert.NoError(t, offs.Init(ctx))
	assert.NoError(t, NewProgressStore(conn).Init(ctx))
	assert.NoError(t, NewWatermarkStore(conn).Init(ctx))
	exec(t, conn, `CREATE TABLE win (bucket TIMESTAMPTZ, n BIGINT)`)
	po, ok := conn.(adbc.PostInitOptions)
	assert.That(t, ok)
	assert.NoError(t, po.SetOption(adbc.OptionKeyAutoCommit, adbc.OptionValueDisabled))
	tx, ok := conn.(stateTx)
	assert.That(t, ok)
	return &windowedState{db: db, conn: conn, reader: reader, tx: tx, offs: offs}
}

// committedWatermark is what a reader on another connection sees.
func (s *windowedState) committedWatermark(t *testing.T) (time.Time, bool) {
	t.Helper()
	at, ok, err := LoadWatermark(context.Background(), s.reader, "win")
	assert.NoError(t, err)
	return at, ok
}

// failingSaver refuses its first write, then delegates.
type failingSaver struct {
	WatermarkSaver
	mu    sync.Mutex
	fails int
}

func (f *failingSaver) Save(ctx context.Context, name string, at time.Time) error {
	f.mu.Lock()
	fail := f.fails > 0
	if fail {
		f.fails--
	}
	f.mu.Unlock()
	if fail {
		return errors.New("disk full")
	}
	return f.WatermarkSaver.Save(ctx, name, at)
}

// The watermark rides the batch's transaction. Before the commit a reader
// sees neither the rows nor the assertion; after it, both. A commit that
// fails leaves the row where it was and the tracker where it was, so the
// replayed batch asserts the same value again: there is no instant at which
// rows are visible without the watermark that accounts for them, which is
// what makes #374 unrepresentable rather than fixed.
func TestStateDurability_TheWatermarkRidesTheStateTransaction(t *testing.T) {
	coverage.Covers(t, "state.durability")
	ctx := context.Background()
	st := openWindowedState(t)
	clk := &wmClock{at: wmT0}
	w := NewWatermarks([]WindowSpec{{Name: "win", Size: time.Minute, Grace: time.Minute}}, clk.now)
	saver := &failingSaver{WatermarkSaver: NewWatermarkStore(st.conn)}
	tb := NewTurbine(newIdleSource(), &fakeHandler{}, &fakeSink{}, 1000, time.Hour,
		&sync.Mutex{}, PipelineErrorPolicies{},
		WithStateStore(st.offs, st.tx), WithProgressStore(NewProgressStore(st.conn)),
		WithWindows(w, saver), WithWatermarkWriteInterval(0), WithClock(clk.now))

	// The batch: a row in the window table, inside the open transaction,
	// and the record it came from observed.
	exec(t, st.conn, `INSERT INTO win VALUES (TIMESTAMPTZ '2026-09-25 12:05:00+00:00', 1)`)
	w.Observe("", 0, wmT0.Add(5*time.Minute).UnixNano())
	_, ok := st.committedWatermark(t)
	assert.That(t, !ok)

	assert.NoError(t, tb.commitState(ctx, progressOnInterval))
	at, ok := st.committedWatermark(t)
	assert.That(t, ok)
	assert.Equal(t, wmT0.Add(4*time.Minute), at)
	assert.Equal(t, int64(1), countCommitted(t, st.db, "win"))

	// The next batch's commit fails at the watermark write. The rows roll
	// back with it, the row on disk still says 12:04, and so does the
	// tracker: nothing was asserted.
	exec(t, st.conn, `INSERT INTO win VALUES (TIMESTAMPTZ '2026-09-25 12:09:00+00:00', 1)`)
	w.Observe("", 0, wmT0.Add(9*time.Minute).UnixNano())
	saver.fails = 1
	err := tb.commitState(ctx, progressOnInterval)
	assert.Equal(t, errs.CodeStateCommitFailed, errs.CodeOf(err))
	at, _ = st.committedWatermark(t)
	assert.Equal(t, wmT0.Add(4*time.Minute), at)
	assert.Equal(t, int64(1), countCommitted(t, st.db, "win"))
	asserted, _ := w.Asserted("win")
	assert.Equal(t, wmT0.Add(4*time.Minute), asserted)

	// The replay: the same row, the same observation, and this time the
	// assertion lands with it.
	exec(t, st.conn, `INSERT INTO win VALUES (TIMESTAMPTZ '2026-09-25 12:09:00+00:00', 1)`)
	w.Observe("", 0, wmT0.Add(9*time.Minute).UnixNano())
	assert.NoError(t, tb.commitState(ctx, progressOnInterval))
	at, _ = st.committedWatermark(t)
	assert.Equal(t, wmT0.Add(8*time.Minute), at)
	assert.Equal(t, int64(2), countCommitted(t, st.db, "win"))
}

// An idle tick writes no progress row, and asserts a watermark only when a
// partition went idle since the last commit: a quiet pipeline writes
// nothing, and a stream that stops closes once.
func TestStateDurability_AnIdleTickAssertsOnlyWhenAPartitionWentIdle(t *testing.T) {
	coverage.Covers(t, "state.durability")
	ctx := context.Background()
	db, err := duckdb.OpenPath(ctx, "")
	assert.NoError(t, err)
	defer db.Close()
	conn, err := db.Connect(ctx)
	assert.NoError(t, err)
	defer conn.Close()
	store := NewWatermarkStore(conn)
	assert.NoError(t, store.Init(ctx))

	clk := &wmClock{at: wmT0}
	rec := &progressRecorder{}
	w := NewWatermarks([]WindowSpec{{Name: "win", Size: time.Minute, Grace: time.Minute, IdleClose: 10 * time.Second}}, clk.now)
	tb := NewTurbine(newIdleSource(), &fakeHandler{}, &fakeSink{}, 1000, time.Hour,
		&sync.Mutex{}, PipelineErrorPolicies{},
		WithProgressStore(rec), WithProgressWriteInterval(0),
		WithWindows(w, store), WithWatermarkWriteInterval(0), WithClock(clk.now))

	// A batch: the progress row and the watermark, both written.
	w.Observe("", 0, wmT0.Add(5*time.Minute).UnixNano())
	assert.NoError(t, tb.commitState(ctx, progressOnInterval))
	at, ok, err := store.Load(ctx, "win")
	assert.NoError(t, err)
	assert.That(t, ok)
	assert.Equal(t, wmT0.Add(4*time.Minute), at)
	_, n := rec.last()
	assert.Equal(t, 1, n)

	// An idle tick with nothing changed: no progress write, no assertion.
	assert.NoError(t, tb.commitState(ctx, progressSkipped))
	at, _, _ = store.Load(ctx, "win")
	assert.Equal(t, wmT0.Add(4*time.Minute), at)
	_, n = rec.last()
	assert.Equal(t, 1, n)

	// The tick after the partition has been silent for the bound: the
	// stream is done with what it has, and the window closes through the
	// newest row's bucket. Still no progress write.
	clk.tick(11 * time.Second)
	assert.NoError(t, tb.commitState(ctx, progressSkipped))
	at, _, _ = store.Load(ctx, "win")
	assert.Equal(t, wmT0.Add(6*time.Minute), at)
	_, n = rec.last()
	assert.Equal(t, 1, n)

	// And the drain forces the progress write, and asserts as it stands.
	assert.NoError(t, tb.SyncState(ctx))
	_, n = rec.last()
	assert.Equal(t, 2, n)
}

// deliveringSource is an idle source that says whether it can deliver.
type deliveringSource struct {
	*idleSource
	mu sync.Mutex
	ok bool
}

func (s *deliveringSource) set(ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ok = ok
}

func (s *deliveringSource) Delivering() (time.Duration, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return 0, s.ok
}

// A source that cannot deliver holds its windows: its one partition is
// lost, not idle, through the outage, and idleness is measured again from
// the resumption. A websocket reconnecting for longer than idle_close is not
// a quiet stream.
func TestStateDurability_ASourceThatCannotDeliverHoldsItsWindows(t *testing.T) {
	coverage.Covers(t, "state.durability")
	ctx := context.Background()
	db, err := duckdb.OpenPath(ctx, "")
	assert.NoError(t, err)
	defer db.Close()
	conn, err := db.Connect(ctx)
	assert.NoError(t, err)
	defer conn.Close()
	store := NewWatermarkStore(conn)
	assert.NoError(t, store.Init(ctx))

	clk := &wmClock{at: wmT0}
	src := &deliveringSource{idleSource: newIdleSource(), ok: true}
	w := NewWatermarks([]WindowSpec{{Name: "win", Size: time.Minute, Grace: time.Minute, IdleClose: 10 * time.Second}}, clk.now)
	tb := NewTurbine(src, &fakeHandler{}, &fakeSink{}, 1000, time.Hour,
		&sync.Mutex{}, PipelineErrorPolicies{},
		WithWindows(w, store), WithWatermarkWriteInterval(0), WithClock(clk.now))

	w.Observe("", 0, wmT0.Add(5*time.Minute).UnixNano())
	assert.NoError(t, tb.commitState(ctx, progressOnInterval))
	at, _, _ := store.Load(ctx, "win")
	assert.Equal(t, wmT0.Add(4*time.Minute), at)

	// An hour without the source, with the loop ticking: held.
	src.set(false)
	clk.tick(time.Hour)
	assert.NoError(t, tb.commitState(ctx, progressSkipped))
	at, _, _ = store.Load(ctx, "win")
	assert.Equal(t, wmT0.Add(4*time.Minute), at)

	// Back for three seconds: not idle yet. Back for eleven: idle, closed.
	src.set(true)
	clk.tick(3 * time.Second)
	assert.NoError(t, tb.commitState(ctx, progressSkipped))
	at, _, _ = store.Load(ctx, "win")
	assert.Equal(t, wmT0.Add(4*time.Minute), at)
	clk.tick(11 * time.Second)
	assert.NoError(t, tb.commitState(ctx, progressSkipped))
	at, _, _ = store.Load(ctx, "win")
	assert.Equal(t, wmT0.Add(6*time.Minute), at)
}

// Through the loop: a record the engine cannot place never reaches the
// tracker, so it moves no watermark; the placed record beside it does.
func TestCoreConsumeLoop_AnUnplaceableRecordMovesNoWatermark(t *testing.T) {
	coverage.Covers(t, "core.consume_loop")
	ctx := context.Background()
	db, err := duckdb.OpenPath(ctx, "")
	assert.NoError(t, err)
	defer db.Close()
	conn, err := db.Connect(ctx)
	assert.NoError(t, err)
	defer conn.Close()
	store := NewWatermarkStore(conn)
	assert.NoError(t, store.Init(ctx))

	placed := time.Now().Add(-time.Hour).Truncate(time.Microsecond)
	src := newBlockingSource([]Message{
		{Value: []byte(`{"a":1}`), EventAtNanos: placed.UnixNano()},
		{Value: []byte(`{"a":2}`), EventAtNanos: time.Date(2099, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano()},
	})
	w := NewWatermarks([]WindowSpec{{Name: "win", Size: time.Minute, Grace: time.Minute}}, nil)
	tb := NewTurbine(src, &fakeHandler{}, &fakeSink{}, 1000, 20*time.Millisecond,
		&sync.Mutex{}, PipelineErrorPolicies{},
		WithEventTimePlacement(true), WithWindows(w, store), WithWatermarkWriteInterval(0))
	done := make(chan struct{})
	go func() { _, _ = tb.ConsumeLoop(ctx, 0); close(done) }()

	waitFor(t, "the batch's watermark", 5*time.Second, func() bool {
		_, ok := w.Asserted("win")
		return ok
	})
	at, _ := w.Asserted("win")
	assert.Equal(t, placed.Add(-time.Minute).UTC(), at)
	stored, ok, err := store.Load(ctx, "win")
	assert.NoError(t, err)
	assert.That(t, ok)
	assert.Equal(t, placed.Add(-time.Minute).UTC(), stored)
	close(src.release)
	<-done
}

// The write is paced, and a skip is safe. Two commits inside one interval
// write once, so a pipeline committing two hundred times a second pays one
// statement a second rather than two hundred; the watermark is then older
// than the rows it describes, which delays a close and can never bring one
// forward. The drain forces the write whatever the pace, so a clean stop
// asserts where the stream actually got to.
func TestStateDurability_TheWatermarkWriteIsPaced(t *testing.T) {
	coverage.Covers(t, "state.durability")
	ctx := context.Background()
	db, err := duckdb.OpenPath(ctx, "")
	assert.NoError(t, err)
	defer db.Close()
	conn, err := db.Connect(ctx)
	assert.NoError(t, err)
	defer conn.Close()
	store := NewWatermarkStore(conn)
	assert.NoError(t, store.Init(ctx))
	counting := &countingSaver{inner: store}

	clk := &wmClock{at: wmT0}
	w := NewWatermarks([]WindowSpec{{Name: "win", Size: time.Minute, Grace: time.Minute}}, clk.now)
	tb := NewTurbine(newIdleSource(), &fakeHandler{}, &fakeSink{}, 1000, time.Hour,
		&sync.Mutex{}, PipelineErrorPolicies{},
		WithWindows(w, counting), WithWatermarkWriteInterval(time.Second), WithClock(clk.now))

	// The first commit is always due.
	w.Observe("", 0, wmT0.Add(5*time.Minute).UnixNano())
	assert.NoError(t, tb.commitState(ctx, progressOnInterval))
	assert.Equal(t, 1, counting.count())
	at, _, _ := store.Load(ctx, "win")
	assert.Equal(t, wmT0.Add(4*time.Minute), at)

	// Two more inside the interval: the stream moves on and the row does not.
	clk.tick(100 * time.Millisecond)
	w.Observe("", 0, wmT0.Add(6*time.Minute).UnixNano())
	assert.NoError(t, tb.commitState(ctx, progressOnInterval))
	clk.tick(100 * time.Millisecond)
	w.Observe("", 0, wmT0.Add(7*time.Minute).UnixNano())
	assert.NoError(t, tb.commitState(ctx, progressOnInterval))
	assert.Equal(t, 1, counting.count())
	at, _, _ = store.Load(ctx, "win")
	assert.Equal(t, wmT0.Add(4*time.Minute), at)
	// And the tracker has not recorded what it did not write, so the next
	// write carries wherever the stream has got to rather than the value it
	// skipped.
	asserted, _ := w.Asserted("win")
	assert.Equal(t, wmT0.Add(4*time.Minute), asserted)

	// Past the interval: one write, carrying the newest.
	clk.tick(time.Second)
	assert.NoError(t, tb.commitState(ctx, progressOnInterval))
	assert.Equal(t, 2, counting.count())
	at, _, _ = store.Load(ctx, "win")
	assert.Equal(t, wmT0.Add(6*time.Minute), at)

	// The drain forces it, inside the interval.
	clk.tick(10 * time.Millisecond)
	w.Observe("", 0, wmT0.Add(9*time.Minute).UnixNano())
	assert.NoError(t, tb.SyncState(ctx))
	assert.Equal(t, 3, counting.count())
	at, _, _ = store.Load(ctx, "win")
	assert.Equal(t, wmT0.Add(8*time.Minute), at)
}

// countingSaver counts the writes it passes on.
type countingSaver struct {
	inner WatermarkSaver
	mu    sync.Mutex
	n     int
}

func (c *countingSaver) Save(ctx context.Context, name string, at time.Time) error {
	c.mu.Lock()
	c.n++
	c.mu.Unlock()
	return c.inner.Save(ctx, name, at)
}

func (c *countingSaver) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.n
}
