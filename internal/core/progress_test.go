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
	"github.com/turbolytics/sql-flow/internal/errs"
	"github.com/zeebo/assert"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
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

	// Now the source is silent. Idle ticks move the commit clock in the
	// snapshot, which is what /healthz and /stats report, and keep the
	// arrival the batch had; they write nothing to the table, because
	// nothing reads it between batches and a quiet pipeline should write
	// nothing.
	deadline = time.After(5 * time.Second)
	for {
		snap := tb.Progress()
		if snap.LastCommit.After(afterBatch.LastCommit) {
			assert.Equal(t, afterBatch.LastArrival, snap.LastArrival)
			assert.Equal(t, int64(3), snap.Messages)
			break
		}
		select {
		case <-deadline:
			t.Fatal("idle ticks did not move the snapshot's commit clock")
		default:
			time.Sleep(5 * time.Millisecond)
		}
	}
	_, n := rec.last()
	assert.Equal(t, 1, n)
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
// every tick, the arrival clock never does, and the file and its WAL grow by
// no more than the progress row's update. A commit that cost more would make
// a quiet pipeline expensive to leave running, which is exactly what a slow
// stream does.
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

	// The file and its WAL together: the row's update lands in the WAL
	// until a checkpoint folds it in, and a test that stats the file alone
	// cannot see what each tick costs.
	sizeAfter := func(ticks int64) int64 {
		waitFor(t, fmt.Sprintf("%d idle commits", ticks), 10*time.Second, func() bool {
			return tb.commitCount() >= ticks
		})
		var total int64
		for _, name := range []string{path, path + ".wal"} {
			if fi, err := os.Stat(name); err == nil {
				total += fi.Size()
			}
		}
		return total
	}
	s1 := sizeAfter(1)
	s20 := sizeAfter(20)

	// Nothing ever arrived, so the arrival clock is where the loop started
	// while the commit clock has moved twenty times: the quiet the row
	// confirms is only what this process has watched.
	snap := tb.Progress()
	assert.That(t, !snap.LastArrival.IsZero())
	assert.That(t, !snap.LastArrival.After(snap.LastCommit))
	assert.That(t, !snap.LastCommit.IsZero())
	assert.Equal(t, int64(0), snap.Messages)

	// Nineteen more ticks are nineteen updates of one row: under half a
	// kilobyte of WAL each, measured, so 64 KiB is a generous bound. The
	// first commit may also write a checkpoint, which s1 absorbs.
	if s20 > s1+64*1024 {
		t.Fatalf("state grew %d bytes across nineteen idle commits: %d after one, %d after twenty", s20-s1, s1, s20)
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

// The progress row is written on the write interval, and only there: an
// arrival never forces a write ahead of it. Every commit under load used to,
// which was a statement per batch, about 8 to 9 percent of throughput at
// batch 5000 and a quarter at batch 500 on the container benchmark. The
// interval is enough because the idle close reads the row's two clocks
// against each other, so a write that lands late is a late close and never
// an early one.
//
// Two cases, neither depending on how fast the machine is. An hour-long
// interval makes only the first commit due, so a stream of batches produces
// exactly one write however many arrive. A one millisecond interval is
// always due against a source paced at ten, so every commit writes.
func TestCoreConsumeLoop_TheProgressWriteIntervalGoverns(t *testing.T) {
	coverage.Covers(t, "core.consume_loop")

	const batches = 6

	run := func(t *testing.T, interval time.Duration) *progressRecorder {
		t.Helper()
		rec := &progressRecorder{}
		src := newPacedSource(batches, 10*time.Millisecond)
		t.Cleanup(func() { close(src.release) })
		tb := NewTurbine(src, &fakeHandler{}, &fakeSink{}, 1, 5*time.Millisecond,
			&sync.Mutex{}, PipelineErrorPolicies{},
			WithProgressStore(rec), WithProgressWriteInterval(interval))
		done := make(chan struct{})
		go func() { _, _ = tb.ConsumeLoop(context.Background(), batches); close(done) }()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Fatal("the consume loop did not finish")
		}
		return rec
	}

	t.Run("an arrival does not force a write", func(t *testing.T) {
		rec := run(t, time.Hour)
		if _, n := rec.last(); n != 1 {
			t.Fatalf("%d batches inside one interval produced %d writes, want exactly 1: "+
				"an arrival forced a write ahead of the interval", batches, n)
		}
	})

	t.Run("and the interval keeps the row current", func(t *testing.T) {
		rec := run(t, time.Millisecond)
		if _, n := rec.last(); n < 2 {
			t.Fatalf("a 1ms interval over %d paced batches produced %d writes", batches, n)
		}
	})
}

// The interval leaves a staleness bound: a commit inside the interval of the
// write before it is skipped, so the newest arrival can be missing from the
// table when the stream stops. The drain closes that bound on the way out,
// and what it writes is the arrival at its true time, not the drain's. Driven
// commit by commit, so the skip is certain rather than a matter of timing.
func TestStateDurability_TheDrainWritesAnArrivalTheIntervalSkipped(t *testing.T) {
	coverage.Covers(t, "state.durability")
	rec := &progressRecorder{}
	tb := NewTurbine(newIdleSource(), &fakeHandler{}, &fakeSink{}, 1000, time.Hour,
		&sync.Mutex{}, PipelineErrorPolicies{},
		WithProgressStore(rec), WithProgressWriteInterval(time.Hour))

	ctx := context.Background()
	first := time.Now().UTC()
	tb.lastArrival = first
	assert.NoError(t, tb.commitState(ctx, progressOnInterval)) // always due: writes `first`

	newest := first.Add(time.Second)
	tb.lastArrival = newest
	assert.NoError(t, tb.commitState(ctx, progressOnInterval)) // inside the interval, not owed

	last, n := rec.last()
	assert.Equal(t, 1, n)
	assert.That(t, last.LastArrival.Equal(first)) // the table is behind

	assert.NoError(t, tb.SyncState(ctx))
	last, n = rec.last()
	assert.Equal(t, 2, n)
	assert.That(t, last.LastArrival.Equal(newest))
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

// errorCodes returns how many errors error_count recorded under each code,
// and under each code and phase as "code@phase".
func errorCodes(t *testing.T, r *sdkmetric.ManualReader) map[string]int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	assert.NoError(t, r.Collect(context.Background(), &rm))
	codes := map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "error_count" {
				continue
			}
			data, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				continue
			}
			for _, dp := range data.DataPoints {
				code, ok := dp.Attributes.Value("code")
				if !ok {
					continue
				}
				codes[code.AsString()] += dp.Value
				if phase, ok := dp.Attributes.Value("phase"); ok {
					codes[code.AsString()+"@"+phase.AsString()] += dp.Value
				}
			}
		}
	}
	return codes
}

// A failing store is retried on the write interval, and the row has the
// newest arrival again on the first write that succeeds. Until then the row
// confirms no quiet, so no window closes on it: the failure is in the late
// direction. Without a state path the write autocommits by itself, so the
// failure does not touch the batch; it is recorded under its own code, so an
// alert can tell a stalled liveness row from a state commit that failed.
func TestStateDurability_AFailingProgressStoreRecoversOnItsFirstSuccessfulWrite(t *testing.T) {
	coverage.Covers(t, "state.durability")
	r := sdkmetric.NewManualReader()
	m, err := NewMetrics(sdkmetric.NewMeterProvider(sdkmetric.WithReader(r)))
	assert.NoError(t, err)

	store := &flakyProgress{failFirst: 3}
	// A zero interval: every commit is due, so this is about what a failure
	// does to the next write and not about pacing, which the next test pins.
	tb := NewTurbine(newIdleSource(), &fakeHandler{}, &fakeSink{}, 1000, time.Hour,
		&sync.Mutex{}, PipelineErrorPolicies{},
		WithMetrics(m), WithProgressStore(store), WithProgressWriteInterval(0))

	ctx := context.Background()
	var newest time.Time
	for i := 0; i < 6; i++ {
		newest = time.Now().UTC().Add(time.Duration(i) * time.Millisecond)
		tb.lastArrival = newest
		// Stateless, so the commit itself succeeds whatever the write did.
		assert.NoError(t, tb.commitState(ctx, progressOnInterval))
	}

	attempts, recs := store.state()
	assert.Equal(t, 6, attempts)
	assert.Equal(t, 3, len(recs))
	assert.That(t, recs[len(recs)-1].LastArrival.Equal(newest))

	codes := errorCodes(t, r)
	assert.Equal(t, int64(3), codes[string(errs.CodeProgressWriteFailed)])
	assert.Equal(t, int64(0), codes[string(errs.CodeStateCommitFailed)])
	// Under the commit phase: the write is part of the commit, and an alert
	// on the phase must find it there rather than under the sink's flush.
	assert.Equal(t, int64(3), codes[string(errs.CodeProgressWriteFailed)+"@"+phaseStateCommit])
	assert.Equal(t, int64(3), tb.Progress().Errors)
}

// The write interval paces a failing store: the throttle advances on every
// attempt, so a store that keeps failing is tried once an interval and not on
// every commit, however many arrivals those commits carry.
func TestStateDurability_AFailingProgressStoreIsRetriedOnTheInterval(t *testing.T) {
	coverage.Covers(t, "state.durability")
	store := &flakyProgress{failFirst: 1 << 30}
	tb := NewTurbine(newIdleSource(), &fakeHandler{}, &fakeSink{}, 1000, time.Hour,
		&sync.Mutex{}, PipelineErrorPolicies{},
		WithProgressStore(store), WithProgressWriteInterval(time.Hour))

	ctx := context.Background()
	for i := 0; i < 50; i++ {
		tb.lastArrival = time.Now().UTC().Add(time.Duration(i) * time.Millisecond)
		assert.NoError(t, tb.commitState(ctx, progressOnInterval))
	}
	if n, _ := store.state(); n != 1 {
		t.Fatalf("a failing store was attempted %d times in 50 commits inside one interval", n)
	}
}

// A batch, a failed write, then silence and shutdown. The next interval
// write would carry the arrival, but the signal comes first. The drain forces
// the write, so the managers' final poll sees the arrival and the quiet since
// it rather than a row that confirms nothing.
func TestStateDurability_TheDrainWritesTheArrivalAFailedWriteLeftBehind(t *testing.T) {
	coverage.Covers(t, "state.durability")
	store := &flakyProgress{failFirst: 1}
	tb := NewTurbine(newIdleSource(), &fakeHandler{}, &fakeSink{}, 1000, time.Hour,
		&sync.Mutex{}, PipelineErrorPolicies{},
		WithProgressStore(store), WithProgressWriteInterval(time.Hour))

	ctx := context.Background()
	arrival := time.Now().UTC()
	tb.lastArrival = arrival
	assert.NoError(t, tb.commitState(ctx, progressOnInterval)) // due, and the write fails

	// Silence: an idle commit sees no newer arrival and sits inside the
	// interval, so it does not write.
	assert.NoError(t, tb.commitState(ctx, progressOnInterval))
	if n, _ := store.state(); n != 1 {
		t.Fatalf("an idle commit inside the interval wrote: %d attempts", n)
	}

	// The drain does, and what it writes is the arrival left behind.
	assert.NoError(t, tb.SyncState(ctx))
	n, recs := store.state()
	assert.Equal(t, 2, n)
	assert.Equal(t, 1, len(recs))
	assert.That(t, recs[0].LastArrival.Equal(arrival))
}

// refusingProgress is a progress write the database refuses, run on the
// batch's own connection the way ProgressStore's is. A NOT NULL violation is
// one of the failures DuckDB answers by aborting the open transaction.
type refusingProgress struct{ conn adbc.Connection }

func (r refusingProgress) Record(ctx context.Context, _ Progress) error {
	stmt, err := r.conn.NewStatement()
	if err != nil {
		return err
	}
	defer stmt.Close()
	if err := stmt.SetSqlQuery(`UPDATE ` + progressTable + ` SET messages = NULL`); err != nil {
		return err
	}
	_, err = stmt.ExecuteUpdate(ctx)
	return err
}

// countCommitted counts a table's rows from a second connection, which sees
// committed rows only.
func countCommitted(t *testing.T, db *duckdb.DB, table string) int64 {
	t.Helper()
	ctx := context.Background()
	conn, err := db.Connect(ctx)
	assert.NoError(t, err)
	defer conn.Close()
	stmt, err := conn.NewStatement()
	assert.NoError(t, err)
	defer stmt.Close()
	assert.NoError(t, stmt.SetSqlQuery(`SELECT count(*)::BIGINT FROM `+table))
	reader, _, err := stmt.ExecuteQuery(ctx)
	assert.NoError(t, err)
	defer reader.Release()
	assert.That(t, reader.Next())
	col, ok := reader.Record().Column(0).(*array.Int64)
	assert.That(t, ok)
	return col.Value(0)
}

// With a state path the progress UPDATE runs inside the batch's transaction,
// and a statement DuckDB refuses aborts that transaction. A source with no
// offsets, such as a webhook, then has nothing left to write before Commit,
// and Commit on the aborted transaction reports success while keeping none of
// the batch. The rows are gone and nothing says so.
//
// commitState promises the opposite: a batch it cannot make durable is
// replayed rather than lost. So the refused write must fail the commit.
func TestStateDurability_AProgressWriteTheStateTransactionRefusesFailsTheCommit(t *testing.T) {
	coverage.Covers(t, "state.durability")
	ctx := context.Background()

	db, err := duckdb.OpenPath(ctx, filepath.Join(t.TempDir(), "state.db"))
	assert.NoError(t, err)
	defer db.Close()
	conn, err := db.Connect(ctx)
	assert.NoError(t, err)
	defer conn.Close()

	// run's order: the engine's tables and the handler's under autocommit,
	// then autocommit off so each batch is one transaction.
	offsets := NewOffsetStore(conn)
	assert.NoError(t, offsets.Init(ctx))
	assert.NoError(t, NewProgressStore(conn).Init(ctx))
	exec(t, conn, `CREATE TABLE out (v INTEGER)`)
	po, ok := conn.(adbc.PostInitOptions)
	assert.That(t, ok)
	assert.NoError(t, po.SetOption(adbc.OptionKeyAutoCommit, adbc.OptionValueDisabled))
	tx, ok := conn.(stateTx)
	assert.That(t, ok)

	tb := NewTurbine(newIdleSource(), &fakeHandler{}, &fakeSink{}, 1000, time.Hour,
		&sync.Mutex{}, PipelineErrorPolicies{},
		WithStateStore(offsets, tx), WithProgressStore(refusingProgress{conn}))

	// The batch: the handler's write inside the open transaction, and no
	// marks, as from a source that has no offsets.
	exec(t, conn, `INSERT INTO out VALUES (1)`)
	tb.lastArrival = time.Now().UTC()

	err = tb.commitState(ctx, progressOnInterval)
	if err == nil && countCommitted(t, db, "out") == 0 {
		t.Fatal("commitState reported success and the batch's row is not durable: " +
			"the batch was discarded without an error")
	}
	assert.Error(t, err)
	assert.Equal(t, errs.CodeStateCommitFailed, errs.CodeOf(err))

	// The failed commit rolled its transaction back, so the connection is
	// not left inside an aborted one. run's drain calls SyncState twice on
	// the way out; stuck in the aborted transaction both would fail, and the
	// drain's forced progress write would never land. The store is healthy
	// again for this write, so it must succeed and be durable.
	tb.progress = NewProgressStore(conn)
	exec(t, conn, `INSERT INTO out VALUES (2)`)
	assert.NoError(t, tb.SyncState(ctx))
	assert.Equal(t, int64(1), countCommitted(t, db, "out"))
}

// A host clock that steps back must not stop the progress write. The throttle
// compares now with the last write; after a jump back that difference is
// negative, and a plain `elapsed >= interval` would be false for as long as
// the jump was, or forever after a jump of more than the interval. The row
// would freeze, and with it last_commit, so no window would close on idleness
// for the length of the jump.
func TestStateDurability_AClockThatStepsBackDoesNotStopTheProgressWrite(t *testing.T) {
	coverage.Covers(t, "state.durability")
	rec := &progressRecorder{}
	tb := NewTurbine(newIdleSource(), &fakeHandler{}, &fakeSink{}, 1000, time.Hour,
		&sync.Mutex{}, PipelineErrorPolicies{},
		WithProgressStore(rec), WithProgressWriteInterval(time.Hour))

	ctx := context.Background()
	assert.NoError(t, tb.commitState(ctx, progressOnInterval)) // the first write, always due
	if _, n := rec.last(); n != 1 {
		t.Fatalf("want one write, got %d", n)
	}

	// The clock steps back a day: the last write now lies in the future.
	tb.lock.Lock()
	tb.progressWrittenAt = time.Now().UTC().Add(24 * time.Hour)
	tb.lock.Unlock()

	assert.NoError(t, tb.commitState(ctx, progressOnInterval))
	if _, n := rec.last(); n != 2 {
		t.Fatalf("after the clock stepped back the write was skipped: %d writes, want 2", n)
	}
}

// heldSink holds every write for a while, the way a sink retrying a
// destination does.
