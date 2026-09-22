package core

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
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

// The invariant the row has to keep for the idle close, now that the rule
// reads the row's two clocks against each other: a record never overstates
// the quiet. last_commit - last_arrival in any record must be at most the
// quiet the stream had actually seen when that record was written. A record
// that overstates it is the one that closes a window early.
//
// The shape that would break it is a batch inside the interval of an idle
// tick. The batch's own commit is skipped, and the next write is a later idle
// tick. That write carries the batch's arrival at its true time, so its gap
// is exactly the quiet since the batch -- not the quiet since the write
// before it, which the row would have reported had the skip stamped nothing.
func TestStateDurability_ProgressNeverOverstatesTheQuiet(t *testing.T) {
	coverage.Covers(t, "state.durability")
	rec := &progressRecorder{}

	// An idle tick writes first, then the batch lands inside the interval of
	// that write and its commit is skipped, then more idle ticks. A source
	// that delivers immediately makes the batch the first write of all,
	// which is always due, and the case never arises.
	src := newPacedSource(1, 150*time.Millisecond)

	// Long enough that only idle ticks past it are ever due, and short
	// enough that several are.
	tb := NewTurbine(src, &fakeHandler{}, &fakeSink{}, 1000, 20*time.Millisecond,
		&sync.Mutex{}, PipelineErrorPolicies{},
		WithProgressStore(rec), WithProgressWriteInterval(200*time.Millisecond))
	done := make(chan struct{})
	go func() { _, _ = tb.ConsumeLoop(context.Background(), 0); close(done) }()

	waitFor(t, "the batch to be consumed", 5*time.Second, func() bool {
		return tb.Progress().Messages == 1
	})
	// The snapshot is the truth about the newest arrival.
	newest := tb.Progress().LastArrival
	assert.That(t, !newest.IsZero())
	waitFor(t, "a write after the batch", 5*time.Second, func() bool {
		last, _ := rec.last()
		return last.LastArrival.Equal(newest)
	})
	close(src.release)
	<-done

	rec.mu.Lock()
	defer rec.mu.Unlock()
	assert.That(t, len(rec.recs) >= 2)
	var sawBatch bool
	for i, r := range rec.recs {
		if r.LastArrival.After(newest) {
			t.Fatalf("record %d reports an arrival from the future: %v > %v", i, r.LastArrival, newest)
		}
		// Before the batch the row carries the loop's start, which confirms
		// only the quiet this process has watched. From the first write
		// after the batch, the gap is measured from the batch's true
		// arrival, so it can never exceed the real quiet.
		if r.LastArrival.Before(newest) {
			continue
		}
		sawBatch = true
		if !r.LastArrival.Equal(newest) {
			t.Fatalf("record %d carries arrival %v, want the batch's %v", i, r.LastArrival, newest)
		}
		if real := r.LastCommit.Sub(newest); r.LastCommit.Sub(r.LastArrival) > real {
			t.Fatalf("record %d confirms %v of quiet against a real %v: it overstates it, "+
				"and a window reading it closes early", i, r.LastCommit.Sub(r.LastArrival), real)
		}
	}
	assert.That(t, sawBatch)
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
	tb.quietSince = first
	assert.NoError(t, tb.commitState(ctx, false)) // always due: writes `first`

	newest := first.Add(time.Second)
	tb.quietSince = newest
	assert.NoError(t, tb.commitState(ctx, false)) // inside the interval, not owed

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
		tb.quietSince = newest
		// Stateless, so the commit itself succeeds whatever the write did.
		assert.NoError(t, tb.commitState(ctx, false))
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
		tb.quietSince = time.Now().UTC().Add(time.Duration(i) * time.Millisecond)
		assert.NoError(t, tb.commitState(ctx, false))
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
	tb.quietSince = arrival
	assert.NoError(t, tb.commitState(ctx, false)) // due, and the write fails

	// Silence: an idle commit sees no newer arrival and sits inside the
	// interval, so it does not write.
	assert.NoError(t, tb.commitState(ctx, false))
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
	tb.quietSince = time.Now().UTC()

	err = tb.commitState(ctx, false)
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
	assert.NoError(t, tb.commitState(ctx, false)) // the first write, always due
	if _, n := rec.last(); n != 1 {
		t.Fatalf("want one write, got %d", n)
	}

	// The clock steps back a day: the last write now lies in the future.
	tb.lock.Lock()
	tb.progressWrittenAt = time.Now().UTC().Add(24 * time.Hour)
	tb.lock.Unlock()

	assert.NoError(t, tb.commitState(ctx, false))
	if _, n := rec.last(); n != 2 {
		t.Fatalf("after the clock stepped back the write was skipped: %d writes, want 2", n)
	}
}

// heldSink holds every write for a while, the way a sink retrying a
// destination does.
type heldSink struct {
	fakeSink
	hold time.Duration
}

func (s *heldSink) WriteTable(ctx context.Context, batch arrow.Table) error {
	time.Sleep(s.hold)
	return s.fakeSink.WriteTable(ctx, batch)
}

// progress.quiet_is_watched, for a sink held in retries.
//
// The commit that ends a held write must not record the hold as quiet. The
// engine was inside the sink, not waiting on the source, and messages may
// have been waiting at the source the whole time. Stamped before the write,
// the arrival made a 400ms hold read as 400ms of quiet on a live stream, and
// a sink retrying for longer than idle_close closed every bucket.
func TestStateDurability_ASinkWriteHeldInRetriesIsNotQuiet(t *testing.T) {
	coverage.Covers(t, "state.durability")
	rec := &progressRecorder{}
	src := newBlockingSource(messages(3))
	sink := &heldSink{hold: 200 * time.Millisecond}
	tb := NewTurbine(src, &fakeHandler{}, sink, 3, time.Hour,
		&sync.Mutex{}, PipelineErrorPolicies{},
		WithProgressStore(rec), WithProgressWriteInterval(0))
	done := make(chan struct{})
	go func() { _, _ = tb.ConsumeLoop(context.Background(), 0); close(done) }()

	waitFor(t, "the batch's write", 5*time.Second, func() bool {
		p, _ := rec.last()
		return p.Messages == 3
	})
	close(src.release)
	<-done

	p, _ := rec.last()
	if quiet := p.LastCommit.Sub(p.LastArrival); quiet >= sink.hold {
		t.Fatalf("the batch's commit confirms %v of quiet; the sink held its write for %v and nothing about the stream was watched meanwhile", quiet, sink.hold)
	}
}

// progress.quiet_is_watched, for a restart.
//
// A process that just started has watched no quiet. The row it inherits
// carries the previous process's last arrival, and a write that leaves it
// there confirms the whole outage: a power cut of ten minutes, then a first
// idle tick before the consumer group has rejoined, closed every bucket
// saved in the state database, and the backlog replayed afterwards was late.
func TestStateDurability_ARestartConfirmsNoQuietItDidNotSee(t *testing.T) {
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

	// The previous process: an arrival, then a commit, an hour ago.
	before := time.Now().Add(-time.Hour)
	assert.NoError(t, store.Record(ctx, Progress{LastArrival: before, LastCommit: before, Messages: 7}))

	// This process: no batch yet, and the first idle tick.
	tb := NewTurbine(newIdleSource(), &fakeHandler{}, &fakeSink{}, 1000, time.Hour,
		&sync.Mutex{}, PipelineErrorPolicies{},
		WithProgressStore(store), WithProgressWriteInterval(0))
	assert.NoError(t, tb.commitState(ctx, false))

	stmt, err := conn.NewStatement()
	assert.NoError(t, err)
	defer stmt.Close()
	assert.NoError(t, stmt.SetSqlQuery(`SELECT epoch_us(last_commit) - epoch_us(last_arrival) FROM sqlflow_progress`))
	reader, _, err := stmt.ExecuteQuery(ctx)
	assert.NoError(t, err)
	defer reader.Release()
	assert.That(t, reader.Next())
	quiet := time.Duration(reader.Record().Column(0).(*array.Int64).Value(0)) * time.Microsecond
	if quiet > 10*time.Second {
		t.Fatalf("the first tick after a restart confirms %v of quiet; this process has been running for milliseconds", quiet)
	}
}

// progress.quiet_is_monotonic.
//
// A wall clock that steps forward between an arrival and a commit must not
// turn the step into quiet: a gateway with no hardware clock boots near
// 1970, and NTP moves it by decades after the first batch. Go measures
// between two readings on the monotonic clock only when both carry one, and
// UTC() strips it. A step cannot be staged in a test, so this pins the
// mechanism: the stamps keep their readings, and the commit clock is
// derived from the arrival's by the monotonic elapsed rather than read from
// the wall.
func TestStateDurability_TheQuietIsMeasuredOnTheMonotonicClock(t *testing.T) {
	coverage.Covers(t, "state.durability")
	rec := &progressRecorder{}
	src := newBlockingSource(messages(1))
	tb := NewTurbine(src, &fakeHandler{}, &fakeSink{}, 1, time.Hour,
		&sync.Mutex{}, PipelineErrorPolicies{},
		WithProgressStore(rec), WithProgressWriteInterval(0))
	done := make(chan struct{})
	go func() { _, _ = tb.ConsumeLoop(context.Background(), 0); close(done) }()
	waitFor(t, "the batch's write", 5*time.Second, func() bool {
		p, _ := rec.last()
		return p.Messages == 1
	})
	close(src.release)
	<-done

	tb.lock.Lock()
	since := tb.quietSince
	written := tb.progressWrittenAt
	tb.lock.Unlock()
	// A reading is printed as m=±... by String, and by nothing else.
	assert.That(t, strings.Contains(since.String(), " m="))
	assert.That(t, strings.Contains(written.String(), " m="))
	p, _ := rec.last()
	assert.That(t, strings.Contains(p.LastArrival.String(), " m="))
	assert.That(t, strings.Contains(p.LastCommit.String(), " m="))
}

// progress.quiet_is_watched, for the work between building the turbine and
// starting its loop.
//
// The constructor seeds the quiet clock, and the loop seeds it again when it
// starts, because on a slow device the handler's Init and the source's Start
// can sit between the two for a while, and that time was not spent waiting
// on the source. The restart test cannot see the second seed: it runs the
// loop the moment the turbine exists. This one moves the clock an hour back
// after construction and checks the loop's first write ignores it.
func TestStateDurability_TheLoopStartsTheQuietClockAgain(t *testing.T) {
	coverage.Covers(t, "state.durability")
	rec := &progressRecorder{}
	src := newIdleSource()
	tb := NewTurbine(src, &fakeHandler{}, &fakeSink{}, 1000, 20*time.Millisecond,
		&sync.Mutex{}, PipelineErrorPolicies{},
		WithProgressStore(rec), WithProgressWriteInterval(0))
	tb.lock.Lock()
	tb.quietSince = time.Now().Add(-time.Hour) // a slow start
	tb.lock.Unlock()

	done := make(chan struct{})
	go func() { _, _ = tb.ConsumeLoop(context.Background(), 0); close(done) }()
	waitFor(t, "the first idle tick", 5*time.Second, func() bool {
		_, n := rec.last()
		return n >= 1
	})
	close(src.release)
	<-done

	p, _ := rec.last()
	if quiet := p.LastCommit.Sub(p.LastArrival); quiet > 10*time.Second {
		t.Fatalf("the loop's first write confirms %v of quiet; the loop had run for milliseconds", quiet)
	}
}
