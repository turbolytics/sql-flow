package run

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/core"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/turbolytics/sql-flow/internal/sinks"
	"github.com/zeebo/assert"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.uber.org/zap"
)

// The idle close is a contract between two parties that never share a
// connection: the engine asserts the window's watermark, and the manager
// reads it. The manager's own tests write that row by hand. These drive the
// real writer and the real reader together, built the way run builds them,
// because the rule is only as good as what the engine actually writes.

// nilHandler accepts every message and yields no table, as the real handlers
// do for an empty batch. The consume loop marks the arrival either way.
type nilHandler struct{}

func (nilHandler) Init(context.Context) error                  { return nil }
func (nilHandler) Write([]byte) error                          { return nil }
func (nilHandler) Invoke(context.Context) (arrow.Table, error) { return nil, nil }
func (nilHandler) RowsRead() int64                             { return 0 }

type noopSink struct{}

func (noopSink) WriteTable(context.Context, arrow.Table) error { return nil }
func (noopSink) Flush(context.Context) error                   { return nil }

// tickingSource delivers one-message batches every interval, up to limit (no
// limit when zero), and then holds its stream open the way a quiet source
// does.
type tickingSource struct {
	every   time.Duration
	limit   int
	release chan struct{}
}

func (s *tickingSource) Start() error  { return nil }
func (s *tickingSource) Commit() error { return nil }
func (s *tickingSource) Close() error  { return nil }

func (s *tickingSource) Stream() <-chan []core.Message {
	ch := make(chan []core.Message)
	go func() {
		defer close(ch)
		for i := 0; s.limit == 0 || i < s.limit; i++ {
			select {
			case <-time.After(s.every):
			case <-s.release:
				return
			}
			select {
			case ch <- []core.Message{{Value: []byte(`{}`), Topic: "t", Offset: int64(i)}}:
			case <-s.release:
				return
			}
		}
		<-s.release
	}()
	return ch
}

// failingAfter passes its first ok writes to the real store and refuses every
// one after, the way a store does once its database turns read-only or runs
// out of memory.
type failingAfter struct {
	inner core.ProgressSaver
	ok    int

	mu sync.Mutex
	n  int
}

func (f *failingAfter) Record(ctx context.Context, p core.Progress) error {
	f.mu.Lock()
	f.n++
	refuse := f.n > f.ok
	f.mu.Unlock()
	if refuse {
		return errors.New("progress store is down")
	}
	return f.inner.Record(ctx, p)
}

// idleCloseRig is a pipeline with one windowed table holding one open bucket,
// a manager built the way run builds it, and a turbine wired the way run
// wires it -- including the watermark tracker, restored from the table as run
// restores it once the tables exist. The handler writes nothing: the bucket is
// seeded, and the stream exists to keep the partition out of idleness.
type idleCloseRig struct {
	published func() int64
	poll      func() error
	turbine   *core.Turbine
	stop      func()
}

func newIdleCloseRig(t *testing.T, src *tickingSource, wrap func(core.ProgressSaver) core.ProgressSaver) *idleCloseRig {
	t.Helper()
	ctx := context.Background()

	db, conn := rowsTestDB(t)
	rowsTestExec(t, conn, "CREATE TABLE win (bucket TIMESTAMPTZ, n BIGINT)")
	rowsTestExec(t, conn, "CREATE TABLE published (bucket TIMESTAMPTZ, n BIGINT)")
	// One bucket, so the stream clock can never close it: that takes a later
	// bucket. Whatever closes it is the idle rule.
	rowsTestExec(t, conn, "INSERT INTO win VALUES (TIMESTAMPTZ '2026-09-13 10:00:00+00', 1)")

	conf := &config.Conf{Tables: &config.Tables{SQL: []config.TableSQL{{
		Name: "win",
		Window: &config.Window{
			TimeColumn:       "bucket",
			SizeSeconds:      60,
			GraceSeconds:     60,
			IdleCloseSeconds: 1,
			LateRows:         "drop",
			PollIntervalSecs: 3600,
			EmitSQL:          "SELECT bucket, sum(n)::BIGINT AS n FROM closed GROUP BY ALL",
			Sink: config.Sink{Type: "sqlcommand", SQLCommand: &config.SQLCommandSink{
				SQL: "INSERT INTO published SELECT bucket, n FROM sqlflow_sink_batch",
			}},
		},
	}}}}

	store := core.NewProgressStore(conn)
	assert.NoError(t, store.Init(ctx))
	assert.NoError(t, initWindowStores(ctx, conf, conn))

	managed, closeConns, err := buildManagedTables(ctx, conf, db, zap.NewNop(),
		sdkmetric.NewMeterProvider(), nil, sinks.RetryEvents{})
	assert.NoError(t, err)
	assert.Equal(t, 1, len(managed))

	var recorder core.ProgressSaver = store
	if wrap != nil {
		recorder = wrap(store)
	}
	// The lock every party on this connection holds, the way run wires it.
	// The turbine writes sqlflow_progress on it once a second; published()
	// reads from it every 25ms. An anonymous mutex here made the turbine the
	// only holder, so the two raced, DuckDB closed whichever result was
	// pending, and the read came back with no rows at all -- #280's failure
	// mode, manufactured by the rig rather than by the engine.
	lock := &sync.Mutex{}

	// The watermark tracker, wired and restored the way run does it. The
	// pipeline has seen no rows of its own, so the close it makes when the
	// stream stops is measured from what the table holds.
	watermarks, windowOpts := windowOptions(conf, conn)
	lock.Lock()
	assert.NoError(t, restoreWindows(ctx, conf, conn, watermarks))
	lock.Unlock()

	// A 20ms flush interval, so a quiet stream ticks often. The progress
	// write interval stays at its production second, and the store is wired
	// the one way run wires it.
	tb := core.NewTurbine(src, nilHandler{}, noopSink{}, 1, 20*time.Millisecond,
		lock, core.PipelineErrorPolicies{},
		append([]core.TurbineOption{core.WithProgressStore(recorder)}, windowOpts...)...)

	loopCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { _, _ = tb.ConsumeLoop(loopCtx, 0); close(done) }()

	var once sync.Once
	stop := func() {
		once.Do(func() {
			cancel()
			close(src.release)
			<-done
			closeConns()
		})
	}
	t.Cleanup(stop)

	return &idleCloseRig{
		published: func() int64 {
			lock.Lock()
			defer lock.Unlock()
			return sharedConnCount(t, conn, "published")
		},
		poll:    func() error { return managed[0].Poll(ctx) },
		turbine: tb,
		stop:    stop,
	}
}

// A stream that stops closes what it has. One batch, then silence: the
// engine's ticks keep committing, and once one finds the partition silent for
// the idle bound the stream is done with what it has -- the assertion moves
// past the newest bucket the table holds, and the manager publishes it.
func TestManagerWindow_AQuietStreamClosesOnTheEnginesAssertion(t *testing.T) {
	coverage.Covers(t, "manager.window")
	rig := newIdleCloseRig(t, &tickingSource{every: time.Millisecond, limit: 1, release: make(chan struct{})}, nil)

	deadline := time.Now().Add(15 * time.Second)
	for rig.published() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the stream has been quiet for far longer than idle_close_seconds and the bucket " +
				"never closed: the engine's idle ticks are not confirming the quiet")
		}
		assert.NoError(t, rig.poll())
		time.Sleep(25 * time.Millisecond)
	}
	assert.Equal(t, int64(1), rig.published())
}

// The other direction, and the one that loses data. The stream is live the
// whole time, a batch every ten milliseconds, so the partition is never
// silent for the bound and nothing closes on idleness -- while the progress
// store refuses every write after the first, which under the old design
// froze last_arrival and made a live stream read as a stopped one.
//
// The progress row cannot affect a close at all now: the engine asserts the
// watermark from what it has seen per partition, and a failing liveness write
// is a failing liveness write. This proves the two are independent rather
// than that the old reading was careful.
func TestManagerWindow_ALiveStreamNeverClosesOnIdleness(t *testing.T) {
	coverage.Covers(t, "manager.window")
	src := &tickingSource{every: 10 * time.Millisecond, release: make(chan struct{})}
	rig := newIdleCloseRig(t, src, func(inner core.ProgressSaver) core.ProgressSaver {
		return &failingAfter{inner: inner, ok: 1}
	})

	// Three times the idle bound, polling all the way through.
	for end := time.Now().Add(3 * time.Second); time.Now().Before(end); {
		assert.NoError(t, rig.poll())
		if n := rig.published(); n != 0 {
			t.Fatalf("a live stream had its open bucket closed on idleness (%d rows published): "+
				"a partition that keeps delivering must never leave the minimum", n)
		}
		time.Sleep(50 * time.Millisecond)
	}

	// The stream really was live and the store really was failing; otherwise
	// the loop above proved nothing.
	p := rig.turbine.Progress()
	assert.That(t, p.Messages > 50)
	assert.That(t, p.Errors > 0)
}
