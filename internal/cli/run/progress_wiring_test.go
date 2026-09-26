package run

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/core"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/turbolytics/sql-flow/internal/duckdb"
	"github.com/zeebo/assert"
)

// countingProgress counts the progress writes it is handed.
type countingProgress struct {
	mu sync.Mutex
	n  int
}

func (c *countingProgress) Record(context.Context, core.Progress) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.n++
	return nil
}

func (c *countingProgress) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.n
}

func windowedConf(idleCloseSeconds int) *config.Conf {
	return &config.Conf{Tables: &config.Tables{SQL: []config.TableSQL{{
		Name: "t",
		Window: &config.Window{
			TimeColumn: "bucket", SizeSeconds: 60, GraceSeconds: 5,
			IdleCloseSeconds: idleCloseSeconds, LateRows: "drop",
		},
	}}}}
}

// run's wiring decides which pipelines assert a watermark. Wired to always,
// a pipeline with no window would carry a tracker for nothing; wired to
// never, a window would have no watermark and never close. The specs the
// tracker is built from are the config's durations, one per window.
func TestManagerWindow_OnlyAWindowingPipelineAssertsWatermarks(t *testing.T) {
	coverage.Covers(t, "manager.window")
	ctx := context.Background()
	db, err := duckdb.OpenPath(ctx, "")
	assert.NoError(t, err)
	defer db.Close()
	conn, err := db.Connect(ctx)
	assert.NoError(t, err)
	defer conn.Close()

	w, opts := windowOptions(&config.Conf{}, conn)
	assert.That(t, w == nil)
	assert.Equal(t, 0, len(opts))

	w, opts = windowOptions(windowedConf(10), conn)
	assert.That(t, w != nil)
	assert.Equal(t, 1, len(opts))

	specs := windowSpecs(windowedConf(10))
	assert.Equal(t, 1, len(specs))
	assert.Equal(t, core.WindowSpec{Name: "t", Size: time.Minute, Grace: 5 * time.Second, IdleClose: 10 * time.Second}, specs[0])
}

// Idle ticks write no progress row on any pipeline: nothing reads it
// between batches, and on a box that runs its state from an SD card a
// statement, a WAL append and an fsync every flush interval is the
// difference between a quiet pipeline and a busy one. This ties the config
// to the turbine through the options run builds, on a source that never
// delivers so every commit is an idle tick.
func TestManagerWindow_IdleTicksWriteNoProgressRow(t *testing.T) {
	coverage.Covers(t, "manager.window")
	ctx := context.Background()
	db, err := duckdb.OpenPath(ctx, "")
	assert.NoError(t, err)
	defer db.Close()
	conn, err := db.Connect(ctx)
	assert.NoError(t, err)
	defer conn.Close()
	assert.NoError(t, initWindowStores(ctx, windowedConf(10), conn))

	for _, tc := range []struct {
		name string
		conf *config.Conf
	}{
		{"a window with idle_close_seconds", windowedConf(10)},
		{"a window without it", windowedConf(0)},
		{"a pipeline with no window", &config.Conf{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := &countingProgress{}
			src := &tickingSource{every: time.Hour, release: make(chan struct{})}
			_, windowOpts := windowOptions(tc.conf, conn)
			opts := append([]core.TurbineOption{core.WithProgressStore(store)}, windowOpts...)
			tb := core.NewTurbine(src, nilHandler{}, noopSink{}, 1, 20*time.Millisecond,
				&sync.Mutex{}, core.PipelineErrorPolicies{}, opts...)
			done := make(chan struct{})
			go func() { _, _ = tb.ConsumeLoop(context.Background(), 0); close(done) }()

			// Ten ticks' worth.
			time.Sleep(200 * time.Millisecond)
			close(src.release)
			<-done

			assert.Equal(t, 0, store.count())
		})
	}
}
