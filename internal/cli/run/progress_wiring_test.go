package run

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/core"
	"github.com/turbolytics/sql-flow/internal/coverage"
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

// run's wiring decides which pipelines write the progress row on an idle
// tick. Wired to always, a pipeline with no window pays a statement, a WAL
// append and an fsync every flush interval for a row nothing reads. Wired
// to never, a window with idle_close_seconds is never confirmed quiet and
// no bucket closes on idleness. Each half is tested on its own; this ties
// the config to the turbine through the options run builds, on a source
// that never delivers so every commit is an idle tick.
func TestManagerWindow_OnlyAnIdleCloseWindowConfirmsQuietOnIdleTicks(t *testing.T) {
	coverage.Covers(t, "manager.window")

	window := func(idleCloseSeconds int) *config.Conf {
		return &config.Conf{Tables: &config.Tables{SQL: []config.TableSQL{{
			Name:   "t",
			Window: &config.Window{SizeSeconds: 60, IdleCloseSeconds: idleCloseSeconds},
		}}}}
	}

	for _, tc := range []struct {
		name   string
		conf   *config.Conf
		writes bool
	}{
		{"a window with idle_close_seconds is confirmed quiet", window(10), true},
		{"a window without it is not", window(0), false},
		{"nor is a pipeline with no window", &config.Conf{}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := &countingProgress{}
			src := &tickingSource{every: time.Hour, release: make(chan struct{})}
			tb := core.NewTurbine(src, nilHandler{}, noopSink{}, 1, 20*time.Millisecond,
				&sync.Mutex{}, core.PipelineErrorPolicies{}, progressOptions(tc.conf, store)...)
			done := make(chan struct{})
			go func() { _, _ = tb.ConsumeLoop(context.Background(), 0); close(done) }()

			// Ten ticks' worth. The first tick's write is always due, so
			// one write is the signal and zero is the other.
			time.Sleep(200 * time.Millisecond)
			close(src.release)
			<-done

			assert.Equal(t, tc.writes, store.count() > 0)
		})
	}
}
