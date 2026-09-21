package run

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/core"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

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

// run's wiring decides which pipelines keep the per-arrival progress write.
// Backwards, a window with idle_close_seconds loses it, reads a stale
// last_arrival, and closes buckets on a live stream. The write interval is an
// hour, so the first commit is the only one the clock makes due: any write
// after it was forced by an arrival.
func TestManagerWindow_OnlyAnIdleCloseWindowKeepsThePerArrivalProgressWrite(t *testing.T) {
	coverage.Covers(t, "manager.window")
	const batches = 6

	window := func(idleCloseSeconds int) *config.Conf {
		return &config.Conf{Tables: &config.Tables{SQL: []config.TableSQL{{
			Name:   "t",
			Window: &config.Window{SizeSeconds: 60, IdleCloseSeconds: idleCloseSeconds},
		}}}}
	}

	for _, tc := range []struct {
		name   string
		conf   *config.Conf
		forced bool
	}{
		{"a window with idle_close_seconds is owed every arrival", window(10), true},
		{"a window without it is not", window(0), false},
		{"nor is a pipeline with no window", &config.Conf{}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := &countingProgress{}
			src := &sliceSource{}
			for i := 0; i < batches; i++ {
				src.batches = append(src.batches,
					[]core.Message{{Value: []byte(`{}`), Topic: "t", Offset: int64(i)}})
			}
			opts := append(progressOptions(tc.conf, store), core.WithProgressWriteInterval(time.Hour))
			tb := core.NewTurbine(src, nilHandler{}, noopSink{}, 1, time.Hour, &sync.Mutex{},
				core.PipelineErrorPolicies{}, opts...)
			_, err := tb.ConsumeLoop(context.Background(), batches)
			assert.NoError(t, err)

			n := store.count()
			if tc.forced && n < 2 {
				t.Fatalf("%d progress writes over %d batches: no arrival forced one", n, batches)
			}
			if !tc.forced && n != 1 {
				t.Fatalf("%d progress writes over %d batches inside one interval, want 1", n, batches)
			}
		})
	}
}
