package core

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

// skippingHandler reports that every re-initialisation skipped its
// checkpoint, the way the structured handler does while a window holds a
// write open.
type skippingHandler struct{ fakeHandler }

func (h *skippingHandler) CheckpointSkipped() bool { return true }

// A skipped checkpoint is counted once per re-initialisation, so a window
// whose I/O outlasts a batch shows as a rising count rather than as memory
// growth with no explanation. The loop re-initialises the handler once
// before the first batch and once after each batch.
func TestCoreConsumeLoop_CountsSkippedCheckpoints(t *testing.T) {
	coverage.Covers(t, "core.consume_loop")
	r := sdkmetric.NewManualReader()
	m, err := NewMetrics(sdkmetric.NewMeterProvider(sdkmetric.WithReader(r)))
	assert.NoError(t, err)

	src := &fakeSource{batches: [][]Message{messages(10), messages(10)}}
	tb := NewTurbine(src, &skippingHandler{}, &fakeSink{}, 10, time.Second,
		&sync.Mutex{}, PipelineErrorPolicies{}, WithMetrics(m))
	_, err = tb.ConsumeLoop(context.Background(), 0)
	assert.NoError(t, err)

	skipped := flatValue(t, r, "handler_checkpoints_skipped")
	assert.That(t, skipped >= 2)

	// A handler that never skips records nothing.
	r2 := sdkmetric.NewManualReader()
	m2, err := NewMetrics(sdkmetric.NewMeterProvider(sdkmetric.WithReader(r2)))
	assert.NoError(t, err)
	tb2 := NewTurbine(&fakeSource{batches: [][]Message{messages(10)}}, &fakeHandler{}, &fakeSink{}, 10,
		time.Second, &sync.Mutex{}, PipelineErrorPolicies{}, WithMetrics(m2))
	_, err = tb2.ConsumeLoop(context.Background(), 0)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), flatValue(t, r2, "handler_checkpoints_skipped"))
}
