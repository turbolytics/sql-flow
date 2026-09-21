package turbostats

import (
	"context"
	"runtime"
	"testing"

	"errors"
	"github.com/turbolytics/sql-flow/internal/core"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/turbolytics/sql-flow/internal/managers"
	"github.com/turbolytics/sql-flow/internal/sinks"
	"github.com/zeebo/assert"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

// liveHeap is the bytes still reachable after a collection.
//
// Not resident memory, which the handler leak test reads. That test needs it
// because its leak is native, across the ADBC boundary, where the Go heap
// sees nothing. The aggregation here is pure Go, so a leak in it is a
// reachable object, and the live heap counts those exactly. Resident memory
// under the race detector wandered 1.3 MiB either way over the same run,
// which is the size of the signal.
func liveHeap() int64 {
	runtime.GC()
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return int64(ms.HeapAlloc)
}

// Collect does not grow the process, however often it runs.
//
// A reporter collects for weeks and a control plane may poll the route every
// second, so a leak here is linear in collects rather than in messages. A
// twenty-minute soak sees about a thousand of them and cannot tell 0.05 MiB a
// minute from its own noise. This runs fifty thousand, which is five weeks of
// a minute interval, over every attributed series the bundle summarizes: 32
// lag partitions, two windows, late rows under both policies, and two sinks.
//
// It does not skip under -short. CI runs only -short and TestIntegration, so
// a skip here would be a test nothing ever runs.
func TestCollect_DoesNotGrowOverManyCollects(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	ctx := context.Background()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	m, err := core.NewMetrics(mp)
	assert.NoError(t, err)

	for p := 0; p < 32; p++ {
		m.Lag.Set("posts", int32(p), int64(p))
	}
	for _, name := range []string{"hourly", "daily"} {
		window := managers.NewWindowMetrics(mp, name)
		w := metric.WithAttributes(attribute.String("window", name))
		window.Late.Add(ctx, 1, metric.WithAttributes(
			attribute.String("window", name), attribute.String("policy", string(managers.LateDrop))))
		window.Late.Add(ctx, 1, metric.WithAttributes(
			attribute.String("window", name), attribute.String("policy", string(managers.LateReemit))))
		window.Closed.Add(ctx, 1, w)
		window.NewestStart.Record(ctx, 1757570000, w)
		window.CloseLag.Record(ctx, 0, w)
	}
	failed := errors.New("refused")
	sinks.RetryCounter(mp, "postgres")(1, failed)
	sinks.RetryCounter(mp, "kafka")(1, failed)

	src := runSource(reader, nil)
	const warmup, iters = 5_000, 50_000
	for i := 0; i < warmup; i++ {
		_, err := Collect(ctx, src)
		assert.NoError(t, err)
	}
	before := liveHeap()
	for i := 0; i < iters; i++ {
		// The series keep moving, as they do in a live pipeline.
		m.Lag.Set("posts", int32(i%32), int64(i%1000))
		b, err := Collect(ctx, src)
		assert.NoError(t, err)
		if i == iters-1 {
			assert.Equal(t, 32, *b.Pipeline.LagPartitions)
			assert.Equal(t, int64(2), *b.Pipeline.LateRowsDropped)
		}
	}
	after := liveHeap()

	growth := after - before
	t.Logf("live heap %d KiB before, %d KiB after %d collects: %+d KiB",
		before>>10, after>>10, iters, growth>>10)
	// Retaining one summary a collect is 160 bytes each, 7.6 MiB here. The
	// live heap after a collection moves by kilobytes.
	const limit = 1 << 20
	if growth > limit {
		t.Fatalf("process grew %d KiB over %d collects; Collect is retaining memory", growth>>10, iters)
	}
}
