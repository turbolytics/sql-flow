package turbostats

import (
	"context"
	"testing"

	"github.com/turbolytics/sql-flow/internal/core"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

// What one bundle costs to build.
//
// Collect runs once a report interval and once per GET /turbostats/v1, and
// it walks every instrument to do it. The durations made that walk read
// histograms and fold their buckets, so this is the path that change is
// most likely to slow down.
//
// Compare two commits with benchstat and interleaved runs rather than
// reading absolute figures: on a laptop the arm that runs second is the
// warmer one.
func BenchmarkCollectBundle(b *testing.B) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	m, err := core.NewMetrics(mp)
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	m.MessageCount.Add(ctx, 1000)
	m.HandlerRowsRead.Add(ctx, 1000)
	m.PipelineErrors.Add(ctx, 2)
	for _, seconds := range []float64{0.0005, 0.004, 0.02, 0.1, 0.5, 2, 12, 40, 90} {
		m.BatchProcessingLatency.Record(ctx, seconds)
		m.SinkFlushLatency.Record(ctx, seconds)
	}
	src := Source{Static: static, Reader: reader, Pipeline: &PipelineSource{}}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := Collect(ctx, src); err != nil {
			b.Fatal(err)
		}
	}
}
