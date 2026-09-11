package core

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/embedded"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

// recordingGauge keeps every value and partition handed to it, which the
// SDK's aggregation would collapse into one last value per partition and so
// could not show how many times Record was called.
type recordingGauge struct {
	embedded.Int64Gauge
	mu    sync.Mutex
	calls []lagCall
}

type lagCall struct {
	partition int
	value     int64
}

func (g *recordingGauge) Enabled(context.Context) bool { return true }

func (g *recordingGauge) Record(_ context.Context, v int64, opts ...metric.RecordOption) {
	set := metric.NewRecordConfig(opts).Attributes()
	part, _ := set.Value(attribute.Key("partition"))
	g.mu.Lock()
	g.calls = append(g.calls, lagCall{partition: int(part.AsInt64()), value: v})
	g.mu.Unlock()
}

// The consumer lag gauge is a last value, so one record per partition per
// fetch says everything a record per message would, and the difference is
// paid ten million times on the 10M benchmark: v1.1.0 ran it at 1.16M msg/s
// against a noop provider, main at 0.95M once the provider became the SDK
// so TurboStats could read it. The record has to carry the newest offset the
// fetch delivered, not the first.
func TestObservabilityMetrics_ConsumerLagRecordsOncePerFetchAndPartition(t *testing.T) {
	coverage.Covers(t, "observability.metrics")
	m, err := NewMetrics(nil)
	assert.NoError(t, err)
	gauge := &recordingGauge{}
	m.ConsumerLag = gauge

	// One fetch: a thousand messages from partition 0 ending at offset 999
	// under a watermark of 1200, then ten from partition 1 that reach the
	// watermark exactly.
	fetch := kafkaMessages("events", 0, 0, 1000)
	for i := range fetch {
		fetch[i].HighWatermark = 1200
	}
	tail := kafkaMessages("events", 1, 500, 10)
	for i := range tail {
		tail[i].HighWatermark = 510
	}
	fetch = append(fetch, tail...)

	src := &fakeSource{batches: [][]Message{fetch}}
	tb := NewTurbine(src, &fakeHandler{}, &fakeSink{}, 100, time.Second,
		&sync.Mutex{}, PipelineErrorPolicies{}, WithMetrics(m))
	_, err = tb.ConsumeLoop(context.Background(), 0)
	assert.NoError(t, err)

	gauge.mu.Lock()
	defer gauge.mu.Unlock()
	assert.Equal(t, 2, len(gauge.calls))
	byPartition := map[int]int64{}
	for _, c := range gauge.calls {
		byPartition[c.partition] = c.value
	}
	// 1200 - 999 - 1: the last processed offset, not the first.
	assert.Equal(t, int64(200), byPartition[0])
	// 510 - 509 - 1: caught up.
	assert.Equal(t, int64(0), byPartition[1])
}

// BenchmarkGaugeRecord is what one Record costs on the provider a pipeline
// actually runs with. The noop case is what v1.1.0 paid per message; the sdk
// case is what main paid once the provider stopped being a noop. Multiply by
// messages per second to see the throughput it costs when recorded per
// message, which is why mark no longer does.
//
//	go test ./internal/core/ -bench GaugeRecord -benchmem -run '^$'
func BenchmarkGaugeRecord(b *testing.B) {
	ctx := context.Background()
	attrs := []metric.RecordOption{metric.WithAttributes(
		attribute.String("topic", "events"),
		attribute.Int("partition", 0),
	)}

	b.Run("noop", func(b *testing.B) {
		m, err := NewMetrics(nil)
		if err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			m.ConsumerLag.Record(ctx, int64(i), attrs...)
		}
	})

	b.Run("sdk", func(b *testing.B) {
		mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(sdkmetric.NewManualReader()))
		m, err := NewMetrics(mp)
		if err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			m.ConsumerLag.Record(ctx, int64(i), attrs...)
		}
	})
}
