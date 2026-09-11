package core

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// The flat instruments exist so a reader that cannot filter attributes has an
// honest number. Which measurements reach them is decided here, at the
// recording site, rather than re-derived by whoever reads the series.

// flatValue reads the dimensionless point of one instrument, which is what
// turbostats.Collect does. A point carrying attributes is a different series.
func flatValue(t *testing.T, r *sdkmetric.ManualReader, name string) int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	assert.NoError(t, r.Collect(context.Background(), &rm))

	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			switch data := m.Data.(type) {
			case metricdata.Sum[int64]:
				for _, dp := range data.DataPoints {
					if dp.Attributes.Len() == 0 {
						return dp.Value
					}
				}
			case metricdata.Gauge[int64]:
				for _, dp := range data.DataPoints {
					if dp.Attributes.Len() == 0 {
						return dp.Value
					}
				}
			}
		}
	}
	return 0
}

// meteredTurbine builds a pipeline whose instruments record into a reader the
// test can read back.
func meteredTurbine(t *testing.T, src Source, sink Sink, batchSize int) (*Turbine, *sdkmetric.ManualReader) {
	t.Helper()
	r := sdkmetric.NewManualReader()
	m, err := NewMetrics(sdkmetric.NewMeterProvider(sdkmetric.WithReader(r)))
	assert.NoError(t, err)

	tb := NewTurbine(src, &fakeHandler{}, sink, batchSize, time.Second,
		&sync.Mutex{}, PipelineErrorPolicies{}, WithMetrics(m))
	return tb, r
}

// flushFailingSink accepts rows and fails every flush, the way a sink whose
// destination has stopped answering does.
type flushFailingSink struct {
	mu sync.Mutex
}

func (s *flushFailingSink) WriteTable(ctx context.Context, batch arrow.Table) error { return nil }
func (s *flushFailingSink) Flush(ctx context.Context) error {
	return errors.New("destination unreachable")
}

// A failed flush delivered nothing, so it must not count. The dimensioned
// series carries result=error for a dashboard to filter; a reader that adds
// ok and error together reports a number true of nothing, which is why the
// bundle reads this series instead.
func TestCoreConsumeLoop_FlushesCountSuccessesOnly(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	src := &fakeSource{batches: [][]Message{messages(10)}}
	tb, reader := meteredTurbine(t, src, &flushFailingSink{}, 10)

	_, err := tb.ConsumeLoop(context.Background(), 0)
	assert.Error(t, err)

	assert.Equal(t, int64(0), flatValue(t, reader, "pipeline_flushes"))
	// The error did reach the flat error counter.
	assert.That(t, flatValue(t, reader, "pipeline_errors") > 0)
}

func TestCoreConsumeLoop_ASuccessfulFlushCounts(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	src := &fakeSource{batches: [][]Message{messages(10)}}
	tb, reader := meteredTurbine(t, src, &fakeSink{}, 10)

	_, err := tb.ConsumeLoop(context.Background(), 0)
	assert.NoError(t, err)

	assert.Equal(t, int64(1), flatValue(t, reader, "pipeline_flushes"))
	assert.Equal(t, int64(0), flatValue(t, reader, "pipeline_errors"))
}

// Receiving messages stamps the time, so a reader can tell a pipeline that is
// idle from one that is wedged. A pipeline that has received nothing reports
// nothing, because zero is not a time.
func TestCoreConsumeLoop_StampsWhenMessagesLastArrived(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	src := &fakeSource{batches: [][]Message{messages(10)}}
	tb, reader := meteredTurbine(t, src, &fakeSink{}, 10)

	before := time.Now().Unix()
	_, err := tb.ConsumeLoop(context.Background(), 0)
	assert.NoError(t, err)

	got := flatValue(t, reader, "pipeline_last_message_timestamp")
	assert.That(t, got >= before)
}

// The DLQ's rows must not reach the flat counters, or a pipeline looks
// healthier the more records it rejects.
func TestToolingCoverage_TheDLQsRowsStayOutOfTheFlatCounters(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	r := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(r))

	pipeline := NewCountingSink(&fakeSink{}, mp, "console", SinkRolePipeline)
	dlq := NewCountingSink(&fakeSink{}, mp, "console", "dlq")

	ctx := context.Background()
	tbl := countIDTable(1)
	defer tbl.Release()

	assert.NoError(t, pipeline.WriteTable(ctx, tbl))
	assert.NoError(t, pipeline.Flush(ctx))
	assert.NoError(t, dlq.WriteTable(ctx, tbl))
	assert.NoError(t, dlq.Flush(ctx))

	// Two sinks wrote one row each; only the pipeline's counts.
	assert.Equal(t, int64(1), flatValue(t, r, "pipeline_rows_written"))
	assert.Equal(t, int64(1), flatValue(t, r, "pipeline_rows_accepted"))
}
