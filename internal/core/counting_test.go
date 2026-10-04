package core

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
	"go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

// countStubSink records what it was given and fails its flush on demand.
type countStubSink struct {
	rows      int64
	failFlush bool
	buffered  int
}

func (s *countStubSink) WriteTable(_ context.Context, batch arrow.Table) error {
	if batch != nil {
		s.rows += batch.NumRows()
	}
	return nil
}

func (s *countStubSink) Flush(context.Context) error {
	if s.failFlush {
		return errors.New("stub: sink is down")
	}
	return nil
}

func (s *countStubSink) BufferedRows() int { return s.buffered }

// plainSink implements Sink and nothing else.
type plainSink struct{}

func (plainSink) WriteTable(context.Context, arrow.Table) error { return nil }
func (plainSink) Flush(context.Context) error                   { return nil }

// countIDTable builds an n-row table with a single int64 column.
func countIDTable(n int64) arrow.Table {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	b := array.NewRecordBuilder(memory.NewGoAllocator(), schema)
	defer b.Release()
	for i := int64(0); i < n; i++ {
		b.Field(0).(*array.Int64Builder).Append(i)
	}

	rec := b.NewRecord()
	defer rec.Release()
	return array.NewTableFromRecords(schema, []arrow.Record{rec})
}

// TestCountingCountsDeliveredRowsOnce is the invariant
// sink.rows.counted_on_delivery in miniature. A failed flush counts nothing,
// and the retry that delivers those rows counts them exactly once.
func TestCountingCountsDeliveredRowsOnce(t *testing.T) {
	inner := &countStubSink{failFlush: true}
	c := NewCountingSink(inner, noop.NewMeterProvider(), "console", "pipeline").(*countingBuffered)

	ctx := context.Background()
	tbl := countIDTable(3)
	defer tbl.Release()

	assert.NoError(t, c.WriteTable(ctx, tbl))
	assert.Equal(t, int64(3), c.pendingRows())

	assert.Error(t, c.Flush(ctx))
	assert.Equal(t, int64(3), c.pendingRows())

	inner.failFlush = false
	assert.NoError(t, c.Flush(ctx))
	assert.Equal(t, int64(0), c.pendingRows())
}

// TestCountingForwardsBufferedRows guards the trap the retry wrapper fell
// into: a decorator that does not forward an optional interface silently
// removes it from every sink it wraps.
func TestCountingForwardsBufferedRows(t *testing.T) {
	inner := &countStubSink{buffered: 7}
	c := NewCountingSink(inner, noop.NewMeterProvider(), "console", "pipeline")

	reporter, ok := c.(BufferedRowReporter)
	assert.That(t, ok)
	assert.Equal(t, 7, reporter.BufferedRows())
}

// TestCountingDoesNotInventBufferedRows is the other half: a sink that reports
// no depth must not appear to report one just because it was wrapped.
func TestCountingDoesNotInventBufferedRows(t *testing.T) {
	c := NewCountingSink(plainSink{}, noop.NewMeterProvider(), "noop", "pipeline")
	_, ok := c.(BufferedRowReporter)
	assert.That(t, !ok)
}

// TestCountingNilBatchIsNoop matches sink.flush.empty_is_noop.
func TestCountingNilBatchIsNoop(t *testing.T) {
	c := NewCountingSink(&countStubSink{}, noop.NewMeterProvider(), "console", "pipeline").(*countingBuffered)
	assert.NoError(t, c.WriteTable(context.Background(), nil))
	assert.Equal(t, int64(0), c.pendingRows())
}

// TestCountingNilProviderRecordsNothing keeps a pipeline started without
// --metrics from needing a branch at the call site.
func TestCountingNilProviderRecordsNothing(t *testing.T) {
	c := NewCountingSink(&countStubSink{}, nil, "console", "pipeline")
	tbl := countIDTable(2)
	defer tbl.Release()

	assert.NoError(t, c.WriteTable(context.Background(), tbl))
	assert.NoError(t, c.Flush(context.Background()))
}

const lastWrite = "pipeline_last_sink_write_timestamp"

func meteredCounting(t *testing.T, inner Sink, role string) (Sink, *sdkmetric.ManualReader) {
	t.Helper()
	r := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(r))
	return NewCountingSink(inner, mp, "postgres", role), r
}

// The last write is the destination's acknowledgment, not the buffer's. A
// failed flush delivered nothing, and the retry that delivers is the write.
func TestCounting_RecordsTheLastWriteOnDeliveryOnly(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	inner := &countStubSink{failFlush: true}
	c, r := meteredCounting(t, inner, SinkRolePipeline)
	ctx := context.Background()

	assert.NoError(t, c.WriteTable(ctx, countIDTable(3)))
	assert.Error(t, c.Flush(ctx))
	assert.Equal(t, int64(0), flatValue(t, r, lastWrite))

	inner.failFlush = false
	before := time.Now().Unix()
	assert.NoError(t, c.Flush(ctx))
	got := flatValue(t, r, lastWrite)
	assert.That(t, got >= before && got <= time.Now().Unix())
}

// A periodic flush with nothing pending delivered nothing. Counting it would
// make a pipeline that writes nothing look fresh.
func TestCounting_AFlushWithNothingPendingIsNotAWrite(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	c, r := meteredCounting(t, &countStubSink{}, SinkRolePipeline)
	assert.NoError(t, c.Flush(context.Background()))
	assert.Equal(t, int64(0), flatValue(t, r, lastWrite))
}

// A windowed pipeline's output lands through its manager's sink, so that is
// a write too.
func TestCounting_AManagerSinkWriteIsAWrite(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	c, r := meteredCounting(t, &countStubSink{}, SinkRoleManager)
	ctx := context.Background()
	assert.NoError(t, c.WriteTable(ctx, countIDTable(1)))
	assert.NoError(t, c.Flush(ctx))
	assert.That(t, flatValue(t, r, lastWrite) > 0)
}

// The DLQ's writes are failures landing. Counting them would make a
// pipeline look fresh while every row it touched failed.
func TestCounting_TheDLQNeverMovesTheLastWrite(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	c, r := meteredCounting(t, &countStubSink{}, "dlq")
	ctx := context.Background()
	assert.NoError(t, c.WriteTable(ctx, countIDTable(2)))
	assert.NoError(t, c.Flush(ctx))
	assert.Equal(t, int64(0), flatValue(t, r, lastWrite))
}

// A noop sink delivers nothing anywhere. A windowed pipeline's pipeline sink
// is noop and is handed DuckDB's one-row INSERT count every batch; recording
// that as a write kept a pipeline fresh while its window sink was down.
func TestCounting_ANoopSinkNeverMovesTheLastWrite(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	r := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(r))
	c := NewCountingSink(&countStubSink{}, mp, "noop", SinkRolePipeline)
	ctx := context.Background()
	assert.NoError(t, c.WriteTable(ctx, countIDTable(1)))
	assert.NoError(t, c.Flush(ctx))
	assert.Equal(t, int64(0), flatValue(t, r, lastWrite))
}
