package core

import (
	"context"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

// counting counts the rows a sink accepts and the rows a flush delivers.
//
// The two are separate numbers on purpose. Their difference is the sink's
// buffer depth, derived rather than self-reported, which is why this replaces
// the sink_buffered_rows gauge: a sink can misreport its own depth -- see
// lyingSink in the conformance suite -- and a count of what the pipeline
// watched it accept cannot.
//
// It lives here rather than in internal/sinks because the conformance harness
// has to apply it too, and internal/sinks test files import that harness. A
// conformance -> sinks import would cycle the sinks test binary.
type counting struct {
	inner    Sink
	accepted metric.Int64Counter
	written  metric.Int64Counter
	attrs    metric.AddOption

	mu      sync.Mutex
	pending int64
}

// countingBuffered forwards the optional depth report of a sink that has one.
//
// Go has no conditional interface satisfaction, so the choice is made at
// construction. Skipping this is not cosmetic: the retry wrapper declares only
// WriteTable and Flush, which is why sink_buffered_rows never recorded for the
// ClickHouse and Iceberg sinks it wraps -- a wrapper silently removed the
// interface the gauge depended on.
type countingBuffered struct {
	*counting
	reporter BufferedRowReporter
}

func (c *countingBuffered) BufferedRows() int { return c.reporter.BufferedRows() }

// NewCountingSink wraps a sink so its rows are counted.
//
// A nil provider yields instruments that record nothing, so a pipeline started
// without --metrics needs no branch at the call site. A constructor error
// returns the sink unwrapped: losing a counter is not worth failing a pipeline
// over.
func NewCountingSink(inner Sink, mp metric.MeterProvider, sinkType, role string) Sink {
	if mp == nil {
		mp = noop.NewMeterProvider()
	}
	meter := mp.Meter("sqlflow")

	accepted, err := meter.Int64Counter(
		"sink_rows_accepted",
		metric.WithDescription("Rows the sink buffered, which WriteTable accepted"),
		metric.WithUnit("rows"),
	)
	if err != nil {
		return inner
	}

	written, err := meter.Int64Counter(
		"sink_rows_written",
		metric.WithDescription("Rows the destination acknowledged, which a Flush delivered"),
		metric.WithUnit("rows"),
	)
	if err != nil {
		return inner
	}

	c := &counting{
		inner:    inner,
		accepted: accepted,
		written:  written,
		// Built once: the attribute set is the same for every row this sink
		// counts, and rebuilding it per batch allocates on the hot path.
		attrs: metric.WithAttributes(
			attribute.String("sink", sinkType),
			attribute.String("role", role),
		),
	}

	if reporter, ok := inner.(BufferedRowReporter); ok {
		return &countingBuffered{counting: c, reporter: reporter}
	}
	return c
}

func (c *counting) WriteTable(ctx context.Context, batch arrow.Table) error {
	if err := c.inner.WriteTable(ctx, batch); err != nil {
		return err
	}
	if batch == nil {
		return nil
	}

	n := batch.NumRows()
	c.mu.Lock()
	c.pending += n
	c.mu.Unlock()

	c.accepted.Add(ctx, n, c.attrs)
	return nil
}

// Flush counts on success only.
//
// WriteTable reaches nothing and only Flush does (sink.write.buffers_only), so
// counting at write time would report rows the destination never received --
// the defect #221 fixed. A failed flush keeps its rows buffered
// (sink.flush.keeps_batch), so the pending count carries forward and the retry
// that delivers them counts them once.
func (c *counting) Flush(ctx context.Context) error {
	if err := c.inner.Flush(ctx); err != nil {
		return err
	}

	c.mu.Lock()
	n := c.pending
	c.pending = 0
	c.mu.Unlock()

	if n > 0 {
		c.written.Add(ctx, n, c.attrs)
	}
	return nil
}

// pendingRows reports rows accepted since the last successful flush.
func (c *counting) pendingRows() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.pending
}
