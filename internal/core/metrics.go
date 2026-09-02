package core

import (
	"fmt"

	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

// Metrics holds the instruments the pipeline records, mirroring the names,
// descriptions and units the Python engine exports.
type Metrics struct {
	MessageCount           metric.Int64Counter
	ErrorCount             metric.Int64Counter
	SourceReadLatency      metric.Float64Histogram
	SinkFlushLatency       metric.Float64Histogram
	SinkFlushNumRows       metric.Int64Gauge
	SinkFlushCount         metric.Int64Counter
	BatchProcessingLatency metric.Float64Histogram
	ConsumerLag            metric.Int64Gauge
}

// NewMetrics builds the instruments from a meter provider. Passing a noop
// provider yields instruments that record nothing, so the pipeline needs no
// nil checks.
func NewMetrics(mp metric.MeterProvider) (*Metrics, error) {
	if mp == nil {
		mp = noop.NewMeterProvider()
	}
	meter := mp.Meter("sqlflow")

	var (
		m   Metrics
		err error
	)

	if m.MessageCount, err = meter.Int64Counter(
		"message_count",
		metric.WithDescription("Number of messages processed"),
		metric.WithUnit("messages"),
	); err != nil {
		return nil, fmt.Errorf("message_count: %w", err)
	}

	if m.ErrorCount, err = meter.Int64Counter(
		"error_count",
		metric.WithDescription("Number of errors that occurred during pipeline execution"),
		metric.WithUnit("count"),
	); err != nil {
		return nil, fmt.Errorf("error_count: %w", err)
	}

	if m.SourceReadLatency, err = meter.Float64Histogram(
		"source_read_latency",
		metric.WithDescription("Latency of reading a message from the source"),
		metric.WithUnit("seconds"),
	); err != nil {
		return nil, fmt.Errorf("source_read_latency: %w", err)
	}

	if m.SinkFlushLatency, err = meter.Float64Histogram(
		"sink_flush_latency",
		metric.WithDescription("Latency of flushing data to the sink"),
		metric.WithUnit("seconds"),
	); err != nil {
		return nil, fmt.Errorf("sink_flush_latency: %w", err)
	}

	if m.SinkFlushNumRows, err = meter.Int64Gauge(
		"sink_flush_num_rows",
		metric.WithDescription("Number of rows flushed to the sink"),
		metric.WithUnit("rows"),
	); err != nil {
		return nil, fmt.Errorf("sink_flush_num_rows: %w", err)
	}

	if m.SinkFlushCount, err = meter.Int64Counter(
		"sink_flush_count",
		metric.WithDescription("Number of times sink was flushed, corresponds to the # of batches processed"),
		metric.WithUnit("flushes"),
	); err != nil {
		return nil, fmt.Errorf("sink_flush_count: %w", err)
	}

	if m.BatchProcessingLatency, err = meter.Float64Histogram(
		"batch_processing_latency",
		metric.WithDescription("Latency of processing a batch of data, from first message to flush"),
		metric.WithUnit("seconds"),
	); err != nil {
		return nil, fmt.Errorf("batch_processing_latency: %w", err)
	}

	if m.ConsumerLag, err = meter.Int64Gauge(
		"consumer_lag",
		metric.WithDescription("Messages between the last one processed and the partition's high watermark"),
		metric.WithUnit("messages"),
	); err != nil {
		return nil, fmt.Errorf("consumer_lag: %w", err)
	}

	return &m, nil
}
