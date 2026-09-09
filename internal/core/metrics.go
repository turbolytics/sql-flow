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
	HandlerRowsRead        metric.Int64Counter
	ErrorCount             metric.Int64Counter
	SourceReadLatency      metric.Float64Histogram
	SinkFlushLatency       metric.Float64Histogram
	SinkFlushNumRows       metric.Int64Gauge
	SinkBufferedRows       metric.Int64Gauge
	SinkFlushCount         metric.Int64Counter
	BatchProcessingLatency metric.Float64Histogram
	StateCommitLatency     metric.Float64Histogram
	StateCommitCount       metric.Int64Counter
	StateSizeBytes         metric.Int64Gauge
	StateTableRows         metric.Int64Gauge
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

	// The denominator of the enrichment ratio. message_count counts messages,
	// which stops being the same number the moment a message is rejected or a
	// batch fails to ingest.
	if m.HandlerRowsRead, err = meter.Int64Counter(
		"handler_rows_read",
		metric.WithDescription("Rows the handler ingested into the batch table, which the pipeline SQL ran over"),
		metric.WithUnit("rows"),
	); err != nil {
		return nil, fmt.Errorf("handler_rows_read: %w", err)
	}

	// The unit stays "count", which the exporter drops as unitless, so this
	// exports as error_count_total. A descriptive unit would rename it to
	// error_count_errors_total and break every dashboard built on the current
	// name. Measured against the exporter, not inferred.
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

	// A sink holds every row a flush could not deliver, and nothing bounds
	// that buffer. Today a failed flush stops the pipeline, so the buffer dies
	// with the process; the day a flush failure stops being fatal, this gauge
	// is what tells an operator the buffer is growing rather than draining.
	if m.SinkBufferedRows, err = meter.Int64Gauge(
		"sink_buffered_rows",
		metric.WithDescription("Rows the sink is holding that no flush has delivered yet"),
		metric.WithUnit("rows"),
	); err != nil {
		return nil, fmt.Errorf("sink_buffered_rows: %w", err)
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

	if m.StateCommitLatency, err = meter.Float64Histogram(
		"state_commit_latency",
		metric.WithDescription("Latency of committing state and offsets together"),
		metric.WithUnit("seconds"),
	); err != nil {
		return nil, fmt.Errorf("state_commit_latency: %w", err)
	}

	if m.StateCommitCount, err = meter.Int64Counter(
		"state_commit_count",
		metric.WithDescription("Number of state transactions committed"),
		metric.WithUnit("commits"),
	); err != nil {
		return nil, fmt.Errorf("state_commit_count: %w", err)
	}

	if m.StateSizeBytes, err = meter.Int64Gauge(
		"state_db_size_bytes",
		metric.WithDescription("Size on disk of the pipeline's state database"),
		metric.WithUnit("By"),
	); err != nil {
		return nil, fmt.Errorf("state_db_size_bytes: %w", err)
	}

	if m.StateTableRows, err = meter.Int64Gauge(
		"state_table_rows",
		metric.WithDescription("Rows in each managed state table"),
		metric.WithUnit("rows"),
	); err != nil {
		return nil, fmt.Errorf("state_table_rows: %w", err)
	}

	return &m, nil
}
