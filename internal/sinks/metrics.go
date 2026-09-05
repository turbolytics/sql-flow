package sinks

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

// retryCounter builds the callback the retry ladder reports attempts through.
//
// A stalled sink has to be visible before its deadline fires. Without this, a
// pipeline retrying every batch for the full deadline looks the same on a
// dashboard as one that is merely slow, and the first signal is the pipeline
// exiting.
//
// A nil provider yields a counter that records nothing, so a pipeline started
// without --metrics needs no branch here.
func retryCounter(mp metric.MeterProvider, sinkType string) func(attempt int, err error) {
	if mp == nil {
		mp = noop.NewMeterProvider()
	}

	counter, err := mp.Meter("sqlflow").Int64Counter(
		"sink_retry_count",
		metric.WithDescription("Number of sink writes retried after a failure"),
		metric.WithUnit("count"),
	)
	if err != nil {
		// Losing a counter is not worth failing a pipeline over.
		return func(int, error) {}
	}

	// Built once: the attribute set is the same for every retry this sink
	// makes, and rebuilding it per attempt allocates on a path that is already
	// unhappy.
	attrs := metric.WithAttributes(attribute.String("sink", sinkType))

	return func(attempt int, err error) {
		counter.Add(context.Background(), 1, attrs)
	}
}
