package sinks

import (
	"context"
	"testing"

	"github.com/turbolytics/sql-flow/internal/errs"
	"github.com/zeebo/assert"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// A stalled sink has to be visible before its deadline fires. Without a
// counter, a pipeline retrying every batch for ten seconds looks identical to
// one that is merely slow.

// retryAttempts reads the recorded value of sink_retry_count.
func retryAttempts(t *testing.T, reader sdkmetric.Reader) int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	assert.NoError(t, reader.Collect(context.Background(), &rm))

	var total int64
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "sink_retry_count" {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("sink_retry_count is %T, want a counter", m.Data)
			}
			for _, dp := range sum.DataPoints {
				total += dp.Value
			}
		}
	}
	return total
}

func newMeteredRetry(inner *flakySink, p RetryPolicy) (*retrying, sdkmetric.Reader) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	r, _ := newTestRetry(inner, p)
	r.onRetry = retryCounter(mp, "clickhouse")
	return r, reader
}

// Two failures then a success is two retries, counted before the batch lands.
func TestMetrics_CountsEveryRetry(t *testing.T) {
	inner := &flakySink{failures: 2, err: errs.New(errs.CodeSinkUnreachable, "refused")}
	r, reader := newMeteredRetry(inner, testPolicy())

	assert.NoError(t, r.Flush(context.Background()))

	assert.Equal(t, int64(2), retryAttempts(t, reader))
}

// A sink that works records nothing, so a nonzero counter always means the
// destination is struggling.
func TestMetrics_SuccessCountsNoRetries(t *testing.T) {
	inner := &flakySink{}
	r, reader := newMeteredRetry(inner, testPolicy())

	assert.NoError(t, r.Flush(context.Background()))

	assert.Equal(t, int64(0), retryAttempts(t, reader))
}

// A rejected write is not retried, so it must not inflate the retry counter.
func TestMetrics_NonRetryableCountsNoRetries(t *testing.T) {
	inner := &flakySink{failures: 99, err: errs.New(errs.CodeSinkWriteFailed, "no such column")}
	r, reader := newMeteredRetry(inner, testPolicy())

	assert.Error(t, r.Flush(context.Background()))

	assert.Equal(t, int64(0), retryAttempts(t, reader))
}

// An exhausted ladder counts every retry it made, which is one fewer than the
// attempts: the last attempt is not followed by another.
func TestMetrics_ExhaustedLadderCountsItsRetries(t *testing.T) {
	p := testPolicy()
	p.MaxAttempts = 4
	inner := &flakySink{failures: 99, err: errs.New(errs.CodeSinkUnreachable, "refused")}
	r, reader := newMeteredRetry(inner, p)

	assert.Error(t, r.Flush(context.Background()))

	assert.Equal(t, int64(3), retryAttempts(t, reader))
}

// A nil provider must not panic. A pipeline started without --metrics still
// retries.
func TestMetrics_NilProviderIsSafe(t *testing.T) {
	inner := &flakySink{failures: 1, err: errs.New(errs.CodeSinkUnreachable, "refused")}
	r, _ := newTestRetry(inner, testPolicy())
	r.onRetry = retryCounter(nil, "clickhouse")

	assert.NoError(t, r.Flush(context.Background()))
}
