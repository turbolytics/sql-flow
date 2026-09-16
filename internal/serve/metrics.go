package serve

import (
	"context"
	"time"

	prom "github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

// meterName names serve's instruments apart from the pipeline's, which run
// under their own meter in the same process shape.
const meterName = "sqlflow.serve"

// latencyBuckets span a 1 ms query and a request that sat out a 10 s timeout,
// so both land in a bucket rather than in the overflow.
var latencyBuckets = []float64{
	0.001, 0.002, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 4, 8, 16,
}

// metrics is the six instruments the pool needs to be sized, and nothing
// else. Every method tolerates a nil receiver, so the request path records
// unconditionally and a server without the endpoint pays one nil check.
type metrics struct {
	requests        metric.Int64Counter
	requestDuration metric.Float64Histogram
	queryDuration   metric.Float64Histogram
	sessionWait     metric.Float64Histogram
}

// newMetrics builds the instruments against reg and registers the gauges'
// callback, which reads stats whenever the endpoint is scraped.
func newMetrics(reg *prom.Registry, stats func() Stats) (*metrics, error) {
	exp, err := prometheus.New(prometheus.WithRegisterer(reg))
	if err != nil {
		return nil, err
	}
	// One view, so every histogram here gets buckets chosen for this workload
	// rather than the SDK's defaults, which stop at 10 s.
	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(exp),
		sdkmetric.WithView(sdkmetric.NewView(
			sdkmetric.Instrument{Kind: sdkmetric.InstrumentKindHistogram},
			sdkmetric.Stream{Aggregation: sdkmetric.AggregationExplicitBucketHistogram{
				Boundaries: latencyBuckets,
			}},
		)),
	)
	m := mp.Meter(meterName)

	var mm metrics
	if mm.requests, err = m.Int64Counter("sqlflow_serve_requests_total",
		metric.WithDescription("Requests answered, by dataset, grain and outcome.")); err != nil {
		return nil, err
	}
	if mm.requestDuration, err = m.Float64Histogram("sqlflow_serve_request_duration_seconds",
		metric.WithDescription("What a caller waited, including time queued for a session."),
		metric.WithUnit("s")); err != nil {
		return nil, err
	}
	if mm.queryDuration, err = m.Float64Histogram("sqlflow_serve_query_duration_seconds",
		metric.WithDescription("The query alone, so a slow backend is distinguishable from a full pool."),
		metric.WithUnit("s")); err != nil {
		return nil, err
	}
	if mm.sessionWait, err = m.Float64Histogram("sqlflow_serve_session_wait_seconds",
		metric.WithDescription("Time spent waiting for a session. This is what sizes the pool: a p99 that climbs while query duration stays flat means too few sessions."),
		metric.WithUnit("s")); err != nil {
		return nil, err
	}

	inUse, err := m.Int64ObservableGauge("sqlflow_serve_sessions_in_use",
		metric.WithDescription("Sessions currently borrowed. Pinned at the total means saturated."))
	if err != nil {
		return nil, err
	}
	total, err := m.Int64ObservableGauge("sqlflow_serve_sessions_total",
		metric.WithDescription("Sessions the executor holds, so a dashboard need not hardcode the config."))
	if err != nil {
		return nil, err
	}
	if _, err := m.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		s := stats()
		o.ObserveInt64(inUse, int64(s.InUse))
		o.ObserveInt64(total, int64(s.Size))
		return nil
	}, inUse, total); err != nil {
		return nil, err
	}

	return &mm, nil
}

// observeWait records one Acquire, including the ones that gave up waiting.
func (m *metrics) observeWait(d time.Duration) {
	if m == nil {
		return
	}
	m.sessionWait.Record(context.Background(), d.Seconds())
}

// observeRequest records one finished request. code is the error code, or
// "ok".
func (m *metrics) observeRequest(dataset, grain, code string, query, total time.Duration) {
	if m == nil {
		return
	}
	attrs := metric.WithAttributes(
		attribute.String("dataset", dataset),
		attribute.String("grain", grain),
		attribute.String("code", code),
	)
	ctx := context.Background()
	m.requests.Add(ctx, 1, attrs)
	m.requestDuration.Record(ctx, total.Seconds(), attrs)
	m.queryDuration.Record(ctx, query.Seconds(), attrs)
}
