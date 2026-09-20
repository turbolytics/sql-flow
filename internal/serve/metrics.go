package serve

import (
	"context"
	"net/http"
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

// metrics is the six instruments the pool needs to be sized, and the cache's
// four when a dataset opted into it, and nothing else. Every method tolerates a nil receiver, so the request path records
// unconditionally and a server without the endpoint pays one nil check.
type metrics struct {
	requests        metric.Int64Counter
	requestDuration metric.Float64Histogram
	queryDuration   metric.Float64Histogram
	sessionWait     metric.Float64Histogram
	// cacheRequests is nil on a server where no dataset opted into the cache.
	cacheRequests  metric.Int64Counter
	cacheEvictions metric.Int64Counter

	// The flat twins. Each is the dimensionless series the TurboStats bundle
	// reads, recorded beside the attributed instrument it shadows, so which
	// measurements count is decided here and the bundle's reader sums
	// nothing. The cache ones are nil without a cache.
	flatRequests      metric.Int64Counter
	flatRequestErrors metric.Int64Counter
	flatLastRequest   metric.Int64Gauge
	flatCacheHits     metric.Int64Counter
	flatCacheMisses   metric.Int64Counter
	flatCacheShared   metric.Int64Counter
	flatCacheEvicted  metric.Int64Counter
}

// newMetrics builds the provider every instrument records into.
//
// The manual reader is attached always: it is what the TurboStats bundle
// reads. The Prometheus exporter is a second reader, attached only when reg
// is non-nil. Both read the same instruments. Before this the provider
// existed only for /metrics, and without it every instrument recorded into
// nothing, so a bundle had nothing to read. `sqlflow run` had the same defect;
// see newMeterProvider in internal/cli/run/metrics.go.
//
// The gauges' callbacks read stats and cacheStats whenever a reader collects.
// cacheStats is nil for a server without a cache.
func newMetrics(reg *prom.Registry, stats func() Stats, cacheStats func() (int64, int)) (*metrics, *sdkmetric.ManualReader, error) {
	reader := sdkmetric.NewManualReader()
	opts := []sdkmetric.Option{
		sdkmetric.WithReader(reader),
		// One view, so every histogram here gets buckets chosen for this
		// workload rather than the SDK's defaults, which stop at 10 s.
		sdkmetric.WithView(sdkmetric.NewView(
			sdkmetric.Instrument{Kind: sdkmetric.InstrumentKindHistogram},
			sdkmetric.Stream{Aggregation: sdkmetric.AggregationExplicitBucketHistogram{
				Boundaries: latencyBuckets,
			}},
		)),
	}
	if reg != nil {
		exp, err := prometheus.New(prometheus.WithRegisterer(reg))
		if err != nil {
			return nil, nil, err
		}
		opts = append(opts, sdkmetric.WithReader(exp))
	}
	mp := sdkmetric.NewMeterProvider(opts...)
	m := mp.Meter(meterName)

	var mm metrics
	var err error
	if mm.requests, err = m.Int64Counter("sqlflow_serve_requests_total",
		metric.WithDescription("Requests answered, by dataset, grain and outcome.")); err != nil {
		return nil, nil, err
	}
	if mm.requestDuration, err = m.Float64Histogram("sqlflow_serve_request_duration_seconds",
		metric.WithDescription("What a caller waited, including time queued for a session."),
		metric.WithUnit("s")); err != nil {
		return nil, nil, err
	}
	if mm.queryDuration, err = m.Float64Histogram("sqlflow_serve_query_duration_seconds",
		metric.WithDescription("The query alone, so a slow backend is distinguishable from a full pool."),
		metric.WithUnit("s")); err != nil {
		return nil, nil, err
	}
	if mm.sessionWait, err = m.Float64Histogram("sqlflow_serve_session_wait_seconds",
		metric.WithDescription("Time spent waiting for a session. This is what sizes the pool: a p99 that climbs while query duration stays flat means too few sessions."),
		metric.WithUnit("s")); err != nil {
		return nil, nil, err
	}

	if mm.flatRequests, err = m.Int64Counter("serve_requests",
		metric.WithDescription("Dataset requests answered, any dataset, any outcome. The TurboStats bundle's request_count.")); err != nil {
		return nil, nil, err
	}
	if mm.flatRequestErrors, err = m.Int64Counter("serve_request_errors",
		metric.WithDescription("Dataset requests answered 5xx. A 4xx is the caller's error and is not counted.")); err != nil {
		return nil, nil, err
	}
	if mm.flatLastRequest, err = m.Int64Gauge("serve_last_request_timestamp",
		metric.WithDescription("When a dataset request was last answered, Unix seconds."),
		metric.WithUnit("s")); err != nil {
		return nil, nil, err
	}

	inUse, err := m.Int64ObservableGauge("sqlflow_serve_sessions_in_use",
		metric.WithDescription("Sessions currently borrowed. Pinned at the total means saturated."))
	if err != nil {
		return nil, nil, err
	}
	total, err := m.Int64ObservableGauge("sqlflow_serve_sessions_total",
		metric.WithDescription("Sessions the executor holds, so a dashboard need not hardcode the config."))
	if err != nil {
		return nil, nil, err
	}
	if _, err := m.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		s := stats()
		o.ObserveInt64(inUse, int64(s.InUse))
		o.ObserveInt64(total, int64(s.Size))
		return nil
	}, inUse, total); err != nil {
		return nil, nil, err
	}

	// A server where no dataset opted in has no cache, and publishes nothing
	// about one.
	if cacheStats != nil {
		if mm.cacheRequests, err = m.Int64Counter("sqlflow_serve_cache_requests_total",
			metric.WithDescription("Requests to cached datasets, by dataset and by whether the answer was a hit, a miss, or shared with a query already running.")); err != nil {
			return nil, nil, err
		}
		if mm.cacheEvictions, err = m.Int64Counter("sqlflow_serve_cache_evictions_total",
			metric.WithDescription("Entries dropped, by reason: size, when the bound needed the room, or expired.")); err != nil {
			return nil, nil, err
		}
		if mm.flatCacheHits, err = m.Int64Counter("serve_cache_hits",
			metric.WithDescription("Cached-dataset requests answered from the cache, any dataset.")); err != nil {
			return nil, nil, err
		}
		if mm.flatCacheMisses, err = m.Int64Counter("serve_cache_misses",
			metric.WithDescription("Cached-dataset requests that ran the query, any dataset.")); err != nil {
			return nil, nil, err
		}
		if mm.flatCacheShared, err = m.Int64Counter("serve_cache_shared",
			metric.WithDescription("Cached-dataset requests that shared a query already running, any dataset.")); err != nil {
			return nil, nil, err
		}
		if mm.flatCacheEvicted, err = m.Int64Counter("serve_cache_evicted",
			metric.WithDescription("Entries dropped from the cache, any reason.")); err != nil {
			return nil, nil, err
		}
		cacheBytes, err := m.Int64ObservableGauge("sqlflow_serve_cache_bytes",
			metric.WithDescription("Bytes of encoded results held. After a quiet spell this includes expired entries, until the next store reclaims them."),
			metric.WithUnit("By"))
		if err != nil {
			return nil, nil, err
		}
		cacheEntries, err := m.Int64ObservableGauge("sqlflow_serve_cache_entries",
			metric.WithDescription("Results held, counted the way the bytes are."))
		if err != nil {
			return nil, nil, err
		}
		if _, err := m.RegisterCallback(func(_ context.Context, o metric.Observer) error {
			bytes, entries := cacheStats()
			o.ObserveInt64(cacheBytes, bytes)
			o.ObserveInt64(cacheEntries, int64(entries))
			return nil
		}, cacheBytes, cacheEntries); err != nil {
			return nil, nil, err
		}
	}

	return &mm, reader, nil
}

// observeEviction counts one entry leaving the cache.
func (m *metrics) observeEviction(reason string) {
	if m == nil || m.cacheEvictions == nil {
		return
	}
	ctx := context.Background()
	m.cacheEvictions.Add(ctx, 1, metric.WithAttributes(attribute.String("reason", reason)))
	m.flatCacheEvicted.Add(ctx, 1)
}

// observeWait records one Acquire, including the ones that gave up waiting.
func (m *metrics) observeWait(d time.Duration) {
	if m == nil {
		return
	}
	m.sessionWait.Record(context.Background(), d.Seconds())
}

// observeCache counts one cached dataset's request by how it was answered.
func (m *metrics) observeCache(dataset string, outcome cacheOutcome) {
	if m == nil || m.cacheRequests == nil {
		return
	}
	ctx := context.Background()
	m.cacheRequests.Add(ctx, 1, metric.WithAttributes(
		attribute.String("dataset", dataset),
		attribute.String("outcome", string(outcome)),
	))
	switch outcome {
	case cacheHit:
		m.flatCacheHits.Add(ctx, 1)
	case cacheMiss:
		m.flatCacheMisses.Add(ctx, 1)
	case cacheShared:
		m.flatCacheShared.Add(ctx, 1)
	}
}

// observeRequest records one finished request. code is the error code, or
// "ok". status is the HTTP status written. ran is whether the request ran a
// query: a cache hit did not, and recording its zero would flatten the query
// histogram.
func (m *metrics) observeRequest(dataset, grain, code string, status int, query time.Duration, ran bool, total time.Duration) {
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
	if ran {
		m.queryDuration.Record(ctx, query.Seconds(), attrs)
	}

	m.flatRequests.Add(ctx, 1)
	// A 5xx is the server's failure. A 4xx is the caller's, and counting it
	// would let one misbehaving client paint the server unhealthy.
	if status >= http.StatusInternalServerError {
		m.flatRequestErrors.Add(ctx, 1)
	}
	m.flatLastRequest.Record(ctx, time.Now().Unix())
}
