package serve

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	prom "github.com/prometheus/client_golang/prometheus"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/turbolytics/sql-flow/internal/turbostats"
	"github.com/zeebo/assert"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// bundleOf reads the instruments the way a bundle does: through
// turbostats.Collect and its real reader. A copy of that reader here would
// keep passing after the real one broke, and the flat series exist for the
// bundle alone, so the bundle is what these tests assert on.
func bundleOf(t *testing.T, r *sdkmetric.ManualReader, withCache bool) *turbostats.Serve {
	t.Helper()
	src := &turbostats.ServeSource{Sessions: func() (int, int) { return 0, 1 }}
	if withCache {
		src.Cache = func() (int64, int) { return 0, 0 }
	}
	b, err := turbostats.Collect(context.Background(), turbostats.Source{Reader: r, Serve: src})
	assert.NoError(t, err)
	assert.That(t, b.Serve != nil)
	return b.Serve
}

// instrumentNames lists what the provider holds, by name alone. One test asks
// whether an instrument exists at all, which no bundle field can say.
func instrumentNames(t *testing.T, r *sdkmetric.ManualReader) []string {
	t.Helper()
	var rm metricdata.ResourceMetrics
	assert.NoError(t, r.Collect(context.Background(), &rm))
	var names []string
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			names = append(names, m.Name)
		}
	}
	return names
}

func noStats() Stats { return Stats{Size: 1} }

// A 5xx is the server's failure and a 4xx is the caller's. The choice is made
// here, where the request is recorded, so the bundle's reader sums nothing.
func TestServeFlat_A5xxIsAnErrorAndA4xxIsNot(t *testing.T) {
	coverage.Covers(t, "observability.turbostats.serve")
	m, reader, err := newMetrics(nil, noStats, nil)
	assert.NoError(t, err)

	m.observeRequest("d", "1h", "ok", http.StatusOK, time.Millisecond, true, time.Millisecond)
	m.observeRequest("d", "1h", "bad_request", http.StatusBadRequest, 0, false, time.Millisecond)
	m.observeRequest("d", "1h", "internal", http.StatusInternalServerError, 0, false, time.Millisecond)
	m.observeRequest("d", "1h", "timeout", http.StatusGatewayTimeout, 0, false, time.Millisecond)

	sv := bundleOf(t, reader, false)
	assert.Equal(t, int64(4), sv.RequestCount)
	assert.Equal(t, int64(2), sv.RequestErrorCount)
	assert.That(t, sv.LastRequestAt != nil)
}

func TestServeFlat_CacheOutcomesAndEvictionsEachHaveATotal(t *testing.T) {
	coverage.Covers(t, "observability.turbostats.serve")
	m, reader, err := newMetrics(nil, noStats, func() (int64, int) { return 0, 0 })
	assert.NoError(t, err)

	m.observeCache("d", cacheHit)
	m.observeCache("d", cacheHit)
	m.observeCache("d", cacheMiss)
	m.observeCache("d", cacheShared)
	m.observeEviction("size")
	m.observeEviction("expired")

	c := bundleOf(t, reader, true).Cache
	assert.That(t, c != nil)
	assert.Equal(t, int64(2), c.HitCount)
	assert.Equal(t, int64(1), c.MissCount)
	assert.Equal(t, int64(1), c.SharedCount)
	assert.Equal(t, int64(2), c.EvictionCount)
}

// A server without a cache publishes nothing about one.
func TestServeFlat_NoCacheInstrumentsWithoutACache(t *testing.T) {
	coverage.Covers(t, "observability.turbostats.serve")
	m, reader, err := newMetrics(nil, noStats, nil)
	assert.NoError(t, err)
	m.observeCache("d", cacheHit)
	m.observeEviction("size")

	for _, name := range instrumentNames(t, reader) {
		assert.That(t, !strings.HasPrefix(name, "serve_cache_"))
		assert.That(t, !strings.HasPrefix(name, "sqlflow_serve_cache_"))
	}
}

// The defect this fixes: without /metrics the instruments recorded into
// nothing, so a bundle had nothing to read.
func TestServeFlat_RecordsWithMetricsOff(t *testing.T) {
	coverage.Covers(t, "observability.turbostats.serve")
	ts := newTestServerWith(t, testServe)

	assert.Equal(t, http.StatusOK, ts.get(t, "/v1/datasets/status").status)
	assert.Equal(t, http.StatusNotFound, ts.do(t, http.MethodGet, "/metrics", nil).status)

	sv := bundleOf(t, ts.srv.reader, false)
	assert.Equal(t, int64(1), sv.RequestCount)
	assert.Equal(t, int64(0), sv.RequestErrorCount)
}

// One instrument feeds both readers. The flat names must not collide with the
// attributed ones once the exporter appends _total, and a collision surfaces
// when the registry is gathered, not when the instrument is created.
func TestServeFlat_ExportsBesideTheAttributedSeries(t *testing.T) {
	coverage.Covers(t, "observability.turbostats.serve")
	enabled := strings.Replace(testServe, "  limits:", "  metrics: {enabled: true}\n  limits:", 1)
	assert.That(t, enabled != testServe)
	ts := newTestServerWith(t, enabled, WithMetrics(prom.NewRegistry()))

	assert.Equal(t, http.StatusOK, ts.get(t, "/v1/datasets/status").status)
	r := ts.do(t, http.MethodGet, "/metrics", nil)
	assert.Equal(t, http.StatusOK, r.status)
	for _, name := range []string{"serve_requests_total", "sqlflow_serve_requests_total"} {
		if !strings.Contains(r.raw, "\n"+name+"{") && !strings.Contains(r.raw, "\n"+name+" ") {
			t.Fatalf("/metrics does not carry %s:\n%s", name, r.raw)
		}
	}
}
