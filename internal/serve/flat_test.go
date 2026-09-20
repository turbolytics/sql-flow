package serve

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	prom "github.com/prometheus/client_golang/prometheus"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// flatOf reads every dimensionless int64 point, the way turbostats.Collect
// does.
func flatOf(t *testing.T, r *sdkmetric.ManualReader) map[string]int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	assert.NoError(t, r.Collect(context.Background(), &rm))
	out := map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			switch data := m.Data.(type) {
			case metricdata.Sum[int64]:
				for _, dp := range data.DataPoints {
					if dp.Attributes.Len() == 0 {
						out[m.Name] = dp.Value
					}
				}
			case metricdata.Gauge[int64]:
				for _, dp := range data.DataPoints {
					if dp.Attributes.Len() == 0 {
						out[m.Name] = dp.Value
					}
				}
			}
		}
	}
	return out
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

	flat := flatOf(t, reader)
	assert.Equal(t, int64(4), flat["serve_requests"])
	assert.Equal(t, int64(2), flat["serve_request_errors"])
	assert.That(t, flat["serve_last_request_timestamp"] > 0)
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

	flat := flatOf(t, reader)
	assert.Equal(t, int64(2), flat["serve_cache_hits"])
	assert.Equal(t, int64(1), flat["serve_cache_misses"])
	assert.Equal(t, int64(1), flat["serve_cache_shared"])
	assert.Equal(t, int64(2), flat["serve_cache_evicted"])
}

// A server without a cache publishes nothing about one.
func TestServeFlat_NoCacheInstrumentsWithoutACache(t *testing.T) {
	coverage.Covers(t, "observability.turbostats.serve")
	m, reader, err := newMetrics(nil, noStats, nil)
	assert.NoError(t, err)
	m.observeCache("d", cacheHit)
	m.observeEviction("size")

	for name := range flatOf(t, reader) {
		assert.That(t, !strings.HasPrefix(name, "serve_cache_"))
	}
}

// The defect this fixes: without /metrics the instruments recorded into
// nothing, so a bundle had nothing to read.
func TestServeFlat_RecordsWithMetricsOff(t *testing.T) {
	coverage.Covers(t, "observability.turbostats.serve")
	ts := newTestServerWith(t, testServe)

	assert.Equal(t, http.StatusOK, ts.get(t, "/v1/datasets/status").status)
	assert.Equal(t, http.StatusNotFound, ts.do(t, http.MethodGet, "/metrics", nil).status)

	flat := flatOf(t, ts.srv.reader)
	assert.Equal(t, int64(1), flat["serve_requests"])
	assert.Equal(t, int64(0), flat["serve_request_errors"])
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
