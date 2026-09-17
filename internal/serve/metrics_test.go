package serve

import (
	"net/http"
	"strings"
	"testing"

	prom "github.com/prometheus/client_golang/prometheus"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

// The endpoint sits on the public listener and its labels name every dataset,
// so it exists only when the config asks for it.
func TestCliServe_MetricsAreOffUnlessEnabled(t *testing.T) {
	coverage.Covers(t, "cli.serve")

	// Even handed a registry: the config decides, not the caller.
	ts := newTestServerWith(t, testServe, WithMetrics(prom.NewRegistry()))
	assert.Equal(t, http.StatusNotFound, ts.do(t, http.MethodGet, "/metrics", nil).status)
}

// Every instrument carries a sample after one request, so a dashboard built
// against this list is not built against a blank.
func TestCliServe_MetricsCarryEveryInstrument(t *testing.T) {
	coverage.Covers(t, "cli.serve")

	enabled := strings.Replace(testServe, "  limits:", "  metrics: {enabled: true}\n  limits:", 1)
	assert.That(t, enabled != testServe)
	ts := newTestServerWith(t, enabled, WithMetrics(prom.NewRegistry()))

	assert.Equal(t, http.StatusOK, ts.get(t, "/v1/datasets/status").status)

	r := ts.do(t, http.MethodGet, "/metrics", nil)
	assert.Equal(t, http.StatusOK, r.status)
	for _, name := range []string{
		"sqlflow_serve_requests_total",
		"sqlflow_serve_request_duration_seconds",
		"sqlflow_serve_query_duration_seconds",
		"sqlflow_serve_session_wait_seconds",
		"sqlflow_serve_sessions_in_use",
		"sqlflow_serve_sessions_total",
	} {
		if !strings.Contains(r.raw, name) {
			t.Fatalf("/metrics does not carry %s:\n%s", name, r.raw)
		}
	}
	// The gauge reports the pool rather than a constant. The exporter appends
	// otel_scope labels, so the value is read from the end of its line.
	if got := sampleValue(t, r.raw, "sqlflow_serve_sessions_total"); got != "1" {
		t.Fatalf("sessions_total is %q, want 1 for a pool of one:\n%s", got, r.raw)
	}
	// The labels a dashboard breaks the rate down by.
	assert.That(t, strings.Contains(r.raw, `dataset="status"`))
	assert.That(t, strings.Contains(r.raw, `code="ok"`))
}

// sampleValue returns the value of the first sample whose name matches,
// ignoring its labels.
func sampleValue(t *testing.T, text, name string) string {
	t.Helper()
	for _, line := range strings.Split(text, "\n") {
		if !strings.HasPrefix(line, name) || strings.HasPrefix(line, "#") {
			continue
		}
		// A sample is "name{labels} value" or "name value".
		if rest := strings.TrimPrefix(line, name); rest == "" || (rest[0] != '{' && rest[0] != ' ') {
			continue
		}
		fields := strings.Fields(line)
		return fields[len(fields)-1]
	}
	t.Fatalf("no sample named %s", name)
	return ""
}

// labelledValue returns the value of the sample with this name that carries
// every one of labels, whatever order the exporter wrote them in.
func labelledValue(t *testing.T, text, name string, labels ...string) string {
	t.Helper()
next:
	for _, line := range strings.Split(text, "\n") {
		if !strings.HasPrefix(line, name+"{") {
			continue
		}
		for _, l := range labels {
			if !strings.Contains(line, l) {
				continue next
			}
		}
		fields := strings.Fields(line)
		return fields[len(fields)-1]
	}
	t.Fatalf("no sample named %s with %v:\n%s", name, labels, text)
	return ""
}

// The cache's four instruments, and the rule that a hit leaves the query
// histogram alone.
func TestCliServe_CacheMetrics(t *testing.T) {
	coverage.Covers(t, "cli.serve")

	text := strings.Replace(cachedTestServe, "serve:\n", "serve:\n  metrics: {enabled: true}\n", 1)
	ts := newTestServerWith(t, text, WithMetrics(prom.NewRegistry()))

	for i := 0; i < 3; i++ {
		assert.Equal(t, http.StatusOK, ts.get(t, "/v1/datasets/status").status)
	}

	body := ts.do(t, http.MethodGet, "/metrics", nil).raw
	assert.Equal(t, "1", labelledValue(t, body, "sqlflow_serve_cache_requests_total", `dataset="status"`, `outcome="miss"`))
	assert.Equal(t, "2", labelledValue(t, body, "sqlflow_serve_cache_requests_total", `dataset="status"`, `outcome="hit"`))
	assert.Equal(t, "1", sampleValue(t, body, "sqlflow_serve_cache_entries"))
	assert.That(t, sampleValue(t, body, "sqlflow_serve_cache_bytes") != "0")

	// Three requests, one query.
	assert.Equal(t, "3", labelledValue(t, body, "sqlflow_serve_request_duration_seconds_count", `dataset="status"`, `code="ok"`))
	assert.Equal(t, "1", labelledValue(t, body, "sqlflow_serve_query_duration_seconds_count", `dataset="status"`, `code="ok"`))
}

// A server where no dataset opted in publishes nothing about a cache.
func TestCliServe_NoCacheMetricsWithoutACachedDataset(t *testing.T) {
	coverage.Covers(t, "cli.serve")

	enabled := strings.Replace(testServe, "  limits:", "  metrics: {enabled: true}\n  limits:", 1)
	ts := newTestServerWith(t, enabled, WithMetrics(prom.NewRegistry()))
	assert.Equal(t, http.StatusOK, ts.get(t, "/v1/datasets/status").status)
	assert.That(t, !strings.Contains(ts.do(t, http.MethodGet, "/metrics", nil).raw, "sqlflow_serve_cache"))
}
