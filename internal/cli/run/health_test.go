package run

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/turbolytics/sql-flow/internal/core"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

// Idle is healthy: the commit clock keeps moving while nothing arrives, and
// that is the whole point. A pipeline on a trickle looks dead to anything
// watching message counts, so the health check watches commits instead.
// Three intervals without one is failed, and the reason says how old it is.
//
// This endpoint answered "ok" and "stuck" before it had four states. The HTTP
// codes for those two did not change, so a probe written against them keeps
// working; only the status string in the body did.
func TestLifecycleHealth_HealthzTellsIdleFromStuck(t *testing.T) {
	coverage.Covers(t, "lifecycle.health")
	clock := time.Date(2026, 9, 10, 12, 0, 0, 0, time.UTC)
	now := func() time.Time { return clock }
	p := core.Progress{
		LastArrival: clock.Add(-time.Hour),
		LastCommit:  clock.Add(-20 * time.Second),
		Messages:    42,
	}
	mux := newHTTPMux(nil, nil, nil, func() core.Progress { return p }, nil, 30*time.Second, now)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	get := func(path string) (int, map[string]any) {
		resp, err := http.Get(srv.URL + path)
		assert.NoError(t, err)
		defer resp.Body.Close()
		var body map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&body)
		return resp.StatusCode, body
	}

	// An hour with no message, a commit twenty seconds ago: healthy.
	code, body := get("/healthz")
	assert.Equal(t, http.StatusOK, code)
	assert.Equal(t, "healthy", body["status"])
	_, hasReason := body["reason"]
	assert.That(t, !hasReason)

	// /stats carries both ages, so a dashboard can show "quiet for an hour"
	// without calling that an outage.
	_, stats := get("/stats")
	prog := stats["progress"].(map[string]any)
	assert.Equal(t, float64(42), prog["messages"])
	assert.Equal(t, float64(3600), prog["arrival_age_seconds"])
	assert.Equal(t, float64(20), prog["commit_age_seconds"])

	// Three intervals without a commit: failed.
	p.LastCommit = clock.Add(-91 * time.Second)
	code, body = get("/healthz")
	assert.Equal(t, http.StatusServiceUnavailable, code)
	assert.Equal(t, "failed", body["status"])
	assert.Equal(t, "no commit for 91s", body["reason"])
	assert.Equal(t, float64(91), body["commit_age_seconds"])

	// Nothing recorded yet, which is every pipeline for its first interval.
	// Starting, measured from when the server started, not from the epoch.
	p = core.Progress{}
	code, body = get("/healthz")
	assert.Equal(t, http.StatusOK, code)
	assert.Equal(t, "starting", body["status"])
	assert.Equal(t, float64(0), body["commit_age_seconds"])

	// A pipeline that never commits is stuck once the grace runs out, even
	// though it has never recorded anything.
	clock = clock.Add(2 * time.Minute)
	code, body = get("/healthz")
	assert.Equal(t, http.StatusServiceUnavailable, code)
	assert.Equal(t, "failed", body["status"])
}

// The endpoint reads the process's health as well as its progress. A failure
// run records turns a pipeline that committed a second ago into a 503 whose
// reason is the error, and a ladder in flight is degraded with a 200.
func TestLifecycleHealth_EndpointReportsFailuresAndRetries(t *testing.T) {
	coverage.Covers(t, "lifecycle.health")
	clock := time.Date(2026, 9, 12, 12, 0, 0, 0, time.UTC)
	now := func() time.Time { return clock }
	p := core.Progress{LastCommit: clock.Add(-time.Second)}
	h := newHealth()
	mux := newHTTPMux(nil, nil, nil, func() core.Progress { return p }, h.Snapshot,
		30*time.Second, now)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	get := func() (int, map[string]any) {
		resp, err := http.Get(srv.URL + "/healthz")
		assert.NoError(t, err)
		defer resp.Body.Close()
		var body map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&body)
		return resp.StatusCode, body
	}

	h.Retry("clickhouse", 2, errors.New("connection reset by peer"))
	code, body := get()
	assert.Equal(t, http.StatusOK, code)
	assert.Equal(t, "degraded", body["status"])
	assert.Equal(t, "sink clickhouse is retrying, attempt 2", body["reason"])

	h.Settle("clickhouse")
	code, body = get()
	assert.Equal(t, http.StatusOK, code)
	assert.Equal(t, "healthy", body["status"])

	h.Fail(errors.New("[system.sink.write_failed] rejected"))
	code, body = get()
	assert.Equal(t, http.StatusServiceUnavailable, code)
	assert.Equal(t, "failed", body["status"])
	assert.Equal(t, "[system.sink.write_failed] rejected", body["reason"])
}

// Without a progress source there is no health to report, and the endpoint
// must not exist rather than answer "ok" for a pipeline it cannot see.
func TestObservabilityMetrics_HealthzAbsentWithoutProgress(t *testing.T) {
	coverage.Covers(t, "observability.metrics")
	mux := newHTTPMux(nil, nil, nil, nil, nil, 30*time.Second, time.Now)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/healthz")
	assert.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}
