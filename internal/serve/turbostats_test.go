package serve

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/turbolytics/sql-flow/internal/turbostats"
	"github.com/turbolytics/sql-flow/turbostats/wire"
	"github.com/zeebo/assert"
)

var testStatic = turbostats.Static{
	Name:       "test-serve",
	Version:    "v0.0.0",
	Commit:     "abc1234",
	ConfigHash: "sha256:00",
	StartedAt:  time.Date(2026, 9, 1, 8, 0, 0, 0, time.UTC),
}

// A process does not answer on a route unless told to.
func TestServeTurbostats_IsOffUnlessAsked(t *testing.T) {
	coverage.Covers(t, "observability.turbostats.serve")
	ts := newTestServerWith(t, testServe)
	assert.Equal(t, http.StatusNotFound, ts.do(t, http.MethodGet, "/turbostats/v1", nil).status)
}

// End to end through the real server: a request is answered, the flat series
// moves, and the bundle the route serves says so. This is the test that
// catches the two packages disagreeing about an instrument name.
func TestServeTurbostats_ServesAServeSection(t *testing.T) {
	coverage.Covers(t, "observability.turbostats.serve")
	ts := newTestServerWith(t, testServe, WithTurbostats(testStatic))

	assert.Equal(t, http.StatusOK, ts.get(t, "/v1/datasets/status").status)

	r := ts.do(t, http.MethodGet, "/turbostats/v1", nil)
	assert.Equal(t, http.StatusOK, r.status)

	var b wire.Bundle
	assert.NoError(t, json.Unmarshal([]byte(r.raw), &b))
	assert.Equal(t, 1, b.V)
	assert.Equal(t, "test-serve", b.Instance.Name)
	assert.That(t, b.Pipeline == nil)
	assert.That(t, b.Serve != nil)
	assert.Equal(t, int64(1), b.Serve.RequestCount)
	assert.Equal(t, int64(0), b.Serve.RequestErrorCount)
	// newTestServerWith builds a pool of one.
	assert.Equal(t, 1, b.Serve.SessionsTotal)
	assert.That(t, b.Serve.LastRequestAt != nil)
	assert.That(t, b.LastActivityAt != nil)
	// testServe opts no dataset into the cache.
	assert.That(t, b.Serve.Cache == nil)
}
