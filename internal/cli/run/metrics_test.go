package run

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/turbolytics/sql-flow/internal/core"
	"github.com/zeebo/assert"
)

// DuckDB takes an exclusive lock on the state file, so a second process
// cannot read it while the pipeline holds it -- not even read-only. A running
// pipeline therefore has to serve its own stats.
func TestObservabilityMetrics_StatsHandler_ReportsState(t *testing.T) {
	want := &core.StateStats{
		Path:      "/state/state.db",
		SizeBytes: 4096,
		Tables:    []core.TableStat{{Name: "agg", Rows: 24}},
		Offsets:   []core.OffsetStat{{Topic: "events", Partition: 0, Offset: 999, LeaderEpoch: 7}},
	}

	mux := newHTTPMux(nil, func() (*core.StateStats, error) { return want, nil })

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/stats", nil))
	assert.Equal(t, http.StatusOK, rec.Code)

	var got map[string]any
	assert.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))

	state, ok := got["state"].(map[string]any)
	assert.That(t, ok)
	assert.Equal(t, "/state/state.db", state["path"])
	assert.Equal(t, float64(4096), state["size_bytes"])

	tables, ok := state["tables"].([]any)
	assert.That(t, ok)
	assert.Equal(t, float64(24), tables[0].(map[string]any)["rows"])

	offsets, ok := state["offsets"].([]any)
	assert.That(t, ok)
	assert.Equal(t, float64(999), offsets[0].(map[string]any)["offset"])
}

// A pipeline with no state path still answers, with a null state block. The
// endpoint stays useful for the counters even when nothing is durable.
func TestObservabilityMetrics_StatsHandler_NullStateWithoutAStateDatabase(t *testing.T) {
	mux := newHTTPMux(nil, func() (*core.StateStats, error) { return nil, nil })

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/stats", nil))
	assert.Equal(t, http.StatusOK, rec.Code)

	var got map[string]any
	assert.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
	assert.That(t, got["state"] == nil)
}

// A failure reading state is reported as a server error rather than an empty
// success, so a monitoring system sees the problem instead of a healthy-looking
// blank.
func TestObservabilityMetrics_StatsHandler_ReportsCollectionFailure(t *testing.T) {
	mux := newHTTPMux(nil, func() (*core.StateStats, error) {
		return nil, errors.New("state database unreadable")
	})

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/stats", nil))
	assert.Equal(t, http.StatusInternalServerError, rec.Code)
}

// With no stats provider at all -- metrics enabled but state unwired -- the
// endpoint must not be registered as a half-working route.
func TestObservabilityMetrics_StatsHandler_AbsentWithoutAProvider(t *testing.T) {
	mux := newHTTPMux(nil, nil)

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/stats", nil))
	assert.Equal(t, http.StatusNotFound, rec.Code)
}
