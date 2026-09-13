package run

import (
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/turbolytics/sql-flow/internal/core"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

// The four states, one table. The first matching rule wins: a recorded
// failure beats everything, then the commit clock, then a retry in flight,
// then a recent error, then a pipeline that has not committed yet.
func TestLifecycleHealth_StatusTable(t *testing.T) {
	coverage.Covers(t, "lifecycle.health")
	now := time.Date(2026, 9, 12, 12, 0, 0, 0, time.UTC)
	interval := 30 * time.Second
	committed := now.Add(-10 * time.Second)

	cases := []struct {
		name      string
		p         core.Progress
		commitAge float64
		snap      healthSnapshot
		status    string
		reason    string
		code      int
	}{
		{"healthy", core.Progress{LastCommit: committed}, 10, healthSnapshot{},
			"healthy", "", http.StatusOK},
		{"starting", core.Progress{}, 5, healthSnapshot{},
			"starting", "no commit yet", http.StatusOK},
		{"stuck is failed", core.Progress{LastCommit: committed}, 91, healthSnapshot{},
			"failed", "no commit for 91s", http.StatusServiceUnavailable},
		{"never committed past the grace", core.Progress{}, 120, healthSnapshot{},
			"failed", "no commit for 120s", http.StatusServiceUnavailable},
		{"retrying", core.Progress{LastCommit: committed}, 10,
			healthSnapshot{retrying: map[string]int{"clickhouse": 2}},
			"degraded", "sink clickhouse is retrying, attempt 2", http.StatusOK},
		{"two sinks retrying names the first by type", core.Progress{LastCommit: committed}, 10,
			healthSnapshot{retrying: map[string]int{"iceberg": 1, "clickhouse": 3}},
			"degraded", "sink clickhouse is retrying, attempt 3", http.StatusOK},
		{"recent error", core.Progress{LastCommit: committed, LastError: now.Add(-5 * time.Second), Errors: 3},
			10, healthSnapshot{},
			"degraded", "3 errors recorded, last 5s ago", http.StatusOK},
		{"an error older than an interval is history", core.Progress{LastCommit: committed, LastError: now.Add(-time.Hour), Errors: 3},
			10, healthSnapshot{},
			"healthy", "", http.StatusOK},
		{"retrying before the first commit is degraded, not starting", core.Progress{}, 5,
			healthSnapshot{retrying: map[string]int{"clickhouse": 1}},
			"degraded", "sink clickhouse is retrying, attempt 1", http.StatusOK},
		{"a failure beats a fresh commit", core.Progress{LastCommit: committed}, 10,
			healthSnapshot{failure: errors.New("[system.sink.write_failed] rejected")},
			"failed", "[system.sink.write_failed] rejected", http.StatusServiceUnavailable},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			coverage.Covers(t, "lifecycle.health")
			status, reason, code := healthStatus(c.p, c.commitAge, c.snap, interval, now)
			assert.Equal(t, c.status, status)
			assert.Equal(t, c.reason, reason)
			assert.Equal(t, c.code, code)
		})
	}
}

// Fail keeps the first failure. A drain that then times out is a consequence,
// and the endpoint should name the cause.
func TestLifecycleHealth_FirstFailureIsKept(t *testing.T) {
	coverage.Covers(t, "lifecycle.health")
	h := newHealth()
	h.Fail(nil)
	assert.Nil(t, h.Snapshot().failure)
	h.Fail(errors.New("first"))
	h.Fail(errors.New("second"))
	assert.Equal(t, "first", h.Snapshot().failure.Error())
}

// A retry is in flight from its first failed attempt until the ladder
// settles. The snapshot is a copy, so a reader cannot race the ladder.
func TestLifecycleHealth_RetryAndSettle(t *testing.T) {
	coverage.Covers(t, "lifecycle.health")
	h := newHealth()
	h.Retry("clickhouse", 1, errors.New("reset"))
	h.Retry("clickhouse", 2, errors.New("reset"))

	snap := h.Snapshot()
	assert.Equal(t, 2, snap.retrying["clickhouse"])

	h.Settle("clickhouse")
	assert.Equal(t, 0, len(h.Snapshot().retrying))
	assert.Equal(t, 2, snap.retrying["clickhouse"])
}
