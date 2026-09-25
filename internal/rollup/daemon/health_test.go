package daemon

import (
	"net/http"
	"testing"
	"time"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/turbolytics/sql-flow/internal/errs"
	"github.com/zeebo/assert"
)

func TestCliRollupRun_HealthzRulesInOrder(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")

	now := time.Date(2026, 9, 25, 12, 0, 0, 0, time.UTC)
	interval := 15 * time.Second
	for _, c := range []struct {
		name   string
		s      snapshot
		status string
		code   int
		reason string
	}{
		{"no round trip for three intervals", snapshot{Role: roleLeader, LastContact: now.Add(-46 * time.Second)},
			"failed", http.StatusServiceUnavailable, "no database round trip for 46s"},
		{"failed beats standby", snapshot{Role: roleStandby, LastContact: now.Add(-time.Hour)},
			"failed", http.StatusServiceUnavailable, "no database round trip for 3600s"},
		{"standby", snapshot{Role: roleStandby, LastContact: now},
			"standby", http.StatusOK, "another instance holds the leader lock"},
		{"backfilling", snapshot{Role: roleLeader, LastContact: now, PendingTables: 2, Filling: "posts_by_lang_5m", Remaining: 36 * time.Hour},
			"backfilling", http.StatusOK, "2 tables to fill; posts_by_lang_5m has 1.5 days of history left"},
		{"backfilling before its first chunk", snapshot{Role: roleLeader, LastContact: now, PendingTables: 1},
			"backfilling", http.StatusOK, "1 tables to fill"},
		{"starting", snapshot{Role: roleStarting, LastContact: now},
			"starting", http.StatusOK, "installing and taking the leader lock"},
		{"a new leader that has not read its tables to fill", snapshot{Role: roleLeader, LastContact: now},
			"starting", http.StatusOK, "reading the tables to fill"},
		{"healthy", snapshot{Role: roleLeader, LastContact: now, PendingRead: true},
			"healthy", http.StatusOK, ""},
	} {
		t.Run(c.name, func(t *testing.T) {
			status, reason, code := healthStatus(c.s, now, interval)
			assert.Equal(t, c.status, status)
			assert.Equal(t, c.code, code)
			assert.Equal(t, c.reason, reason)
		})
	}
}

func TestCliRollupRun_TheHealthStateCountsFromItsStart(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")

	start := time.Date(2026, 9, 25, 12, 0, 0, 0, time.UTC)
	h := newHealthState(start)
	assert.Equal(t, roleStarting, h.get().Role)
	// Before the first round trip, the age counts from the process's start.
	status, _, _ := healthStatus(h.get(), start.Add(10*time.Second), 15*time.Second)
	assert.Equal(t, "starting", status)
	h.touch(start.Add(time.Minute))
	h.setRole(roleLeader)
	h.setPending(1)
	h.setFilling("posts_total_5m", 24*time.Hour)
	s := h.get()
	assert.Equal(t, start.Add(time.Minute), s.LastContact)
	assert.Equal(t, "posts_total_5m", s.Filling)
}

func TestCliRollupRun_TheMetricsFlagTakesPrometheusOrNothing(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")

	mp, registry, err := newProvider("")
	assert.NoError(t, err)
	assert.NotNil(t, mp)
	assert.That(t, registry == nil)

	_, registry, err = newProvider("Prometheus")
	assert.NoError(t, err)
	assert.NotNil(t, registry)

	_, _, err = newProvider("statsd")
	assert.Error(t, err)
	assert.Equal(t, errs.CodeConfigInvalid, errs.CodeOf(err))

	_, err = newInstruments(mp)
	assert.NoError(t, err)
}
