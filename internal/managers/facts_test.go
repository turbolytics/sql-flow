package managers

import (
	"testing"
	"time"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

// Every fact says which clock measured it, so a comparison that mixes event
// time with the engine's clock is visible in review rather than in a chart
// nine months later.
func TestManagerWindow_EveryFactNamesItsClock(t *testing.T) {
	coverage.Covers(t, "manager.window")
	decl := testDecl()
	s := StateOf(decl, t0.Add(decl.Size), true, t0, true, 90*time.Second, time.Hour, true)

	byName := map[string]Fact{}
	for _, f := range FactsOf(decl, s, 90*time.Second, time.Hour, t0.Add(decl.Size), t0) {
		byName[f.Name] = f
	}
	assert.Equal(t, 3, len(byName))

	assert.Equal(t, EventTime, byName["data"].Clock)
	assert.Equal(t, decl.Grace, byName["data"].Limit)
	assert.Equal(t, decl.Size, byName["data"].Measured)

	assert.Equal(t, EngineClock, byName["idle"].Clock)
	assert.Equal(t, decl.IdleClose, byName["idle"].Limit)
	assert.Equal(t, 90*time.Second, byName["idle"].Measured)

	assert.Equal(t, EngineClock, byName["source"].Clock)
	assert.Equal(t, time.Hour, byName["source"].Measured)
}

// A fact's value is the one the state carries, so the log line and the state
// cannot disagree about what was decided.
func TestManagerWindow_AFactCarriesTheStatesValue(t *testing.T) {
	coverage.Covers(t, "manager.window")
	decl := testDecl()
	s := StateOf(decl, t0, true, t0, true, time.Hour, 0, false)

	for _, f := range FactsOf(decl, s, 0, 0, t0, t0) {
		switch f.Name {
		case "data":
			assert.Equal(t, string(s.Data), f.Value)
		case "idle":
			assert.Equal(t, string(s.Idle), f.Value)
		case "source":
			assert.Equal(t, string(s.Source), f.Value)
		}
	}
}

// The rendered line is what an operator reads after an early close, so it
// carries the measurement, the limit and the clock rather than the value
// alone.
func TestManagerWindow_AFactRendersItsEvidence(t *testing.T) {
	coverage.Covers(t, "manager.window")
	f := Fact{Name: "idle", Value: "confirmed", Measured: 90 * time.Second,
		Limit: time.Minute, Clock: EngineClock, From: "sqlflow_progress"}
	assert.Equal(t, "idle=confirmed 1m30s against 1m0s, engine", f.String())

	noLimit := Fact{Name: "source", Value: "delivering", Measured: time.Hour, Clock: EngineClock}
	assert.Equal(t, "source=delivering 1h0m0s, engine", noLimit.String())
}
