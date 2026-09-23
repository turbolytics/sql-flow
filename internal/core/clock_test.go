package core

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

// The turbine reads every instant a decision rests on through one function,
// so a test can place the pipeline anywhere on the clock rather than wait for
// it to arrive there.
func TestTurbineClock_ReadsDecisionInstantsThroughTheInjectedClock(t *testing.T) {
	coverage.Covers(t, "core.consume_loop")
	at := time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)
	tb := NewTurbine(&fakeSource{}, &fakeHandler{}, &fakeSink{}, 1, time.Second,
		&sync.Mutex{}, PipelineErrorPolicies{}, WithClock(func() time.Time { return at }))

	assert.Equal(t, at, tb.now())
	// Seeded after the options run, so the injected clock governs the first
	// instant as well as every later one.
	assert.Equal(t, at, tb.quietSince)
	assert.Equal(t, at.UTC(), tb.stats.StartTime)
}

// Without the option the turbine reads time.Now, which carries a monotonic
// reading. Every duration the quiet clock measures depends on that: UTC()
// strips it, and a wall-clock step then moves the difference.
func TestTurbineClock_DefaultsToTheWallClockWithItsMonotonicReading(t *testing.T) {
	coverage.Covers(t, "core.consume_loop")
	tb := NewTurbine(&fakeSource{}, &fakeHandler{}, &fakeSink{}, 1, time.Second,
		&sync.Mutex{}, PipelineErrorPolicies{})

	assert.That(t, strings.Contains(tb.now().String(), "m=+"))
	assert.That(t, strings.Contains(tb.quietSince.String(), "m=+"))
}
