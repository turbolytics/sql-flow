package run

import (
	"testing"
	"time"

	"github.com/turbolytics/sql-flow/internal/core"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

// The invariant registry claimed a zero flush interval removes the ticker,
// leaving a batch a low-traffic topic never fills to wait forever. It does
// not: the run command has always defaulted it. This pins that for every
// value a config can produce, so the claim and the code agree.
func TestCliInvocation_FlushIntervalNeverZero(t *testing.T) {
	coverage.Covers(t, "cli.invocation")
	// Absent from the config decodes as zero.
	assert.Equal(t, 30*time.Second, flushIntervalFor(0))
	assert.Equal(t, 30*time.Second, flushIntervalFor(-5))
	assert.Equal(t, 45*time.Second, flushIntervalFor(45))
	assert.Equal(t, time.Second, flushIntervalFor(1))
}

// The drain deadline resolves the way the flush interval does. A config that
// omits it, or writes a nonsense value past the schema, still gets a bound.
func TestLifecycleDrain_DeadlineDefaultsWhenAbsent(t *testing.T) {
	coverage.Covers(t, "lifecycle.drain")
	assert.Equal(t, core.DefaultDrainDeadline, drainDeadlineFor(0))
	assert.Equal(t, core.DefaultDrainDeadline, drainDeadlineFor(-5))
	assert.Equal(t, 45*time.Second, drainDeadlineFor(45))
}
