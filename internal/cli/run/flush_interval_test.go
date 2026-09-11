package run

import (
	"testing"
	"time"

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
