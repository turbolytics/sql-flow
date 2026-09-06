package sinks

import (
	"testing"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

// A sink the engine can build and integrations.yml does not name has no
// invariant cells at all, which is the sink.iceberg failure: nothing written
// down, so nothing can be missing. This runs in the unit pass, in
// milliseconds, so the gap closes before the matrix is ever regenerated.
func TestSinkRegistry_MatchesTheConstructorSwitch(t *testing.T) {
	declared, err := coverage.Integrations("sink")
	assert.NoError(t, err)
	assert.DeepEqual(t, declared, Kinds())
}
