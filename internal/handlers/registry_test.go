package handlers

import (
	"testing"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

// A handler the engine can build and the registry does not name has no
// invariant cells at all, which is the sink.iceberg failure: nothing written
// down, so nothing can be missing. This runs in the unit pass, in
// milliseconds, so the gap closes before the matrix is ever regenerated.
func TestToolingCoverageHandlerRegistry_MatchesTheConstructorSwitch(t *testing.T) {
	coverage.Covers(t, "tooling.coverage")
	declared, err := coverage.Integrations("handler")
	assert.NoError(t, err)
	assert.DeepEqual(t, declared, Kinds())
}

// The config's names and the registry's are two maps, so they can drift. A
// config type with no builder panics on a nil function rather than reporting
// an unsupported handler.
func TestToolingCoverageHandlerRegistry_EveryConfigTypeHasABuilder(t *testing.T) {
	coverage.Covers(t, "tooling.coverage")
	for configType, kind := range configTypes {
		if _, ok := builders[kind]; !ok {
			t.Errorf("config type %q maps to %q, which has no builder", configType, kind)
		}
	}
	assert.Equal(t, len(builders), len(configTypes))
}

// The bundle reports the short name, and Kind reads the map New dispatches
// on. A second copy in the run command would let a pipeline be built as one
// handler and reported as another.
func TestObservabilityTurbostats_HandlerKindIsTheNameNewDispatchesOn(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	for _, configType := range ConfigTypes() {
		kind := Kind(configType)
		assert.That(t, kind != "")
		_, ok := builders[kind]
		assert.That(t, ok)
	}
	// An unknown handler reports no type rather than a guess.
	assert.Equal(t, "", Kind("handlers.NotAHandler"))
}
