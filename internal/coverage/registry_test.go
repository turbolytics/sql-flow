package coverage

import (
	"testing"

	"github.com/zeebo/assert"
)

func TestCoverageRegistry_ListsSinksByBareName(t *testing.T) {
	got, err := Integrations("sink")
	assert.NoError(t, err)

	// The registry's ids are prefixed by kind; a constructor switch's cases
	// are not. The bare name is the form both sides can compare.
	assert.DeepEqual(t, []string{
		"clickhouse", "console", "iceberg", "kafka", "noop", "sqlcommand",
	}, got)
}

func TestCoverageRegistry_ListsSources(t *testing.T) {
	got, err := Integrations("source")
	assert.NoError(t, err)
	assert.DeepEqual(t, []string{"kafka", "webhook", "websocket"}, got)
}

func TestCoverageRegistry_ListsHandlers(t *testing.T) {
	got, err := Integrations("handler")
	assert.NoError(t, err)
	assert.DeepEqual(t, []string{"inferred_disk", "inferred_mem", "structured"}, got)
}

// pipeline is a kind an invariant can apply to, but no constructor builds
// one. Asking for its integrations is a mistake worth naming.
func TestCoverageRegistry_RejectsAKindNothingConstructs(t *testing.T) {
	_, err := Integrations("pipeline")
	assert.Error(t, err)
}

func TestCoverageRegistry_RejectsAnUnknownKind(t *testing.T) {
	_, err := Integrations("router")
	assert.Error(t, err)
}
