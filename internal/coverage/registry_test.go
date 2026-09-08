package coverage

import (
	"testing"

	"github.com/zeebo/assert"
)

func TestToolingCoverageRegistry_ListsSinksByBareName(t *testing.T) {
	Covers(t, "tooling.coverage")
	got, err := Integrations("sink")
	assert.NoError(t, err)

	// The registry's ids are prefixed by kind; a constructor switch's cases
	// are not. The bare name is the form both sides can compare.
	assert.DeepEqual(t, []string{
		"clickhouse", "console", "iceberg", "kafka", "noop", "sqlcommand",
	}, got)
}

func TestToolingCoverageRegistry_ListsSources(t *testing.T) {
	Covers(t, "tooling.coverage")
	got, err := Integrations("source")
	assert.NoError(t, err)
	assert.DeepEqual(t, []string{"kafka", "webhook", "websocket"}, got)
}

func TestToolingCoverageRegistry_ListsHandlers(t *testing.T) {
	Covers(t, "tooling.coverage")
	got, err := Integrations("handler")
	assert.NoError(t, err)
	assert.DeepEqual(t, []string{"inferred_disk", "inferred_mem", "structured"}, got)
}

// pipeline is a kind an invariant can apply to, but no constructor builds
// one. Asking for its integrations is a mistake worth naming.
func TestToolingCoverageRegistry_RejectsAKindNothingConstructs(t *testing.T) {
	Covers(t, "tooling.coverage")
	_, err := Integrations("pipeline")
	assert.Error(t, err)
}

func TestToolingCoverageRegistry_RejectsAnUnknownKind(t *testing.T) {
	Covers(t, "tooling.coverage")
	_, err := Integrations("router")
	assert.Error(t, err)
}

// The lattice is the closed set every integration's type table is checked
// against. A duplicate key silently drops a row, and a key naming no DuckDB
// cast renders as a blank cell on the integration page.
func TestToolingCoverageRegistry_LatticeIsClosedAndWellFormed(t *testing.T) {
	Covers(t, "tooling.coverage")

	entries, err := Lattice()
	assert.NoError(t, err)
	assert.Equal(t, len(entries), 29)

	seen := map[string]bool{}
	for _, e := range entries {
		if seen[e.Key] {
			t.Errorf("lattice.yml: duplicate key %q", e.Key)
		}
		seen[e.Key] = true

		if len(e.DuckDB) == 0 {
			t.Errorf("lattice.yml: %q names no DuckDB SQL type", e.Key)
		}
		if e.Depth != 1 && e.Depth != 2 {
			t.Errorf("lattice.yml: %q has depth %d, want 1 or 2", e.Key, e.Depth)
		}
	}
}
