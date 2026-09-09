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
	assert.Equal(t, len(entries), 51)

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

// The unit is a pair, not a key. DuckDB's VARCHAR is the universal text
// carrier and ClickHouse reparses text into UUID, Decimal and Enum, so one
// Arrow key reaches destinations that each demand different content.
func TestToolingCoverageRegistry_AColumnCarriesItsOwnValue(t *testing.T) {
	Covers(t, "tooling.coverage")

	declared, err := TypesFor("sink.clickhouse")
	assert.NoError(t, err)

	for _, d := range declared {
		if d.Outcome == "unsupported" {
			continue
		}
		if len(d.Columns) == 0 {
			t.Errorf("%s is %s and names no column", d.Key, d.Outcome)
		}
		for _, c := range d.Columns {
			if c.Type == "" {
				t.Errorf("%s has a column entry with no type", d.Key)
			}
			if c.Expect == "" {
				t.Errorf("%s into %s names no expect", d.Key, c.Type)
			}
			// Only text is reparsed by a destination, so only the utf8 row
			// may override the value it writes.
			if c.Value != "" && d.Key != "utf8" {
				t.Errorf("%s into %s declares a value, and only utf8 may: a "+
					"destination reparses text and nothing else", d.Key, c.Type)
			}
		}
	}
}

// Every lattice key must carry an outcome, or the sink's behaviour on that
// type is undeclared and the matrix cannot tell a gap from a pass.
func TestToolingCoverageRegistry_ClickhouseDeclaresEveryLatticeKey(t *testing.T) {
	Covers(t, "tooling.coverage")

	declared, err := TypesFor("sink.clickhouse")
	assert.NoError(t, err)

	byKey := map[string]TypeDecl{}
	for _, d := range declared {
		byKey[d.Key] = d
	}

	entries, err := Lattice()
	assert.NoError(t, err)
	for _, e := range entries {
		d, ok := byKey[e.Key]
		if !ok {
			t.Errorf("sink.clickhouse declares no outcome for %q", e.Key)
			continue
		}
		switch d.Outcome {
		case "exact":
			if len(d.Columns) == 0 {
				t.Errorf("sink.clickhouse: %q is exact and names no column type", e.Key)
			}
		case "coerced":
			if d.Rule == "" {
				t.Errorf("sink.clickhouse: %q is coerced with no rule", e.Key)
			}
		case "unsupported":
			if d.Code == "" {
				t.Errorf("sink.clickhouse: %q is unsupported with no code", e.Key)
			}
		default:
			t.Errorf("sink.clickhouse: %q has outcome %q", e.Key, d.Outcome)
		}
	}
}
