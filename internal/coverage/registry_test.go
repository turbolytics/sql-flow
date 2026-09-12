package coverage

import (
	"strings"
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

// --- One file per integration ----------------------------------------------
//
// The registry was one file of 602 lines, seven functions each parsed it into
// a partial struct of their own, and a field reached one caller and not
// another. It is now one file per integration, read once into one type.

func TestToolingCoverageRegistry_ReadsEveryFileInTheDirectory(t *testing.T) {
	Covers(t, "tooling.coverage")
	all, err := loadIntegrations()
	assert.NoError(t, err)

	// Every kind is present, including the two no constructor builds:
	// pipeline configurations and managers. A loader that silently skipped a
	// file would leave an integration with no cells, which is the
	// sink.iceberg failure.
	kinds := map[string]int{}
	for _, entry := range all {
		assert.That(t, entry.ID != "")
		assert.That(t, entry.Kind != "")
		kinds[entry.Kind]++
	}
	assert.Equal(t, 5, len(kinds))
	assert.That(t, kinds["sink"] >= 6)
	assert.That(t, kinds["pipeline"] >= 2)
	assert.That(t, kinds["manager"] >= 1)
}

func TestToolingCoverageRegistry_ReadsInIDOrder(t *testing.T) {
	Covers(t, "tooling.coverage")
	all, err := loadIntegrations()
	assert.NoError(t, err)

	for i := 1; i < len(all); i++ {
		assert.That(t, all[i-1].ID < all[i].ID)
	}
}

// One entry carries every field, so a question about an integration is one
// read. Each of these came from a different partial struct before.
func TestToolingCoverageRegistry_OneEntryCarriesEveryField(t *testing.T) {
	Covers(t, "tooling.coverage")
	entry, err := integration("sink.clickhouse")
	assert.NoError(t, err)

	assert.Equal(t, "sink", entry.Kind)
	assert.Equal(t, "sink.clickhouse", entry.Feature)
	assert.That(t, !entry.TestOnly)
	assert.That(t, len(entry.Types) > 0)
	assert.That(t, len(entry.Nulls) > 0)
}

func TestToolingCoverageRegistry_NamesTheIDItCannotFind(t *testing.T) {
	Covers(t, "tooling.coverage")
	_, err := integration("sink.nothing")
	assert.Error(t, err)
	assert.That(t, strings.Contains(err.Error(), "sink.nothing"))
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
