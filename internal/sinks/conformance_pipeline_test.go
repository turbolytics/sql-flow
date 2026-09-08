package sinks

// The consume loop against a real destination.
//
// It lives here rather than in internal/core because the Iceberg catalog
// helpers do, and building one from outside this package would duplicate the
// property translation the sink already owns.
//
// The other pipeline subjects run on doubles and judge the order the loop does
// things. That is a mechanism: a rewrite that committed optimistically and
// rolled back would fail those checks while keeping the guarantee. This one
// judges the guarantee, by counting rows a real Iceberg table actually holds
// against the position the pipeline committed.
//
// No container. The catalog is sqlite and the warehouse is a directory, so the
// fault is a warehouse the sink cannot write into, and the whole thing runs in
// the unit pass.

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/turbolytics/sql-flow/internal/conformance"
	"github.com/turbolytics/sql-flow/internal/core"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

func TestPipelineStatefulRealSink_Conformance(t *testing.T) {
	coverage.Covers(t, "state.durability")

	runPipelineAgainstIceberg(t, "pipeline.stateful", true,
		func(r *conformance.Recorder) []core.TurbineOption {
			return []core.TurbineOption{core.WithStateStore(r.Offsets(), r.Tx())}
		})
}

// The same loop with no durable state. It still must never commit offsets for
// rows the sink did not write, so it needs a real destination too: the
// doubles-based subject can only judge the order of events.
func TestPipelineStatelessRealSink_Conformance(t *testing.T) {
	coverage.Covers(t, "core.consume_loop")

	runPipelineAgainstIceberg(t, "pipeline.stateless", false,
		func(*conformance.Recorder) []core.TurbineOption { return nil })
}

// runPipelineAgainstIceberg drives one configuration against a real Iceberg
// table, with its own catalog and warehouse so the configurations cannot see
// each other's rows.
func runPipelineAgainstIceberg(t *testing.T, integration string, keepsState bool,
	options func(*conformance.Recorder) []core.TurbineOption) {
	t.Helper()

	isolateCatalogLookup(t)

	// Two directories: Break makes the warehouse unwritable, and the catalog
	// has to stay readable for the read-back to work.
	warehouse := t.TempDir()
	catalogDir := t.TempDir()

	const catalogName = "pipelinetest"
	t.Setenv("PYICEBERG_CATALOG__PIPELINETEST__URI",
		"sqlite:///"+filepath.Join(catalogDir, "catalog.db"))
	t.Setenv("PYICEBERG_CATALOG__PIPELINETEST__WAREHOUSE", "file://"+warehouse)

	ctx := context.Background()
	props, err := icebergCatalogProperties(catalogName)
	assert.NoError(t, err)

	cat, err := catalog.Load(ctx, catalogName, props)
	assert.NoError(t, err)
	assert.NoError(t, cat.CreateNamespace(ctx, catalog.ToIdentifier("default"), nil))

	// The harness's handler emits a single int64 "id" column.
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64},
	)
	_, err = cat.CreateTable(ctx, catalog.ToIdentifier("default", "rows"), schema)
	assert.NoError(t, err)

	conformance.Pipelines(t, conformance.PipelineSubject{
		Integration: integration,
		KeepsState:  keepsState,
		Options:     options,

		NewSink: func(t *testing.T) core.Sink {
			s, err := NewIcebergSink(ctx, catalogName, "default.rows")
			assert.NoError(t, err)
			return s
		},

		// A warehouse the sink cannot write into: a full volume, a revoked
		// credential, a read-only mount.
		Break: func(t *testing.T) { chmodTree(t, warehouse, 0o500) },
		Heal:  func(t *testing.T) { chmodTree(t, warehouse, 0o700) },

		ReadBack: func(t *testing.T) []conformance.Row {
			return icebergIDs(t, catalogName)
		},
	})
}
