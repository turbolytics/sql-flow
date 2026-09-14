package handlers

import (
	"context"
	"os"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/drivermgr"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

// TestStructuredInit_SkipsACheckpointAnotherWriterHolds: a window closes and
// publishes on DuckDB connections of its own (#281), so another connection
// can hold a write transaction at the moment Init checkpoints. DuckDB refuses
// that checkpoint, and Init returned the refusal, which stopped the pipeline.
// The bluesky demo exited that way minutes into its first deploy of
// v2026.09.14. Init skips a refused checkpoint, and the next one reclaims
// what the skipped ones left.
func TestStructuredInit_SkipsACheckpointAnotherWriterHolds(t *testing.T) {
	coverage.Covers(t, "handler.structured")
	ctx := context.Background()

	lib := os.Getenv("SQLFLOW_DUCKDB_LIB")
	if lib == "" {
		lib = "/opt/homebrew/lib/libduckdb.dylib"
	}
	var drv drivermgr.Driver
	db, err := drv.NewDatabase(map[string]string{"driver": lib, "entrypoint": "duckdb_adbc_init"})
	assert.NoError(t, err)
	defer db.Close()
	conn, err := db.Open(ctx)
	assert.NoError(t, err)
	defer conn.Close()

	createTable(t, conn, `CREATE TABLE soak_src (sensor_id BIGINT, ts VARCHAR, value DOUBLE);`)
	createTable(t, conn, `CREATE TABLE sqlflow_windows (watermark BIGINT);`)
	createTable(t, conn, `INSERT INTO sqlflow_windows VALUES (0);`)
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "sensor_id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "ts", Type: arrow.BinaryTypes.String},
		{Name: "value", Type: arrow.PrimitiveTypes.Float64},
	}, nil)
	h, err := NewStructuredBatchHandler(conn,
		"SELECT sensor_id, COUNT(*) AS n FROM soak_src GROUP BY sensor_id", "soak_src", schema)
	assert.NoError(t, err)
	assert.NoError(t, h.Init(ctx))

	const batchSize = 1000
	driveStructured(t, h, 20, batchSize)
	before := duckdbTableBytes(t, conn)

	// The window's connection saving its watermark: autocommit off, an
	// UPDATE of a committed row, not yet committed. DuckDB refuses a
	// checkpoint while it is open. So does the sqlcommand sink's DROP and
	// CREATE of its batch table; an INSERT or a DELETE does not.
	window, err := db.Open(ctx)
	assert.NoError(t, err)
	defer window.Close()
	assert.NoError(t, window.(adbc.PostInitOptions).SetOption(adbc.OptionKeyAutoCommit, adbc.OptionValueDisabled))
	createTable(t, window, `UPDATE sqlflow_windows SET watermark = 1`)

	// driveStructured fails the test on any Init error.
	driveStructured(t, h, 50, batchSize)
	assert.That(t, h.CheckpointSkipped())
	held := duckdbTableBytes(t, conn)
	assert.That(t, held-before > 1<<20)

	assert.NoError(t, window.(interface{ Commit(context.Context) error }).Commit(ctx))
	driveStructured(t, h, 1, batchSize)
	assert.That(t, !h.CheckpointSkipped())
	after := duckdbTableBytes(t, conn)

	t.Logf("duckdb table storage: before %d KiB, while the window's write was open %d KiB, after it committed %d KiB",
		before>>10, held>>10, after>>10)
	if after-before > 1<<20 {
		t.Fatalf("the first checkpoint after the writer committed left %d KiB unreclaimed", (after-before)>>10)
	}
}
