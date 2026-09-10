package handlers

import (
	"context"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/turbolytics/sql-flow/internal/coverage"
)

// duckdbTableBytes is what DuckDB itself says it is holding for tables on
// this connection. It is the instrument for the growth that Init's TRUNCATE
// leaves behind: the rows are gone, but their row groups are not, and DuckDB
// counts them here until a checkpoint reclaims them.
func duckdbTableBytes(t *testing.T, conn adbc.Connection) int64 {
	t.Helper()
	stmt, err := conn.NewStatement()
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()
	if err := stmt.SetSqlQuery(`SELECT CAST(coalesce(sum(memory_usage_bytes), 0) AS BIGINT)
		FROM duckdb_memory() WHERE tag IN ('IN_MEMORY_TABLE', 'BASE_TABLE')`); err != nil {
		t.Fatal(err)
	}
	reader, _, err := stmt.ExecuteQuery(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Release()
	if !reader.Next() {
		t.Fatal("duckdb_memory() returned no rows")
	}
	return reader.Record().Column(0).(*array.Int64).Value(0)
}

func newSoakStructuredHandler(t *testing.T) (*StructuredBatchHandler, func() int64) {
	t.Helper()
	conn, cleanup := newTestADBCConn(t)
	t.Cleanup(cleanup)

	createTable(t, conn, `CREATE TABLE soak_src (sensor_id BIGINT, ts VARCHAR, value DOUBLE);`)
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "sensor_id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "ts", Type: arrow.BinaryTypes.String},
		{Name: "value", Type: arrow.PrimitiveTypes.Float64},
	}, nil)
	h, err := NewStructuredBatchHandler(conn,
		"SELECT sensor_id, COUNT(*) AS n, AVG(value) AS avg_value FROM soak_src GROUP BY sensor_id",
		"soak_src", schema)
	if err != nil {
		t.Fatal(err)
	}
	if err := h.Init(context.Background()); err != nil {
		t.Fatal(err)
	}
	return h, func() int64 { return duckdbTableBytes(t, conn) }
}

// driveStructured pushes n batches of batchSize messages through the handler
// the way the pipeline does: Write, Invoke, Release, Init.
func driveStructured(t *testing.T, h *StructuredBatchHandler, n, batchSize int) {
	t.Helper()
	ctx := context.Background()
	msg := []byte(`{"sensor_id":3,"ts":"2026-09-11T00:00:00","value":12.5}`)
	for i := 0; i < n; i++ {
		for j := 0; j < batchSize; j++ {
			if err := h.Write(msg); err != nil {
				t.Fatal(err)
			}
		}
		tbl, err := h.Invoke(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if tbl == nil {
			t.Fatal("expected a table")
		}
		tbl.Release()
		if err := h.Init(ctx); err != nil {
			t.Fatal(err)
		}
	}
}

// TestStructuredInit_ReleasesTruncatedStorage: Init empties the declared
// table with TRUNCATE, and TRUNCATE leaves the dead row groups in place. On
// a 60-minute soak at 1,000 messages a second that was about 53 bytes
// retained per message, reported by duckdb_memory() as IN_MEMORY_TABLE for a
// table holding zero rows, and it grew for as long as the process lived.
// A CHECKPOINT after the truncate reclaims all of it. This asserts the
// storage DuckDB holds for the table does not grow across batches.
func TestStructuredInit_ReleasesTruncatedStorage(t *testing.T) {
	coverage.Covers(t, "handler.structured")
	h, tableBytes := newSoakStructuredHandler(t)

	const batchSize, warmup, iters = 1000, 20, 200
	driveStructured(t, h, warmup, batchSize)
	before := tableBytes()
	driveStructured(t, h, iters, batchSize)
	after := tableBytes()

	const limit = 4 << 20
	growth := after - before
	t.Logf("duckdb table storage: before %d KiB, after %d KiB, growth %d KiB over %d rows through an emptied table",
		before>>10, after>>10, growth>>10, iters*batchSize)
	if growth > limit {
		t.Fatalf("DuckDB kept %d KiB of storage for a table Init had emptied; TRUNCATE without a checkpoint retains dead row groups", growth>>10)
	}
}

// TestStructuredInvoke_DoesNotLeakNativeMemory is the process-level view of
// the same path, the instrument that catches whatever DuckDB's own accounting
// does not: half a million messages, and the process must not grow.
func TestStructuredInvoke_DoesNotLeakNativeMemory(t *testing.T) {
	coverage.Covers(t, "handler.structured")
	h, _ := newSoakStructuredHandler(t)

	const batchSize, warmup, iters = 1000, 50, 500
	driveStructured(t, h, warmup, batchSize)
	settle()
	before := residentAnonBytes(t)
	driveStructured(t, h, iters, batchSize)
	settle()
	after := residentAnonBytes(t)

	const limit = 8 << 20
	growth := after - before
	t.Logf("resident anon memory: before %d MiB, after %d MiB, growth %d MiB over %d messages",
		before>>20, after>>20, growth>>20, iters*batchSize)
	if growth > limit {
		t.Fatalf("process grew %d MiB over %d messages; the structured handler is retaining memory", growth>>20, iters*batchSize)
	}
}
