package handlers

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/zeebo/assert"
)

// tableToPylist renders a result table the way the Python tests compare it,
// as one map per row, so expectations can be written inline.
func tableToPylist(t *testing.T, table arrow.Table) []map[string]any {
	t.Helper()

	var rows []map[string]any
	reader := array.NewTableReader(table, 0)
	defer reader.Release()

	for reader.Next() {
		rec := reader.Record()
		if rec.NumRows() == 0 {
			continue
		}
		for i := int64(0); i < rec.NumRows(); i++ {
			row := map[string]any{}
			for c, f := range rec.Schema().Fields() {
				b, err := json.Marshal(rec.Column(c).GetOneForMarshal(int(i)))
				assert.NoError(t, err)
				var v any
				assert.NoError(t, json.Unmarshal(b, &v))
				row[f.Name] = v
			}
			rows = append(rows, row)
		}
	}
	assert.NoError(t, reader.Err())
	return rows
}

var flatMessages = [][]byte{
	[]byte(`{"event":"search","city":"New York","user_id":"123412ds"}`),
	[]byte(`{"event":"search","city":"New York","user_id":"123412ds"}`),
	[]byte(`{"event":"search","city":"Baltimore","user_id":"123412ds"}`),
}

func newTestDiskHandler(t *testing.T, sql string) (*InferredDiskBatchHandler, string) {
	t.Helper()

	conn, cleanup := newTestADBCConn(t)
	t.Cleanup(cleanup)

	// A per-test directory keeps the fixed consumer_batch.json / out.json
	// names from colliding across parallel packages.
	dir := filepath.Join(t.TempDir(), "resultscache")

	h, err := NewInferredDiskBatchHandler(conn, sql, dir)
	assert.NoError(t, err)
	t.Cleanup(func() { h.Close() })

	return h, dir
}

func TestInferredDiskBatchHandler_SingleRowReturn(t *testing.T) {
	h, _ := newTestDiskHandler(t, "SELECT COUNT(*) as num_rows FROM batch")

	ctx := context.Background()
	assert.NoError(t, h.Init(ctx))
	for _, msg := range flatMessages {
		assert.NoError(t, h.Write(msg))
	}

	res, err := h.Invoke(ctx)
	assert.NoError(t, err)
	defer res.Release()

	assert.DeepEqual(t, []map[string]any{{"num_rows": float64(3)}}, tableToPylist(t, res))
}

func TestInferredDiskBatchHandler_NestedReturn(t *testing.T) {
	h, _ := newTestDiskHandler(t, `
SELECT
    {'city': city} as s1,
    {'event': event} as nested_event
FROM batch`)

	ctx := context.Background()
	assert.NoError(t, h.Init(ctx))
	for _, msg := range flatMessages {
		assert.NoError(t, h.Write(msg))
	}

	res, err := h.Invoke(ctx)
	assert.NoError(t, err)
	defer res.Release()

	assert.DeepEqual(t, []map[string]any{
		{"s1": map[string]any{"city": "New York"}, "nested_event": map[string]any{"event": "search"}},
		{"s1": map[string]any{"city": "New York"}, "nested_event": map[string]any{"event": "search"}},
		{"s1": map[string]any{"city": "Baltimore"}, "nested_event": map[string]any{"event": "search"}},
	}, tableToPylist(t, res))
}

// The cache dir is the handler's to manage: a fresh checkout has no
// /tmp/sqlflow/resultscache, and the Python engine's open() would fail there.
func TestInferredDiskBatchHandler_CreatesCacheDir(t *testing.T) {
	h, dir := newTestDiskHandler(t, "SELECT COUNT(*) as num_rows FROM batch")

	info, err := os.Stat(dir)
	assert.NoError(t, err)
	assert.True(t, info.IsDir())

	ctx := context.Background()
	assert.NoError(t, h.Init(ctx))
	assert.NoError(t, h.Write(flatMessages[0]))

	res, err := h.Invoke(ctx)
	assert.NoError(t, err)
	res.Release()
}

// Each batch must start from an empty buffer and an absent `batch` table:
// the second invoke sees only its own messages, and CREATE TABLE batch
// would fail outright if the previous invoke had not dropped it.
func TestInferredDiskBatchHandler_ResetsBetweenBatches(t *testing.T) {
	h, _ := newTestDiskHandler(t, "SELECT COUNT(*) as num_rows FROM batch")

	ctx := context.Background()

	assert.NoError(t, h.Init(ctx))
	for _, msg := range flatMessages {
		assert.NoError(t, h.Write(msg))
	}
	res, err := h.Invoke(ctx)
	assert.NoError(t, err)
	assert.DeepEqual(t, []map[string]any{{"num_rows": float64(3)}}, tableToPylist(t, res))
	res.Release()

	assert.NoError(t, h.Init(ctx))
	assert.NoError(t, h.Write(flatMessages[0]))
	res, err = h.Invoke(ctx)
	assert.NoError(t, err)
	assert.DeepEqual(t, []map[string]any{{"num_rows": float64(1)}}, tableToPylist(t, res))
	res.Release()
}

func TestInferredDiskBatchHandler_BatchTableDroppedAfterInvoke(t *testing.T) {
	h, _ := newTestDiskHandler(t, "SELECT COUNT(*) as num_rows FROM batch")

	ctx := context.Background()
	assert.NoError(t, h.Init(ctx))
	assert.NoError(t, h.Write(flatMessages[0]))

	res, err := h.Invoke(ctx)
	assert.NoError(t, err)
	res.Release()

	stmt, err := h.conn.NewStatement()
	assert.NoError(t, err)
	defer stmt.Close()
	assert.NoError(t, stmt.SetSqlQuery("SELECT COUNT(*) FROM duckdb_tables() WHERE table_name = 'batch'"))
	reader, _, err := stmt.ExecuteQuery(ctx)
	assert.NoError(t, err)
	defer reader.Release()

	assert.True(t, reader.Next())
	assert.Equal(t, int64(0), reader.Record().Column(0).(*array.Int64).Value(0))
}

// Malformed JSON is rejected per message, matching InferredMemBatch: a bad
// line buffered to disk would otherwise fail the whole batch at read_json.
func TestInferredDiskBatchHandler_RejectsInvalidJSON(t *testing.T) {
	h, _ := newTestDiskHandler(t, "SELECT COUNT(*) as num_rows FROM batch")

	assert.NoError(t, h.Init(context.Background()))
	assert.Error(t, h.Write([]byte(`{"event": `)))
}

func TestInferredDiskBatchHandler_EmptyBatchErrors(t *testing.T) {
	h, _ := newTestDiskHandler(t, "SELECT COUNT(*) as num_rows FROM batch")

	ctx := context.Background()
	assert.NoError(t, h.Init(ctx))

	_, err := h.Invoke(ctx)
	assert.Error(t, err)
}

// A query matching no rows is a normal outcome, not an error: COPY writes an
// empty file, which must come back as an empty table rather than a read
// failure that would fail the batch and stall the source offset.
func TestInferredDiskBatchHandler_EmptyResult(t *testing.T) {
	h, _ := newTestDiskHandler(t, "SELECT city FROM batch WHERE city = 'Nowhere'")

	ctx := context.Background()
	assert.NoError(t, h.Init(ctx))
	for _, msg := range flatMessages {
		assert.NoError(t, h.Write(msg))
	}

	res, err := h.Invoke(ctx)
	assert.NoError(t, err)
	defer res.Release()
	assert.Equal(t, int64(0), res.NumRows())
}

// Close removes the staged files; leaving a many-MB consumer_batch.json in
// the cache dir after shutdown is a leak.
func TestInferredDiskBatchHandler_CloseRemovesFiles(t *testing.T) {
	h, dir := newTestDiskHandler(t, "SELECT COUNT(*) as num_rows FROM batch")

	ctx := context.Background()
	assert.NoError(t, h.Init(ctx))
	assert.NoError(t, h.Write(flatMessages[0]))
	res, err := h.Invoke(ctx)
	assert.NoError(t, err)
	res.Release()

	assert.NoError(t, h.Close())

	for _, name := range []string{"consumer_batch.json", "out.json"} {
		_, err := os.Stat(filepath.Join(dir, name))
		assert.True(t, os.IsNotExist(err))
	}
}
