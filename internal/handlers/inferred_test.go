package handlers

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/zeebo/assert"
)

// invokeRows runs the handler over messages and returns the result table.
func invokeRows(t *testing.T, h *InferredMemBatchHandler, messages []string) arrow.Table {
	t.Helper()
	assert.NoError(t, h.Init(context.Background()))
	for _, m := range messages {
		assert.NoError(t, h.Write([]byte(m)))
	}
	res, err := h.Invoke(context.Background())
	assert.NoError(t, err)
	return res
}

func TestInferredMemBatchHandler_NestedStructAggregation(t *testing.T) {
	conn, cleanup := newTestADBCConn(t)
	defer cleanup()

	h, err := NewInferredMemBatchHandler(conn,
		"SELECT properties.city as city, COUNT(*) as city_count FROM batch GROUP BY city ORDER BY city")
	assert.NoError(t, err)

	res := invokeRows(t, h, []string{
		`{"event": "click", "properties": {"city": "NYC"}}`,
		`{"event": "view", "properties": {"city": "SF"}}`,
		`{"event": "click", "properties": {"city": "NYC"}}`,
	})
	defer res.Release()

	// 2 distinct cities: NYC, SF
	assert.Equal(t, int64(2), res.NumRows())
}

func TestInferredMemBatchHandler_InfersNumericAndBoolTypes(t *testing.T) {
	conn, cleanup := newTestADBCConn(t)
	defer cleanup()

	// SUM over BIGINT yields HUGEINT in DuckDB; cast so the assertions read
	// plain int64 columns.
	h, err := NewInferredMemBatchHandler(conn,
		"SELECT CAST(SUM(count) AS BIGINT) as total, SUM(ratio) as ratio_total, COUNT(*) FILTER (WHERE active) as num_active FROM batch")
	assert.NoError(t, err)

	res := invokeRows(t, h, []string{
		`{"count": 2, "ratio": 1.5, "active": true}`,
		`{"count": 3, "ratio": 2.25, "active": false}`,
	})
	defer res.Release()

	assert.Equal(t, int64(1), res.NumRows())
	total := res.Column(0).Data().Chunk(0).(*array.Int64).Value(0)
	assert.Equal(t, int64(5), total)
	numActive := res.Column(2).Data().Chunk(0).(*array.Int64).Value(0)
	assert.Equal(t, int64(1), numActive)
}

func TestInferredMemBatchHandler_FieldMissingFromSomeRows(t *testing.T) {
	conn, cleanup := newTestADBCConn(t)
	defer cleanup()

	h, err := NewInferredMemBatchHandler(conn,
		"SELECT COUNT(city) as with_city, COUNT(*) as total FROM batch")
	assert.NoError(t, err)

	res := invokeRows(t, h, []string{
		`{"event": "a"}`,
		`{"event": "b", "city": "NYC"}`,
	})
	defer res.Release()

	assert.Equal(t, int64(1), res.NumRows())
}

func TestInferredMemBatchHandler_InvalidJSONIsWriteError(t *testing.T) {
	conn, cleanup := newTestADBCConn(t)
	defer cleanup()

	h, err := NewInferredMemBatchHandler(conn, "SELECT * FROM batch")
	assert.NoError(t, err)
	assert.NoError(t, h.Init(context.Background()))

	err = h.Write([]byte(`{!invalidJSON!`))
	assert.Error(t, err)
}

func TestInferredMemBatchHandler_SuccessiveBatches(t *testing.T) {
	conn, cleanup := newTestADBCConn(t)
	defer cleanup()

	h, err := NewInferredMemBatchHandler(conn, "SELECT COUNT(*) as cnt FROM batch")
	assert.NoError(t, err)

	first := invokeRows(t, h, []string{`{"a": 1}`, `{"a": 2}`})
	assert.Equal(t, int64(1), first.NumRows())
	first.Release()

	// A second batch must not see the first batch's rows, and may carry a
	// different shape.
	second := invokeRows(t, h, []string{`{"b": "x"}`})
	assert.Equal(t, int64(1), second.NumRows())
	second.Release()
}

func TestInferredMemBatchHandler_SingleRecord(t *testing.T) {
	conn, cleanup := newTestADBCConn(t)
	defer cleanup()

	h, err := NewInferredMemBatchHandler(conn, "SELECT * FROM batch")
	assert.NoError(t, err)
	assert.NoError(t, h.Init(context.Background()))

	assert.NoError(t, h.Write([]byte(`{"id": 1, "name": "alice"}`)))

	res, err := h.Invoke(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, int64(1), res.NumRows())
	assert.Equal(t, 2, res.Schema().NumFields())
	res.Release()
}
