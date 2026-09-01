package handlers

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/turbolytics/sql-flow/internal/core"
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

// The Python engine builds the batch with pyarrow.Table.from_pylist, which
// takes the column set from the first row. These tests hold the Go handler to
// the same contract.

func TestInferredMemBatchHandler_ColumnsComeFromFirstRow(t *testing.T) {
	conn, cleanup := newTestADBCConn(t)
	defer cleanup()

	h, err := NewInferredMemBatchHandler(conn, "SELECT * FROM batch")
	assert.NoError(t, err)

	// "city" appears only in the second row, so it is not a column.
	res := invokeRows(t, h, []string{
		`{"event": "a"}`,
		`{"event": "b", "city": "NYC"}`,
	})
	defer res.Release()

	assert.Equal(t, int64(2), res.NumRows())
	assert.Equal(t, 1, res.Schema().NumFields())
	assert.Equal(t, "event", res.Schema().Field(0).Name)
}

func TestInferredMemBatchHandler_FieldMissingFromLaterRowIsNull(t *testing.T) {
	conn, cleanup := newTestADBCConn(t)
	defer cleanup()

	h, err := NewInferredMemBatchHandler(conn,
		"SELECT COUNT(city) as with_city, COUNT(*) as total FROM batch")
	assert.NoError(t, err)

	res := invokeRows(t, h, []string{
		`{"event": "a", "city": "NYC"}`,
		`{"event": "b"}`,
	})
	defer res.Release()

	withCity := res.Column(0).Data().Chunk(0).(*array.Int64).Value(0)
	total := res.Column(1).Data().Chunk(0).(*array.Int64).Value(0)
	assert.Equal(t, int64(1), withCity)
	assert.Equal(t, int64(2), total)
}

func TestInferredMemBatchHandler_PromotesIntToFloat(t *testing.T) {
	conn, cleanup := newTestADBCConn(t)
	defer cleanup()

	h, err := NewInferredMemBatchHandler(conn, "SELECT SUM(x) as total FROM batch")
	assert.NoError(t, err)

	res := invokeRows(t, h, []string{`{"x": 1}`, `{"x": 2.5}`})
	defer res.Release()

	total := res.Column(0).Data().Chunk(0).(*array.Float64).Value(0)
	assert.Equal(t, 3.5, total)
}

func TestInferredMemBatchHandler_ConflictingTypesError(t *testing.T) {
	conn, cleanup := newTestADBCConn(t)
	defer cleanup()

	h, err := NewInferredMemBatchHandler(conn, "SELECT * FROM batch")
	assert.NoError(t, err)
	assert.NoError(t, h.Init(context.Background()))

	// pyarrow raises ArrowInvalid here; the batch must fail rather than
	// silently null the value, so the error policy can route it.
	assert.NoError(t, h.Write([]byte(`{"x": 1}`)))
	assert.NoError(t, h.Write([]byte(`{"x": "hello"}`)))

	_, err = h.Invoke(context.Background())
	assert.Error(t, err)
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

// A handler SQL statement may be an INSERT that populates a managed table
// rather than a SELECT that returns rows, as the tumbling window config does.
func TestInferredMemBatchHandler_InsertIntoManagedTable(t *testing.T) {
	conn, cleanup := newTestADBCConn(t)
	defer cleanup()

	createTable(t, conn, `CREATE TABLE agg (city VARCHAR, count INT);`)

	h, err := NewInferredMemBatchHandler(conn,
		`INSERT INTO agg BY NAME SELECT properties.city as city, COUNT(*) as count FROM batch GROUP BY properties.city`)
	assert.NoError(t, err)

	res := invokeRows(t, h, []string{
		`{"properties": {"city": "NYC"}}`,
		`{"properties": {"city": "NYC"}}`,
		`{"properties": {"city": "SF"}}`,
	})
	if res != nil {
		res.Release()
	}

	stmt, err := conn.NewStatement()
	assert.NoError(t, err)
	defer stmt.Close()
	assert.NoError(t, stmt.SetSqlQuery("SELECT COUNT(*) FROM agg"))
	reader, _, err := stmt.ExecuteQuery(context.Background())
	assert.NoError(t, err)
	defer reader.Release()

	var rows int64 = -1
	for reader.Next() {
		rec := reader.Record()
		if rec.NumRows() > 0 {
			rows = rec.Column(0).(*array.Int64).Value(0)
		}
	}
	// Two distinct cities were aggregated into the managed table.
	assert.Equal(t, int64(2), rows)
}

// The Kafka source can supply per-message metadata, which the Python engine
// injects as kafka_topic / kafka_partition / kafka_offset columns. The
// idempotent MotherDuck pattern reads them to skip already-ingested offsets.
func TestInferredMemBatchHandler_InjectsKafkaMetadata(t *testing.T) {
	conn, cleanup := newTestADBCConn(t)
	defer cleanup()

	h, err := NewInferredMemBatchHandler(conn,
		"SELECT city, kafka_topic, kafka_partition, kafka_offset FROM batch ORDER BY kafka_offset")
	assert.NoError(t, err)
	assert.NoError(t, h.Init(context.Background()))

	assert.NoError(t, h.WriteMessage(core.Message{
		Value: []byte(`{"city": "NYC"}`), Topic: "events", Partition: 2, Offset: 100,
	}))
	assert.NoError(t, h.WriteMessage(core.Message{
		Value: []byte(`{"city": "SF"}`), Topic: "events", Partition: 2, Offset: 101,
	}))

	res, err := h.Invoke(context.Background())
	assert.NoError(t, err)
	defer res.Release()

	assert.Equal(t, int64(2), res.NumRows())
	assert.Equal(t, "events", res.Column(1).Data().Chunk(0).(*array.String).Value(0))
	assert.Equal(t, int32(2), res.Column(2).Data().Chunk(0).(*array.Int32).Value(0))
	assert.Equal(t, int64(100), res.Column(3).Data().Chunk(0).(*array.Int64).Value(0))
	assert.Equal(t, int64(101), res.Column(3).Data().Chunk(0).(*array.Int64).Value(1))
}

// Without metadata the columns must not appear, so a plain config is unchanged.
func TestInferredMemBatchHandler_NoMetadataColumnsWithoutSource(t *testing.T) {
	conn, cleanup := newTestADBCConn(t)
	defer cleanup()

	h, err := NewInferredMemBatchHandler(conn, "SELECT * FROM batch")
	assert.NoError(t, err)

	res := invokeRows(t, h, []string{`{"city": "NYC"}`})
	defer res.Release()

	assert.Equal(t, 1, res.Schema().NumFields())
}

// A batch can reach Invoke with nothing buffered: the pipeline counts a
// message it consumed even when the handler rejected it, so a batch whose
// messages were all malformed JSON arrives here empty. That is not an error,
// it is a batch with no rows -- and erroring turns an IGNORE policy into a
// stream of spurious handler failures.
func TestInferredMemBatchHandler_EmptyBatchIsNoOp(t *testing.T) {
	conn, cleanup := newTestADBCConn(t)
	defer cleanup()

	h, err := NewInferredMemBatchHandler(conn, "SELECT * FROM batch")
	assert.NoError(t, err)
	assert.NoError(t, h.Init(context.Background()))

	res, err := h.Invoke(context.Background())
	assert.NoError(t, err)
	assert.Nil(t, res)
}

// The batch that follows an empty one must still work.
func TestInferredMemBatchHandler_BatchAfterEmptyBatch(t *testing.T) {
	conn, cleanup := newTestADBCConn(t)
	defer cleanup()

	h, err := NewInferredMemBatchHandler(conn, "SELECT city FROM batch")
	assert.NoError(t, err)

	assert.NoError(t, h.Init(context.Background()))
	res, err := h.Invoke(context.Background())
	assert.NoError(t, err)
	assert.Nil(t, res)

	res = invokeRows(t, h, []string{`{"city": "NYC"}`})
	defer res.Release()
	assert.Equal(t, int64(1), res.NumRows())
}
