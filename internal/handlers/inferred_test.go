package handlers

import (
	"context"
	"strings"
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

// JSON arrays become Arrow lists, as pyarrow.Table.from_pylist produces them.
// Every Bluesky post record carries at least one -- langs, facets, or
// embed.images -- so a firehose config cannot run without these.

func TestInferredMemBatchHandler_InfersListOfStrings(t *testing.T) {
	conn, cleanup := newTestADBCConn(t)
	defer cleanup()

	h, err := NewInferredMemBatchHandler(conn,
		"SELECT CAST(SUM(len(langs)) AS BIGINT) as total FROM batch")
	assert.NoError(t, err)

	res := invokeRows(t, h, []string{
		`{"langs": ["en", "ja"]}`,
		`{"langs": ["fr"]}`,
	})
	defer res.Release()

	total := res.Column(0).Data().Chunk(0).(*array.Int64).Value(0)
	assert.Equal(t, int64(3), total)
}

func TestInferredMemBatchHandler_InfersListOfStructs(t *testing.T) {
	conn, cleanup := newTestADBCConn(t)
	defer cleanup()

	// The Bluesky facets shape: a list of structs, each holding a nested
	// struct and a list of structs of its own.
	h, err := NewInferredMemBatchHandler(conn,
		"SELECT facets[1].index.byteStart as start, facets[1].features[1].tag as tag FROM batch")
	assert.NoError(t, err)

	res := invokeRows(t, h, []string{
		`{"facets": [{"features": [{"tag": "pits"}], "index": {"byteStart": 15, "byteEnd": 20}}]}`,
	})
	defer res.Release()

	assert.Equal(t, int64(15), res.Column(0).Data().Chunk(0).(*array.Int64).Value(0))
	assert.Equal(t, "pits", res.Column(1).Data().Chunk(0).(*array.String).Value(0))
}

func TestInferredMemBatchHandler_InfersListInsideStruct(t *testing.T) {
	conn, cleanup := newTestADBCConn(t)
	defer cleanup()

	// Bluesky nests the arrays several structs deep, under commit.record.
	h, err := NewInferredMemBatchHandler(conn,
		"SELECT commit.record.langs[1] as lang FROM batch")
	assert.NoError(t, err)

	res := invokeRows(t, h, []string{
		`{"commit": {"record": {"langs": ["en", "ja"]}}}`,
	})
	defer res.Release()

	assert.Equal(t, "en", res.Column(0).Data().Chunk(0).(*array.String).Value(0))
}

func TestInferredMemBatchHandler_PromotesListElementTypeAcrossBatch(t *testing.T) {
	conn, cleanup := newTestADBCConn(t)
	defer cleanup()

	h, err := NewInferredMemBatchHandler(conn, "SELECT SUM(list_sum(x)) as total FROM batch")
	assert.NoError(t, err)

	res := invokeRows(t, h, []string{`{"x": [1, 2]}`, `{"x": [2.5]}`})
	defer res.Release()

	assert.Equal(t, 5.5, res.Column(0).Data().Chunk(0).(*array.Float64).Value(0))
}

// An empty array in the first message carries no element type. pyarrow yields
// list<item: null>; a later message that has elements must widen it rather
// than fail the batch.
func TestInferredMemBatchHandler_EmptyListTakesTypeFromLaterMessage(t *testing.T) {
	conn, cleanup := newTestADBCConn(t)
	defer cleanup()

	h, err := NewInferredMemBatchHandler(conn,
		"SELECT CAST(SUM(len(langs)) AS BIGINT) as total FROM batch")
	assert.NoError(t, err)

	res := invokeRows(t, h, []string{`{"langs": []}`, `{"langs": ["en"]}`})
	defer res.Release()

	assert.Equal(t, int64(1), res.Column(0).Data().Chunk(0).(*array.Int64).Value(0))
}

func TestInferredMemBatchHandler_EmptyListInEveryMessage(t *testing.T) {
	conn, cleanup := newTestADBCConn(t)
	defer cleanup()

	h, err := NewInferredMemBatchHandler(conn,
		"SELECT CAST(SUM(len(langs)) AS BIGINT) as total FROM batch")
	assert.NoError(t, err)

	res := invokeRows(t, h, []string{`{"langs": []}`, `{"langs": []}`})
	defer res.Release()

	assert.Equal(t, int64(0), res.Column(0).Data().Chunk(0).(*array.Int64).Value(0))
}

// A message missing the array leaves a null list, not an empty one, matching
// the null a missing scalar produces.
func TestInferredMemBatchHandler_MissingListIsNull(t *testing.T) {
	conn, cleanup := newTestADBCConn(t)
	defer cleanup()

	h, err := NewInferredMemBatchHandler(conn,
		"SELECT COUNT(langs) as present, COUNT(*) as total FROM batch")
	assert.NoError(t, err)

	res := invokeRows(t, h, []string{`{"langs": ["en"]}`, `{"other": 1}`})
	defer res.Release()

	assert.Equal(t, int64(1), res.Column(0).Data().Chunk(0).(*array.Int64).Value(0))
	assert.Equal(t, int64(2), res.Column(1).Data().Chunk(0).(*array.Int64).Value(0))
}

func TestInferredMemBatchHandler_InfersNestedLists(t *testing.T) {
	conn, cleanup := newTestADBCConn(t)
	defer cleanup()

	h, err := NewInferredMemBatchHandler(conn, "SELECT m[1][2] as v FROM batch")
	assert.NoError(t, err)

	res := invokeRows(t, h, []string{`{"m": [[1, 2], [3]]}`})
	defer res.Release()

	assert.Equal(t, int64(2), res.Column(0).Data().Chunk(0).(*array.Int64).Value(0))
}

// A list whose elements cannot be reconciled must fail the batch, the same way
// a conflicting scalar column does, rather than silently nulling the value.
func TestInferredMemBatchHandler_ConflictingListElementTypesError(t *testing.T) {
	conn, cleanup := newTestADBCConn(t)
	defer cleanup()

	h, err := NewInferredMemBatchHandler(conn, "SELECT * FROM batch")
	assert.NoError(t, err)
	assert.NoError(t, h.Init(context.Background()))

	assert.NoError(t, h.Write([]byte(`{"x": [1]}`)))
	assert.NoError(t, h.Write([]byte(`{"x": ["hello"]}`)))

	_, err = h.Invoke(context.Background())
	assert.Error(t, err)
	// Specifically a type conflict, not "arrays are unsupported".
	assert.That(t, strings.Contains(err.Error(), "cannot convert"))
}

// jsonparser hands back the raw bytes between a string's quotes, so escape
// sequences arrive undecoded: "a\nb" is a backslash and an n, not a newline.
// The Python engine decodes with json.loads, and a sink that stores the raw
// bytes silently corrupts the value rather than failing.
func TestInferredMemBatchHandler_DecodesJSONStringEscapes(t *testing.T) {
	cases := []struct {
		name string
		msg  string
		want string
	}{
		{"newline", `{"v": "line one\nline two"}`, "line one\nline two"},
		{"tab", `{"v": "a\tb"}`, "a\tb"},
		{"quote", `{"v": "say \"hi\""}`, `say "hi"`},
		{"backslash", `{"v": "back\\slash"}`, `back\slash`},
		{"unicode escape", `{"v": "less \u003c than"}`, "less < than"},
		{"surrogate pair", `{"v": "emoji \ud83d\ude00"}`, "emoji 😀"},
		// Raw UTF-8 is not escaped and must survive the fast path untouched.
		{"raw utf8", `{"v": "L'Œil 👁 café"}`, "L'Œil 👁 café"},
		{"plain ascii", `{"v": "nothing to decode"}`, "nothing to decode"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			conn, cleanup := newTestADBCConn(t)
			defer cleanup()

			h, err := NewInferredMemBatchHandler(conn, "SELECT v FROM batch")
			assert.NoError(t, err)

			res := invokeRows(t, h, []string{tc.msg})
			defer res.Release()

			got := res.Column(0).Data().Chunk(0).(*array.String).Value(0)
			assert.Equal(t, tc.want, got)
		})
	}
}

// The same decoding must apply wherever a string is built, not just at the top
// level -- appendJSONValue is shared by struct fields and list elements.
func TestInferredMemBatchHandler_DecodesEscapesInNestedValues(t *testing.T) {
	conn, cleanup := newTestADBCConn(t)
	defer cleanup()

	h, err := NewInferredMemBatchHandler(conn, "SELECT o.s AS nested, l[1] AS elem FROM batch")
	assert.NoError(t, err)

	res := invokeRows(t, h, []string{`{"o": {"s": "a\nb"}, "l": ["c\td"]}`})
	defer res.Release()

	assert.Equal(t, "a\nb", res.Column(0).Data().Chunk(0).(*array.String).Value(0))
	assert.Equal(t, "c\td", res.Column(1).Data().Chunk(0).(*array.String).Value(0))
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
