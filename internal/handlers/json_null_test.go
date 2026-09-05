package handlers

// An explicit JSON null must reach DuckDB as a NULL, the same as a missing
// key. It used to be appended to string columns as the four-character text
// "null", which a `GROUP BY` counted as a real value and a `coalesce()` never
// saw. The user's SQL decides what a null becomes; the handler must not.

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/zeebo/assert"
)

func TestHandlerInferredMem_ExplicitNullStringIsNull(t *testing.T) {
	conn, cleanup := newTestADBCConn(t)
	defer cleanup()

	h, err := NewInferredMemBatchHandler(conn, "SELECT name FROM batch ORDER BY name NULLS LAST")
	assert.NoError(t, err)

	res := invokeRows(t, h, []string{
		`{"name": "alice"}`,
		`{"name": null}`,
	})
	defer res.Release()

	assert.Equal(t, int64(2), res.NumRows())
	name := res.Column(0).Data().Chunk(0).(*array.String)
	assert.Equal(t, "alice", name.Value(0))
	assert.That(t, name.IsNull(1))
}

// The first message decides the column's type. A null there leaves the column
// untyped until a later message supplies the type, and the null row must still
// be a null once it does.
func TestHandlerInferredMem_ExplicitNullInFirstRowIsNull(t *testing.T) {
	conn, cleanup := newTestADBCConn(t)
	defer cleanup()

	h, err := NewInferredMemBatchHandler(conn, "SELECT name FROM batch ORDER BY name NULLS LAST")
	assert.NoError(t, err)

	res := invokeRows(t, h, []string{
		`{"name": null}`,
		`{"name": "alice"}`,
	})
	defer res.Release()

	assert.Equal(t, int64(2), res.NumRows())
	name := res.Column(0).Data().Chunk(0).(*array.String)
	assert.Equal(t, "alice", name.Value(0))
	assert.That(t, name.IsNull(1))
}

// List elements arrive through a different entry point than keyed fields.
func TestHandlerInferredMem_ExplicitNullListElementIsNull(t *testing.T) {
	conn, cleanup := newTestADBCConn(t)
	defer cleanup()

	h, err := NewInferredMemBatchHandler(conn, "SELECT langs[1] AS first, langs[2] AS second FROM batch")
	assert.NoError(t, err)

	res := invokeRows(t, h, []string{`{"langs": ["en", null]}`})
	defer res.Release()

	assert.Equal(t, int64(1), res.NumRows())
	assert.Equal(t, "en", res.Column(0).Data().Chunk(0).(*array.String).Value(0))
	assert.That(t, res.Column(1).Data().Chunk(0).(*array.String).IsNull(0))
}

func TestHandlerStructured_ExplicitNullStringIsNull(t *testing.T) {
	conn, cleanup := newTestADBCConn(t)
	defer cleanup()

	createTable(t, conn, `CREATE TABLE source (event STRING, name STRING);`)

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "event", Type: arrow.BinaryTypes.String},
		{Name: "name", Type: arrow.BinaryTypes.String},
	}, nil)

	h, err := NewStructuredBatchHandler(conn, "SELECT name FROM source ORDER BY event", "source", schema)
	assert.NoError(t, err)
	assert.NoError(t, h.Init(context.Background()))

	assert.NoError(t, h.Write([]byte(`{"event": "a", "name": "alice"}`)))
	assert.NoError(t, h.Write([]byte(`{"event": "b", "name": null}`)))

	res, err := h.Invoke(context.Background())
	assert.NoError(t, err)
	defer res.Release()

	assert.Equal(t, int64(2), res.NumRows())
	name := res.Column(0).Data().Chunk(0).(*array.String)
	assert.Equal(t, "alice", name.Value(0))
	assert.That(t, name.IsNull(1))
}

// A null where a struct is expected is a null struct, not a struct whose every
// field is null. SQL tells them apart: `props IS NULL` is true for the first
// and false for the second, so a `WHERE props IS NULL` used to miss the rows
// it exists to find.
//
// The third row carries as much weight as the second. StructBuilder.AppendNull
// is Append(false), which appends nothing of its own to the child builders --
// it back-fills them -- and newData reads the parent's length and the
// children's separately with no check between them. A row after the null is
// what proves the child stayed aligned with its parent.
func TestHandlerInferredMem_ExplicitNullStructIsANullStruct(t *testing.T) {
	conn, cleanup := newTestADBCConn(t)
	defer cleanup()

	h, err := NewInferredMemBatchHandler(conn,
		"SELECT props IS NULL AS props_is_null, props.city AS city FROM batch ORDER BY id")
	assert.NoError(t, err)

	res := invokeRows(t, h, []string{
		`{"id": 1, "props": {"city": "nyc"}}`,
		`{"id": 2, "props": null}`,
		`{"id": 3, "props": {"city": "sfo"}}`,
	})
	defer res.Release()

	assert.Equal(t, int64(3), res.NumRows())

	isNull := res.Column(0).Data().Chunk(0).(*array.Boolean)
	assert.That(t, !isNull.Value(0))
	assert.That(t, isNull.Value(1))
	assert.That(t, !isNull.Value(2))

	city := res.Column(1).Data().Chunk(0).(*array.String)
	assert.Equal(t, "nyc", city.Value(0))
	assert.That(t, city.IsNull(1))
	assert.Equal(t, "sfo", city.Value(2))
}
