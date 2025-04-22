package handlers

import (
	"context"
	"database/sql"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/marcboeker/go-duckdb"
	_ "github.com/marcboeker/go-duckdb"
	"github.com/zeebo/assert"
	"testing"
)

func TestArrowGetSchema(t *testing.T) {
	connector, err := duckdb.NewConnector(":memory:", nil)
	assert.NoError(t, err)
	defer func() {
		err := connector.Close()
		assert.NoError(t, err)
	}()

	db := sql.OpenDB(connector)
	defer func() {
		err := db.Close()
		assert.NoError(t, err)
	}()

	// Create a table with 5 columns
	createStmt := `
	CREATE TABLE users (
		id INTEGER,
		name TEXT,
		email TEXT,
		age INTEGER,
		created_at TIMESTAMP
	);
	`

	_, err = db.Exec(createStmt)
	assert.NoError(t, err)

	conn, err := connector.Connect(context.Background())
	assert.NoError(t, err)
	defer func() {
		err := conn.Close()
		assert.NoError(t, err)
	}()

	arr, err := duckdb.NewArrowFromConn(conn)
	assert.NoError(t, err)

	rdr, err := arr.QueryContext(
		context.Background(),
		"select * from users",
	)
	assert.NoError(t, err)

	schema := rdr.Schema()
	assert.Equal(t, 5, len(schema.Fields()))
}

func TestStructuredBatchHandler_SelectCount(t *testing.T) {
	connector, err := duckdb.NewConnector(":memory:", nil)
	assert.NoError(t, err)
	defer func() {
		err := connector.Close()
		assert.NoError(t, err)
	}()

	db := sql.OpenDB(connector)
	defer func() {
		err := db.Close()
		assert.NoError(t, err)
	}()

	// Create a table with 5 columns
	createStmt := `
	CREATE TABLE users (
		id INTEGER,
		name TEXT,
		email TEXT,
		age INTEGER,
		created_at TIMESTAMP
	);
	`

	_, err = db.Exec(createStmt)
	assert.NoError(t, err)

	conn, err := connector.Connect(context.Background())
	assert.NoError(t, err)
	defer func() {
		err := conn.Close()
		assert.NoError(t, err)
	}()

	arr, err := duckdb.NewArrowFromConn(conn)
	assert.NoError(t, err)

	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int32},
			{Name: "name", Type: arrow.BinaryTypes.String},
			{Name: "email", Type: arrow.BinaryTypes.String},
			{Name: "age", Type: arrow.PrimitiveTypes.Int32},
			{Name: "created_at", Type: arrow.FixedWidthTypes.Timestamp_ms},
		},
		nil)

	h, err := NewStructuredBatchHandler(
		arr,
		"SELECT * FROM users",
		"users",
		schema,
	)
	assert.NoError(t, err)

	err = h.Init(context.Background())
	assert.NoError(t, err)

	err = h.Write([]byte(`{"id": 1, "name": "test name", "email": "test email", "age": 77, "created_at": "2025-04-10 15:00:00"}`))
	assert.NoError(t, err)

	res, err := h.Invoke(context.Background())
	assert.NoError(t, err)

	assert.Equal(t, 1, res.NumRows())
}
