package handlers

import (
	"context"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/drivermgr"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/zeebo/assert"
	"os"
	"testing"
)

func newTestADBCConn(t *testing.T) (adbc.Connection, func()) {
	t.Helper()
	return newADBCConn(t)
}

func newBenchADBCConn(b *testing.B) (adbc.Connection, func()) {
	b.Helper()
	return newADBCConn(b)
}

func newADBCConn(tb testing.TB) (adbc.Connection, func()) {
	tb.Helper()
	lib := os.Getenv("SQLFLOW_DUCKDB_LIB")
	if lib == "" {
		lib = "/opt/homebrew/lib/libduckdb.dylib"
	}
	var drv drivermgr.Driver
	db, err := drv.NewDatabase(map[string]string{
		"driver":     lib,
		"entrypoint": "duckdb_adbc_init",
	})
	if err != nil {
		tb.Fatal(err)
	}

	conn, err := db.Open(context.Background())
	if err != nil {
		tb.Fatal(err)
	}

	return conn, func() { conn.Close() }
}

func createTable(t *testing.T, conn adbc.Connection, ddl string) {
	t.Helper()
	stmt, err := conn.NewStatement()
	assert.NoError(t, err)
	defer stmt.Close()

	assert.NoError(t, stmt.SetSqlQuery(ddl))
	_, err = stmt.ExecuteUpdate(context.Background())
	assert.NoError(t, err)
}

func TestStructuredBatchHandler_SingleRecord(t *testing.T) {
	conn, cleanup := newTestADBCConn(t)
	defer cleanup()

	createTable(t, conn, `CREATE TABLE users (id INTEGER, name TEXT);`)

	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int32},
			{Name: "name", Type: arrow.BinaryTypes.String},
		}, nil)

	h, err := NewStructuredBatchHandler(conn, "SELECT * FROM users", "users", schema)
	assert.NoError(t, err)
	assert.NoError(t, h.Init(context.Background()))

	assert.NoError(t, h.Write([]byte(`{"id": 1, "name": "alice"}`)))

	res, err := h.Invoke(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, int64(1), res.NumRows())
	res.Release()
}

func TestStructuredBatchHandler_MultipleRecords(t *testing.T) {
	conn, cleanup := newTestADBCConn(t)
	defer cleanup()

	createTable(t, conn, `CREATE TABLE events (event TEXT, city TEXT);`)

	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "event", Type: arrow.BinaryTypes.String},
			{Name: "city", Type: arrow.BinaryTypes.String},
		}, nil)

	h, err := NewStructuredBatchHandler(conn, "SELECT city, COUNT(*) as cnt FROM events GROUP BY city ORDER BY city", "events", schema)
	assert.NoError(t, err)
	assert.NoError(t, h.Init(context.Background()))

	messages := []string{
		`{"event": "click", "city": "NYC"}`,
		`{"event": "view", "city": "NYC"}`,
		`{"event": "click", "city": "SF"}`,
		`{"event": "view", "city": "LA"}`,
		`{"event": "click", "city": "SF"}`,
	}
	for _, msg := range messages {
		assert.NoError(t, h.Write([]byte(msg)))
	}

	res, err := h.Invoke(context.Background())
	assert.NoError(t, err)

	// 3 distinct cities: LA, NYC, SF
	assert.Equal(t, int64(3), res.NumRows())
	res.Release()
}

func TestStructuredBatchHandler_NestedStruct(t *testing.T) {
	conn, cleanup := newTestADBCConn(t)
	defer cleanup()

	createTable(t, conn, `CREATE TABLE source (event STRING, properties STRUCT(city TEXT));`)

	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "event", Type: arrow.BinaryTypes.String},
			{Name: "properties", Type: arrow.StructOf(
				arrow.Field{Name: "city", Type: arrow.BinaryTypes.String},
			)},
		}, nil)

	h, err := NewStructuredBatchHandler(conn, "SELECT properties.city as city, COUNT(*) as cnt FROM source GROUP BY properties.city ORDER BY city", "source", schema)
	assert.NoError(t, err)
	assert.NoError(t, h.Init(context.Background()))

	// JSON with extra fields that should be ignored (only event + properties needed)
	messages := []string{
		`{"ip": "1.2.3.4", "event": "click", "properties": {"city": "NYC", "country": "US"}, "timestamp": "2024-01-01", "type": "track", "userId": "abc"}`,
		`{"ip": "5.6.7.8", "event": "view", "properties": {"city": "SF", "country": "US"}, "timestamp": "2024-01-01", "type": "track", "userId": "def"}`,
		`{"ip": "9.0.1.2", "event": "click", "properties": {"city": "NYC", "country": "US"}, "timestamp": "2024-01-01", "type": "track", "userId": "ghi"}`,
	}
	for _, msg := range messages {
		assert.NoError(t, h.Write([]byte(msg)))
	}

	res, err := h.Invoke(context.Background())
	assert.NoError(t, err)
	// 2 distinct cities: NYC, SF
	assert.Equal(t, int64(2), res.NumRows())
	res.Release()
}

func TestStructuredBatchHandler_LargeBatch(t *testing.T) {
	conn, cleanup := newTestADBCConn(t)
	defer cleanup()

	createTable(t, conn, `CREATE TABLE items (id INTEGER, name TEXT);`)

	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int32},
			{Name: "name", Type: arrow.BinaryTypes.String},
		}, nil)

	h, err := NewStructuredBatchHandler(conn, "SELECT COUNT(*) as cnt FROM items", "items", schema)
	assert.NoError(t, err)
	assert.NoError(t, h.Init(context.Background()))

	// Write 500 records to simulate a real batch
	for i := 0; i < 500; i++ {
		assert.NoError(t, h.Write([]byte(`{"id": 1, "name": "test"}`)))
	}

	res, err := h.Invoke(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, int64(1), res.NumRows())
	res.Release()
}
