package sinks

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/zeebo/assert"
)

func TestClickhouseOptions_PythonDSN(t *testing.T) {
	// The dsn the Python configs carry. clickhouse_connect speaks only the
	// HTTP interface, so 8123 is an HTTP port and must not be dialed with
	// the native protocol.
	opts, err := clickhouseOptions("clickhouse://localhost:8123/test")
	assert.NoError(t, err)

	assert.Equal(t, clickhouse.HTTP, opts.Protocol)
	assert.DeepEqual(t, []string{"localhost:8123"}, opts.Addr)
	assert.Equal(t, "test", opts.Auth.Database)
	assert.Equal(t, "default", opts.Auth.Username)
	assert.Equal(t, "", opts.Auth.Password)
	assert.Nil(t, opts.TLS)
}

func TestClickhouseOptions_Credentials(t *testing.T) {
	opts, err := clickhouseOptions("clickhouse://alice:s3cret@ch.example.com/analytics")
	assert.NoError(t, err)

	// No port in the dsn: the HTTP interface default.
	assert.DeepEqual(t, []string{"ch.example.com:8123"}, opts.Addr)
	assert.Equal(t, "alice", opts.Auth.Username)
	assert.Equal(t, "s3cret", opts.Auth.Password)
	assert.Equal(t, "analytics", opts.Auth.Database)
}

func TestClickhouseOptions_Secure(t *testing.T) {
	opts, err := clickhouseOptions("clickhouses://ch.example.com/analytics")
	assert.NoError(t, err)

	assert.Equal(t, clickhouse.HTTP, opts.Protocol)
	assert.DeepEqual(t, []string{"ch.example.com:8443"}, opts.Addr)
	assert.NotNil(t, opts.TLS)
}

func TestClickhouseOptions_Native(t *testing.T) {
	opts, err := clickhouseOptions("tcp://localhost:9000/test")
	assert.NoError(t, err)

	assert.Equal(t, clickhouse.Native, opts.Protocol)
	assert.DeepEqual(t, []string{"localhost:9000"}, opts.Addr)
}

func TestClickhouseOptions_Invalid(t *testing.T) {
	_, err := clickhouseOptions("postgres://localhost:5432/test")
	assert.Error(t, err)

	_, err = clickhouseOptions("")
	assert.Error(t, err)
}

func TestNewClickhouseSink_RequiresTable(t *testing.T) {
	_, err := NewClickhouseSink(config.ClickhouseSink{DSN: "clickhouse://localhost:8123/test"})
	assert.Error(t, err)
}

// Batch mirrors the Python ClickhouseSink, which alone among the sinks
// returns nothing: the rows go straight to ClickHouse and are not held for a
// downstream reader.
func TestClickhouseSink_BatchIsNil(t *testing.T) {
	s := newLiveClickhouseSink(t, "")

	batch, err := s.Batch()
	assert.NoError(t, err)
	assert.Nil(t, batch)
}

// clickhouseTestDSN points the live tests at the dev-stack ClickHouse.
func clickhouseTestDSN() string {
	if dsn := os.Getenv("SQLFLOW_CLICKHOUSE_DSN"); dsn != "" {
		return dsn
	}
	return "clickhouse://localhost:8123/default"
}

// newLiveClickhouseSink builds a sink against the dev-stack ClickHouse and
// creates its target table from ddl (a format string taking the table name;
// empty to skip). It skips the test when no server is reachable rather than
// failing a local `go test`.
func newLiveClickhouseSink(t *testing.T, ddl string) *ClickhouseSink {
	t.Helper()

	table := fmt.Sprintf("turbine_sink_test_%d", time.Now().UnixNano())

	s, err := NewClickhouseSink(config.ClickhouseSink{
		DSN:   clickhouseTestDSN(),
		Table: table,
	})
	if err != nil {
		t.Skipf("clickhouse unavailable: %v", err)
	}
	t.Cleanup(func() { s.Close() })

	ctx := context.Background()
	if err := s.conn.Ping(ctx); err != nil {
		t.Skipf("clickhouse unavailable at %s: %v", clickhouseTestDSN(), err)
	}

	if ddl != "" {
		assert.NoError(t, s.conn.Exec(ctx, fmt.Sprintf(ddl, table)))
		t.Cleanup(func() {
			s.conn.Exec(context.Background(), "DROP TABLE IF EXISTS "+table)
		})
	}

	return s
}

func clickhouseRowCount(t *testing.T, s *ClickhouseSink) uint64 {
	t.Helper()

	var count uint64
	row := s.conn.QueryRow(context.Background(), "SELECT count() FROM "+s.table)
	assert.NoError(t, row.Scan(&count))
	return count
}

func TestClickhouseSink_InsertsRows(t *testing.T) {
	s := newLiveClickhouseSink(t, `CREATE TABLE %s (
		timestamp DateTime,
		user_id UInt64,
		action String,
		browser String,
		score Float64,
		active Bool
	) ENGINE = MergeTree() ORDER BY (timestamp, user_id)`)

	table := clickhouseFixtureTable(t)
	defer table.Release()

	assert.NoError(t, s.WriteTable(table))
	assert.NoError(t, s.Flush())

	assert.Equal(t, uint64(2), clickhouseRowCount(t, s))

	// The buffered table is released on flush; a second flush with nothing
	// pending must be a no-op rather than a re-insert.
	assert.NoError(t, s.Flush())
	assert.Equal(t, uint64(2), clickhouseRowCount(t, s))
}

// A handler whose query matched no rows yields an empty, column-less table.
// Flushing it must be a no-op: the column list would otherwise be empty and
// the INSERT malformed.
func TestClickhouseSink_EmptyTableIsNoop(t *testing.T) {
	s := newLiveClickhouseSink(t, "")

	table := array.NewTable(arrow.NewSchema(nil, nil), nil, 0)
	defer table.Release()

	assert.NoError(t, s.WriteTable(table))
	assert.NoError(t, s.Flush())
}

func TestClickhouseSink_NullsBecomeDefaults(t *testing.T) {
	s := newLiveClickhouseSink(t, `CREATE TABLE %s (
		user_id UInt64,
		action Nullable(String)
	) ENGINE = MergeTree() ORDER BY user_id`)

	alloc := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "user_id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "action", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	b := array.NewRecordBuilder(alloc, schema)
	defer b.Release()
	b.Field(0).(*array.Int64Builder).AppendValues([]int64{7, 8}, nil)
	b.Field(1).(*array.StringBuilder).Append("click")
	b.Field(1).(*array.StringBuilder).AppendNull()

	rec := b.NewRecord()
	defer rec.Release()
	table := array.NewTableFromRecords(schema, []arrow.Record{rec})
	defer table.Release()

	assert.NoError(t, s.WriteTable(table))
	assert.NoError(t, s.Flush())

	var action *string
	row := s.conn.QueryRow(context.Background(), "SELECT action FROM "+s.table+" WHERE user_id = 8")
	assert.NoError(t, row.Scan(&action))
	assert.Nil(t, action)
}

// clickhouseFixtureTable builds a batch spanning the Arrow types a handler
// realistically emits: strings, ints, floats, bools and timestamps.
func clickhouseFixtureTable(t *testing.T) arrow.Table {
	t.Helper()

	alloc := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "timestamp", Type: arrow.FixedWidthTypes.Timestamp_us},
		{Name: "user_id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "action", Type: arrow.BinaryTypes.String},
		{Name: "browser", Type: arrow.BinaryTypes.String},
		{Name: "score", Type: arrow.PrimitiveTypes.Float64},
		{Name: "active", Type: arrow.FixedWidthTypes.Boolean},
	}, nil)

	b := array.NewRecordBuilder(alloc, schema)
	defer b.Release()

	ts0, err := arrow.TimestampFromTime(time.Unix(1700000000, 0).UTC(), arrow.Microsecond)
	assert.NoError(t, err)
	ts1, err := arrow.TimestampFromTime(time.Unix(1700000060, 0).UTC(), arrow.Microsecond)
	assert.NoError(t, err)

	b.Field(0).(*array.TimestampBuilder).AppendValues([]arrow.Timestamp{ts0, ts1}, nil)
	b.Field(1).(*array.Int64Builder).AppendValues([]int64{1, 2}, nil)
	b.Field(2).(*array.StringBuilder).AppendValues([]string{"click", "view"}, nil)
	b.Field(3).(*array.StringBuilder).AppendValues([]string{"chrome", "firefox"}, nil)
	b.Field(4).(*array.Float64Builder).AppendValues([]float64{1.5, 2.5}, nil)
	b.Field(5).(*array.BooleanBuilder).AppendValues([]bool{true, false}, nil)

	rec := b.NewRecord()
	defer rec.Release()

	table := array.NewTableFromRecords(schema, []arrow.Record{rec})
	return table
}
