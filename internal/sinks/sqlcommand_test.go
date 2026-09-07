package sinks

// The sqlcommand sink is how the Python engine wrote parquet, S3, Postgres,
// DuckLake and MotherDuck, and it is the one sink that needs no server at all:
// it runs the user's SQL against the pipeline's own DuckDB connection. Nothing
// here skips.

import (
	"context"
	"strings"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/turbolytics/sql-flow/internal/duckdb"
	"github.com/zeebo/assert"
)

// newSinkTestConn opens an in-memory DuckDB connection. It fails rather than
// skips: a sink test that skips reads as coverage in the matrix and is not.
func newSinkTestConn(t *testing.T) adbc.Connection {
	t.Helper()

	conn, err := duckdb.Open(context.Background())
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

// exec runs a statement that returns nothing.
func exec(t *testing.T, conn adbc.Connection, sql string) {
	t.Helper()

	stmt, err := conn.NewStatement()
	assert.NoError(t, err)
	defer stmt.Close()

	assert.NoError(t, stmt.SetSqlQuery(sql))
	_, err = stmt.ExecuteUpdate(context.Background())
	assert.NoError(t, err)
}

// queryInt64s reads a single-column result into a slice, in row order.
func queryInt64s(t *testing.T, conn adbc.Connection, sql string) []int64 {
	t.Helper()

	stmt, err := conn.NewStatement()
	assert.NoError(t, err)
	defer stmt.Close()

	assert.NoError(t, stmt.SetSqlQuery(sql))
	reader, _, err := stmt.ExecuteQuery(context.Background())
	assert.NoError(t, err)
	defer reader.Release()

	var out []int64
	for reader.Next() {
		rec := reader.Record()
		col := rec.Column(0).(*array.Int64)
		for i := 0; i < col.Len(); i++ {
			out = append(out, col.Value(i))
		}
	}
	assert.NoError(t, reader.Err())
	return out
}

// queryStrings reads a single string column into a slice, in row order.
func queryStrings(t *testing.T, conn adbc.Connection, sql string) []string {
	t.Helper()

	stmt, err := conn.NewStatement()
	assert.NoError(t, err)
	defer stmt.Close()

	assert.NoError(t, stmt.SetSqlQuery(sql))
	reader, _, err := stmt.ExecuteQuery(context.Background())
	assert.NoError(t, err)
	defer reader.Release()

	var out []string
	for reader.Next() {
		rec := reader.Record()
		col := rec.Column(0).(*array.String)
		for i := 0; i < col.Len(); i++ {
			// Value aliases the Arrow value buffer, which the reader frees on
			// the next Next. Without the copy the caller reads freed memory.
			out = append(out, strings.Clone(col.Value(i)))
		}
	}
	assert.NoError(t, reader.Err())
	return out
}

func TestSinkSqlcommand_NewRequiresSQL(t *testing.T) {
	coverage.Covers(t, "sink.sqlcommand")
	conn := newSinkTestConn(t)

	_, err := NewSQLCommandSink(conn, "", nil)
	assert.Error(t, err)

	// Whitespace is not a SQL statement either.
	_, err = NewSQLCommandSink(conn, "   \n\t ", nil)
	assert.Error(t, err)
}

// The batch reaches the user's SQL under the name the Python sink registered
// it as. A different name and every shipped sqlcommand config stops working.
func TestSinkSqlcommand_RunsSQLAgainstTheBatch(t *testing.T) {
	coverage.Covers(t, "sink.sqlcommand")
	conn := newSinkTestConn(t)
	exec(t, conn, "CREATE TABLE out (city VARCHAR, count BIGINT)")

	s, err := NewSQLCommandSink(conn,
		"INSERT INTO out SELECT city, count FROM sqlflow_sink_batch", nil)
	assert.NoError(t, err)

	table := newTestTable(t, []string{"nyc", "sfo"}, []int64{1, 2})
	defer table.Release()

	assert.NoError(t, s.WriteTable(context.Background(), table))
	assert.NoError(t, s.Flush(context.Background()))

	assert.DeepEqual(t, []int64{1, 2}, queryInt64s(t, conn, "SELECT count FROM out ORDER BY count"))
}

// Writes accumulate until a flush, so a batch split across several WriteTable
// calls reaches the SQL as one table rather than as the last write.
func TestSinkSqlcommand_AccumulatesUntilFlush(t *testing.T) {
	coverage.Covers(t, "sink.sqlcommand")
	conn := newSinkTestConn(t)
	exec(t, conn, "CREATE TABLE out (city VARCHAR, count BIGINT)")

	s, err := NewSQLCommandSink(conn,
		"INSERT INTO out SELECT city, count FROM sqlflow_sink_batch", nil)
	assert.NoError(t, err)

	first := newTestTable(t, []string{"nyc"}, []int64{1})
	defer first.Release()
	second := newTestTable(t, []string{"sfo"}, []int64{2})
	defer second.Release()

	assert.NoError(t, s.WriteTable(context.Background(), first))
	assert.NoError(t, s.WriteTable(context.Background(), second))

	// Nothing has run yet. The SQL fires on flush, not on write.
	assert.Equal(t, 0, len(queryInt64s(t, conn, "SELECT count FROM out")))

	assert.NoError(t, s.Flush(context.Background()))
	assert.DeepEqual(t, []int64{1, 2}, queryInt64s(t, conn, "SELECT count FROM out ORDER BY count"))
}

// Each flush replaces the batch table rather than appending to it. Without the
// DROP the rows of every earlier batch are still there, and the user's SQL
// rewrites them on every flush -- a pipeline that duplicates its whole history
// once per interval.
func TestSinkSqlcommand_EachFlushReplacesTheBatchTable(t *testing.T) {
	coverage.Covers(t, "sink.sqlcommand")
	conn := newSinkTestConn(t)
	exec(t, conn, "CREATE TABLE out (city VARCHAR, count BIGINT)")

	s, err := NewSQLCommandSink(conn,
		"INSERT INTO out SELECT city, count FROM sqlflow_sink_batch", nil)
	assert.NoError(t, err)

	first := newTestTable(t, []string{"nyc"}, []int64{1})
	defer first.Release()
	assert.NoError(t, s.WriteTable(context.Background(), first))
	assert.NoError(t, s.Flush(context.Background()))

	second := newTestTable(t, []string{"sfo"}, []int64{2})
	defer second.Release()
	assert.NoError(t, s.WriteTable(context.Background(), second))
	assert.NoError(t, s.Flush(context.Background()))

	// Three rows, not four: the second flush must not rewrite the first batch.
	assert.DeepEqual(t, []int64{1, 2}, queryInt64s(t, conn, "SELECT count FROM out ORDER BY count"))
}

// A flush with nothing buffered must not run the SQL. The pipeline flushes on
// an interval whether or not a batch arrived, so an INSERT that fired on an
// empty flush would write the previous batch again every interval.
func TestSinkSqlcommand_FlushWithNothingPendingIsANoop(t *testing.T) {
	coverage.Covers(t, "sink.sqlcommand")
	conn := newSinkTestConn(t)
	exec(t, conn, "CREATE TABLE out (city VARCHAR, count BIGINT)")

	s, err := NewSQLCommandSink(conn,
		"INSERT INTO out SELECT city, count FROM sqlflow_sink_batch", nil)
	assert.NoError(t, err)

	table := newTestTable(t, []string{"nyc"}, []int64{1})
	defer table.Release()
	assert.NoError(t, s.WriteTable(context.Background(), table))
	assert.NoError(t, s.Flush(context.Background()))

	assert.NoError(t, s.Flush(context.Background()))
	assert.NoError(t, s.Flush(context.Background()))

	assert.DeepEqual(t, []int64{1}, queryInt64s(t, conn, "SELECT count FROM out"))
}

// An empty batch still counts as a batch: the SQL runs against zero rows
// rather than being skipped, so a statement with a side effect of its own --
// the COPY that writes a parquet file, an ATTACHed table's DELETE -- still
// happens.
func TestSinkSqlcommand_EmptyBatchStillRunsTheSQL(t *testing.T) {
	coverage.Covers(t, "sink.sqlcommand")
	conn := newSinkTestConn(t)
	exec(t, conn, "CREATE TABLE runs (n BIGINT)")

	s, err := NewSQLCommandSink(conn, "INSERT INTO runs VALUES (1)", nil)
	assert.NoError(t, err)

	empty := array.NewTable(arrow.NewSchema(nil, nil), nil, 0)
	defer empty.Release()

	assert.NoError(t, s.WriteTable(context.Background(), empty))
	assert.NoError(t, s.Flush(context.Background()))

	assert.DeepEqual(t, []int64{1}, queryInt64s(t, conn, "SELECT n FROM runs"))
}

// Substitutions are how a config writes one file per flush instead of
// overwriting a single path. Each flush must produce a different value.
func TestSinkSqlcommand_SubstitutesUUID4(t *testing.T) {
	coverage.Covers(t, "sink.sqlcommand")
	conn := newSinkTestConn(t)
	exec(t, conn, "CREATE TABLE out (name VARCHAR)")

	s, err := NewSQLCommandSink(conn,
		"INSERT INTO out VALUES ('part-<uuid>.parquet')",
		[]config.SQLCommandSubstitution{{Var: "<uuid>", Type: "uuid4"}})
	assert.NoError(t, err)

	for i := 0; i < 2; i++ {
		table := newTestTable(t, []string{"nyc"}, []int64{1})
		assert.NoError(t, s.WriteTable(context.Background(), table))
		assert.NoError(t, s.Flush(context.Background()))
		table.Release()
	}

	names := queryStrings(t, conn, "SELECT name FROM out")
	assert.Equal(t, 2, len(names))
	for _, name := range names {
		assert.That(t, strings.HasPrefix(name, "part-"))
		assert.That(t, strings.HasSuffix(name, ".parquet"))
		assert.That(t, !strings.Contains(name, "<uuid>"))
	}
	assert.That(t, names[0] != names[1])
}

// An unsupported substitution type fails the flush rather than leaving the
// placeholder in the SQL, where it would be a syntax error whose message names
// the wrong problem.
func TestSinkSqlcommand_RejectsAnUnknownSubstitution(t *testing.T) {
	coverage.Covers(t, "sink.sqlcommand")
	conn := newSinkTestConn(t)

	s, err := NewSQLCommandSink(conn, "INSERT INTO out VALUES ('<x>')",
		[]config.SQLCommandSubstitution{{Var: "<x>", Type: "sequence"}})
	assert.NoError(t, err)

	table := newTestTable(t, []string{"nyc"}, []int64{1})
	defer table.Release()

	assert.NoError(t, s.WriteTable(context.Background(), table))
	err = s.Flush(context.Background())
	assert.Error(t, err)
	assert.That(t, strings.Contains(err.Error(), "sequence"))
}

// A statement DuckDB rejects must surface as an error, not be swallowed. The
// pipeline's error policy is what decides the consequence, and it can only
// decide on an error it is handed.
func TestSinkSqlcommand_ReportsASQLError(t *testing.T) {
	coverage.Covers(t, "sink.sqlcommand")
	conn := newSinkTestConn(t)

	s, err := NewSQLCommandSink(conn, "INSERT INTO no_such_table SELECT * FROM sqlflow_sink_batch", nil)
	assert.NoError(t, err)

	table := newTestTable(t, []string{"nyc"}, []int64{1})
	defer table.Release()

	assert.NoError(t, s.WriteTable(context.Background(), table))
	assert.Error(t, s.Flush(context.Background()))
}

// Batch is what the tumbling-window manager reads back after a write.
func TestSinkSqlcommand_BatchIsTheLastWrite(t *testing.T) {
	coverage.Covers(t, "sink.sqlcommand")
	conn := newSinkTestConn(t)

	s, err := NewSQLCommandSink(conn, "SELECT 1", nil)
	assert.NoError(t, err)

	batch, err := s.Batch()
	assert.NoError(t, err)
	assert.Nil(t, batch)

	table := newTestTable(t, []string{"nyc", "sfo"}, []int64{1, 2})
	defer table.Release()
	assert.NoError(t, s.WriteTable(context.Background(), table))

	batch, err = s.Batch()
	assert.NoError(t, err)
	assert.Equal(t, int64(2), batch.NumRows())
}
