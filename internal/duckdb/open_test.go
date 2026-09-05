package duckdb

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/zeebo/assert"
)

func exec(tb testing.TB, conn adbc.Connection, sql string) {
	tb.Helper()

	stmt, err := conn.NewStatement()
	if err != nil {
		tb.Fatal(err)
	}
	defer stmt.Close()

	if err := stmt.SetSqlQuery(sql); err != nil {
		tb.Fatal(err)
	}
	if _, err := stmt.ExecuteUpdate(context.Background()); err != nil {
		tb.Fatal(err)
	}
}

func queryInt64(tb testing.TB, conn adbc.Connection, sql string) int64 {
	tb.Helper()

	stmt, err := conn.NewStatement()
	if err != nil {
		tb.Fatal(err)
	}
	defer stmt.Close()

	if err := stmt.SetSqlQuery(sql); err != nil {
		tb.Fatal(err)
	}
	reader, _, err := stmt.ExecuteQuery(context.Background())
	if err != nil {
		tb.Fatal(err)
	}
	defer reader.Release()

	for reader.Next() {
		rec := reader.Record()
		if rec.NumRows() > 0 {
			switch col := rec.Column(0).(type) {
			case *array.Int64:
				return col.Value(0)
			case *array.Int32:
				return int64(col.Value(0))
			}
		}
	}
	return -1
}

// A pipeline that declares a state path must get a DuckDB backed by that file,
// so window state survives the process. In-memory state is lost on a crash
// while its offsets are already committed -- silently, with the consumer group
// reporting no lag.
func TestStateDurability_OpenPathPersistsAcrossProcesses(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")

	db, err := OpenPath(context.Background(), path)
	assert.NoError(t, err)
	conn, err := db.Connect(context.Background())
	assert.NoError(t, err)

	exec(t, conn, "CREATE TABLE agg (city VARCHAR, n INTEGER)")
	exec(t, conn, "INSERT INTO agg VALUES ('NYC', 7)")
	conn.Close()
	assert.NoError(t, db.Close())

	_, statErr := os.Stat(path)
	assert.NoError(t, statErr)

	// A second handle over the same file sees the committed row.
	db2, err := OpenPath(context.Background(), path)
	assert.NoError(t, err)
	defer db2.Close()
	conn2, err := db2.Connect(context.Background())
	assert.NoError(t, err)
	defer conn2.Close()

	assert.Equal(t, int64(7), queryInt64(t, conn2, "SELECT n FROM agg WHERE city = 'NYC'"))
}

// An empty path keeps today's in-memory behaviour, so a config without a state
// path is unaffected.
func TestStateDurability_OpenPathEmptyPathIsInMemory(t *testing.T) {
	db, err := OpenPath(context.Background(), "")
	assert.NoError(t, err)
	defer db.Close()

	conn, err := db.Connect(context.Background())
	assert.NoError(t, err)
	defer conn.Close()

	exec(t, conn, "CREATE TABLE t (i INTEGER)")
	assert.Equal(t, "", db.Path())
}

// Holding the database handle is the point of this type: a second connection
// is what lets stats be read without disturbing the writer. Each connection
// has its own transaction state, so an uncommitted batch stays invisible.
func TestStateDurability_DBSecondConnectionSeesOnlyCommittedState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")

	db, err := OpenPath(context.Background(), path)
	assert.NoError(t, err)
	defer db.Close()

	writer, err := db.Connect(context.Background())
	assert.NoError(t, err)
	defer writer.Close()
	reader, err := db.Connect(context.Background())
	assert.NoError(t, err)
	defer reader.Close()

	exec(t, writer, "CREATE TABLE agg (i INTEGER)")
	exec(t, writer, "INSERT INTO agg VALUES (1)")
	assert.Equal(t, int64(1), queryInt64(t, reader, "SELECT count(*) FROM agg"))

	// The writer goes transactional, as the pipeline does, and adds a row it
	// has not committed.
	po, ok := writer.(adbc.PostInitOptions)
	assert.That(t, ok)
	assert.NoError(t, po.SetOption(adbc.OptionKeyAutoCommit, adbc.OptionValueDisabled))
	exec(t, writer, "INSERT INTO agg VALUES (2)")

	// The reader must not see it: stats report what would survive a crash.
	assert.Equal(t, int64(1), queryInt64(t, reader, "SELECT count(*) FROM agg"))

	committer, ok := writer.(interface{ Commit(context.Context) error })
	assert.That(t, ok)
	assert.NoError(t, committer.Commit(context.Background()))

	assert.Equal(t, int64(2), queryInt64(t, reader, "SELECT count(*) FROM agg"))
}

// Open keeps working for callers that want one connection and no handle.
func TestStateDurability_OpenReturnsAConnection(t *testing.T) {
	conn, err := Open(context.Background())
	assert.NoError(t, err)
	defer conn.Close()

	exec(t, conn, "CREATE TABLE t (i INTEGER)")
	exec(t, conn, "INSERT INTO t VALUES (3)")
	assert.Equal(t, int64(3), queryInt64(t, conn, "SELECT i FROM t"))
}
