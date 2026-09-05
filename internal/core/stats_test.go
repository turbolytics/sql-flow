package core

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/turbolytics/sql-flow/internal/duckdb"
	"github.com/zeebo/assert"
)

func execSQL(tb testing.TB, conn adbc.Connection, sql string) {
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

// The stats snapshot answers the two questions an operator has about a
// stateful pipeline: is my state growing without bound, and where am I in the
// stream. It reports the user's tables and the stored offsets, and must not
// leak the engine's own bookkeeping tables into either.
func TestStateDurability_Stats(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	conn := newStateConn(t, path)

	s := NewOffsetStore(conn)
	assert.NoError(t, s.Init(context.Background()))
	saved := NewMarks()
	saved.Advance("events", 0, Mark{Offset: 41, LeaderEpoch: 7})
	assert.NoError(t, s.Save(context.Background(), saved))

	execSQL(t, conn, "CREATE TABLE agg (city VARCHAR, n INTEGER)")
	execSQL(t, conn, "INSERT INTO agg VALUES ('NYC', 1), ('SF', 2), ('LA', 3)")

	stats, err := CollectStateStats(context.Background(), conn, path)
	assert.NoError(t, err)

	assert.Equal(t, path, stats.Path)
	assert.That(t, stats.SizeBytes > 0)

	// sqlflow_offsets is engine bookkeeping, not user state.
	assert.Equal(t, 1, len(stats.Tables))
	assert.Equal(t, "agg", stats.Tables[0].Name)
	assert.Equal(t, int64(3), stats.Tables[0].Rows)

	assert.Equal(t, 1, len(stats.Offsets))
	assert.Equal(t, "events", stats.Offsets[0].Topic)
	assert.Equal(t, int32(0), stats.Offsets[0].Partition)
	assert.Equal(t, int64(41), stats.Offsets[0].Offset)
	assert.Equal(t, int32(7), stats.Offsets[0].LeaderEpoch)
}

// A fresh state file reports zero tables rather than failing, so the endpoint
// is useful on a pipeline that has not processed anything yet.
func TestStateDurability_Stats_EmptyState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	conn := newStateConn(t, path)
	assert.NoError(t, NewOffsetStore(conn).Init(context.Background()))

	stats, err := CollectStateStats(context.Background(), conn, path)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(stats.Tables))
	assert.Equal(t, 0, len(stats.Offsets))
	assert.That(t, stats.SizeBytes >= 0)
}

// The transient batch table the inferred handlers create per batch is engine
// machinery too; reporting it as user state would be noise that changes on
// every batch.
func TestStateDurability_Stats_ExcludesTheBatchTable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	conn := newStateConn(t, path)
	assert.NoError(t, NewOffsetStore(conn).Init(context.Background()))

	execSQL(t, conn, "CREATE TABLE batch (i INTEGER)")
	execSQL(t, conn, "CREATE TABLE agg (i INTEGER)")

	stats, err := CollectStateStats(context.Background(), conn, path)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(stats.Tables))
	assert.Equal(t, "agg", stats.Tables[0].Name)
}

// Tables and offsets are sorted so /stats output and CLI diffs stay stable
// between runs; DuckDB's row order and map iteration are not guaranteed.
func TestStateDurability_Stats_IsDeterministicallyOrdered(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	conn := newStateConn(t, path)

	s := NewOffsetStore(conn)
	assert.NoError(t, s.Init(context.Background()))
	saved := NewMarks()
	saved.Advance("zeta", 0, Mark{Offset: 1})
	saved.Advance("alpha", 2, Mark{Offset: 2})
	saved.Advance("alpha", 0, Mark{Offset: 3})
	assert.NoError(t, s.Save(context.Background(), saved))

	execSQL(t, conn, "CREATE TABLE zulu (i INTEGER)")
	execSQL(t, conn, "CREATE TABLE alpha (i INTEGER)")
	execSQL(t, conn, "CREATE TABLE mike (i INTEGER)")

	stats, err := CollectStateStats(context.Background(), conn, path)
	assert.NoError(t, err)

	assert.Equal(t, []string{"alpha", "mike", "zulu"},
		[]string{stats.Tables[0].Name, stats.Tables[1].Name, stats.Tables[2].Name})

	assert.Equal(t, "alpha", stats.Offsets[0].Topic)
	assert.Equal(t, int32(0), stats.Offsets[0].Partition)
	assert.Equal(t, "alpha", stats.Offsets[1].Topic)
	assert.Equal(t, int32(2), stats.Offsets[1].Partition)
	assert.Equal(t, "zeta", stats.Offsets[2].Topic)
}

// Stats read a dedicated connection, so an in-flight batch stays invisible
// until it commits. Without this the endpoint would report rows that a
// rollback then erased -- a durability feature reporting numbers that never
// survived anything.
func TestStateDurability_Stats_DoesNotSeeUncommittedWrites(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")

	db, err := duckdb.OpenPath(context.Background(), path)
	assert.NoError(t, err)
	defer db.Close()

	writer, err := db.Connect(context.Background())
	assert.NoError(t, err)
	defer writer.Close()
	reader, err := db.Connect(context.Background())
	assert.NoError(t, err)
	defer reader.Close()

	assert.NoError(t, NewOffsetStore(writer).Init(context.Background()))
	execSQL(t, writer, "CREATE TABLE agg (city VARCHAR)")
	execSQL(t, writer, "INSERT INTO agg VALUES ('NYC')")

	// The writer goes transactional, as a stateful pipeline does, and adds a
	// row it has not committed.
	po, ok := writer.(adbc.PostInitOptions)
	assert.That(t, ok)
	assert.NoError(t, po.SetOption(adbc.OptionKeyAutoCommit, adbc.OptionValueDisabled))
	execSQL(t, writer, "INSERT INTO agg VALUES ('SF')")

	stats, err := CollectStateStats(context.Background(), reader, path)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), stats.Tables[0].Rows)

	committer, ok := writer.(interface{ Commit(context.Context) error })
	assert.That(t, ok)
	assert.NoError(t, committer.Commit(context.Background()))

	stats, err = CollectStateStats(context.Background(), reader, path)
	assert.NoError(t, err)
	assert.Equal(t, int64(2), stats.Tables[0].Rows)
}

// A table name is read out of a record reader, whose backing buffer is freed
// when the reader is released. Returning a string that aliases it would hand
// callers memory that can be reused underneath them.
func TestStateDurability_Stats_TableNamesSurviveReaderRelease(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	conn := newStateConn(t, path)
	assert.NoError(t, NewOffsetStore(conn).Init(context.Background()))

	execSQL(t, conn, "CREATE TABLE a_table_with_a_deliberately_long_name (i INTEGER)")

	stats, err := CollectStateStats(context.Background(), conn, path)
	assert.NoError(t, err)

	// Churn the allocator: if the name aliased the reader's buffer, this is
	// where it would be corrupted.
	for i := 0; i < 200; i++ {
		_, err := CollectStateStats(context.Background(), conn, path)
		assert.NoError(t, err)
	}

	assert.Equal(t, "a_table_with_a_deliberately_long_name", stats.Tables[0].Name)
}
