package core

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/turbolytics/sql-flow/internal/errs"
	"github.com/zeebo/assert"
)

// exec runs one statement and reports whether DuckDB accepted it.
func exec(t *testing.T, conn adbc.Connection, sql string) {
	t.Helper()
	stmt, err := conn.NewStatement()
	assert.NoError(t, err)
	defer stmt.Close()
	assert.NoError(t, stmt.SetSqlQuery(sql))
	_, err = stmt.ExecuteUpdate(context.Background())
	assert.NoError(t, err)
}

// columnsOf reports the offsets table's columns as "name type" strings, so a
// test can prove Init left a damaged table exactly as it found it.
func columnsOf(t *testing.T, conn adbc.Connection) []string {
	t.Helper()
	stmt, err := conn.NewStatement()
	assert.NoError(t, err)
	defer stmt.Close()
	assert.NoError(t, stmt.SetSqlQuery(fmt.Sprintf(
		`SELECT column_name || ' ' || data_type FROM information_schema.columns
		 WHERE table_name = '%s' ORDER BY ordinal_position`, offsetsTable)))
	reader, _, err := stmt.ExecuteQuery(context.Background())
	assert.NoError(t, err)
	defer reader.Release()

	var out []string
	for reader.Next() {
		rec := reader.Record()
		col, ok := rec.Column(0).(*array.String)
		assert.That(t, ok)
		for i := 0; i < int(rec.NumRows()); i++ {
			// Clone: the value aliases the record buffer, which the next
			// call to Next may reuse.
			out = append(out, strings.Clone(col.Value(i)))
		}
	}
	return out
}

// A state file whose offsets table has the wrong columns is not a state file
// this build can read. Creating it fresh would restart from the beginning and
// call that healthy.
func TestOffsetStore_InitRejectsWrongColumns(t *testing.T) {
	conn := newStateConn(t, filepath.Join(t.TempDir(), "state.db"))
	exec(t, conn, `CREATE TABLE `+offsetsTable+` (topic VARCHAR, partition INTEGER)`)

	err := NewOffsetStore(conn).Init(context.Background())

	assert.Error(t, err)
	assert.Equal(t, errs.CodeStateCorrupt, errs.CodeOf(err))
}

// Right names, wrong types. Load only notices this when the table happens to
// hold rows, so the check has to happen at open.
func TestOffsetStore_InitRejectsWrongTypes(t *testing.T) {
	conn := newStateConn(t, filepath.Join(t.TempDir(), "state.db"))
	exec(t, conn, `CREATE TABLE `+offsetsTable+` (
		topic VARCHAR, partition VARCHAR, "offset" VARCHAR, leader_epoch VARCHAR)`)

	err := NewOffsetStore(conn).Init(context.Background())

	assert.Error(t, err)
	assert.Equal(t, errs.CodeStateCorrupt, errs.CodeOf(err))
}

// The error has to name the table and say what was wrong, or an operator
// cannot tell a damaged state file from any other startup failure.
func TestOffsetStore_InitNamesWhatIsWrong(t *testing.T) {
	conn := newStateConn(t, filepath.Join(t.TempDir(), "state.db"))
	exec(t, conn, `CREATE TABLE `+offsetsTable+` (topic VARCHAR, partition INTEGER)`)

	err := NewOffsetStore(conn).Init(context.Background())

	assert.Error(t, err)
	msg := err.Error()
	for _, want := range []string{offsetsTable, "offset"} {
		if !strings.Contains(msg, want) {
			t.Errorf("error %q does not mention %q", msg, want)
		}
	}
}

// Never truncate, recreate, or silently repair. A damaged table is evidence an
// operator needs, and it is the only copy of the positions.
func TestOffsetStore_InitLeavesADamagedTableUntouched(t *testing.T) {
	conn := newStateConn(t, filepath.Join(t.TempDir(), "state.db"))
	exec(t, conn, `CREATE TABLE `+offsetsTable+` (topic VARCHAR, partition INTEGER)`)
	exec(t, conn, `INSERT INTO `+offsetsTable+` VALUES ('events', 3)`)
	before := columnsOf(t, conn)

	assert.Error(t, NewOffsetStore(conn).Init(context.Background()))

	assert.DeepEqual(t, before, columnsOf(t, conn))
	n, err := countRows(context.Background(), conn, offsetsTable)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), n)
}

// A fresh database is the normal first run and must still work.
func TestOffsetStore_InitAcceptsAFreshDatabase(t *testing.T) {
	conn := newStateConn(t, filepath.Join(t.TempDir(), "state.db"))
	assert.NoError(t, NewOffsetStore(conn).Init(context.Background()))

	marks, err := NewOffsetStore(conn).Load(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 0, marks.Len())
}

// A healthy file from a previous run resumes, and Init does not disturb it.
func TestOffsetStore_InitAcceptsAPriorRunAndKeepsItsRows(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	conn := newStateConn(t, path)
	s := NewOffsetStore(conn)
	assert.NoError(t, s.Init(context.Background()))
	m := NewMarks()
	m.Advance("events", 0, Mark{Offset: 99, LeaderEpoch: 4})
	assert.NoError(t, s.Save(context.Background(), m))

	// Same file, second run.
	assert.NoError(t, s.Init(context.Background()))

	out, err := s.Load(context.Background())
	assert.NoError(t, err)
	got, ok := out.Get("events", 0)
	assert.That(t, ok)
	assert.Equal(t, int64(99), got.Offset)
}

// A path that exists but is not a DuckDB database is a damaged state file, not
// a fresh one. Exit terminal so a supervisor stops rather than crash-looping.
func TestOpenState_RejectsAFileThatIsNotADatabase(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	assert.NoError(t, os.WriteFile(path, []byte("not a duckdb database"), 0o644))

	_, err := OpenState(context.Background(), path)

	assert.Error(t, err)
	assert.Equal(t, errs.CodeStateCorrupt, errs.CodeOf(err))
	assert.Equal(t, errs.ExitStateCorrupt, errs.ExitCode(err))
}

// A zero-byte file is the shape a crash between create and first write leaves.
func TestOpenState_RejectsAZeroByteFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	assert.NoError(t, os.WriteFile(path, nil, 0o644))

	_, err := OpenState(context.Background(), path)

	assert.Error(t, err)
	assert.Equal(t, errs.CodeStateCorrupt, errs.CodeOf(err))
}

// A directory at the state path is a config mistake, not a damaged file. The
// operator fixes the config, so it must not read as state corruption.
func TestOpenState_ReportsADirectoryAsAConfigError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	assert.NoError(t, os.MkdirAll(path, 0o755))

	_, err := OpenState(context.Background(), path)

	assert.Error(t, err)
	assert.Equal(t, errs.CodeConfigInvalid, errs.CodeOf(err))
}

// A missing file is the first run. It must open and create.
func TestOpenState_CreatesAMissingFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "state.db")

	db, err := OpenState(context.Background(), path)
	assert.NoError(t, err)
	t.Cleanup(func() { db.Close() })

	conn, err := db.Connect(context.Background())
	assert.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	assert.NoError(t, NewOffsetStore(conn).Init(context.Background()))
}

// A damaged file must survive the failed start. It is the only copy of the
// positions, and an operator needs it to recover them.
func TestOpenState_LeavesADamagedFileOnDisk(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	content := []byte("not a duckdb database")
	assert.NoError(t, os.WriteFile(path, content, 0o644))

	_, err := OpenState(context.Background(), path)
	assert.Error(t, err)

	after, readErr := os.ReadFile(path)
	assert.NoError(t, readErr)
	assert.Equal(t, string(content), string(after))
}
