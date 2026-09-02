package core

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/turbolytics/sql-flow/internal/duckdb"
	"github.com/zeebo/assert"
)

func newStateConn(t *testing.T, path string) adbc.Connection {
	t.Helper()
	if os.Getenv("SQLFLOW_DUCKDB_LIB") == "" {
		os.Setenv("SQLFLOW_DUCKDB_LIB", "/opt/homebrew/lib/libduckdb.dylib")
	}
	db, err := duckdb.OpenPath(context.Background(), path)
	assert.NoError(t, err)
	conn, err := db.Connect(context.Background())
	assert.NoError(t, err)
	t.Cleanup(func() { conn.Close(); db.Close() })
	return conn
}

// Offsets round-trip through DuckDB, which is what lets a restart resume from
// the position that produced the state currently in the database.
func TestOffsetStore_RoundTrip(t *testing.T) {
	conn := newStateConn(t, filepath.Join(t.TempDir(), "state.db"))
	s := NewOffsetStore(conn)
	assert.NoError(t, s.Init(context.Background()))

	in := NewMarks()
	in.Advance("events", 0, Mark{Offset: 41, LeaderEpoch: 7})
	in.Advance("events", 1, Mark{Offset: 8, LeaderEpoch: 7})
	assert.NoError(t, s.Save(context.Background(), in))

	out, err := s.Load(context.Background())
	assert.NoError(t, err)
	p0, ok := out.Get("events", 0)
	assert.That(t, ok)
	assert.Equal(t, int64(41), p0.Offset)
	assert.Equal(t, int32(7), p0.LeaderEpoch)
	p1, _ := out.Get("events", 1)
	assert.Equal(t, int64(8), p1.Offset)
}

// Saving the same partition again advances it rather than duplicating it.
func TestOffsetStore_SaveIsAnUpsert(t *testing.T) {
	conn := newStateConn(t, filepath.Join(t.TempDir(), "state.db"))
	s := NewOffsetStore(conn)
	assert.NoError(t, s.Init(context.Background()))

	first := NewMarks()
	first.Advance("events", 0, Mark{Offset: 10, LeaderEpoch: 1})
	assert.NoError(t, s.Save(context.Background(), first))
	second := NewMarks()
	second.Advance("events", 0, Mark{Offset: 99, LeaderEpoch: 2})
	assert.NoError(t, s.Save(context.Background(), second))

	out, err := s.Load(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 1, out.Len())
	got, _ := out.Get("events", 0)
	assert.Equal(t, int64(99), got.Offset)
	assert.Equal(t, int32(2), got.LeaderEpoch)
}

// A fresh state file has no offsets; the caller must treat that as "start
// where auto_offset_reset says", not as offset zero.
func TestOffsetStore_LoadEmptyIsEmptyNotZero(t *testing.T) {
	conn := newStateConn(t, filepath.Join(t.TempDir(), "state.db"))
	s := NewOffsetStore(conn)
	assert.NoError(t, s.Init(context.Background()))

	out, err := s.Load(context.Background())
	assert.NoError(t, err)
	assert.That(t, out.Empty())
}

// Init runs on every start, including against an existing state file.
func TestOffsetStore_InitIsIdempotent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	conn := newStateConn(t, path)
	s := NewOffsetStore(conn)
	assert.NoError(t, s.Init(context.Background()))
	assert.NoError(t, s.Init(context.Background()))
}
