package managers

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/drivermgr"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/zeebo/assert"
)

func newTestConn(tb testing.TB) (adbc.Connection, func()) {
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

func countRows(tb testing.TB, conn adbc.Connection, table string) int64 {
	tb.Helper()

	stmt, err := conn.NewStatement()
	if err != nil {
		tb.Fatal(err)
	}
	defer stmt.Close()

	if err := stmt.SetSqlQuery("SELECT COUNT(*) FROM " + table); err != nil {
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
			return rec.Column(0).(*array.Int64).Value(0)
		}
	}
	return -1
}

// recordingSink captures what the manager publishes.
type recordingSink struct {
	mu      sync.Mutex
	rows    int64
	flushes int
}

func (s *recordingSink) WriteTable(batch arrow.Table) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rows += batch.NumRows()
	return nil
}

func (s *recordingSink) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.flushes++
	return nil
}

func (s *recordingSink) Batch() (arrow.Table, error) { return nil, nil }

func (s *recordingSink) counts() (int64, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.rows, s.flushes
}

// seedWindows creates the windowed table with one closed and one open window.
func seedWindows(tb testing.TB, conn adbc.Connection) {
	exec(tb, conn, `CREATE TABLE agg_cities_count (bucket TIMESTAMPTZ, city VARCHAR, count INT);`)
	exec(tb, conn, `INSERT INTO agg_cities_count VALUES
		(now()::timestamptz - INTERVAL '600' SECOND, 'NYC', 3),
		(now()::timestamptz - INTERVAL '600' SECOND, 'SF', 1),
		(now()::timestamptz, 'LA', 7);`)
}

const (
	collectSQL = `SELECT bucket, city, count FROM agg_cities_count
		WHERE bucket < (now()::timestamptz - INTERVAL '60' SECOND) ORDER BY city`
	deleteSQL = `DELETE FROM agg_cities_count
		WHERE bucket < (now()::timestamptz - INTERVAL '60' SECOND)`
)

func newTestTumbling(conn adbc.Connection, sink *recordingSink) *Tumbling {
	return NewTumbling(conn, collectSQL, deleteSQL, time.Millisecond, sink, &sync.Mutex{})
}

func TestTumbling_PublishesAndDeletesClosedWindows(t *testing.T) {
	conn, cleanup := newTestConn(t)
	defer cleanup()
	seedWindows(t, conn)

	sink := &recordingSink{}
	m := newTestTumbling(conn, sink)

	assert.NoError(t, m.Poll(context.Background()))

	rows, flushes := sink.counts()
	assert.Equal(t, int64(2), rows)
	assert.That(t, flushes > 0)

	// Only the open window survives.
	assert.Equal(t, int64(1), countRows(t, conn, "agg_cities_count"))
}

func TestTumbling_NoClosedWindowsIsANoop(t *testing.T) {
	conn, cleanup := newTestConn(t)
	defer cleanup()

	exec(t, conn, `CREATE TABLE agg_cities_count (bucket TIMESTAMPTZ, city VARCHAR, count INT);`)
	exec(t, conn, `INSERT INTO agg_cities_count VALUES (now()::timestamptz, 'LA', 7);`)

	sink := &recordingSink{}
	m := newTestTumbling(conn, sink)

	assert.NoError(t, m.Poll(context.Background()))

	rows, _ := sink.counts()
	assert.Equal(t, int64(0), rows)
	assert.Equal(t, int64(1), countRows(t, conn, "agg_cities_count"))
}

// Start must poll until its context is cancelled, and return cleanly.
func TestTumbling_StartPollsUntilContextCancelled(t *testing.T) {
	conn, cleanup := newTestConn(t)
	defer cleanup()
	seedWindows(t, conn)

	sink := &recordingSink{}
	m := newTestTumbling(conn, sink)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- m.Start(ctx) }()

	deadline := time.After(5 * time.Second)
	for {
		if rows, _ := sink.counts(); rows >= 2 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("manager did not publish closed windows")
		default:
			time.Sleep(5 * time.Millisecond)
		}
	}

	cancel()
	select {
	case err := <-done:
		assert.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Start did not return after cancellation")
	}
}
