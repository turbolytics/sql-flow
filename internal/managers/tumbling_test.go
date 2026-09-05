package managers

import (
	"context"
	"errors"
	"github.com/turbolytics/sql-flow/internal/duckdb"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
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

func (s *recordingSink) WriteTable(ctx context.Context, batch arrow.Table) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rows += batch.NumRows()
	return nil
}

func (s *recordingSink) Flush(ctx context.Context) error {
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

func TestManagerTumblingWindow__PublishesAndDeletesClosedWindows(t *testing.T) {
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

func TestManagerTumblingWindow__NoClosedWindowsIsANoop(t *testing.T) {
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
func TestManagerTumblingWindow__StartPollsUntilContextCancelled(t *testing.T) {
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

// --- Failure paths: a window must never be dropped -------------------------

// failingSink fails on the nominated call, so each half of write-then-flush
// can be exercised separately.
//
// The flags are atomic because one test flips them while the manager's
// goroutine is polling.
type failingSink struct {
	recordingSink
	failWrite atomic.Bool
	failFlush atomic.Bool
}

func (s *failingSink) WriteTable(ctx context.Context, batch arrow.Table) error {
	if s.failWrite.Load() {
		return errors.New("sink unreachable")
	}
	return s.recordingSink.WriteTable(ctx, batch)
}

func (s *failingSink) Flush(ctx context.Context) error {
	if s.failFlush.Load() {
		return errors.New("broker rejected the batch")
	}
	return s.recordingSink.Flush(context.Background())
}

// A window that could not be written must stay in the table. Deleting it would
// lose the aggregate with no record anywhere that it existed.
func TestManagerTumblingWindow__SinkWriteFailureLeavesTheWindow(t *testing.T) {
	conn, cleanup := newTestConn(t)
	defer cleanup()
	seedWindows(t, conn)

	sink := &failingSink{}
	sink.failWrite.Store(true)
	m := newTestTumbling(conn, &sink.recordingSink)
	m.sink = sink

	err := m.Poll(context.Background())
	assert.Error(t, err)

	// All three rows survive: two closed, one open.
	assert.Equal(t, int64(3), countRows(t, conn, "agg_cities_count"))
}

// The same holds for a flush failure. Flush is where a Kafka sink blocks on
// broker acks, so this is the likely half to fail in production.
func TestManagerTumblingWindow__SinkFlushFailureLeavesTheWindow(t *testing.T) {
	conn, cleanup := newTestConn(t)
	defer cleanup()
	seedWindows(t, conn)

	sink := &failingSink{}
	sink.failFlush.Store(true)
	m := newTestTumbling(conn, &sink.recordingSink)
	m.sink = sink

	err := m.Poll(context.Background())
	assert.Error(t, err)
	assert.Equal(t, int64(3), countRows(t, conn, "agg_cities_count"))
}

// A failed poll must be retryable: the next one publishes the same window
// rather than skipping it.
//
// The retry re-writes rows the sink already received. WriteTable succeeded on
// the first attempt and only Flush failed, so a Kafka sink has those rows
// buffered and a retry sends them again. That is the at-least-once guarantee,
// and it is why a window sink wants a key it can deduplicate on.
func TestManagerTumblingWindow__RetriesAfterAFailedPoll(t *testing.T) {
	conn, cleanup := newTestConn(t)
	defer cleanup()
	seedWindows(t, conn)

	sink := &failingSink{}
	sink.failFlush.Store(true)
	m := newTestTumbling(conn, &sink.recordingSink)
	m.sink = sink

	assert.Error(t, m.Poll(context.Background()))

	// The sink recovers.
	sink.failFlush.Store(false)
	assert.NoError(t, m.Poll(context.Background()))

	// Four writes for two windows: both attempts wrote before the first one's
	// flush failed.
	rows, _ := sink.recordingSink.counts()
	assert.Equal(t, int64(4), rows)

	// The table is drained regardless, so the windows are not published a
	// third time.
	assert.Equal(t, int64(1), countRows(t, conn, "agg_cities_count"))
}

// A broken delete statement must surface as an error rather than silently
// republishing the same window on every poll.
func TestManagerTumblingWindow__DeleteFailureIsReported(t *testing.T) {
	conn, cleanup := newTestConn(t)
	defer cleanup()
	seedWindows(t, conn)

	sink := &recordingSink{}
	m := NewTumbling(conn, collectSQL,
		"DELETE FROM a_table_that_does_not_exist", time.Millisecond, sink, &sync.Mutex{})

	err := m.Poll(context.Background())
	assert.Error(t, err)

	// The window was published before the delete failed, so it is still in
	// the table and will be published again. That is at-least-once, and the
	// error is what tells an operator to fix the SQL.
	rows, _ := sink.counts()
	assert.Equal(t, int64(2), rows)
	assert.Equal(t, int64(3), countRows(t, conn, "agg_cities_count"))
}

// --- Close logic: only closed windows move ---------------------------------

// An open window must survive a poll untouched. Publishing it early would
// emit a partial aggregate as if it were final.
func TestManagerTumblingWindow__OpenWindowsAreNeitherPublishedNorDeleted(t *testing.T) {
	conn, cleanup := newTestConn(t)
	defer cleanup()

	exec(t, conn, `CREATE TABLE agg_cities_count (bucket TIMESTAMPTZ, city VARCHAR, count INT);`)
	// Every row is inside the open window.
	exec(t, conn, `INSERT INTO agg_cities_count VALUES
		(now()::timestamptz, 'NYC', 3),
		(now()::timestamptz, 'SF', 1);`)

	sink := &recordingSink{}
	m := newTestTumbling(conn, sink)
	assert.NoError(t, m.Poll(context.Background()))

	rows, _ := sink.counts()
	assert.Equal(t, int64(0), rows)
	assert.Equal(t, int64(2), countRows(t, conn, "agg_cities_count"))
}

// A window that closes between two polls is published on the second, not
// missed because the first poll saw it open.
func TestManagerTumblingWindow__PublishesAWindowThatClosesBetweenPolls(t *testing.T) {
	conn, cleanup := newTestConn(t)
	defer cleanup()

	exec(t, conn, `CREATE TABLE agg_cities_count (bucket TIMESTAMPTZ, city VARCHAR, count INT);`)
	exec(t, conn, `INSERT INTO agg_cities_count VALUES (now()::timestamptz, 'NYC', 3);`)

	sink := &recordingSink{}
	m := newTestTumbling(conn, sink)

	assert.NoError(t, m.Poll(context.Background()))
	rows, _ := sink.counts()
	assert.Equal(t, int64(0), rows)

	// Age the row past the close predicate rather than sleeping for it.
	exec(t, conn, `UPDATE agg_cities_count SET bucket = now()::timestamptz - INTERVAL '600' SECOND;`)

	assert.NoError(t, m.Poll(context.Background()))
	rows, _ = sink.counts()
	assert.Equal(t, int64(1), rows)
	assert.Equal(t, int64(0), countRows(t, conn, "agg_cities_count"))
}

// Polling a table whose closed windows have already been published must not
// publish them a second time.
func TestManagerTumblingWindow__RepeatedPollsDoNotRepublish(t *testing.T) {
	conn, cleanup := newTestConn(t)
	defer cleanup()
	seedWindows(t, conn)

	sink := &recordingSink{}
	m := newTestTumbling(conn, sink)

	assert.NoError(t, m.Poll(context.Background()))
	assert.NoError(t, m.Poll(context.Background()))
	assert.NoError(t, m.Poll(context.Background()))

	rows, _ := sink.counts()
	assert.Equal(t, int64(2), rows)
}

// --- Shutdown --------------------------------------------------------------

// Start runs one final poll after its context is cancelled, so a window that
// closes during shutdown is published rather than stranded in the table.
func TestManagerTumblingWindow__FinalPollOnShutdownPublishesAClosedWindow(t *testing.T) {
	conn, cleanup := newTestConn(t)
	defer cleanup()
	seedWindows(t, conn)

	sink := &recordingSink{}
	// A poll interval longer than the test guarantees the only poll that runs
	// is the final one.
	m := NewTumbling(conn, collectSQL, deleteSQL, time.Hour, sink, &sync.Mutex{})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- m.Start(ctx) }()

	// Nothing published yet: the ticker will not fire for an hour.
	time.Sleep(50 * time.Millisecond)
	rows, _ := sink.counts()
	assert.Equal(t, int64(0), rows)

	cancel()
	select {
	case err := <-done:
		assert.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Start did not return after cancellation")
	}

	rows, _ = sink.counts()
	assert.Equal(t, int64(2), rows)
	assert.Equal(t, int64(1), countRows(t, conn, "agg_cities_count"))
}

// A failing poll must not kill the manager: the rows stay and the next tick
// retries them.
func TestManagerTumblingWindow__StartSurvivesAFailedPoll(t *testing.T) {
	conn, cleanup := newTestConn(t)
	defer cleanup()
	seedWindows(t, conn)

	sink := &failingSink{}
	sink.failFlush.Store(true)
	m := NewTumbling(conn, collectSQL, deleteSQL, 5*time.Millisecond,
		&sink.recordingSink, &sync.Mutex{})
	m.sink = sink

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- m.Start(ctx) }()

	time.Sleep(60 * time.Millisecond)
	sink.failFlush.Store(false)

	deadline := time.After(5 * time.Second)
	for {
		if rows, _ := sink.recordingSink.counts(); rows >= 2 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("manager stopped polling after a failure")
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

// --- Interaction with the pipeline's state transaction ---------------------

// A stateful pipeline runs with autocommit disabled, and the manager shares
// its connection. The manager's DELETE therefore lands inside the pipeline's
// open transaction rather than committing on its own.
//
// That is correct, and these tests pin the two halves of why. Within the
// process the manager sees its own delete, so a window is published once. The
// delete becomes durable only when the pipeline commits its next batch, so a
// crash in between republishes the window on restart -- at-least-once, the
// same guarantee the sink path gives.
func TestManagerTumblingWindow__DeleteJoinsThePipelineTransaction(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	db, err := duckdb.OpenPath(context.Background(), path)
	assert.NoError(t, err)
	defer db.Close()

	writer, err := db.Connect(context.Background())
	assert.NoError(t, err)
	defer writer.Close()
	observer, err := db.Connect(context.Background())
	assert.NoError(t, err)
	defer observer.Close()

	seedWindows(t, writer)

	// The pipeline turns autocommit off, as it does for a state path.
	po, ok := writer.(adbc.PostInitOptions)
	assert.That(t, ok)
	assert.NoError(t, po.SetOption(adbc.OptionKeyAutoCommit, adbc.OptionValueDisabled))

	sink := &recordingSink{}
	m := newTestTumbling(writer, sink)
	assert.NoError(t, m.Poll(context.Background()))

	rows, _ := sink.counts()
	assert.Equal(t, int64(2), rows)

	// The manager sees its own delete, so a second poll republishes nothing.
	assert.Equal(t, int64(1), countRows(t, writer, "agg_cities_count"))
	assert.NoError(t, m.Poll(context.Background()))
	rows, _ = sink.counts()
	assert.Equal(t, int64(2), rows)

	// Another connection still sees all three rows: the delete is not
	// durable yet. A crash here republishes those windows on restart.
	assert.Equal(t, int64(3), countRows(t, observer, "agg_cities_count"))

	// The pipeline's next batch commits, and the delete becomes durable.
	committer, ok := writer.(interface{ Commit(context.Context) error })
	assert.That(t, ok)
	assert.NoError(t, committer.Commit(context.Background()))
	assert.Equal(t, int64(1), countRows(t, observer, "agg_cities_count"))
}

// A rollback discards the manager's delete along with the batch that failed.
// The window was already published, so the next poll publishes it again --
// at-least-once rather than a lost window.
func TestManagerTumblingWindow__DeleteRollsBackWithTheBatch(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	db, err := duckdb.OpenPath(context.Background(), path)
	assert.NoError(t, err)
	defer db.Close()

	writer, err := db.Connect(context.Background())
	assert.NoError(t, err)
	defer writer.Close()

	seedWindows(t, writer)

	po, ok := writer.(adbc.PostInitOptions)
	assert.That(t, ok)
	assert.NoError(t, po.SetOption(adbc.OptionKeyAutoCommit, adbc.OptionValueDisabled))

	sink := &recordingSink{}
	m := newTestTumbling(writer, sink)
	assert.NoError(t, m.Poll(context.Background()))
	assert.Equal(t, int64(1), countRows(t, writer, "agg_cities_count"))

	// The pipeline's batch fails and rolls back.
	roller, ok := writer.(interface{ Rollback(context.Context) error })
	assert.That(t, ok)
	assert.NoError(t, roller.Rollback(context.Background()))

	// The closed windows are back, and the next poll republishes them.
	assert.Equal(t, int64(3), countRows(t, writer, "agg_cities_count"))
	assert.NoError(t, m.Poll(context.Background()))
	rows, _ := sink.counts()
	assert.Equal(t, int64(4), rows)
}

// The bug this pins: with autocommit disabled the connection holds one open
// transaction, and DuckDB's now() returns that transaction's start time. A
// window close predicate written against now() is therefore evaluated against
// a frozen clock, so a window that closes while the pipeline is idle is never
// collected -- no error, no metric, and the rows keep being reported as live
// state.
//
// Committing is what advances the clock, which is why the consume loop now
// commits on its flush tick even with nothing buffered.
func TestManagerTumblingWindow__ClockAdvancesOnlyAcrossACommit(t *testing.T) {
	conn, cleanup := newTestConn(t)
	defer cleanup()

	exec(t, conn, `CREATE TABLE agg_cities_count (bucket TIMESTAMPTZ, city VARCHAR, count INT);`)
	// One window, open now and closed two seconds from now.
	exec(t, conn, `INSERT INTO agg_cities_count VALUES (now()::timestamptz, 'NYC', 3);`)

	po, ok := conn.(adbc.PostInitOptions)
	assert.That(t, ok)
	assert.NoError(t, po.SetOption(adbc.OptionKeyAutoCommit, adbc.OptionValueDisabled))
	tx := conn.(interface {
		Commit(context.Context) error
		Rollback(context.Context) error
	})

	const closeAfter = `(now()::timestamptz - INTERVAL '2' SECOND)`
	sink := &recordingSink{}
	m := NewTumbling(conn,
		`SELECT bucket, city, count FROM agg_cities_count WHERE bucket < `+closeAfter,
		`DELETE FROM agg_cities_count WHERE bucket < `+closeAfter,
		time.Hour, sink, &sync.Mutex{})

	// This poll pins the transaction clock.
	assert.NoError(t, m.Poll(context.Background()))
	rows, _ := sink.counts()
	assert.Equal(t, int64(0), rows)

	// Wall clock crosses the threshold.
	time.Sleep(3 * time.Second)

	// Still frozen, so still nothing -- this is the failure users would see.
	assert.NoError(t, m.Poll(context.Background()))
	rows, _ = sink.counts()
	assert.Equal(t, int64(0), rows)

	// A commit is what moves the clock.
	assert.NoError(t, tx.Commit(context.Background()))

	assert.NoError(t, m.Poll(context.Background()))
	rows, _ = sink.counts()
	assert.Equal(t, int64(1), rows)
	assert.NoError(t, tx.Commit(context.Background()))
	assert.Equal(t, int64(0), countRows(t, conn, "agg_cities_count"))
}
