package managers

import (
	"context"
	"fmt"
	"github.com/turbolytics/sql-flow/internal/core"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/turbolytics/sql-flow/internal/duckdb"
)

// testDB is one DuckDB with the two connections a windowed pipeline has: the
// pipeline's, which the handler writes through, and the manager's, with
// autocommit off, which the watermark manager owns.
type testDB struct {
	db       *duckdb.DB
	pipeline adbc.Connection
	manager  adbc.Connection
}

// newTestDB opens the database at path, or in memory for "", and both
// connections. The windows table is created on the pipeline connection under
// autocommit, the way run does it.
func newTestDB(tb testing.TB, path string) *testDB {
	tb.Helper()
	ctx := context.Background()
	db, err := duckdb.OpenPath(ctx, path)
	if err != nil {
		tb.Fatal(err)
	}
	pipeline, err := db.Connect(ctx)
	if err != nil {
		tb.Fatal(err)
	}
	if err := NewStore(pipeline).Init(ctx); err != nil {
		tb.Fatal(err)
	}
	if err := core.NewWatermarkStore(pipeline).Init(ctx); err != nil {
		tb.Fatal(err)
	}
	manager := managerConn(tb, db)
	tb.Cleanup(func() {
		manager.Close()
		pipeline.Close()
		db.Close()
	})
	return &testDB{db: db, pipeline: pipeline, manager: manager}
}

// managerConn opens a connection with autocommit off, which is what a
// watermark manager runs on.
func managerConn(tb testing.TB, db *duckdb.DB) adbc.Connection {
	tb.Helper()
	conn, err := db.Connect(context.Background())
	if err != nil {
		tb.Fatal(err)
	}
	po, ok := conn.(adbc.PostInitOptions)
	if !ok {
		tb.Fatal("connection has no options")
	}
	if err := po.SetOption(adbc.OptionKeyAutoCommit, adbc.OptionValueDisabled); err != nil {
		tb.Fatal(err)
	}
	return conn
}

// newTestConn is one connection to a fresh in-memory database, for tests
// that need no second one.
func newTestConn(tb testing.TB) (adbc.Connection, func()) {
	tb.Helper()
	d := newTestDB(tb, "")
	return d.pipeline, func() {}
}

func exec(tb testing.TB, conn adbc.Connection, sql string) {
	tb.Helper()
	if _, err := execRows(context.Background(), conn, sql); err != nil {
		tb.Fatalf("%s: %v", sql, err)
	}
}

func countRows(tb testing.TB, conn adbc.Connection, table string) int64 {
	tb.Helper()
	n, _, err := queryInt64(context.Background(), conn, "SELECT count(*)::BIGINT FROM "+quoteIdent(table))
	if err != nil {
		tb.Fatal(err)
	}
	return n
}

// recordingSink captures what the manager publishes.
type recordingSink struct {
	mu      sync.Mutex
	rows    int64
	flushes int
	// batches holds every published table's first column as text, per
	// flush, so a test can see which buckets left.
	batches [][]string
	pending []string
}

func (s *recordingSink) WriteTable(ctx context.Context, batch arrow.Table) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rows += batch.NumRows()
	s.pending = append(s.pending, firstColumn(batch)...)
	return nil
}

func (s *recordingSink) Flush(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.flushes++
	s.batches = append(s.batches, s.pending)
	s.pending = nil
	return nil
}

func (s *recordingSink) counts() (int64, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.rows, s.flushes
}

func (s *recordingSink) published() [][]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([][]string(nil), s.batches...)
}

// firstColumn renders the first column of every record as text.
func firstColumn(t arrow.Table) []string {
	var out []string
	col := t.Column(0)
	for _, chunk := range col.Data().Chunks() {
		for i := 0; i < chunk.Len(); i++ {
			// Cloned: the value aliases the record's buffer, which the
			// manager releases once the sink returns.
			out = append(out, strings.Clone(chunk.ValueStr(i)))
		}
	}
	return out
}

// failingSink refuses every flush.
type failingSink struct {
	recordingSink
	err error
}

func (s *failingSink) Flush(ctx context.Context) error {
	s.mu.Lock()
	s.flushes++
	s.mu.Unlock()
	return s.err
}

// hangingSink blocks in Flush until its context ends.
type hangingSink struct{}

func (hangingSink) WriteTable(context.Context, arrow.Table) error { return nil }
func (hangingSink) Flush(ctx context.Context) error {
	<-ctx.Done()
	return ctx.Err()
}

// The window under test: one-minute buckets on agg_cities_count.
const testTable = "agg_cities_count"

func testDecl() Declaration {
	return Declaration{
		Table:      testTable,
		TimeColumn: "bucket",
		Size:       time.Minute,
		Grace:      time.Minute,
		IdleClose:  5 * time.Minute,
		Late:       LateReemit,
	}
}

// t0 is the start of the first bucket every test seeds.
var t0 = time.Date(2026, 9, 13, 10, 0, 0, 0, time.UTC)

// bucket is the start of the nth bucket after t0.
func bucket(n int) time.Time { return t0.Add(time.Duration(n) * time.Minute) }

func createWindowTable(tb testing.TB, conn adbc.Connection) {
	tb.Helper()
	exec(tb, conn, `CREATE TABLE IF NOT EXISTS agg_cities_count (bucket TIMESTAMPTZ, city VARCHAR, count INT)`)
}

// insertBucket writes one row into the nth bucket.
func insertBucket(tb testing.TB, conn adbc.Connection, n int, city string, count int) {
	tb.Helper()
	exec(tb, conn, fmt.Sprintf(`INSERT INTO agg_cities_count VALUES (TIMESTAMPTZ '%s', '%s', %d)`,
		bucket(n).Format("2006-01-02 15:04:05-07:00"), city, count))
}

// assertAt writes the engine's watermark for the test table by hand, as a
// batch's commit leaves it. The table is created if no engine did.
func assertAt(tb testing.TB, conn adbc.Connection, at time.Time) {
	tb.Helper()
	if err := core.NewWatermarkStore(conn).Init(context.Background()); err != nil {
		tb.Fatal(err)
	}
	if err := core.NewWatermarkStore(conn).Save(context.Background(), testTable, at); err != nil {
		tb.Fatal(err)
	}
}

// newTestWatermark builds the manager on the manager connection. The
// engine's watermark table exists from here, empty until assertAt.
func newTestWatermark(tb testing.TB, d *testDB, decl Declaration, sink interface {
	WriteTable(context.Context, arrow.Table) error
	Flush(context.Context) error
}, opts ...Option) *Watermark {
	tb.Helper()
	if err := core.NewWatermarkStore(d.pipeline).Init(context.Background()); err != nil {
		tb.Fatal(err)
	}
	w, err := NewWatermark(d.manager, decl, time.Hour, sink, opts...)
	if err != nil {
		tb.Fatal(err)
	}
	return w
}

var _ = array.NewInt64Builder
