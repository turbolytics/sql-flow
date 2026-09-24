package simulate

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/turbolytics/sql-flow/internal/core"
	"github.com/turbolytics/sql-flow/internal/duckdb"
	"github.com/turbolytics/sql-flow/internal/managers"
)

// The windowed runner pairs the real consume loop with a real window manager
// over one DuckDB, which is the interaction neither the model nor the manager
// tests cover: the model has no engine under it, and the manager tests write
// the progress row by hand rather than having a loop write it.
//
// The handler does what a windowed pipeline's handler does: it inserts a row
// per batch into the window table, bucketed by event time, and the pipeline
// sink receives nothing. The manager polls its own connection, as it does in
// production.

const windowTable = "sim_window"

// Poll runs the manager once, as its ticker would.
type Poll struct{}

// windowDB is the pipeline's connection and the manager's, over one database.
type windowDB struct {
	db       *duckdb.DB
	pipeline adbc.Connection
	manager  adbc.Connection
	// reader is the script's own connection. The loop owns the pipeline one
	// and takes no lock on the runner's behalf, and two statements on one
	// DuckDB connection close each other's pending results (#280).
	reader adbc.Connection
}

func openWindowDB(t *testing.T) *windowDB {
	t.Helper()
	ctx := context.Background()
	db, err := duckdb.OpenPath(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	pipeline, err := db.Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := managers.NewStore(pipeline).Init(ctx); err != nil {
		t.Fatal(err)
	}
	if err := core.NewProgressStore(pipeline).Init(ctx); err != nil {
		t.Fatal(err)
	}
	execSQL(t, pipeline, fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS %s (bucket TIMESTAMPTZ, n BIGINT)`, windowTable))

	manager, err := db.Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	po, ok := manager.(adbc.PostInitOptions)
	if !ok {
		t.Fatal("connection has no options")
	}
	// The manager commits its own closes, as it does in production.
	if err := po.SetOption(adbc.OptionKeyAutoCommit, adbc.OptionValueDisabled); err != nil {
		t.Fatal(err)
	}

	reader, err := db.Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		reader.Close()
		manager.Close()
		pipeline.Close()
		db.Close()
	})
	return &windowDB{db: db, pipeline: pipeline, manager: manager, reader: reader}
}

func execSQL(t *testing.T, conn adbc.Connection, q string) {
	t.Helper()
	stmt, err := conn.NewStatement()
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()
	if err := stmt.SetSqlQuery(q); err != nil {
		t.Fatal(err)
	}
	if _, err := stmt.ExecuteUpdate(context.Background()); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
}

func queryInt(t *testing.T, conn adbc.Connection, q string) int64 {
	t.Helper()
	stmt, err := conn.NewStatement()
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()
	if err := stmt.SetSqlQuery(q); err != nil {
		t.Fatal(err)
	}
	reader, _, err := stmt.ExecuteQuery(context.Background())
	if err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	defer reader.Release()
	for reader.Next() {
		rec := reader.Record()
		if rec.NumRows() == 0 {
			continue
		}
		col, ok := rec.Column(0).(*array.Int64)
		if !ok || col.IsNull(0) {
			return 0
		}
		return col.Value(0)
	}
	return 0
}

// windowHandler writes one row per batch into the window table, bucketed by
// the simulated clock, which is what a windowed pipeline's handler SQL does.
type windowHandler struct {
	t     *testing.T
	conn  adbc.Connection
	clock func() time.Time
	size  time.Duration

	mu sync.Mutex
	n  int64
}

func (h *windowHandler) Init(context.Context) error { return nil }

func (h *windowHandler) Write([]byte) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.n++
	return nil
}

func (h *windowHandler) Invoke(context.Context) (arrow.Table, error) {
	h.mu.Lock()
	n := h.n
	h.n = 0
	h.mu.Unlock()

	if n > 0 {
		bucket := h.clock().UTC().Truncate(h.size)
		execSQL(h.t, h.conn, fmt.Sprintf(
			`INSERT INTO %s VALUES (TIMESTAMPTZ '%s', %d)`,
			windowTable, bucket.Format("2006-01-02 15:04:05-07:00"), n))
	}

	// The pipeline sink receives nothing: the window's sink is what publishes.
	schema := arrow.NewSchema([]arrow.Field{{Name: "n", Type: arrow.PrimitiveTypes.Int64}}, nil)
	b := array.NewInt64Builder(memory.DefaultAllocator)
	defer b.Release()
	col := b.NewArray()
	defer col.Release()
	rec := array.NewRecord(schema, []arrow.Array{col}, 0)
	defer rec.Release()
	return array.NewTableFromRecords(schema, []arrow.Record{rec}), nil
}

func (h *windowHandler) RowsRead() int64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.n
}

// windowSink counts the rows every close published.
type windowSink struct {
	mu      sync.Mutex
	rows    int64
	buckets map[int64]int
}

func newWindowSink() *windowSink { return &windowSink{buckets: map[int64]int{}} }

func (s *windowSink) WriteTable(_ context.Context, t arrow.Table) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var bucketCol, nCol int = -1, -1
	for i := 0; i < int(t.NumCols()); i++ {
		switch t.Schema().Field(i).Name {
		case "bucket":
			bucketCol = i
		case "n":
			nCol = i
		}
	}
	if nCol < 0 {
		return fmt.Errorf("published table has no n column")
	}
	for _, chunk := range t.Column(nCol).Data().Chunks() {
		ns := chunk.(*array.Int64)
		for i := 0; i < ns.Len(); i++ {
			s.rows += ns.Value(i)
		}
	}
	if bucketCol >= 0 {
		// Counted once per flush: one close publishes several rows for a
		// bucket, and republication is the same bucket in two closes.
		inThisFlush := map[int64]bool{}
		for _, chunk := range t.Column(bucketCol).Data().Chunks() {
			if ts, ok := chunk.(*array.Timestamp); ok {
				for i := 0; i < ts.Len(); i++ {
					inThisFlush[int64(ts.Value(i))] = true
				}
			}
		}
		for b := range inThisFlush {
			s.buckets[b]++
		}
	}
	return nil
}

func (s *windowSink) Flush(context.Context) error { return nil }

func (s *windowSink) counts() (rows int64, republished int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, n := range s.buckets {
		if n > 1 {
			republished += n - 1
		}
	}
	return s.rows, republished
}

// WindowResult is what a windowed run produced and what its window published.
type WindowResult struct {
	Produced    int
	Published   int64
	StillOpen   int64
	Republished int
	LateDropped int64
}

// RunWindowed replays a script against a real consume loop and a real window
// manager over one database, and reports what the window published.
func RunWindowed(t *testing.T, owned []int32, decl managers.Declaration, script []Step) WindowResult {
	t.Helper()
	db := openWindowDB(t)

	r := &run{
		t:     t,
		coord: newCoordinator(),
		sink:  newSink(),
		now:   time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC),
		owned: owned,
	}
	r.window = &windowRun{db: db, decl: decl, sink: newWindowSink()}
	r.window.handler = &windowHandler{t: t, conn: db.pipeline, clock: r.clock, size: decl.Size}

	wm, err := managers.NewWatermark(db.manager, decl, time.Hour, r.window.sink)
	if err != nil {
		t.Fatal(err)
	}
	r.window.manager = wm

	r.start()
	for _, s := range script {
		r.elapse()
		s.apply(r)
	}
	r.stop()

	rows, republished := r.window.sink.counts()
	return WindowResult{
		Produced:    r.produced,
		Published:   rows,
		StillOpen:   queryInt(t, db.reader, fmt.Sprintf(`SELECT coalesce(sum(n), 0)::BIGINT FROM %s`, windowTable)),
		Republished: republished,
		LateDropped: r.window.lateDropped,
	}
}

// windowRun is the window half of a run: the database, the manager, and what
// the window's sink received.
type windowRun struct {
	db          *windowDB
	decl        managers.Declaration
	handler     *windowHandler
	manager     *managers.Watermark
	sink        *windowSink
	lateDropped int64
}

func (Poll) apply(r *run) {
	if r.window == nil {
		r.t.Fatal("Poll needs a windowed run")
	}
	if err := r.window.manager.Poll(context.Background()); err != nil {
		r.t.Fatalf("the manager's poll failed: %v", err)
	}
}

// lastCommitMicros is the commit clock the manager reads, so a step can wait
// for the commit it caused rather than for a duration.
func (r *run) lastCommitMicros() int64 {
	return queryInt(r.t, r.window.db.reader,
		`SELECT coalesce(epoch_us(last_commit), 0)::BIGINT FROM sqlflow_progress`)
}

// windowRows is what the window table still holds.
func (r *run) windowRows() int64 {
	return queryInt(r.t, r.window.db.reader,
		fmt.Sprintf(`SELECT coalesce(sum(n), 0)::BIGINT FROM %s`, windowTable))
}
