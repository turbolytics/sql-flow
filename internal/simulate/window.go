package simulate

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
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
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// The windowed runner pairs the real consume loop with a real window manager
// over one DuckDB, which is the interaction neither the model nor the manager
// tests cover: the model has no engine under it, and the manager tests write
// the watermark by hand rather than having a loop write it.
//
// The handler does what a windowed pipeline's handler does: it inserts a row
// per batch into the window table, bucketed by event time, and the pipeline
// sink receives nothing. The manager runs on its own connection, as it does
// in production, and the engine decides each record's lateness before the
// handler sees it. A script drives the manager's passes by hand, so a
// scenario is a sequence; RunWindowedDriven runs the manager's own loop
// instead, so the kick that follows a commit can be seen to work.

const windowTable = "sim_window"

// Pass runs the manager once, as the engine's kick after a commit would.
type Pass struct{}

// StartPass runs the pass Start runs first: a Pass that also republishes,
// whole, every bucket still retained under the window's lateness. It is
// what a restarted manager does, so a script that Restarts and wants the
// manager's recovery says so with this step.
type StartPass struct{}

// AwaitPublished waits, in a driven run, until the window's sink holds at
// least Rows rows: the kick has been sent and the manager has acted on it.
type AwaitPublished struct{ Rows int64 }

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
	if err := core.NewWatermarkStore(pipeline).Init(ctx); err != nil {
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
	// Rows buffered for this batch, by the bucket their event time falls in.
	// A batch can straddle buckets, which is what makes a window a window:
	// the handler groups by the data's own clock, not by when it ran.
	buffered map[time.Time]int64
	n        int64
}

func (h *windowHandler) Init(context.Context) error { return nil }

// Write reads the record's event time out of the payload, the way a real
// windowed pipeline's SQL reads it out of the message, and buckets on it
// the way time_bucket does: from DuckDB's origin, which is the bucket the
// engine decided the record's lateness against.
func (h *windowHandler) Write(msg []byte) error {
	at := h.clock()
	if _, after, ok := strings.Cut(string(msg), ","); ok {
		if micros, err := strconv.ParseInt(after, 10, 64); err == nil {
			at = time.UnixMicro(micros).UTC()
		}
	}

	h.mu.Lock()
	defer h.mu.Unlock()
	if h.buffered == nil {
		h.buffered = map[time.Time]int64{}
	}
	h.buffered[core.BucketStart(at.UTC(), h.size)]++
	h.n++
	return nil
}

func (h *windowHandler) Invoke(context.Context) (arrow.Table, error) {
	h.mu.Lock()
	buffered := h.buffered
	h.buffered, h.n = nil, 0
	h.mu.Unlock()

	// One row per bucket the batch touched, in bucket order so a script's
	// inserts are deterministic.
	buckets := make([]time.Time, 0, len(buffered))
	for b := range buffered {
		buckets = append(buckets, b)
	}
	sort.Slice(buckets, func(i, j int) bool { return buckets[i].Before(buckets[j]) })
	for _, b := range buckets {
		execSQL(h.t, h.conn, fmt.Sprintf(
			`INSERT INTO %s VALUES (TIMESTAMPTZ '%s', %d)`,
			windowTable, b.Format("2006-01-02 15:04:05-07:00"), buffered[b]))
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

// windowSink counts the rows every close published, and keeps the last value
// it was handed for each bucket, the way a sink that replaces by key holds
// it: a bucket published twice is worth its second value, not the sum.
type windowSink struct {
	mu      sync.Mutex
	rows    int64
	buckets map[int64]int
	// pending is this flush's rows per bucket; last is what the sink holds
	// for each bucket once the flush lands.
	pending map[int64]int64
	last    map[int64]int64
}

func newWindowSink() *windowSink {
	return &windowSink{buckets: map[int64]int{}, last: map[int64]int64{}}
}

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
	nChunks := t.Column(nCol).Data().Chunks()
	var bucketChunks []arrow.Array
	if bucketCol >= 0 {
		bucketChunks = t.Column(bucketCol).Data().Chunks()
	}
	if s.pending == nil {
		s.pending = map[int64]int64{}
	}
	// Counted once per flush: one close publishes several rows for a
	// bucket, and republication is the same bucket in two closes.
	inThisFlush := map[int64]bool{}
	for c, chunk := range nChunks {
		ns := chunk.(*array.Int64)
		var ts *array.Timestamp
		if bucketChunks != nil {
			ts, _ = bucketChunks[c].(*array.Timestamp)
		}
		for i := 0; i < ns.Len(); i++ {
			s.rows += ns.Value(i)
			if ts != nil {
				b := int64(ts.Value(i))
				inThisFlush[b] = true
				s.pending[b] += ns.Value(i)
			}
		}
	}
	for b := range inThisFlush {
		s.buckets[b]++
	}
	return nil
}

// Flush is where a value lands: what this flush carried for a bucket
// replaces what the sink held for it.
func (s *windowSink) Flush(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for b, n := range s.pending {
		s.last[b] = n
	}
	s.pending = nil
	return nil
}

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

// lastValues is what the sink holds per bucket start.
func (s *windowSink) lastValues() map[time.Time]int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[time.Time]int64, len(s.last))
	for b, n := range s.last {
		out[time.UnixMicro(b).UTC()] = n
	}
	return out
}

// WindowResult is what a windowed run produced and what its window published.
type WindowResult struct {
	Produced int
	// Published sums every row the sink was handed, across every flush; a
	// republished bucket counts each time.
	Published int64
	// StillOpen is what the window table holds at the end.
	StillOpen int64
	// Republished is how many times a bucket reached the sink after its
	// first time.
	Republished int
	// LateRefused is what the engine refused before the handler: records for
	// a bucket that had closed more than the lateness before the watermark.
	// From the engine's own counter.
	LateRefused int64
	// Unplaceable is what the engine could not place at all: a record stamped
	// beyond its own clock, refused before the handler and never late.
	Unplaceable int64
	// Recomputes is how many buckets the manager republished whole because a
	// late row landed in them.
	Recomputes int64
	// LastValue is what the sink holds for each bucket start: the last value
	// it was handed, as a sink that replaces by key holds it.
	LastValue map[time.Time]int64
}

// RunWindowed replays a script against a real consume loop and a real window
// manager over one database, and reports what the window published. The
// script drives the manager with Pass steps.
func RunWindowed(t *testing.T, owned []int32, decl managers.Declaration, script []Step) WindowResult {
	t.Helper()
	return runWindowed(t, owned, decl, script, false)
}

// RunWindowedDriven is RunWindowed with the manager's own loop running:
// the engine's kick after each commit is what makes it pass, and a script
// waits with AwaitPublished rather than passing by hand.
func RunWindowedDriven(t *testing.T, owned []int32, decl managers.Declaration, script []Step) WindowResult {
	t.Helper()
	return runWindowed(t, owned, decl, script, true)
}

func runWindowed(t *testing.T, owned []int32, decl managers.Declaration, script []Step, driven bool) WindowResult {
	t.Helper()
	db := openWindowDB(t)

	// The engine's and the manager's instruments, read at the end: what was
	// refused and what was recomputed come from the counters, not from a
	// residual, so a row lost some other way is not reported as refused.
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	metrics, err := core.NewMetrics(mp)
	if err != nil {
		t.Fatal(err)
	}

	r := &run{
		t:       t,
		coord:   newCoordinator(),
		sink:    newSink(),
		now:     time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC),
		owned:   owned,
		eventAt: map[int64]time.Time{},
	}
	r.window = &windowRun{db: db, decl: decl, sink: newWindowSink(), metrics: metrics, reader: reader}
	r.window.handler = &windowHandler{t: t, conn: db.pipeline, clock: r.clock, size: decl.Size}

	// One signal for the window's lifetime, shared by every tracker the run
	// builds: a Restart rebuilds the tracker and the manager keeps its signal,
	// as a manager does across the engine's own restarts.
	r.window.signal = core.NewWatermarks([]core.WindowSpec{{Name: decl.Table}}, r.clock).Signal(decl.Table)
	wm, err := managers.NewWatermark(db.manager, decl, r.window.sink, r.window.signal, managers.WithMeterProvider(mp))
	if err != nil {
		t.Fatal(err)
	}
	r.window.manager = wm

	r.start()
	var (
		managerDone   chan error
		cancelManager context.CancelFunc
	)
	if driven {
		ctx, cancel := context.WithCancel(context.Background())
		cancelManager = cancel
		managerDone = make(chan error, 1)
		go func() { managerDone <- wm.Start(ctx) }()
	}
	for _, s := range script {
		r.elapse()
		s.apply(r)
	}
	r.stop()
	if driven {
		cancelManager()
		select {
		case err := <-managerDone:
			if err != nil {
				t.Fatalf("the manager's loop failed: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("the manager's loop did not stop")
		}
	}

	rows, republished := r.window.sink.counts()
	stillOpen := queryInt(t, db.reader, fmt.Sprintf(`SELECT coalesce(sum(n), 0)::BIGINT FROM %s`, windowTable))
	return WindowResult{
		Produced:    r.produced,
		Published:   rows,
		StillOpen:   stillOpen,
		Republished: republished,
		LateRefused: counterSum(t, reader, "window_late_rows", attribute.String("outcome", "refused")),
		Unplaceable: counterSum(t, reader, "messages_unplaceable_total"),
		Recomputes:  counterSum(t, reader, "window_recomputes"),
		LastValue:   r.window.sink.lastValues(),
	}
}

// counterSum reads a counter's total across every data point carrying the
// given attributes.
func counterSum(t *testing.T, reader *sdkmetric.ManualReader, name string, with ...attribute.KeyValue) int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	var total int64
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				continue
			}
			for _, dp := range sum.DataPoints {
				matches := true
				for _, kv := range with {
					if v, ok := dp.Attributes.Value(kv.Key); !ok || v.AsString() != kv.Value.AsString() {
						matches = false
					}
				}
				if matches {
					total += dp.Value
				}
			}
		}
	}
	return total
}

// windowRun is the window half of a run: the database, the manager, and what
// the window's sink received.
type windowRun struct {
	db      *windowDB
	decl    managers.Declaration
	handler *windowHandler
	manager *managers.Watermark
	sink    *windowSink
	signal  *core.WindowSignal
	// tracker is the engine's, for the current process: what deliver asks
	// whether a record will be refused.
	tracker *core.Watermarks
	metrics *core.Metrics
	reader  *sdkmetric.ManualReader
}

func (Pass) apply(r *run) {
	if r.window == nil {
		r.t.Fatal("Pass needs a windowed run")
	}
	if err := r.window.manager.Pass(context.Background()); err != nil {
		r.t.Fatalf("the manager's pass failed: %v", err)
	}
}

func (StartPass) apply(r *run) {
	if r.window == nil {
		r.t.Fatal("StartPass needs a windowed run")
	}
	if err := r.window.manager.StartPass(context.Background()); err != nil {
		r.t.Fatalf("the manager's start pass failed: %v", err)
	}
}

func (s AwaitPublished) apply(r *run) {
	if r.window == nil {
		r.t.Fatal("AwaitPublished needs a windowed run")
	}
	r.await(fmt.Sprintf("the window's sink to hold %d rows", s.Rows),
		func() bool { rows, _ := r.window.sink.counts(); return rows >= s.Rows })
}

// newWatermarks builds the engine's tracker for the window, restored from
// what the table and the row hold, the way run does at start. On the first
// start both are empty; after a Restart they are what the previous process
// left. The recompute set is in memory, so a Restart loses it: the start
// pass is what recovers those buckets.
func (w *windowRun) newWatermarks(t *testing.T, clock func() time.Time) *core.Watermarks {
	t.Helper()
	ctx := context.Background()
	w.signal.TakeRecompute()
	wm := core.NewWatermarksWithSignals([]core.WindowSpec{{
		Name: w.decl.Table, Size: w.decl.Size, Grace: w.decl.Grace, IdleClose: w.decl.IdleClose, Lateness: w.decl.Lateness,
	}}, clock, []*core.WindowSignal{w.signal})
	newest, _, err := core.NewestBucketStart(ctx, w.db.reader, w.decl.Table, w.decl.TimeColumn)
	if err != nil {
		t.Fatal(err)
	}
	asserted, _, err := core.LoadWatermark(ctx, w.db.reader, w.decl.Table)
	if err != nil {
		t.Fatal(err)
	}
	wm.Restore(w.decl.Table, newest, asserted)
	w.tracker = wm
	return wm
}

// asserted is the engine's watermark for the window, for a diagram's
// column: zero when nothing has been asserted.
func (r *run) asserted() time.Time {
	at, _, err := core.LoadWatermark(context.Background(), r.window.db.reader, r.window.decl.Table)
	if err != nil {
		r.t.Fatal(err)
	}
	return at
}

// windowRows is what the window table still holds.
func (r *run) windowRows() int64 {
	return queryInt(r.t, r.window.db.reader,
		fmt.Sprintf(`SELECT coalesce(sum(n), 0)::BIGINT FROM %s`, windowTable))
}
