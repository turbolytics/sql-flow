package core

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/turbolytics/sql-flow/internal/duckdb"
	"github.com/zeebo/assert"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

// fixedDeliverer is a source whose Delivering answer the test sets.
type fixedDeliverer struct {
	*blockingSource
	for_ time.Duration
	ok   bool
}

func (s *fixedDeliverer) Delivering() (time.Duration, bool) { return s.for_, s.ok }

// The row says since when the source could deliver, on the same clock as the
// other two columns, so a reader subtracts row values and never its own now.
func TestStateDurability_TheRowCarriesWhenTheSourceCouldDeliver(t *testing.T) {
	coverage.Covers(t, "state.durability")
	at := time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)
	rec := &progressRecorder{}
	src := &fixedDeliverer{blockingSource: newBlockingSource(nil), for_: 30 * time.Second, ok: true}
	tb := NewTurbine(src, &fakeHandler{}, &fakeSink{}, 1, time.Hour,
		&sync.Mutex{}, PipelineErrorPolicies{},
		WithProgressStore(rec), WithClock(func() time.Time { return at }))

	assert.NoError(t, tb.recordProgress(context.Background(), progressForced))

	rec.mu.Lock()
	defer rec.mu.Unlock()
	last := rec.recs[len(rec.recs)-1]
	assert.That(t, last.DeliveringFor != nil)
	assert.Equal(t, 30*time.Second, *last.DeliveringFor)
}

// A source that cannot deliver says so with a negative duration, which a
// reader tells apart from the absence a source that was never asked leaves.
func TestStateDurability_ASourceThatCannotDeliverSaysSoWithANegativeDuration(t *testing.T) {
	coverage.Covers(t, "state.durability")
	at := time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)
	rec := &progressRecorder{}
	src := &fixedDeliverer{blockingSource: newBlockingSource(nil), ok: false}
	tb := NewTurbine(src, &fakeHandler{}, &fakeSink{}, 1, time.Hour,
		&sync.Mutex{}, PipelineErrorPolicies{},
		WithProgressStore(rec), WithClock(func() time.Time { return at }))

	assert.NoError(t, tb.recordProgress(context.Background(), progressForced))

	rec.mu.Lock()
	defer rec.mu.Unlock()
	last := rec.recs[len(rec.recs)-1]
	assert.That(t, last.DeliveringFor != nil)
	assert.That(t, *last.DeliveringFor < 0)
}

// A source with no opinion on the matter -- one that does not implement
// Deliverer at all -- leaves the column empty too, and the manager reads that
// as an engine with nothing to say rather than as a source that is down.
func TestStateDurability_ASourceWithoutDelivererLeavesTheColumnEmpty(t *testing.T) {
	coverage.Covers(t, "state.durability")
	at := time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)
	rec := &progressRecorder{}
	tb := NewTurbine(newBlockingSource(nil), &fakeHandler{}, &fakeSink{}, 1, time.Hour,
		&sync.Mutex{}, PipelineErrorPolicies{},
		WithProgressStore(rec), WithClock(func() time.Time { return at }))

	assert.NoError(t, tb.recordProgress(context.Background(), progressForced))

	rec.mu.Lock()
	defer rec.mu.Unlock()
	last := rec.recs[len(rec.recs)-1]
	assert.That(t, last.DeliveringFor == nil)
}

// The transition an operator watches for is logged from the one place that
// still asks the source. A window that holds because of it moves no
// watermark, so the manager logs nothing: without these two lines a rebalance
// is silent in both.
func TestStateDurability_TheSourceOutageIsLogged(t *testing.T) {
	coverage.Covers(t, "state.durability")
	at := time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)
	obs, logs := observer.New(zapcore.DebugLevel)

	src := &fixedDeliverer{blockingSource: newBlockingSource(nil), ok: false}
	tb := NewTurbine(src, &fakeHandler{}, &fakeSink{}, 1, time.Hour,
		&sync.Mutex{}, PipelineErrorPolicies{},
		WithProgressStore(&progressRecorder{}), WithTurbineLogger(zap.New(obs)),
		WithClock(func() time.Time { return at }))

	assert.NoError(t, tb.recordProgress(context.Background(), progressForced))
	assert.Equal(t, 1, logs.FilterMessageSnippet("not delivering").Len())

	// Once only: a line per commit would drown the log of a long outage.
	assert.NoError(t, tb.recordProgress(context.Background(), progressForced))
	assert.Equal(t, 1, logs.FilterMessageSnippet("not delivering").Len())

	src.ok = true
	src.for_ = time.Second
	assert.NoError(t, tb.recordProgress(context.Background(), progressForced))
	back := logs.FilterMessageSnippet("delivering again")
	assert.Equal(t, 1, back.Len())
	assert.That(t, back.All()[0].ContextMap()["not_delivering_for"] != nil)
}

// A state file written by an engine from before the column exists opens
// without it, and the manager's read names it: the pipeline would fail on
// every poll. Init adds it, keeps the row that is already there, and the
// second Init is a no-op, because an engine that upgrades runs Init on every
// start for the rest of the file's life.
func TestStateDurability_AnOlderStateFileGainsTheColumn(t *testing.T) {
	coverage.Covers(t, "state.durability")
	ctx := context.Background()
	db, err := duckdb.OpenPath(ctx, "")
	assert.NoError(t, err)
	defer db.Close()
	conn, err := db.Connect(ctx)
	assert.NoError(t, err)
	defer conn.Close()

	// The table as the previous release created it, with a row in it.
	old := NewProgressStore(conn)
	for _, q := range []string{
		`CREATE TABLE sqlflow_progress (
		    last_arrival TIMESTAMPTZ,
		    last_commit  TIMESTAMPTZ,
		    messages     BIGINT NOT NULL
		)`,
		`INSERT INTO sqlflow_progress VALUES (TIMESTAMPTZ '2026-09-23 11:00:00+00:00',
		                                      TIMESTAMPTZ '2026-09-23 11:30:00+00:00', 7)`,
	} {
		assert.NoError(t, old.exec(ctx, q))
	}

	assert.NoError(t, old.Init(ctx))
	assert.NoError(t, old.Init(ctx))

	// The column is there and empty, which reads as a source that never said,
	// and the messages the old engine counted are still counted.
	stmt, err := conn.NewStatement()
	assert.NoError(t, err)
	defer stmt.Close()
	assert.NoError(t, stmt.SetSqlQuery(
		`SELECT count(*), max(messages), coalesce(max(delivering_for_us), -1) FROM sqlflow_progress`))
	reader, _, err := stmt.ExecuteQuery(ctx)
	assert.NoError(t, err)
	defer reader.Release()
	assert.That(t, reader.Next())
	rec := reader.Record()
	assert.Equal(t, int64(1), rec.Column(0).(*array.Int64).Value(0))
	assert.Equal(t, int64(7), rec.Column(1).(*array.Int64).Value(0))
	assert.Equal(t, int64(-1), rec.Column(2).(*array.Int64).Value(0))

	// And the first write after the upgrade fills it.
	for_ := 30 * time.Second
	assert.NoError(t, old.Record(ctx, Progress{
		LastCommit: time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC),
		Messages:   8, DeliveringFor: &for_,
	}))
	assert.NoError(t, stmt.SetSqlQuery(`SELECT delivering_for_us FROM sqlflow_progress`))
	after, _, err := stmt.ExecuteQuery(ctx)
	assert.NoError(t, err)
	defer after.Release()
	assert.That(t, after.Next())
	assert.Equal(t, int64(30_000_000), after.Record().Column(0).(*array.Int64).Value(0))
}
