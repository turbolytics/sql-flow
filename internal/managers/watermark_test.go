package managers

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/turbolytics/sql-flow/internal/core"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/turbolytics/sql-flow/internal/errs"
	"github.com/zeebo/assert"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// The manager against the one fact it reads. assertAt is the engine's
// commit, written by hand; every test here is a poll against it.

// Buckets close up to the asserted watermark and no further: with the
// engine at bucket 0's end, bucket 0 publishes and bucket 1 stays.
func TestManagerWindow_ClosesUpToTheAssertedWatermark(t *testing.T) {
	coverage.Covers(t, "manager.window")
	ctx := context.Background()
	d := newTestDB(t, "")
	createWindowTable(t, d.pipeline)
	sink := &recordingSink{}
	w := newTestWatermark(t, d, testDecl(), sink)

	insertBucket(t, d.pipeline, 0, "NYC", 3)
	insertBucket(t, d.pipeline, 1, "NYC", 1)
	assert.NoError(t, w.Poll(ctx))
	rows, flushes := sink.counts()
	assert.Equal(t, int64(0), rows)
	assert.Equal(t, 0, flushes)

	assertAt(t, d.pipeline, bucket(1))
	assert.NoError(t, w.Poll(ctx))
	rows, flushes = sink.counts()
	assert.Equal(t, int64(1), rows)
	assert.Equal(t, 1, flushes)
	assert.Equal(t, int64(1), countRows(t, d.pipeline, testTable))

	wm, ok, err := NewStore(d.pipeline).Load(ctx, testTable)
	assert.NoError(t, err)
	assert.That(t, ok)
	assert.That(t, wm.Equal(bucket(1)))

	// Nothing new: nothing published, nothing deleted.
	assert.NoError(t, w.Poll(ctx))
	rows, flushes = sink.counts()
	assert.Equal(t, int64(1), rows)
	assert.Equal(t, 1, flushes)
}

// An assertion past every bucket closes everything, the newest included.
// That is what the engine asserts when every partition has gone idle.
func TestManagerWindow_AnAssertionPastEveryBucketClosesEverything(t *testing.T) {
	coverage.Covers(t, "manager.window")
	ctx := context.Background()
	d := newTestDB(t, "")
	createWindowTable(t, d.pipeline)
	sink := &recordingSink{}
	w := newTestWatermark(t, d, testDecl(), sink)

	insertBucket(t, d.pipeline, 0, "NYC", 3)
	insertBucket(t, d.pipeline, 1, "SF", 1)
	assertAt(t, d.pipeline, bucket(2))
	assert.NoError(t, w.Poll(ctx))
	rows, flushes := sink.counts()
	assert.Equal(t, int64(2), rows)
	assert.Equal(t, 1, flushes)
	assert.Equal(t, int64(0), countRows(t, d.pipeline, testTable))

	wm, _, err := NewStore(d.pipeline).Load(ctx, testTable)
	assert.NoError(t, err)
	assert.That(t, wm.Equal(bucket(2)))
}

// Without an assertion nothing closes, however many polls run and whatever
// the table holds: the manager has no clock to grow impatient on, and the
// newest bucket is an observation, not a fact it decides on.
func TestManagerWindow_AnUnassertedWindowNeverCloses(t *testing.T) {
	coverage.Covers(t, "manager.window")
	ctx := context.Background()
	d := newTestDB(t, "")
	createWindowTable(t, d.pipeline)
	sink := &recordingSink{}
	w := newTestWatermark(t, d, testDecl(), sink)

	insertBucket(t, d.pipeline, 0, "NYC", 3)
	insertBucket(t, d.pipeline, 5, "SF", 1)
	for i := 0; i < 3; i++ {
		assert.NoError(t, w.Poll(ctx))
		if rows, _ := sink.counts(); rows != 0 {
			t.Fatalf("%d rows published with no watermark asserted", rows)
		}
	}
	assert.Equal(t, int64(2), countRows(t, d.pipeline, testTable))
	_, ok, err := NewStore(d.pipeline).Load(ctx, testTable)
	assert.NoError(t, err)
	assert.That(t, !ok)
}

// A watermark that moved over an empty stretch is still saved, or the next
// poll would decide the same move again.
func TestManagerWindow_AnAssertionOverAnEmptyTableIsSaved(t *testing.T) {
	coverage.Covers(t, "manager.window")
	ctx := context.Background()
	d := newTestDB(t, "")
	createWindowTable(t, d.pipeline)
	sink := &recordingSink{}
	w := newTestWatermark(t, d, testDecl(), sink)

	assertAt(t, d.pipeline, bucket(5))
	assert.NoError(t, w.Poll(ctx))
	_, flushes := sink.counts()
	assert.Equal(t, 0, flushes)
	wm, ok, err := NewStore(d.pipeline).Load(ctx, testTable)
	assert.NoError(t, err)
	assert.That(t, ok)
	assert.That(t, wm.Equal(bucket(5)))
}

// The closed watermark never moves backwards. An assertion at or below it
// is one that has been acted on, and a poll changes nothing; the rows it
// would cover are late.
func TestManagerWindow_WatermarkNeverRegresses(t *testing.T) {
	coverage.Covers(t, "manager.window")
	ctx := context.Background()
	d := newTestDB(t, "")
	createWindowTable(t, d.pipeline)
	sink := &recordingSink{}
	decl := testDecl()
	decl.Late = LateDrop
	w := newTestWatermark(t, d, decl, sink)

	insertBucket(t, d.pipeline, 0, "NYC", 3)
	insertBucket(t, d.pipeline, 5, "NYC", 1)
	assertAt(t, d.pipeline, bucket(4))
	assert.NoError(t, w.Poll(ctx))
	wm, _, err := NewStore(d.pipeline).Load(ctx, testTable)
	assert.NoError(t, err)
	assert.That(t, wm.Equal(bucket(4)))

	// The engine cannot assert lower, by construction; were the row to say
	// so anyway, the manager holds.
	exec(t, d.pipeline, `DELETE FROM agg_cities_count`)
	insertBucket(t, d.pipeline, 1, "late", 1)
	assertAt(t, d.pipeline, bucket(2))
	assert.NoError(t, w.Poll(ctx))
	wm2, _, err := NewStore(d.pipeline).Load(ctx, testTable)
	assert.NoError(t, err)
	assert.That(t, wm2.Equal(bucket(4)))
	assert.Equal(t, int64(0), countRows(t, d.pipeline, testTable))
}

// A manager built over the state another one saved starts from its
// watermark, so a restart does not publish a bucket twice.
func TestManagerWindow_ARestartResumesFromTheWatermark(t *testing.T) {
	coverage.Covers(t, "manager.window")
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state.db")
	d := newTestDB(t, path)
	createWindowTable(t, d.pipeline)

	sink := &recordingSink{}
	w := newTestWatermark(t, d, testDecl(), sink)
	insertBucket(t, d.pipeline, 0, "NYC", 3)
	insertBucket(t, d.pipeline, 2, "NYC", 1)
	assertAt(t, d.pipeline, bucket(1))
	assert.NoError(t, w.Poll(ctx))
	rows, _ := sink.counts()
	assert.Equal(t, int64(1), rows)

	// A late row for bucket 0 arrives, and a fresh manager under drop reads
	// only the persisted watermark.
	insertBucket(t, d.pipeline, 0, "late", 1)
	sink2 := &recordingSink{}
	decl := testDecl()
	decl.Late = LateDrop
	w2, err := NewWatermark(managerConn(t, d.db), decl, time.Hour, sink2)
	assert.NoError(t, err)
	assert.NoError(t, w2.Poll(ctx))
	rows2, flushes2 := sink2.counts()
	assert.Equal(t, int64(0), rows2)
	assert.Equal(t, 0, flushes2)
	assert.Equal(t, int64(1), countRows(t, d.pipeline, testTable))
}

// drop discards late rows without a flush and counts them; reemit publishes
// them.
func TestManagerWindow_LateRowsFollowThePolicy(t *testing.T) {
	coverage.Covers(t, "manager.window")
	for _, policy := range []LatePolicy{LateDrop, LateReemit} {
		t.Run(string(policy), func(t *testing.T) {
			coverage.Covers(t, "manager.window")
			ctx := context.Background()
			d := newTestDB(t, "")
			createWindowTable(t, d.pipeline)
			reader := sdkmetric.NewManualReader()
			mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
			sink := &recordingSink{}
			decl := testDecl()
			decl.Late = policy
			w := newTestWatermark(t, d, decl, sink, WithMeterProvider(mp))

			insertBucket(t, d.pipeline, 0, "NYC", 3)
			insertBucket(t, d.pipeline, 2, "NYC", 1)
			assertAt(t, d.pipeline, bucket(1))
			assert.NoError(t, w.Poll(ctx))

			insertBucket(t, d.pipeline, 0, "late", 1)
			insertBucket(t, d.pipeline, 0, "late", 1)
			assert.NoError(t, w.Poll(ctx))

			rows, flushes := sink.counts()
			if policy == LateDrop {
				assert.Equal(t, int64(1), rows)
				assert.Equal(t, 1, flushes)
			} else {
				assert.Equal(t, int64(3), rows)
				assert.Equal(t, 2, flushes)
			}
			assert.Equal(t, int64(1), countRows(t, d.pipeline, testTable))
			assert.Equal(t, int64(2), counterValue(t, reader, "window_late_rows"))
		})
	}
}

// Late rows are counted once, by the close that commits their fate.
//
// The counter used to be incremented before the close committed. A close
// that then failed rolled the delete back and kept the count, so the next
// poll found the same rows and counted them again: two rows dropped once read
// as four. This counter is the data-loss signal the TurboStats bundle
// reports, so it counts what happened rather than what was attempted.
func TestManagerWindow_LateRowsAreCountedOnlyWhenTheCloseCommits(t *testing.T) {
	coverage.Covers(t, "manager.window")
	ctx := context.Background()
	d := newTestDB(t, "")
	createWindowTable(t, d.pipeline)
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	decl := testDecl()
	decl.Late = LateDrop

	first := newTestWatermark(t, d, decl, &recordingSink{}, WithMeterProvider(mp))
	insertBucket(t, d.pipeline, 0, "NYC", 3)
	insertBucket(t, d.pipeline, 2, "NYC", 1)
	assertAt(t, d.pipeline, bucket(1))
	assert.NoError(t, first.Poll(ctx))

	// Two late rows for the closed bucket, and a newer bucket so the next
	// close has something to publish -- to a sink that is down.
	insertBucket(t, d.pipeline, 0, "late", 1)
	insertBucket(t, d.pipeline, 0, "late", 1)
	insertBucket(t, d.pipeline, 4, "NYC", 1)
	assertAt(t, d.pipeline, bucket(3))
	failing := newTestWatermark(t, d, decl, &failingSink{err: errors.New("sink down")}, WithMeterProvider(mp))
	assert.Error(t, failing.Poll(ctx))
	late, _ := metricValue(t, reader, "window_late_rows")
	assert.Equal(t, int64(0), late)

	retry := newTestWatermark(t, d, decl, &recordingSink{}, WithMeterProvider(mp))
	assert.NoError(t, retry.Poll(ctx))
	assert.Equal(t, int64(2), counterValue(t, reader, "window_late_rows"))
}

// The newest bucket is reported on every poll that finds rows, including one
// whose close fails. Rows stamped in the future are therefore visible the
// moment they arrive, even while the sink the close publishes to is down.
func TestManagerWindow_TheNewestBucketIsReportedEvenWhenTheCloseFails(t *testing.T) {
	coverage.Covers(t, "manager.window")
	ctx := context.Background()
	d := newTestDB(t, "")
	createWindowTable(t, d.pipeline)
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	insertBucket(t, d.pipeline, 0, "NYC", 3)
	insertBucket(t, d.pipeline, 2, "NYC", 1)
	assertAt(t, d.pipeline, bucket(1))
	failing := newTestWatermark(t, d, testDecl(), &failingSink{err: errors.New("sink down")}, WithMeterProvider(mp))
	assert.Error(t, failing.Poll(ctx))
	assert.Equal(t, bucket(2).Unix(), gaugeValue(t, reader, "window_newest_bucket_start_seconds"))
}

// Closes that stall while the engine's assertion moves on show as lag, in
// event time: the asserted watermark less the closed one.
//
// One close commits at bucket 1; the engine then asserts bucket 3 and the
// next close fails. Two minutes of closes are overdue. No clock enters
// it: the manager has none, so a gateway that booted without a real-time
// clock reports the same figure.
func TestManagerWindow_AStalledCloseIsLagInEventTime(t *testing.T) {
	coverage.Covers(t, "manager.window")
	ctx := context.Background()
	d := newTestDB(t, "")
	createWindowTable(t, d.pipeline)
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	insertBucket(t, d.pipeline, 0, "NYC", 3)
	insertBucket(t, d.pipeline, 2, "NYC", 1)
	assertAt(t, d.pipeline, bucket(1))
	ok := newTestWatermark(t, d, testDecl(), &recordingSink{}, WithMeterProvider(mp))
	assert.NoError(t, ok.Poll(ctx))
	assert.Equal(t, int64(0), gaugeValue(t, reader, "window_close_lag_seconds"))

	insertBucket(t, d.pipeline, 3, "NYC", 1)
	insertBucket(t, d.pipeline, 4, "NYC", 1)
	assertAt(t, d.pipeline, bucket(3))
	failing := newTestWatermark(t, d, testDecl(), &failingSink{err: errors.New("sink down")}, WithMeterProvider(mp))
	assert.Error(t, failing.Poll(ctx))
	assert.Equal(t, int64(120), gaugeValue(t, reader, "window_close_lag_seconds"))
}

// A stall that began before a restart is reported by the first poll after
// it, with no close needed: the stored watermark says where the window is,
// and the assertion says where it should be.
func TestManagerWindow_AStallIsReportedByTheFirstPollAfterARestart(t *testing.T) {
	coverage.Covers(t, "manager.window")
	ctx := context.Background()
	d := newTestDB(t, "")
	createWindowTable(t, d.pipeline)

	insertBucket(t, d.pipeline, 0, "NYC", 3)
	insertBucket(t, d.pipeline, 2, "NYC", 1)
	assertAt(t, d.pipeline, bucket(1))
	before := newTestWatermark(t, d, testDecl(), &recordingSink{})
	assert.NoError(t, before.Poll(ctx))
	insertBucket(t, d.pipeline, 4, "NYC", 1)
	assertAt(t, d.pipeline, bucket(3))

	// A new process: fresh metrics, the same database, a sink that is down.
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	after := newTestWatermark(t, d, testDecl(), &failingSink{err: errors.New("sink down")}, WithMeterProvider(mp))
	assert.Error(t, after.Poll(ctx))
	assert.Equal(t, int64(120), gaugeValue(t, reader, "window_close_lag_seconds"))
}

// A window whose sink was down from the start reports lag before its first
// close, measured from when that close was due: the oldest bucket's end.
func TestManagerWindow_ANeverClosedWindowReportsLag(t *testing.T) {
	coverage.Covers(t, "manager.window")
	ctx := context.Background()
	d := newTestDB(t, "")
	createWindowTable(t, d.pipeline)
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	// Bucket 0 was due to close once the watermark reached its end, bucket 1;
	// the engine has asserted bucket 3.
	insertBucket(t, d.pipeline, 0, "NYC", 3)
	insertBucket(t, d.pipeline, 4, "NYC", 1)
	assertAt(t, d.pipeline, bucket(3))
	failing := newTestWatermark(t, d, testDecl(), &failingSink{err: errors.New("sink down")}, WithMeterProvider(mp))
	assert.Error(t, failing.Poll(ctx))
	assert.Equal(t, int64(120), gaugeValue(t, reader, "window_close_lag_seconds"))
}

// A sparse stream is not a stalled window. After the assertion has closed
// everything, nothing is overdue however long the stream stays quiet: the
// lag compares the two watermarks, and neither moves while nothing arrives.
func TestManagerWindow_AQuietStreamAfterAnIdleCloseIsNotLag(t *testing.T) {
	coverage.Covers(t, "manager.window")
	ctx := context.Background()
	d := newTestDB(t, "")
	createWindowTable(t, d.pipeline)
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	w := newTestWatermark(t, d, testDecl(), &recordingSink{}, WithMeterProvider(mp))

	insertBucket(t, d.pipeline, 0, "NYC", 3)
	insertBucket(t, d.pipeline, 1, "SF", 1)
	assertAt(t, d.pipeline, bucket(2))
	assert.NoError(t, w.Poll(ctx))
	assert.Equal(t, int64(0), countRows(t, d.pipeline, testTable))
	assert.Equal(t, int64(0), gaugeValue(t, reader, "window_close_lag_seconds"))

	for i := 0; i < 3; i++ {
		assert.NoError(t, w.Poll(ctx))
		assert.Equal(t, int64(0), gaugeValue(t, reader, "window_close_lag_seconds"))
	}
}

// A window reports that it exists before its first close.
func TestManagerWindow_ExistsInTheMetricsBeforeItsFirstClose(t *testing.T) {
	coverage.Covers(t, "manager.window")
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	NewWindowMetrics(mp, "hourly")
	closed, ok := metricValue(t, reader, "window_closed")
	assert.That(t, ok)
	assert.Equal(t, int64(0), closed)
}

// reemit runs emit_sql over the late rows alone. The bucket's earlier rows
// were deleted when it closed, so a sum over the bucket after a late row is
// the late rows' sum, not the bucket's total. A sink that replaces the
// bucket's value with it loses everything published before.
func TestManagerWindow_ReemitPublishesTheLateRowsAlone(t *testing.T) {
	coverage.Covers(t, "manager.window")
	ctx := context.Background()
	d := newTestDB(t, "")
	createWindowTable(t, d.pipeline)
	sink := &recordingSink{}
	decl := testDecl()
	decl.Late = LateReemit
	decl.EmitSQL = "SELECT sum(count)::INT AS total, bucket, city FROM closed GROUP BY ALL"
	w := newTestWatermark(t, d, decl, sink)

	insertBucket(t, d.pipeline, 0, "NYC", 5)
	insertBucket(t, d.pipeline, 2, "NYC", 1)
	assertAt(t, d.pipeline, bucket(1))
	assert.NoError(t, w.Poll(ctx))

	insertBucket(t, d.pipeline, 0, "NYC", 1)
	assert.NoError(t, w.Poll(ctx))

	assert.DeepEqual(t, sink.published(), [][]string{{"5"}, {"1"}})
}

// A close reads committed rows only. Rows an open transaction on another
// connection has written are not in the bucket the sink receives.
func TestManagerWindow_UncommittedRowsAreNotPublished(t *testing.T) {
	coverage.Covers(t, "manager.window")
	ctx := context.Background()
	d := newTestDB(t, "")
	createWindowTable(t, d.pipeline)
	sink := &recordingSink{}
	w := newTestWatermark(t, d, testDecl(), sink)

	insertBucket(t, d.pipeline, 0, "NYC", 3)
	insertBucket(t, d.pipeline, 2, "NYC", 1)
	assertAt(t, d.pipeline, bucket(1))

	open := managerConn(t, d.db)
	defer open.Close()
	insertBucket(t, open, 0, "uncommitted", 9)

	assert.NoError(t, w.Poll(ctx))
	rows, _ := sink.counts()
	assert.Equal(t, int64(1), rows)
	assert.NoError(t, open.(transaction).Rollback(ctx))
}

// A failed flush leaves the rows and the watermark where they were, and the
// next poll publishes the same bucket rather than treating it as late.
func TestManagerWindow_AFailedFlushLeavesEverything(t *testing.T) {
	coverage.Covers(t, "manager.window")
	ctx := context.Background()
	d := newTestDB(t, "")
	createWindowTable(t, d.pipeline)
	failing := &failingSink{err: errors.New("sink down")}
	w := newTestWatermark(t, d, testDecl(), failing)

	insertBucket(t, d.pipeline, 0, "NYC", 3)
	insertBucket(t, d.pipeline, 2, "NYC", 1)
	assertAt(t, d.pipeline, bucket(1))
	assert.Error(t, w.Poll(ctx))
	assert.Equal(t, int64(2), countRows(t, d.pipeline, testTable))
	_, ok, err := NewStore(d.pipeline).Load(ctx, testTable)
	assert.NoError(t, err)
	assert.That(t, !ok)

	sink := &recordingSink{}
	w2 := newTestWatermark(t, d, testDecl(), sink)
	assert.NoError(t, w2.Poll(ctx))
	rows, _ := sink.counts()
	assert.Equal(t, int64(1), rows)
}

// A poll that finds nothing ends its transaction, so the next poll sees rows
// and assertions committed in between.
func TestManagerWindow_AnEmptyPollDoesNotFreezeTheView(t *testing.T) {
	coverage.Covers(t, "manager.window")
	ctx := context.Background()
	d := newTestDB(t, "")
	createWindowTable(t, d.pipeline)
	sink := &recordingSink{}
	w := newTestWatermark(t, d, testDecl(), sink)

	assert.NoError(t, w.Poll(ctx))
	insertBucket(t, d.pipeline, 0, "NYC", 3)
	insertBucket(t, d.pipeline, 2, "NYC", 1)
	assertAt(t, d.pipeline, bucket(1))
	assert.NoError(t, w.Poll(ctx))
	rows, _ := sink.counts()
	assert.Equal(t, int64(1), rows)
}

// emit_sql shapes the closed rows: here it sums the appended rows of a
// bucket into one, as every shipped example does.
func TestManagerWindow_EmitSQLShapesTheClosedRows(t *testing.T) {
	coverage.Covers(t, "manager.window")
	ctx := context.Background()
	d := newTestDB(t, "")
	createWindowTable(t, d.pipeline)
	sink := &recordingSink{}
	decl := testDecl()
	decl.EmitSQL = `WITH totals AS (SELECT city, sum(count)::INT AS count FROM closed GROUP BY ALL)
		SELECT city, count FROM totals ORDER BY city`
	w := newTestWatermark(t, d, decl, sink)

	insertBucket(t, d.pipeline, 0, "NYC", 3)
	insertBucket(t, d.pipeline, 0, "NYC", 4)
	insertBucket(t, d.pipeline, 0, "SF", 1)
	insertBucket(t, d.pipeline, 2, "NYC", 1)
	assertAt(t, d.pipeline, bucket(1))
	assert.NoError(t, w.Poll(ctx))
	rows, _ := sink.counts()
	assert.Equal(t, int64(2), rows)
	assert.DeepEqual(t, [][]string{{"NYC", "SF"}}, sink.published())
	assert.Equal(t, int64(1), countRows(t, d.pipeline, testTable))
}

// The metrics: the watermark as Unix time, closes, and late rows.
func TestManagerWindow_MetricsReportTheWatermark(t *testing.T) {
	coverage.Covers(t, "manager.window")
	ctx := context.Background()
	d := newTestDB(t, "")
	createWindowTable(t, d.pipeline)
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	w := newTestWatermark(t, d, testDecl(), &recordingSink{}, WithMeterProvider(mp))

	insertBucket(t, d.pipeline, 0, "NYC", 3)
	insertBucket(t, d.pipeline, 2, "NYC", 1)
	assertAt(t, d.pipeline, bucket(1))
	assert.NoError(t, w.Poll(ctx))

	assert.Equal(t, bucket(1).Unix(), gaugeValue(t, reader, "window_watermark_seconds"))
	assert.Equal(t, int64(1), counterValue(t, reader, "window_closed"))
}

// Start, cancel, final poll: the shape #270 and #273 gave the loop. A sink
// that never answers the final poll is bounded by the drain deadline, the
// bucket stays, and the error carries the drain code.
func TestManagerWindow_FinalPollStopsAtTheDrainDeadline(t *testing.T) {
	coverage.Covers(t, "manager.window")
	d := newTestDB(t, "")
	createWindowTable(t, d.pipeline)
	budget := core.NewDrainBudget(200 * time.Millisecond)
	defer budget.Stop()
	w := newTestWatermark(t, d, testDecl(), hangingSink{}, WithDrainBudget(budget))
	insertBucket(t, d.pipeline, 0, "NYC", 3)
	insertBucket(t, d.pipeline, 2, "NYC", 1)
	assertAt(t, d.pipeline, bucket(1))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Start(ctx) }()
	cancel()
	select {
	case err := <-done:
		assert.Equal(t, errs.CodeDrainIncomplete, errs.CodeOf(err))
	case <-time.After(5 * time.Second):
		t.Fatal("the final poll outlived the drain deadline")
	}
	assert.Equal(t, int64(2), countRows(t, d.pipeline, testTable))
}

func metricValue(t *testing.T, reader *sdkmetric.ManualReader, name string) (int64, bool) {
	t.Helper()
	var rm metricdata.ResourceMetrics
	assert.NoError(t, reader.Collect(context.Background(), &rm))
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			switch data := m.Data.(type) {
			case metricdata.Sum[int64]:
				var total int64
				for _, dp := range data.DataPoints {
					total += dp.Value
				}
				return total, true
			case metricdata.Gauge[int64]:
				var last int64
				for _, dp := range data.DataPoints {
					last = dp.Value
				}
				return last, true
			}
		}
	}
	return 0, false
}

func counterValue(t *testing.T, reader *sdkmetric.ManualReader, name string) int64 {
	t.Helper()
	v, ok := metricValue(t, reader, name)
	assert.That(t, ok)
	return v
}

func gaugeValue(t *testing.T, reader *sdkmetric.ManualReader, name string) int64 {
	t.Helper()
	v, ok := metricValue(t, reader, name)
	assert.That(t, ok)
	return v
}
