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

// live is a clock that says the stream is not idle: the last arrival was a
// moment ago.
func live(d *testDB, tb testing.TB) func() time.Time {
	arrivedAt(tb, d.pipeline, t0)
	return func() time.Time { return t0.Add(time.Second) }
}

// Bucket 0 closes once the stream is one grace past its end: with one-minute
// buckets and a one-minute grace, when bucket 2 has a row. Bucket 1 has not
// been passed by the grace yet and stays.
func TestManagerWindow_ClosesAgainstTheStreamClock(t *testing.T) {
	coverage.Covers(t, "manager.window")
	ctx := context.Background()
	d := newTestDB(t, "")
	createWindowTable(t, d.pipeline)
	now := live(d, t)
	sink := &recordingSink{}
	w := newTestWatermark(t, d, testDecl(), sink, now)

	insertBucket(t, d.pipeline, 0, "NYC", 3)
	insertBucket(t, d.pipeline, 1, "NYC", 1)
	assert.NoError(t, w.Poll(ctx))
	rows, flushes := sink.counts()
	assert.Equal(t, int64(0), rows)
	assert.Equal(t, 0, flushes)

	// The stream reaches bucket 2: bucket 0's end plus the grace.
	insertBucket(t, d.pipeline, 2, "NYC", 7)
	assert.NoError(t, w.Poll(ctx))
	rows, flushes = sink.counts()
	assert.Equal(t, int64(1), rows)
	assert.Equal(t, 1, flushes)
	assert.Equal(t, int64(2), countRows(t, d.pipeline, testTable))

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

// After the idle bound with no arrival, every bucket closes, the newest
// included.
func TestManagerWindow_IdleCloseClosesEverything(t *testing.T) {
	coverage.Covers(t, "manager.window")
	ctx := context.Background()
	d := newTestDB(t, "")
	createWindowTable(t, d.pipeline)
	arrivedAt(t, d.pipeline, t0)
	clock := t0.Add(time.Second)
	sink := &recordingSink{}
	w := newTestWatermark(t, d, testDecl(), sink, func() time.Time { return clock })

	insertBucket(t, d.pipeline, 0, "NYC", 3)
	insertBucket(t, d.pipeline, 1, "SF", 1)
	assert.NoError(t, w.Poll(ctx))
	rows, _ := sink.counts()
	assert.Equal(t, int64(0), rows)

	// One second short of the idle bound: still open.
	clock = t0.Add(5*time.Minute - time.Second)
	assert.NoError(t, w.Poll(ctx))
	rows, _ = sink.counts()
	assert.Equal(t, int64(0), rows)

	clock = t0.Add(5 * time.Minute)
	assert.NoError(t, w.Poll(ctx))
	rows, flushes := sink.counts()
	assert.Equal(t, int64(2), rows)
	assert.Equal(t, 1, flushes)
	assert.Equal(t, int64(0), countRows(t, d.pipeline, testTable))

	wm, _, err := NewStore(d.pipeline).Load(ctx, testTable)
	assert.NoError(t, err)
	assert.That(t, wm.Equal(bucket(2)))
}

// The watermark never moves backwards. Once bucket 0 has closed, deleting the
// newer rows leaves a table whose newest bucket is older than the watermark,
// and a poll changes nothing.
func TestManagerWindow_WatermarkNeverRegresses(t *testing.T) {
	coverage.Covers(t, "manager.window")
	ctx := context.Background()
	d := newTestDB(t, "")
	createWindowTable(t, d.pipeline)
	now := live(d, t)
	sink := &recordingSink{}
	w := newTestWatermark(t, d, testDecl(), sink, now)

	insertBucket(t, d.pipeline, 0, "NYC", 3)
	insertBucket(t, d.pipeline, 5, "NYC", 1)
	assert.NoError(t, w.Poll(ctx))
	wm, _, err := NewStore(d.pipeline).Load(ctx, testTable)
	assert.NoError(t, err)
	assert.That(t, wm.Equal(bucket(4)))

	exec(t, d.pipeline, `DELETE FROM agg_cities_count`)
	insertBucket(t, d.pipeline, 1, "late", 1)
	assert.NoError(t, w.Poll(ctx))
	wm2, _, err := NewStore(d.pipeline).Load(ctx, testTable)
	assert.NoError(t, err)
	assert.That(t, wm2.Equal(bucket(4)))
}

// A manager built over the state another one saved starts from its
// watermark, so a restart does not publish a bucket twice.
func TestManagerWindow_ARestartResumesFromTheWatermark(t *testing.T) {
	coverage.Covers(t, "manager.window")
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state.db")
	d := newTestDB(t, path)
	createWindowTable(t, d.pipeline)
	now := live(d, t)

	sink := &recordingSink{}
	w := newTestWatermark(t, d, testDecl(), sink, now)
	insertBucket(t, d.pipeline, 0, "NYC", 3)
	insertBucket(t, d.pipeline, 2, "NYC", 1)
	assert.NoError(t, w.Poll(ctx))
	rows, _ := sink.counts()
	assert.Equal(t, int64(1), rows)

	// A late row for bucket 0 arrives, and a fresh manager under drop reads
	// only the persisted watermark.
	insertBucket(t, d.pipeline, 0, "late", 1)
	sink2 := &recordingSink{}
	decl := testDecl()
	decl.Late = LateDrop
	w2, err := NewWatermark(managerConn(t, d.db), decl, time.Hour, sink2, WithClock(now))
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
			now := live(d, t)
			reader := sdkmetric.NewManualReader()
			mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
			sink := &recordingSink{}
			decl := testDecl()
			decl.Late = policy
			w := newTestWatermark(t, d, decl, sink, now, WithMeterProvider(mp))

			insertBucket(t, d.pipeline, 0, "NYC", 3)
			insertBucket(t, d.pipeline, 2, "NYC", 1)
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

// reemit runs emit_sql over the late rows alone. The bucket's earlier rows
// were deleted when it closed, so a sum over the bucket after a late row is
// the late rows' sum, not the bucket's total. A sink that replaces the
// bucket's value with it loses everything published before.
func TestManagerWindow_ReemitPublishesTheLateRowsAlone(t *testing.T) {
	coverage.Covers(t, "manager.window")
	ctx := context.Background()
	d := newTestDB(t, "")
	createWindowTable(t, d.pipeline)
	now := live(d, t)
	sink := &recordingSink{}
	decl := testDecl()
	decl.Late = LateReemit
	decl.EmitSQL = "SELECT sum(count)::INT AS total, bucket, city FROM closed GROUP BY ALL"
	w := newTestWatermark(t, d, decl, sink, now)

	insertBucket(t, d.pipeline, 0, "NYC", 5)
	insertBucket(t, d.pipeline, 2, "NYC", 1)
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
	now := live(d, t)
	sink := &recordingSink{}
	w := newTestWatermark(t, d, testDecl(), sink, now)

	insertBucket(t, d.pipeline, 0, "NYC", 3)
	insertBucket(t, d.pipeline, 2, "NYC", 1)

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
	now := live(d, t)
	failing := &failingSink{err: errors.New("sink down")}
	w := newTestWatermark(t, d, testDecl(), failing, now)

	insertBucket(t, d.pipeline, 0, "NYC", 3)
	insertBucket(t, d.pipeline, 2, "NYC", 1)
	assert.Error(t, w.Poll(ctx))
	assert.Equal(t, int64(2), countRows(t, d.pipeline, testTable))
	_, ok, err := NewStore(d.pipeline).Load(ctx, testTable)
	assert.NoError(t, err)
	assert.That(t, !ok)

	sink := &recordingSink{}
	w2 := newTestWatermark(t, d, testDecl(), sink, now)
	assert.NoError(t, w2.Poll(ctx))
	rows, _ := sink.counts()
	assert.Equal(t, int64(1), rows)
}

// A poll that finds nothing ends its transaction, so the next poll sees rows
// committed in between.
func TestManagerWindow_AnEmptyPollDoesNotFreezeTheView(t *testing.T) {
	coverage.Covers(t, "manager.window")
	ctx := context.Background()
	d := newTestDB(t, "")
	createWindowTable(t, d.pipeline)
	now := live(d, t)
	sink := &recordingSink{}
	w := newTestWatermark(t, d, testDecl(), sink, now)

	assert.NoError(t, w.Poll(ctx))
	insertBucket(t, d.pipeline, 0, "NYC", 3)
	insertBucket(t, d.pipeline, 2, "NYC", 1)
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
	now := live(d, t)
	sink := &recordingSink{}
	decl := testDecl()
	decl.EmitSQL = `WITH totals AS (SELECT city, sum(count)::INT AS count FROM closed GROUP BY ALL)
		SELECT city, count FROM totals ORDER BY city`
	w := newTestWatermark(t, d, decl, sink, now)

	insertBucket(t, d.pipeline, 0, "NYC", 3)
	insertBucket(t, d.pipeline, 0, "NYC", 4)
	insertBucket(t, d.pipeline, 0, "SF", 1)
	insertBucket(t, d.pipeline, 2, "NYC", 1)
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
	now := live(d, t)
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	w := newTestWatermark(t, d, testDecl(), &recordingSink{}, now, WithMeterProvider(mp))

	insertBucket(t, d.pipeline, 0, "NYC", 3)
	insertBucket(t, d.pipeline, 2, "NYC", 1)
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
	now := live(d, t)
	budget := core.NewDrainBudget(200 * time.Millisecond)
	defer budget.Stop()
	w := newTestWatermark(t, d, testDecl(), hangingSink{}, now, WithDrainBudget(budget))
	insertBucket(t, d.pipeline, 0, "NYC", 3)
	insertBucket(t, d.pipeline, 2, "NYC", 1)

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
