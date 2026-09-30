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
// commit, written by hand; every test here is a pass against it, driven by
// the test the way a kick would drive it.

// Buckets close up to the asserted watermark and no further: with the
// engine at bucket 0's end, bucket 0 publishes and bucket 1 stays.
func TestManagerWindow_ClosesUpToTheAssertedWatermark(t *testing.T) {
	coverage.Covers(t, "manager.window")
	ctx := context.Background()
	d := newTestDB(t, "")
	createWindowTable(t, d.pipeline)
	sink := &recordingSink{}
	w, _ := newTestWatermark(t, d, testDecl(), sink)

	insertBucket(t, d.pipeline, 0, "NYC", 3)
	insertBucket(t, d.pipeline, 1, "NYC", 1)
	assert.NoError(t, w.Pass(ctx))
	rows, flushes := sink.counts()
	assert.Equal(t, int64(0), rows)
	assert.Equal(t, 0, flushes)

	assertAt(t, d.pipeline, bucket(1))
	assert.NoError(t, w.Pass(ctx))
	rows, flushes = sink.counts()
	assert.Equal(t, int64(1), rows)
	assert.Equal(t, 1, flushes)
	assert.Equal(t, int64(1), countRows(t, d.pipeline, testTable))

	wm, ok, err := NewStore(d.pipeline).Load(ctx, testTable)
	assert.NoError(t, err)
	assert.That(t, ok)
	assert.That(t, wm.Equal(bucket(1)))

	// Nothing new: nothing published, nothing deleted.
	assert.NoError(t, w.Pass(ctx))
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
	w, _ := newTestWatermark(t, d, testDecl(), sink)

	insertBucket(t, d.pipeline, 0, "NYC", 3)
	insertBucket(t, d.pipeline, 1, "SF", 1)
	assertAt(t, d.pipeline, bucket(2))
	assert.NoError(t, w.Pass(ctx))
	rows, flushes := sink.counts()
	assert.Equal(t, int64(2), rows)
	assert.Equal(t, 1, flushes)
	assert.Equal(t, int64(0), countRows(t, d.pipeline, testTable))

	wm, _, err := NewStore(d.pipeline).Load(ctx, testTable)
	assert.NoError(t, err)
	assert.That(t, wm.Equal(bucket(2)))
}

// Without an assertion nothing closes, however many passes run and whatever
// the table holds: the manager has no clock to grow impatient on, and the
// newest bucket is an observation, not a fact it decides on.
func TestManagerWindow_AnUnassertedWindowNeverCloses(t *testing.T) {
	coverage.Covers(t, "manager.window")
	ctx := context.Background()
	d := newTestDB(t, "")
	createWindowTable(t, d.pipeline)
	sink := &recordingSink{}
	w, _ := newTestWatermark(t, d, testDecl(), sink)

	insertBucket(t, d.pipeline, 0, "NYC", 3)
	insertBucket(t, d.pipeline, 5, "SF", 1)
	for i := 0; i < 3; i++ {
		assert.NoError(t, w.Pass(ctx))
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
// pass would decide the same move again.
func TestManagerWindow_AnAssertionOverAnEmptyTableIsSaved(t *testing.T) {
	coverage.Covers(t, "manager.window")
	ctx := context.Background()
	d := newTestDB(t, "")
	createWindowTable(t, d.pipeline)
	sink := &recordingSink{}
	w, _ := newTestWatermark(t, d, testDecl(), sink)

	assertAt(t, d.pipeline, bucket(5))
	assert.NoError(t, w.Pass(ctx))
	_, flushes := sink.counts()
	assert.Equal(t, 0, flushes)
	wm, ok, err := NewStore(d.pipeline).Load(ctx, testTable)
	assert.NoError(t, err)
	assert.That(t, ok)
	assert.That(t, wm.Equal(bucket(5)))
}

// The closed watermark never moves backwards. An assertion at or below it
// is one that has been acted on, and a pass changes nothing: it publishes
// nothing and, with nothing moved, deletes nothing either.
func TestManagerWindow_WatermarkNeverRegresses(t *testing.T) {
	coverage.Covers(t, "manager.window")
	ctx := context.Background()
	d := newTestDB(t, "")
	createWindowTable(t, d.pipeline)
	sink := &recordingSink{}
	w, _ := newTestWatermark(t, d, testDecl(), sink)

	insertBucket(t, d.pipeline, 0, "NYC", 3)
	insertBucket(t, d.pipeline, 5, "NYC", 1)
	assertAt(t, d.pipeline, bucket(4))
	assert.NoError(t, w.Pass(ctx))
	wm, _, err := NewStore(d.pipeline).Load(ctx, testTable)
	assert.NoError(t, err)
	assert.That(t, wm.Equal(bucket(4)))

	// The engine cannot assert lower, by construction; were the row to say
	// so anyway, the manager holds. The row for bucket 1 is one the engine
	// would have refused; written by hand, it sits there, unpublished.
	exec(t, d.pipeline, `DELETE FROM agg_cities_count`)
	insertBucket(t, d.pipeline, 1, "late", 1)
	assertAt(t, d.pipeline, bucket(2))
	assert.NoError(t, w.Pass(ctx))
	wm2, _, err := NewStore(d.pipeline).Load(ctx, testTable)
	assert.NoError(t, err)
	assert.That(t, wm2.Equal(bucket(4)))
	rows, _ := sink.counts()
	assert.Equal(t, int64(1), rows)
	assert.Equal(t, int64(1), countRows(t, d.pipeline, testTable))
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
	w, _ := newTestWatermark(t, d, testDecl(), sink)
	insertBucket(t, d.pipeline, 0, "NYC", 3)
	insertBucket(t, d.pipeline, 2, "NYC", 1)
	assertAt(t, d.pipeline, bucket(1))
	assert.NoError(t, w.Pass(ctx))
	rows, _ := sink.counts()
	assert.Equal(t, int64(1), rows)

	// A row for bucket 0 is in the table -- one the engine would have
	// refused, written by hand -- and a fresh manager reads only the
	// persisted watermark: bucket 0 is behind it, and nothing is published.
	insertBucket(t, d.pipeline, 0, "late", 1)
	sink2 := &recordingSink{}
	w2, err := NewWatermark(managerConn(t, d.db), testDecl(), sink2, nil)
	assert.NoError(t, err)
	assert.NoError(t, w2.Pass(ctx))
	rows2, flushes2 := sink2.counts()
	assert.Equal(t, int64(0), rows2)
	assert.Equal(t, 0, flushes2)
	assert.Equal(t, int64(2), countRows(t, d.pipeline, testTable))
}

// With lateness, a closed bucket's rows stay until the watermark passes
// end + lateness, and a recompute republishes the whole bucket: the sink's
// last value for it is the exact count, not the late rows alone.
//
//	step                         table          closed   published (last value for bucket 0)
//	----------------------------------------------------------------------------------------
//	buckets 0 (3 rows), 2 (1)    0:3, 2:1       -        -
//	assert 10:01, pass           0:3, 2:1       10:01    bucket 0 → 3        rows kept: lateness 5m
//	late row for 0, recompute    0:4, 2:1       10:01    bucket 0 → 4        the whole bucket, again
//	assert 10:07, pass           -              10:07    bucket 2 → 1        0 expired: 10:01 + 5m <= 10:07
func TestManagerWindow_ARecomputeRepublishesTheWholeBucket(t *testing.T) {
	coverage.Covers(t, "manager.window")
	ctx := context.Background()
	d := newTestDB(t, "")
	createWindowTable(t, d.pipeline)
	sink := &recordingSink{}
	decl := testDecl()
	decl.Lateness = 5 * time.Minute
	decl.EmitSQL = "SELECT sum(count)::INT AS total, bucket FROM closed GROUP BY ALL"
	w, sig := newTestWatermark(t, d, decl, sink)

	insertBucket(t, d.pipeline, 0, "NYC", 3)
	insertBucket(t, d.pipeline, 2, "NYC", 1)
	assertAt(t, d.pipeline, bucket(1))
	assert.NoError(t, w.Pass(ctx))
	assert.DeepEqual(t, [][]string{{"3"}}, sink.published())
	assert.Equal(t, int64(2), countRows(t, d.pipeline, testTable)) // bucket 0 retained

	insertBucket(t, d.pipeline, 0, "late", 1)
	sig.Recompute(bucket(0))
	assert.NoError(t, w.Pass(ctx))
	assert.DeepEqual(t, [][]string{{"3"}, {"4"}}, sink.published())
	assert.Equal(t, int64(3), countRows(t, d.pipeline, testTable))

	assertAt(t, d.pipeline, bucket(7))
	assert.NoError(t, w.Pass(ctx))
	assert.DeepEqual(t, [][]string{{"3"}, {"4"}, {"1"}}, sink.published())
	// Bucket 0 expired at 10:06 and is gone; bucket 2 ended 10:03, and
	// 10:03 + 5m = 10:08 is past the assertion, so it is retained.
	assert.Equal(t, int64(1), countRows(t, d.pipeline, testTable))
}

// A recompute the pass could not publish is not lost: the buckets go back on
// the signal, and the pass that succeeds republishes them.
func TestManagerWindow_AFailedRecomputeIsRetried(t *testing.T) {
	coverage.Covers(t, "manager.window")
	ctx := context.Background()
	d := newTestDB(t, "")
	createWindowTable(t, d.pipeline)
	decl := testDecl()
	decl.Lateness = 5 * time.Minute
	decl.EmitSQL = "SELECT sum(count)::INT AS total, bucket FROM closed GROUP BY ALL"
	sink := &recordingSink{}
	w, sig := newTestWatermark(t, d, decl, sink)
	insertBucket(t, d.pipeline, 0, "NYC", 3)
	insertBucket(t, d.pipeline, 2, "NYC", 1)
	assertAt(t, d.pipeline, bucket(1))
	assert.NoError(t, w.Pass(ctx))

	insertBucket(t, d.pipeline, 0, "late", 1)
	sig.Recompute(bucket(0))
	down, _ := newTestWatermark(t, d, decl, &failingSink{err: errors.New("sink down")})
	// The failing manager shares the signal only through the test: hand it
	// the same one by taking and re-queueing on the same signal.
	down.signal = sig
	assert.Error(t, down.Pass(ctx))

	assert.NoError(t, w.Pass(ctx))
	assert.DeepEqual(t, [][]string{{"3"}, {"4"}}, sink.published())
}

// The start pass republishes every retained bucket whole. A late row admitted
// just before a restart left its recompute in memory only; its rows are in
// the table, and the manager that starts over them publishes the exact value
// without being told which bucket it was.
func TestManagerWindow_TheStartPassRepublishesRetainedBuckets(t *testing.T) {
	coverage.Covers(t, "manager.window")
	ctx := context.Background()
	d := newTestDB(t, "")
	createWindowTable(t, d.pipeline)
	decl := testDecl()
	decl.Lateness = 5 * time.Minute
	decl.EmitSQL = "SELECT sum(count)::INT AS total, bucket FROM closed GROUP BY ALL ORDER BY bucket"
	first := &recordingSink{}
	w, _ := newTestWatermark(t, d, decl, first)
	insertBucket(t, d.pipeline, 0, "NYC", 3)
	insertBucket(t, d.pipeline, 2, "NYC", 1)
	assertAt(t, d.pipeline, bucket(1))
	assert.NoError(t, w.Pass(ctx))
	assert.DeepEqual(t, [][]string{{"3"}}, first.published())

	// The late row lands, the engine commits it, and the process dies before
	// the manager passes. A new manager over the same database.
	insertBucket(t, d.pipeline, 0, "late", 1)
	second := &recordingSink{}
	restarted, err := NewWatermark(managerConn(t, d.db), decl, second, nil)
	assert.NoError(t, err)
	assert.NoError(t, restarted.StartPass(ctx))
	assert.DeepEqual(t, [][]string{{"4"}}, second.published())

	// Only the first pass republishes; the next one has nothing to do.
	assert.NoError(t, restarted.Pass(ctx))
	assert.DeepEqual(t, [][]string{{"4"}}, second.published())

	// Without lateness there is nothing retained, and the start pass is a
	// pass: a manager over a table whose watermark has not moved publishes
	// nothing.
	plain := &recordingSink{}
	none, err := NewWatermark(managerConn(t, d.db), testDecl(), plain, nil)
	assert.NoError(t, err)
	assert.NoError(t, none.StartPass(ctx))
	_, flushes := plain.counts()
	assert.Equal(t, 0, flushes)
}

// With no lateness the close deletes what it publishes, as it always did.
func TestManagerWindow_WithoutLatenessACloseDeletesWhatItPublishes(t *testing.T) {
	coverage.Covers(t, "manager.window")
	ctx := context.Background()
	d := newTestDB(t, "")
	createWindowTable(t, d.pipeline)
	sink := &recordingSink{}
	w, _ := newTestWatermark(t, d, testDecl(), sink)
	insertBucket(t, d.pipeline, 0, "NYC", 3)
	insertBucket(t, d.pipeline, 2, "NYC", 1)
	assertAt(t, d.pipeline, bucket(1))
	assert.NoError(t, w.Pass(ctx))
	assert.Equal(t, int64(1), countRows(t, d.pipeline, testTable))
}

// Start runs one pass on start, one on every kick, and one on the drain;
// nothing wakes it otherwise. An assertion nobody kicks for sees no pass;
// the kick sees one.
func TestManagerWindow_StartPassesOnStartKickAndDrain(t *testing.T) {
	coverage.Covers(t, "manager.window")
	d := newTestDB(t, "")
	createWindowTable(t, d.pipeline)
	sink := &recordingSink{}
	w, sig := newTestWatermark(t, d, testDecl(), sink)
	insertBucket(t, d.pipeline, 0, "NYC", 3)
	insertBucket(t, d.pipeline, 2, "NYC", 1)
	assertAt(t, d.pipeline, bucket(1))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Start(ctx) }()

	// The start pass publishes bucket 0.
	waitFor(t, "the start pass", 5*time.Second, func() bool { r, _ := sink.counts(); return r == 1 })
	insertBucket(t, d.pipeline, 3, "NYC", 1)
	assertAt(t, d.pipeline, bucket(3))
	time.Sleep(100 * time.Millisecond)
	rows, _ := sink.counts()
	assert.Equal(t, int64(1), rows) // nothing woke it
	sig.Kick()
	waitFor(t, "the kicked pass", 5*time.Second, func() bool { r, _ := sink.counts(); return r == 2 })

	insertBucket(t, d.pipeline, 5, "NYC", 1)
	assertAt(t, d.pipeline, bucket(5))
	cancel()
	assert.NoError(t, <-done)
	rows, _ = sink.counts()
	assert.Equal(t, int64(3), rows) // the drain pass
	assert.Equal(t, int64(1), countRows(t, d.pipeline, testTable))
}

// A context already cancelled when Start is called runs the drain pass only:
// the shape a shutdown that raced startup has, and the one the conformance
// harness's drain check builds.
func TestManagerWindow_StartOnACancelledContextDrainsOnly(t *testing.T) {
	coverage.Covers(t, "manager.window")
	d := newTestDB(t, "")
	createWindowTable(t, d.pipeline)
	sink := &recordingSink{}
	w, _ := newTestWatermark(t, d, testDecl(), sink)
	insertBucket(t, d.pipeline, 0, "NYC", 3)
	insertBucket(t, d.pipeline, 2, "NYC", 1)
	assertAt(t, d.pipeline, bucket(1))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	assert.NoError(t, w.Start(ctx))
	rows, flushes := sink.counts()
	assert.Equal(t, int64(1), rows)
	assert.Equal(t, 1, flushes)
}

// The newest bucket is reported on every pass that finds rows, including one
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
	failing, _ := newTestWatermark(t, d, testDecl(), &failingSink{err: errors.New("sink down")}, WithMeterProvider(mp))
	assert.Error(t, failing.Pass(ctx))
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
	ok, _ := newTestWatermark(t, d, testDecl(), &recordingSink{}, WithMeterProvider(mp))
	assert.NoError(t, ok.Pass(ctx))
	assert.Equal(t, int64(0), gaugeValue(t, reader, "window_close_lag_seconds"))

	insertBucket(t, d.pipeline, 3, "NYC", 1)
	insertBucket(t, d.pipeline, 4, "NYC", 1)
	assertAt(t, d.pipeline, bucket(3))
	failing, _ := newTestWatermark(t, d, testDecl(), &failingSink{err: errors.New("sink down")}, WithMeterProvider(mp))
	assert.Error(t, failing.Pass(ctx))
	assert.Equal(t, int64(120), gaugeValue(t, reader, "window_close_lag_seconds"))
}

// A stall that began before a restart is reported by the first pass after
// it, with no close needed: the stored watermark says where the window is,
// and the assertion says where it should be.
func TestManagerWindow_AStallIsReportedByTheFirstPassAfterARestart(t *testing.T) {
	coverage.Covers(t, "manager.window")
	ctx := context.Background()
	d := newTestDB(t, "")
	createWindowTable(t, d.pipeline)

	insertBucket(t, d.pipeline, 0, "NYC", 3)
	insertBucket(t, d.pipeline, 2, "NYC", 1)
	assertAt(t, d.pipeline, bucket(1))
	before, _ := newTestWatermark(t, d, testDecl(), &recordingSink{})
	assert.NoError(t, before.Pass(ctx))
	insertBucket(t, d.pipeline, 4, "NYC", 1)
	assertAt(t, d.pipeline, bucket(3))

	// A new process: fresh metrics, the same database, a sink that is down.
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	after, _ := newTestWatermark(t, d, testDecl(), &failingSink{err: errors.New("sink down")}, WithMeterProvider(mp))
	assert.Error(t, after.Pass(ctx))
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
	failing, _ := newTestWatermark(t, d, testDecl(), &failingSink{err: errors.New("sink down")}, WithMeterProvider(mp))
	assert.Error(t, failing.Pass(ctx))
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
	w, _ := newTestWatermark(t, d, testDecl(), &recordingSink{}, WithMeterProvider(mp))

	insertBucket(t, d.pipeline, 0, "NYC", 3)
	insertBucket(t, d.pipeline, 1, "SF", 1)
	assertAt(t, d.pipeline, bucket(2))
	assert.NoError(t, w.Pass(ctx))
	assert.Equal(t, int64(0), countRows(t, d.pipeline, testTable))
	assert.Equal(t, int64(0), gaugeValue(t, reader, "window_close_lag_seconds"))

	for i := 0; i < 3; i++ {
		assert.NoError(t, w.Pass(ctx))
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

// A close reads committed rows only. Rows an open transaction on another
// connection has written are not in the bucket the sink receives.
func TestManagerWindow_UncommittedRowsAreNotPublished(t *testing.T) {
	coverage.Covers(t, "manager.window")
	ctx := context.Background()
	d := newTestDB(t, "")
	createWindowTable(t, d.pipeline)
	sink := &recordingSink{}
	w, _ := newTestWatermark(t, d, testDecl(), sink)

	insertBucket(t, d.pipeline, 0, "NYC", 3)
	insertBucket(t, d.pipeline, 2, "NYC", 1)
	assertAt(t, d.pipeline, bucket(1))

	open := managerConn(t, d.db)
	defer open.Close()
	insertBucket(t, open, 0, "uncommitted", 9)

	assert.NoError(t, w.Pass(ctx))
	rows, _ := sink.counts()
	assert.Equal(t, int64(1), rows)
	assert.NoError(t, open.(transaction).Rollback(ctx))
}

// A failed flush leaves the rows and the watermark where they were, and the
// next pass publishes the same bucket rather than treating it as late.
func TestManagerWindow_AFailedFlushLeavesEverything(t *testing.T) {
	coverage.Covers(t, "manager.window")
	ctx := context.Background()
	d := newTestDB(t, "")
	createWindowTable(t, d.pipeline)
	failing := &failingSink{err: errors.New("sink down")}
	w, _ := newTestWatermark(t, d, testDecl(), failing)

	insertBucket(t, d.pipeline, 0, "NYC", 3)
	insertBucket(t, d.pipeline, 2, "NYC", 1)
	assertAt(t, d.pipeline, bucket(1))
	assert.Error(t, w.Pass(ctx))
	assert.Equal(t, int64(2), countRows(t, d.pipeline, testTable))
	_, ok, err := NewStore(d.pipeline).Load(ctx, testTable)
	assert.NoError(t, err)
	assert.That(t, !ok)

	sink := &recordingSink{}
	w2, _ := newTestWatermark(t, d, testDecl(), sink)
	assert.NoError(t, w2.Pass(ctx))
	rows, _ := sink.counts()
	assert.Equal(t, int64(1), rows)
}

// A pass that finds nothing ends its transaction, so the next pass sees rows
// and assertions committed in between.
func TestManagerWindow_AnEmptyPassDoesNotFreezeTheView(t *testing.T) {
	coverage.Covers(t, "manager.window")
	ctx := context.Background()
	d := newTestDB(t, "")
	createWindowTable(t, d.pipeline)
	sink := &recordingSink{}
	w, _ := newTestWatermark(t, d, testDecl(), sink)

	assert.NoError(t, w.Pass(ctx))
	insertBucket(t, d.pipeline, 0, "NYC", 3)
	insertBucket(t, d.pipeline, 2, "NYC", 1)
	assertAt(t, d.pipeline, bucket(1))
	assert.NoError(t, w.Pass(ctx))
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
	w, _ := newTestWatermark(t, d, decl, sink)

	insertBucket(t, d.pipeline, 0, "NYC", 3)
	insertBucket(t, d.pipeline, 0, "NYC", 4)
	insertBucket(t, d.pipeline, 0, "SF", 1)
	insertBucket(t, d.pipeline, 2, "NYC", 1)
	assertAt(t, d.pipeline, bucket(1))
	assert.NoError(t, w.Pass(ctx))
	rows, _ := sink.counts()
	assert.Equal(t, int64(2), rows)
	assert.DeepEqual(t, [][]string{{"NYC", "SF"}}, sink.published())
	assert.Equal(t, int64(1), countRows(t, d.pipeline, testTable))
}

// The metrics: the watermark as Unix time, closes, and recomputes.
func TestManagerWindow_MetricsReportTheWatermark(t *testing.T) {
	coverage.Covers(t, "manager.window")
	ctx := context.Background()
	d := newTestDB(t, "")
	createWindowTable(t, d.pipeline)
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	decl := testDecl()
	decl.Lateness = 5 * time.Minute
	w, sig := newTestWatermark(t, d, decl, &recordingSink{}, WithMeterProvider(mp))

	insertBucket(t, d.pipeline, 0, "NYC", 3)
	insertBucket(t, d.pipeline, 2, "NYC", 1)
	assertAt(t, d.pipeline, bucket(1))
	assert.NoError(t, w.Pass(ctx))

	assert.Equal(t, bucket(1).Unix(), gaugeValue(t, reader, "window_watermark_seconds"))
	assert.Equal(t, int64(1), counterValue(t, reader, "window_closed"))

	sig.Recompute(bucket(0))
	assert.NoError(t, w.Pass(ctx))
	assert.Equal(t, int64(1), counterValue(t, reader, "window_recomputes"))
	assert.Equal(t, int64(1), counterValue(t, reader, "window_closed"))
}

// Start, cancel, final pass: the shape #270 and #273 gave the loop. A sink
// that never answers the final pass is bounded by the drain deadline, the
// bucket stays, and the error carries the drain code.
func TestManagerWindow_FinalPassStopsAtTheDrainDeadline(t *testing.T) {
	coverage.Covers(t, "manager.window")
	d := newTestDB(t, "")
	createWindowTable(t, d.pipeline)
	budget := core.NewDrainBudget(200 * time.Millisecond)
	defer budget.Stop()
	w, _ := newTestWatermark(t, d, testDecl(), hangingSink{}, WithDrainBudget(budget))
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
		t.Fatal("the final pass outlived the drain deadline")
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

// The engine expires offset records on the manager's closed watermark, and
// learns it from the signal after each committed pass.
func TestManagerWindow_PassPublishesClosedOnTheSignal(t *testing.T) {
	coverage.Covers(t, "manager.window")
	ctx := context.Background()
	d := newTestDB(t, "")
	createWindowTable(t, d.pipeline)
	w, sig := newTestWatermark(t, d, testDecl(), &recordingSink{})
	insertBucket(t, d.pipeline, 0, "NYC", 3)
	_, ok := sig.Closed()
	assert.That(t, !ok)

	assertAt(t, d.pipeline, bucket(1))
	assert.NoError(t, w.Pass(ctx))
	got, ok := sig.Closed()
	assert.That(t, ok)
	assert.That(t, got.Equal(bucket(1)))
}
