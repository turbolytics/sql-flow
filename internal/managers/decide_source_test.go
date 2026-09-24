package managers

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

// deliveringAt writes the progress row a source that can deliver produces:
// quiet between arrival and commit, and how long it has been able to deliver,
// which bounds that quiet.
func deliveringAt(tb testing.TB, conn adbc.Connection, arrival, commit time.Time, for_ time.Duration) {
	tb.Helper()
	progressAt(tb, conn, arrival, commit)
	exec(tb, conn, fmt.Sprintf(
		`UPDATE sqlflow_progress SET delivering_for_us = %d`, for_.Microseconds()))
}

// notDeliveringAt writes the row a source holding nothing produces, whatever
// the arrival clock says.
func notDeliveringAt(tb testing.TB, conn adbc.Connection, arrival, commit time.Time) {
	tb.Helper()
	progressAt(tb, conn, arrival, commit)
	exec(tb, conn, `UPDATE sqlflow_progress SET delivering_for_us = -1`)
}

// A source that cannot deliver holds every open bucket, as a row of the table
// rather than as a correction the engine applies before the table runs.
func TestManagerWindow_NotDeliveringIsARowOfTheTable(t *testing.T) {
	coverage.Covers(t, "manager.window")
	decl := testDecl()
	// The reading the engine writes while the source holds nothing: an hour
	// of silence in the row and a negative duration beside it. The silence is
	// reported as the hour it was — the row is not corrected before the table
	// sees it — and the table is what refuses to close on it.
	s := StateOf(decl, t0.Add(decl.Size), true, t0, true, time.Hour, -1, false)

	assert.Equal(t, SourceNotDelivering, s.Source)
	assert.Equal(t, IdleConfirmed, s.Idle)
	assert.Equal(t, DataOpen, s.Data)
	assert.Equal(t, "hold.not_delivering", watermarkRuleFor(s).Name)
	assert.Equal(t, Hold, Decide(s))

	// A ripe bucket carries the stream's own evidence, so the same source
	// holding nothing does not hold it: it closes on the grace, as it did
	// before the row could say anything about the source at all.
	ripe := StateOf(decl, t0.Add(decl.Size+decl.Grace+time.Microsecond), true, t0, true, time.Hour, -1, false)
	assert.Equal(t, DataRipe, ripe.Data)
	assert.Equal(t, "close.grace.not_delivering", watermarkRuleFor(ripe).Name)
	assert.Equal(t, CloseByGrace, Decide(ripe))
}

// Quiet is bounded by the resumption: a source back for thirty seconds has
// confirmed thirty seconds, however old the last arrival is.
func TestManagerWindow_QuietIsBoundedByTheResumption(t *testing.T) {
	coverage.Covers(t, "manager.window")
	decl := testDecl() // IdleClose is five minutes

	back := StateOf(decl, t0, true, t0, true, time.Hour, 30*time.Second, true)
	assert.Equal(t, IdleUnconfirmed, back.Idle)

	long := StateOf(decl, t0, true, t0, true, time.Hour, time.Hour, true)
	assert.Equal(t, IdleConfirmed, long.Idle)

	// A source that never said bounds nothing, which is what a row written
	// before these columns existed reads as.
	never := StateOf(decl, t0, true, t0, true, time.Hour, -1, true)
	assert.Equal(t, IdleConfirmed, never.Idle)
}

// The close outcome the engine's hold used to produce, now produced by the
// row: a bucket the idle bound would otherwise close stays open while the
// source holds nothing, and closes once it is back.
func TestManagerWindow_ARowThatCannotDeliverClosesNothingOnIdleness(t *testing.T) {
	coverage.Covers(t, "manager.window")
	ctx := context.Background()
	decl := testDecl()

	d := newTestDB(t, "")
	createWindowTable(t, d.pipeline)
	sink := &recordingSink{}
	w := newTestWatermark(t, d, decl, sink, func() time.Time { return t0.Add(time.Hour) })
	insertBucket(t, d.pipeline, 0, "NYC", 3)

	// An hour of silence in the row, and a source that held nothing through
	// it: nothing closes.
	notDeliveringAt(t, d.pipeline, t0, t0.Add(time.Hour))
	assert.NoError(t, w.Poll(ctx))
	_, flushes := sink.counts()
	assert.Equal(t, 0, flushes)

	// The same silence, from a source that has been back longer than the
	// bound: the bucket closes.
	deliveringAt(t, d.pipeline, t0, t0.Add(time.Hour), time.Hour)
	assert.NoError(t, w.Poll(ctx))
	_, flushes = sink.counts()
	assert.Equal(t, 1, flushes)
}
