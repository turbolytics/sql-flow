package core

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

// countingHandler counts every Write the loop makes, across batches. The
// fakeHandler's own buffered count is reset by Init after each batch, so it
// cannot say how many records reached the handler over a run; this can.
type countingHandler struct {
	fakeHandler
	writes int
}

func (h *countingHandler) Write(msg []byte) error {
	h.writes++
	return h.fakeHandler.Write(msg)
}

// A record whose event time the engine cannot place is refused before the
// handler, on a pipeline that windows, and kept on one that does not.
//
// The refused record is consumed rather than failed: its position is
// finished with, so the commit moves past it, and the pipeline carries on.
// One such record used to drag a window's watermark 73 years ahead and drop
// every correctly stamped record after it as late (#358).
func TestCoreConsumeLoop_AnUnplaceableEventTimeIsRefusedBeforeTheHandler(t *testing.T) {
	coverage.Covers(t, "core.consume_loop")

	batch := func() []Message {
		msgs := messages(3)
		// The middle record claims to be from 2099: a device with a fast
		// clock, or a malformed field. The other two stamp nothing, which is
		// placeable, because a source with no event time has nothing to
		// window on.
		msgs[1].EventAtNanos = time.Date(2099, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano()
		return msgs
	}

	run := func(places bool) (*countingHandler, Progress) {
		rec := &progressRecorder{}
		src := newBlockingSource(batch())
		h := &countingHandler{}
		// A refused record is not part of the batch, so on the windowing run
		// only two of three fill it and batch_size never trips. The flush
		// interval is what commits a partial batch, as it does on a
		// low-traffic topic; an hour here hangs the test, which is itself
		// the point being tested.
		tb := NewTurbine(src, h, &fakeSink{}, 3, 50*time.Millisecond,
			&sync.Mutex{}, PipelineErrorPolicies{},
			WithProgressStore(rec), WithProgressWriteInterval(0),
			WithEventTimePlacement(places))
		done := make(chan struct{})
		go func() { _, _ = tb.ConsumeLoop(context.Background(), 0); close(done) }()
		waitFor(t, "the batch to commit", 5*time.Second, func() bool {
			return tb.Progress().Messages == 3
		})
		close(src.release)
		<-done
		return h, tb.Progress()
	}

	// A windowing pipeline: the 2099 record never reaches the handler, and
	// is still counted as consumed so the position is safe to commit past.
	h, p := run(true)
	assert.Equal(t, 2, h.writes)
	assert.Equal(t, int64(3), p.Messages)

	// A pipeline with no window keeps it: there is nothing for a wrong
	// timestamp to damage, and refusing would lose a record for no reason.
	h, p = run(false)
	assert.Equal(t, 3, h.writes)
	assert.Equal(t, int64(3), p.Messages)
}

// The rule itself, at its edges. Zero is placeable; the floor and now are
// inclusive; a nanosecond past now is not.
func TestCoreConsumeLoop_PlacementRule(t *testing.T) {
	coverage.Covers(t, "core.consume_loop")
	now := time.Date(2026, 9, 25, 12, 0, 0, 0, time.UTC).UnixNano()
	floor := EventTimeFloor.UnixNano()

	assert.That(t, CanPlace(0, now))
	assert.That(t, CanPlace(floor, now))
	assert.That(t, CanPlace(now, now))
	assert.That(t, !CanPlace(floor-1, now))
	assert.That(t, !CanPlace(now+1, now))
	assert.That(t, !CanPlace(time.Date(1970, 1, 1, 0, 0, 1, 0, time.UTC).UnixNano(), now))
}
