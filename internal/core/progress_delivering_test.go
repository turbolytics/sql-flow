package core

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
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
