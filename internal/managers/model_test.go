package managers

import (
	"testing"
	"time"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

// modelAlphabet is every event that has produced a defect in this package:
// a batch, an idle tick, a restart, a source going and coming back, and the
// wall clock stepping either way.
func modelAlphabet() []Event {
	return []Event{
		{Kind: Arrive, Rows: 2},
		{Kind: IdleTick},
		{Kind: Restart},
		{Kind: SourceLost},
		{Kind: SourceBack},
		{Kind: ClockStep, By: 2 * time.Minute},
		{Kind: ClockStep, By: -2 * time.Minute},
	}
}

func modelDecl() Declaration {
	return Declaration{
		Table:      "m",
		TimeColumn: "bucket",
		Size:       time.Minute,
		Grace:      time.Minute,
		IdleClose:  10 * time.Second,
		Late:       LateDrop,
	}
}

// Every sequence of events up to four long publishes each row exactly once,
// counting the rows the drop policy discarded and the rows a run ends still
// holding. checkTables proves every state selects one rule; this proves a run
// of events ends with the right total.
func TestManagerWindow_EverySequenceCountsEveryRowOnce(t *testing.T) {
	coverage.Covers(t, "manager.window")
	alphabet := modelAlphabet()

	var seq []Event
	var walk func(depth int)
	runs := 0
	walk = func(depth int) {
		if depth > 0 {
			runs++
			checkSequence(t, seq)
		}
		if depth == 4 {
			return
		}
		for _, e := range alphabet {
			seq = append(seq, e)
			walk(depth + 1)
			seq = seq[:len(seq)-1]
		}
	}
	walk(0)
	// 7 + 49 + 343 + 2401
	assert.Equal(t, 2800, runs)
}

// checkSequence replays one sequence and asserts the property.
func checkSequence(t *testing.T, seq []Event) {
	t.Helper()
	m := NewModel(modelDecl())
	published := 0
	seen := map[time.Time]bool{}

	for i, e := range seq {
		for _, p := range m.Apply(e) {
			// A bucket is published once: a second publication of the same
			// bucket is the split-bucket defect, counted here so a sequence
			// that produces one fails rather than balancing out.
			if seen[p.Bucket] {
				t.Fatalf("step %d of %v: bucket %s published twice", i, kinds(seq), p.Bucket)
			}
			seen[p.Bucket] = true
			published += p.Rows
		}
	}
	published += m.Drain()

	if m.Produced != published+m.Dropped {
		t.Fatalf("%v: produced %d, published %d, dropped %d",
			kinds(seq), m.Produced, published, m.Dropped)
	}
}

func kinds(seq []Event) []EventKind {
	out := make([]EventKind, len(seq))
	for i, e := range seq {
		out[i] = e.Kind
	}
	return out
}

// A source that cannot deliver holds its buckets open however long the
// silence runs, which is the trade the engine made deliberately: before it,
// those buckets closed early and their rows were dropped as late.
func TestManagerWindow_ASourceThatCannotDeliverPublishesNothing(t *testing.T) {
	coverage.Covers(t, "manager.window")
	m := NewModel(modelDecl())
	m.Apply(Event{Kind: Arrive, Rows: 5})
	m.Apply(Event{Kind: SourceLost})

	for i := 0; i < 60; i++ {
		assert.Equal(t, 0, len(m.Apply(Event{Kind: IdleTick})))
	}
	assert.Equal(t, 5, m.Drain())
}

// The same silence from a source that is back does close, once it has been
// back longer than the bound.
func TestManagerWindow_ASourceThatIsBackClosesOnIdleness(t *testing.T) {
	coverage.Covers(t, "manager.window")
	m := NewModel(modelDecl())
	m.Apply(Event{Kind: Arrive, Rows: 5})

	published := 0
	for i := 0; i < 30; i++ {
		for _, p := range m.Apply(Event{Kind: IdleTick}) {
			published += p.Rows
		}
	}
	assert.Equal(t, 5, published)
}
