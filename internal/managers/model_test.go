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

// The idle bound is two seconds because an event takes one: a bound longer
// than a sequence can run is a bound no sequence ever reaches, and the first
// cut of this file declared ten. Every mutant of the idle rules survived,
// because the enumeration never once asked them anything.
func modelDecl() Declaration {
	return Declaration{
		Table:      "m",
		TimeColumn: "bucket",
		Size:       time.Minute,
		Grace:      time.Minute,
		IdleClose:  2 * time.Second,
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
	fired := map[string]int{}

	var seq []Event
	var walk func(depth int)
	runs := 0
	walk = func(depth int) {
		if depth > 0 {
			runs++
			checkSequence(t, seq, fired)
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

	// An enumeration that never reaches a rule proves nothing about it, and
	// says so in no way a reader would notice: the suite is green either way.
	// These four are the rules a sequence of these events must be able to
	// reach, and a declaration or an alphabet that stops reaching one of them
	// fails here rather than quietly going blind.
	for _, rule := range []string{"hold.empty", "hold.open", "close.grace", "close.idle"} {
		if fired[rule] == 0 {
			t.Fatalf("no sequence reached %s, so nothing here tests it: %v", rule, fired)
		}
	}
}

// checkSequence replays one sequence, asserts the properties, and records
// which rules the run reached.
func checkSequence(t *testing.T, seq []Event, fired map[string]int) {
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

	// Counting alone cannot fail on a close that came early: the close makes
	// the next arrival late, the drop policy counts it, and the total
	// balances. So the one close that rests on silence is checked against the
	// evidence it rested on. The row must have said the source was
	// delivering, and for at least the silence the close spent, because a
	// source cannot confirm a quiet longer than it has been listening.
	for _, d := range m.Decisions {
		fired[d.Rule]++
		if d.Action != CloseByIdle {
			continue
		}
		if !d.Delivering || d.DeliveringFor < m.decl.IdleClose {
			t.Fatalf("%v: closed on %s of silence from a source delivering=%v for %s",
				kinds(seq), d.Quiet, d.Delivering, d.DeliveringFor)
		}
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
