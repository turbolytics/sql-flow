package managers

import (
	"fmt"
	"testing"
	"time"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

// modelAlphabet is every event that has produced a defect in this package
// and that the engine is meant to survive: rows from each of two partitions,
// rows that move the stream on, silence past the idle bound, a partition
// lost and assigned again, one revoked for good, and a restart.
func modelAlphabet(idleClose time.Duration) []Event {
	elapse := 3 * time.Second
	if idleClose > 0 {
		elapse = idleClose + time.Second
	}
	return []Event{
		{Kind: Produce, Partition: 0, Rows: 2},
		{Kind: Produce, Partition: 0, Rows: 2, Ahead: true},
		{Kind: Produce, Partition: 1, Rows: 2},
		{Kind: Elapse, By: elapse},
		{Kind: Lose, Partition: 0},
		{Kind: Assign, Partition: 0},
		{Kind: Revoke, Partition: 1},
		{Kind: Restart},
	}
}

// shape is one of the four configurations the spec works backwards from.
type shape struct {
	name  string
	grace time.Duration
	idle  time.Duration
}

var shapes = []shape{
	{"A: grace 0, no idle bound", 0, 0},
	{"B: grace, no idle bound", time.Minute, 0},
	{"C: grace 0, idle bound", 0, 2 * time.Second},
	{"D: grace and idle bound, the IoT default", time.Minute, 2 * time.Second},
}

// The idle bound is two seconds because an event takes one: a bound longer
// than a sequence can run is a bound no sequence ever reaches, and the first
// cut of this file declared ten. Every mutant of the idle rules survived,
// because the enumeration never once asked them anything.
func (s shape) decl() Declaration {
	return Declaration{
		Table:      "m",
		TimeColumn: "bucket",
		Size:       time.Minute,
		Grace:      s.grace,
		IdleClose:  s.idle,
		Late:       LateDrop,
	}
}

// Every sequence of events up to four long, under each configuration shape,
// ends with each row published exactly once or dropped as late, and never
// drops a row that was in order for a partition still in the minimum. Once
// the run quiesces, nothing that can close stays open.
func TestManagerWindow_EverySequenceCountsEveryRowOnce(t *testing.T) {
	coverage.Covers(t, "manager.window")
	for _, s := range shapes {
		t.Run(s.name, func(t *testing.T) {
			alphabet := modelAlphabet(s.idle)
			fired := map[string]int{}
			idleCloses := 0

			var seq []Event
			var walk func(depth int)
			runs := 0
			walk = func(depth int) {
				if depth > 0 {
					runs++
					idleCloses += checkSequence(t, s, seq, fired)
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
			// 8 + 64 + 512 + 4096
			assert.Equal(t, 4680, runs)

			// An enumeration that never reaches a rule proves nothing about
			// it, and says so in no way a reader would notice: the suite is
			// green either way. Every rule must be reached by the run's own
			// decisions, not the quiescing tail's.
			for _, rule := range []string{"hold.unasserted", "hold.behind", "follow"} {
				if fired[rule] == 0 {
					t.Fatalf("no sequence reached %s, so nothing here tests it: %v", rule, fired)
				}
			}
			// And the all-idle close is reachable exactly where the config
			// allows it: with a bound, some sequence closes on silence; without
			// one, none ever does, which is shape A and B's documented
			// failure mode and not an accident of the alphabet.
			if (s.idle > 0) != (idleCloses > 0) {
				t.Fatalf("idle closes under %s: %d", s.name, idleCloses)
			}
		})
	}
}

// checkSequence replays one sequence, asserts the properties, records the
// rules the run reached, and reports how many idle closes it made.
func checkSequence(t *testing.T, s shape, seq []Event, fired map[string]int) int {
	t.Helper()
	m := NewModel(s.decl(), 0, 1)
	published := 0
	seen := map[time.Time]bool{}
	take := func(what string, pubs []Publication) {
		for _, p := range pubs {
			// A bucket is published once: a second publication of the same
			// bucket is the split-bucket defect, counted here so a sequence
			// that produces one fails rather than balancing out.
			if seen[p.Bucket] {
				t.Fatalf("%s %v: bucket %s published twice", what, kinds(seq), p.Bucket)
			}
			seen[p.Bucket] = true
			published += p.Rows
		}
	}

	for i, e := range seq {
		take(fmt.Sprintf("step %d of", i), m.Apply(e))
		// The minimum's promise: a partition that holds and was not idle at
		// the last poll can still deliver an in-order row for any bucket at
		// or after its own newest less the grace, so no such bucket may have
		// closed underneath it.
		for _, p := range []int32{0, 1} {
			if m.InOrderRowWouldBeLate(p) {
				t.Fatalf("step %d of %v: partition %d holds and is not idle, and its next in-order row would be late",
					i, kinds(seq), p)
			}
		}
	}
	body := len(m.Decisions)
	idleCloses := m.IdleCloses

	// Liveness, before the counting. Publishing nothing at all satisfies a
	// counting property, which is the #183 failure mode and what an early
	// close gets traded for by a careless fix. So the run is quiesced: every
	// partition this worker still has comes back and the stream goes quiet.
	// With an idle bound that closes everything. Without one, a stream that
	// stops never publishes its last bucket, by design; so the stream is
	// moved on instead, and what may stay open is only what the watermark
	// has not reached.
	var tail []Event
	for _, p := range []int32{0, 1} {
		if m.parts[p] == lost {
			tail = append(tail, Event{Kind: Assign, Partition: p})
		}
	}
	if s.idle > 0 {
		tail = append(tail, Event{Kind: Elapse, By: s.idle + time.Second}, Event{Kind: Elapse, By: s.idle + time.Second})
	} else {
		tail = append(tail,
			Event{Kind: Produce, Partition: 0, Rows: 1, Ahead: true},
			Event{Kind: Produce, Partition: 1, Rows: 1, Ahead: true},
			Event{Kind: Elapse, By: 2 * (s.grace + 2*time.Minute)},
			Event{Kind: Produce, Partition: 0, Rows: 1, Ahead: true},
			Event{Kind: Produce, Partition: 1, Rows: 1, Ahead: true})
	}
	for _, e := range tail {
		take("quiescing", m.Apply(e))
	}

	open, newestEnd := m.Open()
	asserted, _ := m.Asserted()
	switch {
	case s.idle > 0 && open != 0:
		t.Fatalf("%v: %d rows still open after every partition came back and the stream "+
			"went quiet: nothing will ever close them", kinds(seq), open)
	case s.idle == 0 && open != 0 && !newestEnd.After(asserted):
		t.Fatalf("%v: %d rows still open in a bucket ending %s, under the assertion %s",
			kinds(seq), open, newestEnd, asserted)
	}
	published += m.Drain()

	if m.Produced != published+m.Dropped {
		t.Fatalf("%v: produced %d, published %d, dropped %d",
			kinds(seq), m.Produced, published, m.Dropped)
	}
	for i, d := range m.Decisions {
		if i < body {
			fired[d.Rule]++
		}
	}
	return idleCloses
}

func kinds(seq []Event) []string {
	out := make([]string, len(seq))
	for i, e := range seq {
		out[i] = string(e.Kind)
		if e.Kind == Produce || e.Kind == Lose || e.Kind == Assign || e.Kind == Revoke {
			out[i] += fmt.Sprintf("(p%d)", e.Partition)
		}
		if e.Ahead {
			out[i] += "+"
		}
	}
	return out
}

// A partition racing ahead in event time cannot close the buckets a slower
// one is still filling: the minimum holds for the slow one, and its rows are
// never late. This is the defect the simulator pinned before the watermark
// (TestSimulate_AFastPartitionClosesASlowOnesBuckets), inverted. Under
// shape B, with no idle bound: with one, the slow partition would leave
// the minimum after the bound, which is the documented trade.
func TestManagerWindow_AFastPartitionHoldsForTheSlowOne(t *testing.T) {
	coverage.Covers(t, "manager.window")
	m := NewModel(shapes[1].decl(), 0, 1)
	m.Apply(Event{Kind: Produce, Partition: 0, Rows: 3})
	m.Apply(Event{Kind: Produce, Partition: 1, Rows: 3})
	for i := 0; i < 3; i++ {
		m.Apply(Event{Kind: Produce, Partition: 0, Rows: 4, Ahead: true})
	}
	m.Apply(Event{Kind: Produce, Partition: 1, Rows: 5})
	assert.Equal(t, 0, m.Dropped)
	assert.That(t, !m.InOrderRowWouldBeLate(1))
}

// A lost partition holds the window: however long the silence, nothing
// closes until it is back and has been silent for the bound.
func TestManagerWindow_ALostPartitionHoldsTheWindow(t *testing.T) {
	coverage.Covers(t, "manager.window")
	m := NewModel(shapes[3].decl(), 0)
	m.Apply(Event{Kind: Produce, Partition: 0, Rows: 5})
	m.Apply(Event{Kind: Lose, Partition: 0})
	for i := 0; i < 60; i++ {
		assert.Equal(t, 0, len(m.Apply(Event{Kind: Elapse, By: time.Minute})))
	}
	m.Apply(Event{Kind: Assign, Partition: 0})
	// Back for the one second the step itself takes: under the bound.
	assert.Equal(t, 0, len(m.Apply(Event{Kind: Elapse})))
	pubs := m.Apply(Event{Kind: Elapse, By: 3 * time.Second})
	assert.Equal(t, 1, len(pubs))
	assert.Equal(t, 5, pubs[0].Rows)
}

// A revoked partition leaves the minimum: the remaining partition's
// progress closes what the revoked one contributed. Holding for it would
// freeze the window on this worker for good.
func TestManagerWindow_ARevokedPartitionLeavesTheMinimum(t *testing.T) {
	coverage.Covers(t, "manager.window")
	m := NewModel(shapes[1].decl(), 0, 1)
	m.Apply(Event{Kind: Produce, Partition: 0, Rows: 3})
	m.Apply(Event{Kind: Produce, Partition: 1, Rows: 3})
	m.Apply(Event{Kind: Revoke, Partition: 1})
	published := 0
	for _, p := range m.Apply(Event{Kind: Produce, Partition: 0, Rows: 1, Ahead: true}) {
		published += p.Rows
	}
	assert.Equal(t, 6, published)
}

// A stream that stops closes only under an idle bound. Shapes A and B leave
// the last bucket open, which is documented, and shapes C and D close it.
func TestManagerWindow_AStreamThatStopsClosesOnlyWithAnIdleBound(t *testing.T) {
	coverage.Covers(t, "manager.window")
	for _, s := range shapes {
		m := NewModel(s.decl(), 0)
		m.Apply(Event{Kind: Produce, Partition: 0, Rows: 5})
		published := 0
		for i := 0; i < 10; i++ {
			for _, p := range m.Apply(Event{Kind: Elapse, By: time.Minute}) {
				published += p.Rows
			}
		}
		if s.idle > 0 {
			assert.Equal(t, 5, published)
		} else {
			assert.Equal(t, 0, published)
			assert.Equal(t, 5, m.Drain())
		}
	}
}

// A restart loses nothing the table and the row hold: the buckets and both
// watermarks survive, and the idle close still finds the newest bucket.
func TestManagerWindow_ARestartStillClosesByIdleness(t *testing.T) {
	coverage.Covers(t, "manager.window")
	m := NewModel(shapes[3].decl(), 0)
	m.Apply(Event{Kind: Produce, Partition: 0, Rows: 5})
	m.Apply(Event{Kind: Restart})
	published := 0
	for _, p := range m.Apply(Event{Kind: Elapse, By: 3 * time.Second}) {
		published += p.Rows
	}
	assert.Equal(t, 5, published)
}
