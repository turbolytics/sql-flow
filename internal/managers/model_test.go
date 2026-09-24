package managers

import (
	"testing"
	"time"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

// modelAlphabet is every event that has produced a defect in this package and
// that the engine is meant to survive: a batch, an idle tick, a restart, a
// source going and coming back, and the wall clock stepping either way.
//
// Insert is not here, deliberately. It produced #374, which the engine has
// today and which the watermark design removes rather than patches; enumerating
// it would turn every sequence red against a defect we have chosen not to fix
// in this shape. It has its own test below, asserting the defect, and the
// ledger carries it.
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
	for _, rule := range []string{"hold.empty", "hold.open", "hold.not_delivering", "close.grace", "close.idle"} {
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
	// Liveness, before the counting. Drain counted whatever a run ended
	// holding as though it had been published, so the counting property could
	// not fail on a bucket that never closes: publishing nothing at all
	// satisfied it. That is the #183 failure mode, and it is what an early
	// close gets traded for by a careless fix -- the worse of the two, because
	// it has no late rows and no error to notice.
	//
	// So the run is quiesced instead: the source comes back and the stream
	// goes quiet, which is the one condition under which every open bucket
	// must close. Whatever is still open after that was never going to close.
	// Everything the run reached on its own, before the tail adds to it.
	body := len(m.Decisions)
	for _, e := range []Event{{Kind: SourceBack},
		{Kind: IdleTick}, {Kind: IdleTick}, {Kind: IdleTick}, {Kind: IdleTick}} {
		for _, p := range m.Apply(e) {
			if seen[p.Bucket] {
				t.Fatalf("quiescing %v: bucket %s published twice", kinds(seq), p.Bucket)
			}
			seen[p.Bucket] = true
			published += p.Rows
		}
	}
	if stuck := m.Drain(); stuck != 0 {
		t.Fatalf("%v: %d rows still open after the source returned and the stream "+
			"went quiet: nothing will ever close them", kinds(seq), stuck)
	}

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
	for i, d := range m.Decisions {
		// Only the run's own decisions count as reached. The tail ends every
		// sequence with a source that is back and a stream going quiet, so
		// counting it would have the coverage check confirm the tail rather
		// than the alphabet: with the tail counted, a bound longer than a
		// sequence can run still shows close.idle as reached, which is the
		// blindness this check exists to catch.
		if i < body {
			fired[d.Rule]++
		}
		if d.Action != CloseByIdle {
			continue
		}
		// Against the row's own instants, not the row's own claim. Comparing
		// the duration with the bound proves only that StateOf honoured the
		// row; a commit that writes a duration the model's world does not
		// support passes it unseen, and the row is the half the model exists
		// to stand in for.
		truth := d.RowAt.Sub(d.RowSince)
		switch {
		case !d.Delivering:
			t.Fatalf("%v: closed on idleness while the row said the source held nothing",
				kinds(seq))
		case d.DeliveringFor != truth:
			t.Fatalf("%v: the row claims %s of delivering, its own instants say %s",
				kinds(seq), d.DeliveringFor, truth)
		case min(d.Quiet, truth) < m.decl.IdleClose:
			t.Fatalf("%v: closed on %s of silence from a source delivering %s",
				kinds(seq), d.Quiet, truth)
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

// A burst's first rows, visible before the commit that accounts for them,
// close the bucket on the silence they ended and take the rest of the burst
// with them. This is #374, in the model, and it is the interleaving neither
// layer of this framework could express until Insert existed: Arrive did the
// handler's write and the progress write as one step, so there was nowhere to
// put the poll.
//
// It asserts the defect rather than the fix, because the defect is what the
// engine has: #375 fixed it, #377 reverted that to clear main, and the
// watermark design in 2026-09-24-window-watermark-design.md removes the two
// separate facts this depends on. When that lands, this test inverts -- the
// burst survives, and the sequence below becomes indistinguishable from the
// one above it.
func TestManagerWindow_RowsSeenBeforeTheirCommitCloseTheBucketEarly(t *testing.T) {
	coverage.Covers(t, "manager.window")
	m := NewModel(modelDecl())

	// A bucket, then silence long enough to close it: the ordinary path.
	m.Apply(Event{Kind: Arrive, Rows: 2})
	for i := 0; i < 3; i++ {
		m.Apply(Event{Kind: IdleTick})
	}
	// The stream moves on two minutes, so the burst lands in a new bucket
	// rather than the closed one.
	m.Apply(Event{Kind: ClockStep, By: 2 * time.Minute})

	// The burst. Its first rows are visible; the row still confirms the
	// silence they just ended.
	published := 0
	for _, p := range m.Apply(Event{Kind: Insert, Rows: 5}) {
		published += p.Rows
	}
	assert.Equal(t, 5, published)

	// And the rest of the same burst is behind the watermark that close just
	// moved, so it is late.
	before := m.Dropped
	m.Apply(Event{Kind: Insert, Rows: 10})
	assert.Equal(t, before+10, m.Dropped)

	// Ten of the fifteen rows the burst produced, gone, with the bucket
	// published from the five that happened to be committed first.
	assert.Equal(t, 17, m.Produced)
	assert.Equal(t, 10, m.Dropped)
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
