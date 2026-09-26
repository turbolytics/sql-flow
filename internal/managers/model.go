package managers

import (
	"time"

	"github.com/turbolytics/sql-flow/internal/core"
)

// The model is the window over a sequence of events, with no database under
// it. Its engine half is core.Watermarks itself -- the real computation,
// driven on a clock the model owns, not a copy of it -- and its manager half
// is the table in decide.go. checkTables proves every state selects one
// rule; this proves that a run of events ends with every row published once
// or dropped under the declared policy, which is the property no invariant
// owned while a reproduction of #183 lost 12% of its records.
//
// One clock. Event time is what a producer stamps, and here producers stamp
// the model's clock, so a row's bucket is where the clock stood when it was
// produced -- except a row produced Ahead, which is stamped beyond the grace
// and is what moves the stream on. The engine's clock, the same one, decides
// only which partitions are idle.

// EventKind is one thing that can happen to a pipeline.
type EventKind string

const (
	// Produce is rows from one partition reaching the handler and committing,
	// with the watermark asserted in that commit. A partition that is lost
	// delivers nothing; one that was revoked delivers to another worker.
	Produce EventKind = "produce"
	// Elapse is the clock moving with nothing arriving, then an idle tick
	// committing: a partition silent for the bound leaves the minimum here.
	Elapse EventKind = "elapse"
	// Lose is the partition's session failing. It may come back.
	Lose EventKind = "lose"
	// Assign is the partition being assigned, or assigned again.
	Assign EventKind = "assign"
	// Revoke is another worker holding the partition from now on.
	Revoke EventKind = "revoke"
	// Restart is the process dying and coming back with its table and both
	// watermarks, and nothing in memory.
	Restart EventKind = "restart"
)

// Event is one step of a sequence. A poll follows every step.
type Event struct {
	Kind      EventKind
	Partition int32
	Rows      int
	// Ahead stamps the rows beyond the grace, so the stream moves on.
	Ahead bool
	By    time.Duration
}

// Publication is what a close handed the sink.
type Publication struct {
	Bucket time.Time
	Rows   int
}

// Decision is one poll: the rule the one fact selected, and the fact.
type Decision struct {
	Rule     string
	Action   Action
	Asserted time.Time
	Closed   time.Time
}

// held is what the source knows about a partition.
type held int

const (
	holding held = iota
	lost
	revoked
)

// Model is one pipeline: its partitions, its buckets, the engine's tracker
// over both, and the two watermarks.
type Model struct {
	decl Declaration
	spec core.WindowSpec

	clock  time.Time
	engine *core.Watermarks
	parts  map[int32]held
	// The newest event time each partition delivered, for the property that
	// an in-order row is never late.
	newest map[int32]time.Time
	// Whether each partition was idle at the last poll, for the same
	// property: a partition that went idle may find its bucket closed.
	// Idleness is measured as the engine measures it, from the later of the
	// partition's last row and its assignment.
	idleAtPoll map[int32]bool
	// behind is a partition that left the minimum by going idle and has not
	// yet delivered past what closed while it was out. Its in-order rows may
	// be late until it catches up: that is idle_close's documented trade,
	// the same one Flink makes, and not a close the minimum owed it.
	behind    map[int32]bool
	lastRow   map[int32]time.Time
	heldSince map[int32]time.Time

	buckets map[time.Time]int
	// Produced counts rows the source actually delivered. Dropped counts
	// rows that arrived for a bucket the window had already closed, which
	// late_rows drop discards and the property excludes. Late counts the
	// rows that were dropped while in order for their partition, and not
	// idle: what the minimum exists to prevent.
	Produced, Dropped, EarlyClosed int

	asserted    time.Time
	hasAsserted bool
	closed      time.Time
	hadClosed   bool

	// IdleCloses counts assertions made by an idle tick with nothing
	// arriving: the all-idle close.
	IdleCloses int
	// Decisions is every poll's rule and the fact it read, in order.
	Decisions []Decision
}

var modelEpoch = time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)

// NewModel starts a pipeline holding the given partitions, with an empty
// window and nothing asserted.
func NewModel(decl Declaration, partitions ...int32) *Model {
	m := &Model{
		decl:       decl,
		spec:       core.WindowSpec{Name: decl.Table, Size: decl.Size, Grace: decl.Grace, IdleClose: decl.IdleClose},
		clock:      modelEpoch,
		parts:      map[int32]held{},
		newest:     map[int32]time.Time{},
		idleAtPoll: map[int32]bool{},
		behind:     map[int32]bool{},
		lastRow:    map[int32]time.Time{},
		heldSince:  map[int32]time.Time{},
		buckets:    map[time.Time]int{},
	}
	m.engine = core.NewWatermarks([]core.WindowSpec{m.spec}, m.now)
	for _, p := range partitions {
		m.assign(p)
	}
	return m
}

func (m *Model) assign(p int32) {
	m.parts[p] = holding
	m.heldSince[p] = m.clock
	m.engine.Assigned(map[string][]int32{"t": {p}})
}

func (m *Model) now() time.Time { return m.clock }

// commit is the engine's commit: whatever moved is asserted.
func (m *Model) commit() (moved bool) {
	for _, at := range m.engine.Advance() {
		m.asserted, m.hasAsserted = at, true
		moved = true
	}
	return moved
}

// Apply advances the model by one event and returns what the poll after it
// published.
func (m *Model) Apply(e Event) []Publication {
	// Every event takes a second, so a sequence spans time without the
	// caller saying so.
	m.clock = m.clock.Add(time.Second)

	switch e.Kind {
	case Produce:
		if m.parts[e.Partition] != holding {
			break
		}
		at := m.clock
		if e.Ahead {
			at = at.Add(m.decl.Grace + 2*m.decl.Size)
		}
		m.Produced += e.Rows
		m.buckets[at.Truncate(m.decl.Size)] += e.Rows
		m.engine.Observe("t", e.Partition, at.UnixNano())
		m.lastRow[e.Partition] = m.clock
		if at.After(m.newest[e.Partition]) {
			m.newest[e.Partition] = at
		}
		m.commit()
	case Elapse:
		m.clock = m.clock.Add(e.By)
		if m.commit() {
			m.IdleCloses++
		}
	// A change to what the source holds reaches the row at the next commit,
	// which the loop makes at its next batch or tick; here that commit
	// follows the change. It is not an idle close: nothing went silent.
	case Lose:
		m.parts[e.Partition] = lost
		m.engine.Lost(map[string][]int32{"t": {e.Partition}})
		m.commit()
	case Assign:
		m.assign(e.Partition)
		m.commit()
	case Revoke:
		m.parts[e.Partition] = revoked
		m.engine.Released(map[string][]int32{"t": {e.Partition}})
		m.commit()
	case Restart:
		// Nothing in memory survives: a new tracker, restored from the table
		// and the row, and the group assigns every partition this worker is
		// still a member for. A lost session is over; a revoked partition is
		// another worker's.
		m.engine = core.NewWatermarks([]core.WindowSpec{m.spec}, m.now)
		var newest time.Time
		for b := range m.buckets {
			if b.After(newest) {
				newest = b
			}
		}
		m.engine.Restore(m.spec.Name, newest, m.asserted)
		m.lastRow = map[int32]time.Time{}
		for p, h := range m.parts {
			if h != revoked {
				m.assign(p)
			}
		}
		m.commit()
	}

	return m.poll()
}

// poll is one manager pass: reduce to the fact, decide, act.
func (m *Model) poll() []Publication {
	// Late rows first, against the closed watermark, as the manager does.
	// Under drop they are deleted and counted; an in-order row from a
	// partition that was in the minimum should never be among them.
	if m.hadClosed {
		for b, rows := range m.buckets {
			if !b.Add(m.decl.Size).After(m.closed) {
				m.Dropped += rows
				delete(m.buckets, b)
			}
		}
	}

	state := StateOf(m.asserted, m.hasAsserted, m.closed, m.hadClosed)
	rule := watermarkRuleFor(state)
	m.Decisions = append(m.Decisions, Decision{
		Rule: rule.Name, Action: rule.Action, Asserted: m.asserted, Closed: m.closed,
	})
	next, moved := rule.Action.Next(m.asserted, m.closed)
	m.noteIdle()
	if !moved {
		return nil
	}
	m.closed, m.hadClosed = next, true
	return m.collect()
}

// noteIdle records which partitions are idle as of this poll, for the
// in-order property: a partition that has been silent for the bound has
// left the minimum, and a row it delivers afterwards may be late by
// design.
func (m *Model) noteIdle() {
	for p := range m.parts {
		since := m.heldSince[p]
		if last := m.lastRow[p]; last.After(since) {
			since = last
		}
		m.idleAtPoll[p] = m.decl.IdleClose > 0 && m.clock.Sub(since) >= m.decl.IdleClose
		switch {
		case m.idleAtPoll[p]:
			m.behind[p] = true
		case m.behind[p] && m.hadClosed && !m.newest[p].Add(-m.decl.Grace).Before(m.closed):
			// Caught up: its own position is at or past what closed, so
			// the minimum owes it again from here.
			m.behind[p] = false
		}
	}
}

// collect publishes and deletes every bucket the watermark has passed.
func (m *Model) collect() []Publication {
	var out []Publication
	for b, rows := range m.buckets {
		if !b.Add(m.decl.Size).After(m.closed) {
			out = append(out, Publication{Bucket: b, Rows: rows})
			delete(m.buckets, b)
		}
	}
	return out
}

// InOrderRowWouldBeLate reports whether a row the partition would deliver
// now, in order and stamped with the clock, lands in a bucket the window
// has already closed -- while the partition holds, was not idle at the last
// poll, and is not still behind from an idleness it has not caught up from.
// That is a close the minimum should have prevented.
func (m *Model) InOrderRowWouldBeLate(p int32) bool {
	if m.parts[p] != holding || m.idleAtPoll[p] || m.behind[p] || !m.hadClosed {
		return false
	}
	at := m.clock.Add(time.Second)
	if at.Before(m.newest[p]) {
		return false // out of order for its own partition: may be late
	}
	return !at.Truncate(m.decl.Size).Add(m.decl.Size).After(m.closed)
}

// Open is how many rows the window still holds, and the newest bucket's end
// among them.
func (m *Model) Open() (rows int, newestEnd time.Time) {
	for b, n := range m.buckets {
		rows += n
		if end := b.Add(m.decl.Size); end.After(newestEnd) {
			newestEnd = end
		}
	}
	return rows, newestEnd
}

// Drain publishes what is still open, as a close that finally comes would,
// and reports the rows it carried.
func (m *Model) Drain() int {
	rows, _ := m.Open()
	m.buckets = map[time.Time]int{}
	return rows
}

// Asserted is the engine's watermark, if any.
func (m *Model) Asserted() (time.Time, bool) { return m.asserted, m.hasAsserted }
