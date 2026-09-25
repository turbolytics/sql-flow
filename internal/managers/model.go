package managers

import "time"

// The model is the table's rules over a sequence of events, with no database
// under it. checkTables proves every state selects one rule; this proves that
// a run of events ends with every row published once, which is the property
// no invariant owned while a reproduction of #183 lost 12% of its records.
//
// Two clocks, as the engine has. mono advances only by elapsing, and the
// engine's three instants are taken from it, so their differences survive a
// wall-clock step. wall is mono plus a skew a step moves, and rows land in
// buckets by wall time, because event time comes from the producer.

// EventKind is one thing that can happen to a pipeline.
type EventKind string

const (
	// Arrive is a batch reaching the handler and committing, the two as one
	// step. That is what a state path gives: the handler's rows and the
	// progress row that accounts for them ride the same transaction, so a
	// manager reading both sees them together or not at all.
	Arrive EventKind = "arrive"
	// Insert is the first half of that batch on its own: the rows reach the
	// window table, where a manager on another connection can see them, and
	// the commit that accounts for them has not happened yet. Without a state
	// path that is every batch -- the handler's INSERT autocommits as it
	// runs, while the arrival clock is stamped after the sink has flushed and
	// its write is throttled besides -- and a poll landing in between reads
	// the rows beside the silence they just ended (#374).
	Insert EventKind = "insert"
	// IdleTick is a commit with nothing buffered.
	IdleTick EventKind = "idle_tick"
	// Restart is the process dying and coming back: in-memory nothing
	// survives but the state database, which holds the buckets and the
	// watermark.
	Restart EventKind = "restart"
	// SourceLost is a consumer losing its partitions, or a websocket
	// dropping.
	SourceLost EventKind = "source_lost"
	// SourceBack is the assignment or the dial that follows.
	SourceBack EventKind = "source_back"
	// ClockStep is NTP moving the wall clock, in either direction.
	ClockStep EventKind = "clock_step"
)

// Event is one step of a sequence.
type Event struct {
	Kind EventKind
	Rows int
	By   time.Duration
}

// Publication is what a close handed the sink.
type Publication struct {
	Bucket time.Time
	Rows   int
}

// Decision is one poll: the rule the facts selected, and the facts. Counting
// the rows a run published cannot fail on a close that came early, so the
// evidence each close rested on is kept for the property to read.
type Decision struct {
	Rule   string
	Action Action
	Quiet  time.Duration
	// The source as the loop saw it at the commit this row came from, and
	// the two instants that say how long it had been delivering. The row
	// itself carries none of this: the loop's hold is what keeps the quiet
	// honest, and the property recomputes from these rather than trusting
	// that it did.
	Delivering bool
	RowAt      time.Time
	RowSince   time.Time
}

// Model is one pipeline: its buckets, its watermark, and the engine instants
// the window decides on.
type Model struct {
	decl Declaration

	mono time.Time
	skew time.Duration

	buckets map[time.Time]int
	// Produced counts rows the source actually delivered. A batch while the
	// source holds nothing delivers none, so it is not produced against this
	// pipeline at all.
	Produced int
	// Dropped counts rows that arrived for a bucket the watermark had
	// already passed, which late_rows drop discards and the property
	// excludes.
	Dropped int

	watermark    time.Time
	hadWatermark bool

	// What the loop holds in memory: when its quiet began, and what the
	// source is doing right now.
	quietSince      time.Time
	deliveringSince time.Time
	delivering      bool

	// What the progress row holds, which is the whole of what the manager
	// can see. The loop asks the source before an idle tick's commit and at
	// no other time, so a source that goes or comes back changes nothing the
	// manager reads until a commit writes the held clock down.
	rowArrival time.Time
	rowCommit  time.Time
	// The source and its instants at that commit, for the property.
	rowDelivering bool
	rowAt         time.Time
	rowSince      time.Time

	// Decisions is every poll's rule and the facts it read, in order.
	Decisions []Decision
}

var modelEpoch = time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)

// NewModel starts a pipeline whose source is delivering and whose window is
// empty.
func NewModel(decl Declaration) *Model {
	m := &Model{
		decl:    decl,
		mono:    modelEpoch,
		buckets: map[time.Time]int{},

		quietSince:      modelEpoch,
		deliveringSince: modelEpoch,
		delivering:      true,

		// A progress table with no row in it reads as never said: no quiet,
		// no duration, and nothing denying that the source delivers.
		rowArrival:    modelEpoch,
		rowCommit:     modelEpoch,
		rowDelivering: true,
	}
	return m
}

// insert puts a batch's rows in their buckets, where a reader on another
// connection can see them. It is the half of a batch that the handler does.
func (m *Model) insert(rows int) {
	if !m.delivering {
		return
	}
	m.Produced += rows
	bucket := m.wall().Truncate(m.decl.Size)
	// A row for a bucket the watermark has passed is late, and the drop
	// policy discards it.
	if m.hadWatermark && !bucket.Add(m.decl.Size).After(m.watermark) {
		m.Dropped += rows
	} else {
		m.buckets[bucket] += rows
	}
	// The engine stamps the quiet clock after the sink write, so an arrival
	// ends whatever quiet was accruing.
	m.quietSince = m.mono
}

// hold is holdQuietWhileNotDelivering: before every commit that is not a
// batch's, the loop keeps the quiet clock from running over time the source
// could not deliver. A source that is not delivering resets it to now; one
// that resumed since the clock was last set moves it to the resumption, so
// the tick after a rebalance confirms quiet from the assignment.
func (m *Model) hold() {
	switch {
	case !m.delivering:
		m.quietSince = m.mono
	case m.deliveringSince.After(m.quietSince):
		m.quietSince = m.deliveringSince
	}
}

// commit is a progress write, which is the only moment the row changes.
func (m *Model) commit() {
	m.rowArrival, m.rowCommit = m.quietSince, m.mono
	m.rowDelivering = m.delivering
	m.rowAt, m.rowSince = m.mono, m.deliveringSince
}

// wall is the clock the data is stamped on.
func (m *Model) wall() time.Time { return m.mono.Add(m.skew) }

// Apply advances the model by one event and returns what the poll after it
// published.
func (m *Model) Apply(e Event) []Publication {
	// Every event takes a second, so a sequence spans time without the
	// caller saying so.
	m.mono = m.mono.Add(time.Second)

	switch e.Kind {
	case Insert:
		m.insert(e.Rows)
	case Arrive:
		// A batch is messages, and a source holding nothing delivers none:
		// no rows, no commit. The only commit a loop makes while its source
		// holds nothing is the idle tick, and that one holds the clock
		// first. An earlier cut committed here regardless, and the row's
		// own source column hid it; against an engine whose row carries
		// only the quiet, that phantom commit confirms two seconds of
		// silence with nothing to say the source was gone.
		if m.delivering {
			m.insert(e.Rows)
			m.commit()
		}
	case IdleTick:
		m.hold()
		m.commit()
	case Restart:
		// The loop seeds its quiet clock at its start, so the outage before
		// it is not quiet this process watched — but it writes no row until
		// its first commit, so until then the manager is still polling the
		// dead process's. The source is new too: it has delivered since this
		// instant and no longer.
		m.quietSince = m.mono
		if m.delivering {
			m.deliveringSince = m.mono
		}
	case SourceLost:
		m.delivering = false
		m.deliveringSince = time.Time{}
	case SourceBack:
		m.delivering = true
		m.deliveringSince = m.mono
	case ClockStep:
		m.skew += e.By
	}

	return m.poll()
}

// poll is one manager pass: reduce to facts, decide, act.
func (m *Model) poll() []Publication {
	var newest time.Time
	for b := range m.buckets {
		if b.After(newest) {
			newest = b
		}
	}
	hasRows := len(m.buckets) > 0

	// Three readings of one row, which is all the manager gets.
	quiet := m.rowCommit.Sub(m.rowArrival)

	state := StateOf(m.decl, newest, hasRows, m.watermark, m.hadWatermark, quiet)
	rule := watermarkRuleFor(state)
	m.Decisions = append(m.Decisions, Decision{
		Rule: rule.Name, Action: rule.Action, Quiet: quiet,
		Delivering: m.rowDelivering, RowAt: m.rowAt, RowSince: m.rowSince,
	})
	next, moved := rule.Action.Next(m.decl, newest, m.watermark)
	if !moved || (m.hadWatermark && !next.After(m.watermark)) {
		return nil
	}
	m.watermark, m.hadWatermark = next, true
	return m.collect()
}

// collect publishes and deletes every bucket the watermark has passed.
func (m *Model) collect() []Publication {
	var out []Publication
	for b, rows := range m.buckets {
		if !b.Add(m.decl.Size).After(m.watermark) {
			out = append(out, Publication{Bucket: b, Rows: rows})
			delete(m.buckets, b)
		}
	}
	return out
}

// Drain publishes what is still open, as a close that finally comes would,
// and reports the rows it carried. A sequence's property is checked over the
// whole run, so what a run ends holding still has to be counted.
func (m *Model) Drain() int {
	rows := 0
	for _, n := range m.buckets {
		rows += n
	}
	m.buckets = map[time.Time]int{}
	return rows
}
