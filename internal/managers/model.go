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
	// Arrive is a batch reaching the handler and committing.
	Arrive EventKind = "arrive"
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

// Model is one pipeline: its buckets, its watermark, and the three engine
// instants the window decides on.
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

	lastArrival     time.Time
	lastCommit      time.Time
	deliveringSince time.Time
	delivering      bool
}

var modelEpoch = time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)

// NewModel starts a pipeline whose source is delivering and whose window is
// empty.
func NewModel(decl Declaration) *Model {
	m := &Model{
		decl:    decl,
		mono:    modelEpoch,
		buckets: map[time.Time]int{},

		lastArrival:     modelEpoch,
		lastCommit:      modelEpoch,
		deliveringSince: modelEpoch,
		delivering:      true,
	}
	return m
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
	case Arrive:
		if m.delivering {
			m.Produced += e.Rows
			bucket := m.wall().Truncate(m.decl.Size)
			// A row for a bucket the watermark has passed is late, and the
			// drop policy discards it.
			if m.hadWatermark && !bucket.Add(m.decl.Size).After(m.watermark) {
				m.Dropped += e.Rows
			} else {
				m.buckets[bucket] += e.Rows
			}
			// The engine stamps the quiet clock after the sink write, so an
			// arrival ends whatever quiet was accruing.
			m.lastArrival = m.mono
		}
		m.lastCommit = m.mono
	case IdleTick:
		m.lastCommit = m.mono
	case Restart:
		// The loop seeds the quiet clock at its start, so the outage before
		// it is not quiet this process watched.
		m.lastArrival = m.mono
		m.lastCommit = m.mono
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

	quiet := m.lastCommit.Sub(m.lastArrival)
	deliveringFor := time.Duration(-1)
	if m.delivering && !m.deliveringSince.IsZero() {
		deliveringFor = m.lastCommit.Sub(m.deliveringSince)
	}

	state := StateOf(m.decl, newest, hasRows, m.watermark, m.hadWatermark,
		quiet, deliveringFor, m.delivering)
	next, moved := Decide(state).Next(m.decl, newest, m.watermark)
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
