package managers

import (
	"fmt"
	"time"
)

// FactClock is which clock measured a fact.
//
// Event time comes from the data: the newest bucket the stream has reached,
// against the committed watermark. The engine clock comes from the progress
// row, whose values are only ever subtracted from each other. The two are
// never compared, and every defect this package has had was a comparison that
// mixed them or reached for the wall clock beside them.
type FactClock string

const (
	EventTime   FactClock = "event_time"
	EngineClock FactClock = "engine"
	NoClock     FactClock = "none"
)

// Fact is one input to a decision, with the evidence for its value.
type Fact struct {
	Name     string
	Value    string
	Measured time.Duration
	Limit    time.Duration
	Clock    FactClock
	From     string
}

// String renders a fact for a log line: idle=confirmed 1m30s against 1m0s,
// engine.
func (f Fact) String() string {
	if f.Clock == NoClock {
		return fmt.Sprintf("%s=%s", f.Name, f.Value)
	}
	if f.Limit == 0 {
		return fmt.Sprintf("%s=%s %s, %s", f.Name, f.Value, f.Measured, f.Clock)
	}
	return fmt.Sprintf("%s=%s %s against %s, %s", f.Name, f.Value, f.Measured, f.Limit, f.Clock)
}

// FactsOf is the evidence behind a state: what each fact measured, the limit
// it was measured against, and which clock produced it.
func FactsOf(decl Declaration, s State, quiet, deliveringFor time.Duration,
	newest, previous time.Time) []Fact {
	var streamPast time.Duration
	if !newest.IsZero() && !previous.IsZero() {
		streamPast = newest.Sub(previous)
	}
	return []Fact{
		{
			Name: "data", Value: string(s.Data),
			Measured: streamPast, Limit: decl.Grace,
			Clock: EventTime, From: "window table",
		},
		{
			Name: "idle", Value: string(s.Idle),
			Measured: quiet, Limit: decl.IdleClose,
			Clock: EngineClock, From: "sqlflow_progress",
		},
		{
			Name: "source", Value: string(s.Source),
			Measured: deliveringFor,
			Clock:    EngineClock, From: "sqlflow_progress",
		},
	}
}

// factDescriptions is the three facts with no measurement, for the rendered
// decision page: what each one is, and which clock answers it.
func factDescriptions() []Fact {
	return []Fact{
		{Name: "data", Value: "none | behind | open | ripe", Clock: EventTime, From: "the window table's newest bucket against the committed watermark"},
		{Name: "idle", Value: "off | unconfirmed | confirmed", Clock: EngineClock, From: "last_commit - last_arrival, bounded by delivering_for_us"},
		{Name: "source", Value: "delivering | not_delivering", Clock: EngineClock, From: "delivering_for_us, which is negative while the source holds nothing and NULL from one that never said"},
	}
}
