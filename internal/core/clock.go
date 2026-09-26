package core

import "time"

// WithClock replaces the instant source behind the turbine's decisions.
// Production leaves it alone; a simulator sets it so a sequence of events can
// be placed on the clock rather than waited for.
//
// The function must keep the monotonic reading time.Now() carries. A
// partition's idleness is a duration between two of its results, and a fake
// returning UTC() values makes that wall-clock arithmetic, which a clock
// step would then read as silence.
//
// Latency timers are not part of this. They measure a span that begins and
// ends inside one call, they feed histograms rather than decisions, and a
// frozen clock would report every span as zero.
func WithClock(now func() time.Time) TurbineOption {
	return func(t *Turbine) { t.clock = now }
}

// now is every instant a window decision rests on: when a batch stopped being
// worked, when a commit happened, and when the loop started.
func (t *Turbine) now() time.Time {
	if t.clock == nil {
		return time.Now()
	}
	return t.clock()
}
