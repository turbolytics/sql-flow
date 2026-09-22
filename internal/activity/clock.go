// Package activity measures how long a process has run, and how long since
// it last did work, on the monotonic clock.
//
// The wall clock is the wrong instrument for both. A gateway without a
// hardware clock boots near 1970 and then syncs, and NTP steps a fast clock
// back. A duration taken from two wall readings across either step is wrong
// by the step, for the life of the process. Go's monotonic reading does not
// step, so every duration here is time.Since on a start that carries one.
package activity

import (
	"sync/atomic"
	"time"
)

// Clock is one process's start and its most recent work.
type Clock struct {
	start time.Time
	// last is nanoseconds from start to the most recent Mark, plus one, so
	// zero means no work yet.
	last atomic.Int64
	// elapsed is time since start. time.Since in production.
	elapsed func() time.Duration
}

// Start takes the process start now.
//
// It takes no argument on purpose. A caller handing in a time could hand in
// one whose monotonic reading was stripped: t.UTC() strips it, and the run
// command once did exactly that.
func Start() *Clock {
	c := &Clock{start: time.Now()}
	c.elapsed = func() time.Duration { return time.Since(c.start) }
	return c
}

// Fake is a clock whose elapsed time the caller supplies. It is for tests in
// other packages, which cannot wait for the monotonic clock to move.
func Fake(elapsed func() time.Duration) *Clock {
	return &Clock{start: time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC), elapsed: elapsed}
}

// StartedAt is the start as wall time, for display. Never subtract it from
// another wall reading: that is the arithmetic this package exists to
// replace.
func (c *Clock) StartedAt() time.Time { return c.start.UTC() }

// Mark records work now. It is safe on a nil Clock, so a caller built without
// one does not have to check.
func (c *Clock) Mark() {
	if c == nil {
		return
	}
	c.last.Store(int64(c.elapsed()) + 1)
}

// Read returns the uptime, the time since the last Mark, and whether there
// has been one.
//
// Both come from a single elapsed reading. Two readings would let idle
// exceed uptime by the time between them, and a receiver that checks
// idle <= uptime would call the bundle contradictory.
func (c *Clock) Read() (uptime, idle time.Duration, worked bool) {
	uptime = c.elapsed()
	last := c.last.Load()
	if last == 0 {
		return uptime, 0, false
	}
	idle = uptime - time.Duration(last-1)
	if idle < 0 {
		// A Mark raced this Read and landed after the elapsed reading.
		idle = 0
	}
	return uptime, idle, true
}
