package activity

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

// fake is a clock whose elapsed time the test sets.
func fake() (*Clock, *time.Duration) {
	elapsed := new(time.Duration)
	return Fake(func() time.Duration { return *elapsed }), elapsed
}

// t.UTC() strips the monotonic reading, and root.go once took its start
// that way. Start is the only production constructor, so it is the one
// place that has to keep it.
func TestClock_StartKeepsTheMonotonicReading(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	c := Start()
	assert.That(t, strings.Contains(c.start.String(), "m=+"))
	// StartedAt is for display, in UTC, and is allowed to lose it.
	assert.Equal(t, time.UTC, c.StartedAt().Location())
}

func TestClock_NoWorkYetIsNotIdle(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	c, elapsed := fake()
	*elapsed = 90 * time.Second

	uptime, idle, worked := c.Read()
	assert.Equal(t, 90*time.Second, uptime)
	assert.Equal(t, time.Duration(0), idle)
	assert.That(t, !worked)
}

func TestClock_IdleIsTimeSinceTheLastMark(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	c, elapsed := fake()
	*elapsed = 30 * time.Second
	c.Mark()
	*elapsed = 90 * time.Second

	uptime, idle, worked := c.Read()
	assert.Equal(t, 90*time.Second, uptime)
	assert.Equal(t, 60*time.Second, idle)
	assert.That(t, worked)
}

// A mark at the instant of start is still work. Zero is the sentinel for
// none, so the offset is stored plus one.
func TestClock_AMarkAtStartIsWork(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	c, elapsed := fake()
	c.Mark()
	*elapsed = 5 * time.Second

	_, idle, worked := c.Read()
	assert.That(t, worked)
	assert.Equal(t, 5*time.Second, idle)
}

// Both durations come from one reading, so idle never exceeds uptime, even
// when a caller truncates both to whole seconds.
func TestClock_IdleNeverExceedsUptime(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	c, elapsed := fake()
	for i := 0; i < 1000; i++ {
		*elapsed = time.Duration(i) * 997 * time.Millisecond
		if i%7 == 0 {
			c.Mark()
		}
		uptime, idle, _ := c.Read()
		assert.That(t, idle <= uptime)
		assert.That(t, idle/time.Second <= uptime/time.Second)
	}
}

func TestClock_MarkOnNilIsANoop(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	var c *Clock
	c.Mark()
}

// The pipeline marks from its consume loop while the reporter reads from
// its own goroutine. Run with -race.
func TestClock_MarkAndReadAreSafeTogether(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	c := Start()
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				c.Mark()
				_, _, _ = c.Read()
			}
		}()
	}
	wg.Wait()
	_, _, worked := c.Read()
	assert.That(t, worked)
}

// Mark runs once per batch in the consume loop and once per request in
// serve, so it must cost nothing a soak would see. Read runs once per
// report.
func TestClock_MarkAndReadDoNotAllocate(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	c := Start()
	assert.Equal(t, 0.0, testing.AllocsPerRun(1000, c.Mark))
	assert.Equal(t, 0.0, testing.AllocsPerRun(1000, func() { _, _, _ = c.Read() }))
	var none *Clock
	assert.Equal(t, 0.0, testing.AllocsPerRun(1000, none.Mark))
}
