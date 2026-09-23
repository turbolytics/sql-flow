package core

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/turbolytics/sql-flow/internal/coverage"
)

// countingSaver counts the progress writes a test provoked.
type countingSaver struct {
	mu sync.Mutex
	n  int
}

func (c *countingSaver) Record(context.Context, Progress) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.n++
	return nil
}

func (c *countingSaver) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.n
}

// An injected trigger is the only thing that fires an idle tick, so a test
// decides when one happens rather than waiting out a flush interval. The
// interval here is an hour: without the trigger nothing would be written.
func TestTurbineTrigger_AnInjectedTickDrivesTheIdleCommit(t *testing.T) {
	coverage.Covers(t, "core.consume_loop")
	saver := &countingSaver{}
	src := newBlockingSource(nil)
	trigger := make(chan time.Time)
	tb := NewTurbine(src, &fakeHandler{}, &fakeSink{}, 1, time.Hour,
		&sync.Mutex{}, PipelineErrorPolicies{},
		WithProgressStore(saver), WithFlushTrigger(trigger))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_, _ = tb.ConsumeLoop(ctx, 0)
		close(done)
	}()

	before := saver.count()
	trigger <- time.Now()
	waitFor(t, "an idle tick to commit", 5*time.Second, func() bool {
		return saver.count() > before
	})

	cancel()
	close(src.release)
	<-done
}
