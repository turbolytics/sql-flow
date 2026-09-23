package managers

import (
	"context"
	"testing"
	"time"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

// An injected trigger is the only thing that makes the manager poll, so a
// test decides when a poll happens rather than waiting out the interval. The
// manager here is built with an hour's interval: without the trigger the
// close below would never run.
func TestManagerWindow_AnInjectedTriggerDrivesThePoll(t *testing.T) {
	coverage.Covers(t, "manager.window")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	d := newTestDB(t, "")
	createWindowTable(t, d.pipeline)
	now := live(d, t)
	sink := &recordingSink{}
	trigger := make(chan time.Time)
	w := newTestWatermark(t, d, testDecl(), sink, now, WithPollTrigger(trigger))

	// Bucket 0 is a grace behind bucket 2, so one poll closes it.
	insertBucket(t, d.pipeline, 0, "NYC", 3)
	insertBucket(t, d.pipeline, 2, "NYC", 7)

	done := make(chan struct{})
	go func() {
		_ = w.Start(ctx)
		close(done)
	}()

	trigger <- time.Now()

	deadline := time.After(5 * time.Second)
	for {
		if _, flushes := sink.counts(); flushes > 0 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("the trigger did not produce a poll")
		case <-time.After(10 * time.Millisecond):
		}
	}

	rows, flushes := sink.counts()
	assert.Equal(t, int64(1), rows)
	assert.Equal(t, 1, flushes)

	cancel()
	<-done
}
