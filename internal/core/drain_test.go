package core

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/turbolytics/sql-flow/internal/errs"
	"github.com/zeebo/assert"
)

// The budget is one clock. Every caller after the first gets the same
// context, so the turbine's final batch and the managers' final poll spend
// the same seconds rather than each getting a fresh deadline.
func TestLifecycleDrain_BudgetIsOneClock(t *testing.T) {
	coverage.Covers(t, "lifecycle.drain")
	b := NewDrainBudget(50 * time.Millisecond)
	defer b.Stop()

	assert.That(t, !b.Exceeded())
	first := b.Context()
	second := b.Context()
	assert.That(t, first == second)

	<-first.Done()
	assert.That(t, b.Exceeded())
	assert.That(t, errors.Is(first.Err(), context.DeadlineExceeded))
}

// A budget built with no deadline still has one. A shutdown the config
// forgot to bound is bounded anyway.
func TestLifecycleDrain_ZeroDeadlineMeansTheDefault(t *testing.T) {
	coverage.Covers(t, "lifecycle.drain")
	assert.Equal(t, DefaultDrainDeadline, NewDrainBudget(0).Deadline())
	assert.Equal(t, DefaultDrainDeadline, NewDrainBudget(-time.Second).Deadline())
}

// hangingSink blocks in Flush until its context ends. It is the sink a drain
// deadline exists for: one that neither succeeds nor fails on its own.
type hangingSink struct {
	mu      sync.Mutex
	flushes int
}

func (s *hangingSink) WriteTable(context.Context, arrow.Table) error { return nil }

func (s *hangingSink) Flush(ctx context.Context) error {
	s.mu.Lock()
	s.flushes++
	s.mu.Unlock()
	<-ctx.Done()
	return ctx.Err()
}

// rejectingSink refuses every flush with a coded error, immediately.
type rejectingSink struct{ err error }

func (s *rejectingSink) WriteTable(context.Context, arrow.Table) error { return nil }
func (s *rejectingSink) Flush(context.Context) error                   { return s.err }

// drainRun buffers n messages, cancels, and returns what ConsumeLoop returned
// and how long the drain took after the cancel.
func drainRun(t *testing.T, n int, sink Sink, opts ...TurbineOption) (error, time.Duration) {
	t.Helper()
	src := newBlockingSource(messages(n))
	h := &drainHandler{wrote: make(chan struct{})}
	tb := NewTurbine(src, h, sink, 1000, time.Hour, &sync.Mutex{},
		PipelineErrorPolicies{}, opts...)

	ctx, cancel := context.WithCancel(context.Background())
	defer close(src.release)

	var (
		err  error
		done = make(chan struct{})
	)
	go func() {
		defer close(done)
		_, err = tb.ConsumeLoop(ctx, 0)
	}()

	for i := 0; i < n; i++ {
		<-h.wrote
	}
	started := time.Now()
	cancel()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the drain outlived its deadline by more than 5s")
	}
	return err, time.Since(started)
}

// A sink that never answers must not hold the process past the deadline.
// The loop returns inside the budget with the drain code, and the rows stay
// unwritten and uncommitted for the next start to replay.
func TestLifecycleDrain_DeadlineBoundsTheFinalBatch(t *testing.T) {
	coverage.Covers(t, "lifecycle.drain")
	budget := NewDrainBudget(200 * time.Millisecond)
	defer budget.Stop()

	err, took := drainRun(t, 10, &hangingSink{}, WithDrainBudget(budget))

	assert.That(t, took < 2*time.Second)
	assert.Error(t, err)
	assert.Equal(t, errs.CodeDrainIncomplete, errs.CodeOf(err))
	assert.That(t, budget.Exceeded())
}

// A sink that fails for its own reason during the drain keeps its own code.
// Only running out of time is a drain failure.
func TestLifecycleDrain_ASinkFailureInsideTheDeadlineKeepsItsCode(t *testing.T) {
	coverage.Covers(t, "lifecycle.drain")
	budget := NewDrainBudget(5 * time.Second)
	defer budget.Stop()

	sink := &rejectingSink{err: errs.New(errs.CodeSinkWriteFailed, "rejected")}
	err, _ := drainRun(t, 3, sink, WithDrainBudget(budget))

	assert.Equal(t, errs.CodeSinkWriteFailed, errs.CodeOf(err))
	assert.That(t, !budget.Exceeded())
}

// expiryTx records whether each boundary call arrived on a live context.
type expiryTx struct {
	mu     sync.Mutex
	events []string
}

func (x *expiryTx) record(call string, ctx context.Context) error {
	x.mu.Lock()
	defer x.mu.Unlock()
	if ctx.Err() != nil {
		x.events = append(x.events, call+"-on-expired-context")
		return ctx.Err()
	}
	x.events = append(x.events, call)
	return nil
}

func (x *expiryTx) Commit(ctx context.Context) error   { return x.record("commit", ctx) }
func (x *expiryTx) Rollback(ctx context.Context) error { return x.record("rollback", ctx) }

// The batch the drain could not write must be rolled back even though the
// deadline has passed. A rollback on the expired context fails in DuckDB and
// leaves the handler's writes in the open transaction, where the next state
// sync commits them without their offsets: a replay then counts them twice.
func TestLifecycleDrain_RollbackOutlivesTheDeadline(t *testing.T) {
	coverage.Covers(t, "lifecycle.drain")
	budget := NewDrainBudget(100 * time.Millisecond)
	defer budget.Stop()

	var offsetEvents []string
	tx := &expiryTx{}
	err, _ := drainRun(t, 5, &hangingSink{}, WithDrainBudget(budget),
		WithStateStore(&fakeOffsetStore{events: &offsetEvents}, tx))

	assert.Equal(t, errs.CodeDrainIncomplete, errs.CodeOf(err))
	tx.mu.Lock()
	defer tx.mu.Unlock()
	assert.DeepEqual(t, []string{"rollback"}, tx.events)
}
