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

// The batch the drain could not write is rolled back on a live context even
// though the deadline has passed. The DuckDB driver ignores the context on a
// rollback, so this guards a transaction that honours it: refused, it would
// leave the handler's writes in the open transaction for the next state sync
// to commit without their offsets, and a replay would count them twice.
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

// gatedSink blocks in Flush until released or until its context ends, and
// says which one happened.
type gatedSink struct {
	entered chan struct{}
	release chan struct{}
	mu      sync.Mutex
	rows    int64
	pending int64
	flushes int
}

func newGatedSink() *gatedSink {
	return &gatedSink{entered: make(chan struct{}, 1), release: make(chan struct{})}
}

func (s *gatedSink) WriteTable(_ context.Context, batch arrow.Table) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if batch != nil {
		s.pending += batch.NumRows()
	}
	return nil
}

func (s *gatedSink) Flush(ctx context.Context) error {
	s.mu.Lock()
	s.flushes++
	s.mu.Unlock()
	select {
	case s.entered <- struct{}{}:
	default:
	}
	select {
	case <-s.release:
		s.mu.Lock()
		s.rows += s.pending
		s.pending = 0
		s.mu.Unlock()
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// A SIGTERM that lands while a batch is being flushed does not abort the
// flush. The batch keeps the drain deadline, finishes, and the loop stops
// clean with the rows delivered. Before this, the cancel killed the flush at
// once, the loop returned the sink's error, and the batch replayed on the
// next start for no reason.
func TestLifecycleDrain_ACancelDuringAFlushLetsItFinish(t *testing.T) {
	coverage.Covers(t, "lifecycle.drain")
	src := newBlockingSource(messages(5))
	sink := newGatedSink()
	budget := NewDrainBudget(5 * time.Second)
	defer budget.Stop()
	tb := NewTurbine(src, &fakeHandler{}, sink, 5, time.Hour, &sync.Mutex{},
		PipelineErrorPolicies{}, WithDrainBudget(budget))

	ctx, cancel := context.WithCancel(context.Background())
	defer close(src.release)
	done := make(chan error, 1)
	go func() {
		_, err := tb.ConsumeLoop(ctx, 0)
		done <- err
	}()

	<-sink.entered
	cancel()
	select {
	case err := <-done:
		t.Fatalf("the loop returned %v while the batch was still being flushed", err)
	case <-time.After(200 * time.Millisecond):
	}
	close(sink.release)

	select {
	case err := <-done:
		assert.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("the loop did not return after the flush finished")
	}
	sink.mu.Lock()
	defer sink.mu.Unlock()
	assert.Equal(t, int64(5), sink.rows)
	assert.Equal(t, 1, sink.flushes)
}

// The same cancel against a flush that never finishes: the drain deadline
// ends it, and the loop reports the drain as incomplete rather than the
// sink's own error.
func TestLifecycleDrain_ACancelDuringAFlushIsBoundedByTheDeadline(t *testing.T) {
	coverage.Covers(t, "lifecycle.drain")
	src := newBlockingSource(messages(5))
	sink := newGatedSink()
	budget := NewDrainBudget(300 * time.Millisecond)
	defer budget.Stop()
	tb := NewTurbine(src, &fakeHandler{}, sink, 5, time.Hour, &sync.Mutex{},
		PipelineErrorPolicies{}, WithDrainBudget(budget))

	ctx, cancel := context.WithCancel(context.Background())
	defer close(src.release)
	done := make(chan error, 1)
	go func() {
		_, err := tb.ConsumeLoop(ctx, 0)
		done <- err
	}()

	<-sink.entered
	started := time.Now()
	cancel()
	var err error
	select {
	case err = <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the drain outlived its deadline")
	}
	took := time.Since(started)

	assert.Equal(t, errs.CodeDrainIncomplete, errs.CodeOf(err))
	assert.That(t, took >= 250*time.Millisecond)
	assert.That(t, took < 3*time.Second)
	assert.Equal(t, int64(1), tb.Progress().Errors)
}
