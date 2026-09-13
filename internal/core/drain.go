package core

import (
	"context"
	"sync"
	"time"

	"github.com/turbolytics/sql-flow/internal/config"
)

// DefaultDrainDeadline bounds a shutdown that the config did not bound. The
// value is declared in config.DefaultDrainDeadlineSeconds, where `sqlflow
// validate` reads it without linking DuckDB.
const DefaultDrainDeadline = config.DefaultDrainDeadlineSeconds * time.Second

// DrainBudget is one deadline for everything a shutdown does.
//
// After a SIGTERM four things reach DuckDB or a sink: the turbine's final
// batch, a state sync, each manager's final poll and a second sync. A deadline
// per step would let the shutdown take four deadlines, and a supervisor gives
// it one. So the budget starts its clock on the first call to Context and
// hands the same context to every caller after that.
//
// The consume loop and every manager goroutine call it, so it is guarded by
// a mutex rather than left to the order the goroutines happen to run in.
type DrainBudget struct {
	deadline time.Duration

	mu     sync.Mutex
	ctx    context.Context
	cancel context.CancelFunc
}

// NewDrainBudget makes a budget whose clock has not started. Zero and
// negative mean DefaultDrainDeadline.
func NewDrainBudget(deadline time.Duration) *DrainBudget {
	if deadline <= 0 {
		deadline = DefaultDrainDeadline
	}
	return &DrainBudget{deadline: deadline}
}

// Context starts the clock on the first call and returns the same context on
// every call after it.
func (b *DrainBudget) Context() context.Context {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.ctx == nil {
		b.ctx, b.cancel = context.WithTimeout(context.Background(), b.deadline)
	}
	return b.ctx
}

// Exceeded reports whether the deadline has passed. False before the clock
// starts.
func (b *DrainBudget) Exceeded() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.ctx != nil && b.ctx.Err() != nil
}

// Deadline is what the budget was built with, for log lines and errors.
func (b *DrainBudget) Deadline() time.Duration { return b.deadline }

// Stop releases the timer. Safe before the clock starts. A context handed
// out before Stop ends with context.Canceled, which Exceeded also reports, so
// call Stop only once nothing is draining.
func (b *DrainBudget) Stop() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.cancel != nil {
		b.cancel()
	}
}

// WithDrainBudget bounds the turbine's final batch. A turbine built without
// one gets DefaultDrainDeadline.
func WithDrainBudget(b *DrainBudget) TurbineOption {
	return func(t *Turbine) { t.drain = b }
}
