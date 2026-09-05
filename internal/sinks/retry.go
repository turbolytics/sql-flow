package sinks

import (
	"context"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/turbolytics/sql-flow/internal/core"
	"github.com/turbolytics/sql-flow/internal/errs"
)

// RetryPolicy bounds how long a sink keeps trying a destination that is not
// answering.
//
// Deadline bounds the whole ladder rather than one attempt, and it is the
// field that matters most. The retry runs inside the pipeline's open state
// transaction, and DuckDB's now() returns that transaction's start time, so a
// ladder that outlives the flush interval freezes the window clock -- the bug
// #158 fixed, reached by a second route.
type RetryPolicy struct {
	MaxAttempts    int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	Deadline       time.Duration
}

// retrying wraps a sink whose destination may be temporarily unavailable.
//
// It is a decorator rather than a change to each sink because the sinks do not
// want it uniformly: the Kafka sink hands records to franz-go, which already
// retries with its own backoff, and a second ladder on top of that one is
// worse than none. New decides which sinks get wrapped.
type retrying struct {
	inner  core.Sink
	policy RetryPolicy

	// sleep waits, or reports why it stopped. Injected so the tests assert the
	// backoff ladder without taking it.
	sleep func(ctx context.Context, d time.Duration) error

	// now reads the clock the deadline is measured against. Injected alongside
	// sleep: a test that fakes one and not the other measures a deadline
	// against a clock that never advances, and the deadline never binds.
	now func() time.Time

	// onRetry reports an attempt that failed and will be tried again, so a
	// stalled sink is visible before the deadline fires.
	onRetry func(attempt int, err error)
}

func newRetrying(inner core.Sink, policy RetryPolicy) *retrying {
	return &retrying{
		inner:   inner,
		policy:  policy,
		sleep:   sleepCtx,
		now:     time.Now,
		onRetry: func(int, error) {},
	}
}

// sleepCtx waits for d, or returns early when the context ends. Returning the
// context's error is what stops a ladder on SIGTERM instead of letting it
// outlive the drain.
func sleepCtx(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (r *retrying) Batch() (arrow.Table, error) { return r.inner.Batch() }

// WriteTable is not retried. It buffers into the sink rather than reaching the
// destination, so there is nothing here for a backoff to wait on.
func (r *retrying) WriteTable(ctx context.Context, batch arrow.Table) error {
	return r.inner.WriteTable(ctx, batch)
}

func (r *retrying) Flush(ctx context.Context) error {
	// A policy bounds how many times the write is retried. It never licenses
	// skipping the write: max_attempts of 0 must still deliver the batch, or
	// the caller is told a flush happened that never did.
	maxAttempts := r.policy.MaxAttempts
	if maxAttempts < 1 {
		maxAttempts = 1
	}

	maxBackoff := r.policy.MaxBackoff
	if maxBackoff < 0 {
		maxBackoff = 0
	}
	// Clamped before the first sleep, not after it. Applying the cap only on
	// the way round the loop lets the first wait exceed the ceiling the
	// operator set.
	backoff := clamp(r.policy.InitialBackoff, maxBackoff)
	deadline := r.now().Add(r.policy.Deadline)

	for attempt := 1; ; attempt++ {
		err := r.inner.Flush(ctx)
		if err == nil {
			return nil
		}
		// A rejected write or a bad config keeps its own code. Rewriting it as
		// unreachable would tell a supervisor to retry a pipeline that cannot
		// succeed.
		if !retryable(err) {
			return err
		}

		if attempt >= maxAttempts {
			return errs.Wrap(errs.CodeSinkUnreachable, err,
				"sink still failing after %d attempts", attempt)
		}

		// Checked before sleeping, so a ladder that would wake past the
		// deadline stops now instead of overrunning it. The clock, not the sum
		// of the backoffs: a flush attempt that blocks on a TCP timeout spends
		// the deadline too.
		if r.now().Add(backoff).After(deadline) {
			return errs.Wrap(errs.CodeSinkUnreachable, err,
				"sink still failing after %d attempts, %s retry deadline reached",
				attempt, r.policy.Deadline)
		}

		r.onRetry(attempt, err)
		if sleepErr := r.sleep(ctx, backoff); sleepErr != nil {
			// The context ended. Report the sink's failure rather than the
			// cancellation: the sink is why this batch did not land.
			return err
		}

		backoff = grow(backoff, maxBackoff)
	}
}

// clamp keeps a backoff inside [0, max]. A negative duration would make
// time.Timer fire immediately and turn the ladder into a spin.
func clamp(d, max time.Duration) time.Duration {
	if d < 0 {
		d = 0
	}
	if d > max {
		d = max
	}
	return d
}

// grow doubles a backoff, stopping at max. The overflow check matters because
// doubling past the int64 ceiling wraps negative, which clamp would then read
// as zero and spin on.
func grow(d, max time.Duration) time.Duration {
	next := d * 2
	if next < d {
		next = max
	}
	return clamp(next, max)
}

// retryable reports whether another attempt could plausibly succeed.
//
// A rejected write and a bad configuration fail identically every time, so
// retrying them only delays the report past the deadline. Everything else is
// retried, including errors carrying no code: a driver's timeout or reset
// arrives unclassified, and those are exactly the failures this exists for.
// The deadline bounds the cost of guessing wrong.
func retryable(err error) bool {
	switch errs.CodeOf(err) {
	case errs.CodeSinkWriteFailed, errs.CodeSinkInvalid, errs.CodeConfigInvalid:
		return false
	default:
		return true
	}
}
