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

	// onRetry reports an attempt that failed and will be tried again, so a
	// stalled sink is visible before the deadline fires.
	onRetry func(attempt int, err error)
}

func newRetrying(inner core.Sink, policy RetryPolicy) *retrying {
	return &retrying{
		inner:   inner,
		policy:  policy,
		sleep:   sleepCtx,
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
	deadline := time.Now().Add(r.policy.Deadline)
	backoff := r.policy.InitialBackoff

	var err error
	for attempt := 1; attempt <= r.policy.MaxAttempts; attempt++ {
		err = r.inner.Flush(ctx)
		if err == nil {
			return nil
		}
		if !retryable(err) {
			return err
		}
		if attempt == r.policy.MaxAttempts {
			break
		}

		// Checked before sleeping rather than after, so a ladder that would
		// wake past the deadline stops now instead of overrunning it.
		if time.Now().Add(backoff).After(deadline) {
			break
		}

		r.onRetry(attempt, err)
		if sleepErr := r.sleep(ctx, backoff); sleepErr != nil {
			return err
		}

		backoff *= 2
		if backoff > r.policy.MaxBackoff {
			backoff = r.policy.MaxBackoff
		}
	}

	// The ladder ran out. Report the destination as unreachable so the exit
	// code says "the dependency may come back" rather than "we have a bug".
	return errs.Wrap(errs.CodeSinkUnreachable, err,
		"sink still failing after %d attempts", r.policy.MaxAttempts)
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
