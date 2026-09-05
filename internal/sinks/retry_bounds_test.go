package sinks

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/turbolytics/sql-flow/internal/errs"
	"github.com/zeebo/assert"
)

// Every bound of RetryPolicy, including the degenerate values a config can
// produce. A retry ladder sits between a batch and its destination, so a bound
// that misbehaves either drops the batch or spins on it.

func alwaysFails() *flakySink {
	return &flakySink{failures: 1 << 30, err: errs.New(errs.CodeSinkUnreachable, "refused")}
}

// --- MaxAttempts -----------------------------------------------------------

// The batch must reach the sink even under a nonsense policy. Skipping the
// flush loses the batch, and the caller is told it succeeded.
func TestSinkRetry_BoundsMaxAttemptsZeroStillFlushesOnce(t *testing.T) {
	inner := &flakySink{}
	p := testPolicy()
	p.MaxAttempts = 0
	r, _ := newTestRetry(inner, p)

	err := r.Flush(context.Background())

	assert.NoError(t, err)
	assert.Equal(t, 1, inner.attempts)
}

func TestSinkRetry_BoundsMaxAttemptsNegativeStillFlushesOnce(t *testing.T) {
	inner := &flakySink{}
	p := testPolicy()
	p.MaxAttempts = -3
	r, _ := newTestRetry(inner, p)

	assert.NoError(t, r.Flush(context.Background()))
	assert.Equal(t, 1, inner.attempts)
}

// A failure under a zero policy still has to be reported, not swallowed.
func TestSinkRetry_BoundsMaxAttemptsZeroReportsTheFailure(t *testing.T) {
	inner := alwaysFails()
	p := testPolicy()
	p.MaxAttempts = 0
	r, _ := newTestRetry(inner, p)

	err := r.Flush(context.Background())

	assert.Error(t, err)
	assert.Equal(t, 1, inner.attempts)
}

func TestSinkRetry_BoundsMaxAttemptsOneMakesExactlyOneAttempt(t *testing.T) {
	inner := alwaysFails()
	p := testPolicy()
	p.MaxAttempts = 1
	r, slept := newTestRetry(inner, p)

	assert.Error(t, r.Flush(context.Background()))
	assert.Equal(t, 1, inner.attempts)
	assert.Equal(t, 0, len(*slept))
}

func TestSinkRetry_BoundsMaxAttemptsTwoSleepsExactlyOnce(t *testing.T) {
	inner := alwaysFails()
	p := testPolicy()
	p.MaxAttempts = 2
	r, slept := newTestRetry(inner, p)

	assert.Error(t, r.Flush(context.Background()))
	assert.Equal(t, 2, inner.attempts)
	assert.Equal(t, 1, len(*slept))
}

// The ladder sleeps between attempts, never after the last one.
func TestSinkRetry_BoundsNeverSleepsAfterTheFinalAttempt(t *testing.T) {
	for attempts := 1; attempts <= 6; attempts++ {
		inner := alwaysFails()
		p := testPolicy()
		p.MaxAttempts = attempts
		r, slept := newTestRetry(inner, p)

		assert.Error(t, r.Flush(context.Background()))
		if inner.attempts != attempts {
			t.Errorf("max_attempts %d: made %d attempts", attempts, inner.attempts)
		}
		if len(*slept) != attempts-1 {
			t.Errorf("max_attempts %d: slept %d times, want %d",
				attempts, len(*slept), attempts-1)
		}
	}
}

// --- Backoff ---------------------------------------------------------------

// A cap below the initial backoff must bind on the first sleep. Applying it
// only after the first sleep lets one wait exceed the cap the operator set.
func TestSinkRetry_BoundsInitialBackoffAboveTheCapIsCapped(t *testing.T) {
	inner := alwaysFails()
	p := testPolicy()
	p.MaxAttempts = 3
	p.InitialBackoff = 5 * time.Second
	p.MaxBackoff = 100 * time.Millisecond
	p.Deadline = time.Hour
	r, slept := newTestRetry(inner, p)

	assert.Error(t, r.Flush(context.Background()))

	for i, d := range *slept {
		if d > p.MaxBackoff {
			t.Errorf("sleep %d was %v, above the %v cap", i, d, p.MaxBackoff)
		}
	}
}

// Zero backoff means retry immediately, which is a legitimate ask. It must
// stay bounded by MaxAttempts rather than spinning.
func TestSinkRetry_BoundsZeroBackoffRetriesImmediately(t *testing.T) {
	inner := alwaysFails()
	p := testPolicy()
	p.MaxAttempts = 4
	p.InitialBackoff = 0
	p.MaxBackoff = 0
	p.Deadline = time.Hour
	r, slept := newTestRetry(inner, p)

	assert.Error(t, r.Flush(context.Background()))
	assert.Equal(t, 4, inner.attempts)
	for _, d := range *slept {
		assert.Equal(t, time.Duration(0), d)
	}
}

// A negative backoff must not become a negative sleep or an endless one.
func TestSinkRetry_BoundsNegativeBackoffIsTreatedAsZero(t *testing.T) {
	inner := alwaysFails()
	p := testPolicy()
	p.MaxAttempts = 3
	p.InitialBackoff = -time.Second
	p.MaxBackoff = -time.Second
	p.Deadline = time.Hour
	r, slept := newTestRetry(inner, p)

	assert.Error(t, r.Flush(context.Background()))
	assert.Equal(t, 3, inner.attempts)
	for i, d := range *slept {
		if d < 0 {
			t.Errorf("sleep %d was negative: %v", i, d)
		}
	}
}

// Doubling must not overflow into a negative duration, which would turn a
// long backoff into a tight loop.
func TestSinkRetry_BoundsHugeBackoffDoesNotOverflow(t *testing.T) {
	inner := alwaysFails()
	p := testPolicy()
	p.MaxAttempts = 8
	p.InitialBackoff = time.Duration(1) << 61
	p.MaxBackoff = time.Duration(1) << 62
	p.Deadline = time.Hour
	r, slept := newTestRetry(inner, p)

	assert.Error(t, r.Flush(context.Background()))
	for i, d := range *slept {
		if d < 0 {
			t.Errorf("sleep %d overflowed to %v", i, d)
		}
	}
}

// --- Deadline --------------------------------------------------------------

// A zero deadline leaves no room to wait, so the ladder makes its one attempt
// and reports. It must not skip the flush.
func TestSinkRetry_BoundsZeroDeadlineMakesOneAttempt(t *testing.T) {
	inner := alwaysFails()
	p := testPolicy()
	p.MaxAttempts = 10
	p.Deadline = 0
	r, slept := newTestRetry(inner, p)

	assert.Error(t, r.Flush(context.Background()))
	assert.Equal(t, 1, inner.attempts)
	assert.Equal(t, 0, len(*slept))
}

func TestSinkRetry_BoundsNegativeDeadlineMakesOneAttempt(t *testing.T) {
	inner := alwaysFails()
	p := testPolicy()
	p.MaxAttempts = 10
	p.Deadline = -time.Minute
	r, _ := newTestRetry(inner, p)

	assert.Error(t, r.Flush(context.Background()))
	assert.Equal(t, 1, inner.attempts)
}

// The deadline bounds the total wait. A ladder that would sleep past it stops
// rather than overrunning, because it runs inside the state transaction whose
// clock the window depends on.
func TestSinkRetry_BoundsTotalSleepNeverExceedsTheDeadline(t *testing.T) {
	inner := alwaysFails()
	p := testPolicy()
	p.MaxAttempts = 50
	p.InitialBackoff = 10 * time.Millisecond
	p.MaxBackoff = 10 * time.Millisecond
	p.Deadline = 55 * time.Millisecond
	r, slept := newTestRetry(inner, p)

	assert.Error(t, r.Flush(context.Background()))

	var total time.Duration
	for _, d := range *slept {
		total += d
	}
	if total > p.Deadline {
		t.Errorf("slept %v in total, past the %v deadline", total, p.Deadline)
	}
}

// --- What stopped the ladder ----------------------------------------------

// An operator reading the error has to know whether to raise max_attempts or
// the deadline. Reporting "after N attempts" when the deadline stopped it
// early sends them to the wrong knob.
func TestSinkRetry_BoundsErrorSaysTheDeadlineStoppedIt(t *testing.T) {
	inner := alwaysFails()
	p := testPolicy()
	p.MaxAttempts = 100
	p.InitialBackoff = 20 * time.Millisecond
	p.MaxBackoff = 20 * time.Millisecond
	p.Deadline = 30 * time.Millisecond
	r, _ := newTestRetry(inner, p)

	err := r.Flush(context.Background())

	assert.Error(t, err)
	if strings.Contains(err.Error(), "100 attempts") {
		t.Errorf("the deadline stopped the ladder but the error blames attempts: %v", err)
	}
	assert.That(t, strings.Contains(err.Error(), "deadline"))
}

func TestSinkRetry_BoundsErrorSaysAttemptsWereExhausted(t *testing.T) {
	inner := alwaysFails()
	p := testPolicy()
	p.MaxAttempts = 3
	p.Deadline = time.Hour
	r, _ := newTestRetry(inner, p)

	err := r.Flush(context.Background())

	assert.Error(t, err)
	assert.That(t, strings.Contains(err.Error(), "3 attempts"))
}

// The cause has to survive so an operator sees what the destination said.
func TestSinkRetry_BoundsExhaustedErrorWrapsTheCause(t *testing.T) {
	cause := errors.New("dial tcp 10.0.0.1:9000: connect: connection refused")
	inner := &flakySink{failures: 1 << 30, err: cause}
	p := testPolicy()
	p.MaxAttempts = 2
	r, _ := newTestRetry(inner, p)

	err := r.Flush(context.Background())

	assert.Error(t, err)
	assert.That(t, errors.Is(err, cause))
	assert.That(t, strings.Contains(err.Error(), "connection refused"))
}

// --- Success ---------------------------------------------------------------

// Succeeding on the final attempt is a success, not an exhausted ladder.
func TestSinkRetry_BoundsSuccessOnTheFinalAttempt(t *testing.T) {
	inner := &flakySink{failures: 3, err: errs.New(errs.CodeSinkUnreachable, "refused")}
	p := testPolicy()
	p.MaxAttempts = 4
	r, _ := newTestRetry(inner, p)

	assert.NoError(t, r.Flush(context.Background()))
	assert.Equal(t, 4, inner.attempts)
}

// --- Cancellation ----------------------------------------------------------

// A context cancelled partway through stops the ladder there.
func TestSinkRetry_BoundsCancellationMidLadderStops(t *testing.T) {
	inner := alwaysFails()
	p := testPolicy()
	p.MaxAttempts = 10

	ctx, cancel := context.WithCancel(context.Background())
	r := newRetrying(inner, p)
	r.sleep = func(ctx context.Context, d time.Duration) error {
		if inner.attempts >= 3 {
			cancel()
		}
		return ctx.Err()
	}

	assert.Error(t, r.Flush(ctx))
	assert.Equal(t, 3, inner.attempts)
	cancel()
}

// --- Non-retryable, under every bound --------------------------------------

// A rejected write must stop after one attempt regardless of how generous the
// policy is.
func TestSinkRetry_BoundsNonRetryableStopsUnderAnyPolicy(t *testing.T) {
	for _, p := range []RetryPolicy{
		{MaxAttempts: 0},
		{MaxAttempts: 1},
		{MaxAttempts: 100, InitialBackoff: time.Millisecond, MaxBackoff: time.Millisecond, Deadline: time.Hour},
	} {
		inner := &flakySink{failures: 1 << 30, err: errs.New(errs.CodeSinkWriteFailed, "no such column")}
		r, _ := newTestRetry(inner, p)

		err := r.Flush(context.Background())

		assert.Error(t, err)
		if inner.attempts != 1 {
			t.Errorf("policy %+v: made %d attempts on a non-retryable error", p, inner.attempts)
		}
		// The code is preserved rather than being rewritten as unreachable.
		assert.Equal(t, errs.CodeSinkWriteFailed, errs.CodeOf(err))
	}
}
