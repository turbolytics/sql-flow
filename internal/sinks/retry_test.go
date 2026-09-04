package sinks

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/core"
	"github.com/turbolytics/sql-flow/internal/errs"
	"github.com/zeebo/assert"
)

// flakySink fails a set number of times, then succeeds. It records how many
// attempts it saw so a test can assert the ladder ran the expected length.
type flakySink struct {
	failures int
	err      error
	attempts int
}

func (s *flakySink) WriteTable(ctx context.Context, batch arrow.Table) error { return nil }
func (s *flakySink) Batch() (arrow.Table, error)                             { return nil, nil }

func (s *flakySink) Flush(ctx context.Context) error {
	s.attempts++
	if s.attempts <= s.failures {
		return s.err
	}
	return nil
}

// testPolicy retries quickly so the tests do not sleep. The sleeps are
// recorded rather than taken.
func testPolicy() RetryPolicy {
	return RetryPolicy{
		MaxAttempts:    5,
		InitialBackoff: 10 * time.Millisecond,
		MaxBackoff:     100 * time.Millisecond,
		Deadline:       time.Minute,
	}
}

// newTestRetry wraps inner and captures the backoffs instead of sleeping.
func newTestRetry(inner *flakySink, p RetryPolicy) (*retrying, *[]time.Duration) {
	var slept []time.Duration
	r := newRetrying(inner, p)
	r.sleep = func(ctx context.Context, d time.Duration) error {
		slept = append(slept, d)
		return ctx.Err()
	}
	return r, &slept
}

// An unreachable sink is the case retry exists for: the destination may come
// back, and a restart to recover from a 50ms hiccup costs a cold start and a
// group rejoin.
func TestRetry_RetriesAnUnreachableSink(t *testing.T) {
	inner := &flakySink{failures: 2, err: errs.New(errs.CodeSinkUnreachable, "connection refused")}
	r, slept := newTestRetry(inner, testPolicy())

	assert.NoError(t, r.Flush(context.Background()))

	assert.Equal(t, 3, inner.attempts)
	assert.Equal(t, 2, len(*slept))
}

// A rejected write fails the same way every attempt. Retrying a schema
// mismatch burns the deadline and reports the failure minutes late.
func TestRetry_DoesNotRetryARejectedWrite(t *testing.T) {
	inner := &flakySink{failures: 99, err: errs.New(errs.CodeSinkWriteFailed, "no such column")}
	r, slept := newTestRetry(inner, testPolicy())

	err := r.Flush(context.Background())

	assert.Error(t, err)
	assert.Equal(t, 1, inner.attempts)
	assert.Equal(t, 0, len(*slept))
	assert.Equal(t, errs.CodeSinkWriteFailed, errs.CodeOf(err))
}

// A misconfigured sink is the user's to fix. No number of attempts helps.
func TestRetry_DoesNotRetryAConfigError(t *testing.T) {
	inner := &flakySink{failures: 99, err: errs.New(errs.CodeSinkInvalid, "table is required")}
	r, _ := newTestRetry(inner, testPolicy())

	err := r.Flush(context.Background())

	assert.Error(t, err)
	assert.Equal(t, 1, inner.attempts)
	assert.Equal(t, errs.CodeSinkInvalid, errs.CodeOf(err))
}

// Exhausting the ladder reports the destination as unreachable, which maps to
// a retryable exit code so a supervisor restarts rather than giving up.
func TestRetry_ExhaustedAttemptsReportUnreachable(t *testing.T) {
	inner := &flakySink{failures: 99, err: errs.New(errs.CodeSinkUnreachable, "connection refused")}
	r, _ := newTestRetry(inner, testPolicy())

	err := r.Flush(context.Background())

	assert.Error(t, err)
	assert.Equal(t, 5, inner.attempts)
	assert.Equal(t, errs.CodeSinkUnreachable, errs.CodeOf(err))
	assert.Equal(t, errs.ExitSinkUnreachable, errs.ExitCode(err))
}

// Backoff grows and then stops growing. An unbounded ladder sleeps past the
// flush interval and freezes the window clock.
func TestRetry_BackoffGrowsAndIsCapped(t *testing.T) {
	p := testPolicy()
	p.MaxAttempts = 6
	p.InitialBackoff = 10 * time.Millisecond
	p.MaxBackoff = 40 * time.Millisecond
	inner := &flakySink{failures: 99, err: errs.New(errs.CodeSinkUnreachable, "refused")}
	r, slept := newTestRetry(inner, p)

	r.Flush(context.Background())

	assert.DeepEqual(t, []time.Duration{
		10 * time.Millisecond,
		20 * time.Millisecond,
		40 * time.Millisecond,
		40 * time.Millisecond,
		40 * time.Millisecond,
	}, *slept)
}

// The reason the interface carries a context. A SIGTERM mid-ladder must stop
// the retries, or the graceful drain waits out the whole deadline.
func TestRetry_CancellationStopsTheLadder(t *testing.T) {
	inner := &flakySink{failures: 99, err: errs.New(errs.CodeSinkUnreachable, "refused")}
	r := newRetrying(inner, testPolicy())

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := r.Flush(ctx)

	assert.Error(t, err)
	// One attempt was made before the context was consulted; the ladder does
	// not continue past a cancelled context.
	assert.Equal(t, 1, inner.attempts)
}

// A sink that works costs nothing.
func TestRetry_SuccessDoesNotSleep(t *testing.T) {
	inner := &flakySink{failures: 0}
	r, slept := newTestRetry(inner, testPolicy())

	assert.NoError(t, r.Flush(context.Background()))

	assert.Equal(t, 1, inner.attempts)
	assert.Equal(t, 0, len(*slept))
}

// The deadline bounds the whole ladder, not each attempt. It is what keeps a
// retry from outliving the flush interval.
func TestRetry_DeadlineStopsTheLadder(t *testing.T) {
	p := testPolicy()
	p.MaxAttempts = 100
	p.Deadline = 25 * time.Millisecond
	inner := &flakySink{failures: 99, err: errs.New(errs.CodeSinkUnreachable, "refused")}
	r := newRetrying(inner, p)

	start := time.Now()
	err := r.Flush(context.Background())
	elapsed := time.Since(start)

	assert.Error(t, err)
	assert.That(t, inner.attempts < 100)
	assert.That(t, elapsed < 2*time.Second)
}

// An uncoded error is not evidence the destination is reachable or not.
// Retrying it is the safer default: the alternative fails a pipeline on a
// driver error nobody classified yet.
func TestRetry_RetriesAnUncodedError(t *testing.T) {
	inner := &flakySink{failures: 1, err: errors.New("i/o timeout")}
	r, _ := newTestRetry(inner, testPolicy())

	assert.NoError(t, r.Flush(context.Background()))
	assert.Equal(t, 2, inner.attempts)
}

func isRetrying(s core.Sink) bool {
	_, ok := s.(*retrying)
	return ok
}

// Kafka must not be wrapped. franz-go already retries a produce with its own
// backoff, and a second ladder on top of that one delays the report without
// improving delivery. The sinks reaching nothing remote gain nothing either.
func TestNew_WrapsOnlyTheSinksThatCrossANetwork(t *testing.T) {
	for _, tc := range []struct {
		sink config.Sink
		want bool
	}{
		{config.Sink{Type: "noop"}, false},
		{config.Sink{Type: "console"}, false},
		{config.Sink{Type: ""}, false},
		{config.Sink{Type: "kafka", Kafka: &config.KafkaSink{
			Brokers: []string{"localhost:9092"}, Topic: "t"}}, false},
		{config.Sink{Type: "clickhouse", Clickhouse: &config.ClickhouseSink{
			DSN: "clickhouse://localhost:8123/db", Table: "t"}}, true},
	} {
		s, err := New(tc.sink, nil)
		assert.NoError(t, err)
		if got := isRetrying(s); got != tc.want {
			t.Errorf("sink %q: wrapped=%v, want %v", tc.sink.Type, got, tc.want)
		}
	}
}

// max_attempts: 1 is how an operator turns retrying off.
func TestNew_MaxAttemptsOneDisablesRetrying(t *testing.T) {
	s, err := New(config.Sink{
		Type:       "clickhouse",
		Clickhouse: &config.ClickhouseSink{DSN: "clickhouse://localhost:8123/db", Table: "t"},
		Retry:      &config.SinkRetry{MaxAttempts: 1},
	}, nil)

	assert.NoError(t, err)
	assert.That(t, !isRetrying(s))
}

// An omitted block takes the defaults rather than turning retrying off. One
// refused connection killing the process was the defect, not the baseline.
func TestRetryPolicyFrom_DefaultsAreOn(t *testing.T) {
	p := RetryPolicyFrom(nil)

	assert.That(t, p.Enabled())
	assert.Equal(t, DefaultRetryMaxAttempts, p.MaxAttempts)
	assert.Equal(t, DefaultRetryDeadline, p.Deadline)
}

func TestRetryPolicyFrom_OverridesOnlyWhatIsSet(t *testing.T) {
	p := RetryPolicyFrom(&config.SinkRetry{MaxAttempts: 9, DeadlineSeconds: 3})

	assert.Equal(t, 9, p.MaxAttempts)
	assert.Equal(t, 3*time.Second, p.Deadline)
	// Untouched fields keep their defaults.
	assert.Equal(t, DefaultRetryInitialBackoff, p.InitialBackoff)
	assert.Equal(t, DefaultRetryMaxBackoff, p.MaxBackoff)
}
