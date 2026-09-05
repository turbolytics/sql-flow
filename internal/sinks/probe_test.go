package sinks

import (
	"context"
	"errors"
	"syscall"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/errs"
	"github.com/zeebo/assert"
)

// A sink that never dials looks healthy until the first batch reaches it.
// With a long flush interval that is minutes of a pipeline a supervisor calls
// running while nothing can leave it.

type probeSink struct {
	err    error
	probes int
}

func (s *probeSink) WriteTable(ctx context.Context, b arrow.Table) error { return nil }
func (s *probeSink) Flush(ctx context.Context) error                     { return nil }
func (s *probeSink) Batch() (arrow.Table, error)                         { return nil, nil }

func (s *probeSink) Probe(ctx context.Context) error {
	s.probes++
	return s.err
}

func TestSinkRetry_ProbeReachableSinkStarts(t *testing.T) {
	s := &probeSink{}

	assert.NoError(t, probe(context.Background(), s))
	assert.Equal(t, 1, s.probes)
}

// A destination that is not there fails the start with the code whose exit
// status tells a supervisor the dependency may come back.
func TestSinkRetry_ProbeUnreachableSinkFailsTheStart(t *testing.T) {
	s := &probeSink{err: syscall.ECONNREFUSED}

	err := probe(context.Background(), s)

	assert.Error(t, err)
	assert.Equal(t, errs.CodeSinkUnreachable, errs.CodeOf(err))
	assert.Equal(t, errs.ExitSinkUnreachable, errs.ExitCode(err))
	// Retryable: the server may come back, unlike a bad DSN.
	assert.That(t, errs.Retryable(errs.ExitCode(err)))
}

// A server that answers and refuses is the user's to fix. Reporting it as
// unreachable would have a supervisor retry a pipeline that cannot start.
func TestSinkRetry_ProbeAuthFailureIsAUserError(t *testing.T) {
	s := &probeSink{err: errors.New("code: 516, message: Authentication failed")}

	err := probe(context.Background(), s)

	assert.Error(t, err)
	assert.Equal(t, errs.CodeSinkInvalid, errs.CodeOf(err))
	assert.That(t, !errs.Retryable(errs.ExitCode(err)))
}

// A sink with nothing to dial is not probed and must not fail the start.
func TestSinkRetry_ProbeSinksWithNothingToDialAreSkipped(t *testing.T) {
	for _, s := range []config.Sink{
		{Type: "noop"},
		{Type: "console"},
		{Type: ""},
	} {
		built, err := NewWithContext(context.Background(), s, nil)
		assert.NoError(t, err)
		assert.That(t, built != nil)
	}
}

// The probe dials once and does not retry.
//
// A retryable exit code already means the supervisor restarts the pipeline,
// so the restart is the retry. A ladder here would only delay the report of a
// destination that is genuinely down, and it would run before the pipeline has
// consumed anything, where there is nothing to lose by failing fast.
func TestSinkRetry_ProbeDialsOnceAndDoesNotRetry(t *testing.T) {
	s := &probeSink{err: syscall.ECONNREFUSED}

	assert.Error(t, probe(context.Background(), s))
	assert.Equal(t, 1, s.probes)
}

// A cancelled context stops the start rather than dialing anyway.
func TestSinkRetry_ProbeRespectsACancelledContext(t *testing.T) {
	s := &probeSink{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	assert.Error(t, probe(ctx, s))
	assert.Equal(t, 0, s.probes)
}
