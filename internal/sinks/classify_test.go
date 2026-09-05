package sinks

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"syscall"
	"testing"

	"github.com/turbolytics/sql-flow/internal/errs"
	"github.com/zeebo/assert"
)

// Retry keys off the error code, so a driver failure that is really a network
// problem has to be coded as one. Coding a refused connection as "the server
// rejected the write" means the ladder never runs for the case it exists for.

type fakeTimeout struct{}

func (fakeTimeout) Error() string   { return "i/o timeout" }
func (fakeTimeout) Timeout() bool   { return true }
func (fakeTimeout) Temporary() bool { return true }

func TestSinkRetry_ClassifyNetworkFailuresAreUnreachable(t *testing.T) {
	for name, err := range map[string]error{
		"connection refused":  syscall.ECONNREFUSED,
		"connection reset":    syscall.ECONNRESET,
		"host unreachable":    syscall.EHOSTUNREACH,
		"network unreachable": syscall.ENETUNREACH,
		"broken pipe":         syscall.EPIPE,
		"eof":                 io.EOF,
		"unexpected eof":      io.ErrUnexpectedEOF,
		"dns failure":         &net.DNSError{Err: "no such host", Name: "clickhouse.invalid"},
		"net op error":        &net.OpError{Op: "dial", Err: syscall.ECONNREFUSED},
		"timeout":             fakeTimeout{},
		"deadline exceeded":   context.DeadlineExceeded,
		// Wrapped the way a driver hands it back.
		"wrapped refusal": fmt.Errorf("clickhouse: %w", syscall.ECONNREFUSED),
	} {
		if !isUnreachable(err) {
			t.Errorf("%s: should be classified unreachable, was not (%v)", name, err)
		}
	}
}

// A server that answered and rejected the write is not unreachable. Retrying
// a schema mismatch burns the deadline and reports it late.
func TestSinkRetry_ClassifyServerRejectionsAreNotUnreachable(t *testing.T) {
	for name, err := range map[string]error{
		"missing column": errors.New("code: 16, message: No such column city in table events"),
		"type mismatch":  errors.New("code: 53, message: Type mismatch"),
		"plain":          errors.New("something went wrong"),
	} {
		if isUnreachable(err) {
			t.Errorf("%s: should not be classified unreachable", name)
		}
	}
}

func TestSinkRetry_ClassifyNilIsNotUnreachable(t *testing.T) {
	assert.That(t, !isUnreachable(nil))
}

// sinkError applies the classification, so a call site can wrap once and get
// the right code either way.
func TestSinkRetry_SinkErrorCodesByCause(t *testing.T) {
	network := sinkError(syscall.ECONNREFUSED, "clickhouse sink: prepare batch")
	assert.Equal(t, errs.CodeSinkUnreachable, errs.CodeOf(network))
	assert.That(t, errors.Is(network, syscall.ECONNREFUSED))

	rejected := sinkError(errors.New("no such column"), "clickhouse sink: append")
	assert.Equal(t, errs.CodeSinkWriteFailed, errs.CodeOf(rejected))
}

// The classification has to reach the ladder: an unreachable cause is
// retried, a rejection is not.
func TestSinkRetry_SinkErrorDrivesTheRetryDecision(t *testing.T) {
	assert.That(t, retryable(sinkError(syscall.ECONNREFUSED, "prepare")))
	assert.That(t, !retryable(sinkError(errors.New("no such column"), "append")))
}
