package sinks

import (
	"context"
	"errors"
	"io"
	"net"
	"syscall"

	"github.com/turbolytics/sql-flow/internal/errs"
)

// isUnreachable reports whether an error means the pipeline could not talk to
// the destination, as opposed to the destination answering and refusing.
//
// The distinction drives the retry ladder. A refused connection may succeed on
// the next attempt; a rejected column never will. Before this, the ClickHouse
// sink coded every failure from PrepareBatch as a rejected write -- and
// PrepareBatch dials, so a refused connection was classified as the one thing
// retry deliberately does not retry.
func isUnreachable(err error) bool {
	if err == nil {
		return false
	}

	// Timeouts first: a net.Error carries its own verdict, and a driver's own
	// timeout type satisfies the interface without being a syscall error.
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}

	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return true
	}

	var opErr *net.OpError
	if errors.As(err, &opErr) {
		return true
	}

	for _, target := range []error{
		syscall.ECONNREFUSED,
		syscall.ECONNRESET,
		syscall.ECONNABORTED,
		syscall.EHOSTUNREACH,
		syscall.ENETUNREACH,
		syscall.ENETDOWN,
		syscall.EPIPE,
		syscall.ETIMEDOUT,
		io.EOF,
		io.ErrUnexpectedEOF,
		context.DeadlineExceeded,
	} {
		if errors.Is(err, target) {
			return true
		}
	}

	return false
}

// sinkError wraps a sink failure with the code its cause deserves, so one call
// site gets both cases right.
func sinkError(err error, format string, args ...any) error {
	code := errs.CodeSinkWriteFailed
	if isUnreachable(err) {
		code = errs.CodeSinkUnreachable
	}
	return errs.Wrap(code, err, format, args...)
}
