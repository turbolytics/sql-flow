package sinks

// Every real sink buffers in WriteTable and drains that buffer in Flush. The
// retry ladder calls Flush again after a failure, and the second call finds
// the buffer already empty, so it reports success for a batch that was never
// delivered. The pipeline then commits the offsets and the rows are gone with
// no error anywhere.
//
// flakySink in retry_test.go cannot catch this: its Flush holds no buffer, so
// a retry legitimately succeeds. These tests model the drain.

import (
	"context"
	"errors"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/zeebo/assert"
)

// bufferingSink is the shape every real sink has: rows accumulate in
// WriteTable and are drained by Flush, whether or not the delivery succeeds.
type bufferingSink struct {
	buffered  int // rows handed to WriteTable and not yet delivered
	delivered int // rows that actually reached the destination
	attempts  int
	failures  int // fail this many Flush calls before succeeding
	err       error
}

func (s *bufferingSink) WriteTable(ctx context.Context, batch arrow.Table) error {
	s.buffered++
	return nil
}

func (s *bufferingSink) Batch() (arrow.Table, error) { return nil, nil }

func (s *bufferingSink) Flush(ctx context.Context) error {
	s.attempts++

	// The drain happens first, exactly as ClickhouseSink.Flush takes
	// s.tables and sets it to nil before attempting the insert.
	pending := s.buffered
	s.buffered = 0

	if pending == 0 {
		return nil
	}
	if s.attempts <= s.failures {
		return s.err
	}
	s.delivered += pending
	return nil
}

// A retried flush must not report success for a batch it never delivered.
func TestRetry_RetriedFlushDoesNotLoseTheBatch(t *testing.T) {
	sink := &bufferingSink{failures: 1, err: errors.New("connection reset by peer")}
	r := newRetrying(sink, testPolicy())

	ctx := context.Background()
	assert.NoError(t, r.WriteTable(ctx, nil))

	err := r.Flush(ctx)

	// Either the ladder delivers the batch, or it reports the failure. What it
	// must never do is return nil having delivered nothing.
	if err == nil {
		assert.Equal(t, 1, sink.delivered)
	}
}

// The same defect stated as the caller sees it: a flush that returns nil is a
// promise that the rows reached the destination.
func TestRetry_SuccessfulFlushMeansRowsWereDelivered(t *testing.T) {
	sink := &bufferingSink{failures: 1, err: errors.New("connection reset by peer")}
	r := newRetrying(sink, testPolicy())

	ctx := context.Background()
	assert.NoError(t, r.WriteTable(ctx, nil))

	if err := r.Flush(ctx); err == nil {
		assert.That(t, sink.delivered > 0)
	}
}
