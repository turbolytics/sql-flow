package sinks

// The Kafka sink needs no broker to be tested where it matters. Construction
// validates config without dialing, and the context tests below are about what
// the sink does when the destination is *not* there -- which is exactly the
// case a live broker cannot produce.

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/turbolytics/sql-flow/internal/errs"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/zeebo/assert"
)

// unreachableBroker is a port nothing listens on. Port 1 is privileged and
// unbound, so a dial fails at once rather than hanging on a firewall.
const unreachableBroker = "127.0.0.1:1"

func newUnreachableKafkaSink(t *testing.T) *KafkaSink {
	t.Helper()

	s, err := NewKafkaSink(config.KafkaSink{
		Brokers: []string{unreachableBroker},
		Topic:   "sink-test",
	})
	assert.NoError(t, err)
	t.Cleanup(func() { s.Close() })
	return s
}

// flushWithin runs Flush off the test goroutine and fails if it has not
// returned within limit.
//
// Calling Flush directly would be simpler and much worse: a sink that stopped
// honouring its context blocks forever, so the regression these tests exist to
// catch would surface as a ten-minute package timeout with no failing test
// name. Here it surfaces in a second, pointing at the sink.
func flushWithin(t *testing.T, s *KafkaSink, ctx context.Context, limit time.Duration) error {
	t.Helper()

	done := make(chan error, 1)
	go func() { done <- s.Flush(ctx) }()

	select {
	case err := <-done:
		return err
	case <-time.After(limit):
		t.Fatalf("Flush did not return within %s, so it ignored its context", limit)
		return nil
	}
}

func TestSinkKafka_NewRequiresATopic(t *testing.T) {
	coverage.Covers(t, "sink.kafka")
	_, err := NewKafkaSink(config.KafkaSink{Brokers: []string{unreachableBroker}})
	assert.Error(t, err)
}

// A security block turbine cannot honour must fail the start. Building the
// client anyway produces a sink that connects in the clear, which is worse
// than not starting.
func TestSinkKafka_NewRejectsAnUnknownSecurityProtocol(t *testing.T) {
	coverage.Covers(t, "sink.kafka")
	_, err := NewKafkaSink(config.KafkaSink{
		Brokers:          []string{unreachableBroker},
		Topic:            "sink-test",
		SecurityProtocol: "SASL_CARRIER_PIGEON",
	})
	assert.Error(t, err)
}

// Flush must return when its context is done.
//
// The pipeline's drain already hands the sink a context stripped of
// cancellation, precisely so a SIGTERM cannot fail the write the drain exists
// to finish. That care is wasted on a sink that ignores the context it is
// given: every other caller -- a flush interval that elapsed, a cancelled run
// -- has no way to stop a flush against a broker that is not answering, and
// franz-go retries a produce indefinitely by default. The process hangs on
// shutdown and a supervisor eventually kills it.
func TestSinkKafka_FlushHonoursItsContext(t *testing.T) {
	coverage.Covers(t, "sink.kafka")
	s := newUnreachableKafkaSink(t)

	table := newTestTable(t, []string{"nyc"}, []int64{1})
	defer table.Release()
	assert.NoError(t, s.WriteTable(context.Background(), table))

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	err := flushWithin(t, s, ctx, 10*time.Second)
	assert.Error(t, err)
	assert.That(t, errors.Is(err, context.DeadlineExceeded))

	// And the row is still there for the next attempt. A flush that gives up
	// must not also discard what it failed to send.
	s.mu.Lock()
	pending := len(s.pending)
	s.mu.Unlock()
	assert.Equal(t, 1, pending)
}

// WriteTable reaches nothing, so its context governs nothing.
//
// This replaces a test asserting that a cancelled write context failed those
// records. It passed for the wrong reason: franz-go watches a Produce context
// only while a topic is unresolved, and the topic could never resolve against
// 127.0.0.1:1. The sink now buffers on the way in, so no context can fail a
// write, and the guarantee that matters moved to Flush.
func TestSinkKafka_WriteTableBuffersWhateverItsContextSays(t *testing.T) {
	coverage.Covers(t, "sink.kafka")
	s := newUnreachableKafkaSink(t)

	table := newTestTable(t, []string{"nyc"}, []int64{1})
	defer table.Release()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	assert.NoError(t, s.WriteTable(ctx, table))

	s.mu.Lock()
	pending := len(s.pending)
	s.mu.Unlock()
	assert.Equal(t, 1, pending)
}

// A flush with no deadline must still return.
//
// Every production caller passes one. The tumbling manager's context is
// cancellable but carries no deadline (run/root.go), and the pipeline's drain
// strips the deadline along with the cancellation via context.WithoutCancel.
// If only the context could stop a flush, an outage would stop a windowed
// pipeline publishing with nothing logged, and shutdown would wait for the
// supervisor to kill the process.
func TestSinkKafka_FlushReturnsWithoutADeadline(t *testing.T) {
	coverage.Covers(t, "sink.kafka")

	// franz-go rejects a record timeout below a second, so this is the floor
	// rather than a round number.
	s, err := NewKafkaSink(config.KafkaSink{
		Brokers: []string{unreachableBroker},
		Topic:   "sink-test",
	}, kgo.RecordDeliveryTimeout(time.Second))
	assert.NoError(t, err)
	t.Cleanup(func() { s.Close() })

	table := newTestTable(t, []string{"nyc"}, []int64{1})
	defer table.Release()
	assert.NoError(t, s.WriteTable(context.Background(), table))

	assert.Error(t, flushWithin(t, s, context.Background(), 10*time.Second))

	// And the row is still owed, so the next attempt re-sends it.
	assert.Equal(t, 1, s.BufferedRows())
}

// The default has to be set, or the test above only proves the option exists.
func TestSinkKafka_HasADeliveryTimeoutByDefault(t *testing.T) {
	coverage.Covers(t, "sink.kafka")
	s := newUnreachableKafkaSink(t)

	table := newTestTable(t, []string{"nyc"}, []int64{1})
	defer table.Release()
	assert.NoError(t, s.WriteTable(context.Background(), table))

	// Longer than recordDeliveryTimeout, so a sink with no default hangs here.
	assert.Error(t, flushWithin(t, s, context.Background(),
		recordDeliveryTimeout+10*time.Second))
}

// A dead broker must exit 12, not 1.
//
// errs.CodeOf falls back to system.internal.unexpected for an uncoded error, so
// a bare fmt.Errorf tells a supervisor the pipeline hit a bug and labels the
// error metric the same way. Both are wrong, and both are what an operator
// reads first.
func TestSinkKafka_FlushCodesAnUnreachableBroker(t *testing.T) {
	coverage.Covers(t, "sink.kafka")
	s := newUnreachableKafkaSink(t)

	table := newTestTable(t, []string{"nyc"}, []int64{1})
	defer table.Release()
	assert.NoError(t, s.WriteTable(context.Background(), table))

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	err := flushWithin(t, s, ctx, 10*time.Second)
	assert.Error(t, err)
	assert.Equal(t, errs.CodeSinkUnreachable, errs.CodeOf(err))
	assert.Equal(t, errs.ExitSinkUnreachable, errs.ExitCode(err))
}

// A record that expired waiting for a broker that never answered is the same
// failure as a refused connection.
//
// isUnreachable matches syscall and net errors and has no reason to know
// franz-go's sentinel, so the sink classifies it. Coding it as a rejected write
// would exit 1 again -- and the delivery timeout makes this the common error
// against a broker that is down.
func TestSinkKafka_ARecordTimeoutIsUnreachable(t *testing.T) {
	coverage.Covers(t, "sink.kafka")
	err := kafkaSinkError(kgo.ErrRecordTimeout, "kafka sink: %d rows", 1)

	assert.Equal(t, errs.CodeSinkUnreachable, errs.CodeOf(err))
	assert.Equal(t, errs.ExitSinkUnreachable, errs.ExitCode(err))
}

// Produce errors must not be swallowed. The pipeline commits its offsets only
// after a flush returns clean, so a flush that hides a failed produce loses
// the batch and every offset behind it.
func TestSinkKafka_FlushReportsProduceErrors(t *testing.T) {
	coverage.Covers(t, "sink.kafka")
	s := newUnreachableKafkaSink(t)

	table := newTestTable(t, []string{"nyc", "sfo"}, []int64{1, 2})
	defer table.Release()

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	assert.NoError(t, s.WriteTable(context.Background(), table))
	assert.Error(t, flushWithin(t, s, ctx, 10*time.Second))
}
