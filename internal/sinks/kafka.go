package sinks

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/errs"
	tkafka "github.com/turbolytics/sql-flow/internal/kafka"
	"github.com/twmb/franz-go/pkg/kgo"
)

// recordDeliveryTimeout bounds how long franz-go may hold a record the broker
// has not acknowledged.
//
// franz-go retries a produce indefinitely by default, and no caller bounds the
// wait: the tumbling manager's context is cancellable but carries no deadline,
// and the pipeline's drain strips the deadline with context.WithoutCancel. So
// a flush against a hung broker blocked until the run was cancelled. A
// windowed pipeline stopped publishing every window with nothing logged, and
// shutdown waited for SIGKILL.
//
// Shorter than a supervisor's usual 30s termination grace period, so the drain
// fails and reports rather than being killed mid-flush. Kafka's own
// delivery.timeout.ms default of two minutes is far past that.
const recordDeliveryTimeout = 20 * time.Second

// kafkaSinkError codes a flush failure, teaching sinkError the two franz-go
// errors it cannot recognise.
//
// isUnreachable matches syscall and net errors. A record that expired waiting
// for a broker that never answered carries neither, and a client closed out
// from under a flush carries neither. Both are the same partition a refused
// connection is, reported by a timer or a shutdown rather than by the kernel.
// Coding them as a rejected write exits 1 and labels the error metric for a
// bug, which is what the bare fmt.Errorf here used to do.
func kafkaSinkError(err error, format string, args ...any) error {
	if errors.Is(err, kgo.ErrRecordTimeout) || errors.Is(err, kgo.ErrClientClosed) {
		return errs.Wrap(errs.CodeSinkUnreachable, err, format, args...)
	}
	return sinkError(err, format, args...)
}

// KafkaSink produces one message per result row, JSON encoded, matching the
// Python KafkaSink.
type KafkaSink struct {
	client *kgo.Client
	topic  string

	mu sync.Mutex
	// pending holds the encoded rows that Flush has not yet had acknowledged.
	pending [][]byte
}

// NewKafkaSink builds the sink. Extra client options are appended last, so a
// caller can override what this function set: the conformance test dials
// every broker address through a proxy, which is the only way to fault the
// connection from outside. A testcontainers broker advertises its own mapped
// host port, so a client that merely bootstraps through a proxy reconnects
// around it on the first metadata response.
func NewKafkaSink(conf config.KafkaSink, extra ...kgo.Opt) (*KafkaSink, error) {
	if conf.Topic == "" {
		return nil, fmt.Errorf("kafka sink: topic is required")
	}
	brokers := conf.Brokers
	if len(brokers) == 0 {
		brokers = []string{"localhost:9092"}
	}

	opts := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.AllowAutoTopicCreation(),
		kgo.RecordDeliveryTimeout(recordDeliveryTimeout),
	}

	securityOpts, err := tkafka.SecurityOptions(conf.SecurityProtocol, conf.SSL, conf.SASL)
	if err != nil {
		return nil, fmt.Errorf("kafka sink security: %w", err)
	}
	opts = append(opts, securityOpts...)
	opts = append(opts, extra...)

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("kafka sink client: %w", err)
	}

	return &KafkaSink{client: client, topic: conf.Topic}, nil
}

// WriteTable buffers the encoded rows. Nothing is produced here.
//
// It used to call Produce for every row, so records reached the broker before
// any flush and a flush failure could not hold them back. The pipeline commits
// offsets on what Flush reports, so a sink that delivers earlier than it
// reports leaves the two out of step in the direction that loses rows.
func (s *KafkaSink) WriteTable(ctx context.Context, batch arrow.Table) error {
	rows, err := tableRowsAsJSON(batch)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.pending = append(s.pending, rows...)
	return nil
}

// Flush blocks until every buffered record has been acknowledged, so a
// batch is durable before its source offsets are committed.
//
// It waits on ctx, not on a background context. franz-go retries a produce
// indefinitely by default, so against a broker that stopped answering a
// background context has nothing to stop it: the flush interval elapsing, a
// cancelled run and a SIGTERM all wait for a broker that may never come back,
// and the supervisor kills the process instead. The pipeline's drain already
// hands the sink a context stripped of cancellation, so honouring the context
// here cannot cut the final write short.
func (s *KafkaSink) Flush(ctx context.Context) error {
	s.mu.Lock()
	pending := s.pending
	s.pending = nil
	s.mu.Unlock()

	if len(pending) == 0 {
		return nil
	}

	// One promise per record, indexed so an unacknowledged row can be put back
	// where it was. Results arrive in completion order, which is not the order
	// the rows were written.
	var (
		mu     sync.Mutex
		failed = make(map[int]error, 0)
		acked  = make(map[int]bool, len(pending))
	)
	for i, row := range pending {
		i := i
		s.client.Produce(
			ctx,
			&kgo.Record{Topic: s.topic, Value: row},
			func(_ *kgo.Record, err error) {
				mu.Lock()
				defer mu.Unlock()
				acked[i] = true
				if err != nil {
					failed[i] = err
				}
			},
		)
	}

	// Flush honours the context; Produce does not. franz-go retries a produce
	// indefinitely by default, so against a broker that stopped answering only
	// this call can end the wait -- an elapsed flush interval, a cancelled run
	// and a SIGTERM all reach the sink through ctx. The pipeline's drain hands
	// the sink a context stripped of cancellation, so honouring it here cannot
	// cut the final write short.
	flushErr := s.client.Flush(ctx)

	mu.Lock()
	defer mu.Unlock()

	// A record with no promise yet was still in flight when the context ended.
	// It is not delivered, so it stays pending.
	var (
		keep     [][]byte
		firstErr error
	)
	for i, row := range pending {
		err, didFail := failed[i]
		if !acked[i] || didFail {
			keep = append(keep, row)
			if firstErr == nil {
				if err != nil {
					firstErr = err
				} else {
					firstErr = flushErr
				}
			}
		}
	}

	if len(keep) == 0 {
		return nil
	}

	// At the head, ahead of anything written while the flush was in flight, so
	// a retry produces them in the order they arrived.
	s.mu.Lock()
	s.pending = append(keep, s.pending...)
	s.mu.Unlock()

	if firstErr == nil {
		firstErr = errors.New("not acknowledged")
	}
	return kafkaSinkError(firstErr, "kafka sink: %d of %d rows not acknowledged",
		len(keep), len(pending))
}

func (s *KafkaSink) Close() error {
	s.client.Close()
	return nil
}

// BufferedRows reports the rows this sink is holding that no flush has had
// acknowledged. The pipeline publishes it as sink_buffered_rows, so an
// operator can tell a sink retrying a broker from one that has stopped
// draining.
func (s *KafkaSink) BufferedRows() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.pending)
}
