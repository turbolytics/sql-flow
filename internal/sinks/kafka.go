package sinks

import (
	"context"
	"fmt"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/turbolytics/sql-flow/internal/config"
	tkafka "github.com/turbolytics/sql-flow/internal/kafka"
	"github.com/twmb/franz-go/pkg/kgo"
)

// KafkaSink produces one message per result row, JSON encoded, matching the
// Python KafkaSink.
type KafkaSink struct {
	client *kgo.Client
	topic  string

	mu    sync.Mutex
	batch arrow.Table
	errs  []error
}

func NewKafkaSink(conf config.KafkaSink) (*KafkaSink, error) {
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
	}

	securityOpts, err := tkafka.SecurityOptions(conf.SecurityProtocol, conf.SSL, conf.SASL)
	if err != nil {
		return nil, fmt.Errorf("kafka sink security: %w", err)
	}
	opts = append(opts, securityOpts...)

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("kafka sink client: %w", err)
	}

	return &KafkaSink{client: client, topic: conf.Topic}, nil
}

func (s *KafkaSink) WriteTable(ctx context.Context, batch arrow.Table) error {
	rows, err := tableRowsAsJSON(batch)
	if err != nil {
		return err
	}

	s.mu.Lock()
	s.batch = batch
	s.mu.Unlock()

	for _, row := range rows {
		s.client.Produce(
			context.Background(),
			&kgo.Record{Topic: s.topic, Value: row},
			func(_ *kgo.Record, err error) {
				if err != nil {
					s.mu.Lock()
					s.errs = append(s.errs, err)
					s.mu.Unlock()
				}
			},
		)
	}
	return nil
}

// Flush blocks until every buffered record has been acknowledged, so a
// batch is durable before its source offsets are committed.
func (s *KafkaSink) Flush(ctx context.Context) error {
	if err := s.client.Flush(context.Background()); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.errs) > 0 {
		err := fmt.Errorf("kafka sink: %d produce error(s), first: %w", len(s.errs), s.errs[0])
		s.errs = nil
		return err
	}
	return nil
}

func (s *KafkaSink) Batch() (arrow.Table, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.batch, nil
}

func (s *KafkaSink) Close() error {
	s.client.Close()
	return nil
}
