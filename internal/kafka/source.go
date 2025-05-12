package kafka

import (
	"errors"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/turbolytics/turbine/internal/core"
	"go.uber.org/zap"
	"sync"
	"time"
)

type Message struct {
	value []byte
}

func (m Message) Value() []byte {
	return m.value
}

type Source struct {
	consumer    *kafka.Consumer
	topics      []string
	readTimeout time.Duration
	streamChan  chan core.Message
	closeOnce   sync.Once

	logger *zap.Logger
}

type Option func(*Source)

func WithReadTimeout(timeout time.Duration) Option {
	return func(s *Source) {
		s.readTimeout = timeout
	}
}

func WithLogger(logger *zap.Logger) Option {
	return func(s *Source) {
		l := logger.Named("source.kafka")
		s.logger = l
	}
}

func NewSource(consumer *kafka.Consumer, topics []string, opts ...Option) (*Source, error) {
	s := &Source{
		consumer:    consumer,
		topics:      topics,
		readTimeout: time.Second * 5,
		streamChan:  make(chan core.Message),

		logger: zap.NewNop(),
	}

	for _, opt := range opts {
		opt(s)
	}

	return s, nil
}

func (k *Source) Start() error {
	k.logger.Info("starting consumer", zap.String("topics", fmt.Sprintf("%v", k.topics)))
	return k.consumer.SubscribeTopics(k.topics, nil)
}

func (k *Source) Close() error {
	k.logger.Info("closing consumer for topics: \n", zap.String("topics", fmt.Sprintf("%v", k.topics)))
	k.closeOnce.Do(func() {
		k.logger.Error("closing consumer in do once")
		close(k.streamChan)
	})
	return k.consumer.Close()
}

func (k *Source) Commit() error {
	_, err := k.consumer.Commit()
	if err != nil {
		k.logger.Error("failed to commit offsets", zap.Error(err))
		var kafkaErr kafka.Error
		if errors.As(err, &kafkaErr) && kafkaErr.Code() == kafka.ErrNoOffset {
			// Handle ErrNoOffset specifically
			fmt.Printf("No offset found for topic %s, ignoring commit error\n", k.topics)
			return nil
		}
	}

	return err
}

func (k *Source) Stream() <-chan core.Message {
	k.logger.Info("starting stream")
	go func() {
		for {
			select {
			/*
				case <-k.stopChan:
					k.logger.Info("stopping stream")
					k.closeOnce.Do(func() {
						close(k.streamChan)
					})
					return

			*/
			default:
				ev := k.consumer.Poll(int(k.readTimeout.Milliseconds()))
				if ev == nil {
					continue
				}

				switch msg := ev.(type) {
				case *kafka.Message:
					k.streamChan <- &Message{value: msg.Value}
				case kafka.PartitionEOF:
					k.logger.Info("%s reached end at offset %v\n",
						zap.String("topic", fmt.Sprintf("%v", k.topics)),
						zap.String("message", fmt.Sprintf("%v", msg)),
					)
				case kafka.Error:
					if msg.Code() == kafka.ErrPartitionEOF {
						k.logger.Info("%s reached end at offset\n",
							zap.String("topic", fmt.Sprintf("%v", k.topics)),
						)
					} else {
						k.logger.Error("Kafka error: %v\n", zap.String("error", msg.Error()))
					}
				default:
					// Ignore other types
				}
			}
		}
	}()
	return k.streamChan
}
