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
	stopChan    chan struct{}
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
		s.logger = logger
	}
}

func NewSource(consumer *kafka.Consumer, topics []string, opts ...Option) (*Source, error) {
	s := &Source{
		consumer:    consumer,
		topics:      topics,
		readTimeout: time.Second * 5,
		streamChan:  make(chan core.Message),
		stopChan:    make(chan struct{}),

		logger: zap.NewNop(),
	}

	for _, opt := range opts {
		opt(s)
	}

	return s, nil
}

func (k *Source) Start() error {
	return k.consumer.SubscribeTopics(k.topics, nil)
}

func (k *Source) Close() error {
	fmt.Printf("closing consumer for topics: %s\n", k.topics)
	k.closeOnce.Do(func() {
		close(k.stopChan)
		close(k.streamChan)
	})
	return k.consumer.Close()
}

func (k *Source) Commit() error {
	_, err := k.consumer.Commit()
	if err != nil {
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
	fmt.Println("starting stream")
	go func() {
		for {
			select {
			case <-k.stopChan:
				fmt.Println("Stopping stream")
				k.closeOnce.Do(func() {
					close(k.streamChan)
				})
				return
			default:
				ev := k.consumer.Poll(int(k.readTimeout.Milliseconds()))
				if ev == nil {
					continue
				}

				switch msg := ev.(type) {
				case *kafka.Message:
					k.streamChan <- &Message{value: msg.Value}
				case kafka.PartitionEOF:
					fmt.Printf("%s reached end at offset %v\n", k.topics, msg)
				case kafka.Error:
					if msg.Code() == kafka.ErrPartitionEOF {
						fmt.Printf("%s reached end at offsetd\n", k.topics)
					} else {
						fmt.Printf("Kafka error: %v\n", msg)
					}
				default:
					// Ignore other types
				}
			}
		}
	}()
	return k.streamChan
}
