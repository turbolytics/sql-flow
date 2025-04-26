package kafka

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/turbolytics/turbine/internal/core"
	"log"
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
}

func NewSource(consumer *kafka.Consumer, topics []string, timeout time.Duration) (*Source, error) {
	return &Source{
		consumer:    consumer,
		topics:      topics,
		readTimeout: timeout,
		streamChan:  make(chan core.Message),
		stopChan:    make(chan struct{}),
	}, nil
}

func (k *Source) Start() error {
	return k.consumer.SubscribeTopics(k.topics, nil)
}

func (k *Source) Close() error {
	close(k.stopChan)
	return k.consumer.Close()
}

func (k *Source) Commit() error {
	_, err := k.consumer.Commit() // sync
	return err
}

func (k *Source) Stream() <-chan core.Message {
	go func() {
		for {
			select {
			case <-k.stopChan:
				close(k.streamChan)
				return
			default:
				ev := k.consumer.Poll(int(k.readTimeout.Milliseconds()))
				if ev == nil {
					k.streamChan <- nil
					continue
				}

				switch msg := ev.(type) {
				case *kafka.Message:
					k.streamChan <- &Message{value: msg.Value}
				case kafka.Error:
					if msg.Code() == kafka.ErrPartitionEOF {
						log.Printf("%s reached end at offsetd\n", k.topics)
						k.streamChan <- nil
					} else {
						log.Printf("Kafka error: %v", msg)
						close(k.streamChan)
						return
					}
				default:
					// Ignore other types
				}
			}
		}
	}()
	return k.streamChan
}
