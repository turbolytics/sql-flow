package kafka

import (
	"errors"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/turbolytics/turbine/internal/core"
	"log"
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
			log.Printf("No offset found for topic %s, ignoring commit error", k.topics)
			return nil
		}
	}

	return err
}

func (k *Source) Stream() <-chan core.Message {
	go func() {
		for {
			select {
			case <-k.stopChan:
				k.closeOnce.Do(func() {
					close(k.streamChan)
				})
				return
			default:
				ev := k.consumer.Poll(int(k.readTimeout.Milliseconds()))
				fmt.Println("here", ev)
				if ev == nil {
					continue
				}

				switch msg := ev.(type) {
				case *kafka.Message:
					k.streamChan <- &Message{value: msg.Value}
				case kafka.PartitionEOF:
					log.Printf("%s reached end at offset %v\n", k.topics, msg)
				case kafka.Error:
					if msg.Code() == kafka.ErrPartitionEOF {
						log.Printf("%s reached end at offsetd\n", k.topics)
					} else {
						log.Printf("Kafka error: %v", msg)
						k.closeOnce.Do(func() {
							close(k.streamChan)
						})
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
