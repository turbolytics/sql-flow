package sources

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/turbolytics/turbine/internal/config"
	"github.com/turbolytics/turbine/internal/core"
	tkafka "github.com/turbolytics/turbine/internal/kafka"
	"time"
)

func New(c config.Source) (core.Source, error) {
	switch c.Type {
	case "kafka":
		consumer, _ := kafka.NewConsumer(&kafka.ConfigMap{
			"bootstrap.servers": "localhost:9092",
			"group.id":          c.Kafka.GroupID,
			"auto.offset.reset": c.Kafka.AutoOffsetReset,
		})
		k, err := tkafka.NewSource(
			consumer,
			c.Kafka.Topics,
			time.Second*5,
		)
		return k, err

	default:
		return nil, fmt.Errorf("source: %q not supported", c.Type)
	}
}
