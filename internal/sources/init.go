package sources

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/turbolytics/turbine/internal/config"
	"github.com/turbolytics/turbine/internal/core"
	tkafka "github.com/turbolytics/turbine/internal/kafka"
	"go.uber.org/zap"
)

func New(c config.Source, l *zap.Logger) (core.Source, error) {
	switch c.Type {
	case "kafka":
		l.Info(
			"initializing kafka source",
			zap.String("topics", fmt.Sprintf("%v", c.Kafka.Topics)),
			zap.String("group.id", c.Kafka.GroupID),
			zap.String("auto.offset.reset", c.Kafka.AutoOffsetReset),
		)
		consumer, _ := kafka.NewConsumer(&kafka.ConfigMap{
			"bootstrap.servers":  "localhost:9092",
			"group.id":           c.Kafka.GroupID,
			"auto.offset.reset":  c.Kafka.AutoOffsetReset,
			"enable.auto.commit": false,
			"fetch.wait.max.ms":  10,
		})
		k, err := tkafka.NewSource(
			consumer,
			c.Kafka.Topics,
			tkafka.WithLogger(l),
		)
		return k, err

	default:
		return nil, fmt.Errorf("source: %q not supported", c.Type)
	}
}
