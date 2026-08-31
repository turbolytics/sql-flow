package sources

import (
	"fmt"
	"github.com/turbolytics/turbine/internal/config"
	"github.com/turbolytics/turbine/internal/core"
	tkafka "github.com/turbolytics/turbine/internal/kafka"
	"github.com/twmb/franz-go/pkg/kgo"
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
		brokers := []string{"localhost:9092"}
		if len(c.Kafka.Brokers) > 0 {
			brokers = c.Kafka.Brokers
		}

		resetOffset := kgo.NewOffset().AtStart()
		if c.Kafka.AutoOffsetReset == "latest" {
			resetOffset = kgo.NewOffset().AtEnd()
		}

		opts := []kgo.Opt{
			kgo.SeedBrokers(brokers...),
			kgo.ConsumerGroup(c.Kafka.GroupID),
			kgo.ConsumeTopics(c.Kafka.Topics...),
			kgo.ConsumeResetOffset(resetOffset),
			kgo.DisableAutoCommit(),
			kgo.FetchMaxPartitionBytes(10 << 20), // 10MB per partition
			kgo.FetchMaxBytes(100 << 20),         // 100MB total per broker
		}

		securityOpts, err := tkafka.SecurityOptions(
			c.Kafka.SecurityProtocol,
			c.Kafka.SSL,
			c.Kafka.SASL,
		)
		if err != nil {
			return nil, fmt.Errorf("kafka source security: %w", err)
		}
		opts = append(opts, securityOpts...)

		client, err := kgo.NewClient(opts...)
		if err != nil {
			return nil, fmt.Errorf("kafka client: %w", err)
		}

		k, err := tkafka.NewSource(client, tkafka.WithLogger(l))
		return k, err

	default:
		return nil, fmt.Errorf("source: %q not supported", c.Type)
	}
}
