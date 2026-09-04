package sources

import (
	"fmt"
	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/core"
	"github.com/turbolytics/sql-flow/internal/errs"
	tkafka "github.com/turbolytics/sql-flow/internal/kafka"
	"github.com/turbolytics/sql-flow/internal/webhook"
	"github.com/turbolytics/sql-flow/internal/websocket"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
)

// New builds the configured source. A nil meter provider leaves sources that
// record metrics recording nothing.
func New(c config.Source, l *zap.Logger, mp metric.MeterProvider) (core.Source, error) {
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

		// Built before the client because it is a client option, and handed
		// to the source afterwards so SeekTo can fill it in. With no marks
		// set it leaves the group's own offsets alone.
		seeker := tkafka.NewOffsetSeeker()

		opts := []kgo.Opt{
			kgo.SeedBrokers(brokers...),
			kgo.ConsumerGroup(c.Kafka.GroupID),
			kgo.ConsumeTopics(c.Kafka.Topics...),
			kgo.ConsumeResetOffset(resetOffset),
			kgo.DisableAutoCommit(),
			kgo.AdjustFetchOffsetsFn(seeker.Adjust),
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
			return nil, errs.Wrap(errs.CodeSourceInternal, err, "kafka client")
		}

		k, err := tkafka.NewSource(client, tkafka.WithLogger(l), tkafka.WithSeeker(seeker))
		return k, err

	case "websocket":
		if c.Websocket == nil {
			return nil, errs.New(errs.CodeSourceInvalid, "websocket source: missing websocket configuration")
		}
		l.Info("initializing websocket source", zap.String("uri", c.Websocket.URI))

		return websocket.NewSource(c.Websocket.URI, websocket.WithLogger(l))

	case "webhook":
		opts := []webhook.Option{
			webhook.WithLogger(l),
			webhook.WithMeterProvider(mp),
		}
		// Only a configured signature type turns validation on, so a webhook
		// block that carries an hmac stanza but no signature_type accepts
		// unvalidated bodies, as in the Python engine.
		if c.Webhook != nil && c.Webhook.SignatureType == "hmac" && c.Webhook.HMAC != nil {
			l.Info("initializing webhook hmac validation",
				zap.String("header", c.Webhook.HMAC.Header),
			)
			opts = append(opts, webhook.WithHMAC(&webhook.HMAC{
				Header: c.Webhook.HMAC.Header,
				SigKey: c.Webhook.HMAC.SigKey,
				Secret: c.Webhook.HMAC.Secret,
			}))
		}

		return webhook.NewSource(opts...)

	default:
		return nil, errs.New(errs.CodeSourceInvalid, "source: %q not supported", c.Type)
	}
}
