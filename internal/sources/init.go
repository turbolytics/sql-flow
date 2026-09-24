package sources

import (
	"fmt"
	"sort"

	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/core"
	"github.com/turbolytics/sql-flow/internal/errs"
	tkafka "github.com/turbolytics/sql-flow/internal/kafka"
	tmqtt "github.com/turbolytics/sql-flow/internal/mqtt"
	"github.com/turbolytics/sql-flow/internal/webhook"
	"github.com/turbolytics/sql-flow/internal/websocket"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
)

// builders constructs each source type.
//
// A map rather than a switch so Kinds can list it. A registry test holds that
// list equal to the registry, and a source the engine can build but
// nothing declares has no invariant cells at all.
var builders = map[string]func(c config.Source, l *zap.Logger, mp metric.MeterProvider) (core.Source, error){
	"kafka": func(c config.Source, l *zap.Logger, _ metric.MeterProvider) (core.Source, error) {
		// Bounds the read-ahead. An absent block is the defaults, and a
		// bound that cannot hold fails here, at startup, before the
		// pipeline consumes anything.
		fetch, err := c.Kafka.Fetch.Resolved()
		if err != nil {
			return nil, err
		}

		l.Info(
			"initializing kafka source",
			zap.String("topics", fmt.Sprintf("%v", c.Kafka.Topics)),
			zap.String("group.id", c.Kafka.GroupID),
			zap.String("auto.offset.reset", c.Kafka.AutoOffsetReset),
			zap.Int("fetch.max_bytes", fetch.MaxBytes),
			zap.Int("fetch.max_partition_bytes", fetch.MaxPartitionBytes),
			zap.Int("fetch.prefetch", fetch.Prefetch),
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
		// A client option too, for the same reason: the rebalance callbacks
		// are how the pipeline stops reporting lag for a partition that moved
		// to another instance.
		partitions := tkafka.NewPartitionEvents()

		opts := []kgo.Opt{
			kgo.SeedBrokers(brokers...),
			kgo.ConsumerGroup(c.Kafka.GroupID),
			kgo.ConsumeTopics(c.Kafka.Topics...),
			kgo.ConsumeResetOffset(resetOffset),
			kgo.DisableAutoCommit(),
			kgo.AdjustFetchOffsetsFn(seeker.Adjust),
			kgo.FetchMaxPartitionBytes(int32(fetch.MaxPartitionBytes)),
			kgo.FetchMaxBytes(int32(fetch.MaxBytes)),
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
		opts = append(opts, partitions.ClientOptions()...)

		client, err := kgo.NewClient(opts...)
		if err != nil {
			return nil, errs.Wrap(errs.CodeSourceInternal, err, "kafka client")
		}

		k, err := tkafka.NewSource(client,
			tkafka.WithLogger(l),
			tkafka.WithSeeker(seeker),
			tkafka.WithPartitionEvents(partitions),
			tkafka.WithChannelBuffer(fetch.Prefetch),
		)
		return k, err
	},

	"websocket": func(c config.Source, l *zap.Logger, _ metric.MeterProvider) (core.Source, error) {
		if c.Websocket == nil {
			return nil, errs.New(errs.CodeSourceInvalid, "websocket source: missing websocket configuration")
		}
		l.Info("initializing websocket source", zap.String("uri", c.Websocket.URI))

		return websocket.NewSource(c.Websocket.URI, websocket.WithLogger(l))
	},

	"webhook": func(c config.Source, l *zap.Logger, mp metric.MeterProvider) (core.Source, error) {
		maxBody, err := c.Webhook.ResolvedMaxBodyBytes()
		if err != nil {
			return nil, err
		}
		maxConns, err := c.Webhook.ResolvedMaxConnections()
		if err != nil {
			return nil, err
		}
		addr, err := c.Webhook.ResolvedAddr()
		if err != nil {
			return nil, err
		}
		l.Info("initializing webhook source",
			zap.String("addr", addr),
			zap.Int64("max_body_bytes", maxBody),
			zap.Int("max_connections", maxConns),
		)
		opts := []webhook.Option{
			webhook.WithLogger(l),
			webhook.WithMeterProvider(mp),
			webhook.WithAddr(addr),
			webhook.WithMaxBodyBytes(maxBody),
			webhook.WithMaxConnections(maxConns),
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
	},

	"mqtt": func(c config.Source, l *zap.Logger, _ metric.MeterProvider) (core.Source, error) {
		r, err := c.Mqtt.Resolved()
		if err != nil {
			return nil, err
		}
		l.Info("initializing mqtt source",
			zap.String("broker", r.Broker.Redacted()),
			zap.String("client_id", r.ClientID),
			zap.Strings("topics", r.Topics),
			zap.Uint32("session_expiry_seconds", r.SessionExpiry),
			zap.Uint16("receive_maximum", r.ReceiveMaximum),
		)
		return tmqtt.NewSource(tmqtt.Config{
			Broker:         r.Broker,
			ClientID:       r.ClientID,
			Topics:         r.Topics,
			SessionExpiry:  r.SessionExpiry,
			ReceiveMaximum: r.ReceiveMaximum,
		}, tmqtt.WithLogger(l))
	},
}

// Kinds lists every source type the engine can build, sorted.
func Kinds() []string {
	out := make([]string, 0, len(builders))
	for kind := range builders {
		out = append(out, kind)
	}
	sort.Strings(out)
	return out
}

// New builds the configured source. A nil meter provider leaves sources that
// record metrics recording nothing.
func New(c config.Source, l *zap.Logger, mp metric.MeterProvider) (core.Source, error) {
	build, ok := builders[c.Type]
	if !ok {
		return nil, errs.New(errs.CodeSourceInvalid, "source: %q not supported", c.Type)
	}
	return build(c, l, mp)
}
