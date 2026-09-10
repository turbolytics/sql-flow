package sources

import (
	"fmt"
	"sort"

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

// builders constructs each source type.
//
// A map rather than a switch so Kinds can list it. A registry test holds that
// list equal to integrations.yml, and a source the engine can build but
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

		client, err := kgo.NewClient(opts...)
		if err != nil {
			return nil, errs.Wrap(errs.CodeSourceInternal, err, "kafka client")
		}

		k, err := tkafka.NewSource(client,
			tkafka.WithLogger(l),
			tkafka.WithSeeker(seeker),
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
