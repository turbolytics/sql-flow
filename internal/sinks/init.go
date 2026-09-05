package sinks

import (
	"context"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/core"
	"github.com/turbolytics/sql-flow/internal/errs"
	"go.opentelemetry.io/otel/metric"
)

type NoopSink struct{}

func (n *NoopSink) WriteTable(ctx context.Context, batch arrow.Table) error {
	// No operation performed
	return nil
}

func (n *NoopSink) Flush(ctx context.Context) error {
	// No operation performed
	return nil
}

func (n *NoopSink) Batch() (arrow.Table, error) {
	// No operation performed, return nil
	return nil, nil
}

// Option configures how a sink is built.
type Option func(*options)

type options struct {
	meterProvider metric.MeterProvider
}

// WithMeterProvider supplies the provider the retry counter records through.
// Without one the counter records nothing, which is what a pipeline started
// with no --metrics wants.
func WithMeterProvider(mp metric.MeterProvider) Option {
	return func(o *options) { o.meterProvider = mp }
}

func New(sink config.Sink, conn adbc.Connection, opts ...Option) (core.Sink, error) {
	return NewWithContext(context.Background(), sink, conn, opts...)
}

// NewWithContext builds a sink and wraps it in a retry ladder where one helps.
//
// Not every sink is wrapped. The Kafka sink hands records to franz-go, which
// already retries a produce with its own backoff; a second ladder on top of
// that one delays the report without improving delivery. The console, noop and
// sqlcommand sinks reach nothing that can be temporarily unavailable -- the
// sqlcommand sink writes through the pipeline's own DuckDB connection, and a
// failure there is not a network blip.
//
// That leaves the sinks that cross a network to somebody else's server.
func NewWithContext(ctx context.Context, sink config.Sink, conn adbc.Connection, opts ...Option) (core.Sink, error) {
	var o options
	for _, opt := range opts {
		opt(&o)
	}

	built, err := buildSink(ctx, sink, conn)
	if err != nil {
		return nil, err
	}

	// Checked before the pipeline consumes anything, so a destination that is
	// not there fails the start rather than the first flush.
	if err := probe(ctx, built); err != nil {
		return nil, err
	}

	policy := RetryPolicyFrom(sink.Retry)
	if !retriesHelp(sink.Type) || !policy.Enabled() {
		return built, nil
	}

	r := newRetrying(built, policy)
	r.onRetry = retryCounter(o.meterProvider, sink.Type)
	return r, nil
}

// retriesHelp reports whether a retry ladder belongs around a sink type.
//
// Only the sinks that cross a network to somebody else's server. Kafka is
// excluded on purpose: it hands records to franz-go, which already retries a
// produce with its own backoff, and a second ladder on top of that one delays
// the report without improving delivery. Console, noop and sqlcommand reach
// nothing that can be temporarily unavailable -- sqlcommand writes through the
// pipeline's own DuckDB connection, and a failure there is not a blip.
//
// Kept as a function of the type alone so the policy is testable without
// building a sink, which would dial.
func retriesHelp(sinkType string) bool {
	switch sinkType {
	case "clickhouse", "iceberg":
		return true
	default:
		return false
	}
}

// buildSink constructs the sink itself. Whether a retry ladder belongs around
// it is retriesHelp's decision, not this one's.
func buildSink(ctx context.Context, sink config.Sink, conn adbc.Connection) (core.Sink, error) {
	switch sink.Type {
	case "noop":
		return &NoopSink{}, nil

	case "console", "":
		// The Python engine falls back to console for an unset type.
		return NewConsoleSink(), nil

	case "kafka":
		if sink.Kafka == nil {
			return nil, errs.New(errs.CodeSinkInvalid, "sink: kafka sink requires a kafka block")
		}
		return NewKafkaSink(*sink.Kafka)

	case "sqlcommand":
		if sink.SQLCommand == nil {
			return nil, errs.New(errs.CodeSinkInvalid, "sink: sqlcommand sink requires a sqlcommand block")
		}
		return NewSQLCommandSink(conn, sink.SQLCommand.SQL, sink.SQLCommand.Substitutions)

	case "clickhouse":
		if sink.Clickhouse == nil {
			return nil, errs.New(errs.CodeSinkInvalid, "sink: clickhouse sink requires a clickhouse block")
		}
		return NewClickhouseSink(*sink.Clickhouse)

	case "iceberg":
		if sink.Iceberg == nil {
			return nil, errs.New(errs.CodeSinkInvalid, "sink: iceberg sink requires an iceberg block")
		}
		return NewIcebergSink(ctx, sink.Iceberg.CatalogName, sink.Iceberg.TableName)

	default:
		return nil, errs.New(errs.CodeSinkInvalid, "sink: %q not supported", sink.Type)
	}
}
