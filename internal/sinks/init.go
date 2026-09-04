package sinks

import (
	"context"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/core"
	"github.com/turbolytics/sql-flow/internal/errs"
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

func New(sink config.Sink, conn adbc.Connection) (core.Sink, error) {
	return NewWithContext(context.Background(), sink, conn)
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
func NewWithContext(ctx context.Context, sink config.Sink, conn adbc.Connection) (core.Sink, error) {
	built, wrap, err := buildSink(ctx, sink, conn)
	if err != nil {
		return nil, err
	}

	policy := RetryPolicyFrom(sink.Retry)
	if !wrap || !policy.Enabled() {
		return built, nil
	}
	return newRetrying(built, policy), nil
}

// buildSink constructs the sink and reports whether a retry ladder belongs
// around it.
func buildSink(ctx context.Context, sink config.Sink, conn adbc.Connection) (core.Sink, bool, error) {
	switch sink.Type {
	case "noop":
		return &NoopSink{}, false, nil

	case "console", "":
		// The Python engine falls back to console for an unset type.
		return NewConsoleSink(), false, nil

	case "kafka":
		if sink.Kafka == nil {
			return nil, false, errs.New(errs.CodeSinkInvalid, "sink: kafka sink requires a kafka block")
		}
		s, err := NewKafkaSink(*sink.Kafka)
		return s, false, err

	case "sqlcommand":
		if sink.SQLCommand == nil {
			return nil, false, errs.New(errs.CodeSinkInvalid, "sink: sqlcommand sink requires a sqlcommand block")
		}
		s, err := NewSQLCommandSink(conn, sink.SQLCommand.SQL, sink.SQLCommand.Substitutions)
		return s, false, err

	case "clickhouse":
		if sink.Clickhouse == nil {
			return nil, false, errs.New(errs.CodeSinkInvalid, "sink: clickhouse sink requires a clickhouse block")
		}
		s, err := NewClickhouseSink(*sink.Clickhouse)
		return s, true, err

	case "iceberg":
		if sink.Iceberg == nil {
			return nil, false, errs.New(errs.CodeSinkInvalid, "sink: iceberg sink requires an iceberg block")
		}
		s, err := NewIcebergSink(ctx, sink.Iceberg.CatalogName, sink.Iceberg.TableName)
		return s, true, err

	default:
		return nil, false, errs.New(errs.CodeSinkInvalid, "sink: %q not supported", sink.Type)
	}
}
