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

func (n *NoopSink) WriteTable(batch arrow.Table) error {
	// No operation performed
	return nil
}

func (n *NoopSink) Flush() error {
	// No operation performed
	return nil
}

func (n *NoopSink) Batch() (arrow.Table, error) {
	// No operation performed, return nil
	return nil, nil
}

func New(sink config.Sink, conn adbc.Connection) (core.Sink, error) {
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
		return NewIcebergSink(context.Background(), sink.Iceberg.CatalogName, sink.Iceberg.TableName)

	default:
		return nil, errs.New(errs.CodeSinkInvalid, "sink: %q not supported", sink.Type)
	}
}
