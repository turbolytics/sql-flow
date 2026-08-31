package sinks

import (
	"fmt"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/turbolytics/turbine/internal/config"
	"github.com/turbolytics/turbine/internal/core"
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
			return nil, fmt.Errorf("sink: kafka sink requires a kafka block")
		}
		return NewKafkaSink(sink.Kafka.Brokers, sink.Kafka.Topic)

	case "sqlcommand":
		if sink.SQLCommand == nil {
			return nil, fmt.Errorf("sink: sqlcommand sink requires a sqlcommand block")
		}
		return NewSQLCommandSink(conn, sink.SQLCommand.SQL, sink.SQLCommand.Substitutions)

	default:
		return nil, fmt.Errorf("sink: %q not supported", sink.Type)
	}
}
