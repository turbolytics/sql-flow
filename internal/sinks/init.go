package sinks

import (
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

func New(sink config.Sink) (core.Sink, error) {
	return &NoopSink{}, nil
}
