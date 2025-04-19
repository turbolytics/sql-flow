package sinks

import (
	"github.com/turbolytics/turbine/internal/config"
	"github.com/turbolytics/turbine/internal/core"
	"github.com/turbolytics/turbine/internal/local"
)

func New(sink config.Sink) (core.Sink, error) {
	return local.NewConsoleSink(), nil
}
