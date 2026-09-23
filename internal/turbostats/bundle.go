// Package turbostats builds the document a sqlflow process reports about
// itself.
//
// The document's types and its signature are the public contract in
// turbostats/wire. This package is the collector: one function builds the
// bundle, and two transports carry it. The HTTP handler here serves it to
// whatever can reach the process, and the reporter posts it outbound to a
// control plane that cannot. Neither transport computes anything.
//
// Field names are the OTel instrument names, not the Prometheus series names
// those instruments render as. The bundle reads the instrument; the suffixes
// belong to one exporter.
package turbostats

import (
	"context"
	"time"

	"github.com/turbolytics/sql-flow/internal/activity"
	"github.com/turbolytics/sql-flow/internal/core"
	"github.com/turbolytics/sql-flow/turbostats/wire"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

// The wire types, aliased so the engine names one package for the collector
// and its document.
type (
	Bundle   = wire.Bundle
	Duration = wire.Duration
	Instance = wire.Instance
	Process  = wire.Process
	Pipeline = wire.Pipeline
	Serve    = wire.Serve
	// The duration groups, one per section.
	PipelineDurations = wire.PipelineDurations
	ServeDurations    = wire.ServeDurations
	ServeCache        = wire.ServeCache
	Exit              = wire.Exit
)

const (
	MediaType = wire.MediaType
	Version   = wire.Version
)

// Static is what a command knows once, at startup, and the package cannot
// learn on its own.
type Static struct {
	ID, Name, Version, Commit, ConfigHash string
	StartedAt                             time.Time
	// IntervalSeconds is the reporter's interval, and 0 without a reporter.
	IntervalSeconds int
	// What this process is made of, from its config. A serve process leaves
	// all three empty: it answers queries over datasets.
	SourceType  string
	SinkType    string
	HandlerType string
	// Labels are the operator's, already validated by the config.
	Labels map[string]string
	// Clock is the process's monotonic start and last work. Nil sends no
	// uptime_seconds or idle_seconds, which a receiver reads as an older
	// engine rather than as a process that just started.
	Clock *activity.Clock
}

// Source is everything Collect reads. A nil Pipeline or Serve omits that
// section: presence says what the process does.
type Source struct {
	Static Static
	Reader *sdkmetric.ManualReader
	// Pipeline is set by `sqlflow run`.
	Pipeline *PipelineSource
	// Serve is set by `sqlflow serve`.
	Serve *ServeSource
}

// PipelineSource is what the pipeline section reads beyond the instruments.
type PipelineSource struct {
	// Stats may be nil for a pipeline with no state path.
	//
	// It takes a context because it queries the state database. Without one
	// the post timeout bounds only the HTTP request, and a COUNT(*) over a
	// large state table holds a shutdown open past the deadline a supervisor
	// is waiting on.
	Stats func(context.Context) (*core.StateStats, error)
	// LastError is the code and time of the last error the pipeline
	// recorded, and false before the first. The message stays in the
	// process: it carries the row that failed.
	LastError func() (code string, at time.Time, ok bool)
}

// ServeSource is what the serve section reads beyond the instruments. Both
// are current values with no cumulative meaning, so they are read when the
// bundle is built rather than recorded.
type ServeSource struct {
	Sessions func() (inUse, total int)
	// Cache is nil on a server where no dataset opted into the cache.
	Cache func() (bytes int64, entries int)
}
