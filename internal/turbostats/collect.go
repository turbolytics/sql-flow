package turbostats

import (
	"context"
	"fmt"
	"runtime"
	"time"

	"github.com/turbolytics/sql-flow/internal/core"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// Collect builds one bundle. It is the only function that does.
//
// It allocates one bundle and touches nothing shared, so the HTTP handler and
// the reporter can call it at once. stats may be nil for a pipeline with no
// state path; a stats error is the bundle's error, because a document with a
// field quietly missing reads as healthy.
func Collect(ctx context.Context, s Static, r *sdkmetric.ManualReader,
	stats func() (*core.StateStats, error)) (Bundle, error) {

	var rm metricdata.ResourceMetrics
	if err := r.Collect(ctx, &rm); err != nil {
		return Bundle{}, fmt.Errorf("turbostats: collecting instruments: %w", err)
	}
	totals := sumByName(rm)

	rss, err := ResidentAnonBytes()
	if err != nil {
		return Bundle{}, fmt.Errorf("turbostats: reading resident memory: %w", err)
	}

	b := Bundle{
		V:      Version,
		SentAt: time.Now().UTC().Truncate(time.Second),
		Instance: Instance{
			ID:         s.ID,
			Pipeline:   s.Pipeline,
			Version:    s.Version,
			Commit:     s.Commit,
			Arch:       runtime.GOOS + "/" + runtime.GOARCH,
			ConfigHash: s.ConfigHash,
		},
		Process: Process{
			StartedAt:  s.StartedAt.UTC().Truncate(time.Second),
			RSSBytes:   rss,
			Goroutines: runtime.NumGoroutine(),
		},
		Pipeline: Pipeline{
			MessageCount:     totals["message_count"],
			HandlerRowsRead:  totals["handler_rows_read"],
			ErrorCount:       totals["error_count"],
			SinkFlushCount:   totals["sink_flush_count"],
			SinkRowsAccepted: totals["sink_rows_accepted"],
			SinkRowsWritten:  totals["sink_rows_written"],
			StateCommitCount: totals["state_commit_count"],
			ConsumerLag:      totals["consumer_lag"],
		},
	}

	if stats != nil {
		st, err := stats()
		if err != nil {
			return Bundle{}, fmt.Errorf("turbostats: reading state stats: %w", err)
		}
		if st != nil {
			size := st.SizeBytes
			b.Pipeline.StateDBSizeBytes = &size
		}
	}
	return b, nil
}

// roleKey is the attribute the sink counters carry to keep the DLQ's rows out
// of the pipeline's delivered series. The bundle keeps them out too.
var roleKey = attribute.Key("role")

// sumByName folds every int64 counter and gauge into one total per name,
// summed across attribute sets, except that a point with a role other than
// "pipeline" is skipped. Histograms and float instruments are ignored: the
// bundle carries none.
func sumByName(rm metricdata.ResourceMetrics) map[string]int64 {
	out := map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			switch data := m.Data.(type) {
			case metricdata.Sum[int64]:
				for _, dp := range data.DataPoints {
					if pipelineRole(dp.Attributes) {
						out[m.Name] += dp.Value
					}
				}
			case metricdata.Gauge[int64]:
				for _, dp := range data.DataPoints {
					if pipelineRole(dp.Attributes) {
						out[m.Name] += dp.Value
					}
				}
			}
		}
	}
	return out
}

func pipelineRole(set attribute.Set) bool {
	v, ok := set.Value(roleKey)
	return !ok || v.AsString() == "pipeline"
}
