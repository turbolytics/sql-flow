package turbostats

import (
	"context"
	"fmt"
	"runtime"
	"time"

	"github.com/turbolytics/sql-flow/internal/core"
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
	flat := scalars(rm)

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
			MessageCount:     flat["message_count"],
			HandlerRowsRead:  flat["handler_rows_read"],
			ErrorCount:       flat["pipeline_errors"],
			SinkFlushCount:   flat["pipeline_flushes"],
			SinkRowsAccepted: flat["pipeline_rows_accepted"],
			SinkRowsWritten:  flat["pipeline_rows_written"],
			StateCommitCount: flat["pipeline_commits"],
		},
	}

	// Zero means no messages yet, which is not a time. The control plane
	// derives staleness as sent_at minus this, both from the instance's own
	// clock, so the difference carries no skew.
	if ts := flat["pipeline_last_message_timestamp"]; ts > 0 {
		at := time.Unix(ts, 0).UTC()
		b.LastMessageAt = &at
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

// scalars reads the dimensionless point of every int64 instrument, by name.
//
// It sums nothing and filters nothing. Every number the bundle reports has a
// dimensionless series recorded for it, so this is a lookup, and which
// measurements count was decided where they were recorded.
//
// That is the whole point. Adding a counter's attribute sets together is
// arithmetic that silently encodes a policy: sink_flush_count carries
// result=ok and result=error, and summing them reports a number of flushes
// that is true of nothing. A point carrying any attribute is not the flat
// series and is skipped.
func scalars(rm metricdata.ResourceMetrics) map[string]int64 {
	out := map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			switch data := m.Data.(type) {
			case metricdata.Sum[int64]:
				for _, dp := range data.DataPoints {
					if dp.Attributes.Len() == 0 {
						out[m.Name] = dp.Value
					}
				}
			case metricdata.Gauge[int64]:
				for _, dp := range data.DataPoints {
					if dp.Attributes.Len() == 0 {
						out[m.Name] = dp.Value
					}
				}
			}
		}
	}
	return out
}
