package turbostats

import (
	"context"
	"fmt"
	"runtime"
	"time"

	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// Collect builds one bundle. It is the only function that does.
//
// It allocates one bundle and touches nothing shared, so the HTTP handler and
// the reporter can call it at once. A stats error is the bundle's error,
// because a document with a field quietly missing reads as healthy.
func Collect(ctx context.Context, src Source) (Bundle, error) {
	var rm metricdata.ResourceMetrics
	if err := src.Reader.Collect(ctx, &rm); err != nil {
		return Bundle{}, fmt.Errorf("turbostats: collecting instruments: %w", err)
	}
	flat := scalars(rm)

	rss, err := ResidentAnonBytes()
	if err != nil {
		return Bundle{}, fmt.Errorf("turbostats: reading resident memory: %w", err)
	}

	s := src.Static
	b := Bundle{
		V:               Version,
		SentAt:          time.Now().UTC().Truncate(time.Second),
		IntervalSeconds: s.IntervalSeconds,
		Instance: Instance{
			ID:         s.ID,
			Name:       s.Name,
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
	}

	if src.Pipeline != nil {
		p, err := pipelineSection(flat, src.Pipeline)
		if err != nil {
			return Bundle{}, err
		}
		b.Pipeline = p
		b.LastActivityAt = later(b.LastActivityAt, p.LastMessageAt)
	}
	if src.Serve != nil {
		sv := serveSection(flat, src.Serve)
		b.Serve = sv
		b.LastActivityAt = later(b.LastActivityAt, sv.LastRequestAt)
	}
	return b, nil
}

func pipelineSection(flat map[string]int64, src *PipelineSource) (*Pipeline, error) {
	p := &Pipeline{
		MessageCount:     flat["message_count"],
		HandlerRowsRead:  flat["handler_rows_read"],
		ErrorCount:       flat["pipeline_errors"],
		SinkFlushCount:   flat["pipeline_flushes"],
		SinkRowsAccepted: flat["pipeline_rows_accepted"],
		SinkRowsWritten:  flat["pipeline_rows_written"],
		StateCommitCount: flat["pipeline_commits"],
		LastMessageAt:    unixTime(flat["pipeline_last_message_timestamp"]),
	}
	if src.Stats != nil {
		st, err := src.Stats()
		if err != nil {
			return nil, fmt.Errorf("turbostats: reading state stats: %w", err)
		}
		if st != nil {
			size := st.SizeBytes
			p.StateDBSizeBytes = &size
		}
	}
	return p, nil
}

// serveSection reads the flat series internal/serve records. The names are
// the contract between the two packages, and a test on each side pins them.
func serveSection(flat map[string]int64, src *ServeSource) *Serve {
	sv := &Serve{
		RequestCount:      flat["serve_requests"],
		RequestErrorCount: flat["serve_request_errors"],
		LastRequestAt:     unixTime(flat["serve_last_request_timestamp"]),
	}
	if src.Sessions != nil {
		sv.SessionsInUse, sv.SessionsTotal = src.Sessions()
	}
	if src.Cache != nil {
		bytes, entries := src.Cache()
		sv.Cache = &ServeCache{
			HitCount:      flat["serve_cache_hits"],
			MissCount:     flat["serve_cache_misses"],
			SharedCount:   flat["serve_cache_shared"],
			EvictionCount: flat["serve_cache_evicted"],
			Bytes:         bytes,
			Entries:       entries,
		}
	}
	return sv
}

// unixTime is nil for zero: nothing has happened yet, and zero is not a time.
// A receiver derives staleness as sent_at minus this, both from the
// instance's own clock, so the difference carries no skew.
func unixTime(seconds int64) *time.Time {
	if seconds <= 0 {
		return nil
	}
	at := time.Unix(seconds, 0).UTC()
	return &at
}

// later returns the later of two optional times. last_activity_at is the
// latest section timestamp, so a process with two sections reports whichever
// moved last.
func later(a, b *time.Time) *time.Time {
	if a == nil {
		return b
	}
	if b == nil || a.After(*b) {
		return a
	}
	return b
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
