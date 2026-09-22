package turbostats

import (
	"context"
	"fmt"
	"runtime"
	"time"

	"go.opentelemetry.io/otel/attribute"
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
	flat, dim := walk(rm)

	// Not fatal. Every other number in the bundle is still true, and an
	// instance that stops reporting is indistinguishable from one that died.
	// The field is omitted instead, which says "unknown" where a zero would
	// have said "no memory".
	// A failed read omits the field rather than failing the bundle. Every
	// other number is still true, and an instance that stops reporting is
	// indistinguishable from one that died, so losing the heartbeat over one
	// number is the worse trade. The gap on the memory chart is the signal.
	rss, err := ResidentAnonBytes()
	if err != nil {
		rss = 0
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

	if s.Clock != nil {
		uptime, idle, worked := s.Clock.Read()
		up := int64(uptime / time.Second)
		b.Process.UptimeSeconds = &up
		if worked {
			i := int64(idle / time.Second)
			b.IdleSeconds = &i
		}
	}

	if src.Pipeline != nil {
		p, err := pipelineSection(ctx, flat, dim, b.SentAt, src.Pipeline)
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

func pipelineSection(ctx context.Context, flat map[string]int64, dim *dimensional,
	sentAt time.Time, src *PipelineSource) (*Pipeline, error) {
	p := &Pipeline{
		MessageCount:        flat["message_count"],
		MessagePayloadBytes: int64Ptr(flat["message_payload_bytes"]),
		HandlerRowsRead:     flat["handler_rows_read"],
		ErrorCount:          flat["pipeline_errors"],
		SinkFlushCount:      flat["pipeline_flushes"],
		SinkRowsAccepted:    flat["pipeline_rows_accepted"],
		SinkRowsWritten:     flat["pipeline_rows_written"],
		StateCommitCount:    flat["pipeline_commits"],
		LastMessageAt:       unixTime(flat["pipeline_last_message_timestamp"]),
		// Always sent, zero included: a pipeline always has a sink.
		SinkRetryCount: &dim.sinkRetries,
	}
	// Present once lag has been measured at all, not only while partitions
	// are held. An instance whose partitions all moved elsewhere, or one in a
	// group with more consumers than partitions, is still a Kafka pipeline:
	// it reports zero partitions and zero lag, dated, rather than the silence
	// that means it has no Kafka source.
	if dim.lagSeen || flat["consumer_lag_observed_timestamp"] > 0 {
		p.LagMaxMessages = &dim.lagMax
		p.LagTotalMessages = &dim.lagTotal
		p.LagPartitions = &dim.lagPoints
		p.LagObservedAt = unixTime(flat["consumer_lag_observed_timestamp"])
	}
	if dim.windowSeen {
		p.WindowClosedCount = &dim.windowClosed
		// A policy with no field of its own leaves both out. Reporting the
		// two known ones would state a count that omits rows, on the field
		// an operator reads for data loss.
		if !dim.lateUnknownPolicy {
			p.LateRowsDropped = &dim.lateDropped
			p.LateRowsReemitted = &dim.lateReemitted
		}
	}
	if dim.closeLagSeen {
		lag := dim.closeLagMax
		p.WindowLagSeconds = &lag
	}
	// A timestamp, not an age. Whether rows are stamped in the future is a
	// comparison with a clock someone trusts, and on a gateway with no
	// real-time clock this host's is not it.
	p.WindowNewestBucketAt = unixTime(dim.newestStartNewest)
	if src.Stats != nil {
		st, err := src.Stats(ctx)
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

// walk reads every instrument once, and yields two things.
//
// flat is the dimensionless point of each int64 instrument, by name. It sums
// nothing and filters nothing, because which measurements count was decided
// where they were recorded.
//
// dim is the summary of the instruments that only ever record under
// attributes, which flat therefore cannot see at all: consumer lag, sink
// retries, and the window counters. A bundle field may collapse one of those
// attributes only when every point under it measures the same thing.
//
//   - A shard attribute -- topic, partition, window, sink -- splits one
//     measurement across parts of one system. Collapsing it is arithmetic
//     that stays true.
//   - An outcome attribute -- result, policy -- splits points that measure
//     different things. Collapsing it reports a number true of nothing.
//     sink_flush_count carries result=ok and result=error, and their sum is
//     a count of flushes that never happened; window_late_rows carries
//     policy=drop and policy=reemit, and one of those lost data while the
//     other did not.
//
// So flat keeps ignoring every attributed point, and dim collapses shards
// only, splitting each outcome into a field of its own.
func walk(rm metricdata.ResourceMetrics) (map[string]int64, *dimensional) {
	flat := map[string]int64{}
	dim := &dimensional{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			switch data := m.Data.(type) {
			case metricdata.Sum[int64]:
				for _, dp := range data.DataPoints {
					if dp.Attributes.Len() == 0 {
						flat[m.Name] = dp.Value
						continue
					}
					dim.add(m.Name, dp.Attributes, dp.Value)
				}
			case metricdata.Gauge[int64]:
				for _, dp := range data.DataPoints {
					if dp.Attributes.Len() == 0 {
						flat[m.Name] = dp.Value
						continue
					}
					dim.add(m.Name, dp.Attributes, dp.Value)
				}
			}
		}
	}
	return flat, dim
}

// dimensional accumulates the attributed series a bundle summarizes.
//
// Each group carries a seen flag rather than relying on a zero, because zero
// is a reading and absence is not: a pipeline that has caught up reports lag
// zero, and one with no Kafka source reports no lag at all.
type dimensional struct {
	lagSeen     bool
	lagMax      int64
	lagTotal    int64
	lagPoints   int
	sinkRetries int64

	windowSeen        bool
	lateDropped       int64
	lateReemitted     int64
	lateUnknownPolicy bool
	windowClosed      int64

	// The most behind window's close lag, with a seen flag because zero lag
	// is a reading. And the newest bucket across windows, a Unix second that
	// unixTime reads as absent while it is zero -- the rule every timestamp
	// in the bundle follows, so an unset one never reports 1970.
	closeLagSeen      bool
	closeLagMax       int64
	newestStartNewest int64
}

// int64Ptr takes a copy, so a field never aliases a map entry.
func int64Ptr(v int64) *int64 { return &v }

func (d *dimensional) add(name string, attrs attribute.Set, v int64) {
	switch name {
	case "consumer_lag":
		// Collapses topic and partition, both shards.
		d.lagSeen = true
		d.lagPoints++
		d.lagTotal += v
		if v > d.lagMax {
			d.lagMax = v
		}
	case "sink_retry_count":
		// Collapses sink, a shard.
		d.sinkRetries += v
	case "window_closed":
		d.windowSeen = true
		d.windowClosed += v
	case "window_late_rows":
		// Collapses window, a shard. Splits policy, an outcome: dropped rows
		// are gone and reemitted rows are not.
		d.windowSeen = true
		switch policy, _ := attrs.Value(attribute.Key("policy")); policy.AsString() {
		case "drop":
			d.lateDropped += v
		case "reemit":
			d.lateReemitted += v
		default:
			// An outcome this contract has no field for. It cannot join
			// either count without making that count false.
			d.lateUnknownPolicy = true
		}
	case "window_close_lag_seconds":
		// Already a duration in event time; the most behind window wins.
		d.windowSeen = true
		if !d.closeLagSeen || v > d.closeLagMax {
			d.closeLagMax = v
		}
		d.closeLagSeen = true
	case "window_newest_bucket_start_seconds":
		d.windowSeen = true
		if v > d.newestStartNewest {
			d.newestStartNewest = v
		}
	}
}
