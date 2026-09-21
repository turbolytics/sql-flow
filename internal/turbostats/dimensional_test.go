package turbostats

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/turbolytics/sql-flow/internal/managers"
	"github.com/turbolytics/sql-flow/turbostats/wire"
	"github.com/zeebo/assert"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

func lagAttrs(topic string, partition int) metric.MeasurementOption {
	return metric.WithAttributes(
		attribute.String("topic", topic), attribute.Int("partition", partition))
}

// Lag is reported as the worst partition, the total, and how many partitions
// those summarize.
//
// Three partitions at 5, 400 and 7. The max says one partition is stuck; the
// total alone, 412, would read as an ordinary backlog spread across three.
// Swapping max for sum in the aggregate fails this test, which is the point:
// an aggregate nothing pins is an aggregate that drifts.
func TestCollect_LagIsTheWorstPartitionAndTheTotal(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	reader, m, _ := provider(t)
	ctx := context.Background()
	m.ConsumerLag.Record(ctx, 5, lagAttrs("posts", 0))
	m.ConsumerLag.Record(ctx, 400, lagAttrs("posts", 1))
	m.ConsumerLag.Record(ctx, 7, lagAttrs("likes", 0))

	b, err := Collect(ctx, runSource(reader, nil))
	assert.NoError(t, err)
	assert.Equal(t, int64(400), *b.Pipeline.LagMaxMessages)
	assert.Equal(t, int64(412), *b.Pipeline.LagTotalMessages)
	assert.Equal(t, 3, *b.Pipeline.LagPartitions)
}

// A pipeline that has caught up reports zero, and one with no Kafka source
// reports nothing. They are different facts and must be different documents.
//
// omitempty on a plain int64 would render both as an absent field, turning
// the healthiest state a pipeline has into the same page as the unknown one.
func TestCollect_CaughtUpIsZeroAndNoKafkaIsAbsent(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	ctx := context.Background()

	reader, m, _ := provider(t)
	m.ConsumerLag.Record(ctx, 0, lagAttrs("posts", 0))
	caughtUp, err := Collect(ctx, runSource(reader, nil))
	assert.NoError(t, err)
	assert.That(t, caughtUp.Pipeline.LagMaxMessages != nil)
	assert.Equal(t, int64(0), *caughtUp.Pipeline.LagMaxMessages)
	raw, err := json.Marshal(caughtUp)
	assert.NoError(t, err)
	assert.That(t, strings.Contains(string(raw), `"lag_max_messages":0`))

	reader, _, _ = provider(t)
	noKafka, err := Collect(ctx, runSource(reader, nil))
	assert.NoError(t, err)
	assert.That(t, noKafka.Pipeline.LagMaxMessages == nil)
	assert.That(t, noKafka.Pipeline.LagTotalMessages == nil)
	assert.That(t, noKafka.Pipeline.LagPartitions == nil)
	raw, err = json.Marshal(noKafka)
	assert.NoError(t, err)
	assert.That(t, !strings.Contains(string(raw), "lag_"))
}

// Late rows are split by what happened to them, and never summed.
//
// policy is an outcome: a dropped row is gone and a reemitted row is not, so
// their sum is true of neither. window is a shard, and collapses. This drives
// the real instruments from the managers package, so a rename there fails
// here rather than quietly zeroing a field.
func TestCollect_LateRowsSplitByOutcomeAndCollapseByWindow(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	ctx := context.Background()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	hourly := managers.NewWindowMetrics(mp, "hourly")
	daily := managers.NewWindowMetrics(mp, "daily")
	drop := func(w string) metric.AddOption {
		return metric.WithAttributes(attribute.String("window", w), attribute.String("policy", "drop"))
	}
	reemit := func(w string) metric.AddOption {
		return metric.WithAttributes(attribute.String("window", w), attribute.String("policy", "reemit"))
	}
	hourly.Late.Add(ctx, 3, drop("hourly"))
	daily.Late.Add(ctx, 4, drop("daily"))
	hourly.Late.Add(ctx, 10, reemit("hourly"))
	hourly.Closed.Add(ctx, 2, metric.WithAttributes(attribute.String("window", "hourly")))
	daily.Closed.Add(ctx, 1, metric.WithAttributes(attribute.String("window", "daily")))

	b, err := Collect(ctx, Source{Static: static, Reader: reader, Pipeline: &PipelineSource{}})
	assert.NoError(t, err)
	assert.Equal(t, int64(7), *b.Pipeline.LateRowsDropped)
	assert.Equal(t, int64(10), *b.Pipeline.LateRowsReemitted)
	assert.Equal(t, int64(3), *b.Pipeline.WindowClosedCount)
}

// The window fields travel together. A pipeline with windows and no late rows
// says zero were dropped; a pipeline with no windows says nothing.
//
// late_rows_dropped is a data-loss counter, so "none were lost" has to be
// distinguishable from "nothing here can lose any".
func TestCollect_WindowFieldsArePresentTogetherOrNotAtAll(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	ctx := context.Background()

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	w := managers.NewWindowMetrics(mp, "hourly")
	w.Closed.Add(ctx, 1, metric.WithAttributes(attribute.String("window", "hourly")))
	windowed, err := Collect(ctx, Source{Static: static, Reader: reader, Pipeline: &PipelineSource{}})
	assert.NoError(t, err)
	assert.That(t, windowed.Pipeline.LateRowsDropped != nil)
	assert.Equal(t, int64(0), *windowed.Pipeline.LateRowsDropped)
	assert.Equal(t, int64(0), *windowed.Pipeline.LateRowsReemitted)

	reader, _, _ = provider(t)
	plain, err := Collect(ctx, runSource(reader, nil))
	assert.NoError(t, err)
	assert.That(t, plain.Pipeline.LateRowsDropped == nil)
	assert.That(t, plain.Pipeline.LateRowsReemitted == nil)
	assert.That(t, plain.Pipeline.WindowClosedCount == nil)
	assert.That(t, plain.Pipeline.WatermarkLagSeconds == nil)
}

// The watermark is reported as an age, taken from the window furthest behind.
func TestCollect_WatermarkLagIsTheOldestWindowsAge(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	ctx := context.Background()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	now := time.Now().UTC()
	fresh := managers.NewWindowMetrics(mp, "fresh")
	stale := managers.NewWindowMetrics(mp, "stale")
	fresh.Watermark.Record(ctx, now.Add(-10*time.Second).Unix(),
		metric.WithAttributes(attribute.String("window", "fresh")))
	stale.Watermark.Record(ctx, now.Add(-300*time.Second).Unix(),
		metric.WithAttributes(attribute.String("window", "stale")))

	b, err := Collect(ctx, Source{Static: static, Reader: reader, Pipeline: &PipelineSource{}})
	assert.NoError(t, err)
	got := *b.Pipeline.WatermarkLagSeconds
	// The stale window's, within the second SentAt is truncated to.
	assert.That(t, got >= 298 && got <= 302)
}

// Sink retries collapse across sinks, and are always reported.
func TestCollect_SinkRetriesSumAcrossSinks(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	ctx := context.Background()
	reader, _, meter := provider(t)
	retries, err := meter.Int64Counter("sink_retry_count")
	assert.NoError(t, err)
	retries.Add(ctx, 2, metric.WithAttributes(attribute.String("sink", "postgres")))
	retries.Add(ctx, 5, metric.WithAttributes(attribute.String("sink", "kafka")))

	b, err := Collect(ctx, runSource(reader, nil))
	assert.NoError(t, err)
	assert.Equal(t, int64(7), b.Pipeline.SinkRetryCount)

	reader, _, _ = provider(t)
	quiet, err := Collect(ctx, runSource(reader, nil))
	assert.NoError(t, err)
	raw, err := json.Marshal(quiet)
	assert.NoError(t, err)
	// A pipeline always has a sink, so no retries is a reading.
	assert.That(t, strings.Contains(string(raw), `"sink_retry_count":0`))
}

// No field's presence or repetition depends on data cardinality.
//
// This is the invariant, and the byte ceiling below is only the smoke alarm.
// A map or a slice keyed by partition would make a bundle's width a function
// of the broker's partition count -- set by someone outside this system -- and
// a heartbeat would become a metrics payload. Summaries are fixed-width; this
// test fails the day someone adds the map.
func TestWire_NoFieldScalesWithCardinality(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	var walk func(t reflect.Type, path string, bad *[]string)
	walk = func(rt reflect.Type, path string, bad *[]string) {
		for rt.Kind() == reflect.Pointer {
			rt = rt.Elem()
		}
		if rt.Kind() != reflect.Struct || rt == reflect.TypeOf(time.Time{}) {
			return
		}
		for i := 0; i < rt.NumField(); i++ {
			f := rt.Field(i)
			at := path + "." + f.Name
			ft := f.Type
			for ft.Kind() == reflect.Pointer {
				ft = ft.Elem()
			}
			switch ft.Kind() {
			case reflect.Map, reflect.Slice, reflect.Array:
				// commands is reserved by the contract and never populated by
				// v1; it is the one repeated field, and it is not data.
				if at == ".Bundle.Commands" {
					continue
				}
				*bad = append(*bad, at)
			default:
				walk(ft, at, bad)
			}
		}
	}
	var bad []string
	walk(reflect.TypeOf(wire.Bundle{}), ".Bundle", &bad)
	assert.Equal(t, 0, len(bad))
}

// A bundle with every field set stays under the ceiling.
//
// 4 KiB, as a smoke alarm against accidental bloat. The old bound was 1 KiB
// and shaped the design, until measurement showed it protected nothing: the
// control plane stores a bundle uncompressed below roughly 2 KB, so its
// stored form was larger than the wire form the bound was limiting.
func TestCollect_AFullBundleStaysUnderTheCeiling(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	big := int64(1) << 62
	n := 1 << 30
	at := time.Now().UTC()
	b := wire.Bundle{
		V: wire.Version, SentAt: at, IntervalSeconds: 86400, LastActivityAt: &at,
		Instance: wire.Instance{
			ID: strings.Repeat("i", 64), Name: strings.Repeat("n", 64),
			Version: "v2026.09.21.12", Commit: strings.Repeat("c", 40),
			Arch: "linux/arm64", ConfigHash: "sha256:" + strings.Repeat("f", 64),
		},
		Process: wire.Process{StartedAt: at, RSSBytes: big, Goroutines: n},
		Pipeline: &wire.Pipeline{
			MessageCount: big, HandlerRowsRead: big, ErrorCount: big,
			SinkFlushCount: big, SinkRowsAccepted: big, SinkRowsWritten: big,
			StateCommitCount: big, StateDBSizeBytes: &big, LastMessageAt: &at,
			SinkRetryCount: big, LagMaxMessages: &big, LagTotalMessages: &big,
			LagPartitions: &n, LateRowsDropped: &big, LateRowsReemitted: &big,
			WindowClosedCount: &big, WatermarkLagSeconds: &big,
		},
		Serve: &wire.Serve{
			RequestCount: big, RequestErrorCount: big, SessionsInUse: n,
			SessionsTotal: n, LastRequestAt: &at,
			Cache: &wire.ServeCache{HitCount: big, MissCount: big, SharedCount: big,
				EvictionCount: big, Bytes: big, Entries: n},
		},
		Exit: &wire.Exit{Reason: "system.internal.unexpected", Code: 255},
	}
	raw, err := json.Marshal(b)
	assert.NoError(t, err)
	t.Logf("a bundle with every field at its widest is %d bytes", len(raw))
	assert.That(t, len(raw) < 4<<10)
}
