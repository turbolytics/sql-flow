package turbostats

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/turbolytics/sql-flow/internal/core"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

// A provider with the engine's real instruments, driven a known distance.
func provider(t *testing.T) (*sdkmetric.ManualReader, *core.Metrics, metric.Meter) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	m, err := core.NewMetrics(mp)
	assert.NoError(t, err)
	return reader, m, mp.Meter("sqlflow")
}

var static = Static{
	ID:         "pi-01",
	Name:       "demo",
	Version:    "v1.2.3",
	Commit:     "abc1234",
	ConfigHash: "sha256:00",
	StartedAt:  time.Date(2026, 9, 1, 8, 12, 44, 0, time.UTC),
}

// runSource is what the run command hands Collect: a pipeline section, and
// no serve section.
func runSource(r *sdkmetric.ManualReader, stats func(context.Context) (*core.StateStats, error)) Source {
	return Source{Static: static, Reader: r, Pipeline: &PipelineSource{Stats: stats}}
}

func TestCollect_CountersAreTotalsSinceStart(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	reader, m, _ := provider(t)
	ctx := context.Background()
	m.MessageCount.Add(ctx, 3)
	m.MessageCount.Add(ctx, 4)
	m.PipelineErrors.Add(ctx, 1)

	b, err := Collect(ctx, runSource(reader, nil))
	assert.NoError(t, err)
	assert.Equal(t, int64(7), b.Pipeline.MessageCount)
	assert.Equal(t, int64(1), b.Pipeline.ErrorCount)
}

// Collect never sums and never filters. A series carrying attributes is not
// the flat one, so it is not the bundle's, whatever its name.
//
// This is what keeps policy out of the reader. sink_flush_count carries
// result=ok and result=error; adding them reports a number of flushes that is
// true of nothing, and the engine records pipeline_flushes instead.
func TestCollect_ADimensionedSeriesIsNotTheBundles(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	reader, _, meter := provider(t)
	ctx := context.Background()
	flushes, err := meter.Int64Counter("pipeline_flushes")
	assert.NoError(t, err)
	flushes.Add(ctx, 3)
	// The same name with an attribute: a different series, and not ours.
	flushes.Add(ctx, 99, metric.WithAttributes(attribute.String("result", "error")))

	b, err := Collect(ctx, runSource(reader, nil))
	assert.NoError(t, err)
	assert.Equal(t, int64(3), b.Pipeline.SinkFlushCount)
}

// The bundle reads the flat twins, not the instruments they shadow.
func TestCollect_ReadsTheFlatSeries(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	reader, m, _ := provider(t)
	ctx := context.Background()
	m.PipelineErrors.Add(ctx, 2)
	m.PipelineFlushes.Add(ctx, 3)
	m.PipelineCommits.Add(ctx, 4)
	m.PipelineRowsAccepted.Add(ctx, 5)
	m.PipelineRowsWritten.Add(ctx, 6)

	b, err := Collect(ctx, runSource(reader, nil))
	assert.NoError(t, err)
	assert.Equal(t, int64(2), b.Pipeline.ErrorCount)
	assert.Equal(t, int64(3), b.Pipeline.SinkFlushCount)
	assert.Equal(t, int64(4), b.Pipeline.StateCommitCount)
	assert.Equal(t, int64(5), b.Pipeline.SinkRowsAccepted)
	assert.Equal(t, int64(6), b.Pipeline.SinkRowsWritten)
}

// Staleness is the question the page asks, and the control plane derives it
// from sent_at minus this. Offset lag could not answer it: a websocket source
// has no offsets at all.
func TestCollect_CarriesWhenMessagesLastArrived(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	reader, m, _ := provider(t)
	ctx := context.Background()
	m.PipelineLastMessage.Record(ctx, 1757570000)

	b, err := Collect(ctx, runSource(reader, nil))
	assert.NoError(t, err)
	assert.That(t, b.Pipeline.LastMessageAt != nil)
	assert.Equal(t, int64(1757570000), b.Pipeline.LastMessageAt.Unix())
	// The top-level field is a copy of the section's, so a receiver reads
	// staleness without knowing which sections exist.
	assert.That(t, b.LastActivityAt != nil)
	assert.Equal(t, int64(1757570000), b.LastActivityAt.Unix())
}

// A pipeline that has received nothing has no last message, and zero is not a
// time. The field is absent rather than 1970.
func TestCollect_OmitsTheLastMessageBeforeAnyArrive(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	reader, _, _ := provider(t)

	b, err := Collect(context.Background(), runSource(reader, nil))
	assert.NoError(t, err)
	assert.That(t, b.Pipeline.LastMessageAt == nil)
	assert.That(t, b.LastActivityAt == nil)
	raw, err := json.Marshal(b)
	assert.NoError(t, err)
	assert.That(t, !strings.Contains(string(raw), "last_message_at"))
	assert.That(t, !strings.Contains(string(raw), "last_activity_at"))
}

func TestCollect_HistogramsAreNotInTheBundle(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	reader, m, _ := provider(t)
	ctx := context.Background()
	m.SinkFlushLatency.Record(ctx, 0.25)

	b, err := Collect(ctx, runSource(reader, nil))
	assert.NoError(t, err)
	raw, err := json.Marshal(b)
	assert.NoError(t, err)
	assert.That(t, !strings.Contains(string(raw), "latency"))
	assert.That(t, !strings.Contains(string(raw), "bucket"))
}

// Absent state and empty state are different facts, the same rule /stats
// follows.
func TestCollect_StateSizeIsOmittedWithoutAStateDatabase(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	reader, _, _ := provider(t)

	b, err := Collect(context.Background(), runSource(reader, nil))
	assert.NoError(t, err)
	raw, err := json.Marshal(b)
	assert.NoError(t, err)
	assert.That(t, !strings.Contains(string(raw), "state_db_size_bytes"))
}

func TestCollect_StateSizeComesFromTheStatsFunction(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	reader, _, _ := provider(t)
	stats := func(context.Context) (*core.StateStats, error) {
		return &core.StateStats{SizeBytes: 4096}, nil
	}

	b, err := Collect(context.Background(), runSource(reader, stats))
	assert.NoError(t, err)
	assert.That(t, b.Pipeline.StateDBSizeBytes != nil)
	assert.Equal(t, int64(4096), *b.Pipeline.StateDBSizeBytes)
}

// A stats failure is the bundle's failure. A monitoring system must see it
// rather than a healthy-looking document with a field quietly missing.
func TestCollect_AStatsFailureFailsTheBundle(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	reader, _, _ := provider(t)
	stats := func(context.Context) (*core.StateStats, error) { return nil, errors.New("unreadable") }

	_, err := Collect(context.Background(), runSource(reader, stats))
	assert.Error(t, err)
}

func TestCollect_CarriesTheStaticFactsAndTheRuntime(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	reader, _, _ := provider(t)

	b, err := Collect(context.Background(), runSource(reader, nil))
	assert.NoError(t, err)
	assert.Equal(t, 1, b.V)
	assert.Equal(t, "pi-01", b.Instance.ID)
	assert.Equal(t, "demo", b.Instance.Name)
	assert.Equal(t, "v1.2.3", b.Instance.Version)
	assert.Equal(t, "abc1234", b.Instance.Commit)
	assert.Equal(t, "sha256:00", b.Instance.ConfigHash)
	assert.That(t, strings.Contains(b.Instance.Arch, "/"))
	assert.Equal(t, static.StartedAt, b.Process.StartedAt)
	assert.That(t, b.Process.Goroutines > 0)
	assert.That(t, b.Process.RSSBytes > 0)
	assert.That(t, b.Exit == nil)
	assert.That(t, !b.SentAt.IsZero())
	assert.Equal(t, 0, b.SentAt.Nanosecond())
}

// A realistic run bundle, through the real collector: a Kafka pipeline with
// windows and a retrying sink, a year into the Bluesky firehose.
//
// The ceiling is 4 KiB. It was 1 KiB until measurement showed that bound
// protected nothing, and this test logs the size so the number the contract's
// spec only estimated is one anybody can read.
func TestCollect_ARunBundleStaysUnderTheCeiling(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	reader, m, meter := provider(t)
	ctx := context.Background()
	for partition := 0; partition < 32; partition++ {
		m.ConsumerLag.Record(ctx, 184203, lagAttrs("bluesky.posts", partition))
	}
	late, err := meter.Int64Counter("window_late_rows")
	assert.NoError(t, err)
	late.Add(ctx, 184203, metric.WithAttributes(
		attribute.String("window", "posts_by_lang"), attribute.String("policy", "drop")))
	late.Add(ctx, 184203, metric.WithAttributes(
		attribute.String("window", "posts_by_lang"), attribute.String("policy", "reemit")))
	closed, err := meter.Int64Counter("window_closed")
	assert.NoError(t, err)
	closed.Add(ctx, 1842033, metric.WithAttributes(attribute.String("window", "posts_by_lang")))
	watermark, err := meter.Int64Gauge("window_watermark_seconds")
	assert.NoError(t, err)
	watermark.Record(ctx, 1757570000, metric.WithAttributes(attribute.String("window", "posts_by_lang")))
	retries, err := meter.Int64Counter("sink_retry_count")
	assert.NoError(t, err)
	retries.Add(ctx, 1842, metric.WithAttributes(attribute.String("sink", "postgres")))
	// Nine-digit totals, the size a year of the Bluesky firehose reaches.
	m.MessageCount.Add(ctx, 184203311)
	m.HandlerRowsRead.Add(ctx, 184203311)
	m.SinkFlushCount.Add(ctx, 1842033)
	m.StateCommitCount.Add(ctx, 1842033)
	m.PipelineRowsAccepted.Add(ctx, 184203311)
	m.PipelineRowsWritten.Add(ctx, 184203311)
	m.PipelineLastMessage.Record(ctx, 1757570000)
	size := int64(4194304)
	stats := func(context.Context) (*core.StateStats, error) { return &core.StateStats{SizeBytes: size}, nil }

	b, err := Collect(ctx, runSource(reader, stats))
	assert.NoError(t, err)
	b.Exit = &Exit{Reason: "SIGTERM", Code: 0}
	raw, err := json.Marshal(b)
	assert.NoError(t, err)
	t.Logf("a realistic run bundle with lag, windows and retries is %d bytes", len(raw))
	// Thirty-two partitions went in, and the bundle is no wider for them.
	assert.Equal(t, 32, *b.Pipeline.LagPartitions)
	assert.That(t, len(raw) < 4<<10)
}

// A section's presence says what the process does. A run bundle has no serve
// section, and a source with neither has neither.
func TestCollect_ARunBundleCarriesPipelineAndNoServe(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	reader, _, _ := provider(t)

	b, err := Collect(context.Background(), runSource(reader, nil))
	assert.NoError(t, err)
	assert.That(t, b.Pipeline != nil)
	assert.That(t, b.Serve == nil)

	bare, err := Collect(context.Background(), Source{Static: static, Reader: reader})
	assert.NoError(t, err)
	assert.That(t, bare.Pipeline == nil)
	assert.That(t, bare.Serve == nil)
}

// The receiver needs the interval to tell late from normal. Without a
// reporter there is no interval, and zero is not one.
func TestCollect_CarriesTheIntervalOnlyWhenThereIsOne(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	reader, _, _ := provider(t)

	b, err := Collect(context.Background(), runSource(reader, nil))
	assert.NoError(t, err)
	raw, err := json.Marshal(b)
	assert.NoError(t, err)
	assert.That(t, !strings.Contains(string(raw), "interval_seconds"))

	src := runSource(reader, nil)
	src.Static.IntervalSeconds = 60
	b, err = Collect(context.Background(), src)
	assert.NoError(t, err)
	assert.Equal(t, 60, b.IntervalSeconds)
}

// v1 never populates the reserved name.
func TestCollect_LeavesCommandsUnset(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	reader, _, _ := provider(t)

	b, err := Collect(context.Background(), runSource(reader, nil))
	assert.NoError(t, err)
	raw, err := json.Marshal(b)
	assert.NoError(t, err)
	assert.That(t, !strings.Contains(string(raw), "commands"))
}

// serveProvider registers the flat serve instruments by name. The names are
// the contract between internal/serve, which records them, and Collect, which
// reads them; this package cannot import internal/serve to get the real ones.
func serveProvider(t *testing.T) (*sdkmetric.ManualReader, metric.Meter) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	return reader, mp.Meter("sqlflow/serve")
}

func addCounter(t *testing.T, m metric.Meter, name string, n int64) {
	t.Helper()
	c, err := m.Int64Counter(name)
	assert.NoError(t, err)
	c.Add(context.Background(), n)
}

func TestCollect_AServeBundleCarriesServeAndNoPipeline(t *testing.T) {
	coverage.Covers(t, "observability.turbostats.serve")
	reader, meter := serveProvider(t)
	addCounter(t, meter, "serve_requests", 40)
	addCounter(t, meter, "serve_request_errors", 2)
	last, err := meter.Int64Gauge("serve_last_request_timestamp")
	assert.NoError(t, err)
	last.Record(context.Background(), 1757570000)

	b, err := Collect(context.Background(), Source{
		Static: static,
		Reader: reader,
		Serve:  &ServeSource{Sessions: func() (int, int) { return 2, 8 }},
	})
	assert.NoError(t, err)
	assert.That(t, b.Pipeline == nil)
	assert.That(t, b.Serve != nil)
	assert.Equal(t, int64(40), b.Serve.RequestCount)
	assert.Equal(t, int64(2), b.Serve.RequestErrorCount)
	assert.Equal(t, 2, b.Serve.SessionsInUse)
	assert.Equal(t, 8, b.Serve.SessionsTotal)
	assert.Equal(t, int64(1757570000), b.Serve.LastRequestAt.Unix())
	assert.Equal(t, int64(1757570000), b.LastActivityAt.Unix())
	// An absent cache and an empty cache are different facts.
	assert.That(t, b.Serve.Cache == nil)
}

func TestCollect_TheCacheSectionReadsTotalsAndCurrentSize(t *testing.T) {
	coverage.Covers(t, "observability.turbostats.serve")
	reader, meter := serveProvider(t)
	addCounter(t, meter, "serve_cache_hits", 30)
	addCounter(t, meter, "serve_cache_misses", 7)
	addCounter(t, meter, "serve_cache_shared", 3)
	addCounter(t, meter, "serve_cache_evicted", 5)

	b, err := Collect(context.Background(), Source{
		Static: static,
		Reader: reader,
		Serve: &ServeSource{
			Sessions: func() (int, int) { return 0, 1 },
			Cache:    func() (int64, int) { return 4096, 12 },
		},
	})
	assert.NoError(t, err)
	c := b.Serve.Cache
	assert.That(t, c != nil)
	assert.Equal(t, int64(30), c.HitCount)
	assert.Equal(t, int64(7), c.MissCount)
	assert.Equal(t, int64(3), c.SharedCount)
	assert.Equal(t, int64(5), c.EvictionCount)
	assert.Equal(t, int64(4096), c.Bytes)
	assert.Equal(t, 12, c.Entries)
}

// A process with two sections reports whichever moved last.
func TestCollect_LastActivityIsTheLaterSectionTimestamp(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	reader, m, meter := provider(t)
	ctx := context.Background()
	m.PipelineLastMessage.Record(ctx, 1757570000)
	last, err := meter.Int64Gauge("serve_last_request_timestamp")
	assert.NoError(t, err)
	last.Record(ctx, 1757570500)

	b, err := Collect(ctx, Source{
		Static:   static,
		Reader:   reader,
		Pipeline: &PipelineSource{},
		Serve:    &ServeSource{Sessions: func() (int, int) { return 0, 1 }},
	})
	assert.NoError(t, err)
	assert.Equal(t, int64(1757570500), b.LastActivityAt.Unix())
}

func TestCollect_AServeBundleStaysUnderTheCeiling(t *testing.T) {
	coverage.Covers(t, "observability.turbostats.serve")
	reader, meter := serveProvider(t)
	for _, name := range []string{"serve_requests", "serve_cache_hits", "serve_cache_misses"} {
		addCounter(t, meter, name, 184203311)
	}
	for _, name := range []string{"serve_request_errors", "serve_cache_shared", "serve_cache_evicted"} {
		addCounter(t, meter, name, 1842033)
	}
	last, err := meter.Int64Gauge("serve_last_request_timestamp")
	assert.NoError(t, err)
	last.Record(context.Background(), 1757570000)

	src := Source{
		Static: static,
		Reader: reader,
		Serve: &ServeSource{
			Sessions: func() (int, int) { return 64, 64 },
			Cache:    func() (int64, int) { return 1 << 30, 100000 },
		},
	}
	src.Static.IntervalSeconds = 60
	b, err := Collect(context.Background(), src)
	assert.NoError(t, err)
	b.Exit = &Exit{Reason: "SIGTERM", Code: 0}
	raw, err := json.Marshal(b)
	assert.NoError(t, err)
	assert.That(t, len(raw) < 4<<10)
}
