package turbostats

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/turbolytics/sql-flow/internal/activity"
	"github.com/turbolytics/sql-flow/internal/core"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/turbolytics/sql-flow/internal/managers"
	"github.com/turbolytics/sql-flow/internal/sinks"
	"github.com/turbolytics/sql-flow/turbostats/wire"
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
//
// It carries the three types and a label set, because every run process
// does. The size guards measure a bundle nobody sends otherwise.
func runSource(r *sdkmetric.ManualReader, stats func(context.Context) (*core.StateStats, error)) Source {
	s := static
	s.SourceType, s.SinkType, s.HandlerType = "kafka", "clickhouse", "inferred_mem"
	s.Labels = map[string]string{"region": "eu_west", "env": "prod"}
	return Source{Static: s, Reader: r, Pipeline: &PipelineSource{Stats: stats}}
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

// The bundle carries the contract's coarse distribution, never the engine's
// histogram. The engine has sixteen boundaries and names its instruments
// after itself; the wire has eight, fixed, so a fleet's buckets can be
// summed and a receiver needs no instrument names.
func TestCollect_TheEnginesHistogramIsNotInTheBundle(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	reader, m, _ := provider(t)
	ctx := context.Background()
	m.SinkFlushLatency.Record(ctx, 0.25)

	b, err := Collect(ctx, runSource(reader, nil))
	assert.NoError(t, err)
	raw, err := json.Marshal(b)
	assert.NoError(t, err)
	assert.That(t, !strings.Contains(string(raw), "latency"))
	assert.That(t, !strings.Contains(string(raw), "bounds"))
	// Nine counts, whatever the engine recorded into sixteen.
	assert.Equal(t, len(wire.DurationBounds)+1, len(b.Pipeline.Duration.SinkFlush.Buckets))
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

// A realistic run bundle, through the real collector, stays under 2 KiB: a
// Kafka pipeline with windows and a retrying sink, a year into the Bluesky
// firehose.
//
// This is the guard a constrained link needs. The bundle is paid for every
// interval, and on a metered or satellite link its size is the cost of
// telemetry. A contract-wide 4 KiB ceiling let this grow fourfold without
// failing anything; here, growing past the bound is a decision someone
// makes, not a drift someone finds on a bill.
//
// It measured 921 bytes at 1 KiB. The types and labels took it to 1041 and
// the bound to 2 KiB; the durations take it to 1121. Each of those was a
// decision, which is the point.
func TestCollect_ARealisticRunBundleStaysUnderItsCeiling(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	m, err := core.NewMetrics(mp)
	assert.NoError(t, err)
	ctx := context.Background()
	for partition := 0; partition < 32; partition++ {
		m.Lag.Set("bluesky.posts", int32(partition), 184203)
	}
	window := managers.NewWindowMetrics(mp, "posts_by_lang")
	w := metric.WithAttributes(attribute.String("window", "posts_by_lang"))
	window.Late.Add(ctx, 184203, metric.WithAttributes(
		attribute.String("window", "posts_by_lang"), attribute.String("policy", "drop")))
	window.Late.Add(ctx, 184203, metric.WithAttributes(
		attribute.String("window", "posts_by_lang"), attribute.String("policy", "reemit")))
	window.Closed.Add(ctx, 1842033, w)
	window.NewestStart.Record(ctx, 1757570000, w)
	window.CloseLag.Record(ctx, 0, w)
	sinks.RetryCounter(mp, "postgres")(1, errors.New("refused"))
	// Nine-digit totals, the size a year of the Bluesky firehose reaches.
	m.MessageCount.Add(ctx, 184203311)
	m.HandlerRowsRead.Add(ctx, 184203311)
	m.SinkFlushCount.Add(ctx, 1842033)
	m.StateCommitCount.Add(ctx, 1842033)
	m.PipelineRowsAccepted.Add(ctx, 184203311)
	m.PipelineRowsWritten.Add(ctx, 184203311)
	m.PipelineLastMessage.Record(ctx, 1757570000)
	m.LagObserved.Record(ctx, 1757570000)
	m.MessagePayloadBytes.Add(ctx, 184203311*412)
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
	// Two KiB, not one. The three types and a label set put a realistic run
	// bundle at 1041 bytes, and the v1 signals amendment adds durations and
	// lag on top. The receiver's limit is 16 KiB and a reporter sends once a
	// minute; this number exists to catch a field that scales with data, not
	// to shave bytes.
	assert.That(t, len(raw) < 2<<10)
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

// The serve section is unchanged by the dimensional fields, so it keeps the
// tighter guard it had. The 4 KiB ceiling is the whole contract's smoke
// alarm; this section growing past 1 KiB would still be news.
func TestCollect_AServeBundleStaysUnderOneKiB(t *testing.T) {
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
	assert.That(t, len(raw) < 1<<10)
}

func TestCollect_ReportsUptimeAndIdleFromTheProcessClock(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	reader, _, _ := provider(t)
	elapsed := 30 * time.Second
	clock := activity.Fake(func() time.Duration { return elapsed })
	clock.Mark()
	elapsed = 90*time.Second + 400*time.Millisecond

	src := runSource(reader, nil)
	src.Static.Clock = clock
	b, err := Collect(context.Background(), src)
	assert.NoError(t, err)

	assert.Equal(t, int64(90), *b.Process.UptimeSeconds)
	assert.Equal(t, int64(60), *b.IdleSeconds)
}

func TestCollect_NoWorkYetOmitsIdleAndKeepsUptime(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	reader, _, _ := provider(t)
	src := runSource(reader, nil)
	src.Static.Clock = activity.Fake(func() time.Duration { return 0 })

	b, err := Collect(context.Background(), src)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), *b.Process.UptimeSeconds)
	assert.That(t, b.IdleSeconds == nil)
}

// A Static built without a clock, as older callers and tests build it,
// sends neither field rather than a zero that reads as a fresh process.
func TestCollect_NoClockSendsNoDurations(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	reader, _, _ := provider(t)
	b, err := Collect(context.Background(), runSource(reader, nil))
	assert.NoError(t, err)
	assert.That(t, b.Process.UptimeSeconds == nil)
	assert.That(t, b.IdleSeconds == nil)
}

// The contract a receiver relies on: idle is at most uptime in every
// bundle, so idle > uptime can only mean a broken producer.
func TestCollect_IdleNeverExceedsUptime(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	reader, _, _ := provider(t)
	var elapsed time.Duration
	clock := activity.Fake(func() time.Duration { return elapsed })
	src := runSource(reader, nil)
	src.Static.Clock = clock
	for i := 0; i < 200; i++ {
		elapsed = time.Duration(i) * 1337 * time.Millisecond
		if i%3 == 0 {
			clock.Mark()
		}
		b, err := Collect(context.Background(), src)
		assert.NoError(t, err)
		if b.IdleSeconds != nil {
			assert.That(t, *b.IdleSeconds <= *b.Process.UptimeSeconds)
		}
	}
}

// The other half of the contract: activity recorded before a report is never
// after that report's sent_at. It also pins the unit. The gauge is whole
// seconds, and one recorded in milliseconds would decode as a date around
// the year 57,000 and fail here. It does not pin whether Collect reads the
// instruments before or after stamping sent_at, because the activity is
// recorded before Collect runs.
func TestCollect_ActivityIsNeverAfterSentAt(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	reader, m, _ := provider(t)
	ctx := context.Background()
	m.PipelineLastMessage.Record(ctx, time.Now().Unix())

	b, err := Collect(ctx, runSource(reader, nil))
	assert.NoError(t, err)
	assert.That(t, b.LastActivityAt != nil)
	assert.That(t, !b.LastActivityAt.After(b.SentAt))
}

func TestCollect_CarriesTheTypesAndLabels(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	reader, _, _ := provider(t)
	src := runSource(reader, nil)
	src.Static.SourceType = "kafka"
	src.Static.SinkType = "postgres"
	src.Static.HandlerType = "structured"
	src.Static.Labels = map[string]string{"region": "eu_west"}

	b, err := Collect(context.Background(), src)
	assert.NoError(t, err)
	assert.Equal(t, "kafka", b.Instance.SourceType)
	assert.Equal(t, "postgres", b.Instance.SinkType)
	assert.Equal(t, "structured", b.Instance.HandlerType)
	assert.Equal(t, "eu_west", b.Instance.Labels["region"])
}

// The labels a process reports are the ones it started with. Collect copies
// the map so a caller that mutates its own cannot change a bundle already
// built, or a bundle being built on another goroutine.
func TestCollect_CopiesTheLabels(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	reader, _, _ := provider(t)
	labels := map[string]string{"region": "eu_west"}
	src := runSource(reader, nil)
	src.Static.Labels = labels

	b, err := Collect(context.Background(), src)
	assert.NoError(t, err)
	labels["region"] = "us_east"
	assert.Equal(t, "eu_west", b.Instance.Labels["region"])
}

// The engine's boundaries are finer than the wire's, and every wire
// boundary is one of them, so the coarse counts are sums of fine ones and
// nothing is interpolated. If a boundary is ever dropped from
// latencyBuckets this test fails rather than silently reporting a
// distribution that is wrong.
func TestDurationOf_SumsTheEnginesBucketsIntoTheWires(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	reader, m, _ := provider(t)
	ctx := context.Background()
	for _, seconds := range []float64{0.0005, 0.002, 0.02, 0.2, 0.7, 3, 40, 120} {
		m.BatchProcessingLatency.Record(ctx, seconds)
	}

	b, err := Collect(ctx, runSource(reader, nil))
	assert.NoError(t, err)
	d := b.Pipeline.Duration.Batch
	assert.Equal(t, uint64(8), d.Count)
	assert.Equal(t, 9, len(d.Buckets))
	// Each sample lands in the first wire bucket whose boundary is at or
	// above it. Nothing lands in the 30 s bucket: the 40 s sample is the
	// next one up.
	assert.DeepEqual(t, []uint64{1, 1, 1, 1, 1, 1, 0, 1, 1}, d.Buckets)

	var sum float64
	for _, c := range d.Buckets {
		sum += float64(c)
	}
	assert.Equal(t, float64(d.Count), sum)
	assert.Equal(t, 0.0005, d.MinSeconds)
	assert.Equal(t, float64(120), d.MaxSeconds)
}

// A phase nothing recorded is absent, and so is the group when no phase
// recorded anything. Zeros would say the work happened and took no time.
func TestCollect_DurationsAreAbsentUntilRecorded(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	reader, _, _ := provider(t)
	b, err := Collect(context.Background(), runSource(reader, nil))
	assert.NoError(t, err)
	assert.That(t, b.Pipeline.Duration == nil)
}

func TestCollect_SinkFlushDurationIsItsOwnPhase(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	reader, m, _ := provider(t)
	ctx := context.Background()
	m.SinkFlushLatency.Record(ctx, 0.3)
	m.SinkFlushLatency.Record(ctx, 0.4)

	b, err := Collect(ctx, runSource(reader, nil))
	assert.NoError(t, err)
	assert.That(t, b.Pipeline.Duration.Batch == nil)
	assert.Equal(t, uint64(2), b.Pipeline.Duration.SinkFlush.Count)
	assert.Equal(t, 0.3, b.Pipeline.Duration.SinkFlush.MinSeconds)
	assert.Equal(t, 0.4, b.Pipeline.Duration.SinkFlush.MaxSeconds)
}

// The contract claims a phase's buckets sum to its count, and that min is
// at most max. A receiver that trusts the first claim reads a percentile
// from the buckets alone; a fold that drops or double-counts a bucket makes
// every one of those wrong, quietly. So a real bundle asserts it.
func TestCollect_ADurationHoldsItsInvariants(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	reader, m, _ := provider(t)
	ctx := context.Background()
	for _, seconds := range []float64{0.0005, 0.004, 0.02, 0.1, 0.5, 2, 12, 40, 90} {
		m.BatchProcessingLatency.Record(ctx, seconds)
	}

	b, err := Collect(ctx, runSource(reader, nil))
	assert.NoError(t, err)
	d := b.Pipeline.Duration.Batch
	var summed uint64
	for _, c := range d.Buckets {
		summed += c
	}
	assert.Equal(t, d.Count, summed)
	assert.Equal(t, uint64(9), d.Count)
	assert.That(t, d.MinSeconds <= d.MaxSeconds)
	assert.Equal(t, len(wire.DurationBounds)+1, len(d.Buckets))
}

// One error_count told an operator that something failed. The phase tells
// them where to look, and the three are the phases the engine attributes
// errors to.
func TestCollect_ErrorsAreCountedPerPhase(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	reader, m, _ := provider(t)
	ctx := context.Background()
	// Both counters, the way recordError moves them: the attributed one
	// carries the phase and the flat twin is the total.
	record := func(phase string, n int) {
		for i := 0; i < n; i++ {
			m.ErrorCount.Add(ctx, 1, metric.WithAttributes(
				attribute.String("class", "system"),
				attribute.String("domain", "sink"),
				attribute.String("code", "system.sink.unreachable"),
				attribute.String("phase", phase),
			))
			m.PipelineErrors.Add(ctx, 1)
		}
	}
	record("handler.invoke", 3)
	record("sink.flush", 5)
	record("state.commit", 7)

	b, err := Collect(ctx, runSource(reader, nil))
	assert.NoError(t, err)
	assert.Equal(t, int64(3), *b.Pipeline.HandlerErrorCount)
	assert.Equal(t, int64(5), *b.Pipeline.SinkErrorCount)
	assert.Equal(t, int64(7), *b.Pipeline.StateErrorCount)
	// The total stays authoritative, and is at least the phases' sum.
	assert.Equal(t, int64(15), b.Pipeline.ErrorCount)
}

// A pipeline that has failed nothing reports zeros, not absence: the engine
// counts these, and zero is a reading.
func TestCollect_NoErrorsReportsZeroPerPhase(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	reader, _, _ := provider(t)
	b, err := Collect(context.Background(), runSource(reader, nil))
	assert.NoError(t, err)
	assert.Equal(t, int64(0), *b.Pipeline.HandlerErrorCount)
	assert.Equal(t, int64(0), *b.Pipeline.SinkErrorCount)
	assert.Equal(t, int64(0), *b.Pipeline.StateErrorCount)
}

// The engine attributes no error to a source phase: a read that fails is
// the source's own retry, and nothing calls recordError with one. So the
// field is absent rather than zero, which would say the source has never
// failed. It appears on its own the day a source phase is recorded.
func TestCollect_NoSourcePhaseNoSourceErrorCount(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	reader, m, _ := provider(t)
	ctx := context.Background()
	m.ErrorCount.Add(ctx, 1, metric.WithAttributes(attribute.String("phase", "sink.flush")))

	b, err := Collect(ctx, runSource(reader, nil))
	assert.NoError(t, err)
	assert.That(t, b.Pipeline.SourceErrorCount == nil)

	m.ErrorCount.Add(ctx, 4, metric.WithAttributes(attribute.String("phase", "source.read")))
	b, err = Collect(ctx, runSource(reader, nil))
	assert.NoError(t, err)
	assert.Equal(t, int64(4), *b.Pipeline.SourceErrorCount)
}

// The DLQ's rows are not the pipeline's rows, and they are not errors
// either: they are what a row's failure cost. They come from the counting
// sink's role attribute.
func TestCollect_DLQRowsComeFromTheDLQsRole(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	reader, _, meter := provider(t)
	ctx := context.Background()
	written, err := meter.Int64Counter("sink_rows_written")
	assert.NoError(t, err)
	written.Add(ctx, 9, metric.WithAttributes(
		attribute.String("sink", "kafka"), attribute.String("role", "dlq")))
	written.Add(ctx, 400, metric.WithAttributes(
		attribute.String("sink", "postgres"), attribute.String("role", "pipeline")))

	b, err := Collect(ctx, runSource(reader, nil))
	assert.NoError(t, err)
	assert.Equal(t, int64(9), *b.Pipeline.DLQRows)
}

// The code and the time, never the message: a message carries the row that
// failed and whatever was in it.
func TestCollect_CarriesTheLastErrorCodeAndTime(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	reader, _, _ := provider(t)
	at := time.Date(2026, 9, 22, 12, 0, 0, 0, time.UTC)
	src := runSource(reader, nil)
	src.Pipeline.LastError = func() (string, time.Time, bool) {
		return "user.sink.encode_failed", at, true
	}

	b, err := Collect(context.Background(), src)
	assert.NoError(t, err)
	assert.Equal(t, "user.sink.encode_failed", *b.Pipeline.LastErrorCode)
	assert.Equal(t, at, b.Pipeline.LastErrorAt.UTC())
}

func TestCollect_NoErrorYetCarriesNoCode(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	reader, _, _ := provider(t)
	src := runSource(reader, nil)
	src.Pipeline.LastError = func() (string, time.Time, bool) { return "", time.Time{}, false }

	b, err := Collect(context.Background(), src)
	assert.NoError(t, err)
	assert.That(t, b.Pipeline.LastErrorCode == nil)
	assert.That(t, b.Pipeline.LastErrorAt == nil)
}

func TestCollect_CarriesTheConsumeLoopsWait(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	reader, m, _ := provider(t)
	m.RecvWaitSeconds.Add(context.Background(), 1.5)

	b, err := Collect(context.Background(), runSource(reader, nil))
	assert.NoError(t, err)
	assert.Equal(t, 1.5, *b.Pipeline.RecvWaitSeconds)
}

// The four travel together: a reading, its age, the worst since start, and
// where the event time came from.
func TestCollect_CarriesTheEventLagAndItsBasis(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	reader, m, _ := provider(t)
	ctx := context.Background()
	m.EventLagSeconds.Record(ctx, 12.5)
	m.EventLagMaxSeconds.Record(ctx, 90)
	m.EventLagObserved.Record(ctx, time.Date(2026, 9, 22, 12, 0, 0, 0, time.UTC).Unix())

	src := runSource(reader, nil)
	src.Pipeline.EventBasis = "kafka_create_time"
	b, err := Collect(ctx, src)
	assert.NoError(t, err)

	assert.Equal(t, 12.5, *b.Pipeline.EventLagSeconds)
	assert.Equal(t, float64(90), *b.Pipeline.EventLagMaxSeconds)
	assert.Equal(t, "kafka_create_time", *b.Pipeline.EventLagBasis)
	assert.Equal(t, 2026, b.Pipeline.EventLagObservedAt.UTC().Year())
}

// A source with no event time sends none of the four, not zeros: a zero lag
// says the pipeline is caught up.
func TestCollect_NoBasisNoLagFields(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	reader, m, _ := provider(t)
	// The instruments moved; without a basis the bundle still sends none of
	// the four, because a reading whose meaning is unknown is not a reading.
	m.EventLagSeconds.Record(context.Background(), 12.5)

	b, err := Collect(context.Background(), runSource(reader, nil))
	assert.NoError(t, err)
	assert.That(t, b.Pipeline.EventLagSeconds == nil)
	assert.That(t, b.Pipeline.EventLagBasis == nil)
	assert.That(t, b.Pipeline.EventLagObservedAt == nil)
}

// A rollup source fills both sections, and a process without one sends
// neither.
func TestCollect_ARollupSourceFillsBothSections(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	reader := sdkmetric.NewManualReader()
	_ = sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	at := time.Now().UTC()
	src := Source{Static: Static{Version: "v", StartedAt: at}, Reader: reader,
		Rollup: &RollupSource{Section: func() (*Rollup, *Freshness) {
			return &Rollup{Role: "leader", Rollups: []RollupEntry{{Name: "posts", Strategy: "trigger"}}},
				&Freshness{StoreID: "pg:0123456789abcdef", StoreIDKind: "system", ObservedAt: at,
					Tables: []FreshTable{{Table: "public.posts_1h", GrainSeconds: 3600}}}
		}}}
	b, err := Collect(context.Background(), src)
	assert.NoError(t, err)
	assert.Equal(t, "leader", b.Rollup.Role)
	assert.Equal(t, 1, len(b.Freshness.Tables))
	assert.That(t, b.Pipeline == nil && b.Serve == nil)

	src.Rollup = nil
	b, err = Collect(context.Background(), src)
	assert.NoError(t, err)
	assert.That(t, b.Rollup == nil && b.Freshness == nil)
}
