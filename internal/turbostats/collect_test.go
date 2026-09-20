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
func runSource(r *sdkmetric.ManualReader, stats func() (*core.StateStats, error)) Source {
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
	stats := func() (*core.StateStats, error) {
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
	stats := func() (*core.StateStats, error) { return nil, errors.New("unreadable") }

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

func TestCollect_TheBundleIsUnderOneKiB(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	reader, m, meter := provider(t)
	ctx := context.Background()
	// Nine-digit totals, the size a year of the Bluesky firehose reaches.
	m.MessageCount.Add(ctx, 184203311)
	m.HandlerRowsRead.Add(ctx, 184203311)
	m.SinkFlushCount.Add(ctx, 1842033)
	m.StateCommitCount.Add(ctx, 1842033)
	m.PipelineRowsAccepted.Add(ctx, 184203311)
	m.PipelineRowsWritten.Add(ctx, 184203311)
	m.PipelineLastMessage.Record(ctx, 1757570000)
	_ = meter
	size := int64(4194304)
	stats := func() (*core.StateStats, error) { return &core.StateStats{SizeBytes: size}, nil }

	b, err := Collect(ctx, runSource(reader, stats))
	assert.NoError(t, err)
	b.Exit = &Exit{Reason: "SIGTERM", Code: 0}
	raw, err := json.Marshal(b)
	assert.NoError(t, err)
	assert.That(t, len(raw) < 1024)
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
