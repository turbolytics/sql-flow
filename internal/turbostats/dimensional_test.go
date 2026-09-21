package turbostats

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/turbolytics/sql-flow/internal/managers"
	"github.com/turbolytics/sql-flow/internal/sinks"
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

// windowed is a reader and provider for driving the real window instruments.
func windowed() (*sdkmetric.ManualReader, metric.MeterProvider) {
	reader := sdkmetric.NewManualReader()
	return reader, sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
}

func pipelineOf(t *testing.T, reader *sdkmetric.ManualReader) *wire.Pipeline {
	t.Helper()
	b, err := Collect(context.Background(), Source{Static: static, Reader: reader, Pipeline: &PipelineSource{}})
	assert.NoError(t, err)
	return b.Pipeline
}

func win(name string) metric.MeasurementOption {
	return metric.WithAttributes(attribute.String("window", name))
}

// --- lag ---------------------------------------------------------------

// Lag is reported as the worst partition, the total, and how many partitions
// those summarize.
//
// Three partitions at 5, 400 and 7. The max says one partition is stuck; the
// total alone, 412, would read as an ordinary backlog spread across three.
func TestCollect_LagIsTheWorstPartitionAndTheTotal(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	reader, m, _ := provider(t)
	m.Lag.Set("posts", 0, 5)
	m.Lag.Set("posts", 1, 400)
	m.Lag.Set("likes", 0, 7)

	p := pipelineOf(t, reader)
	assert.Equal(t, int64(400), *p.LagMaxMessages)
	assert.Equal(t, int64(412), *p.LagTotalMessages)
	assert.Equal(t, 3, *p.LagPartitions)
}

// A pipeline that has caught up reports zero, and one with no Kafka source
// reports nothing. They are different facts and must be different documents.
func TestCollect_CaughtUpIsZeroAndNoKafkaIsAbsent(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	reader, m, _ := provider(t)
	m.Lag.Set("posts", 0, 0)
	caughtUp, err := Collect(context.Background(), runSource(reader, nil))
	assert.NoError(t, err)
	raw, err := json.Marshal(caughtUp)
	assert.NoError(t, err)
	assert.That(t, strings.Contains(string(raw), `"lag_max_messages":0`))

	reader, _, _ = provider(t)
	noKafka, err := Collect(context.Background(), runSource(reader, nil))
	assert.NoError(t, err)
	raw, err = json.Marshal(noKafka)
	assert.NoError(t, err)
	assert.That(t, !strings.Contains(string(raw), "lag_"))
}

// Lag carries when it was measured.
//
// It is measured when a message is processed, so a consumer that stops
// receiving keeps its last reading -- usually zero -- while the backlog grows.
// Without the date, that frozen zero is the healthiest reading the bundle can
// give, shown at exactly the moment the pipeline has stopped.
func TestCollect_LagCarriesWhenItWasMeasured(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	reader, m, _ := provider(t)
	observed := time.Date(2026, 9, 21, 9, 0, 0, 0, time.UTC)
	m.Lag.Set("posts", 0, 0)
	m.LagObserved.Record(context.Background(), observed.Unix())

	p := pipelineOf(t, reader)
	assert.That(t, p.LagObservedAt != nil)
	assert.Equal(t, observed, *p.LagObservedAt)
}

// A partition taken away in a rebalance leaves this instance's bundle.
//
// It used to stay, at the lag it had when it left, for as long as the process
// ran, and a fleet summing lag counted it on two instances. A slow consumer
// is a common reason for a rebalance, so the value left behind was often a
// backlog this instance no longer owed.
func TestCollect_APartitionThatMovedAwayLeavesTheBundle(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	reader, m, _ := provider(t)
	m.Lag.Assigned(map[string][]int32{"posts": {0, 1, 2}})
	m.Lag.Set("posts", 0, 5)
	m.Lag.Set("posts", 1, 90000)
	m.Lag.Set("posts", 2, 7)
	m.Lag.Released(map[string][]int32{"posts": {1}})

	p := pipelineOf(t, reader)
	assert.Equal(t, 2, *p.LagPartitions)
	assert.Equal(t, int64(7), *p.LagMaxMessages)
	assert.Equal(t, int64(12), *p.LagTotalMessages)
}

// An instance holding no partitions is still a Kafka pipeline.
//
// All of its partitions can move elsewhere, and a group with more consumers
// than partitions leaves some holding none. Absent lag would say it has no
// Kafka source at all.
func TestCollect_AKafkaPipelineHoldingNoPartitionsReportsZero(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	reader, m, _ := provider(t)
	m.Lag.Assigned(map[string][]int32{"posts": {0}})
	m.Lag.Set("posts", 0, 40)
	m.LagObserved.Record(context.Background(), time.Now().Unix())
	m.Lag.Released(map[string][]int32{"posts": {0}})

	p := pipelineOf(t, reader)
	assert.That(t, p.LagPartitions != nil)
	assert.Equal(t, 0, *p.LagPartitions)
	assert.Equal(t, int64(0), *p.LagMaxMessages)
	assert.That(t, p.LagObservedAt != nil)
}

// --- window counters ---------------------------------------------------

// Late rows are split by what happened to them, and never summed.
//
// policy is an outcome: a dropped row is gone and a reemitted row is not, so
// their sum is true of neither. window is a shard, and collapses. This drives
// the real instruments, so a rename in managers fails here.
func TestCollect_LateRowsSplitByOutcomeAndCollapseByWindow(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	ctx := context.Background()
	reader, mp := windowed()
	hourly := managers.NewWindowMetrics(mp, "hourly")
	daily := managers.NewWindowMetrics(mp, "daily")
	drop := func(w string) metric.AddOption {
		return metric.WithAttributes(attribute.String("window", w), attribute.String("policy", string(managers.LateDrop)))
	}
	reemit := func(w string) metric.AddOption {
		return metric.WithAttributes(attribute.String("window", w), attribute.String("policy", string(managers.LateReemit)))
	}
	hourly.Late.Add(ctx, 3, drop("hourly"))
	daily.Late.Add(ctx, 4, drop("daily"))
	hourly.Late.Add(ctx, 10, reemit("hourly"))
	hourly.Closed.Add(ctx, 2, win("hourly"))
	daily.Closed.Add(ctx, 1, win("daily"))

	p := pipelineOf(t, reader)
	assert.Equal(t, int64(7), *p.LateRowsDropped)
	assert.Equal(t, int64(10), *p.LateRowsReemitted)
	assert.Equal(t, int64(3), *p.WindowClosedCount)
}

// A windowed pipeline reports its counters from the moment it starts.
//
// The window manager used to record nothing until its first close committed,
// so after a start or a restart a pipeline configured to drop late rows read
// as one that has no windows at all -- "nothing here drops rows".
func TestCollect_WindowCountersArePresentBeforeTheFirstClose(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	reader, mp := windowed()
	managers.NewWindowMetrics(mp, "hourly")

	p := pipelineOf(t, reader)
	assert.That(t, p.LateRowsDropped != nil)
	assert.That(t, p.LateRowsReemitted != nil)
	assert.That(t, p.WindowClosedCount != nil)
	assert.Equal(t, int64(0), *p.LateRowsDropped)

	reader, _, _ = provider(t)
	plain := pipelineOf(t, reader)
	assert.That(t, plain.LateRowsDropped == nil)
	assert.That(t, plain.LateRowsReemitted == nil)
	assert.That(t, plain.WindowClosedCount == nil)
	assert.That(t, plain.WindowLagSeconds == nil)
	assert.That(t, plain.WindowAheadSeconds == nil)
}

// A policy the contract has no field for leaves both late counts out.
//
// Adding it to neither would report the known counts as complete while some
// rows went uncounted, on the field an operator reads for data loss.
func TestCollect_AnUnknownLatePolicyReportsNeitherCount(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	reader, mp := windowed()
	w := managers.NewWindowMetrics(mp, "hourly")
	w.Late.Add(context.Background(), 5, metric.WithAttributes(
		attribute.String("window", "hourly"), attribute.String("policy", "quarantine")))

	p := pipelineOf(t, reader)
	assert.That(t, p.LateRowsDropped == nil)
	assert.That(t, p.LateRowsReemitted == nil)
	// The window is still there, and says so.
	assert.That(t, p.WindowClosedCount != nil)
}

// --- window times ------------------------------------------------------

// window reports one window's state as the manager records it: the newest
// bucket it holds, and when its next close is due. A window keeping up has
// its watermark a grace period behind the newest bucket, so the next close is
// due at the newest bucket's end.
func windowAt(w managers.WindowMetrics, name string, newest time.Time, due time.Time) {
	ctx := context.Background()
	w.NewestStart.Record(ctx, newest.Unix(), win(name))
	w.CloseDue.Record(ctx, due.Unix(), win(name))
}

// A window that is keeping up reports no lag, whatever its size.
//
// The field used to be now minus the oldest watermark, and a watermark trails
// the newest bucket by the grace period and moves only on a close. A healthy
// hourly window read 3,600 to 7,200 seconds behind while fully caught up.
func TestCollect_AWindowKeepingUpReportsNoLagWhateverItsSize(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	reader, mp := windowed()
	hourly := managers.NewWindowMetrics(mp, "hourly")
	current := time.Now().UTC().Truncate(time.Hour)
	windowAt(hourly, "hourly", current, current.Add(time.Hour))

	p := pipelineOf(t, reader)
	assert.Equal(t, int64(0), *p.WindowLagSeconds)
	assert.Equal(t, int64(0), *p.WindowAheadSeconds)
}

// A stalled small window shows through a healthy large one.
//
// Under the old field the hourly window's watermark was always the oldest, so
// a one-minute window stuck for half an hour never appeared.
func TestCollect_AStalledSmallWindowShowsThroughAHealthyLargeOne(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	reader, mp := windowed()
	hourly := managers.NewWindowMetrics(mp, "hourly")
	minute := managers.NewWindowMetrics(mp, "minute")
	now := time.Now().UTC()
	windowAt(hourly, "hourly", now.Truncate(time.Hour), now.Truncate(time.Hour).Add(time.Hour))
	stuck := now.Add(-31 * time.Minute).Truncate(time.Minute)
	windowAt(minute, "minute", stuck, stuck.Add(time.Minute))

	lag := *pipelineOf(t, reader).WindowLagSeconds
	assert.That(t, lag >= 29*60 && lag <= 31*60)
}

// A close that stops committing shows even while rows keep arriving.
//
// Measured from the newest bucket, this read zero: rows were current, so the
// window looked healthy while nothing it held was being published. Measured
// from the close that is due, it grows.
func TestCollect_AStalledCloseShowsWhileRowsKeepArriving(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	reader, mp := windowed()
	w := managers.NewWindowMetrics(mp, "minute")
	now := time.Now().UTC()
	windowAt(w, "minute", now.Truncate(time.Minute), now.Add(-20*time.Minute))

	lag := *pipelineOf(t, reader).WindowLagSeconds
	assert.That(t, lag >= 19*60 && lag <= 21*60)
}

// Rows stamped in the future are reported, not hidden.
//
// This used to be clamped to zero as "a clock artefact", but a watermark is
// event time from the data. One device whose clock is a day fast moves it a
// day ahead, and every correctly-timed row after that is late; under a drop
// policy each is deleted. The bundle showed its best possible reading.
func TestCollect_RowsStampedInTheFutureReportHowFarAhead(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	reader, mp := windowed()
	w := managers.NewWindowMetrics(mp, "hourly")
	tomorrow := time.Now().UTC().Add(24 * time.Hour).Truncate(time.Hour)
	windowAt(w, "hourly", tomorrow, tomorrow.Add(time.Hour))

	p := pipelineOf(t, reader)
	ahead := *p.WindowAheadSeconds
	assert.That(t, ahead >= 22*3600 && ahead <= 25*3600)
	assert.Equal(t, int64(0), *p.WindowLagSeconds)
}

// The window times need both readings, and travel together.
//
// Each is a subtraction from a Unix second, and an unset one is zero: the
// epoch. With the guard removed, a window that had recorded neither reported
// 1.79 billion seconds of lag and every test still passed.
func TestCollect_WindowTimesArePresentTogetherOrNotAtAll(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")

	reader, mp := windowed()
	managers.NewWindowMetrics(mp, "hourly") // no rows, no close
	none := pipelineOf(t, reader)
	assert.That(t, none.WindowLagSeconds == nil)
	assert.That(t, none.WindowAheadSeconds == nil)

	reader, mp = windowed()
	half := managers.NewWindowMetrics(mp, "hourly")
	half.CloseDue.Record(context.Background(), time.Now().Unix(), win("hourly"))
	onlyDue := pipelineOf(t, reader)
	assert.That(t, onlyDue.WindowLagSeconds == nil)
	assert.That(t, onlyDue.WindowAheadSeconds == nil)

	reader, mp = windowed()
	other := managers.NewWindowMetrics(mp, "hourly")
	other.NewestStart.Record(context.Background(), time.Now().Unix(), win("hourly"))
	onlyNewest := pipelineOf(t, reader)
	assert.That(t, onlyNewest.WindowLagSeconds == nil)
	assert.That(t, onlyNewest.WindowAheadSeconds == nil)
}

// --- sink retries ------------------------------------------------------

// Sink retries collapse across sinks, and zero is sent.
//
// Driven through the sinks package's own counter. A string that happened to
// match it passed this test after the instrument was renamed, and a bundle
// would then have reported zero retries for ever.
func TestCollect_SinkRetriesSumAcrossSinks(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	reader, mp := windowed()
	failed := errors.New("refused")
	postgres := sinks.RetryCounter(mp, "postgres")
	kafka := sinks.RetryCounter(mp, "kafka")
	postgres(1, failed)
	postgres(2, failed)
	kafka(1, failed)

	p := pipelineOf(t, reader)
	assert.Equal(t, int64(3), *p.SinkRetryCount)

	reader, _, _ = provider(t)
	quiet, err := Collect(context.Background(), runSource(reader, nil))
	assert.NoError(t, err)
	raw, err := json.Marshal(quiet)
	assert.NoError(t, err)
	assert.That(t, strings.Contains(string(raw), `"sink_retry_count":0`))
}

// An engine that predates sink_retry_count reads as unknown, not as zero.
//
// As a plain integer the field decoded to zero from a bundle that never
// carried it, so an old instance whose sink retried constantly read as one
// that never had. A receiver serving a mixed fleet cannot tell them apart any
// other way.
func TestWire_AnOlderEnginesRetriesDecodeAsUnknown(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	var p wire.Pipeline
	assert.NoError(t, json.Unmarshal([]byte(`{"message_count": 12}`), &p))
	assert.That(t, p.SinkRetryCount == nil)
}

// --- shape and size ----------------------------------------------------

// No field's presence or repetition depends on data cardinality.
//
// A map or a slice keyed by partition would make a bundle's width a function
// of the broker's partition count -- set by someone outside this system --
// and on a constrained link that width is paid every interval.
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
			SinkRetryCount: &big, LagMaxMessages: &big, LagTotalMessages: &big,
			LagPartitions: &n, LagObservedAt: &at, LateRowsDropped: &big,
			LateRowsReemitted: &big, WindowClosedCount: &big,
			WindowLagSeconds: &big, WindowAheadSeconds: &big,
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
