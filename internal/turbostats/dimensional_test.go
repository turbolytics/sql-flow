package turbostats

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/turbolytics/sql-flow/internal/config"
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
	assert.That(t, plain.WindowNewestBucketAt == nil)
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

// A window keeping up reports no lag, whatever its size, and the most behind
// window is the one reported.
//
// The field used to be now minus the oldest watermark, so a healthy hourly
// window read an hour or two behind and a stalled one-minute window hid
// behind it. The manager now measures each window's overdue close in event
// time; the bundle takes the worst.
func TestCollect_WindowLagIsTheMostBehindWindow(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	reader, mp := windowed()
	hourly := managers.NewWindowMetrics(mp, "hourly")
	minute := managers.NewWindowMetrics(mp, "minute")
	hourly.CloseLag.Record(context.Background(), 0, win("hourly"))
	minute.CloseLag.Record(context.Background(), 1800, win("minute"))

	assert.Equal(t, int64(1800), *pipelineOf(t, reader).WindowLagSeconds)
}

// Rows stamped in the future are reported as the event time they claim, not
// as an age this host computed.
//
// This used to be an age clamped to zero as "a clock artefact". A watermark is
// event time from the data, and one device with a fast clock moves it past
// every correctly-timed row. Whether a bucket is ahead is a comparison with a
// clock someone trusts, and on a gateway with no real-time clock this host's
// is not it, so the bundle carries the timestamp and the receiver compares.
func TestCollect_RowsStampedInTheFutureArriveAsTheirEventTime(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	reader, mp := windowed()
	early := managers.NewWindowMetrics(mp, "hourly")
	late := managers.NewWindowMetrics(mp, "minute")
	tomorrow := time.Now().UTC().Add(24 * time.Hour).Truncate(time.Hour)
	early.NewestStart.Record(context.Background(), time.Now().Unix(), win("hourly"))
	late.NewestStart.Record(context.Background(), tomorrow.Unix(), win("minute"))

	p := pipelineOf(t, reader)
	assert.That(t, p.WindowNewestBucketAt != nil)
	assert.Equal(t, tomorrow, *p.WindowNewestBucketAt)
}

// Each window time is present when it has been measured, and absent until
// then.
//
// Close lag needs its own seen flag, because zero lag is a reading. The newest
// bucket is a Unix second, and an unset one must not report 1970: under the
// earlier watermark field, a window that had recorded nothing reported 1.79
// billion seconds of lag and every test still passed.
func TestCollect_WindowTimesArePresentOnlyOnceMeasured(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")

	reader, mp := windowed()
	managers.NewWindowMetrics(mp, "hourly") // constructed, never polled
	none := pipelineOf(t, reader)
	assert.That(t, none.WindowLagSeconds == nil)
	assert.That(t, none.WindowNewestBucketAt == nil)

	reader, mp = windowed()
	emptied := managers.NewWindowMetrics(mp, "hourly")
	emptied.CloseLag.Record(context.Background(), 0, win("hourly")) // polled, table empty
	lagOnly := pipelineOf(t, reader)
	assert.That(t, lagOnly.WindowLagSeconds != nil)
	assert.That(t, lagOnly.WindowNewestBucketAt == nil)
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

// --- payload bytes -----------------------------------------------------

// Payload bytes travel with the message count, and zero is sent.
func TestCollect_CarriesPayloadBytes(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	reader, m, _ := provider(t)
	m.MessageCount.Add(context.Background(), 4)
	m.MessagePayloadBytes.Add(context.Background(), 4096)

	p := pipelineOf(t, reader)
	assert.Equal(t, int64(4), p.MessageCount)
	assert.Equal(t, int64(4096), *p.MessagePayloadBytes)

	reader, _, _ = provider(t)
	quiet, err := Collect(context.Background(), runSource(reader, nil))
	assert.NoError(t, err)
	raw, err := json.Marshal(quiet)
	assert.NoError(t, err)
	assert.That(t, strings.Contains(string(raw), `"message_payload_bytes":0`))
}

// An engine that predates message_payload_bytes reads as unknown, not as a
// pipeline that received nothing.
func TestWire_AnOlderEnginesPayloadBytesDecodeAsUnknown(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	var p wire.Pipeline
	assert.NoError(t, json.Unmarshal([]byte(`{"message_count": 12}`), &p))
	assert.That(t, p.MessagePayloadBytes == nil)
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
				// labels are the operator's, from the config, and the config
				// refuses more than config.MaxLabels of them. The width is
				// set by the person who wrote the file, not by a broker's
				// partition count or a stream's error codes, and it cannot
				// change while the process runs.
				if at == ".Bundle.Instance.Labels" {
					continue
				}
				// A duration's buckets are a fixed-length array whose length
				// the contract sets: len(wire.DurationBounds)+1, the same
				// for every process forever. It is the one shape a receiver
				// can sum across a fleet, which is why the length is in the
				// contract rather than in the bundle.
				// TestCollect_ADurationHoldsItsInvariants pins the length.
				if rt == reflect.TypeOf(wire.Duration{}) && f.Name == "Buckets" {
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

// A bundle with every field at its widest value stays under 8 KiB.
//
// A smoke alarm for the shape, not the budget: 64-character ids, ten
// maximal labels and 2^62 in every counter and every bucket are not a
// bundle anyone sends. The budget is the realistic run bundle's guard.
//
// It was 4 KiB until the durations landed. Three phases of nine 2^62
// buckets took the widest bundle to 4212 bytes, which is the price of the
// one nested array the shape guard exempts. The receiver's limit is 16 KiB,
// so 8 leaves the alarm a margin without letting the shape double quietly.
func TestCollect_AFullBundleStaysUnderTheCeiling(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	big := int64(1) << 62
	n := 1 << 30
	at := time.Now().UTC()
	// The longest code in the taxonomy is shorter than this; a code is a
	// bounded string and this is the widest one worth pricing.
	code := "system.internal.unexpected"
	secs := 1e9
	b := wire.Bundle{
		V: wire.Version, SentAt: at, IntervalSeconds: 86400, LastActivityAt: &at,
		Instance: wire.Instance{
			ID: strings.Repeat("i", 64), Name: strings.Repeat("n", 64),
			Version: "v2026.09.21.12", Commit: strings.Repeat("c", 40),
			Arch: "linux/arm64", ConfigHash: "sha256:" + strings.Repeat("f", 64),
			SourceType: "websocket", SinkType: "clickhouse", HandlerType: "inferred_disk",
			Labels: widestLabels(t),
		},
		Process: wire.Process{StartedAt: at, RSSBytes: big, Goroutines: n},
		Pipeline: &wire.Pipeline{
			MessageCount: big, MessagePayloadBytes: &big, HandlerRowsRead: big, ErrorCount: big,
			SinkFlushCount: big, SinkRowsAccepted: big, SinkRowsWritten: big,
			StateCommitCount: big, StateDBSizeBytes: &big, LastMessageAt: &at,
			SinkRetryCount: &big, LagMaxMessages: &big, LagTotalMessages: &big,
			LagPartitions: &n, LagObservedAt: &at, LateRowsDropped: &big,
			LateRowsReemitted: &big, WindowClosedCount: &big,
			WindowLagSeconds: &big, WindowNewestBucketAt: &at,
			SourceErrorCount: &big, HandlerErrorCount: &big, SinkErrorCount: &big,
			StateErrorCount: &big, DLQRows: &big, LastErrorCode: &code,
			LastErrorAt: &at, RecvWaitSeconds: &secs,
			Duration: &wire.PipelineDurations{Batch: widestDuration(), SinkFlush: widestDuration()},
		},
		Serve: &wire.Serve{
			RequestCount: big, RequestErrorCount: big, SessionsInUse: n,
			SessionsTotal: n, LastRequestAt: &at,
			Cache: &wire.ServeCache{HitCount: big, MissCount: big, SharedCount: big,
				EvictionCount: big, Bytes: big, Entries: n},
			Duration: &wire.ServeDurations{Request: widestDuration()},
		},
		Exit: &wire.Exit{Reason: "system.internal.unexpected", Code: 255},
	}
	raw, err := json.Marshal(b)
	assert.NoError(t, err)
	t.Logf("a bundle with every field at its widest is %d bytes", len(raw))
	assert.That(t, len(raw) < 8<<10)
}

// widestLabels is the largest label set the config accepts: the most keys,
// each at its length limit, each with a value at its length limit.
//
// The shape guard exempts the label map because the config bounds it. This
// is what holds that exemption honest: the exemption is only true while
// the widest legal set still fits in the ceiling.
func widestLabels(t *testing.T) map[string]string {
	t.Helper()
	out := make(map[string]string, config.MaxLabels)
	for i := 0; i < config.MaxLabels; i++ {
		key := strings.Repeat("k", config.MaxLabelKeyLen-1) + fmt.Sprintf("%d", i)
		out[key] = strings.Repeat("v", config.MaxLabelValueLen)
	}
	// The config has to accept it, or this is measuring a set nobody can
	// write.
	ts := &config.TurboStats{Labels: out}
	assert.Equal(t, 0, len(ts.Check([]string{"pipeline", "turbostats"})))
	return out
}

// widestDuration is a duration whose every number is as wide as JSON makes
// it. Three of these ride in a full bundle, and the ceiling has to hold with
// them in it: a phase's buckets are the one nested array the shape guard
// exempts, so the size guard is what prices that exemption.
func widestDuration() *wire.Duration {
	buckets := make([]uint64, len(wire.DurationBounds)+1)
	for i := range buckets {
		buckets[i] = 1 << 62
	}
	return &wire.Duration{
		Count: 1 << 62, SumSeconds: 1e9, MinSeconds: 1e-9, MaxSeconds: 1e9,
		Buckets: buckets,
	}
}
