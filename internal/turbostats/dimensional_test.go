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
// outcome is an outcome: a refused row is gone and a recomputed row is in
// its bucket's republished value, so their sum is true of neither. window is
// a shard, and collapses. This drives the real instruments -- the engine's
// counter, since the engine decides lateness -- so a rename fails here.
func TestCollect_LateRowsSplitByOutcomeAndCollapseByWindow(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	ctx := context.Background()
	reader, mp := windowed()
	engine, err := core.NewMetrics(mp)
	assert.NoError(t, err)
	hourly := managers.NewWindowMetrics(mp, "hourly")
	daily := managers.NewWindowMetrics(mp, "daily")
	late := func(w, outcome string) metric.AddOption {
		return metric.WithAttributes(attribute.String("window", w), attribute.String("outcome", outcome))
	}
	engine.WindowLateRows.Add(ctx, 3, late("hourly", "refused"))
	engine.WindowLateRows.Add(ctx, 4, late("daily", "refused"))
	engine.WindowLateRows.Add(ctx, 10, late("hourly", "recomputed"))
	hourly.Closed.Add(ctx, 2, win("hourly"))
	daily.Closed.Add(ctx, 1, win("daily"))

	p := pipelineOf(t, reader)
	assert.Equal(t, int64(7), *p.LateRowsDropped)
	assert.Equal(t, int64(10), *p.LateRowsRecomputed)
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
	assert.That(t, p.LateRowsRecomputed != nil)
	assert.That(t, p.WindowClosedCount != nil)
	assert.Equal(t, int64(0), *p.LateRowsDropped)

	reader, _, _ = provider(t)
	plain := pipelineOf(t, reader)
	assert.That(t, plain.LateRowsDropped == nil)
	assert.That(t, plain.LateRowsRecomputed == nil)
	assert.That(t, plain.WindowClosedCount == nil)
	assert.That(t, plain.WindowLagSeconds == nil)
	assert.That(t, plain.WindowNewestBucketAt == nil)
}

// An outcome the contract has no field for leaves both late counts out.
//
// Adding it to neither would report the known counts as complete while some
// rows went uncounted, on the field an operator reads for data loss.
func TestCollect_AnUnknownLateOutcomeReportsNeitherCount(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	reader, mp := windowed()
	managers.NewWindowMetrics(mp, "hourly")
	engine, err := core.NewMetrics(mp)
	assert.NoError(t, err)
	engine.WindowLateRows.Add(context.Background(), 5, metric.WithAttributes(
		attribute.String("window", "hourly"), attribute.String("outcome", "quarantine")))

	p := pipelineOf(t, reader)
	assert.That(t, p.LateRowsDropped == nil)
	assert.That(t, p.LateRowsRecomputed == nil)
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
				// A rollup daemon's tables and rollups come from its rollups
				// file, and config.MaxReportedRollupTables caps the tables of
				// a file that reports. Every rollup declares a table, so the
				// cap bounds both lists. TestCollect_AFullRollupBundleStaysUnderTheCeiling
				// prices the widest legal file.
				if at == ".Bundle.Freshness.Tables" || at == ".Bundle.Rollup.Rollups" {
					continue
				}
				// A database bundle's tables are the operator's list, capped
				// by wire.MaxDatabaseTables; its replicas are the cluster's
				// topology, capped by wire.MaxDatabaseReplicas; its errors
				// are at most one per table and one per catalog query,
				// wire.MaxDatabaseErrors. None grows with the data in the
				// database. TestCollect_AFullDatabaseBundleStaysUnderTheCeiling
				// prices all three at their caps.
				if at == ".Bundle.Database.Tables" || at == ".Bundle.Database.Replication.Replicas" || at == ".Bundle.Database.Collection.Errors" {
					continue
				}
				// The queries doing the work, opt-in and at most
				// wire.MaxDatabaseQueries, each with normalized text of at
				// most 200 characters; the same test prices them.
				if at == ".Bundle.Database.Queries" {
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

// A bundle with every field at its widest value stays under 10 KiB.
//
// A smoke alarm for the shape, not the budget: 64-character ids, ten
// maximal labels and 2^62 in every counter and every bucket are not a
// bundle anyone sends. The budget is the realistic run bundle's guard.
//
// It was 4 KiB until the durations landed. Three phases of nine 2^62
// buckets took the widest bundle to 4212 bytes, which is the price of the
// one nested array the shape guard exempts. It was 8 KiB until the
// database section's load landed, with one of each of its lists and every
// load field: 8.7 KiB on 2026-10-06, and 9183 bytes on 2026-10-07 with the
// one table's schema hash, one schema change and its write rates. The
// receiver's limit is 16 KiB, so 10 leaves the alarm a margin without
// letting the shape double quietly; the next field here costs the margin.
func TestCollect_AFullBundleStaysUnderTheCeiling(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	raw, err := json.Marshal(widestBundle(t))
	assert.NoError(t, err)
	t.Logf("a bundle with every field at its widest is %d bytes", len(raw))
	assert.That(t, len(raw) < 10<<10)
}

// widestBundle is a bundle with every field at its widest value. The size
// guard prices it, and the schema test validates it, so a field added to
// the contract and forgotten here fails neither.
func widestBundle(t *testing.T) wire.Bundle {
	t.Helper()
	big := int64(1) << 62
	n := 1 << 30
	at := time.Now().UTC()
	// The longest code in the taxonomy is shorter than this; a code is a
	// bounded string and this is the widest one worth pricing.
	code := "system.internal.unexpected"
	secs := 1e9
	basis := "kafka_log_append_time"
	connected := true
	return wire.Bundle{
		V:               wire.Version,
		SentAt:          at,
		IntervalSeconds: 86400,
		LastActivityAt:  &at,
		IdleSeconds:     &big,
		Instance: wire.Instance{
			ID:              strings.Repeat("i", 64),
			Name:            strings.Repeat("n", 64),
			Version:         "v2026.09.21.12",
			Commit:          strings.Repeat("c", 40),
			Arch:            "linux/arm64",
			ConfigHash:      "sha256:" + strings.Repeat("f", 64),
			SourceType:      "websocket",
			SinkType:        "clickhouse",
			HandlerType:     "inferred_disk",
			Runtime:         "kafka-connect",
			RuntimeVersion:  "3.8.0-ccs",
			ReporterVersion: "v2026.10.03.12",
			Labels:          widestLabels(t),
		},
		Process: wire.Process{
			ID:               strings.Repeat("a", 32),
			Host:             strings.Repeat("h", 64),
			StartedAt:        at,
			UptimeSeconds:    &big,
			RSSBytes:         big,
			Goroutines:       n,
			GoRetainedBytes:  big,
			GoHeapBytes:      big,
			MemoryLimitBytes: big,
			Memory: &wire.Memory{
				Runtime:        "jvm",
				RetainedBytes:  big,
				LiveBytes:      big,
				HeapLimitBytes: big,
				GCCount:        big,
			},
		},
		Pipeline: &wire.Pipeline{
			State:                "starting",
			StartedAt:            &at,
			RestartCount:         &big,
			SourceConnected:      &connected,
			MessageCount:         big,
			MessagePayloadBytes:  &big,
			HandlerRowsRead:      big,
			ErrorCount:           big,
			SinkFlushCount:       &big,
			SinkRowsAccepted:     big,
			SinkRowsWritten:      big,
			StateCommitCount:     big,
			StateDBSizeBytes:     &big,
			LastMessageAt:        &at,
			LastSinkWriteAt:      &at,
			ErrorRowsDropped:     &big,
			SourceWireBytes:      &big,
			SinkWireBytes:        &big,
			SinkRetryCount:       &big,
			LagMaxMessages:       &big,
			LagTotalMessages:     &big,
			LagPartitions:        &n,
			LagObservedAt:        &at,
			LateRowsDropped:      &big,
			LateRowsRecomputed:   &big,
			WindowClosedCount:    &big,
			WindowLagSeconds:     &big,
			WindowNewestBucketAt: &at,
			SourceErrorCount:     &big,
			HandlerErrorCount:    &big,
			SinkErrorCount:       &big,
			StateErrorCount:      &big,
			DLQRows:              &big,
			LastErrorCode:        &code,
			LastErrorAt:          &at,
			RecvWaitSeconds:      &secs,
			EventLagSeconds:      &secs,
			EventLagMaxSeconds:   &secs,
			EventLagObservedAt:   &at,
			EventLagBasis:        &basis,
			Duration: &wire.PipelineDurations{
				Batch:     widestDuration(),
				SinkFlush: widestDuration(),
			},
			Backfill: &wire.Backfill{
				State:          "completed",
				BlocksStream:   true,
				ElapsedSeconds: &big,
				Unit:           "partition",
				UnitsTotal:     &n,
				UnitsLeft:      &n,
				RowsRead:       &big,
			},
		},
		Serve: &wire.Serve{
			RequestCount:      big,
			RequestErrorCount: big,
			SessionsInUse:     n,
			SessionsTotal:     n,
			LastRequestAt:     &at,
			Cache: &wire.ServeCache{
				HitCount:      big,
				MissCount:     big,
				SharedCount:   big,
				EvictionCount: big,
				Bytes:         big,
				Entries:       n,
			},
			Duration: &wire.ServeDurations{Request: widestDuration()},
		},
		// One of each repeated field: enough for the schema to validate
		// the shape. The full width is priced by
		// TestCollect_AFullDatabaseBundleStaysUnderTheCeiling.
		Database: widestDatabase(1, 1, 1, 1, 1),
		Exit: &wire.Exit{
			Reason: "system.internal.unexpected",
			Code:   255,
		},
	}
}

// widestDatabase is a database section with every field at its widest
// value: the longest identifiers Postgres allows, the longest host name DNS
// allows, and 2^62 in every count, with the given number of tables,
// replicas, errors and queries, and every load field set.
func widestDatabase(tables, replicas, errors, queries, changes int) *wire.Database {
	big := int64(1) << 62
	n := 1 << 30
	at := time.Now().UTC()
	secs := 1e9
	ratio := 0.123456789
	ident := strings.Repeat("i", 63)
	table := strings.Repeat("s", 63) + "." + strings.Repeat("t", 63)
	host := strings.Repeat("h", 253)
	d := &wire.Database{
		Kind:          "postgres",
		Target:        host + ":65535/" + ident,
		Cluster:       strings.Repeat("c", 64),
		ServerVersion: "PostgreSQL 18.1 (Debian 18.1-1.pgdg13+1) on aarch64-unknown-linux-gnu, compiled by gcc (Debian 14.2.0-19) 14.2.0, 64-bit",
		Probe:         wire.DatabaseProbe{OK: true, LatencyMs: big, LastOKAt: &at, ConsecutiveFailures: n, Error: "timeout"},
		Resources: &wire.DatabaseResources{
			Connections:              &wire.DatabaseConnections{Used: n, Max: n, Waiting: n},
			SizeBytes:                &big,
			OldestTransactionSeconds: &big,
			Memory:                   &wire.DatabaseMemory{SharedBuffersBytes: big},
		},
		Replication: &wire.DatabaseReplication{Role: "primary", LagSeconds: &secs, LastReplayedAt: &at, Upstream: host + ":65535"},
		Load: &wire.DatabaseLoad{
			SessionsActiveNow: &n, SessionsIdleInTransactionNow: &n, SessionsWaitingNow: &n, QueriesQueuedNow: &n,
			LongestQuerySeconds: &secs, QueriesPerSecond: &secs, TransactionsPerSecond: &secs, RollbacksPerSecond: &secs,
			RowsReadPerSecond: &secs, RowsWrittenPerSecond: &secs, BytesScannedPerSecond: &secs, CacheHitRatio: &ratio,
			DeadlocksPerSecond: &secs, TempBytesPerSecond: &secs,
		},
		Collection: wire.DatabaseCollection{Queries: n, DurationMs: big},
	}
	for i := 0; i < queries; i++ {
		d.Queries = append(d.Queries, wire.DatabaseQuery{
			ID: strings.Repeat("q", 64), Text: strings.Repeat("t", 200),
			CallsPerSecond: secs, MeanMs: secs, TimeShare: ratio, RowsPerCall: secs,
		})
	}
	for i := 0; i < tables; i++ {
		d.Tables = append(d.Tables, wire.DatabaseTable{
			Name: table, FreshnessColumn: ident, NewestAt: &at, Rows: &big, RowsExact: true,
			SizeBytes: big, LastVacuumAt: &at, CheckedAt: at,
			DeadRows: &big, SeqScansPerSecond: &secs, IndexScansPerSecond: &secs,
			SchemaHash:            strings.Repeat("a", 16),
			RowsInsertedPerSecond: &secs, RowsUpdatedPerSecond: &secs, RowsDeletedPerSecond: &secs,
		})
	}
	// The schema changes, bundle-wide, land on the tables in turn.
	for i := 0; i < changes && len(d.Tables) > 0; i++ {
		t := &d.Tables[i%len(d.Tables)]
		t.SchemaChanges = append(t.SchemaChanges, wire.DatabaseSchemaChange{
			Column: ident, Change: "nullability", From: strings.Repeat("f", wire.MaxDatabaseTypeText), To: strings.Repeat("t", wire.MaxDatabaseTypeText),
		})
	}
	for i := 0; i < replicas; i++ {
		d.Replication.Replicas = append(d.Replication.Replicas, wire.DatabaseReplica{Name: ident, LagSeconds: &secs, State: "streaming"})
	}
	for i := 0; i < errors; i++ {
		d.Collection.Errors = append(d.Collection.Errors, wire.DatabaseError{Table: table, Error: strings.Repeat("e", 200)})
	}
	return d
}

// The widest database bundle the contract allows stays under 128 KiB:
// wire.MaxDatabaseTables tables, each with the longest schema and table
// name Postgres allows, its load counters, its write rates and a schema
// hash, wire.MaxDatabaseSchemaChanges changes at the widest type names,
// wire.MaxDatabaseReplicas replicas, an error for every table and every
// catalog query, every load field, wire.MaxDatabaseQueries queries at 200
// characters, and every count at 2^62. It measured 47725 bytes on
// 2026-10-05 before load, 60925 with it on 2026-10-06, and 84037 with
// the schema and the writes on 2026-10-07, past the 64 KiB a receiver
// allowed before; a receiver that accepts database bundles allows 128 KiB
// now, which is what the load spec first asked for. A field added here is
// paid for by this comment saying what it measured. The 16 KiB limit that
// holds every pipeline bundle cannot hold it.
func TestCollect_AFullDatabaseBundleStaysUnderTheCeiling(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	at := time.Now().UTC()
	b := wire.Bundle{
		V: wire.Version, SentAt: at, IntervalSeconds: 86400, LastActivityAt: &at,
		Instance: wire.Instance{
			ID: strings.Repeat("i", 64), Name: strings.Repeat("n", 64), Version: "v2026.10.05", Commit: strings.Repeat("c", 40),
			Arch: "linux/arm64", ConfigHash: "sha256:" + strings.Repeat("f", 64), Labels: widestLabels(t),
		},
		Process:  wire.Process{StartedAt: at, RSSBytes: 1 << 62, Goroutines: 1 << 30},
		Database: widestDatabase(wire.MaxDatabaseTables, wire.MaxDatabaseReplicas, wire.MaxDatabaseErrors, wire.MaxDatabaseQueries, wire.MaxDatabaseSchemaChanges),
	}
	raw, err := json.Marshal(b)
	assert.NoError(t, err)
	t.Logf("the widest database bundle is %d bytes", len(raw))
	assert.That(t, len(raw) < 128<<10)
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

// The widest rollup bundle the config allows stays under 14 KiB: the most
// tables a reporting file may declare, each with the longest name Postgres
// allows under the longest schema, a source per rollup, a rollup per table,
// and every optional field at its widest. It measured 12375 bytes on
// 2026-09-26; the receiver's limit is 16 KiB, so 14 leaves the alarm 2 KiB.
func TestCollect_AFullRollupBundleStaysUnderTheCeiling(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	big := int64(1) << 62
	at := time.Now().UTC()
	name := strings.Repeat("n", 63)
	table := strings.Repeat("s", 63) + "." + strings.Repeat("t", 63)
	code := "system.rollup.unreachable"
	n := config.MaxReportedRollupTables
	fresh := make([]wire.FreshTable, 0, 2*n)
	entries := make([]wire.RollupEntry, 0, n)
	for i := 0; i < n; i++ {
		fresh = append(fresh,
			wire.FreshTable{Table: table, GrainSeconds: big, NewestBucketAt: &at},
			wire.FreshTable{Table: table, GrainSeconds: big, NewestBucketAt: &at})
		entries = append(entries, wire.RollupEntry{
			Name: name, Strategy: "trigger",
			Backfill:          &wire.RollupBackfill{TablesLeft: n, HistoryLeftSeconds: &big},
			VerifyBucketCount: big, DriftBucketCount: big,
			Completeness: &wire.RollupCompleteness{Table: table, BucketAt: at, SourceBuckets: big, ExpectedBuckets: big},
			Triggers:     &wire.RollupTriggers{Calls: big, TotalSeconds: 1e18},
		})
	}
	b := wire.Bundle{
		V: wire.Version, SentAt: at, IntervalSeconds: 86400, LastActivityAt: &at,
		Instance: wire.Instance{
			ID: strings.Repeat("i", 64), Version: "v2026.09.21.12", Commit: strings.Repeat("c", 40),
			Arch: "linux/arm64", ConfigHash: "sha256:" + strings.Repeat("f", 64), Labels: widestLabels(t),
		},
		Process:   wire.Process{StartedAt: at, RSSBytes: big, Goroutines: 1 << 30},
		Freshness: &wire.Freshness{StoreID: "pg:" + strings.Repeat("f", 16), StoreIDKind: "address", ObservedAt: at, Tables: fresh},
		Rollup:    &wire.Rollup{Role: "starting", Rollups: entries, LastErrorCode: &code, LastErrorAt: &at},
		Exit:      &wire.Exit{Reason: "system.internal.unexpected", Code: 255},
	}
	raw, err := json.Marshal(b)
	assert.NoError(t, err)
	t.Logf("the widest rollup bundle is %d bytes", len(raw))
	assert.That(t, len(raw) < 14<<10)
}
