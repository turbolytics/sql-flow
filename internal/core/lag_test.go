package core

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// Lag carries the newest offset a fetch delivered, not the first.
//
// The table is written once per partition per fetch rather than per message:
// mark only notes the position, and recordLag publishes it after the fetch.
// That is what took the per-message cost of lag off the 10M benchmark, where
// v1.1.0 ran at 1.16M msg/s and main at 0.95M once lag was recorded per
// message on a real provider.
func TestObservabilityMetrics_ConsumerLagIsTheNewestOffsetPerPartition(t *testing.T) {
	coverage.Covers(t, "observability.metrics")
	m, err := NewMetrics(nil)
	assert.NoError(t, err)

	// One fetch: a thousand messages from partition 0 ending at offset 999
	// under a watermark of 1200, then ten from partition 1 that reach the
	// watermark exactly.
	fetch := kafkaMessages("events", 0, 0, 1000)
	for i := range fetch {
		fetch[i].HighWatermark = 1200
	}
	tail := kafkaMessages("events", 1, 500, 10)
	for i := range tail {
		tail[i].HighWatermark = 510
	}
	fetch = append(fetch, tail...)

	src := &fakeSource{batches: [][]Message{fetch}}
	tb := NewTurbine(src, &fakeHandler{}, &fakeSink{}, 100, time.Second,
		&sync.Mutex{}, PipelineErrorPolicies{}, WithMetrics(m))
	_, err = tb.ConsumeLoop(context.Background(), 0)
	assert.NoError(t, err)

	got := m.Lag.Snapshot()["events"]
	assert.Equal(t, 2, len(got))
	// 1200 - 999 - 1: the last processed offset, not the first.
	assert.Equal(t, int64(200), got[0])
	// 510 - 509 - 1: caught up.
	assert.Equal(t, int64(0), got[1])
}

// Publishing lag dates it.
//
// A lag reading is only as current as the last message that produced it, so
// the bundle carries when it was taken. Without the date a consumer cut off
// from its brokers reports its last lag, usually zero, as if it were now.
func TestObservabilityMetrics_PublishingLagRecordsWhenItWasObserved(t *testing.T) {
	coverage.Covers(t, "observability.metrics")
	reader := sdkmetric.NewManualReader()
	m, err := NewMetrics(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))
	assert.NoError(t, err)

	fetch := kafkaMessages("events", 0, 0, 10)
	for i := range fetch {
		fetch[i].HighWatermark = 10
	}
	before := time.Now().Unix()
	src := &fakeSource{batches: [][]Message{fetch}}
	tb := NewTurbine(src, &fakeHandler{}, &fakeSink{}, 100, time.Second,
		&sync.Mutex{}, PipelineErrorPolicies{}, WithMetrics(m))
	_, err = tb.ConsumeLoop(context.Background(), 0)
	assert.NoError(t, err)

	var rm metricdata.ResourceMetrics
	assert.NoError(t, reader.Collect(context.Background(), &rm))
	var observed int64
	for _, sm := range rm.ScopeMetrics {
		for _, metric := range sm.Metrics {
			if metric.Name == "consumer_lag_observed_timestamp" {
				observed = metric.Data.(metricdata.Gauge[int64]).DataPoints[0].Value
			}
		}
	}
	assert.That(t, observed >= before)
}

func lagPoints(t *testing.T, reader *sdkmetric.ManualReader) map[int64]int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	assert.NoError(t, reader.Collect(context.Background(), &rm))
	out := map[int64]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, metric := range sm.Metrics {
			if metric.Name != "consumer_lag" {
				continue
			}
			for _, dp := range metric.Data.(metricdata.Gauge[int64]).DataPoints {
				p, _ := dp.Attributes.Value("partition")
				out[p.AsInt64()] = dp.Value
			}
		}
	}
	return out
}

// A partition revoked in a rebalance leaves the gauge.
//
// The gauge used to be synchronous, and a synchronous gauge keeps every
// attribute set it has recorded. A partition that moved to another instance
// kept its last lag here for as long as the process ran, and a fleet summing
// lag counted it on both instances.
func TestObservabilityMetrics_AReleasedPartitionLeavesTheGauge(t *testing.T) {
	coverage.Covers(t, "observability.metrics")
	reader := sdkmetric.NewManualReader()
	m, err := NewMetrics(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))
	assert.NoError(t, err)

	m.Lag.Assigned(map[string][]int32{"events": {0, 1, 2}})
	m.Lag.Set("events", 0, 5)
	m.Lag.Set("events", 1, 4000)
	m.Lag.Set("events", 2, 7)
	assert.Equal(t, 3, len(lagPoints(t, reader)))

	m.Lag.Released(map[string][]int32{"events": {1}})
	got := lagPoints(t, reader)
	assert.Equal(t, 2, len(got))
	_, stillThere := got[1]
	assert.That(t, !stillThere)
}

// A record fetched before a revocation does not put the partition back.
//
// The source reads ahead of the consume loop, so records from a partition can
// still be queued when the group takes it away. Processing them afterwards
// used to revive the partition's lag, now frozen, on an instance that no
// longer held it.
func TestObservabilityMetrics_AQueuedRecordDoesNotReviveAReleasedPartition(t *testing.T) {
	coverage.Covers(t, "observability.metrics")
	m, err := NewMetrics(nil)
	assert.NoError(t, err)

	m.Lag.Assigned(map[string][]int32{"events": {0, 1}})
	m.Lag.Set("events", 1, 4000)
	m.Lag.Released(map[string][]int32{"events": {1}})
	m.Lag.Set("events", 1, 3990) // the queued record, processed late

	_, revived := m.Lag.Snapshot()["events"][1]
	assert.That(t, !revived)

	// Assigned back, it reports again.
	m.Lag.Assigned(map[string][]int32{"events": {1}})
	m.Lag.Set("events", 1, 12)
	assert.Equal(t, int64(12), m.Lag.Snapshot()["events"][1])
}

// A lost partition keeps its last lag until the next assignment settles it.
//
// A loss is this process's session failing -- a broker outage, a fence -- and
// says nothing about who holds the partition. Dropping its lag then made an
// instance cut off from its broker report lag 0 over 0 partitions, the same
// document as an idle standby. Once the session recovers, the assignment it
// rejoins with is everything it holds: a lost partition in it is back, and
// one outside it went elsewhere.
func TestObservabilityMetrics_ALostPartitionKeepsItsLagUntilReassigned(t *testing.T) {
	coverage.Covers(t, "observability.metrics")
	m, err := NewMetrics(nil)
	assert.NoError(t, err)

	m.Lag.Assigned(map[string][]int32{"events": {0, 1}})
	m.Lag.Set("events", 0, 40)
	m.Lag.Set("events", 1, 7)

	m.Lag.Lost(map[string][]int32{"events": {0, 1}})
	got := m.Lag.Snapshot()["events"]
	assert.Equal(t, 2, len(got))
	assert.Equal(t, int64(40), got[0])

	// A record queued before the loss does not move a frozen reading.
	m.Lag.Set("events", 0, 39)
	assert.Equal(t, int64(40), m.Lag.Snapshot()["events"][0])

	// The session recovers holding partition 1 only; 0 went elsewhere.
	m.Lag.Assigned(map[string][]int32{"events": {1}})
	got = m.Lag.Snapshot()["events"]
	assert.Equal(t, 1, len(got))
	_, zeroStayed := got[0]
	assert.That(t, !zeroStayed)
	m.Lag.Set("events", 1, 3)
	assert.Equal(t, int64(3), m.Lag.Snapshot()["events"][1])
}

// A source that never reports ownership reports every partition it reads.
func TestObservabilityMetrics_UntrackedLagReportsEveryPartition(t *testing.T) {
	coverage.Covers(t, "observability.metrics")
	m, err := NewMetrics(nil)
	assert.NoError(t, err)
	m.Lag.Set("events", 0, 3)
	m.Lag.Set("events", 1, 4)
	assert.Equal(t, 2, len(m.Lag.Snapshot()["events"]))
}

// BenchmarkLagTableSet is what publishing one partition's lag costs. It is
// paid once per partition per fetch, never per message.
//
//	go test ./internal/core/ -bench LagTableSet -benchmem -run '^$'
func BenchmarkLagTableSet(b *testing.B) {
	m, err := NewMetrics(sdkmetric.NewMeterProvider(sdkmetric.WithReader(sdkmetric.NewManualReader())))
	if err != nil {
		b.Fatal(err)
	}
	m.Lag.Assigned(map[string][]int32{"events": {0}})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Lag.Set("events", 0, int64(i))
	}
}
