package kafka

import (
	"testing"
	"time"

	"github.com/turbolytics/sql-flow/internal/core"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/zeebo/assert"
)

// The record's own timestamp is the event time: a pipeline behind by an
// hour is handling records stamped an hour ago.
func TestSourceKafka_CarriesTheRecordTimestamp(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	stamped := time.Now().Add(-90 * time.Second).Truncate(time.Millisecond)
	msg := messageFrom(&kgo.Record{
		Value: []byte(`{"a":1}`), Timestamp: stamped,
		Topic: "t", Partition: 3, Offset: 42, LeaderEpoch: 7,
	}, 100)

	assert.Equal(t, stamped.UTC(), msg.EventAt.UTC())
	// The rest of the conversion, so lifting it out of the fetch loop
	// cannot quietly drop a field the commit path needs.
	assert.Equal(t, "t", msg.Topic)
	assert.Equal(t, int32(3), msg.Partition)
	assert.Equal(t, int64(42), msg.Offset)
	assert.Equal(t, int32(7), msg.LeaderEpoch)
	assert.Equal(t, int64(100), msg.HighWatermark)
	// Kafka's default is the producer's clock, and a source that has read
	// nothing yet says so rather than claiming the broker stamped it.
	assert.Equal(t, core.EventBasisKafkaCreateTime, (&Source{}).EventTimeBasis())
}

// The basis names the clock, because a lag from a producer's clock and a lag
// from the broker's measure different things. A reader that cannot tell them
// apart cannot compare two pipelines, and the fleet's clocks are the less
// trustworthy of the two.
func TestSourceKafka_BasisNamesTheClockThatStamped(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	s := &Source{}

	s.timestampType.Store(1)
	assert.Equal(t, core.EventBasisKafkaLogAppendTime, s.EventTimeBasis())

	s.timestampType.Store(0)
	assert.Equal(t, core.EventBasisKafkaCreateTime, s.EventTimeBasis())

	// A pre-0.10.0 record carries no timestamp. No basis means the bundle
	// omits the lag, rather than reporting one nothing stamped.
	s.timestampType.Store(-1)
	assert.Equal(t, "", s.EventTimeBasis())
}
