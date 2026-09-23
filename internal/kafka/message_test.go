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
// hour is handling records the broker stamped an hour ago.
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
	assert.Equal(t, core.EventBasisKafkaTimestamp, (&Source{}).EventTimeBasis())
}
