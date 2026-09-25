package kafka

import (
	"testing"
	"time"

	"github.com/turbolytics/sql-flow/internal/core"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/turbolytics/sql-flow/internal/eventtime"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/zeebo/assert"
)

// Without an extractor a record's event time is its Kafka timestamp, as
// before. With one, it is the payload's -- and a record with nothing usable
// at the path is stamped missing rather than handed the Kafka timestamp as
// a fallback, because that is a different clock and mixing the two per
// record is the trap the block exists to close. messageFrom is reachable
// without a broker, which is why it was lifted out of the fetch loop.
func TestSourceKafka_EventTimeComesFromThePayloadWhenTold(t *testing.T) {
	coverage.Covers(t, "source.kafka")
	kafkaAt := time.Date(2026, 9, 25, 12, 0, 0, 0, time.UTC)
	payloadAt := time.Date(2026, 9, 25, 11, 30, 0, 0, time.UTC)
	rec := &kgo.Record{
		Topic: "t", Partition: 0, Offset: 7, Timestamp: kafkaAt,
		Value: []byte(`{"ts": "2026-09-25T11:30:00Z"}`),
	}

	plain := messageFrom(rec, 100, nil)
	assert.Equal(t, kafkaAt.UnixNano(), plain.EventAtNanos)

	ex, err := eventtime.New("ts", eventtime.RFC3339)
	assert.NoError(t, err)
	told := messageFrom(rec, 100, ex)
	assert.Equal(t, payloadAt.UnixNano(), told.EventAtNanos)

	// Same record, but the payload says nothing usable at the path.
	bare := &kgo.Record{Topic: "t", Timestamp: kafkaAt, Value: []byte(`{"other": 1}`)}
	assert.Equal(t, core.EventTimeMissing, messageFrom(bare, 100, ex).EventAtNanos)

	// And the basis names the path, or the Kafka clock when not told.
	s := &Source{}
	assert.Equal(t, core.EventBasisKafkaCreateTime, s.EventTimeBasis())
	s.eventTime = ex
	assert.Equal(t, "ts", s.EventTimeBasis())
}
