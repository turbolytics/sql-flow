package kafka

import (
	"context"
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	"github.com/turbolytics/sql-flow/internal/core"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/zeebo/assert"
)

// brokerOrSkip returns the dev-stack broker, skipping the test when none is
// reachable rather than failing a local `go test`. Kafka-backed tests are
// deliberately not part of CI's unit run.
func brokerOrSkip(t *testing.T) string {
	t.Helper()
	broker := os.Getenv("SQLFLOW_KAFKA_BROKERS")
	if broker == "" {
		broker = "localhost:9092"
	}
	conn, err := net.DialTimeout("tcp", broker, time.Second)
	if err != nil {
		t.Skipf("kafka unavailable at %s: %v", broker, err)
	}
	conn.Close()
	return broker
}

func newTestClient(t *testing.T, broker, topic, group string) *kgo.Client {
	t.Helper()
	// Mirrors sources/init.go: a consumer group with autocommit off, so the
	// only commits are the ones the source makes.
	client, err := kgo.NewClient(
		kgo.SeedBrokers(broker),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
		kgo.AllowAutoTopicCreation(),
	)
	assert.NoError(t, err)
	return client
}

func produce(t *testing.T, client *kgo.Client, topic string, n int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for i := 0; i < n; i++ {
		res := client.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: []byte(`{"i":1}`)})
		assert.NoError(t, res.FirstErr())
	}
}

// The source fetches ahead of whoever reads its stream. Committing must name
// the position the reader has reached, not the position the source has
// fetched to -- the latter is how 20,000 processed messages came to commit
// offset 70,086 and how a batch that never reached ClickHouse had already been
// committed.
func TestSource_CommitMarksCommitsOnlyTheProcessedPosition(t *testing.T) {
	broker := brokerOrSkip(t)
	topic := fmt.Sprintf("turbine-commit-marks-%d", time.Now().UnixNano())
	client := newTestClient(t, broker, topic, topic)
	defer client.Close()

	produce(t, client, topic, 200)

	src, err := NewSource(client)
	assert.NoError(t, err)
	defer src.Close()

	// Take 50 messages off the stream. The source will have fetched all 200
	// by now; the reader has only reached the 50th.
	stream := src.Stream()
	var got []core.Message
	for len(got) < 50 {
		select {
		case batch, ok := <-stream:
			assert.That(t, ok)
			got = append(got, batch...)
		case <-time.After(30 * time.Second):
			t.Fatalf("timed out with %d messages", len(got))
		}
	}
	last := got[49]

	assert.NoError(t, src.CommitMarks(map[string]map[int32]core.Mark{
		topic: {last.Partition: {Offset: last.Offset, LeaderEpoch: last.LeaderEpoch}},
	}))

	// Kafka commits the *next* offset to read, so processing offset 49
	// commits 50. Anything larger is a fetched-but-unprocessed message
	// committed away.
	committed := client.CommittedOffsets()[topic][last.Partition]
	assert.Equal(t, last.Offset+1, committed.Offset)
	assert.Equal(t, int64(50), committed.Offset)
	// The commit names the record's leader epoch, so the broker can detect
	// truncation, rather than the "unknown" sentinel.
	assert.Equal(t, last.LeaderEpoch, committed.Epoch)
}
