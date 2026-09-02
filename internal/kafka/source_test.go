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

	marks := core.NewMarks()
	marks.Advance(topic, last.Partition, core.Mark{Offset: last.Offset, LeaderEpoch: last.LeaderEpoch})
	assert.NoError(t, src.CommitMarks(marks))

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

// Lag is only meaningful against the broker's high watermark, which arrives
// on the fetch itself. Without it, an operator cannot tell a healthy pipeline
// from one falling behind.
func TestSource_MessagesCarryHighWatermark(t *testing.T) {
	broker := brokerOrSkip(t)
	topic := fmt.Sprintf("turbine-hwm-%d", time.Now().UnixNano())

	// A plain producer, not the group-consumer client newTestClient builds:
	// that client starts background fetching as soon as it exists, which
	// races the synchronous produce loop below and can return a fetch whose
	// high watermark predates the last record produced.
	producer, err := kgo.NewClient(kgo.SeedBrokers(broker), kgo.AllowAutoTopicCreation())
	assert.NoError(t, err)
	defer producer.Close()
	produce(t, producer, topic, 10)

	client := newTestClient(t, broker, topic, topic)
	defer client.Close()

	src, err := NewSource(client)
	assert.NoError(t, err)
	defer src.Close()

	select {
	case batch := <-src.Stream():
		assert.That(t, len(batch) >= 1)
		// Ten records were produced, so the high watermark is 10 and the
		// first record sits 9 behind it.
		assert.Equal(t, int64(10), batch[0].HighWatermark)
		assert.Equal(t, int64(0), batch[0].Offset)
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for a message")
	}
}

// A restart must resume from the offsets recorded in the state database, not
// from wherever the consumer group happens to sit. The state file is the
// source of truth; Kafka's committed offsets are advisory.
func TestSource_SeekToResumesFromStoredOffsets(t *testing.T) {
	broker := brokerOrSkip(t)
	topic := fmt.Sprintf("turbine-seek-%d", time.Now().UnixNano())

	// Produced with a plain client, before the group consumer exists: a
	// client configured with ConsumerGroup starts fetching at construction
	// and would race this loop.
	producer, err := kgo.NewClient(kgo.SeedBrokers(broker), kgo.AllowAutoTopicCreation())
	assert.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	for i := 0; i < 100; i++ {
		assert.NoError(t, producer.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: []byte(`{"i":1}`)}).FirstErr())
	}
	cancel()
	producer.Close()

	client := newTestClient(t, broker, topic, topic)
	defer client.Close()

	src, err := NewSource(client)
	assert.NoError(t, err)
	defer src.Close()

	// Resume as though offset 49 had been processed: the next record read
	// must be 50, not 0.
	resume := core.NewMarks()
	resume.Advance(topic, 0, core.Mark{Offset: 49, LeaderEpoch: 0})
	assert.NoError(t, src.SeekTo(resume))

	select {
	case batch := <-src.Stream():
		assert.That(t, len(batch) >= 1)
		assert.Equal(t, int64(50), batch[0].Offset)
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for a message")
	}
}

// No stored offsets means no seek, so auto_offset_reset still governs the
// first run against a fresh state file. Seeking to zero here would be wrong:
// "nothing recorded" and "recorded position zero" are different facts.
func TestSource_SeekToEmptyIsANoop(t *testing.T) {
	broker := brokerOrSkip(t)
	topic := fmt.Sprintf("turbine-seek-empty-%d", time.Now().UnixNano())

	producer, err := kgo.NewClient(kgo.SeedBrokers(broker), kgo.AllowAutoTopicCreation())
	assert.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	for i := 0; i < 3; i++ {
		assert.NoError(t, producer.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: []byte(`{"i":1}`)}).FirstErr())
	}
	cancel()
	producer.Close()

	client := newTestClient(t, broker, topic, topic)
	defer client.Close()

	src, err := NewSource(client)
	assert.NoError(t, err)
	defer src.Close()

	assert.NoError(t, src.SeekTo(core.NewMarks()))
	assert.NoError(t, src.SeekTo(nil))

	select {
	case batch := <-src.Stream():
		assert.Equal(t, int64(0), batch[0].Offset)
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for a message")
	}
}
