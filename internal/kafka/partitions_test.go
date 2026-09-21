package kafka

import (
	"context"
	"fmt"
	"os/exec"
	"sort"
	"testing"
	"time"

	"github.com/turbolytics/sql-flow/internal/core"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/zeebo/assert"
)

func createTopic(t *testing.T, broker, topic string, partitions int32) {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(broker))
	assert.NoError(t, err)
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	req := kmsg.NewPtrCreateTopicsRequest()
	rt := kmsg.NewCreateTopicsRequestTopic()
	rt.Topic, rt.NumPartitions, rt.ReplicationFactor = topic, partitions, 1
	req.Topics = append(req.Topics, rt)
	req.TimeoutMillis = 20000
	resp, err := req.RequestWith(ctx, cl)
	assert.NoError(t, err)
	assert.Equal(t, int16(0), resp.Topics[0].ErrorCode)
}

// consumer is one pipeline's view: a source with the relay, and the lag
// table the pipeline would feed from it.
type consumer struct {
	src *Source
	lag *core.LagTable
}

func startConsumer(t *testing.T, broker, topic, group string) *consumer {
	t.Helper()
	events := NewPartitionEvents()
	return startConsumerWith(t, broker, topic, group, events, events.ClientOptions()...)
}

func startConsumerWith(t *testing.T, broker, topic, group string, events *PartitionEvents, opts ...kgo.Opt) *consumer {
	t.Helper()
	client := newTestClient(t, broker, topic, group, opts...)
	src, err := NewSource(client, WithPartitionEvents(events))
	assert.NoError(t, err)
	m, err := core.NewMetrics(nil)
	assert.NoError(t, err)
	c := &consumer{src: src, lag: m.Lag}
	src.OnPartitions(m.Lag.Assigned, m.Lag.Released, m.Lag.Lost)
	// Drain the stream as the consume loop would, publishing lag as it goes,
	// including for records queued before a revocation.
	go func() {
		for batch := range src.Stream() {
			for _, msg := range batch {
				c.lag.Set(msg.Topic, msg.Partition, msg.HighWatermark-msg.Offset-1)
			}
		}
	}()
	return c
}

func (c *consumer) partitions(topic string) []int32 {
	var out []int32
	for p := range c.lag.Snapshot()[topic] {
		out = append(out, p)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// A partition that moves to another instance leaves the first one's lag.
//
// Reproduced rather than reasoned: two consumers in one group, and the second
// joining takes partitions from the first. Before the relay, the first kept
// reporting every partition it had ever read, so a fleet summing lag counted
// the moved ones twice.
func TestIntegrationSourceKafka_AMovedPartitionLeavesTheFirstConsumersLag(t *testing.T) {
	coverage.Covers(t, "source.kafka")
	broker := brokerOrFail(t)
	topic := fmt.Sprintf("turbine-rebalance-%d", time.Now().UnixNano())
	createTopic(t, broker, topic, 4)

	producer, err := kgo.NewClient(kgo.SeedBrokers(broker),
		kgo.RecordPartitioner(kgo.ManualPartitioner()))
	assert.NoError(t, err)
	defer producer.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for p := int32(0); p < 4; p++ {
		for i := 0; i < 5; i++ {
			res := producer.ProduceSync(ctx, &kgo.Record{Topic: topic, Partition: p, Value: []byte(`{"i":1}`)})
			assert.NoError(t, res.FirstErr())
		}
	}

	first := startConsumer(t, broker, topic, topic)
	defer first.src.Close()
	waitUntil(t, 60*time.Second, "the first consumer reads all four partitions", func() bool {
		return len(first.partitions(topic)) == 4
	})

	second := startConsumer(t, broker, topic, topic)
	defer second.src.Close()
	// More records, so the second consumer has something to read from what
	// it was given.
	for p := int32(0); p < 4; p++ {
		res := producer.ProduceSync(ctx, &kgo.Record{Topic: topic, Partition: p, Value: []byte(`{"i":2}`)})
		assert.NoError(t, res.FirstErr())
	}

	waitUntil(t, 90*time.Second, "the partitions split between the two consumers", func() bool {
		a, b := first.partitions(topic), second.partitions(topic)
		return len(a) > 0 && len(b) > 0 && len(a)+len(b) == 4
	})

	a, b := first.partitions(topic), second.partitions(topic)
	seen := map[int32]string{}
	for _, p := range a {
		seen[p] = "first"
	}
	for _, p := range b {
		assert.Equal(t, "", seen[p]) // no partition reported by both
	}
	t.Logf("first reports %v, second reports %v", a, b)
}

func waitUntil(t *testing.T, limit time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(limit)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("timed out waiting until %s", what)
}

// Closing the source keeps the lag it last saw.
//
// Close leaves the group, and leaving revokes every partition first. Handled
// as a rebalance, that emptied the lag table before the final bundle was
// collected, so every exit reported lag 0 over 0 partitions -- a consumer
// stopped with a backlog read as one that had caught up.
func TestIntegrationSourceKafka_ClosingKeepsTheLastLag(t *testing.T) {
	coverage.Covers(t, "source.kafka")
	broker := brokerOrFail(t)
	topic := fmt.Sprintf("turbine-close-%d", time.Now().UnixNano())
	createTopic(t, broker, topic, 2)

	producer, err := kgo.NewClient(kgo.SeedBrokers(broker),
		kgo.RecordPartitioner(kgo.ManualPartitioner()))
	assert.NoError(t, err)
	defer producer.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for p := int32(0); p < 2; p++ {
		for i := 0; i < 5; i++ {
			res := producer.ProduceSync(ctx, &kgo.Record{Topic: topic, Partition: p, Value: []byte(`{"i":1}`)})
			assert.NoError(t, res.FirstErr())
		}
	}

	c := startConsumer(t, broker, topic, topic)
	waitUntil(t, 60*time.Second, "the consumer reads both partitions", func() bool {
		return len(c.partitions(topic)) == 2
	})

	assert.NoError(t, c.src.Close())
	assert.Equal(t, 2, len(c.partitions(topic)))
}

// A broker outage keeps the lag it last saw, dated.
//
// When heartbeats fail the client reports every partition lost. Handled as a
// rebalance, that emptied the lag table, so an instance cut off from its
// broker reported lag 0 over 0 partitions -- the same document as an idle
// standby, at exactly the moment its backlog was growing unseen.
//
// The broker is paused, not stopped, so its address survives, and the test
// needs to own it: an external broker named by SQLFLOW_KAFKA_BROKERS cannot
// be paused from here.
func TestIntegrationSourceKafka_ABrokerOutageKeepsTheLastLag(t *testing.T) {
	coverage.Covers(t, "source.kafka")
	broker := brokerOrFail(t)
	if brokerCtr == nil {
		t.Skip("needs the package's own broker container to pause; SQLFLOW_KAFKA_BROKERS names an external one")
	}
	topic := fmt.Sprintf("turbine-outage-%d", time.Now().UnixNano())
	createTopic(t, broker, topic, 2)

	producer, err := kgo.NewClient(kgo.SeedBrokers(broker),
		kgo.RecordPartitioner(kgo.ManualPartitioner()))
	assert.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	for p := int32(0); p < 2; p++ {
		res := producer.ProduceSync(ctx, &kgo.Record{Topic: topic, Partition: p, Value: []byte(`{"i":1}`)})
		assert.NoError(t, res.FirstErr())
	}
	cancel()
	producer.Close()

	events := NewPartitionEvents()
	opts := append(events.ClientOptions(),
		kgo.SessionTimeout(6*time.Second), kgo.HeartbeatInterval(time.Second))
	c := startConsumerWith(t, broker, topic, topic, events, opts...)
	defer c.src.Close()
	waitUntil(t, 60*time.Second, "the consumer reads both partitions", func() bool {
		return len(c.partitions(topic)) == 2
	})

	id := brokerCtr.GetContainerID()
	assert.NoError(t, exec.Command("docker", "pause", id).Run())
	unpaused := false
	unpause := func() {
		if !unpaused {
			_ = exec.Command("docker", "unpause", id).Run()
			unpaused = true
		}
	}
	defer unpause()

	// Past the session timeout, so the client has given the partitions up.
	time.Sleep(20 * time.Second)
	held := len(c.partitions(topic))
	unpause()
	assert.Equal(t, 2, held)
}
