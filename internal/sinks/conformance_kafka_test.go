package sinks

// The Kafka sink under the conformance harness, against a real broker behind
// toxiproxy.
//
// The sink had both defects the harness judges. WriteTable produced every row
// on the way in, so a flush failure could not hold anything back, and Flush
// cleared its error list after reporting, so a second flush returned nil
// having produced nothing new. The pipeline commits offsets on what Flush
// reports, and both defects put the two out of step in the direction that
// loses rows.

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/conformance"
	"github.com/turbolytics/sql-flow/internal/core"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/zeebo/assert"
)

// brokerImage matches the one internal/kafka pins, so the two integration
// passes do not pull two images.
const sinkBrokerImage = "confluentinc/confluent-local:7.5.0"

func TestIntegrationSinkKafka_Conformance(t *testing.T) {
	// Before the skip: the unit pass never reaches the harness, and a test
	// that emits nothing there reads as covering no feature at all.
	coverage.Covers(t, "sink.kafka")

	if testing.Short() {
		t.Skip("integration test: -short runs the unit pass only")
	}
	ctx := context.Background()

	nw, err := network.New(ctx)
	assert.NoError(t, err)
	t.Cleanup(func() { _ = nw.Remove(context.Background()) })

	// One partition, because Kafka orders within a partition and nothing else.
	// franz-go's default UniformBytesPartitioner switches partition every
	// 64 KiB, so on a multi-partition topic preserves_order would be
	// untestable rather than false. The sink lets the broker auto-create the
	// topic, so the partition count is the broker's default to set.
	broker, err := tckafka.Run(ctx, sinkBrokerImage,
		network.WithNetwork([]string{"kafka"}, nw),
		testcontainers.WithEnv(map[string]string{"KAFKA_NUM_PARTITIONS": "1"}),
	)
	assert.NoError(t, err)
	t.Cleanup(func() { _ = broker.Terminate(context.Background()) })

	proxy := conformance.NewProxy(t, nw, "kafka:9093")

	topic := fmt.Sprintf("conformance-%d", time.Now().UnixNano())

	// Every connection the sink makes goes to the proxy, whatever address the
	// broker advertises. Without this the sink reconnects around the fault on
	// the first metadata response and Break does nothing.
	viaProxy := kgo.Dialer(func(ctx context.Context, _, _ string) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "tcp", proxy.Addr)
	})

	// The reader talks to the broker directly, so a broken destination is
	// never mistaken for an empty one.
	direct, err := broker.Brokers(ctx)
	assert.NoError(t, err)

	conformance.Sinks(t, conformance.SinkSubject{
		Integration: "sink.kafka",

		New: func(t *testing.T) core.Sink {
			s, err := NewKafkaSink(config.KafkaSink{
				Brokers: []string{proxy.Addr},
				Topic:   topic,
			}, viaProxy)
			assert.NoError(t, err)
			t.Cleanup(func() { s.Close() })
			return s
		},

		Break: proxy.Break,
		Heal:  proxy.Heal,

		ReadBack: func(t *testing.T) []conformance.Row {
			return consumeIDs(t, direct, topic)
		},

		Table: func(t *testing.T, id int64) arrow.Table { return oneRowTable(t, id) },

		// consumeIDs reads the topic from the earliest offset, and the topic
		// has one partition, so the read-back is offset order -- which is the
		// order the broker acknowledged the records in.
		OrderedReadBack: true,
	})
}

// consumeIDs reads the topic from the beginning and decodes the id of every
// message. A fresh client per call keeps the read independent of anything the
// sink's client is doing.
func consumeIDs(t *testing.T, brokers []string, topic string) []conformance.Row {
	t.Helper()

	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	assert.NoError(t, err)
	defer client.Close()

	// Short: the topic is either empty or holds what the sink just wrote, and
	// waiting longer cannot change which.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var out []conformance.Row
	for {
		fetches := client.PollFetches(ctx)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			return out
		}
		empty := true
		fetches.EachRecord(func(rec *kgo.Record) {
			empty = false
			var row map[string]any
			if err := json.Unmarshal(rec.Value, &row); err != nil {
				t.Fatalf("kafka sink wrote something that is not JSON: %v", err)
			}
			out = append(out, conformance.Row{"id": int64(row["id"].(float64))})
		})
		if empty {
			return out
		}
	}
}
