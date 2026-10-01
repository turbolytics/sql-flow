package kafka

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/turbolytics/sql-flow/internal/core"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/zeebo/assert"
)

// A committed position carries the metadata, and the next consumer in the
// group is told it when assigned the partition.
func TestIntegrationSourceKafka_CommitMetadataReachesTheNextOwner(t *testing.T) {
	coverage.Covers(t, "source.kafka")
	broker := brokerOrFail(t)
	t.Parallel()
	topic := fmt.Sprintf("turbine-commit-meta-%d", time.Now().UnixNano())
	createTopic(t, broker, topic, 1)

	first := NewCommittedMetadata()
	client := newTestClient(t, broker, topic, topic, first.ClientOptions()...)
	produce(t, client, topic, 5)
	src, err := NewSource(client, WithCommittedMetadata(first))
	assert.NoError(t, err)
	stream := src.Stream()
	<-stream // the group is joined and the partition held
	marks := core.NewMarks()
	marks.Advance(topic, 0, core.Mark{Offset: 2})
	assert.NoError(t, src.CommitMarksWithMetadata(marks, "sqlflow1 {\"closed\":{}}"))
	assert.NoError(t, src.Close())

	second := NewCommittedMetadata()
	got := make(chan string, 1)
	second.Subscribe(func(tp string, p int32, meta string) {
		if tp == topic && p == 0 {
			got <- meta
		}
	})
	client2 := newTestClient(t, broker, topic, topic, second.ClientOptions()...)
	src2, err := NewSource(client2, WithCommittedMetadata(second))
	assert.NoError(t, err)
	defer src2.Close()
	src2.Stream()
	select {
	case meta := <-got:
		assert.Equal(t, "sqlflow1 {\"closed\":{}}", meta)
	case <-time.After(60 * time.Second):
		t.Fatal("the second consumer was never told the committed metadata")
	}
}

func strPtr(s string) *string { return &s }

// The client joins its group as soon as it exists, so the first offset fetch
// can land before the pipeline has subscribed. A late subscriber is told
// what was fetched, in both response shapes, and empty metadata is nothing.
func TestSourceKafka_CommittedMetadataReachesALateSubscriber(t *testing.T) {
	coverage.Covers(t, "source.kafka")
	c := NewCommittedMetadata()
	resp := kmsg.NewPtrOffsetFetchResponse()
	old := kmsg.NewOffsetFetchResponseTopic()
	old.Topic = "a"
	op := kmsg.NewOffsetFetchResponseTopicPartition()
	op.Partition, op.Metadata = 1, strPtr("top-level")
	old.Partitions = append(old.Partitions, op)
	resp.Topics = append(resp.Topics, old)
	g := kmsg.NewOffsetFetchResponseGroup()
	gt := kmsg.NewOffsetFetchResponseGroupTopic()
	gt.Topic = "b"
	gp := kmsg.NewOffsetFetchResponseGroupTopicPartition()
	gp.Partition, gp.Metadata = 2, strPtr("grouped")
	empty := kmsg.NewOffsetFetchResponseGroupTopicPartition()
	empty.Partition, empty.Metadata = 3, strPtr("")
	gt.Partitions = append(gt.Partitions, gp, empty)
	g.Topics = append(g.Topics, gt)
	resp.Groups = append(resp.Groups, g)
	assert.NoError(t, c.onFetched(context.Background(), nil, resp))

	got := map[string]string{}
	c.Subscribe(func(topic string, p int32, meta string) { got[fmt.Sprintf("%s/%d", topic, p)] = meta })
	assert.Equal(t, map[string]string{"a/1": "top-level", "b/2": "grouped"}, got)
}
