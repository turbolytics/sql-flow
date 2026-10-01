package kafka

import (
	"context"
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// CommittedMetadata relays the metadata the group last committed for each
// partition this consumer is assigned, read from the offset fetch that
// follows every assignment. Built before the client, as PartitionEvents is,
// because the callback is a client option.
//
// It remembers what it fetched, for the reason PartitionEvents remembers
// what was assigned: the client joins its group as soon as it exists, so the
// first fetch can land before the pipeline has subscribed.
//
// franz-go calls the fetch callback before it assigns the fetched partitions
// to the consumer, so a subscriber hears a partition's metadata before any
// record from that partition is polled.
type CommittedMetadata struct {
	mu   sync.Mutex
	subs []func(topic string, partition int32, metadata string)
	last map[string]map[int32]string
}

func NewCommittedMetadata() *CommittedMetadata {
	return &CommittedMetadata{last: map[string]map[int32]string{}}
}

func (c *CommittedMetadata) ClientOptions() []kgo.Opt {
	return []kgo.Opt{kgo.OnOffsetsFetched(c.onFetched)}
}

// Subscribe hands fn what has been fetched so far, then every later fetch.
func (c *CommittedMetadata) Subscribe(fn func(topic string, partition int32, metadata string)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.subs = append(c.subs, fn)
	for topic, parts := range c.last {
		for partition, meta := range parts {
			fn(topic, partition, meta)
		}
	}
}

// onFetched reads both response shapes: v8 and later nest topics under
// groups, and earlier versions list them at the top. A partition with no
// metadata, which is one nothing has committed or one another tool did,
// says nothing.
func (c *CommittedMetadata) onFetched(_ context.Context, _ *kgo.Client, resp *kmsg.OffsetFetchResponse) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	emit := func(topic string, partition int32, meta *string) {
		if meta == nil || *meta == "" {
			return
		}
		if c.last[topic] == nil {
			c.last[topic] = map[int32]string{}
		}
		c.last[topic][partition] = *meta
		for _, fn := range c.subs {
			fn(topic, partition, *meta)
		}
	}
	for _, t := range resp.Topics {
		for _, p := range t.Partitions {
			emit(t.Topic, p.Partition, p.Metadata)
		}
	}
	for _, g := range resp.Groups {
		for _, t := range g.Topics {
			for _, p := range t.Partitions {
				emit(t.Topic, p.Partition, p.Metadata)
			}
		}
	}
	return nil
}
