package kafka

import (
	"context"
	"sync"

	"github.com/turbolytics/sql-flow/internal/core"
	"github.com/twmb/franz-go/pkg/kgo"
)

// PartitionEvents relays the group's assignments to whoever subscribes.
//
// Built before the client, because the rebalance callbacks are client
// options, and handed to the source afterwards, as the offset seeker is.
//
// It remembers what the group has assigned, because a subscriber can arrive
// late. The client joins its group as soon as it exists, so the first
// assignment can land before the pipeline has subscribed; a subscriber that
// missed it would read the first revocation as the loss of every partition.
// Subscribe therefore hands over the current assignment before any change.
type PartitionEvents struct {
	mu       sync.Mutex
	owned    map[string]map[int32]bool
	assigned func(map[string][]int32)
	released func(map[string][]int32)
}

func NewPartitionEvents() *PartitionEvents {
	return &PartitionEvents{owned: map[string]map[int32]bool{}}
}

// ClientOptions are the callbacks to build the client with. Revoked and lost
// both release: a lost partition is one this process no longer holds, and
// the only difference is whether it got to say goodbye.
func (e *PartitionEvents) ClientOptions() []kgo.Opt {
	return []kgo.Opt{
		kgo.OnPartitionsAssigned(e.onAssigned),
		kgo.OnPartitionsRevoked(e.onReleased),
		kgo.OnPartitionsLost(e.onReleased),
	}
}

// Subscribe hands assigned the partitions held now, then relays every change.
func (e *PartitionEvents) Subscribe(assigned, released func(map[string][]int32)) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.assigned, e.released = assigned, released
	if current := e.current(); len(current) > 0 && assigned != nil {
		assigned(current)
	}
}

func (e *PartitionEvents) onAssigned(_ context.Context, _ *kgo.Client, parts map[string][]int32) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for topic, ps := range parts {
		if e.owned[topic] == nil {
			e.owned[topic] = map[int32]bool{}
		}
		for _, p := range ps {
			e.owned[topic][p] = true
		}
	}
	if e.assigned != nil {
		e.assigned(parts)
	}
}

func (e *PartitionEvents) onReleased(_ context.Context, _ *kgo.Client, parts map[string][]int32) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for topic, ps := range parts {
		for _, p := range ps {
			delete(e.owned[topic], p)
		}
	}
	if e.released != nil {
		e.released(parts)
	}
}

func (e *PartitionEvents) current() map[string][]int32 {
	out := map[string][]int32{}
	for topic, ps := range e.owned {
		for p := range ps {
			out[topic] = append(out[topic], p)
		}
	}
	return out
}

var _ core.PartitionOwner = (*Source)(nil)
