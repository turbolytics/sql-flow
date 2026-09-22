package kafka

import (
	"context"
	"sync"
	"time"

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
	mu      sync.Mutex
	owned   map[string]map[int32]bool
	closing bool
	// assignedAt is when the group last assigned anything, with its
	// monotonic reading: the earliest the consumer could have delivered from
	// what it holds now.
	assignedAt time.Time
	assigned   func(map[string][]int32)
	released   func(map[string][]int32)
	lost       func(map[string][]int32)
}

func NewPartitionEvents() *PartitionEvents {
	return &PartitionEvents{owned: map[string]map[int32]bool{}}
}

// ClientOptions are the callbacks to build the client with.
//
// Revoked and lost are different facts and are relayed differently. A
// revocation in a rebalance means another member holds the partition now.
// A loss means this member's session failed -- heartbeats stopped reaching
// the broker, or it was fenced -- and says nothing about who, if anyone,
// holds it. Treating a loss as a revocation emptied the lag table during a
// broker outage, so an instance cut off from its broker read as an idle
// standby.
func (e *PartitionEvents) ClientOptions() []kgo.Opt {
	return []kgo.Opt{
		kgo.OnPartitionsAssigned(e.onAssigned),
		kgo.OnPartitionsRevoked(e.onRevoked),
		kgo.OnPartitionsLost(e.onLost),
	}
}

// Closing marks the client as leaving its group. Leaving revokes every
// partition, and that revocation is this consumer stopping rather than
// another taking over, so it is not relayed: the lag it last saw stays for
// the final bundle. Relayed, it emptied the table first, and every exit
// reported lag 0 over 0 partitions.
func (e *PartitionEvents) Closing() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.closing = true
}

// Subscribe hands assigned the partitions held now, then relays every change.
func (e *PartitionEvents) Subscribe(assigned, released, lost func(map[string][]int32)) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.assigned, e.released, e.lost = assigned, released, lost
	if current := e.current(); len(current) > 0 && assigned != nil {
		assigned(current)
	}
}

// Delivering reports whether the consumer holds any partition, and when the
// group last assigned. It is what core.Deliverer asks a source, so the
// engine counts no quiet while the consumer is between groups: a rejoin
// after a crash waits out the session timeout holding nothing, and that
// wait is not a silent stream.
func (e *PartitionEvents) Delivering() (time.Time, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, ps := range e.owned {
		if len(ps) > 0 {
			return e.assignedAt, true
		}
	}
	return time.Time{}, false
}

func (e *PartitionEvents) onAssigned(_ context.Context, _ *kgo.Client, parts map[string][]int32) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.assignedAt = time.Now()
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

func (e *PartitionEvents) onRevoked(_ context.Context, _ *kgo.Client, parts map[string][]int32) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.forget(parts)
	if e.released != nil && !e.closing {
		e.released(parts)
	}
}

func (e *PartitionEvents) onLost(_ context.Context, _ *kgo.Client, parts map[string][]int32) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.forget(parts)
	if e.lost != nil && !e.closing {
		e.lost(parts)
	}
}

func (e *PartitionEvents) forget(parts map[string][]int32) {
	for topic, ps := range parts {
		for _, p := range ps {
			delete(e.owned[topic], p)
		}
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
var _ core.Deliverer = (*Source)(nil)
