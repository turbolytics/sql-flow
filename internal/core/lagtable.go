package core

import (
	"context"
	"sync"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// PartitionOwner is implemented by a source whose partitions can move to
// another instance. It reports each change so the lag of a partition this
// process no longer holds stops being reported as if it did.
//
// Each function receives topic to partitions:
//
//   - assigned: this process holds these now. Called at once with whatever
//     the source already holds, because a group can assign partitions before
//     anything subscribes, and a missed first assignment would make every
//     later change read as the loss of everything.
//   - released: another member holds these now, after a rebalance.
//   - lost: this process's session failed, and who holds these is unknown.
type PartitionOwner interface {
	OnPartitions(assigned, released, lost func(map[string][]int32))
}

// LagTable is the current consumer lag of each partition, observed by the
// consumer_lag gauge whenever metrics are collected.
//
// It replaces a synchronous gauge, which keeps the last value of every
// attribute set it has ever recorded and has no way to forget one. A
// partition moved to another instance in a rebalance therefore kept its last
// lag in this instance's bundle for as long as the process ran, and a fleet
// summing lag counted it twice -- once on the instance that holds it and once
// on the instance that used to. A slow consumer is a common reason for a
// rebalance, so the value left behind was often a backlog.
//
// Ownership is tracked once a source reports it. Until then every partition
// set is reported, which is all a source with no groups can mean.
type LagTable struct {
	mu      sync.Mutex
	lag     map[lagKey]lagEntry
	tracked bool
	owned   map[lagKey]bool
}

type lagEntry struct {
	value int64
	attrs metric.ObserveOption
	// lost is set when the session holding the partition failed. The entry
	// keeps its last value, which lag_observed_at dates, until the next
	// assignment says whether the partition came back.
	lost bool
}

func newLagTable() *LagTable {
	return &LagTable{lag: map[lagKey]lagEntry{}, owned: map[lagKey]bool{}}
}

// Set records one partition's lag. It is ignored for a partition the source
// has released: records fetched before a revocation are still queued in the
// read-ahead buffer, and processing them after the release would put the
// partition back.
func (l *LagTable) Set(topic string, partition int32, lag int64) {
	key := lagKey{topic: topic, partition: partition}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.tracked && !l.owned[key] {
		return
	}
	entry, ok := l.lag[key]
	if !ok {
		// Built once per partition, not per record: WithAttributes allocates.
		entry.attrs = metric.WithAttributes(
			attribute.String("topic", topic), attribute.Int("partition", int(partition)))
	}
	entry.value = lag
	l.lag[key] = entry
}

// Assigned notes partitions this process now holds.
//
// It also settles every partition a failed session left behind. After a loss
// this process holds nothing, so the next assignment is everything it holds:
// a lost partition in it is back, and one outside it went to another member
// and leaves the table.
func (l *LagTable) Assigned(parts map[string][]int32) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.tracked = true
	for topic, ps := range parts {
		for _, p := range ps {
			key := lagKey{topic: topic, partition: p}
			l.owned[key] = true
			if entry, ok := l.lag[key]; ok && entry.lost {
				entry.lost = false
				l.lag[key] = entry
			}
		}
	}
	for key, entry := range l.lag {
		if entry.lost {
			delete(l.lag, key)
		}
	}
}

// Lost notes partitions whose session failed. Their lag stays, frozen at the
// last reading, because nothing else holds them yet as far as this process
// knows. Removed instead, an instance cut off from its broker reported lag 0
// over 0 partitions -- the same document as an idle standby. The reading is
// only as current as lag_observed_at, and a receiver reads the two together.
func (l *LagTable) Lost(parts map[string][]int32) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.tracked = true
	for topic, ps := range parts {
		for _, p := range ps {
			key := lagKey{topic: topic, partition: p}
			delete(l.owned, key)
			if entry, ok := l.lag[key]; ok {
				entry.lost = true
				l.lag[key] = entry
			}
		}
	}
}

// Released drops partitions another member holds now, and their lag.
func (l *LagTable) Released(parts map[string][]int32) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.tracked = true
	for topic, ps := range parts {
		for _, p := range ps {
			key := lagKey{topic: topic, partition: p}
			delete(l.owned, key)
			delete(l.lag, key)
		}
	}
}

// Snapshot returns the lag of every partition currently reported, by
// partition. For tests and debugging; the gauge reads the table directly.
func (l *LagTable) Snapshot() map[string]map[int32]int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := map[string]map[int32]int64{}
	for key, entry := range l.lag {
		if out[key.topic] == nil {
			out[key.topic] = map[int32]int64{}
		}
		out[key.topic][key.partition] = entry.value
	}
	return out
}

func (l *LagTable) observe(_ context.Context, o metric.Int64Observer) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, entry := range l.lag {
		o.Observe(entry.value, entry.attrs)
	}
	return nil
}
