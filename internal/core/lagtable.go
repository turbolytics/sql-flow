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
// Both functions receive topic to partitions. assigned is called at once with
// whatever the source already holds, because a group can assign partitions
// before anything has subscribed, and a missed first assignment would make
// every later revocation read as the loss of everything.
type PartitionOwner interface {
	OnPartitions(assigned, released func(map[string][]int32))
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
func (l *LagTable) Assigned(parts map[string][]int32) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.tracked = true
	for topic, ps := range parts {
		for _, p := range ps {
			l.owned[lagKey{topic: topic, partition: p}] = true
		}
	}
}

// Released drops partitions this process no longer holds, and their lag.
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
