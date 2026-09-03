package core

import "sort"

// Marks records the last position the pipeline has finished with in each
// partition it has read: a message written to the handler, or one dropped by
// an error policy.
//
// Topic-then-partition nesting is the natural shape for this and the awkward
// one to pass around. Owning it here keeps two rules in one place rather than
// repeated at every call site: a partition only ever moves forward, and
// iteration is ordered so anything rendered from it -- the stats endpoint, the
// CLI -- is stable between runs.
type Marks struct {
	m map[string]map[int32]Mark
}

func NewMarks() *Marks {
	return &Marks{m: map[string]map[int32]Mark{}}
}

// Advance records a position, ignoring one that would move a partition
// backwards. Offsets arrive in order within a partition, so the guard only
// matters if a source redelivers -- and a redelivery must not rewind a
// position that has already been committed.
func (m *Marks) Advance(topic string, partition int32, mark Mark) {
	parts, ok := m.m[topic]
	if !ok {
		parts = map[int32]Mark{}
		m.m[topic] = parts
	}
	if cur, seen := parts[partition]; seen && cur.Offset >= mark.Offset {
		return
	}
	parts[partition] = mark
}

// Get reports the position recorded for a partition. The boolean distinguishes
// "no position yet" from a position at offset zero, which is a real place in
// the log.
func (m *Marks) Get(topic string, partition int32) (Mark, bool) {
	parts, ok := m.m[topic]
	if !ok {
		return Mark{}, false
	}
	mark, ok := parts[partition]
	return mark, ok
}

// Len is the number of partitions held, across all topics.
func (m *Marks) Len() int {
	n := 0
	for _, parts := range m.m {
		n += len(parts)
	}
	return n
}

func (m *Marks) Empty() bool { return m.Len() == 0 }

// Each calls fn for every position, ordered by topic then partition. The order
// is deliberate: map iteration is random, and callers render these into JSON
// and log lines where a shuffling order makes diffs unreadable.
func (m *Marks) Each(fn func(topic string, partition int32, mark Mark)) {
	topics := make([]string, 0, len(m.m))
	for topic := range m.m {
		topics = append(topics, topic)
	}
	sort.Strings(topics)

	for _, topic := range topics {
		parts := m.m[topic]
		partitions := make([]int32, 0, len(parts))
		for p := range parts {
			partitions = append(partitions, p)
		}
		sort.Slice(partitions, func(i, j int) bool { return partitions[i] < partitions[j] })

		for _, p := range partitions {
			fn(topic, p, parts[p])
		}
	}
}
