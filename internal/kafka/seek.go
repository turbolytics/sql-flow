package kafka

import (
	"context"
	"sync"

	"github.com/turbolytics/sql-flow/internal/core"
	"github.com/twmb/franz-go/pkg/kgo"
)

// OffsetSeeker carries the pipeline's durable positions into the consumer
// group's join, so a restart resumes where the state database says it stopped
// rather than where the group happens to sit.
//
// It exists because the obvious approach does not work. Committing those
// positions before consumption starts issues an OffsetCommit with an empty
// member ID, which Kafka accepts only while the group is Empty. The group is
// not empty on exactly the restart this feature exists for: a killed process
// stays a member until its session times out, so the commit fails with
// UNKNOWN_MEMBER_ID and the pipeline crash-loops until the old session
// expires. Measured 3/3 against a group with one other member.
//
// franz-go's AdjustFetchOffsetsFn runs inside the join instead, after the
// group's committed offsets are fetched and before consumption begins. It
// needs no commit and no member ID, so group membership stops mattering.
type OffsetSeeker struct {
	mu    sync.Mutex
	marks *core.Marks
}

// NewOffsetSeeker returns a seeker holding no positions, which leaves the
// group's own offsets untouched.
func NewOffsetSeeker() *OffsetSeeker {
	return &OffsetSeeker{}
}

// SetMarks records the positions to resume from. Call it before the first
// poll; the join has not happened yet at that point.
func (s *OffsetSeeker) SetMarks(marks *core.Marks) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.marks = marks
}

// Adjust replaces the group's committed offset with the durable one, for every
// partition the pipeline holds a mark for. This is where a disagreement
// between Kafka and the state database is settled, and the state database
// wins.
//
// A stored mark names the last offset processed, so consumption resumes at the
// next one -- the same +1 convention CommitMarks uses.
//
// Only partitions already present in the fetched map are touched. franz-go
// assigns exactly the map this returns (consumer_group.go:1737), so adding a
// partition here would make this member consume a partition the group never
// assigned it. A partition with no mark keeps the group's offset, so a
// pipeline that gains partitions still follows auto_offset_reset for them.
func (s *OffsetSeeker) Adjust(_ context.Context, fetched map[string]map[int32]kgo.Offset) (map[string]map[int32]kgo.Offset, error) {
	s.mu.Lock()
	marks := s.marks
	s.mu.Unlock()

	if marks == nil || marks.Empty() {
		return fetched, nil
	}

	adjusted := make(map[string]map[int32]kgo.Offset, len(fetched))
	for topic, partitions := range fetched {
		adjusted[topic] = make(map[int32]kgo.Offset, len(partitions))
		for partition, offset := range partitions {
			adjusted[topic][partition] = offset
		}
	}

	marks.Each(func(topic string, partition int32, m core.Mark) {
		partitions, ok := adjusted[topic]
		if !ok {
			return
		}
		if _, ok := partitions[partition]; !ok {
			return
		}
		partitions[partition] = kgo.NewOffset().At(m.Offset + 1).WithEpoch(m.LeaderEpoch)
	})

	return adjusted, nil
}
