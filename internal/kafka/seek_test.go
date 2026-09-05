package kafka

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/turbolytics/sql-flow/internal/core"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/zeebo/assert"
)

func TestStateOffsets_SeekerNoMarksLeavesTheGroupOffsetsAlone(t *testing.T) {
	s := NewOffsetSeeker()
	fetched := map[string]map[int32]kgo.Offset{
		"events": {0: kgo.NewOffset().At(60)},
	}

	got, err := s.Adjust(context.Background(), fetched)
	assert.NoError(t, err)
	assert.Equal(t, int64(60), got["events"][0].EpochOffset().Offset)
}

// A stored mark names the last offset processed, so consumption resumes at the
// next one.
func TestStateOffsets_SeekerMarkOverridesTheGroupOffset(t *testing.T) {
	s := NewOffsetSeeker()
	marks := core.NewMarks()
	marks.Advance("events", 0, core.Mark{Offset: 9, LeaderEpoch: 4})
	s.SetMarks(marks)

	got, err := s.Adjust(context.Background(), map[string]map[int32]kgo.Offset{
		"events": {0: kgo.NewOffset().At(60)},
	})
	assert.NoError(t, err)

	eo := got["events"][0].EpochOffset()
	assert.Equal(t, int64(10), eo.Offset)
	assert.Equal(t, int32(4), eo.Epoch)
}

// franz-go assigns exactly the map Adjust returns, so a partition invented
// here becomes a partition this member consumes without the group ever
// assigning it -- two consumers reading the same partition.
func TestStateOffsets_SeekerNeverAddsAnUnassignedPartition(t *testing.T) {
	s := NewOffsetSeeker()
	marks := core.NewMarks()
	marks.Advance("events", 0, core.Mark{Offset: 9})
	marks.Advance("events", 7, core.Mark{Offset: 99})  // assigned elsewhere
	marks.Advance("other", 0, core.Mark{Offset: 1234}) // topic not assigned
	s.SetMarks(marks)

	got, err := s.Adjust(context.Background(), map[string]map[int32]kgo.Offset{
		"events": {0: kgo.NewOffset().At(60)},
	})
	assert.NoError(t, err)

	assert.Equal(t, 1, len(got))
	assert.Equal(t, 1, len(got["events"]))
	assert.Equal(t, int64(10), got["events"][0].EpochOffset().Offset)
}

// A partition the pipeline holds no mark for keeps the group's own offset, so
// a pipeline that gains a partition still follows auto_offset_reset for it.
func TestStateOffsets_SeekerUnmarkedPartitionKeepsTheGroupOffset(t *testing.T) {
	s := NewOffsetSeeker()
	marks := core.NewMarks()
	marks.Advance("events", 0, core.Mark{Offset: 9})
	s.SetMarks(marks)

	got, err := s.Adjust(context.Background(), map[string]map[int32]kgo.Offset{
		"events": {0: kgo.NewOffset().At(60), 1: kgo.NewOffset().At(77)},
	})
	assert.NoError(t, err)

	assert.Equal(t, int64(10), got["events"][0].EpochOffset().Offset)
	assert.Equal(t, int64(77), got["events"][1].EpochOffset().Offset)
}

// The regression this guards. Resuming used to work by committing the stored
// positions before consumption started. That commit carries an empty member
// ID, which Kafka accepts only while the group is Empty -- and the group is
// not empty on exactly the restart durable state exists for, because a killed
// process stays a member until its session times out. It failed with
// UNKNOWN_MEMBER_ID, 3 times out of 3, and the pipeline crash-looped.
//
// SeekTo must therefore reach Kafka not at all.
func TestSourceKafka_SeekToIssuesNoCommit(t *testing.T) {
	broker := brokerOrSkip(t)
	topic := fmt.Sprintf("turbine-seek-nocommit-%d", time.Now().UnixNano())
	client := newTestClient(t, broker, topic, topic)
	defer client.Close()

	produce(t, client, topic, 20)

	seeker := NewOffsetSeeker()
	src, err := NewSource(client, WithSeeker(seeker))
	assert.NoError(t, err)

	marks := core.NewMarks()
	marks.Advance(topic, 0, core.Mark{Offset: 9})
	assert.NoError(t, src.SeekTo(marks))

	// Nothing was committed: the positions are held for the join instead.
	assert.Equal(t, 0, len(client.CommittedOffsets()))
}

// End to end: the state database disagrees with the consumer group, and the
// state database wins.
func TestSourceKafka_SeekToResumesFromDurableOffsets(t *testing.T) {
	broker := brokerOrSkip(t)
	topic := fmt.Sprintf("turbine-seek-resume-%d", time.Now().UnixNano())

	producer := newTestClient(t, broker, topic, topic+"-producer")
	produce(t, producer, topic, 100)
	producer.Close()

	// Push the group's own committed offset well past where the durable state
	// says the pipeline got to, so "resumed from the state database" is
	// distinguishable from "resumed from the group".
	first := newTestClient(t, broker, topic, topic)
	first.PollFetches(context.Background())
	first.CommitOffsetsSync(context.Background(),
		map[string]map[int32]kgo.EpochOffset{topic: {0: {Offset: 60, Epoch: -1}}}, nil)
	first.Close()

	seeker := NewOffsetSeeker()
	client, err := kgo.NewClient(
		kgo.SeedBrokers(broker),
		kgo.ConsumerGroup(topic),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
		kgo.AdjustFetchOffsetsFn(seeker.Adjust),
	)
	assert.NoError(t, err)

	src, err := NewSource(client, WithSeeker(seeker))
	assert.NoError(t, err)
	defer src.Close()

	marks := core.NewMarks()
	marks.Advance(topic, 0, core.Mark{Offset: 9, LeaderEpoch: -1})
	assert.NoError(t, src.SeekTo(marks))

	select {
	case batch, ok := <-src.Stream():
		assert.That(t, ok)
		assert.That(t, len(batch) > 0)
		// Processed through 9, so the next record read is 10 -- not the 60
		// the consumer group would have given us.
		assert.Equal(t, int64(10), batch[0].Offset)
	case <-time.After(60 * time.Second):
		t.Fatal("no records after seeking to the stored offsets")
	}
}
