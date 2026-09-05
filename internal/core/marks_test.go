package core

import (
	"fmt"
	"testing"

	"github.com/zeebo/assert"
)

// Advance carries the invariant that a partition only moves forward, so a
// redelivered message cannot rewind a position that has already been
// committed. The rule used to live inline in Turbine.mark; it belongs with
// the data it constrains.
func TestStateOffsets_MarksAdvanceNeverGoesBackwards(t *testing.T) {
	m := NewMarks()
	m.Advance("events", 0, Mark{Offset: 10, LeaderEpoch: 1})
	m.Advance("events", 0, Mark{Offset: 4, LeaderEpoch: 1})

	got, ok := m.Get("events", 0)
	assert.That(t, ok)
	assert.Equal(t, int64(10), got.Offset)
}

func TestStateOffsets_MarksAdvanceMovesForward(t *testing.T) {
	m := NewMarks()
	m.Advance("events", 0, Mark{Offset: 4, LeaderEpoch: 1})
	m.Advance("events", 0, Mark{Offset: 10, LeaderEpoch: 2})

	got, _ := m.Get("events", 0)
	assert.Equal(t, int64(10), got.Offset)
	assert.Equal(t, int32(2), got.LeaderEpoch)
}

func TestStateOffsets_MarksTracksPartitionsIndependently(t *testing.T) {
	m := NewMarks()
	m.Advance("events", 0, Mark{Offset: 5})
	m.Advance("events", 1, Mark{Offset: 99})
	m.Advance("other", 0, Mark{Offset: 1})

	assert.Equal(t, 3, m.Len())

	p0, _ := m.Get("events", 0)
	p1, _ := m.Get("events", 1)
	other, _ := m.Get("other", 0)
	assert.Equal(t, int64(5), p0.Offset)
	assert.Equal(t, int64(99), p1.Offset)
	assert.Equal(t, int64(1), other.Offset)
}

func TestStateOffsets_MarksEmptyAndMissing(t *testing.T) {
	m := NewMarks()
	assert.That(t, m.Empty())
	assert.Equal(t, 0, m.Len())

	_, ok := m.Get("nope", 0)
	assert.That(t, !ok)

	m.Advance("events", 0, Mark{Offset: 1})
	assert.That(t, !m.Empty())
	assert.Equal(t, 1, m.Len())
}

// Offset 0 is a real position, not "unset": a mark at 0 means the first
// message was processed, and Advance must not treat it as absent.
func TestStateOffsets_MarksZeroOffsetIsAPosition(t *testing.T) {
	m := NewMarks()
	m.Advance("events", 0, Mark{Offset: 0, LeaderEpoch: 3})

	got, ok := m.Get("events", 0)
	assert.That(t, ok)
	assert.Equal(t, int64(0), got.Offset)
	assert.Equal(t, int32(3), got.LeaderEpoch)
	assert.Equal(t, 1, m.Len())
}

// Sorted iteration keeps /stats output and CLI diffs stable between runs; map
// order would shuffle them for no reason.
func TestStateOffsets_MarksEachIteratesInSortedOrder(t *testing.T) {
	m := NewMarks()
	m.Advance("zeta", 0, Mark{Offset: 1})
	m.Advance("alpha", 2, Mark{Offset: 2})
	m.Advance("alpha", 0, Mark{Offset: 3})
	m.Advance("alpha", 10, Mark{Offset: 4})

	var order []string
	m.Each(func(topic string, partition int32, mark Mark) {
		order = append(order, fmt.Sprintf("%s/%d", topic, partition))
	})
	// Partitions sort numerically, so 10 comes after 2 rather than before it.
	assert.Equal(t, []string{"alpha/0", "alpha/2", "alpha/10", "zeta/0"}, order)
}

func TestStateOffsets_MarksEachOverEmptyIsANoop(t *testing.T) {
	m := NewMarks()
	called := 0
	m.Each(func(string, int32, Mark) { called++ })
	assert.Equal(t, 0, called)
}
