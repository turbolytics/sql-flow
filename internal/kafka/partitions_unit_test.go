package kafka

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

type relayed struct{ assigned, released, lost []map[string][]int32 }

func (r *relayed) subscribe(e *PartitionEvents) {
	e.Subscribe(
		func(p map[string][]int32) { r.assigned = append(r.assigned, p) },
		func(p map[string][]int32) { r.released = append(r.released, p) },
		func(p map[string][]int32) { r.lost = append(r.lost, p) },
	)
}

// A revocation while closing is this consumer leaving, not another taking
// over, and is not relayed. A loss is relayed as a loss.
func TestPartitionEvents_TellsRebalanceCloseAndLossApart(t *testing.T) {
	coverage.Covers(t, "source.kafka")
	ctx := context.Background()
	parts := map[string][]int32{"events": {0, 1}}

	e := NewPartitionEvents()
	r := &relayed{}
	r.subscribe(e)
	e.onAssigned(ctx, nil, parts)
	e.onRevoked(ctx, nil, map[string][]int32{"events": {1}})
	assert.Equal(t, 1, len(r.released))

	e.onLost(ctx, nil, map[string][]int32{"events": {0}})
	assert.Equal(t, 1, len(r.lost))
	assert.Equal(t, 1, len(r.released))

	e.onAssigned(ctx, nil, parts)
	e.Closing()
	e.onRevoked(ctx, nil, parts)
	assert.Equal(t, 1, len(r.released))
}

// A subscriber that arrives after the group assigned partitions is told what
// is held now.
func TestPartitionEvents_ReplaysTheCurrentAssignmentToALateSubscriber(t *testing.T) {
	coverage.Covers(t, "source.kafka")
	e := NewPartitionEvents()
	e.onAssigned(context.Background(), nil, map[string][]int32{"events": {0, 2}})

	r := &relayed{}
	r.subscribe(e)
	assert.Equal(t, 1, len(r.assigned))
	assert.Equal(t, 2, len(r.assigned[0]["events"]))
}

// The consumer delivers while it holds a partition, from the last
// assignment. Between groups, after a loss or a revocation of everything, it
// delivers nothing, and the engine counts no quiet across that.
func TestPartitionEvents_DeliversWhileHoldingAPartition(t *testing.T) {
	coverage.Covers(t, "source.kafka")
	ctx := context.Background()
	e := NewPartitionEvents()

	_, ok := e.Delivering()
	assert.That(t, !ok)

	before := time.Now()
	e.onAssigned(ctx, nil, map[string][]int32{"events": {0, 1}})
	since, ok := e.Delivering()
	assert.That(t, ok)
	assert.That(t, !since.Before(before))
	assert.That(t, strings.Contains(since.String(), " m=")) // a monotonic reading

	e.onRevoked(ctx, nil, map[string][]int32{"events": {1}})
	_, ok = e.Delivering()
	assert.That(t, ok) // still holds 0

	e.onLost(ctx, nil, map[string][]int32{"events": {0}})
	_, ok = e.Delivering()
	assert.That(t, !ok)

	// A rejoin: delivering again, from the new assignment, not the old.
	e.onAssigned(ctx, nil, map[string][]int32{"events": {0}})
	again, ok := e.Delivering()
	assert.That(t, ok)
	assert.That(t, !again.Before(since))

	// A source with no relay cannot tell, and reports delivering.
	_, ok = (&Source{}).Delivering()
	assert.That(t, ok)
}
