package mqtt

import (
	"errors"
	"testing"

	"github.com/eclipse/paho.golang/paho"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

const integration = "source.mqtt"

// fakeAcker records the packet IDs acknowledged through it, in order.
type fakeAcker struct {
	acked []uint16
	fail  error
}

func (f *fakeAcker) Ack(p *paho.Publish) error {
	if f.fail != nil {
		return f.fail
	}
	f.acked = append(f.acked, p.PacketID)
	return nil
}

func pub(id uint16) *paho.Publish { return &paho.Publish{PacketID: id, QoS: 1} }

// The pipeline has processed through sequence 2. Publishes 3 and 4 were
// received but not processed, and acknowledging them would let a crash
// lose them.
func TestSourceMqtt_AcksOnlyThroughTheCommittedSequence(t *testing.T) {
	coverage.Covers(t, "source.mqtt")
	coverage.Invariant(t, "source.commit.only_processed", integration)
	l := newLedger()
	c := &fakeAcker{}
	for id := uint16(10); id < 15; id++ {
		l.receive(pub(id), c)
	}
	n, err := l.ackThrough(2)
	assert.NoError(t, err)
	assert.Equal(t, 3, n)
	assert.DeepEqual(t, []uint16{10, 11, 12}, c.acked)
}

func TestSourceMqtt_CommittedPositionNeverRegresses(t *testing.T) {
	coverage.Covers(t, "source.mqtt")
	coverage.Invariant(t, "source.marks.never_regress", integration)
	l := newLedger()
	c := &fakeAcker{}
	for id := uint16(1); id <= 6; id++ {
		l.receive(pub(id), c)
	}
	_, err := l.ackThrough(3)
	assert.NoError(t, err)
	n, err := l.ackThrough(1)
	assert.NoError(t, err)
	assert.Equal(t, 0, n)
	_, err = l.ackThrough(5)
	assert.NoError(t, err)
	assert.DeepEqual(t, []uint16{1, 2, 3, 4, 5, 6}, c.acked)
}

// paho's Ack through a client whose connection closed is undefined
// (paho.golang #160). The broker redelivers those publishes on the resumed
// session, so the source drops them rather than acknowledging them.
func TestSourceMqtt_NeverAcksThroughAnEarlierConnection(t *testing.T) {
	coverage.Covers(t, "source.mqtt")
	l := newLedger()
	old, cur := &fakeAcker{}, &fakeAcker{}
	l.receive(pub(1), old)
	l.receive(pub(2), old)
	l.disconnected()
	l.receive(pub(1), cur) // the broker's redelivery, sequence 2
	n, err := l.ackThrough(2)
	assert.NoError(t, err)
	assert.Equal(t, 1, n)
	assert.Equal(t, 0, len(old.acked))
	assert.DeepEqual(t, []uint16{1}, cur.acked)
}

// A publish the old client's router had buffered can arrive after the
// connection went down. It must not become the current connection.
func TestSourceMqtt_IgnoresALatePublishFromARetiredConnection(t *testing.T) {
	coverage.Covers(t, "source.mqtt")
	l := newLedger()
	old, cur := &fakeAcker{}, &fakeAcker{}
	l.receive(pub(1), old)
	l.disconnected()
	l.receive(pub(2), old)
	l.receive(pub(3), cur)
	_, err := l.ackThrough(10)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(old.acked))
	assert.DeepEqual(t, []uint16{3}, cur.acked)
}

func TestSourceMqtt_SequenceNeverResetsOnReconnect(t *testing.T) {
	coverage.Covers(t, "source.mqtt")
	l := newLedger()
	c := &fakeAcker{}
	assert.Equal(t, int64(0), l.receive(pub(1), c))
	l.disconnected()
	assert.Equal(t, int64(1), l.receive(pub(1), &fakeAcker{}))
}

func TestSourceMqtt_HoldsNoQoS0Publish(t *testing.T) {
	coverage.Covers(t, "source.mqtt")
	l := newLedger()
	c := &fakeAcker{}
	l.receive(&paho.Publish{QoS: 0}, c)
	n, err := l.ackThrough(0)
	assert.NoError(t, err)
	assert.Equal(t, 0, n)
}

// A failed Ack means the connection is going. The failed publish and those
// after it stay held until disconnected() drops them. The committed position
// does not advance, so a later commit retries them on a live connection.
func TestSourceMqtt_AckFailureKeepsTheRemainderHeld(t *testing.T) {
	coverage.Covers(t, "source.mqtt")
	l := newLedger()
	c := &fakeAcker{}
	l.receive(pub(1), c)
	l.receive(pub(2), c)
	c.fail = errors.New("connection closed")
	_, err := l.ackThrough(1)
	assert.Error(t, err)
	c.fail = nil
	n, err := l.ackThrough(1)
	assert.NoError(t, err)
	assert.Equal(t, 2, n)
	assert.DeepEqual(t, []uint16{1, 2}, c.acked)
}

// The retired clients list is bounded. A flaky network that reconnects many
// times would otherwise leak memory indefinitely.
func TestSourceMqtt_RetiredClientsAreBounded(t *testing.T) {
	coverage.Covers(t, "source.mqtt")
	l := newLedger()
	var ackers []*fakeAcker
	for i := 0; i < 20; i++ {
		acker := &fakeAcker{}
		ackers = append(ackers, acker)
		l.receive(pub(uint16(i)), acker)
		l.disconnected()
	}
	assert.Equal(t, maxRetired, len(l.retired))
	// The most recently retired client should be refused.
	lastClient := ackers[19]
	l.receive(pub(99), lastClient)
	n, err := l.ackThrough(1000)
	assert.NoError(t, err)
	assert.Equal(t, 0, n)
}

// autopaho calls OnConnectionDown only after its router drains, which can
// take a while when a full stream backs up receive_maximum publishes behind
// it. A commit can land in that gap, addressed to a client whose connection
// is already gone: paho reports that as ErrPacketNotFound, because the
// client reset its own ack tracker on shutdown. The commit must still
// succeed -- the broker redelivers what was held, same as any ordinary
// disconnect -- or a routine broker restart kills the pipeline.
func TestSourceMqtt_AckOnAClosedConnectionRetiresIt(t *testing.T) {
	coverage.Covers(t, "source.mqtt")
	l := newLedger()
	gone := &fakeAcker{fail: paho.ErrPacketNotFound}
	l.receive(pub(1), gone) // seq 0
	l.receive(pub(2), gone) // seq 1

	n, err := l.ackThrough(1)
	assert.NoError(t, err)
	assert.Equal(t, 0, n)

	// The retired client is dropped like any other: a later publish through
	// it is never held.
	l.receive(pub(3), gone) // seq 2, discarded: gone is retired
	n, err = l.ackThrough(2)
	assert.NoError(t, err)
	assert.Equal(t, 0, n)

	// A new client's publishes are held and acked normally.
	fresh := &fakeAcker{}
	l.receive(pub(4), fresh) // seq 3
	n, err = l.ackThrough(3)
	assert.NoError(t, err)
	assert.Equal(t, 1, n)
	assert.DeepEqual(t, []uint16{4}, fresh.acked)
}
