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
