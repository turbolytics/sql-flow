package mqtt

import (
	"errors"
	"sync"

	"github.com/eclipse/paho.golang/paho"
)

// acker is the part of *paho.Client the ledger uses. An interface so the
// ledger's tests need no broker.
type acker interface {
	Ack(*paho.Publish) error
}

type held struct {
	seq int64
	pub *paho.Publish
	via acker
}

// maxRetired is the number of retired clients the ledger remembers. A late
// publish can only come from a router that was just retired, so only recent
// clients matter.
const maxRetired = 8

// ledger holds every QoS 1 publish the pipeline has not committed, and
// acknowledges them only once it has.
//
// Acknowledging on receipt is at-most-once: the source reads ahead of the
// pipeline, and a crash loses whatever it had read. Holding the PUBACK until
// the sink has flushed means the broker still has those publishes and
// redelivers them to the resumed session.
type ledger struct {
	mu   sync.Mutex
	next int64
	// acked is the highest sequence acknowledged. It only moves forward.
	acked int64
	// current is the client of the live connection, set by its first
	// publish. retired holds the most recent clients whose connections went down.
	current acker
	retired []acker
	held    []held
}

func newLedger() *ledger {
	return &ledger{acked: -1, retired: []acker{}}
}

// isRetired reports whether via is in the retired clients list.
func (l *ledger) isRetired(via acker) bool {
	for _, r := range l.retired {
		if r == via {
			return true
		}
	}
	return false
}

// receive records a publish and returns its sequence number. The sequence
// counts every publish in this process and never resets on reconnect, so a
// mark from before a reconnect never covers a publish after it.
func (l *ledger) receive(pub *paho.Publish, via acker) int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	seq := l.next
	l.next++
	if pub.QoS == 0 {
		return seq
	}
	if l.isRetired(via) {
		return seq
	}
	if l.current == nil {
		l.current = via
	}
	if via != l.current {
		return seq
	}
	l.held = append(l.held, held{seq: seq, pub: pub, via: via})
	return seq
}

// disconnected drops every held publish. paho's Ack through a closed client
// is undefined (paho.golang #160). The broker redelivers these publishes on
// the resumed session, and the pipeline may see them twice, which
// at-least-once allows.
func (l *ledger) disconnected() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.retire(l.current)
}

// retire drops every held publish and, if via is a live client, moves it to
// the bounded retired list. Called with mu held.
func (l *ledger) retire(via acker) {
	if via != nil {
		l.retired = append(l.retired, via)
		if len(l.retired) > maxRetired {
			l.retired = l.retired[1:]
		}
	}
	if l.current == via {
		l.current = nil
	}
	l.held = l.held[:0]
}

// ackThrough acknowledges every held publish at or below seq, in receive
// order, as MQTT 5 requires. It returns how many it acknowledged.
//
// autopaho calls OnConnectionDown, and so disconnected(), only after its
// router has drained -- which can take a while when a full stream backs up
// receive_maximum publishes behind it. A commit can land in that gap,
// addressed to a client whose connection already ended: paho reports that
// as ErrPacketNotFound, because the client reset its own ack tracker on
// shutdown (paho/client.go's shutdown). Treat it as disconnected() arriving
// late: retire the client, hold nothing for it, and let the commit succeed.
// The broker redelivers what was held, same as any ordinary disconnect.
func (l *ledger) ackThrough(seq int64) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if seq <= l.acked {
		return 0, nil
	}
	n := 0
	for len(l.held) > 0 && l.held[0].seq <= seq {
		h := l.held[0]
		if err := h.via.Ack(h.pub); err != nil {
			if errors.Is(err, paho.ErrPacketNotFound) {
				l.retire(h.via)
				return 0, nil
			}
			return n, err
		}
		l.held = l.held[1:]
		n++
	}
	l.acked = seq
	return n, nil
}
