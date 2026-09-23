package mqtt

import (
	"encoding/json"
	"net/url"
	"testing"
	"time"

	"github.com/turbolytics/sql-flow/internal/core"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/turbolytics/sql-flow/internal/errs"
	"github.com/zeebo/assert"
)

func newTestSource(t *testing.T, broker *url.URL, clientID string, filters []string, opts ...Option) *Source {
	t.Helper()
	s, err := NewSource(Config{
		Broker: broker, ClientID: clientID, Topics: filters,
		SessionExpiry: 600, ReceiveMaximum: 65535,
	}, opts...)
	assert.NoError(t, err)
	return s
}

// take reads n messages, failing after a timeout.
func take(t *testing.T, s *Source, n int) []core.Message {
	t.Helper()
	out := make([]core.Message, 0, n)
	deadline := time.After(20 * time.Second)
	for len(out) < n {
		select {
		case batch := <-s.Stream():
			out = append(out, batch...)
		case <-deadline:
			t.Fatalf("got %d of %d messages", len(out), n)
		}
	}
	return out
}

// quiet asserts nothing more arrives for d.
func quiet(t *testing.T, s *Source, d time.Duration) {
	t.Helper()
	select {
	case batch := <-s.Stream():
		t.Fatalf("unexpected message: %s", batch[0].Value)
	case <-time.After(d):
	}
}

func payloadIndex(t *testing.T, m core.Message) int {
	t.Helper()
	var v struct{ I int }
	assert.NoError(t, json.Unmarshal(m.Value, &v))
	return v.I
}

func marksThrough(m core.Message) *core.Marks {
	marks := core.NewMarks()
	marks.Advance(m.Topic, m.Partition, core.Mark{Offset: m.Offset})
	return marks
}

func TestSourceMqtt_ImplementsTheEngineInterfaces(t *testing.T) {
	coverage.Covers(t, "source.mqtt")
	var s any = &Source{}
	_, ok := s.(core.MarkCommitter)
	assert.That(t, ok)
	_, ok = s.(core.Deliverer)
	assert.That(t, ok)
	_, ok = s.(core.EventTimeSource)
	assert.That(t, ok)
}

// MQTT has no partition assignment, so there is nothing to revoke and no
// revoke hook to commit from. The engine reaches that hook through
// core.PartitionOwner.
func TestSourceMqtt_HasNoPartitionAssignmentToRevoke(t *testing.T) {
	coverage.Covers(t, "source.mqtt")
	var s any = &Source{}
	_, ok := s.(core.PartitionOwner)
	assert.That(t, !ok)
}

// With a state_path, run hands the stored offsets to SeekTo and fails a
// source without it. The offsets are an earlier process's local sequence
// numbers and mean nothing to the broker, whose session holds the position.
func TestSourceMqtt_SeekToIsANoOp(t *testing.T) {
	coverage.Covers(t, "source.mqtt")
	marks := core.NewMarks()
	marks.Advance("sensors/#", 0, core.Mark{Offset: 500})
	assert.NoError(t, (&Source{}).SeekTo(marks))
}

func TestSourceMqtt_StartReportsAnUnreachableBroker(t *testing.T) {
	coverage.Covers(t, "source.mqtt")
	u, _ := url.Parse("tcp://127.0.0.1:1")
	s := newTestSource(t, u, uniq("unreachable"), []string{"x/#"}, WithConnectTimeout(2*time.Second))
	start := time.Now()
	err := s.Start()
	assert.Error(t, err)
	assert.Equal(t, errs.CodeSourceUnreachable, errs.CodeOf(err))
	assert.That(t, time.Since(start) < 10*time.Second)
	assert.NoError(t, s.Close())
}

func TestIntegrationSourceMqtt_StreamsWithFilterAndSequence(t *testing.T) {
	coverage.Covers(t, "source.mqtt")
	broker := brokerOrFail(t)
	prefix := uniq("stream")
	s := newTestSource(t, broker, uniq("c"), []string{prefix + "/#"})
	assert.NoError(t, s.Start())
	defer s.Close()

	before := time.Now()
	publish(t, broker, prefix+"/pi-1/temperature", 0, 3, false)
	msgs := take(t, s, 3)
	for i, m := range msgs {
		assert.Equal(t, prefix+"/#", m.Topic)
		assert.Equal(t, int32(0), m.Partition)
		assert.Equal(t, int64(i), m.Offset)
		assert.That(t, m.EventAtNanos >= before.Add(-time.Second).UnixNano())
	}
	assert.Equal(t, core.EventBasisArrival, s.EventTimeBasis())
}

// The source has received all 200 publishes. The pipeline has processed 50.
// The commit acknowledges those 50 and no more, so the next process gets the
// other 150.
func TestIntegrationSourceMqtt_CommitMarksAcknowledgesOnlyTheProcessedPosition(t *testing.T) {
	coverage.Invariant(t, "source.commit.only_processed", integration)
	broker := brokerOrFail(t)
	prefix, client := uniq("only-processed"), uniq("c")

	a := newTestSource(t, broker, client, []string{prefix + "/#"})
	assert.NoError(t, a.Start())
	publish(t, broker, prefix+"/d/t", 0, 200, false)
	msgs := take(t, a, 50)
	assert.NoError(t, a.CommitMarks(marksThrough(msgs[49])))
	assert.NoError(t, a.Close())

	b := newTestSource(t, broker, client, []string{prefix + "/#"})
	assert.NoError(t, b.Start())
	defer b.Close()
	got := map[int]bool{}
	for _, m := range take(t, b, 150) {
		got[payloadIndex(t, m)] = true
	}
	for i := 50; i < 200; i++ {
		assert.That(t, got[i])
	}
	quiet(t, b, 2*time.Second)
}

func TestIntegrationSourceMqtt_ResumesFromTheCommittedPosition(t *testing.T) {
	coverage.Invariant(t, "source.resume.from_committed", integration)
	broker := brokerOrFail(t)
	prefix, client := uniq("resume"), uniq("c")

	a := newTestSource(t, broker, client, []string{prefix + "/#"})
	assert.NoError(t, a.Start())
	publish(t, broker, prefix+"/d/t", 0, 100, false)
	msgs := take(t, a, 100)
	assert.NoError(t, a.CommitMarks(marksThrough(msgs[99])))
	assert.NoError(t, a.Close())

	// Published while no SQLFlow is connected. The session queues them.
	publish(t, broker, prefix+"/d/t", 100, 120, false)

	b := newTestSource(t, broker, client, []string{prefix + "/#"})
	assert.NoError(t, b.Start())
	defer b.Close()
	resumed := take(t, b, 20)
	for i, m := range resumed {
		assert.Equal(t, 100+i, payloadIndex(t, m))
	}
	quiet(t, b, 2*time.Second)
}

func TestIntegrationSourceMqtt_IgnoresRetainedReadings(t *testing.T) {
	coverage.Covers(t, "source.mqtt")
	broker := brokerOrFail(t)
	prefix := uniq("retained")
	publish(t, broker, prefix+"/d/t", 0, 1, true)

	s := newTestSource(t, broker, uniq("c"), []string{prefix + "/#"})
	assert.NoError(t, s.Start())
	defer s.Close()
	publish(t, broker, prefix+"/d/t", 1, 2, false)
	m := take(t, s, 1)[0]
	assert.Equal(t, 1, payloadIndex(t, m))
	quiet(t, s, time.Second)
}

func TestIntegrationSourceMqtt_CloseReturnsWhileTheStreamIsFull(t *testing.T) {
	coverage.Covers(t, "source.mqtt")
	broker := brokerOrFail(t)
	prefix := uniq("full")
	s := newTestSource(t, broker, uniq("c"), []string{prefix + "/#"}, WithChannelBuffer(1))
	assert.NoError(t, s.Start())
	publish(t, broker, prefix+"/d/t", 0, 20, false)

	done := make(chan error, 1)
	go func() { done <- s.Close() }()
	select {
	case err := <-done:
		assert.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("Close blocked on a full stream")
	}
}

func TestIntegrationSourceMqtt_DeliversWhileConnected(t *testing.T) {
	coverage.Covers(t, "source.mqtt")
	broker := brokerOrFail(t)
	s := newTestSource(t, broker, uniq("c"), []string{uniq("deliver") + "/#"})
	_, ok := s.Delivering()
	assert.That(t, !ok)

	assert.NoError(t, s.Start())
	d, ok := s.Delivering()
	assert.That(t, ok)
	assert.That(t, d >= 0 && d < 5*time.Second)

	assert.NoError(t, s.Close())
	_, ok = s.Delivering()
	assert.That(t, !ok)
}
