package core

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

// pacedSource delivers one message every `every`, n times, then stays open
// and silent until released. It is the trickle: a stream that never fills a
// batch and never ends.
type pacedSource struct {
	ch      chan []Message
	release chan struct{}
	every   time.Duration
	n       int
}

func newPacedSource(n int, every time.Duration) *pacedSource {
	return &pacedSource{
		ch:      make(chan []Message),
		release: make(chan struct{}),
		every:   every,
		n:       n,
	}
}

func (s *pacedSource) Start() error  { return nil }
func (s *pacedSource) Close() error  { return nil }
func (s *pacedSource) Commit() error { return nil }

func (s *pacedSource) Stream() <-chan []Message {
	go func() {
		defer close(s.ch)
		for i := 0; i < s.n; i++ {
			select {
			case <-time.After(s.every):
			case <-s.release:
				return
			}
			select {
			case s.ch <- []Message{{
				Value:     []byte(`{"i":1}`),
				Topic:     "t",
				Partition: 0,
				Offset:    int64(i),
			}}:
			case <-s.release:
				return
			}
		}
		<-s.release
	}()
	return s.ch
}

// markRecorder is an offsetSaver that keeps the newest saved mark.
type markRecorder struct {
	mu    sync.Mutex
	saves int
	last  *Marks
}

func (m *markRecorder) Save(_ context.Context, marks *Marks) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.saves++
	m.last = marks
	return nil
}

func (m *markRecorder) count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.saves
}

// noopTx satisfies stateTx for a pipeline that has offsets but no real
// transaction; commitState needs both to be non-nil to save marks.
type noopTx struct{}

func (noopTx) Commit(context.Context) error   { return nil }
func (noopTx) Rollback(context.Context) error { return nil }

func waitFor(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.After(timeout)
	for !cond() {
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for %s", what)
		default:
			time.Sleep(5 * time.Millisecond)
		}
	}
}

// A trickle: one message every 40 ms, a batch size it will never reach, a
// 20 ms flush interval. Every message reaches the sink on the interval
// rather than when the batch fills, and every flush saves a mark. This is
// the shape of most streams most of the time, and nothing measured it.
func TestCoreConsumeLoop_TrickleFlushesEveryMessageOnTheInterval(t *testing.T) {
	coverage.Covers(t, "core.consume_loop")
	src := newPacedSource(10, 40*time.Millisecond)
	sink := &fakeSink{}
	marks := &markRecorder{}
	tb := NewTurbine(src, &fakeHandler{}, sink, 1000, 20*time.Millisecond,
		&sync.Mutex{}, PipelineErrorPolicies{}, WithStateStore(marks, noopTx{}))
	done := make(chan struct{})
	go func() { _, _ = tb.ConsumeLoop(context.Background(), 0); close(done) }()

	// The first message lands within its own cadence plus an interval plus
	// scheduling slack, long before a batch of 1,000 could ever fill.
	start := time.Now()
	waitFor(t, "the first row at the sink", 2*time.Second, func() bool {
		sink.mu.Lock()
		defer sink.mu.Unlock()
		return sink.rows >= 1
	})
	assert.That(t, time.Since(start) < 40*time.Millisecond+20*time.Millisecond+250*time.Millisecond)

	waitFor(t, "all ten rows", 5*time.Second, func() bool {
		sink.mu.Lock()
		defer sink.mu.Unlock()
		return sink.rows == 10
	})
	// Ten messages, at least ten flushes: no batch ever held two, because
	// the interval closes each one before the next message arrives.
	assert.That(t, marks.count() >= 10)
	close(src.release)
	<-done
}

// A surge then silence: 5,000 messages in one burst, then nothing. The final
// partial batch reaches the sink rather than waiting for a surge that never
// comes, the saved mark is the last offset, and the silence writes nothing.
func TestCoreConsumeLoop_SurgeThenSilenceFlushesTheTailOnce(t *testing.T) {
	coverage.Covers(t, "core.consume_loop")
	burst := messages(5000)
	for i := range burst {
		burst[i].Topic = "t"
		burst[i].Offset = int64(i)
	}
	src := newBlockingSource(burst)
	sink := &fakeSink{}
	marks := &markRecorder{}
	tb := NewTurbine(src, &fakeHandler{}, sink, 1000, 30*time.Millisecond,
		&sync.Mutex{}, PipelineErrorPolicies{}, WithStateStore(marks, noopTx{}))
	done := make(chan struct{})
	go func() { _, _ = tb.ConsumeLoop(context.Background(), 0); close(done) }()

	waitFor(t, "5000 rows at the sink", 5*time.Second, func() bool {
		sink.mu.Lock()
		defer sink.mu.Unlock()
		return sink.rows == 5000
	})
	marks.mu.Lock()
	last := marks.last
	marks.mu.Unlock()
	assert.That(t, last != nil)
	var newest int64 = -1
	last.Each(func(_ string, _ int32, m Mark) { newest = m.Offset })
	assert.Equal(t, int64(4999), newest)

	// Five intervals of silence: the sink sees no further flush. An idle
	// tick commits state; it must not write an empty batch downstream.
	sink.mu.Lock()
	before := sink.flushes
	sink.mu.Unlock()
	time.Sleep(150 * time.Millisecond)
	sink.mu.Lock()
	after := sink.flushes
	sink.mu.Unlock()
	assert.Equal(t, before, after)
	close(src.release)
	<-done
}
