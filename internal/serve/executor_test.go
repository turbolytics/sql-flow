package serve

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

// fakeSession is a backend that sleeps instead of querying, so pool
// behaviour is testable without DuckDB.
type fakeSession struct {
	delay  time.Duration
	closed bool
	runs   int
}

func (f *fakeSession) run(ctx context.Context, st Statement, values map[string]any) (array.RecordReader, error) {
	f.runs++
	select {
	case <-time.After(f.delay):
		return nil, errors.New("fake session returns no rows")
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (f *fakeSession) close() error { f.closed = true; return nil }

func newFakePool(t *testing.T, n int, delay time.Duration) (*pool, []*fakeSession) {
	t.Helper()
	fakes := make([]*fakeSession, n)
	backends := make([]backendSession, n)
	for i := range fakes {
		fakes[i] = &fakeSession{delay: delay}
		backends[i] = fakes[i]
	}
	return newPool(backends, func(time.Duration) {}), fakes
}

// The whole point: N sessions run N queries in about the time of one.
func TestCliServe_PoolRunsQueriesConcurrently(t *testing.T) {
	coverage.Covers(t, "cli.serve")

	const n = 4
	const delay = 200 * time.Millisecond
	p, _ := newFakePool(t, n, delay)
	defer p.Close()

	start := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s, err := p.Acquire(context.Background())
			assert.NoError(t, err)
			defer s.Release()
			_, _ = s.Run(context.Background(), nil, nil)
		}()
	}
	wg.Wait()

	// Serial would be 4 x 200ms. Two delays of headroom for a loaded CI box.
	assert.That(t, time.Since(start) < 2*delay)
}

// One session is the old behaviour, and the control for the test above.
func TestCliServe_APoolOfOneSerializes(t *testing.T) {
	coverage.Covers(t, "cli.serve")

	const delay = 100 * time.Millisecond
	p, _ := newFakePool(t, 1, delay)
	defer p.Close()

	start := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s, err := p.Acquire(context.Background())
			assert.NoError(t, err)
			defer s.Release()
			_, _ = s.Run(context.Background(), nil, nil)
		}()
	}
	wg.Wait()

	assert.That(t, time.Since(start) >= 3*delay)
}

// A caller that gives up must free its place, or the pool shrinks under load
// until it deadlocks.
func TestCliServe_AcquireReturnsWhenTheRequestGivesUp(t *testing.T) {
	coverage.Covers(t, "cli.serve")

	p, _ := newFakePool(t, 1, 0)
	defer p.Close()

	held, err := p.Acquire(context.Background())
	assert.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err = p.Acquire(ctx)
	assert.That(t, errors.Is(err, context.DeadlineExceeded))
	assert.Equal(t, 1, p.Stats().InUse)

	held.Release()
	next, err := p.Acquire(context.Background())
	assert.NoError(t, err)
	next.Release()
}

// A session closed while a query runs on it takes the process down, and
// nothing outside the reading goroutine can stop that query, so Close waits.
func TestCliServe_CloseWaitsForBorrowedSessions(t *testing.T) {
	coverage.Covers(t, "cli.serve")

	p, fakes := newFakePool(t, 2, 0)
	s, err := p.Acquire(context.Background())
	assert.NoError(t, err)

	closed := make(chan struct{})
	go func() { p.Close(); close(closed) }()

	select {
	case <-closed:
		t.Fatal("Close returned while a session was borrowed")
	case <-time.After(100 * time.Millisecond):
	}

	s.Release()
	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not return after the session came back")
	}
	for i, f := range fakes {
		if !f.closed {
			t.Fatalf("session %d was not closed", i)
		}
	}

	_, err = p.Acquire(context.Background())
	assert.That(t, errors.Is(err, ErrClosed))
}

// A deferred Release beside an early return can run twice.
func TestCliServe_ReleaseTwiceIsSafe(t *testing.T) {
	coverage.Covers(t, "cli.serve")

	p, _ := newFakePool(t, 1, 0)
	defer p.Close()

	s, err := p.Acquire(context.Background())
	assert.NoError(t, err)
	s.Release()
	s.Release()

	assert.Equal(t, 0, p.Stats().InUse)
	next, err := p.Acquire(context.Background())
	assert.NoError(t, err)
	next.Release()
}

func TestCliServe_StatsReportSizeAndUse(t *testing.T) {
	coverage.Covers(t, "cli.serve")

	p, _ := newFakePool(t, 3, 0)
	defer p.Close()

	assert.Equal(t, Stats{Size: 3, InUse: 0}, p.Stats())
	a, _ := p.Acquire(context.Background())
	b, _ := p.Acquire(context.Background())
	assert.Equal(t, Stats{Size: 3, InUse: 2}, p.Stats())
	a.Release()
	b.Release()
	assert.Equal(t, Stats{Size: 3, InUse: 0}, p.Stats())
}
