package turbostats

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

// stop does not return while a collect is in flight.
//
// This is the property a caller needs: the reporter collects from a database
// connection the caller closes on the way out, and a bare `go r.Run(ctx)`
// could not be waited on, so the close raced a collect still reading that
// connection. That is a use-after-free inside DuckDB, which no race detector
// sees because the unsafety is on the C side. The status loop beside it
// already owned its goroutine; the reporter did not.
//
// What this pins is the contract, not either mechanism. Removing the wait in
// stop still passes, because the lock in post makes Final block on the
// collect anyway; removing the lock still passes, because the wait orders
// them. TestReporter_RunAndFinalDoNotOverlap is what discriminates, and it
// drives the type directly for that reason.
func TestStartReporter_StopWaitsForACollectInFlight(t *testing.T) {
	coverage.Covers(t, "observability.turbostats.reporter")
	srv := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }))
	defer srv.Close()

	var (
		entered    sync.Once
		collecting = make(chan struct{})
		release    = make(chan struct{})
		inCollect  atomic.Bool
	)
	r, err := NewReporter(ReporterConfig{
		ReportTo: srv.URL, Key: testKey(t), Interval: time.Hour,
		Collect: func(context.Context) (Bundle, error) {
			inCollect.Store(true)
			entered.Do(func() { close(collecting) })
			<-release
			inCollect.Store(false)
			return Bundle{V: Version}, nil
		},
	})
	assert.NoError(t, err)

	stop := StartReporter(context.Background(), r)
	<-collecting

	// Let the collect finish only once stop is already waiting, so a stop
	// that did not wait would return with inCollect still true.
	go func() {
		time.Sleep(50 * time.Millisecond)
		close(release)
	}()
	stop(context.Background(), Exit{Reason: "stopped"})

	assert.That(t, !inCollect.Load())
}

// Two posts are never in flight at once, and stop sends exactly one bundle
// however often it is called.
//
// Overlapping posts raced on the failing flag, and worse, let a periodic
// bundle land after the final one: the receiver then files a heartbeat after
// an exit and shows a stopped instance as running, which is the one thing the
// final bundle exists to prevent. A receiver that rejects an out-of-order
// bundle hides this, and not every receiver does.
func TestStartReporter_StopIsIdempotentAndSendsOneFinalBundle(t *testing.T) {
	coverage.Covers(t, "observability.turbostats.reporter")

	// Overlap is measured around collect rather than around the HTTP
	// handler. post holds its lock across collect and send, so two posts
	// overlapping means two collects overlapping, and that probe does not
	// depend on when a server happens to flush a response.
	var mu sync.Mutex
	var inCollect, maxInCollect, exits int

	srv := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, req *http.Request) {
			body, _ := io.ReadAll(req.Body)
			var b Bundle
			_ = json.Unmarshal(body, &b)
			if b.Exit != nil {
				mu.Lock()
				exits++
				mu.Unlock()
			}
			w.WriteHeader(http.StatusOK)
		}))
	defer srv.Close()

	r, err := NewReporter(ReporterConfig{
		ReportTo: srv.URL, Key: testKey(t), Interval: 5 * time.Millisecond,
		Collect: func(context.Context) (Bundle, error) {
			mu.Lock()
			inCollect++
			if inCollect > maxInCollect {
				maxInCollect = inCollect
			}
			mu.Unlock()
			// Long enough that an overlapping post would be caught.
			time.Sleep(3 * time.Millisecond)
			mu.Lock()
			inCollect--
			mu.Unlock()
			return Bundle{V: Version}, nil
		},
	})
	assert.NoError(t, err)

	stop := StartReporter(context.Background(), r)
	time.Sleep(60 * time.Millisecond)

	stop(context.Background(), Exit{Reason: "stopped"})
	stop(context.Background(), Exit{Reason: "stopped"})
	stop(context.Background(), Exit{Reason: "stopped"})

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, 1, maxInCollect)
	assert.Equal(t, 1, exits)
}

// Run and Final never overlap, even driven directly.
//
// StartReporter serializes them by construction, so this drives the type the
// way an embedder could: a loop on one goroutine and a final bundle from
// another. Without the lock inside post, two collects overlap and the race
// detector flags the failing flag they share.
//
// This is the test that discriminates. Asserting the same property through
// StartReporter cannot: the lock makes Final wait for the loop's post, so
// removing the wait changes nothing a test can see, and removing the lock is
// hidden by the wait.
func TestReporter_RunAndFinalDoNotOverlap(t *testing.T) {
	coverage.Covers(t, "observability.turbostats.reporter")

	var mu sync.Mutex
	var inCollect, maxInCollect int
	srv := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }))
	defer srv.Close()

	r, err := NewReporter(ReporterConfig{
		ReportTo: srv.URL, Key: testKey(t), Interval: time.Millisecond,
		Collect: func(context.Context) (Bundle, error) {
			mu.Lock()
			inCollect++
			if inCollect > maxInCollect {
				maxInCollect = inCollect
			}
			mu.Unlock()
			time.Sleep(2 * time.Millisecond)
			mu.Lock()
			inCollect--
			mu.Unlock()
			return Bundle{V: Version}, nil
		},
	})
	assert.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		r.Run(ctx)
	}()

	// Final from a second goroutine while the loop is posting, which is what
	// a bare `go r.Run(ctx)` plus an inline Final used to do.
	for i := 0; i < 20; i++ {
		r.Final(context.Background(), Exit{Reason: "stopped"})
	}
	cancel()
	<-done

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, 1, maxInCollect)
}
