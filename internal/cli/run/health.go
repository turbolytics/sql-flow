package run

import (
	"fmt"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/turbolytics/sql-flow/internal/core"
)

// health is what the process knows about itself that the progress snapshot
// does not: a failure that has stopped the loop, and the sinks whose retry
// ladders are running.
type health struct {
	mu       sync.Mutex
	failure  error
	retrying map[string]int
}

func newHealth() *health {
	return &health{retrying: map[string]int{}}
}

// Fail records the failure that is stopping the process. The first one is
// kept: a drain that then runs out of time is a consequence, and the endpoint
// should name the cause.
func (h *health) Fail(err error) {
	if err == nil {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.failure == nil {
		h.failure = err
	}
}

// Retry records a ladder attempt in flight for one sink type. It matches
// sinks.RetryEvents.Retry.
func (h *health) Retry(sinkType string, attempt int, _ error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.retrying[sinkType] = attempt
}

// Settle clears the ladder for one sink type. It matches
// sinks.RetryEvents.Settle.
func (h *health) Settle(sinkType string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.retrying, sinkType)
}

// healthSnapshot is one read of health. The map is a copy, so the status
// function never races a ladder.
type healthSnapshot struct {
	failure  error
	retrying map[string]int
}

// Snapshot copies the current state out from under the lock.
func (h *health) Snapshot() healthSnapshot {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := healthSnapshot{
		failure:  h.failure,
		retrying: make(map[string]int, len(h.retrying)),
	}
	for sinkType, attempt := range h.retrying {
		out.retrying[sinkType] = attempt
	}
	return out
}

// healthFunc reads the process's health. A mux built without one reports on
// progress alone.
type healthFunc func() healthSnapshot

// healthStatus is the table behind /healthz. The first matching rule wins.
//
// 200 means "do not restart" and 503 means "restart, or let it exit". A
// retrying sink and a recent error both answer 200: restarting a pipeline
// whose sink is on its second attempt turns a blip into a cold start and a
// rebalance, which is the cost the retry ladder exists to avoid.
//
// commitAge is measured from when the server started until the first commit,
// so a pipeline that never commits becomes failed once the grace runs out.
func healthStatus(p core.Progress, commitAge float64, snap healthSnapshot,
	interval time.Duration, now time.Time) (status, reason string, httpCode int) {

	if snap.failure != nil {
		return "failed", snap.failure.Error(), http.StatusServiceUnavailable
	}
	if commitAge > float64(stuckIntervals)*interval.Seconds() {
		return "failed", fmt.Sprintf("no commit for %.0fs", commitAge),
			http.StatusServiceUnavailable
	}
	if len(snap.retrying) > 0 {
		// Named in type order, so the reason is stable between scrapes while
		// two ladders run.
		types := make([]string, 0, len(snap.retrying))
		for sinkType := range snap.retrying {
			types = append(types, sinkType)
		}
		sort.Strings(types)
		return "degraded", fmt.Sprintf("sink %s is retrying, attempt %d",
			types[0], snap.retrying[types[0]]), http.StatusOK
	}
	if !p.LastError.IsZero() && now.Sub(p.LastError) <= interval {
		return "degraded", fmt.Sprintf("%d errors recorded, last %.0fs ago",
			p.Errors, now.Sub(p.LastError).Seconds()), http.StatusOK
	}
	if p.LastCommit.IsZero() {
		return "starting", "no commit yet", http.StatusOK
	}
	return "healthy", "", http.StatusOK
}
