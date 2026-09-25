// Package daemon is `sqlflow rollup run`: the process that installs what a
// rollups file declares, leads the rollups it names, fills their pending
// tables, and reports its health.
package daemon

import (
	"fmt"
	"net/http"
	"sync"
	"time"
)

// What this process is doing about the leader lock.
const (
	roleStarting = "starting"
	roleLeader   = "leader"
	roleStandby  = "standby"
)

// unreachableIntervals is how many intervals without a database round trip
// make the daemon failed. Three, so one slow query cannot flap it.
const unreachableIntervals = 3

// snapshot is one read of the daemon's health.
type snapshot struct {
	Role string
	// LastContact is the last successful database round trip, and the
	// process's start before the first.
	LastContact time.Time
	// PendingTables is how many declared tables still need filling, as the
	// leader last read them.
	PendingTables int
	// Filling is the table the last chunk filled, and Remaining the source
	// history left in it.
	Filling   string
	Remaining time.Duration
}

// healthState is what the loop writes and /healthz reads.
type healthState struct {
	mu sync.Mutex
	s  snapshot
}

func newHealthState(start time.Time) *healthState {
	return &healthState{s: snapshot{Role: roleStarting, LastContact: start}}
}

func (h *healthState) touch(now time.Time) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.s.LastContact = now
}

func (h *healthState) setRole(role string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.s.Role = role
}

func (h *healthState) setPending(n int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.s.PendingTables = n
	if n == 0 {
		h.s.Filling, h.s.Remaining = "", 0
	}
}

func (h *healthState) setFilling(table string, remaining time.Duration) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.s.Filling, h.s.Remaining = table, remaining
}

func (h *healthState) get() snapshot {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.s
}

// healthStatus is the table behind /healthz, the rules `sqlflow run` uses:
// 200 means do not restart, and 503 means restart. The first matching rule
// wins. degraded arrives with verify, in Plan 2b.
func healthStatus(s snapshot, now time.Time, interval time.Duration) (status, reason string, code int) {
	if age := now.Sub(s.LastContact); age > unreachableIntervals*interval {
		return "failed", fmt.Sprintf("no database round trip for %.0fs", age.Seconds()), http.StatusServiceUnavailable
	}
	if s.Role == roleStandby {
		return "standby", "another instance holds the leader lock", http.StatusOK
	}
	if s.PendingTables > 0 {
		reason := fmt.Sprintf("%d tables to fill", s.PendingTables)
		if s.Filling != "" {
			reason += fmt.Sprintf("; %s has %.1f days of history left", s.Filling, s.Remaining.Hours()/24)
		}
		return "backfilling", reason, http.StatusOK
	}
	if s.Role == roleStarting {
		return "starting", "installing and taking the leader lock", http.StatusOK
	}
	return "healthy", "", http.StatusOK
}
