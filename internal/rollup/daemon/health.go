// Package daemon is `sqlflow rollup run`: the process that installs what a
// rollups file declares, leads the rollups it names, fills their pending
// tables, and reports its health.
package daemon

import (
	"fmt"
	"net/http"
	"strings"
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
	// PendingRead is false from the moment this process takes the leader
	// lock until it first reads the tables to fill. Until then an empty
	// PendingTables means unknown, not none.
	PendingRead bool
	// Filling is the table the last chunk filled, and Remaining the source
	// history left in it.
	Filling   string
	Remaining time.Duration
	// DriftTables are the tables the last verify pass found drifted, and
	// DriftBuckets how many of their buckets differ. A standby verifies
	// nothing, so it holds none.
	DriftTables  []string
	DriftBuckets int64
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
	if role == roleLeader && h.s.Role != roleLeader {
		h.s.PendingRead = false
	}
	h.s.Role = role
}

func (h *healthState) setPending(n int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.s.PendingTables = n
	h.s.PendingRead = true
	if n == 0 {
		h.s.Filling, h.s.Remaining = "", 0
	}
}

func (h *healthState) setFilling(table string, remaining time.Duration) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.s.Filling, h.s.Remaining = table, remaining
}

func (h *healthState) setDrift(tables []string, buckets int64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.s.DriftTables, h.s.DriftBuckets = tables, buckets
}

func (h *healthState) get() snapshot {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.s
}

// healthStatus is the table behind /healthz, the rules `sqlflow run` uses:
// 200 means do not restart, and 503 means restart. The first matching rule
// wins. degraded is 200: a restart cannot fix drift.
func healthStatus(s snapshot, now time.Time, interval time.Duration) (status, reason string, code int) {
	if age := now.Sub(s.LastContact); age > unreachableIntervals*interval {
		return "failed", fmt.Sprintf("no database round trip for %.0fs", age.Seconds()), http.StatusServiceUnavailable
	}
	if len(s.DriftTables) > 0 {
		return "degraded", fmt.Sprintf("%d buckets differ from the tables they are built from, in %s",
			s.DriftBuckets, strings.Join(s.DriftTables, ", ")), http.StatusOK
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
	if !s.PendingRead {
		return "starting", "reading the tables to fill", http.StatusOK
	}
	return "healthy", "", http.StatusOK
}
