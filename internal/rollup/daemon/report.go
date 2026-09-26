package daemon

import (
	"sync"
	"time"

	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/errs"
	"github.com/turbolytics/sql-flow/internal/freshness"
	"github.com/turbolytics/sql-flow/internal/turbostats"
)

// report is what the daemon's passes measured and counted, for TurboStats.
// The loop writes it; the reporter's goroutine reads it.
type report struct {
	mu       sync.Mutex
	store    *freshness.Store
	observed time.Time
	tables   []turbostats.FreshTable
	byRollup map[string]*rollupTotals
	lastCode string
	lastAt   time.Time
}

// rollupTotals is one rollup's counts since the process started, and what
// its last observe pass read.
type rollupTotals struct {
	verified, drifted int64
	pending           map[string]bool
	historyLeft       map[string]time.Duration
	completeness      *turbostats.RollupCompleteness
	triggers          *turbostats.RollupTriggers
}

func newReport(conf *config.RollupsConf) *report {
	r := &report{byRollup: map[string]*rollupTotals{}}
	for _, ro := range conf.Rollups {
		r.byRollup[ro.Name] = &rollupTotals{pending: map[string]bool{}, historyLeft: map[string]time.Duration{}}
	}
	return r
}

func (r *report) setStore(s freshness.Store) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.store = &s
}

// setObserved replaces the tables an observe pass read, all at once, so a
// bundle never mixes two passes.
func (r *report) setObserved(at time.Time, tables []turbostats.FreshTable) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.observed, r.tables = at, tables
}

func (r *report) setMeasured(rollup string, c *turbostats.RollupCompleteness, tr *turbostats.RollupTriggers) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if t := r.byRollup[rollup]; t != nil {
		t.completeness, t.triggers = c, tr
	}
}

func (r *report) addVerified(rollup string, buckets, drifted int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if t := r.byRollup[rollup]; t != nil {
		t.verified += buckets
		t.drifted += drifted
	}
}

// setPending records the tables still filling, rollup by rollup, as the
// leader last read them. A table that left the list forgets its history.
func (r *report) setPending(pending map[string][]string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for name, t := range r.byRollup {
		now := map[string]bool{}
		for _, table := range pending[name] {
			now[table] = true
		}
		for table := range t.historyLeft {
			if !now[table] {
				delete(t.historyLeft, table)
			}
		}
		t.pending = now
	}
}

func (r *report) setHistoryLeft(rollup, table string, left time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if t := r.byRollup[rollup]; t != nil {
		t.historyLeft[table] = left
	}
}

// failed keeps the code and time of the last error the daemon counted. The
// message stays in the log: it can carry a DSN.
func (r *report) failed(err error, at time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lastCode, r.lastAt = string(errs.CodeOf(err)), at
}

// section is the bundle's rollup and freshness sections. A process that
// does not lead reports its role and its last error, and nothing it
// measured while it led.
func (r *report) section(role string, rollups []config.Rollup) (*turbostats.Rollup, *turbostats.Freshness) {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := &turbostats.Rollup{Role: role}
	if r.lastCode != "" {
		code, at := r.lastCode, r.lastAt.UTC()
		out.LastErrorCode, out.LastErrorAt = &code, &at
	}
	if role != roleLeader {
		return out, nil
	}
	for _, ro := range rollups {
		t := r.byRollup[ro.Name]
		e := turbostats.RollupEntry{Name: ro.Name, Strategy: "trigger",
			VerifyBucketCount: t.verified, DriftBucketCount: t.drifted,
			Completeness: t.completeness, Triggers: t.triggers}
		if len(t.pending) > 0 {
			e.Backfill = &turbostats.RollupBackfill{TablesLeft: len(t.pending)}
			var most time.Duration
			for _, left := range t.historyLeft {
				if left > most {
					most = left
				}
			}
			if len(t.historyLeft) > 0 {
				s := int64(most / time.Second)
				e.Backfill.HistoryLeftSeconds = &s
			}
		}
		out.Rollups = append(out.Rollups, e)
	}
	if r.store == nil || r.observed.IsZero() {
		return out, nil
	}
	return out, &turbostats.Freshness{StoreID: r.store.ID, StoreIDKind: r.store.Kind,
		ObservedAt: r.observed.UTC(), Tables: append([]turbostats.FreshTable(nil), r.tables...)}
}
