package rollup

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/errs"
)

// installLock serializes installs. Every entrypoint of a deploy can run
// install at once, and CREATE TABLE IF NOT EXISTS is not safe against itself.
const installLock = "SELECT pg_advisory_xact_lock(hashtextextended('sqlflow_rollup_install', 0))"

// installLockTimeout bounds each lock wait after the install lock. While
// install waits for a source, the pipeline's next writes queue behind it,
// so a transaction left open on the source fails the install quickly
// rather than stall the pipeline until someone ends it.
var installLockTimeout = 2 * time.Second

// InstallReport says what one install did.
type InstallReport struct {
	Rollups []RollupInstall
	// Undeclared names rollups the state table holds and the file no longer
	// declares. Their tables and triggers stay.
	Undeclared []string
}

// RollupInstall is one declared rollup's part of an install.
type RollupInstall struct {
	Name string
	// Adopted is true when the database had no state row for the rollup and
	// some of its tables existed already: a migration created them.
	Adopted bool
	// Created lists the tables this install created, in the order edges
	// walks them.
	Created []string
	Plan    Plan
}

// Install makes the database hold what conf declares: every table, function
// and trigger, and a state row per rollup that records what was applied and
// which tables await a backfill. It fills nothing; `sqlflow rollup run`
// does.
//
// It runs in one transaction on conn, because Postgres DDL is transactional:
// a violation or an error leaves the database as it was. It locks each
// source before any rollup table, the order a pipeline's write takes them
// in, and holds the lock for the DDL only, never for a backfill.
func Install(ctx context.Context, conn *pgx.Conn, conf *config.RollupsConf, version string) (*InstallReport, error) {
	if err := conf.CheckError(); err != nil {
		return nil, err
	}
	for attempt := 1; ; attempt++ {
		report, err := installOnce(ctx, conn, conf, version)
		if err == nil || attempt == installAttempts || !retryable(err) {
			return report, err
		}
		// Let the writers queued behind this attempt run before the next.
		select {
		case <-ctx.Done():
			return nil, err
		case <-time.After(time.Duration(attempt) * 100 * time.Millisecond):
		}
	}
}

// installAttempts is how many times Install tries before it gives up on a
// lock it could not get within installLockTimeout, or on a deadlock
// Postgres broke by cancelling it.
const installAttempts = 3

// retryable reports a lock wait that timed out or a deadlock. Both clear when
// the other transaction ends, so another attempt can succeed.
func retryable(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && (pgErr.Code == "55P03" || pgErr.Code == "40P01")
}

func installOnce(ctx context.Context, conn *pgx.Conn, conf *config.RollupsConf, version string) (*InstallReport, error) {
	// READ COMMITTED whatever the database's default: the install lock
	// serializes installs only because each statement after it takes a new
	// snapshot and reads the previous install's commit.
	tx, err := conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return nil, installError(err, "begin")
	}
	// A no-op once Commit has run.
	defer func() { _ = tx.Rollback(ctx) }()

	report, err := install(ctx, tx, conf, version)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, installError(err, "commit")
	}
	return report, nil
}

// planned is one rollup checked and planned, waiting for every rollup in the
// file to pass before anything is applied.
type planned struct {
	r       config.Rollup
	prev    *State
	missing map[string]bool
	plan    Plan
}

func install(ctx context.Context, tx pgx.Tx, conf *config.RollupsConf, version string) (*InstallReport, error) {
	// Unqualified CREATE TABLE lands in current_schema(), and the triggers
	// resolve the same names through the same search_path.
	var schema *string
	if err := tx.QueryRow(ctx, "SELECT current_schema()").Scan(&schema); err != nil {
		return nil, installError(err, "read current_schema()")
	}
	if schema == nil {
		return nil, errs.New(errs.CodeConfigInvalid,
			"rollup install: no schema on the connection's search_path exists, so there is nowhere to create the rollup tables")
	}
	// The lock timeout is set after the install lock, because lock_timeout
	// bounds an advisory lock's wait too, and an install should queue behind
	// another install for as long as it takes.
	lockTimeout := fmt.Sprintf("SET LOCAL lock_timeout = '%dms'", installLockTimeout.Milliseconds())
	for _, stmt := range []string{installLock, lockTimeout, stateDDL} {
		if _, err := tx.Exec(ctx, stmt); err != nil {
			return nil, installError(err, "prepare")
		}
	}

	var violations []config.Violation
	var todo []planned
	for i, r := range conf.Rollups {
		path := []string{"rollups", strconv.Itoa(i)}
		v, err := checkSource(ctx, tx, r, path)
		if err != nil {
			return nil, installError(err, "check source "+r.Source.Table)
		}
		if len(v) > 0 {
			violations = append(violations, v...)
			continue
		}

		prev, err := readState(ctx, tx, r.Name)
		if err != nil {
			return nil, installError(err, "read state")
		}
		missing := map[string]bool{}
		for _, e := range edges(r) {
			relation := quote(*schema) + "." + quote(e.Table)
			cols, err := columns(ctx, tx, relation)
			if err != nil {
				return nil, installError(err, "read "+e.Table)
			}
			if len(cols) == 0 {
				missing[e.Table] = true
				continue
			}
			v, err := compareTable(ctx, tx, r, e, relation, path)
			if err != nil {
				return nil, installError(err, "compare "+e.Table)
			}
			violations = append(violations, v...)
		}

		var prevApplied *Applied
		var retained []RetainedTable
		if prev != nil {
			prevApplied, retained = &prev.Declaration, prev.Retained
		}
		plan, v := PlanChange(r, path, prevApplied, retained, missing)
		violations = append(violations, v...)
		todo = append(todo, planned{r: r, prev: prev, missing: missing, plan: plan})
	}
	if len(violations) > 0 {
		return nil, violationError(violations)
	}

	// A writer locks the source, then its statement's triggers lock the
	// rollup tables. CREATE UNIQUE INDEX IF NOT EXISTS locks a rollup table
	// even when the index exists, so an install that reached the tables
	// first deadlocked beside a live writer, and lost. Taking every source
	// first, in name order, puts install in the writers' order.
	sources := map[string]bool{}
	for _, p := range todo {
		sources[p.r.Source.Table] = true
	}
	for _, src := range sortedKeys(sources) {
		if _, err := tx.Exec(ctx, "LOCK TABLE "+quote(src)+" IN SHARE ROW EXCLUSIVE MODE"); err != nil {
			return nil, installError(err, "lock source "+src)
		}
	}

	report := &InstallReport{}
	declared := map[string]bool{}
	for _, p := range todo {
		declared[p.r.Name] = true
		script, err := PostgresObjects(p.r)
		if err != nil {
			return nil, err
		}
		if _, err := tx.Exec(ctx, script); err != nil {
			return nil, installError(err, "apply rollup "+p.r.Name)
		}
		if err := writeState(ctx, tx, nextState(p, version)); err != nil {
			return nil, installError(err, "write state")
		}
		ri := RollupInstall{Name: p.r.Name, Plan: p.plan}
		es := edges(p.r)
		for _, e := range es {
			if p.missing[e.Table] {
				ri.Created = append(ri.Created, e.Table)
			}
		}
		ri.Adopted = p.prev == nil && len(ri.Created) < len(es)
		report.Rollups = append(report.Rollups, ri)
	}

	states, err := readAllStates(ctx, tx)
	if err != nil {
		return nil, installError(err, "read state")
	}
	for _, s := range states {
		if !declared[s.Rollup] {
			report.Undeclared = append(report.Undeclared, s.Rollup)
		}
	}
	return report, nil
}

// nextState is the row an install writes: the declaration as applied now,
// every backfill still in progress, and the plan's changes to both lists.
func nextState(p planned, version string) State {
	s := State{Rollup: p.r.Name, Declaration: AppliedFrom(p.r), Version: version, Backfill: map[string]*time.Time{}}
	if p.prev != nil {
		for t, done := range p.prev.Backfill {
			s.Backfill[t] = done
		}
		for _, t := range p.prev.Retained {
			if !slices.Contains(p.plan.Restore, t.Table) {
				s.Retained = append(s.Retained, t)
			}
		}
	}
	// A table created now holds nothing, whatever an earlier backfill did to
	// a table of that name.
	for _, t := range p.plan.Backfill {
		s.Backfill[t] = nil
	}
	// A retained table keeps a backfill still pending. `run` fills only
	// declared tables, and a table declared again must still read as
	// unfilled, not as complete.
	s.Retained = append(s.Retained, p.plan.Retain...)
	return s
}

// violationError folds every violation into one error, as CheckError does,
// with each one's code: a database check and a declaration change can both
// stop one install.
func violationError(vs []config.Violation) error {
	var b strings.Builder
	b.WriteString("rollup install changed nothing")
	for _, v := range vs {
		fmt.Fprintf(&b, "\n  %s: [%s] %s", strings.Join(v.Path, "."), v.Code, v.Message)
	}
	return errs.New(vs[0].Code, "%s", b.String())
}

func installError(err error, step string) error {
	return errs.Wrap(errs.CodeRollupInternal, err, "rollup install: %s", step)
}
