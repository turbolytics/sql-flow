package rollup

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// stateDDL records what each install applied, one row per rollup. The name
// is fixed, so every version of install and run finds it.
const stateDDL = `CREATE TABLE IF NOT EXISTS sqlflow_rollup_state (
  rollup          TEXT        PRIMARY KEY,
  -- The rollup as last applied, without its serve block. See Applied.
  declaration     JSONB       NOT NULL,
  sqlflow_version TEXT        NOT NULL,
  applied_at      TIMESTAMPTZ NOT NULL,
  -- Each table still being filled, and the start of the oldest source day
  -- filled so far: null before the first chunk.
  backfill        JSONB       NOT NULL DEFAULT '{}',
  -- Tables a later declaration removed, each with the shape that built it.
  -- Their triggers still run, and a pending backfill stays pending.
  retained        JSONB       NOT NULL DEFAULT '[]'
)`

const stateColumns = "rollup, declaration, sqlflow_version, applied_at, backfill, retained"

// State is one rollup's row of sqlflow_rollup_state.
type State struct {
	Rollup      string
	Declaration Applied
	Version     string
	AppliedAt   time.Time
	// Backfill maps each table still being filled to the start of the oldest
	// source day filled so far, and to nil before the first chunk.
	Backfill map[string]*time.Time
	Retained []RetainedTable
}

// querier is a connection or a transaction.
type querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// readState returns the rollup's row, or nil when it has none.
func readState(ctx context.Context, q querier, rollup string) (*State, error) {
	s, err := scanState(q.QueryRow(ctx, "SELECT "+stateColumns+" FROM sqlflow_rollup_state WHERE rollup = $1", rollup))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	return s, err
}

// readAllStates returns every row, in rollup order.
func readAllStates(ctx context.Context, q querier) ([]State, error) {
	rows, err := q.Query(ctx, "SELECT "+stateColumns+" FROM sqlflow_rollup_state ORDER BY rollup")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []State
	for rows.Next() {
		s, err := scanState(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *s)
	}
	return out, rows.Err()
}

func scanState(row pgx.Row) (*State, error) {
	var s State
	var decl, backfill, retained []byte
	if err := row.Scan(&s.Rollup, &decl, &s.Version, &s.AppliedAt, &backfill, &retained); err != nil {
		return nil, err
	}
	for _, f := range []struct {
		raw []byte
		to  any
	}{{decl, &s.Declaration}, {backfill, &s.Backfill}, {retained, &s.Retained}} {
		if err := json.Unmarshal(f.raw, f.to); err != nil {
			return nil, fmt.Errorf("sqlflow_rollup_state row %s: %w", s.Rollup, err)
		}
	}
	return &s, nil
}

// writeState replaces the rollup's row. applied_at is the database's clock,
// so rows written from two hosts compare.
func writeState(ctx context.Context, q querier, s State) error {
	if s.Backfill == nil {
		s.Backfill = map[string]*time.Time{}
	}
	if s.Retained == nil {
		s.Retained = []RetainedTable{}
	}
	decl, err := json.Marshal(s.Declaration)
	if err != nil {
		return err
	}
	backfill, err := json.Marshal(s.Backfill)
	if err != nil {
		return err
	}
	retained, err := json.Marshal(s.Retained)
	if err != nil {
		return err
	}
	_, err = q.Exec(ctx, "INSERT INTO sqlflow_rollup_state ("+stateColumns+`)
VALUES ($1, $2, $3, now(), $4, $5)
ON CONFLICT (rollup) DO UPDATE SET
  declaration = excluded.declaration, sqlflow_version = excluded.sqlflow_version,
  applied_at = excluded.applied_at, backfill = excluded.backfill, retained = excluded.retained`,
		s.Rollup, string(decl), s.Version, string(backfill), string(retained))
	return err
}
