package rollup

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/errs"
)

// sandboxPrefix starts the name of every schema a rollups test creates, so
// an operator can find one a kept or killed run left behind.
const sandboxPrefix = "sqlflow_test_"

// The two invariants every rollups test checks, as its failures name them.
const (
	InvariantEqualsSource = "every table equals a from-scratch GROUP BY of its source at its own width"
	InvariantOnBoundary   = "every bucket lies on its grain's UTC boundary"
)

// Sandbox is the schema a rollups test runs in: a clone of each source, and
// the rollup tables, functions and triggers install creates beside them.
type Sandbox struct {
	Schema string
}

// OpenSandbox creates a schema, clones each source of conf into it, points
// the session's search_path at it alone, and installs conf there. install
// resolves every name through search_path, so it runs unchanged.
//
// A clone keeps the source's columns, NOT NULL, defaults, identity,
// generated columns and indexes, and so the key the sink upserts on. It
// leaves out CHECK constraints, so a generated value never fails a rule the
// rollups do not depend on.
func OpenSandbox(ctx context.Context, conn *pgx.Conn, conf *config.RollupsConf, version string) (*Sandbox, error) {
	// Every source is found before search_path moves, while the team's
	// schemas still resolve it.
	type source struct{ schema, table string }
	var sources []source
	seen := map[string]bool{}
	for _, r := range conf.Rollups {
		if seen[r.Source.Table] {
			continue
		}
		seen[r.Source.Table] = true
		// The outer SELECT returns one row, NULL for a missing table, where a
		// bare lookup would return none.
		var schema *string
		if err := conn.QueryRow(ctx, "SELECT (SELECT relnamespace::regnamespace::text FROM pg_class WHERE oid = to_regclass($1))",
			quote(r.Source.Table)).Scan(&schema); err != nil {
			return nil, sandboxError(err, "find source "+r.Source.Table)
		}
		if schema == nil {
			return nil, errs.New(errs.CodeConfigRollup,
				"rollup test: source table %s does not exist on --dsn's search_path; run the team's migrations against --dsn first",
				r.Source.Table)
		}
		// regnamespace::text is already quoted where an identifier needs it.
		sources = append(sources, source{schema: *schema, table: r.Source.Table})
	}

	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return nil, sandboxError(err, "name the schema")
	}
	sb := &Sandbox{Schema: sandboxPrefix + hex.EncodeToString(b[:])}
	stmts := []string{"CREATE SCHEMA " + quote(sb.Schema)}
	for _, s := range sources {
		stmts = append(stmts, fmt.Sprintf(
			"CREATE TABLE %s.%s (LIKE %s.%s INCLUDING DEFAULTS INCLUDING IDENTITY INCLUDING GENERATED INCLUDING INDEXES)",
			quote(sb.Schema), quote(s.table), s.schema, quote(s.table)))
	}
	stmts = append(stmts, "SET search_path TO "+quote(sb.Schema))
	for _, stmt := range stmts {
		if _, err := conn.Exec(ctx, stmt); err != nil {
			_ = sb.Close(ctx, conn, false)
			return nil, sandboxError(err, stmt)
		}
	}
	if _, err := Install(ctx, conn, conf, version); err != nil {
		_ = sb.Close(ctx, conn, false)
		return nil, err
	}
	return sb, nil
}

// Close restores the session's search_path and, unless keep, drops the
// schema and everything in it.
func (s *Sandbox) Close(ctx context.Context, conn *pgx.Conn, keep bool) error {
	if _, err := conn.Exec(ctx, "SET search_path TO DEFAULT"); err != nil {
		return sandboxError(err, "restore search_path")
	}
	if keep {
		return nil
	}
	if _, err := conn.Exec(ctx, "DROP SCHEMA IF EXISTS "+quote(s.Schema)+" CASCADE"); err != nil {
		return sandboxError(err, "drop "+s.Schema)
	}
	return nil
}

// InvariantFailure is one table that breaks an invariant.
type InvariantFailure struct {
	Rollup    string
	Table     string
	Invariant string
	// Buckets is how many buckets break it, and Sample the first drifted
	// rows for the first invariant.
	Buckets int64
	Sample  []DriftRow
}

// CheckInvariants checks every declared table of conf on the session's
// search_path. The first invariant is verify's statement with the table
// compared to its source rather than to the table it is built from, so the
// comparison rules are verify's: NULL dimensions pair, and a double sum
// matches within 1e-9. The second counts buckets off the grain's boundary.
func CheckInvariants(ctx context.Context, conn *pgx.Conn, conf *config.RollupsConf) ([]InvariantFailure, error) {
	var out []InvariantFailure
	for _, r := range conf.Rollups {
		t := quote(r.Source.TimeColumn)
		for _, e := range edges(r) {
			scratch := e
			scratch.From = r.Source.Table
			scratch.Grain.From = r.Source.Grain
			v, err := VerifySince(ctx, conn, newTarget(r, scratch), nil)
			if err != nil {
				return nil, err
			}
			if v.DriftBuckets > 0 {
				out = append(out, InvariantFailure{Rollup: r.Name, Table: e.Table, Invariant: InvariantEqualsSource,
					Buckets: v.DriftBuckets, Sample: v.Sample})
			}
			var off int64
			if err := conn.QueryRow(ctx, fmt.Sprintf("SELECT count(DISTINCT %s) FROM %s WHERE %s <> %s",
				t, quote(e.Table), t, bin(e.Grain.Width, t))).Scan(&off); err != nil {
				return nil, verifyError(err, e.Table)
			}
			if off > 0 {
				out = append(out, InvariantFailure{Rollup: r.Name, Table: e.Table, Invariant: InvariantOnBoundary, Buckets: off})
			}
		}
	}
	return out, nil
}

func sandboxError(err error, step string) error {
	return errs.Wrap(errs.CodeRollupInternal, err, "rollup test: %s", step)
}
