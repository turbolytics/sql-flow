package sinks

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/jackc/pgx/v5"
	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/errs"
)

// PostgresSink writes batches to a Postgres table over pgx.
//
// One transaction per batch: a COPY into a session temp table, then a
// server-side INSERT ... ON CONFLICT. The cost is the batch, whatever the
// table holds. It touches no DuckDB connection, takes no lock, and every
// step runs on the flush's context, so a drain deadline can stop it.
//
// See docs/superpowers/specs/2026-09-14-postgres-sink-design.md.
type PostgresSink struct {
	connConfig *pgx.ConnConfig
	table      pgx.Identifier
	mode       string
	key        []string
	// timeout bounds one attempt: a probe, or one batch's transaction from
	// connect to commit. See WithPostgresTimeout.
	timeout time.Duration

	// mu guards the buffer.
	mu     sync.Mutex
	tables []arrow.Table

	// connMu guards the connection and the staging table, which Probe and
	// Flush both use. staging is the column set the temp table was created
	// for; nil means it does not exist on this connection.
	connMu   sync.Mutex
	conn     *pgx.Conn
	staging  []string
	warnings []string
}

// PostgresOption configures a PostgresSink.
type PostgresOption func(*PostgresSink)

// WithPostgresTimeout bounds each attempt. New passes the retry deadline, so
// a Postgres that holds packets and keeps the socket open fails an attempt
// inside the deadline instead of blocking the ladder, which checks its
// deadline only between attempts. Without it a window's poll never returned,
// and the window table grew until the container was killed (#290 review).
func WithPostgresTimeout(d time.Duration) PostgresOption {
	return func(s *PostgresSink) {
		if d > 0 {
			s.timeout = d
		}
	}
}

func NewPostgresSink(conf config.PostgresSink, opts ...PostgresOption) (*PostgresSink, error) {
	if strings.TrimSpace(conf.DSN) == "" {
		return nil, errs.New(errs.CodeSinkInvalid, "postgres sink: dsn is required")
	}
	if strings.TrimSpace(conf.Table) == "" {
		return nil, errs.New(errs.CodeSinkInvalid, "postgres sink: table is required")
	}
	switch conf.Mode {
	case PostgresModeUpsert:
		if len(conf.Key) == 0 {
			return nil, errs.New(errs.CodeSinkInvalid, "postgres sink: mode upsert needs key, the columns a row is identified by")
		}
	case PostgresModeAppend:
		if len(conf.Key) > 0 {
			return nil, errs.New(errs.CodeSinkInvalid, "postgres sink: mode append takes no key; every row is inserted as it is")
		}
	case "":
		return nil, errs.New(errs.CodeSinkInvalid, "postgres sink: mode is required: upsert or append")
	default:
		return nil, errs.New(errs.CodeSinkInvalid, "postgres sink: mode must be upsert or append, not %q", conf.Mode)
	}
	seen := map[string]bool{}
	for _, k := range conf.Key {
		if seen[k] {
			return nil, errs.New(errs.CodeSinkInvalid, "postgres sink: key names %q twice", k)
		}
		seen[k] = true
	}

	table, err := parsePostgresTable(conf.Table)
	if err != nil {
		return nil, err
	}
	// ParseConfig reads the DSN and the PG* environment and dials nothing.
	cc, err := pgx.ParseConfig(conf.DSN)
	if err != nil {
		return nil, errs.Wrap(errs.CodeSinkInvalid, err, "postgres sink: dsn")
	}
	s := &PostgresSink{
		connConfig: cc,
		table:      table,
		mode:       conf.Mode,
		key:        append([]string(nil), conf.Key...),
		timeout:    config.DefaultSinkRetryDeadlineSeconds * time.Second,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s, nil
}

// Key reports the columns a row is identified by, or nothing for append.
func (s *PostgresSink) Key() []string {
	if s.mode != PostgresModeUpsert {
		return nil
	}
	return append([]string(nil), s.key...)
}

func (s *PostgresSink) WriteTable(ctx context.Context, batch arrow.Table) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	batch.Retain()
	s.tables = append(s.tables, batch)
	return nil
}

// Flush delivers the buffered batches one at a time, in arrival order, each
// in its own transaction, and stops at the first failure with that batch and
// every later one still buffered.
//
// One transaction per batch, not one for all of them: after a failed flush
// the retry ladder calls Flush with two or more batches buffered, and a
// running total or a CDC stream carries the same key in consecutive batches.
// Merged in one statement those rows would hit ON CONFLICT DO UPDATE twice
// and fail with 21000, which the first attempt would have applied in order.
func (s *PostgresSink) Flush(ctx context.Context) error {
	for {
		s.mu.Lock()
		if len(s.tables) == 0 {
			s.mu.Unlock()
			return nil
		}
		// A reference of the flush's own, so a Close that releases the
		// buffer mid-send cannot free the batch being sent.
		head := s.tables[0]
		head.Retain()
		s.mu.Unlock()

		err := s.send(ctx, head)

		s.mu.Lock()
		// WriteTable only appends, so the head is still this batch unless
		// Close emptied the buffer while it was being sent.
		delivered := err == nil && len(s.tables) > 0 && s.tables[0] == head
		if delivered {
			s.tables = s.tables[1:]
		}
		s.mu.Unlock()
		if delivered {
			head.Release()
		}
		head.Release()
		if err != nil {
			return err
		}
	}
}

// BufferedRows reports the rows no flush has delivered.
func (s *PostgresSink) BufferedRows() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	var rows int64
	for _, t := range s.tables {
		rows += t.NumRows()
	}
	return int(rows)
}

// send is one batch's transaction. It releases nothing.
func (s *PostgresSink) send(ctx context.Context, tbl arrow.Table) error {
	// A handler whose query matched nothing yields an empty, column-less
	// table; there is nothing to copy.
	if tbl.NumRows() == 0 || tbl.NumCols() == 0 {
		return nil
	}
	cols := make([]string, tbl.NumCols())
	for i, f := range tbl.Schema().Fields() {
		cols[i] = f.Name
	}
	for _, k := range s.key {
		if !containsString(cols, k) {
			return errs.New(errs.CodeSinkInvalid, "postgres sink: key column %q is not in the batch, which has %s",
				k, strings.Join(cols, ", "))
		}
	}
	rows, err := postgresRows(tbl)
	if err != nil {
		return err
	}

	s.connMu.Lock()
	defer s.connMu.Unlock()
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	conn, err := s.connect(ctx)
	if err != nil {
		return err
	}
	if err := s.ensureStaging(ctx, conn, cols); err != nil {
		return s.failed(conn, err)
	}

	tx, err := conn.Begin(ctx)
	if err != nil {
		return s.failed(conn, postgresError(err, "postgres sink: begin"))
	}
	// Rollback after Commit is a no-op that returns ErrTxClosed. On the
	// flush's own context, which may already be done, the rollback would
	// fail; WithoutCancel lets it run.
	defer tx.Rollback(context.WithoutCancel(ctx))

	if _, err := tx.CopyFrom(ctx, pgx.Identifier{"pg_temp", "sqlflow_staging"}, append(cols, postgresSeq), pgx.CopyFromRows(rows)); err != nil {
		return s.failed(conn, postgresCopyError(err))
	}
	if _, err := tx.Exec(ctx, postgresMergeSQL(s.table, s.mode, s.key, cols)); err != nil {
		return s.failed(conn, postgresError(err, "postgres sink: merge into %s", s.table.Sanitize()))
	}
	if err := tx.Commit(ctx); err != nil {
		return s.failed(conn, postgresError(err, "postgres sink: commit"))
	}
	return nil
}

// connect returns the open connection, dialing when there is none. A
// connection a failure closed is replaced, and the staging table with it,
// because a temp table dies with its session.
func (s *PostgresSink) connect(ctx context.Context) (*pgx.Conn, error) {
	if s.conn != nil && !s.conn.IsClosed() {
		return s.conn, nil
	}
	s.conn, s.staging = nil, nil
	conn, err := pgx.ConnectConfig(ctx, s.connConfig)
	if err != nil {
		return nil, postgresError(err, "postgres sink: connect")
	}
	s.conn = conn
	return conn, nil
}

// ensureStaging creates the temp table for this column set, once per
// connection and column set. A batch whose columns differ from the last
// drops and recreates it.
func (s *PostgresSink) ensureStaging(ctx context.Context, conn *pgx.Conn, cols []string) error {
	if s.staging != nil && sameStrings(s.staging, cols) {
		return nil
	}
	for _, q := range postgresStagingSQL(s.table, cols) {
		if _, err := conn.Exec(ctx, q); err != nil {
			return postgresError(err, "postgres sink: staging table for %s", s.table.Sanitize())
		}
	}
	s.staging = append([]string(nil), cols...)
	return nil
}

// failed forgets a connection the failure closed, and codes the failure
// retryable when it did.
//
// A pooler or load balancer that closes an idle connection with a FIN sends
// no Postgres error, and pgx reports "conn closed", which classified as
// write_failed and stopped the pipeline with nothing written. A lost
// connection means the batch's transaction did not commit, or committed and
// lost its reply, and one transaction per batch makes the retry safe either
// way: an upsert replaces what it wrote, and append is at-least-once.
func (s *PostgresSink) failed(conn *pgx.Conn, err error) error {
	if !conn.IsClosed() {
		return err
	}
	s.conn, s.staging = nil, nil
	if errs.HasCode(err, errs.CodeSinkWriteFailed) {
		return errs.Wrap(errs.CodeSinkUnreachable, err, "postgres sink: the connection closed")
	}
	return err
}

func sameStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// Probe dials and checks the target: the table exists, every key column is
// a column of it, and for upsert a unique index or constraint covers exactly
// the key. For append a unique index is a warning: a redelivery after a
// crash inserts the same rows again and fails, so append is at-least-once
// for the reader.
func (s *PostgresSink) Probe(ctx context.Context) error {
	s.connMu.Lock()
	defer s.connMu.Unlock()
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	conn, err := s.connect(ctx)
	if err != nil {
		return err
	}
	name := s.table.Sanitize()

	var exists bool
	if err := conn.QueryRow(ctx, postgresTableExistsSQL, name).Scan(&exists); err != nil {
		return s.failed(conn, postgresError(err, "postgres sink: looking up %s", name))
	}
	if !exists {
		return errs.New(errs.CodeSinkInvalid, "postgres sink: table %s does not exist; the sink writes to a table it does not create", name)
	}

	rows, err := conn.Query(ctx, postgresColumnsSQL, name)
	if err != nil {
		return s.failed(conn, postgresError(err, "postgres sink: reading the columns of %s", name))
	}
	var columns []string
	notNull := map[string]bool{}
	for rows.Next() {
		var c string
		var nn bool
		if err := rows.Scan(&c, &nn); err != nil {
			rows.Close()
			return postgresError(err, "postgres sink: reading the columns of %s", name)
		}
		columns = append(columns, c)
		notNull[c] = nn
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return s.failed(conn, postgresError(err, "postgres sink: reading the columns of %s", name))
	}
	for _, k := range s.key {
		if !containsString(columns, k) {
			return errs.New(errs.CodeSinkInvalid, "postgres sink: key column %q is not a column of %s, which has %s",
				k, name, strings.Join(columns, ", "))
		}
		// A unique index treats NULLs as distinct, so ON CONFLICT never
		// matches a null key, and a redelivery inserts the row again.
		if !notNull[k] {
			return errs.New(errs.CodeSinkInvalid,
				"postgres sink: key column %q is nullable on %s; a unique index never matches a null key, so a redelivered row would be inserted twice. Declare it NOT NULL",
				k, name)
		}
	}

	if s.mode == PostgresModeUpsert {
		sorted := append([]string(nil), s.key...)
		sort.Strings(sorted)
		var unique bool
		if err := conn.QueryRow(ctx, postgresUniqueIndexSQL, name, sorted).Scan(&unique); err != nil {
			return s.failed(conn, postgresError(err, "postgres sink: reading the indexes of %s", name))
		}
		if !unique {
			return errs.New(errs.CodeSinkInvalid,
				"postgres sink: no unique index or constraint covers exactly (%s) on %s, and ON CONFLICT needs one; "+
					"a partial or expression index does not count. Add PRIMARY KEY (%s) or CREATE UNIQUE INDEX ON %s (%s)",
				strings.Join(s.key, ", "), name, strings.Join(s.key, ", "), name, strings.Join(s.key, ", "))
		}
		return nil
	}

	var anyUnique bool
	if err := conn.QueryRow(ctx, postgresAnyUniqueIndexSQL, name).Scan(&anyUnique); err != nil {
		return s.failed(conn, postgresError(err, "postgres sink: reading the indexes of %s", name))
	}
	if anyUnique {
		s.warnings = append(s.warnings, fmt.Sprintf(
			"postgres sink: %s has a unique index and the mode is append; a redelivery after a crash inserts the same rows again and fails. append is at-least-once for the reader; use upsert with a key for exactly-once", name))
	}
	return nil
}

// Warnings reports what Probe found that is not an error.
func (s *PostgresSink) Warnings() []string { return append([]string(nil), s.warnings...) }

// Close releases every batch still buffered and the connection. Safe to
// call twice. A batch a failed flush kept is dropped here, not delivered:
// the pipeline did not commit its offsets, so the next start replays it.
func (s *PostgresSink) Close() error {
	s.mu.Lock()
	for _, t := range s.tables {
		t.Release()
	}
	s.tables = nil
	s.mu.Unlock()

	s.connMu.Lock()
	defer s.connMu.Unlock()
	if s.conn == nil {
		return nil
	}
	conn := s.conn
	s.conn, s.staging = nil, nil
	return conn.Close(context.Background())
}
