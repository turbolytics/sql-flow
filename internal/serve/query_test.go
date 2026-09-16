package serve

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/turbolytics/sql-flow/internal/errs"
	"github.com/zeebo/assert"
)

// slowSQL runs for about a second on a 10-core laptop, ten times the
// deadlines below. A 100M-row cross join parallelizes poorly, so a larger CI
// runner does not bring it under them; range(200000000) alone took 320 ms.
const slowSQL = "SELECT sum(hash(a.range * b.range)) AS n FROM range(10000) a, range(10000) b"

func mustPrepare(t *testing.T, ex Executor, sql string, params ...config.ServeParam) Statement {
	t.Helper()
	st, err := ex.Prepare(context.Background(), StatementSpec{Dataset: "ds", SQL: sql, Params: params})
	assert.NoError(t, err)
	return st
}

func queryRows(t *testing.T, ex Executor, st Statement, values map[string]any) []map[string]any {
	t.Helper()
	res, _, err := query(context.Background(), ex, st, values, 100)
	assert.NoError(t, err)
	return decodeRows(t, res)
}

// A missing table fails at startup, naming the dataset and grain, not on the
// first request.
func TestCliServe_PrepareFailsAtStartupNamingTheStatement(t *testing.T) {
	coverage.Covers(t, "cli.serve")
	ex, _ := newExec(t, 1)

	_, err := ex.Prepare(context.Background(), StatementSpec{
		Dataset: "posts", Grain: "1h", SQL: "SELECT * FROM no_such_table"})
	assert.Error(t, err)
	assert.Equal(t, errs.CodeSQLInvalid, errs.CodeOf(err))
	assert.That(t, strings.Contains(err.Error(), "dataset posts grain 1h"))
	assert.That(t, strings.Contains(err.Error(), "no_such_table"))

	_, err = ex.Prepare(context.Background(), StatementSpec{Dataset: "posts", SQL: "SELECT $1"})
	assert.Equal(t, errs.CodeConfigServeDataset, errs.CodeOf(err))
}

// The scanner does not know E'…' strings. A backslash-escaped quote makes it
// see $fake outside the literal, where DuckDB sees none. Binding a value to a
// placeholder DuckDB does not have would misbind every value after it, so the
// server refuses to start.
func TestCliServe_PrepareRefusesAParamCountMismatch(t *testing.T) {
	coverage.Covers(t, "cli.serve")
	ex, _ := newExec(t, 1)

	_, err := ex.Prepare(context.Background(), StatementSpec{
		Dataset: "posts",
		SQL:     `SELECT E'it\'s $fake' AS s`,
		Params:  []config.ServeParam{{Name: "fake", Type: "string"}},
	})
	assert.Error(t, err)
	assert.Equal(t, errs.CodeSQLInvalid, errs.CodeOf(err))
	assert.That(t, strings.Contains(err.Error(), "DuckDB counts 0 parameters and sqlflow counts 1"))
}

// DuckDB binds by position. $b appears first, so it is $1, and $a used twice
// is one placeholder. A misnumbering binds a to b and answers 200 with wrong
// rows.
func TestCliServe_BindsEachValueToItsOwnPlaceholder(t *testing.T) {
	coverage.Covers(t, "cli.serve")
	ex, _ := newExec(t, 1)

	st := mustPrepare(t, ex, "SELECT $b AS b, $a AS a, $a || '!' AS a2",
		config.ServeParam{Name: "a", Type: "string"},
		config.ServeParam{Name: "b", Type: "string"})

	rows := queryRows(t, ex, st, map[string]any{"a": "from a", "b": "from b"})
	assert.Equal(t, "from b", rows[0]["b"])
	assert.Equal(t, "from a", rows[0]["a"])
	assert.Equal(t, "from a!", rows[0]["a2"])
}

// An absent param is a null of its declared type, so coalesce supplies the
// default and the column keeps its type.
func TestCliServe_AnAbsentParamBindsATypedNull(t *testing.T) {
	coverage.Covers(t, "cli.serve")
	ex, _ := newExec(t, 1)

	st := mustPrepare(t, ex, `SELECT
			coalesce($since, TIMESTAMPTZ '2000-01-01 00:00:00+00') AS since,
			coalesce($n, 7) AS n,
			coalesce($s, 'default') AS s`,
		config.ServeParam{Name: "since", Type: "timestamp"},
		config.ServeParam{Name: "n", Type: "integer"},
		config.ServeParam{Name: "s", Type: "string"})

	res, _, err := query(context.Background(), ex, st, map[string]any{}, 10)
	assert.NoError(t, err)
	assert.Equal(t, "TIMESTAMP WITH TIME ZONE", res.Columns[0].Type)
	assert.Equal(t, "BIGINT", res.Columns[1].Type)
	rows := decodeRows(t, res)
	assert.Equal(t, "2000-01-01T00:00:00Z", rows[0]["since"])
	assert.Equal(t, float64(7), rows[0]["n"])
	assert.Equal(t, "default", rows[0]["s"])

	since := time.Date(2026, 9, 10, 12, 30, 0, 0, time.FixedZone("EDT", -4*3600))
	rows = queryRows(t, ex, st, map[string]any{"since": since, "n": int64(-3), "s": "given"})
	assert.Equal(t, "2026-09-10T16:30:00Z", rows[0]["since"])
	assert.Equal(t, float64(-3), rows[0]["n"])
	assert.Equal(t, "given", rows[0]["s"])
}

// Each request plans against the data as it is. StructuredBatch held one
// plan, built against an empty table, and returned nothing forever after.
func TestCliServe_ARequestSeesTheTableAsItIsNow(t *testing.T) {
	coverage.Covers(t, "cli.serve")
	ex, db := newExec(t, 1, "CREATE TABLE t (n BIGINT)")

	// A writer outside the pool, the way an attached source changes under a
	// running server.
	writer, err := db.Connect(context.Background())
	assert.NoError(t, err)
	t.Cleanup(func() { _ = writer.Close() })

	st := mustPrepare(t, ex, "SELECT n FROM t WHERE n >= coalesce($min, 0) ORDER BY n",
		config.ServeParam{Name: "min", Type: "integer"})
	assert.Equal(t, 0, len(queryRows(t, ex, st, nil)))

	execSQL(t, writer, "INSERT INTO t VALUES (1), (2), (3)")
	assert.Equal(t, 3, len(queryRows(t, ex, st, nil)))
	assert.Equal(t, 1, len(queryRows(t, ex, st, map[string]any{"min": int64(3)})))
}

// DuckDB cannot be cancelled, so the deadline bounds the caller's wait and
// the query keeps its session. A request right behind it waits for that
// session and times out too. Once the query finishes, the next request
// answers. This is a pool of one, which is the behaviour serve had before
// there was a pool.
func TestCliServe_ATimeoutReturnsWhileTheQueryHoldsTheSession(t *testing.T) {
	coverage.Covers(t, "cli.serve")
	ex, _ := newExec(t, 1)

	slow := mustPrepare(t, ex, slowSQL)
	fast := mustPrepare(t, ex, "SELECT 1 AS n")
	run := func(st Statement, timeout time.Duration) (result, error) {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		res, _, err := query(ctx, ex, st, nil, 10)
		return res, err
	}

	start := time.Now()
	_, err := run(slow, 100*time.Millisecond)
	assert.That(t, errors.Is(err, context.DeadlineExceeded))
	assert.That(t, time.Since(start) < 500*time.Millisecond)

	_, err = run(fast, 100*time.Millisecond)
	assert.That(t, errors.Is(err, context.DeadlineExceeded))

	res, err := run(fast, 60*time.Second)
	assert.NoError(t, err)
	assert.Equal(t, 1, res.RowCount)
}

// Shutdown closes the sessions after Close returns, so Close must wait for
// the running query, and nothing may start on a session afterwards.
func TestCliServe_CloseWaitsForTheRunningQuery(t *testing.T) {
	coverage.Covers(t, "cli.serve")
	ex, _ := newExec(t, 1)
	slow := mustPrepare(t, ex, slowSQL)

	sess, err := ex.Acquire(context.Background())
	assert.NoError(t, err)

	finished := make(chan time.Time, 1)
	go func() {
		rdr, err := sess.Run(context.Background(), slow, nil)
		if err == nil {
			_, _ = readRows(rdr, 10)
			rdr.Release()
		}
		// Recorded before the session goes back, so a Close that returned
		// first would be a Close that did not wait.
		finished <- time.Now()
		sess.Release()
	}()
	time.Sleep(50 * time.Millisecond)

	ex.Close()
	closedAt := time.Now()
	queryDone := <-finished
	assert.That(t, !closedAt.Before(queryDone))

	_, err = ex.Acquire(context.Background())
	assert.That(t, errors.Is(err, ErrClosed))
}
