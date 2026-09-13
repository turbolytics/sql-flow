package serve

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/turbolytics/sql-flow/internal/errs"
	"github.com/zeebo/assert"
)

// slowSQL runs for about a second on a 10-core laptop, ten times the
// deadlines below. A 100M-row cross join parallelizes poorly, so a larger CI
// runner does not bring it under them; range(200000000) alone took 320 ms.
const slowSQL = "SELECT sum(hash(a.range * b.range)) AS n FROM range(10000) a, range(10000) b"

func mustPrepare(t *testing.T, conn adbc.Connection, sql string, params ...config.ServeParam) *statement {
	t.Helper()
	st, err := prepare(context.Background(), conn, "ds", "", sql, params)
	assert.NoError(t, err)
	return st
}

func queryRows(t *testing.T, conn adbc.Connection, st *statement, values map[string]any) []map[string]any {
	t.Helper()
	res, err := st.query(context.Background(), conn, values, 100)
	assert.NoError(t, err)
	return decodeRows(t, res)
}

// A missing table fails at startup, naming the dataset and grain, not on the
// first request.
func TestCliServe_PrepareFailsAtStartupNamingTheStatement(t *testing.T) {
	coverage.Covers(t, "cli.serve")
	conn := newConn(t)

	_, err := prepare(context.Background(), conn, "posts", "1h", "SELECT * FROM no_such_table", nil)
	assert.Error(t, err)
	assert.Equal(t, errs.CodeSQLInvalid, errs.CodeOf(err))
	assert.That(t, strings.Contains(err.Error(), "dataset posts grain 1h"))
	assert.That(t, strings.Contains(err.Error(), "no_such_table"))

	_, err = prepare(context.Background(), conn, "posts", "", "SELECT $1", nil)
	assert.Equal(t, errs.CodeConfigServeDataset, errs.CodeOf(err))
}

// The scanner does not know E'…' strings. A backslash-escaped quote makes it
// see $fake outside the literal, where DuckDB sees none. Binding a value to a
// placeholder DuckDB does not have would misbind every value after it, so the
// server refuses to start.
func TestCliServe_PrepareRefusesAParamCountMismatch(t *testing.T) {
	coverage.Covers(t, "cli.serve")
	conn := newConn(t)

	_, err := prepare(context.Background(), conn, "posts", "", `SELECT E'it\'s $fake' AS s`,
		[]config.ServeParam{{Name: "fake", Type: "string"}})
	assert.Error(t, err)
	assert.Equal(t, errs.CodeSQLInvalid, errs.CodeOf(err))
	assert.That(t, strings.Contains(err.Error(), "DuckDB counts 0 parameters and sqlflow counts 1"))
}

// DuckDB binds by position. $b appears first, so it is $1, and $a used twice
// is one placeholder. A misnumbering binds a to b and answers 200 with wrong
// rows.
func TestCliServe_BindsEachValueToItsOwnPlaceholder(t *testing.T) {
	coverage.Covers(t, "cli.serve")
	conn := newConn(t)

	st := mustPrepare(t, conn, "SELECT $b AS b, $a AS a, $a || '!' AS a2",
		config.ServeParam{Name: "a", Type: "string"},
		config.ServeParam{Name: "b", Type: "string"})

	rows := queryRows(t, conn, st, map[string]any{"a": "from a", "b": "from b"})
	assert.Equal(t, "from b", rows[0]["b"])
	assert.Equal(t, "from a", rows[0]["a"])
	assert.Equal(t, "from a!", rows[0]["a2"])
}

// An absent param is a null of its declared type, so coalesce supplies the
// default and the column keeps its type.
func TestCliServe_AnAbsentParamBindsATypedNull(t *testing.T) {
	coverage.Covers(t, "cli.serve")
	conn := newConn(t)

	st := mustPrepare(t, conn, `SELECT
			coalesce($since, TIMESTAMPTZ '2000-01-01 00:00:00+00') AS since,
			coalesce($n, 7) AS n,
			coalesce($s, 'default') AS s`,
		config.ServeParam{Name: "since", Type: "timestamp"},
		config.ServeParam{Name: "n", Type: "integer"},
		config.ServeParam{Name: "s", Type: "string"})

	res, err := st.query(context.Background(), conn, map[string]any{}, 10)
	assert.NoError(t, err)
	assert.Equal(t, "TIMESTAMP WITH TIME ZONE", res.Columns[0].Type)
	assert.Equal(t, "BIGINT", res.Columns[1].Type)
	rows := decodeRows(t, res)
	assert.Equal(t, "2000-01-01T00:00:00Z", rows[0]["since"])
	assert.Equal(t, float64(7), rows[0]["n"])
	assert.Equal(t, "default", rows[0]["s"])

	since := time.Date(2026, 9, 10, 12, 30, 0, 0, time.FixedZone("EDT", -4*3600))
	rows = queryRows(t, conn, st, map[string]any{"since": since, "n": int64(-3), "s": "given"})
	assert.Equal(t, "2026-09-10T16:30:00Z", rows[0]["since"])
	assert.Equal(t, float64(-3), rows[0]["n"])
	assert.Equal(t, "given", rows[0]["s"])
}

// Each request plans against the data as it is. StructuredBatch held one
// plan, built against an empty table, and returned nothing forever after.
func TestCliServe_ARequestSeesTheTableAsItIsNow(t *testing.T) {
	coverage.Covers(t, "cli.serve")
	conn := newConn(t)
	execSQL(t, conn, "CREATE TABLE t (n BIGINT)")

	st := mustPrepare(t, conn, "SELECT n FROM t WHERE n >= coalesce($min, 0) ORDER BY n",
		config.ServeParam{Name: "min", Type: "integer"})
	assert.Equal(t, 0, len(queryRows(t, conn, st, nil)))

	execSQL(t, conn, "INSERT INTO t VALUES (1), (2), (3)")
	assert.Equal(t, 3, len(queryRows(t, conn, st, nil)))
	assert.Equal(t, 1, len(queryRows(t, conn, st, map[string]any{"min": int64(3)})))
}

// DuckDB cannot be cancelled, so the deadline bounds the caller's wait and
// the query keeps the lock. A request right behind it waits on the lock and
// times out too. Once the query finishes, the next request answers.
func TestCliServe_ATimeoutReturnsWhileTheQueryHoldsTheLock(t *testing.T) {
	coverage.Covers(t, "cli.serve")
	conn := newConn(t)
	exec := &executor{conn: conn}

	slow := mustPrepare(t, conn, slowSQL)
	fast := mustPrepare(t, conn, "SELECT 1 AS n")
	runStatement := func(st *statement) func(context.Context, adbc.Connection) (result, error) {
		return func(ctx context.Context, conn adbc.Connection) (result, error) {
			return st.query(ctx, conn, nil, 10)
		}
	}

	start := time.Now()
	_, err := exec.run(context.Background(), 100*time.Millisecond, runStatement(slow))
	assert.That(t, errors.Is(err, context.DeadlineExceeded))
	assert.That(t, time.Since(start) < 500*time.Millisecond)

	_, err = exec.run(context.Background(), 100*time.Millisecond, runStatement(fast))
	assert.That(t, errors.Is(err, context.DeadlineExceeded))

	res, err := exec.run(context.Background(), 60*time.Second, runStatement(fast))
	assert.NoError(t, err)
	assert.Equal(t, 1, res.RowCount)
}

// Shutdown closes the connection after close returns, so close must wait for
// the running query, and nothing may start on the connection afterwards.
func TestCliServe_CloseWaitsForTheRunningQuery(t *testing.T) {
	coverage.Covers(t, "cli.serve")
	conn := newConn(t)
	exec := &executor{conn: conn}
	slow := mustPrepare(t, conn, slowSQL)

	finished := make(chan time.Time, 1)
	go func() {
		_, _ = exec.run(context.Background(), 60*time.Second,
			func(ctx context.Context, conn adbc.Connection) (result, error) {
				defer func() { finished <- time.Now() }()
				return slow.query(ctx, conn, nil, 10)
			})
	}()
	time.Sleep(50 * time.Millisecond)

	exec.close()
	closedAt := time.Now()
	queryDone := <-finished
	assert.That(t, !closedAt.Before(queryDone))

	_, err := exec.run(context.Background(), time.Second,
		func(context.Context, adbc.Connection) (result, error) {
			t.Fatal("a query ran after close")
			return result{}, nil
		})
	assert.That(t, errors.Is(err, errClosed))
}
