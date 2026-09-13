package duckdb

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"sync"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// LockChecked wraps a connection so that every statement executed on it, and
// every commit or rollback, must run while the given lock is held. It is a
// test aid for the one rule the pipeline has about its DuckDB connection.
//
// The pipeline, the table managers, the progress store and the debug API all
// share one connection, serialized by one mutex. DuckDB closes a pending
// result on a connection the moment another statement runs on it, so a
// statement that skips the lock does not deadlock or corrupt anything: it
// fails whatever query was in flight, with "Attempting to execute an
// unsuccessful or closed pending query result". The failure lands on the
// other party, at a rate set by traffic, which is why #280 shipped and ran
// for two days before a batch size of one made it reproducible.
//
// A race like that has no deterministic reproduction. The invariant does:
// at the moment a statement executes, the lock is held. TryLock succeeding
// means nobody held it, which is a violation whoever the caller is. TryLock
// failing means someone held it, and this wrapper cannot tell whether that
// someone is the caller; a concurrent holder hides the violation for that
// one call. So run the pipeline single-threaded under the wrapper, and every
// unlocked statement is caught on its first execution.
type LockChecked struct {
	adbc.Connection
	lock *sync.Mutex

	mu         sync.Mutex
	violations []string
}

// NewLockChecked returns conn wrapped so that executing on it without lock
// held is recorded. Wrap after startup: the tables are created before any
// second goroutine exists, and root.go creates them without the lock.
func NewLockChecked(conn adbc.Connection, lock *sync.Mutex) *LockChecked {
	return &LockChecked{Connection: conn, lock: lock}
}

// Violations lists every execution that ran without the lock, each with the
// operation and the frames that led to it.
func (c *LockChecked) Violations() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.violations...)
}

func (c *LockChecked) check(op string) {
	if !c.lock.TryLock() {
		return
	}
	c.lock.Unlock()

	pcs := make([]uintptr, 12)
	n := runtime.Callers(3, pcs)
	frames := runtime.CallersFrames(pcs[:n])
	var where []string
	for {
		f, more := frames.Next()
		if !strings.HasSuffix(f.File, "lockcheck.go") {
			where = append(where, fmt.Sprintf("%s:%d", f.Function, f.Line))
		}
		if !more || len(where) == 6 {
			break
		}
	}

	c.mu.Lock()
	c.violations = append(c.violations, op+" without the lock\n\t"+strings.Join(where, "\n\t"))
	c.mu.Unlock()
}

func (c *LockChecked) Commit(ctx context.Context) error {
	c.check("Commit")
	return c.Connection.Commit(ctx)
}

func (c *LockChecked) Rollback(ctx context.Context) error {
	c.check("Rollback")
	return c.Connection.Rollback(ctx)
}

func (c *LockChecked) NewStatement() (adbc.Statement, error) {
	stmt, err := c.Connection.NewStatement()
	if err != nil {
		return nil, err
	}
	return &lockCheckedStatement{Statement: stmt, conn: c}, nil
}

// SetOption forwards to the wrapped connection's post-init options, which
// root.go uses to turn autocommit off for a state path.
func (c *LockChecked) SetOption(key, val string) error {
	po, ok := c.Connection.(adbc.PostInitOptions)
	if !ok {
		return fmt.Errorf("lockcheck: wrapped connection does not support SetOption")
	}
	return po.SetOption(key, val)
}

type lockCheckedStatement struct {
	adbc.Statement
	conn *LockChecked
	sql  string
}

func (s *lockCheckedStatement) SetSqlQuery(query string) error {
	s.sql = query
	return s.Statement.SetSqlQuery(query)
}

func (s *lockCheckedStatement) ExecuteQuery(ctx context.Context) (array.RecordReader, int64, error) {
	s.conn.check("ExecuteQuery " + firstLine(s.sql))
	reader, n, err := s.Statement.ExecuteQuery(ctx)
	if err != nil {
		return nil, n, err
	}
	return &lockCheckedReader{RecordReader: reader, stmt: s}, n, nil
}

func (s *lockCheckedStatement) ExecuteUpdate(ctx context.Context) (int64, error) {
	s.conn.check("ExecuteUpdate " + firstLine(s.sql))
	return s.Statement.ExecuteUpdate(ctx)
}

// lockCheckedReader checks each Next. A result is streamed, so a reader
// drained after the lock is released is as exposed as a statement executed
// without it.
type lockCheckedReader struct {
	array.RecordReader
	stmt *lockCheckedStatement
}

func (r *lockCheckedReader) Next() bool {
	r.stmt.conn.check("Next " + firstLine(r.stmt.sql))
	return r.RecordReader.Next()
}

func firstLine(sql string) string {
	sql = strings.TrimSpace(sql)
	if i := strings.IndexByte(sql, '\n'); i >= 0 {
		sql = sql[:i]
	}
	if len(sql) > 60 {
		sql = sql[:60]
	}
	return sql
}
