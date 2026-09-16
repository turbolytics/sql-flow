package serve

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/turbolytics/sql-flow/internal/config"
)

// ErrClosed is what Acquire returns once the executor has closed.
var ErrClosed = errors.New("the server is shutting down")

// Executor runs a serve config's datasets. Serve holds one; each request
// borrows a Session from it.
//
// This is an interface because the engine underneath may change. The Postgres
// sink had to leave DuckDB's postgres extension for pgx when that extension
// read the whole target table on every write (#290), and that move was
// expensive because the driver was welded into the write path.
type Executor interface {
	// Prepare checks one dataset statement and returns a handle for it.
	// Called at startup for every statement, so a dataset that cannot run
	// fails the start rather than the first request.
	Prepare(ctx context.Context, spec StatementSpec) (Statement, error)

	// Acquire borrows a session. It blocks until one is free, ctx ends, or
	// the executor closes, and returns ctx.Err() or ErrClosed in those cases.
	Acquire(ctx context.Context) (Session, error)

	// Stats reports what the pool gauges publish.
	Stats() Stats

	// Close waits for every borrowed session to be released, then refuses
	// later acquires and closes the sessions.
	Close()
}

// Session runs one statement at a time. A caller holds it for one request.
type Session interface {
	// Run executes st with one request's values. The returned reader stays
	// valid until it is released, and values read from it may alias buffers
	// the reader owns, so a caller encodes each value before advancing.
	//
	// values maps a declared param name to a string, int64 or time.Time. A
	// name the map does not carry binds NULL.
	Run(ctx context.Context, st Statement, values map[string]any) (array.RecordReader, error)

	// Release returns the session. It is safe to call twice; the second is a
	// no-op, so a deferred Release beside an early return cannot double-free.
	Release()
}

// Statement is one statement an Executor has checked. Its contents belong to
// the executor; serve only names it in errors.
type Statement interface {
	// Where names the statement: "dataset posts_by_lang grain 1h".
	Where() string
}

// StatementSpec is what an Executor needs to prepare a statement.
type StatementSpec struct {
	Dataset string
	// Grain is empty for a dataset without grains.
	Grain  string
	SQL    string
	Params []config.ServeParam
}

// Stats is the pool's state at one instant, for the gauges and for the
// health endpoint. How long callers wait is a histogram, not a gauge, so it
// is not here.
type Stats struct {
	// Size is every session the executor holds.
	Size int
	// InUse is the sessions currently borrowed.
	InUse int
}

// backendSession is what an Executor implementation gives the pool: run a
// statement, and close. The pool owns everything else, because queueing and
// its measurement are the same whatever runs the SQL.
type backendSession interface {
	run(ctx context.Context, st Statement, values map[string]any) (array.RecordReader, error)
	close() error
}

// pool hands out a fixed set of sessions, one caller at a time. An Executor
// implementation embeds it.
type pool struct {
	// free carries every session not currently borrowed. Its capacity is the
	// pool's size, so a Release never blocks.
	free chan *session
	all  []*session
	// onWait receives how long each Acquire waited, including the ones that
	// gave up. The metrics histogram is the only reader.
	onWait func(time.Duration)

	mu     sync.Mutex
	inUse  int
	closed bool
}

// session is one backend session while a caller holds it.
type session struct {
	pool     *pool
	backend  backendSession
	released atomic.Bool
}

func newPool(backends []backendSession, onWait func(time.Duration)) *pool {
	if onWait == nil {
		// A server without the metrics endpoint has nobody to tell.
		onWait = func(time.Duration) {}
	}
	p := &pool{free: make(chan *session, len(backends)), onWait: onWait}
	for _, b := range backends {
		s := &session{pool: p, backend: b}
		s.released.Store(true)
		p.all = append(p.all, s)
		p.free <- s
	}
	return p
}

// Acquire borrows a session, waiting until one is free, ctx ends, or the
// executor closes.
func (p *pool) Acquire(ctx context.Context) (Session, error) {
	start := time.Now()
	select {
	case s, ok := <-p.free:
		if !ok {
			return nil, ErrClosed
		}
		p.onWait(time.Since(start))
		p.mu.Lock()
		p.inUse++
		p.mu.Unlock()
		s.released.Store(false)
		return s, nil
	case <-ctx.Done():
		p.onWait(time.Since(start))
		return nil, ctx.Err()
	}
}

func (p *pool) Stats() Stats {
	p.mu.Lock()
	defer p.mu.Unlock()
	return Stats{Size: len(p.all), InUse: p.inUse}
}

// Close refuses later acquires, waits for every borrowed session to come
// back, and only then closes them. A session closed while a query runs on it
// takes the process down, and nothing outside the goroutine reading that
// query can stop it, so waiting is the only option. The HTTP server's drain
// bounds how long that can be.
func (p *pool) Close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	p.mu.Unlock()

	for range p.all {
		<-p.free
	}
	close(p.free)
	for _, s := range p.all {
		_ = s.backend.close()
	}
}

func (s *session) Run(ctx context.Context, st Statement, values map[string]any) (array.RecordReader, error) {
	return s.backend.run(ctx, st, values)
}

// Release returns the session. The second call is a no-op, so a deferred
// Release beside an early return cannot return one session twice.
func (s *session) Release() {
	if s.released.Swap(true) {
		return
	}
	s.pool.mu.Lock()
	s.pool.inUse--
	s.pool.mu.Unlock()
	// Never blocks: free's capacity is the pool's size, and this session is
	// not in it.
	s.pool.free <- s
}

// query acquires a session, runs st, and encodes at most maxRows rows. It
// returns the rows, how long the acquire waited, and any error.
//
// The work runs on its own goroutine and hands back finished bytes, so a
// caller that has already given up at its deadline cannot race it. The
// session returns to the pool when the query finishes rather than when the
// caller stops waiting: handing a session to the next request while a query
// still runs on it would serialise them behind work nobody wants.
//
// ctx reaches readRows, which stops at the first batch boundary after the
// caller gives up and releases the reader. ADBC documents releasing a reader
// without draining it as equivalent to AdbcStatementCancel, and that is the
// only cancel its Go API offers.
func query(ctx context.Context, ex Executor, st Statement, values map[string]any, maxRows int) (result, time.Duration, error) {
	start := time.Now()
	sess, err := ex.Acquire(ctx)
	if err != nil {
		return result{}, time.Since(start), err
	}
	waited := time.Since(start)

	type outcome struct {
		res result
		err error
	}
	done := make(chan outcome, 1)
	go func() {
		defer sess.Release()
		rdr, err := sess.Run(ctx, st, values)
		if err != nil {
			done <- outcome{err: err}
			return
		}
		res, err := func() (result, error) {
			// Released whether or not the rows are drained, which is what
			// tells the driver to stop, and before the caller is told: the
			// buffers go back before the next request allocates its own.
			defer rdr.Release()
			return readRows(ctx, rdr, maxRows)
		}()
		done <- outcome{res: res, err: err}
	}()

	select {
	case o := <-done:
		return o.res, waited, o.err
	case <-ctx.Done():
		return result{}, waited, ctx.Err()
	}
}
