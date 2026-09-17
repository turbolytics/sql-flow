# Serve Session Pool Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace serve's single serialized DuckDB connection with a pool of sessions behind an executor interface, and add the metrics that size it.

**Architecture:** `internal/serve/executor.go` defines `Executor`, `Session` and `Statement`, and holds the pool mechanics — the session channel, the wait timing, and the goroutine that lets a caller give up without abandoning a running query. `internal/serve/duckdb.go` is the only file in the package that imports ADBC. `serve.New` takes an `Executor` instead of a connection.

**Tech Stack:** Go 1.25, ADBC over DuckDB 1.5.2, arrow-go/v18, OpenTelemetry instruments exported through a `prometheus.Registry`, cobra, zeebo/assert, testcontainers Postgres.

**Spec:** `docs/superpowers/specs/2026-09-16-serve-pool-design.md`

## Global Constraints

- Branch from `origin/main` at or after `54b17f2` (v2026.09.16). One PR.
- Default `pool.size` is **4**, subject to Task 1's measurement.
- `pool.size` accepts 0 (meaning the default) through 64; negative or above 64 is `user.config.invalid` at the key's path.
- Serve runs `SET TimeZone='UTC'` on every session it opens. The config's `commands` run exactly once, on a setup connection.
- The interchange is `array.RecordReader`. Serve owns `readRows` and the `max_rows` cap. Nothing in `executor.go` imports ADBC or names DuckDB.
- A session returns to the pool when its query finishes, never when a caller gives up. ADBC's Go API has no `Cancel` (checked against v1.6.0), so nothing outside the reading goroutine can stop a query.
- Releasing a `RecordReader` without draining it is ADBC's documented equivalent of cancel, so `readRows` takes the request's context and stops at the first batch boundary after it ends.
- Waiting for a session counts toward the dataset's timeout; exhausting it is `504 query_timeout`, the existing code.
- Metrics are off unless `serve.metrics.enabled` is true, and are served at `GET /metrics` on the existing listener with no token.
- Unit tests are `TestCliServe_*` and call `coverage.Covers(t, "cli.serve")`. Integration tests are `TestIntegrationServePool_*`, skip under `-short`, and start their own container.
- Unit: `go test -short -race ./...`. Integration: `go test -run '^TestIntegration' ./internal/serve/`. Goldens regenerate with `UPDATE_GOLDEN=1`; schemas with `make schema`.
- Comments say why, in full sentences, matching the surrounding code. Commit messages are `area: what changed`.

## File Structure

```
internal/serve/executor.go        new: the interfaces, the pool, the query helper
internal/serve/duckdb.go          new: the DuckDB executor, statement, binding
internal/serve/query.go           shrinks: statement/prepare/executor move out
internal/serve/server.go          New takes an Executor; Close closes it
internal/serve/http.go            acquire per request; queued_ms; healthz busy
internal/serve/metrics.go         new: the six instruments and the handler
internal/config/serve.go          ServePool, ServeMetrics, their rules
internal/cli/serve/serve.go       builds the executor, passes commands as init
internal/validate/schemas/serve.json, internal/cli/testdata/serve_example.golden   regenerated
docs/coverage/features.yml        cli.serve gains integration
README.md, CHANGELOG.md           docs
```

---

### Task 1: Measure per-session memory in the release container

The spec's numbers are macOS, where `Maxrss` is bytes; on Linux it is KiB. Everything downstream assumes 4 sessions fit a 256 MB box. Settle it before writing the pool.

**Files:**
- Create, then delete: `cmd/poolmem/main.go`
- Modify, only if the number says so: the default in `internal/config/serve.go` (Task 2) and the spec's "Memory, and why 4"

**Interfaces:**
- Produces: a number, recorded in the PR body. No code survives this task.

- [ ] **Step 1: Branch**

```bash
cd /Users/danielmican/code/github.com/turbolytics/sql-flow
git fetch origin && git switch -c feat/serve-pool origin/main
go build ./... && go test -short ./internal/serve/
```

Expected: builds, and `internal/serve` passes.

- [ ] **Step 2: Write the probe**

Create `cmd/poolmem/main.go`. It opens one in-memory DuckDB, creates a table the size of the demo's widest allowed request (365 buckets × 170 languages), then runs the fold on N connections at once and reports peak resident memory.

```go
// Command poolmem reports peak resident memory with N concurrent DuckDB
// sessions, to size serve's pool. Throwaway: delete it after recording the
// number.
package main

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"sync"
	"syscall"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/turbolytics/sql-flow/internal/duckdb"
)

func run(c adbc.Connection, sql string) error {
	st, err := c.NewStatement()
	if err != nil {
		return err
	}
	defer st.Close()
	if err := st.SetSqlQuery(sql); err != nil {
		return err
	}
	rdr, _, err := st.ExecuteQuery(context.Background())
	if err != nil {
		return err
	}
	defer rdr.Release()
	for rdr.Next() {
	}
	return rdr.Err()
}

// rssMiB reads the process's peak resident size. Linux reports KiB and
// darwin reports bytes, which is the whole reason this runs in the container.
func rssMiB() float64 {
	var u syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &u)
	if runtime.GOOS == "linux" {
		return float64(u.Maxrss) / 1024
	}
	return float64(u.Maxrss) / (1024 * 1024)
}

const fold = `SELECT bucket, CASE WHEN r <= 10 THEN lang ELSE 'other' END AS lang, sum(posts)::BIGINT AS posts
FROM (SELECT bucket, lang, posts, dense_rank() OVER (ORDER BY tot DESC, lang) AS r
      FROM (SELECT bucket, lang, posts, sum(posts) OVER (PARTITION BY lang) AS tot FROM r))
GROUP BY ALL`

func main() {
	n, err := strconv.Atoi(os.Args[1])
	if err != nil {
		panic(err)
	}
	ctx := context.Background()
	db, err := duckdb.OpenPath(ctx, "")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	setup, err := db.Connect(ctx)
	if err != nil {
		panic(err)
	}
	if err := run(setup, "SET memory_limit='128MB'"); err != nil {
		panic(err)
	}
	if err := run(setup, `CREATE TABLE r AS
	  SELECT b.bucket, 'l' || lpad(l.i::VARCHAR, 3, '0') AS lang,
	         (1 + 200000 / (l.i * l.i))::BIGINT AS posts
	  FROM (SELECT unnest(generate_series(TIMESTAMPTZ '2026-01-01', TIMESTAMPTZ '2027-01-01', INTERVAL '1 day')) AS bucket) b,
	       (SELECT unnest(generate_series(1, 170)) AS i) l`); err != nil {
		panic(err)
	}

	conns := []adbc.Connection{setup}
	for i := 1; i < n; i++ {
		c, err := db.Connect(ctx)
		if err != nil {
			panic(err)
		}
		conns = append(conns, c)
	}
	fmt.Printf("%d sessions idle: %.0f MiB\n", n, rssMiB())

	var wg sync.WaitGroup
	for _, c := range conns {
		wg.Add(1)
		go func(c adbc.Connection) {
			defer wg.Done()
			if err := run(c, fold); err != nil {
				fmt.Println("  query failed:", err)
			}
		}(c)
	}
	wg.Wait()
	fmt.Printf("%d concurrent folds: %.0f MiB peak\n", n, rssMiB())
}
```

- [ ] **Step 3: Run it in the release container**

The image carries the matching `libduckdb.so` and sets `SQLFLOW_DUCKDB_LIB`, so the probe must build and run inside it rather than on the host.

```bash
make sqlflow-image SQLFLOW_IMAGE=sqlflow-poolmem:local
docker run --rm -v "$PWD:/src" -w /src --entrypoint /bin/sh sqlflow-poolmem:local -c '
  go run ./cmd/poolmem 1; go run ./cmd/poolmem 4; go run ./cmd/poolmem 8'
```

Expected: three pairs of lines. If `go` is absent from the image, build a static probe on the host for linux/amd64 and mount the binary instead:

```bash
CGO_ENABLED=1 GOOS=linux go build -o /tmp/poolmem ./cmd/poolmem
docker run --rm -v /tmp/poolmem:/poolmem --entrypoint /poolmem sqlflow-poolmem:local 4
```

- [ ] **Step 4: Decide the default**

Record all six numbers. The default stands at 4 if four concurrent folds stay under 160 MiB, which leaves room for the Go runtime inside 256 MB. If four exceed that, set the default to the largest N that fits and say so in the spec's memory table and in the PR body.

- [ ] **Step 5: Delete the probe and commit the finding**

```bash
rm -rf cmd/poolmem
git status --short   # expect nothing, or only the spec if the default changed
```

If the spec changed:

```bash
git add docs/superpowers/specs/2026-09-16-serve-pool-design.md
git commit -m "spec: per-session memory measured in the release container"
```

If it did not, this task produces no commit. Carry the numbers into the PR body either way.

---

### Task 2: Config for the pool and metrics

**Files:**
- Modify: `internal/config/serve.go` (`Serve` struct, new types, `checkLimits`'s neighbours in `Check`)
- Test: `internal/config/serve_test.go`
- Regenerated: `internal/validate/schemas/serve.json`, `internal/cli/testdata/serve_example.golden`

**Interfaces:**
- Produces:
  - `config.ServePool{Size int}` with yaml key `size`, on `Serve.Pool *ServePool` at yaml key `pool`
  - `config.ServeMetrics{Enabled bool}` with yaml key `enabled`, on `Serve.Metrics *ServeMetrics` at yaml key `metrics`
  - `func (s Serve) PoolSize() int`, returning `DefaultServePoolSize` when unset
  - `const DefaultServePoolSize = 4`, `const MaxServePoolSize = 64`
  - `func (s Serve) MetricsEnabled() bool`

- [ ] **Step 1: Write the failing rule tests**

In `internal/config/serve_test.go`, add to the table in `TestCliServe_CheckReportsEachRuleAtItsPath`, after the `min above max` case:

```go
		{"negative pool size", "  limits:", "  pool: {size: -1}\n  limits:",
			errs.CodeConfigInvalid, "serve.pool.size", "must not be negative"},
		{"pool size past the ceiling", "  limits:", "  pool: {size: 65}\n  limits:",
			errs.CodeConfigInvalid, "serve.pool.size", "65 sessions is more than the 64 this version allows"},
```

And add a resolution test after `TestCliServe_LimitsResolveDatasetThenTopLevelThenDefault`:

```go
// The pool defaults rather than failing closed: a config written before the
// pool existed gets concurrency, not one session.
func TestCliServe_PoolSizeDefaults(t *testing.T) {
	coverage.Covers(t, "cli.serve")

	conf := parseServe(t, validServe)
	assert.Equal(t, DefaultServePoolSize, conf.Serve.PoolSize())
	assert.False(t, conf.Serve.MetricsEnabled())

	sized := parseServe(t, strings.Replace(validServe, "  limits:", "  pool: {size: 8}\n  metrics: {enabled: true}\n  limits:", 1))
	assert.Equal(t, 8, sized.Serve.PoolSize())
	assert.True(t, sized.Serve.MetricsEnabled())

	// 0 is unset, not "no sessions".
	zero := parseServe(t, strings.Replace(validServe, "  limits:", "  pool: {size: 0}\n  limits:", 1))
	assert.Equal(t, DefaultServePoolSize, zero.Serve.PoolSize())
}
```

- [ ] **Step 2: Run them to see them fail**

Run: `go test -short ./internal/config/ -run 'TestCliServe_(CheckReportsEachRuleAtItsPath|PoolSizeDefaults)'`
Expected: FAIL to compile, `undefined: DefaultServePoolSize`, and the YAML rejects `field pool not found`.

- [ ] **Step 3: Add the types, the accessors and the rules**

In `internal/config/serve.go`, add to the defaults block:

```go
	// DefaultServePoolSize is the sessions a server holds when the config
	// names no number. Four fits a 256 MB box: idle sessions cost nothing
	// measurable, and a concurrent query costs about 15 MiB.
	DefaultServePoolSize = 4
	// MaxServePoolSize is the most this version accepts. A larger pool on a
	// small box fails queries with out-of-memory rather than queueing them,
	// because DuckDB's memory_limit is one budget shared by every session.
	MaxServePoolSize = 64
```

Add to `Serve`, after `Limits`:

```go
	// How many requests the server runs at once. Omit for the default.
	Pool *ServePool `yaml:"pool,omitempty"`
	// Whether to serve Prometheus metrics at /metrics.
	Metrics *ServeMetrics `yaml:"metrics,omitempty"`
```

Add the types after `ServeRateLimit`:

```go
// ServePool sizes the sessions a server answers requests on.
type ServePool struct {
	// Sessions the server holds. Each is one backend session, and one
	// request uses one at a time, so this is the requests that run at once.
	// 0 means the default, 4.
	Size int `yaml:"size,omitempty"`
}

// ServeMetrics turns on the Prometheus endpoint.
type ServeMetrics struct {
	// Serve GET /metrics on the same listener as the datasets, without a
	// token. Off by default: that listener is public, and the metric labels
	// name every dataset and grain.
	Enabled bool `yaml:"enabled,omitempty"`
}
```

Add the accessors beside `MaxRows`:

```go
// PoolSize is the sessions to hold, defaulted.
func (s Serve) PoolSize() int {
	if s.Pool != nil && s.Pool.Size > 0 {
		return s.Pool.Size
	}
	return DefaultServePoolSize
}

// MetricsEnabled reports whether to serve /metrics.
func (s Serve) MetricsEnabled() bool {
	return s.Metrics != nil && s.Metrics.Enabled
}
```

In `Check`, directly after the `checkLimits(s.Limits, ...)` call:

```go
	if s.Pool != nil {
		switch {
		case s.Pool.Size < 0:
			add(errs.CodeConfigInvalid, []string{"serve", "pool", "size"},
				"pool.size is %d; it must not be negative, and 0 means the default", s.Pool.Size)
		case s.Pool.Size > MaxServePoolSize:
			add(errs.CodeConfigInvalid, []string{"serve", "pool", "size"},
				"pool.size %d sessions is more than the %d this version allows; DuckDB's memory limit is one budget shared by every session, so a pool past the box fails queries rather than queueing them",
				s.Pool.Size, MaxServePoolSize)
		}
	}
```

- [ ] **Step 4: Run the tests and regenerate**

```bash
go test -short ./internal/config/
make schema
git diff --stat
go test -short ./internal/schema/ ./internal/cli/ ./internal/validate/
```

Expected: config passes; `git diff --stat` lists `serve.json` and `serve_example.golden`; the rest passes.

- [ ] **Step 5: Commit**

```bash
git add internal/config/serve.go internal/config/serve_test.go \
  internal/validate/schemas/serve.json internal/cli/testdata/serve_example.golden
git commit -m "serve: config for the session pool and the metrics endpoint"
```

---

### Task 3: The executor interface, the pool, and the DuckDB implementation

The big one. It moves the request path off `adbc.Connection` and onto a pool, with no behaviour change at `size: 1`.

**Files:**
- Create: `internal/serve/executor.go`, `internal/serve/duckdb.go`
- Modify: `internal/serve/query.go` (statement and executor move out; `readRows` stays in encode.go), `internal/serve/server.go`, `internal/serve/http.go`, `internal/cli/serve/serve.go`
- Test: `internal/serve/executor_test.go`, and the existing `internal/serve/http_test.go` helper

**Interfaces:**
- Consumes: `config.ServeParam`, `config.Serve.PoolSize()`, `duckdb.DB`, `core.InitCommands`, `readRows`, `result`.
- Produces:
  - `serve.Executor`, `serve.Session`, `serve.Statement`, `serve.StatementSpec`, `serve.Stats`, `serve.ErrClosed` — exactly as the spec's "The interfaces" section writes them
  - `func NewDuckDBExecutor(ctx context.Context, db *duckdb.DB, size int, init func(context.Context, adbc.Connection) error) (Executor, error)`
  - `func New(ctx context.Context, conf *config.ServeConf, ex Executor, opts ...Option) (*Server, error)` — `New`'s third parameter changes from `adbc.Connection` to `Executor`
  - unexported: `newPool(sessions []backendSession, onWait func(time.Duration)) *pool`, `query(ctx, ex, st, values, maxRows) (result, time.Duration, error)`

- [ ] **Step 1: Write the failing pool tests**

Create `internal/serve/executor_test.go`. These test the pool through a fake backend, so they need no DuckDB and run fast.

```go
package serve

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

// fakeSession is a backend that sleeps instead of querying, so pool
// behaviour is testable without DuckDB.
type fakeSession struct {
	delay  time.Duration
	closed bool
	runs   int
}

func (f *fakeSession) run(ctx context.Context, st Statement, values map[string]any) (array.RecordReader, error) {
	f.runs++
	select {
	case <-time.After(f.delay):
		return nil, errors.New("fake session returns no rows")
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (f *fakeSession) close() error { f.closed = true; return nil }

func newFakePool(t *testing.T, n int, delay time.Duration) (*pool, []*fakeSession) {
	t.Helper()
	fakes := make([]*fakeSession, n)
	backends := make([]backendSession, n)
	for i := range fakes {
		fakes[i] = &fakeSession{delay: delay}
		backends[i] = fakes[i]
	}
	return newPool(backends, func(time.Duration) {}), fakes
}

// The whole point: N sessions run N queries in about the time of one.
func TestCliServe_PoolRunsQueriesConcurrently(t *testing.T) {
	coverage.Covers(t, "cli.serve")

	const n = 4
	const delay = 200 * time.Millisecond
	p, _ := newFakePool(t, n, delay)
	defer p.Close()

	start := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s, err := p.Acquire(context.Background())
			assert.NoError(t, err)
			defer s.Release()
			_, _ = s.Run(context.Background(), nil, nil)
		}()
	}
	wg.Wait()

	// Serial would be 4 x 200ms. Two delays of headroom for a loaded CI box.
	assert.That(t, time.Since(start) < 2*delay)
}

// One session is the old behaviour, and the control for the test above.
func TestCliServe_APoolOfOneSerializes(t *testing.T) {
	coverage.Covers(t, "cli.serve")

	const delay = 100 * time.Millisecond
	p, _ := newFakePool(t, 1, delay)
	defer p.Close()

	start := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s, err := p.Acquire(context.Background())
			assert.NoError(t, err)
			defer s.Release()
			_, _ = s.Run(context.Background(), nil, nil)
		}()
	}
	wg.Wait()

	assert.That(t, time.Since(start) >= 3*delay)
}

// A caller that gives up must free its place, or the pool shrinks under load
// until it deadlocks.
func TestCliServe_AcquireReturnsWhenTheRequestGivesUp(t *testing.T) {
	coverage.Covers(t, "cli.serve")

	p, _ := newFakePool(t, 1, 0)
	defer p.Close()

	held, err := p.Acquire(context.Background())
	assert.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err = p.Acquire(ctx)
	assert.That(t, errors.Is(err, context.DeadlineExceeded))
	assert.Equal(t, 1, p.Stats().InUse)

	held.Release()
	next, err := p.Acquire(context.Background())
	assert.NoError(t, err)
	next.Release()
}

// A session closed while a query runs on it takes the process down, and
// nothing outside the reading goroutine can stop that query, so Close waits.
func TestCliServe_CloseWaitsForBorrowedSessions(t *testing.T) {
	coverage.Covers(t, "cli.serve")

	p, fakes := newFakePool(t, 2, 0)
	s, err := p.Acquire(context.Background())
	assert.NoError(t, err)

	closed := make(chan struct{})
	go func() { p.Close(); close(closed) }()

	select {
	case <-closed:
		t.Fatal("Close returned while a session was borrowed")
	case <-time.After(100 * time.Millisecond):
	}

	s.Release()
	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not return after the session came back")
	}
	for i, f := range fakes {
		if !f.closed {
			t.Fatalf("session %d was not closed", i)
		}
	}

	_, err = p.Acquire(context.Background())
	assert.That(t, errors.Is(err, ErrClosed))
}

// A deferred Release beside an early return can run twice.
func TestCliServe_ReleaseTwiceIsSafe(t *testing.T) {
	coverage.Covers(t, "cli.serve")

	p, _ := newFakePool(t, 1, 0)
	defer p.Close()

	s, err := p.Acquire(context.Background())
	assert.NoError(t, err)
	s.Release()
	s.Release()

	assert.Equal(t, 0, p.Stats().InUse)
	next, err := p.Acquire(context.Background())
	assert.NoError(t, err)
	next.Release()
}

func TestCliServe_StatsReportSizeAndUse(t *testing.T) {
	coverage.Covers(t, "cli.serve")

	p, _ := newFakePool(t, 3, 0)
	defer p.Close()

	assert.Equal(t, Stats{Size: 3, InUse: 0}, p.Stats())
	a, _ := p.Acquire(context.Background())
	b, _ := p.Acquire(context.Background())
	assert.Equal(t, Stats{Size: 3, InUse: 2}, p.Stats())
	a.Release()
	b.Release()
	assert.Equal(t, Stats{Size: 3, InUse: 0}, p.Stats())
}
```

- [ ] **Step 2: Run them to see them fail**

Run: `go test -short ./internal/serve/ -run 'TestCliServe_(Pool|Acquire|Close|Release|Stats)'`
Expected: FAIL to compile, `undefined: newPool`, `undefined: backendSession`, `undefined: ErrClosed`.

- [ ] **Step 3: Write the interfaces and the pool**

Create `internal/serve/executor.go`. Copy the interface declarations and their doc comments verbatim from the spec's "The interfaces" section, then add the pool below them.

```go
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

// ... Executor, Session, Statement, StatementSpec, Stats: verbatim from the spec ...

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
```

- [ ] **Step 4: Write the query helper**

Still in `executor.go`. This replaces the old `executor.run` and carries the rule that matters most.

```go
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
		// Released whether or not the rows are drained, which is what tells
		// the driver to stop.
		defer rdr.Release()
		res, err := readRows(ctx, rdr, maxRows)
		done <- outcome{res: res, err: err}
	}()

	select {
	case o := <-done:
		return o.res, waited, o.err
	case <-ctx.Done():
		return result{}, waited, ctx.Err()
	}
}
```

- [ ] **Step 5: Run the pool tests**

Run: `go test -short -race ./internal/serve/ -run 'TestCliServe_(Pool|APoolOfOne|Acquire|Close|Release|Stats)'`
Expected: PASS. `APoolOfOneSerializes` and `PoolRunsQueriesConcurrently` are the pair that prove the pool does something; if both pass at any size, the concurrency assertion is not biting.

- [ ] **Step 6: Move the DuckDB code behind the interface**

Create `internal/serve/duckdb.go`. Move `statement`, `prepare`, `record` and the query body out of `query.go` into it, renaming `statement` to `duckdbStatement` and its `where()` to `Where()`. Delete `executor`, `outcome` and `errClosed` from `query.go`; `query.go` keeps only what is left, and if that is nothing, delete the file.

```go
package serve

import (
	"context"
	"fmt"
	"strings"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/duckdb"
	"github.com/turbolytics/sql-flow/internal/errs"
	"github.com/turbolytics/sql-flow/internal/sqlparams"
)

// duckdbExecutor runs datasets on DuckDB over ADBC. It is the only type in
// this package that names either.
type duckdbExecutor struct {
	*pool
	db *duckdb.DB
	// setup is the connection the config's commands ran on, kept because
	// Prepare needs a connection and every other one is in the pool. It is
	// idle afterwards, which costs nothing measurable.
	setup adbc.Connection
}

// NewDuckDBExecutor opens size sessions on db and returns an Executor over
// them.
//
// init runs once, on a connection of its own, before any session is used. The
// caller passes the config's commands: ATTACH is database-wide and attaching
// the same alias twice is an error, so they must not run per session.
//
// Every session is then pinned to UTC. That is serve's own contract rather
// than the config's: SET TimeZone is session-scoped, so a session that missed
// it evaluates date_trunc and naive casts in the host's zone and returns
// wrong buckets from a correct config.
func NewDuckDBExecutor(ctx context.Context, db *duckdb.DB, size int,
	init func(context.Context, adbc.Connection) error) (Executor, error) {
	setup, err := db.Connect(ctx)
	if err != nil {
		return nil, err
	}
	if init != nil {
		if err := init(ctx, setup); err != nil {
			_ = setup.Close()
			return nil, err
		}
	}

	backends := make([]backendSession, 0, size)
	for i := 0; i < size; i++ {
		conn, err := db.Connect(ctx)
		if err != nil {
			_ = setup.Close()
			return nil, err
		}
		if err := execOn(ctx, conn, "SET TimeZone='UTC'"); err != nil {
			_ = conn.Close()
			_ = setup.Close()
			return nil, fmt.Errorf("pinning the session timezone: %w", err)
		}
		backends = append(backends, &duckdbSession{conn: conn})
	}

	return &duckdbExecutor{pool: newPool(backends, func(time.Duration) {}), db: db, setup: setup}, nil
}

// execOn runs one statement for its effect.
func execOn(ctx context.Context, conn adbc.Connection, sql string) error {
	stmt, err := conn.NewStatement()
	if err != nil {
		return err
	}
	defer stmt.Close()
	if err := stmt.SetSqlQuery(sql); err != nil {
		return err
	}
	_, err = stmt.ExecuteUpdate(ctx)
	return err
}

func (e *duckdbExecutor) Close() {
	e.pool.Close()
	_ = e.setup.Close()
}

// duckdbSession is one ADBC connection.
type duckdbSession struct{ conn adbc.Connection }

func (d *duckdbSession) close() error { return d.conn.Close() }

func (d *duckdbSession) run(ctx context.Context, st Statement, values map[string]any) (array.RecordReader, error) {
	s, ok := st.(*duckdbStatement)
	if !ok {
		return nil, fmt.Errorf("serve: statement %T did not come from this executor", st)
	}

	stmt, err := d.conn.NewStatement()
	if err != nil {
		return nil, err
	}
	// The statement outlives this call through the reader, so it closes when
	// the reader is released rather than here.
	if err := stmt.SetSqlQuery(s.rewritten); err != nil {
		_ = stmt.Close()
		return nil, err
	}
	if s.schema.NumFields() > 0 {
		rec := s.record(values)
		err := stmt.Bind(ctx, rec)
		rec.Release()
		if err != nil {
			_ = stmt.Close()
			return nil, err
		}
	}
	rdr, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		_ = stmt.Close()
		return nil, err
	}
	return &closingReader{RecordReader: rdr, stmt: stmt}, nil
}

// closingReader closes the statement when the reader is released, so a
// caller that only knows about the reader leaks neither.
type closingReader struct {
	array.RecordReader
	stmt adbc.Statement
}

func (c *closingReader) Release() {
	c.RecordReader.Release()
	_ = c.stmt.Close()
}
```

`Prepare` is the old `prepare` with its signature changed:

```go
// Prepare checks one statement against DuckDB at startup. DuckDB binds a
// statement when its SQL is set, so a syntax error, a missing table and a
// missing column all fail here rather than on the first request.
func (e *duckdbExecutor) Prepare(ctx context.Context, spec StatementSpec) (Statement, error) {
	// ... the body of the old prepare(), building a *duckdbStatement and
	// using e.setup as the connection ...
}
```

Keep the old `prepare`'s parameter-count check and its message verbatim; it catches a scanner that miscounts placeholders and binds values onto the wrong ones.

- [ ] **Step 7: Thread the Executor through the server and the CLI**

In `internal/serve/server.go`, change `New`'s third parameter to `ex Executor`, store it as `s.exec`, replace the `prepare(ctx, conn, ...)` call with `s.exec.Prepare(ctx, StatementSpec{...})`, and change `Server.Close` to call `s.exec.Close()`. The `dataset` struct's `single` and `grains` become `Statement` rather than `*statement`.

In `internal/serve/http.go`, replace the `s.exec.run(...)` call in `queryDataset` with:

```go
	res, _, err := query(r.Context(), s.exec, st, values, ds.maxRows)
```

and leave the error switch exactly as it is: an exhausted pool surfaces as `context.DeadlineExceeded`, which is already `504 query_timeout`.

In `internal/cli/serve/serve.go`, replace the single `db.Connect` and `core.InitCommands` block with:

```go
	ex, err := api.NewDuckDBExecutor(ctx, db, conf.Serve.PoolSize(),
		func(ctx context.Context, conn adbc.Connection) error {
			// Uncoded, as in run: an ATTACH that fails because the database
			// is not up yet exits 1, which a supervisor retries. Redacted
			// because a failed ATTACH prints the connection string.
			if err := core.InitCommands(conn, &config.Conf{Commands: conf.Commands}); err != nil {
				return errors.New("failed to initialize commands: " + api.Redact(err.Error()))
			}
			return nil
		})
	if err != nil {
		return err
	}

	srv, err := api.New(ctx, conf, ex, api.WithLogger(l))
```

`srv.Close()` now closes the executor, so drop the separate `conn.Close()` defer and keep `db.Close()`.

In `internal/serve/http_test.go`, `newTestServer` builds an executor instead of a connection:

```go
	db, err := duckdb.OpenPath(context.Background(), "")
	assert.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	ex, err := NewDuckDBExecutor(context.Background(), db, 1, func(ctx context.Context, conn adbc.Connection) error {
		for _, sql := range []string{"SET TimeZone='UTC'", createPostsTable} {
			if err := execOn(ctx, conn, sql); err != nil {
				return err
			}
		}
		return nil
	})
	assert.NoError(t, err)
```

where `createPostsTable` is the `CREATE TABLE posts AS ...` string already in the helper. Size 1 keeps every existing assertion about ordering and timing true.

- [ ] **Step 8: Run the whole package**

```bash
go build ./... && go vet ./internal/serve/ ./internal/cli/serve/
go test -short -race ./internal/serve/ ./internal/cli/serve/ ./internal/cli/
```

Expected: PASS, with no change to any existing test's assertions. If `TestCliServe_ASlowQueryIsA504AndTurnsHealthRed` fails, the health path still uses the old executor; Task 7 rewrites it, so leave it on the old shape until then by having `healthz` call `query(...)` with a one-row statement.

- [ ] **Step 9: Commit**

```bash
git add internal/serve/ internal/cli/serve/serve.go
git commit -m "serve: run requests on a pool of sessions behind an executor interface"
```

---

### Task 4: Every session is UTC

Task 3 writes the pin. This task proves it can fail, which is the only way to know it is load-bearing.

**Files:**
- Test: `internal/serve/duckdb_test.go` (new)

**Interfaces:**
- Consumes: `NewDuckDBExecutor`, `New`, `Server.Handler`, `execOn`.
- Produces: nothing later tasks use.

- [ ] **Step 1: Write the failing test**

Create `internal/serve/duckdb_test.go`:

```go
package serve

import (
	"context"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/turbolytics/sql-flow/internal/duckdb"
	"github.com/zeebo/assert"
)

// SET TimeZone is session-scoped, so a session the executor did not pin
// inherits the host's zone and evaluates date_trunc and naive casts in it.
// The test runs under a non-UTC TZ, because under UTC a missing pin looks
// correct.
func TestCliServe_EverySessionIsUTC(t *testing.T) {
	coverage.Covers(t, "cli.serve")
	t.Setenv("TZ", "America/New_York")

	ctx := context.Background()
	db, err := duckdb.OpenPath(ctx, "")
	assert.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	const size = 4
	ex, err := NewDuckDBExecutor(ctx, db, size, nil)
	assert.NoError(t, err)
	t.Cleanup(ex.Close)

	// Hold every session at once, so each is checked rather than the same
	// one four times.
	var held []Session
	for i := 0; i < size; i++ {
		s, err := ex.Acquire(ctx)
		assert.NoError(t, err)
		held = append(held, s)
	}
	for i, s := range held {
		zone, err := scalarOn(ctx, s, ex, "SELECT current_setting('TimeZone')")
		assert.NoError(t, err)
		if zone != "UTC" {
			t.Fatalf("session %d is in %s, not UTC", i, zone)
		}
	}
	for _, s := range held {
		s.Release()
	}
}
```

Add the helper below it, which prepares and runs one statement on a held session:

```go
// scalarOn runs sql on one held session and returns its first value.
func scalarOn(ctx context.Context, s Session, ex Executor, sql string) (string, error) {
	st, err := ex.Prepare(ctx, StatementSpec{Dataset: "probe", SQL: sql})
	if err != nil {
		return "", err
	}
	rdr, err := s.Run(ctx, st, nil)
	if err != nil {
		return "", err
	}
	defer rdr.Release()
	res, err := readRows(rdr, 1)
	if err != nil {
		return "", err
	}
	// rows is a JSON array of one object with one column.
	return string(res.Rows), nil
}
```

The returned string is JSON, so assert with `strings.Contains(zone, "UTC")` rather than equality, and import `strings`.

- [ ] **Step 2: Prove the test can fail**

Temporarily delete the `SET TimeZone='UTC'` line from `NewDuckDBExecutor`, run the test, and confirm it fails naming `America/New_York`. Restore the line.

Run: `go test -short ./internal/serve/ -run TestCliServe_EverySessionIsUTC -v`
Expected without the pin: FAIL, `session 0 is in America/New_York, not UTC`. With it: PASS.

- [ ] **Step 3: Commit**

```bash
git add internal/serve/duckdb_test.go
git commit -m "serve: prove every pooled session is pinned to UTC"
```

---

### Task 5: `queued_ms`, and stopping work nobody is waiting for

**Files:**
- Modify: `internal/serve/http.go` (`rowsResponse`, `queryDataset`), `internal/serve/encode.go` (`readRows`)
- Test: `internal/serve/http_test.go`, `internal/serve/encode_test.go`
- Modify: `README.md` (the response example, and the attach guidance)

**Interfaces:**
- Consumes: `query(...)`'s second return value.
- Produces: `rowsResponse.QueuedMS int64` at JSON key `queued_ms`; `readRows(ctx context.Context, rdr array.RecordReader, maxRows int) (result, error)` — the signature gains a leading context.

- [ ] **Step 1: Write the failing test**

Add to `internal/serve/http_test.go`:

```go
// elapsed_ms used to include the wait for a connection, so a 13 ms query on
// a busy server reported a second and sent the reader looking for a slow
// query that did not exist.
func TestCliServe_QueuedMsSeparatesWaitFromWork(t *testing.T) {
	coverage.Covers(t, "cli.serve")
	ts := newTestServer(t, testServe)

	// Idle: nothing waited.
	r := ts.get(t, "/v1/datasets/status")
	assert.Equal(t, http.StatusOK, r.status)
	assert.Equal(t, float64(0), r.body["queued_ms"])

	// Hold the only session, then time a request that has to wait for it.
	sess, err := ts.srv.exec.Acquire(context.Background())
	assert.NoError(t, err)
	done := make(chan response, 1)
	go func() { done <- ts.get(t, "/v1/datasets/status") }()
	time.Sleep(300 * time.Millisecond)
	sess.Release()

	queued := <-done
	assert.Equal(t, http.StatusOK, queued.status)
	assert.That(t, queued.body["queued_ms"].(float64) >= 250)
	// The query itself is unchanged by the wait.
	assert.That(t, queued.body["elapsed_ms"].(float64) < 250)
}
```

- [ ] **Step 2: Run it to see it fail**

Run: `go test -short ./internal/serve/ -run TestCliServe_QueuedMsSeparatesWaitFromWork`
Expected: FAIL, `queued_ms` is nil because the field does not exist.

- [ ] **Step 3: Add the field**

In `internal/serve/http.go`, add to `rowsResponse` after `Truncated`:

```go
	// QueuedMS is how long the request waited for a session. ElapsedMS is the
	// query alone, so a busy server and a slow query are distinguishable.
	QueuedMS int64 `json:"queued_ms"`
```

In `queryDataset`, take the wait from `query` and time only the work:

```go
	start := time.Now()
	res, waited, err := query(r.Context(), s.exec, st, values, ds.maxRows)
```

and fill both fields:

```go
		QueuedMS:  waited.Milliseconds(),
		ElapsedMS: time.Since(start).Sub(waited).Milliseconds(),
```

- [ ] **Step 4: Run the tests**

Run: `go test -short -race ./internal/serve/`
Expected: PASS.

- [ ] **Step 5: Write the failing test for abandoned work**

A session held by a query nobody waits for is a session the next request cannot have, so the reader must stop when its caller does. Add to `internal/serve/encode_test.go`:

```go
// A caller that gave up should not pay for the rest of its own result, and
// more importantly should not hold a session while it drains. Releasing a
// reader without draining it is ADBC's documented equivalent of cancel, so
// stopping early is what actually stops the query.
func TestCliServe_ReadRowsStopsWhenTheRequestIsGone(t *testing.T) {
	coverage.Covers(t, "cli.serve")

	conn := newConn(t)
	execSQL(t, conn, "SET TimeZone='UTC'")

	// More rows than one batch, so there is a boundary to stop at.
	stmt, err := conn.NewStatement()
	assert.NoError(t, err)
	defer stmt.Close()
	assert.NoError(t, stmt.SetSqlQuery("SELECT i FROM range(500000) t(i)"))
	rdr, _, err := stmt.ExecuteQuery(context.Background())
	assert.NoError(t, err)
	defer rdr.Release()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	res, err := readRows(ctx, rdr, 1000000)
	assert.That(t, errors.Is(err, context.Canceled))
	// It stopped rather than reading half a million rows.
	assert.That(t, res.RowCount < 1000000)
}
```

- [ ] **Step 6: Run it to see it fail**

Run: `go test -short ./internal/serve/ -run TestCliServe_ReadRowsStopsWhenTheRequestIsGone`
Expected: FAIL to compile, `too many arguments in call to readRows`.

- [ ] **Step 7: Give readRows the context**

In `internal/serve/encode.go`, add `ctx context.Context` as the first parameter, extend the doc comment, and check between batches:

```go
// readRows encodes at most maxRows rows from rdr.
//
// ... existing paragraphs ...
//
// It also stops at the first batch boundary after ctx ends. ADBC's Go API
// has no Cancel, and documents releasing a reader without consuming it as
// equivalent to AdbcStatementCancel, so stopping here and releasing is the
// only way a caller that gave up stops the work. A session held by a query
// nobody is waiting for is a session the next request cannot have.
func readRows(ctx context.Context, rdr array.RecordReader, maxRows int) (result, error) {
```

and inside the outer loop, as its first statement:

```go
	for !res.Truncated && rdr.Next() {
		if err := ctx.Err(); err != nil {
			return result{}, err
		}
```

Update the two other call sites: `query` in `executor.go` (done in Task 3) and `healthz` (Task 7 rewrites it; until then pass `r.Context()`). Every existing `readRows` call in `encode_test.go` gains `context.Background()`.

- [ ] **Step 8: Run the tests**

Run: `go test -short -race ./internal/serve/`
Expected: PASS. Removing the `ctx.Err()` check makes `ReadRowsStopsWhenTheRequestIsGone` fail, which is the point of it.

- [ ] **Step 9: Document both**

In `README.md`, in the `sqlflow serve` section, add `"queued_ms":0,` to the sample response beside `"elapsed_ms"`, and add after the paragraph that begins "A zoned timestamp is UTC":

```markdown
`elapsed_ms` is the query. `queued_ms` is how long the request waited for a
session, which is what rises when the pool is too small for the load.
```

Then replace the "**A timeout does not stop the query.**" bullet with what is now true:

```markdown
- **A timeout stops reading, not always the query.** At its deadline the
  caller gets `504`, and the reader stops at the next batch and is released,
  which is ADBC's equivalent of cancelling. A single operator that runs long
  before yielding a batch still runs to the end, holding its session. Bound
  the part that is usually slow in the backend: a libpq connection string
  takes `options='-c statement_timeout=30000'`, so an attached Postgres
  enforces its own ceiling.
```

Add the same `options=` note to the attach example in that section.

- [ ] **Step 10: Commit**

```bash
git add internal/serve/http.go internal/serve/encode.go internal/serve/http_test.go \
  internal/serve/encode_test.go README.md
git commit -m "serve: queued_ms, and stop reading for a caller that gave up"
```

---

### Task 6: Metrics

**Files:**
- Create: `internal/serve/metrics.go`, `internal/serve/metrics_test.go`
- Modify: `internal/serve/server.go` (build the instruments, hold them), `internal/serve/http.go` (record, register the route), `internal/serve/duckdb.go` (`NewDuckDBExecutor` takes the wait hook)

**Interfaces:**
- Consumes: `Stats`, `pool.onWait`, `config.Serve.MetricsEnabled()`.
- Produces:
  - `func newMetrics(reg *prom.Registry) (*metrics, error)`
  - `type metrics` with `requests`, `requestDuration`, `queryDuration`, `sessionWait`, and a registered callback for `sessionsInUse` and `sessionsTotal`
  - `func (m *metrics) observeWait(d time.Duration)`
  - `serve.WithMetrics(reg *prom.Registry) Option`
  - `NewDuckDBExecutor(ctx, db, size, init, onWait func(time.Duration))` — the signature gains a fifth parameter

- [ ] **Step 1: Write the failing test**

Create `internal/serve/metrics_test.go`:

```go
package serve

import (
	"net/http"
	"strings"
	"testing"

	prom "github.com/prometheus/client_golang/prometheus"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

// The endpoint is on the public listener, and the labels name every dataset,
// so it exists only when the config asks for it.
func TestCliServe_MetricsAreOffUnlessEnabled(t *testing.T) {
	coverage.Covers(t, "cli.serve")

	ts := newTestServer(t, testServe)
	r := ts.do(t, http.MethodGet, "/metrics", nil)
	assert.Equal(t, http.StatusNotFound, r.status)
}

// Every instrument the spec names carries a sample after one request, so a
// dashboard built against this list is not built against a blank.
func TestCliServe_MetricsCarryEveryInstrument(t *testing.T) {
	coverage.Covers(t, "cli.serve")

	reg := prom.NewRegistry()
	ts := newTestServerWith(t, strings.Replace(testServe, "  limits:", "  metrics: {enabled: true}\n  limits:", 1), reg)

	assert.Equal(t, http.StatusOK, ts.get(t, "/v1/datasets/status").status)

	r := ts.do(t, http.MethodGet, "/metrics", nil)
	assert.Equal(t, http.StatusOK, r.status)
	for _, name := range []string{
		"sqlflow_serve_requests_total",
		"sqlflow_serve_request_duration_seconds",
		"sqlflow_serve_query_duration_seconds",
		"sqlflow_serve_session_wait_seconds",
		"sqlflow_serve_sessions_in_use",
		"sqlflow_serve_sessions_total",
	} {
		if !strings.Contains(r.raw, name) {
			t.Fatalf("/metrics does not carry %s:\n%s", name, r.raw)
		}
	}
	// The gauges report the pool, not a constant.
	assert.That(t, strings.Contains(r.raw, "sqlflow_serve_sessions_total 1"))
	// A dataset label, so a dashboard can break the rate down.
	assert.That(t, strings.Contains(r.raw, `dataset="status"`))
}
```

`newTestServerWith(t, text, reg)` is `newTestServer` with a registry passed to `New` through `WithMetrics`; refactor `newTestServer` to call it with a nil registry.

- [ ] **Step 2: Run it to see it fail**

Run: `go test -short ./internal/serve/ -run TestCliServe_Metrics`
Expected: FAIL to compile, `undefined: newTestServerWith`, `undefined: WithMetrics`.

- [ ] **Step 3: Write the instruments**

Create `internal/serve/metrics.go`. Follow `internal/cli/run/metrics.go`: OpenTelemetry instruments exported through a `prom.Registry`, so serve's metrics reach the same scrape format as run's.

```go
package serve

import (
	"context"
	"time"

	prom "github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

// buckets span a 1 ms query and a request that hit the 10 s timeout, so both
// land in a bucket rather than the overflow.
var buckets = []float64{0.001, 0.002, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 4, 8, 16}

// metrics is the six instruments the spec names, and nothing else.
type metrics struct {
	requests        metric.Int64Counter
	requestDuration metric.Float64Histogram
	queryDuration   metric.Float64Histogram
	sessionWait     metric.Float64Histogram
}

// newMetrics builds the instruments against reg and registers the gauges'
// callback, which reads stats each time the endpoint is scraped.
func newMetrics(reg *prom.Registry, stats func() Stats) (*metrics, error) {
	// ... prometheus.New(prometheus.WithRegisterer(reg)), a MeterProvider with
	// the bucket view, a Meter named "sqlflow.serve", the four instruments,
	// and an Int64ObservableGauge pair whose callback reads stats() ...
}

func (m *metrics) observeWait(d time.Duration) {
	if m == nil {
		return
	}
	m.sessionWait.Record(context.Background(), d.Seconds())
}

// observeRequest records one finished request.
func (m *metrics) observeRequest(dataset, grain, code string, queued, query, total time.Duration) {
	if m == nil {
		return
	}
	attrs := metric.WithAttributes(
		attribute.String("dataset", dataset),
		attribute.String("grain", grain),
		attribute.String("code", code),
	)
	m.requests.Add(context.Background(), 1, attrs)
	m.requestDuration.Record(context.Background(), total.Seconds(), attrs)
	m.queryDuration.Record(context.Background(), query.Seconds(), attrs)
}
```

Every method tolerates a nil receiver, so the request path records unconditionally and metrics being off costs one nil check.

- [ ] **Step 4: Wire it up**

Add `WithMetrics(reg *prom.Registry) Option` to `server.go`, storing the registry. In `New`, when the registry is non-nil and `conf.Serve.MetricsEnabled()`, build the metrics with `stats` reading `ex.Stats()`. In `Handler`, register `/metrics` only when the metrics exist:

```go
	if s.metrics != nil {
		mux.Handle("/metrics", promhttp.HandlerFor(s.registry, promhttp.HandlerOpts{}))
	}
```

Give `NewDuckDBExecutor` its fifth parameter, `onWait func(time.Duration)`, and pass it to `newPool`. The CLI passes the metrics' `observeWait`, which means the metrics must be built before the executor; build them in the CLI from the config and hand both to `New`.

In `queryDataset`, record once at the end, and in the error paths, with the code the response carried.

- [ ] **Step 5: Run the tests**

```bash
go test -short -race ./internal/serve/ ./internal/cli/serve/
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/serve/ internal/cli/serve/serve.go
git commit -m "serve: six metrics, and the session wait that sizes the pool"
```

---

### Task 7: `/healthz` says busy rather than down, and answers HEAD

**Files:**
- Modify: `internal/serve/http.go` (`healthz`, `getOnly`)
- Test: `internal/serve/http_test.go`
- Modify: `README.md` (the routes table)

**Interfaces:**
- Consumes: `query`, `s.healthTimeout`.
- Produces: `503 {"status":"busy"}` distinct from `503 {"status":"unavailable"}`; `HEAD /healthz` answered, `HEAD` elsewhere still `405`.

- [ ] **Step 1: Write the failing test**

```go
// A full pool is not a dead server. A supervisor that cannot tell them apart
// restarts a server that is merely busy, which is the worst moment to do it.
func TestCliServe_HealthzIsBusyNotDownWhenThePoolIsFull(t *testing.T) {
	coverage.Covers(t, "cli.serve")
	ts := newTestServer(t, testServe)

	sess, err := ts.srv.exec.Acquire(context.Background())
	assert.NoError(t, err)
	defer sess.Release()

	r := ts.do(t, http.MethodGet, "/healthz", nil)
	assert.Equal(t, http.StatusServiceUnavailable, r.status)
	assert.Equal(t, "busy", r.body["status"])
}
```

- [ ] **Step 2: Run it to see it fail**

Run: `go test -short ./internal/serve/ -run TestCliServe_HealthzIsBusy`
Expected: FAIL. Today healthz waits and then reports `unavailable`.

- [ ] **Step 3: Rewrite healthz**

```go
// healthz answers from a session, so it reports the path a request takes.
//
// A full pool answers busy, not unavailable: the difference is whether the
// server is broken or merely loaded, and a supervisor reading a restart loop
// needs to know which.
func (s *Server) healthz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), s.healthTimeout)
	defer cancel()

	_, _, err := query(ctx, s.exec, s.health, nil, 1)
	switch {
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, ErrClosed):
		writeJSON(w, r, http.StatusServiceUnavailable, []byte(`{"status":"busy"}`))
	case err != nil:
		s.logger.Warn("health check failed", zap.String("error", Redact(err.Error())))
		writeJSON(w, r, http.StatusServiceUnavailable, []byte(`{"status":"unavailable"}`))
	default:
		writeJSON(w, r, http.StatusOK, []byte(`{"status":"ok"}`))
	}
}
```

`s.health` is a `Statement` for `SELECT 1`, prepared once in `New` and stored on the server, so healthz prepares nothing per request.

A caveat worth a comment: a request that timed out leaves its session busy until its query finishes, so a pool full of abandoned slow queries reports busy. That is the truth about the server.

- [ ] **Step 4: Write the failing HEAD test**

Uptime monitors send `HEAD`, and `getOnly` refuses it with `405` today. Add to `internal/serve/http_test.go`:

```go
// Monitors send HEAD, and the status line is the whole answer. A dataset
// still refuses it: running the query and discarding the rows would spend a
// session on nothing.
func TestCliServe_HealthzAnswersHead(t *testing.T) {
	coverage.Covers(t, "cli.serve")
	ts := newTestServer(t, testServe)

	r := ts.do(t, http.MethodHead, "/healthz", nil)
	assert.Equal(t, http.StatusOK, r.status)
	assert.Equal(t, "", r.raw)

	sess, err := ts.srv.exec.Acquire(context.Background())
	assert.NoError(t, err)
	busy := ts.do(t, http.MethodHead, "/healthz", nil)
	sess.Release()
	assert.Equal(t, http.StatusServiceUnavailable, busy.status)

	ds := ts.do(t, http.MethodHead, "/v1/datasets/status", pageToken)
	assert.Equal(t, http.StatusMethodNotAllowed, ds.status)
	assert.Equal(t, http.MethodGet, ds.header.Get("Allow"))
}
```

`ts.do` unmarshals the body as JSON when it is non-empty; a HEAD response has none, so confirm the helper tolerates an empty body and adjust it if it does not.

- [ ] **Step 5: Run it to see it fail**

Run: `go test -short ./internal/serve/ -run TestCliServe_HealthzAnswersHead`
Expected: FAIL. `getOnly` answers `405` for the two `/healthz` calls.

- [ ] **Step 6: Let HEAD through for healthz only**

In `internal/serve/http.go`:

```go
// getOnly refuses every method but GET, and HEAD on /healthz.
//
// Monitors send HEAD, and for /healthz the status line is the whole answer.
// Every other route stays GET-only on purpose: a HEAD of a dataset would run
// the query, borrow a session and throw the rows away, which spends the pool
// on nothing. net/http suppresses the body of a HEAD response, so the health
// handler needs no special case.
func getOnly(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		allowed := http.MethodGet
		if r.URL.Path == "/healthz" {
			allowed = "GET, HEAD"
		}
		if r.Method == http.MethodGet || (r.Method == http.MethodHead && r.URL.Path == "/healthz") {
			next.ServeHTTP(w, r)
			return
		}
		w.Header().Set("Allow", allowed)
		writeError(w, r, &apiError{http.StatusMethodNotAllowed, "method_not_allowed",
			r.Method + " is not allowed; " + allowedDescription(r.URL.Path)})
	})
}

// allowedDescription names what a route accepts, for the error message.
func allowedDescription(path string) string {
	if path == "/healthz" {
		return "/healthz is GET or HEAD"
	}
	return "every route is GET, and /healthz is also HEAD"
}
```

- [ ] **Step 7: Run the tests**

Run: `go test -short -race ./internal/serve/`
Expected: PASS. `TestCliServe_ASlowQueryIsA504AndTurnsHealthRed` now expects `busy` rather than `unavailable`; update its assertion and its name to `...TurnsHealthBusy`. `TestCliServe_ErrorsCarryTheirCodeAndNameTheCause`'s "not GET" case asserts the `405` message; update its expected substring if it no longer matches.

- [ ] **Step 8: Document and commit**

In `README.md`, change the `/healthz` row of the routes table to:

```markdown
| `/healthz` | none | `200` `{"status":"ok"}`; `503` `{"status":"busy"}` when no session is free, `{"status":"unavailable"}` when the query fails. `HEAD` is answered too, for monitors |
```

and note under the table that every other route is GET only, because a `HEAD` of a dataset would run its query and discard the rows.

```bash
git add internal/serve/http.go internal/serve/http_test.go README.md
git commit -m "serve: healthz reports a busy pool as busy, and answers HEAD"
```

---

### Task 8: What the pool does to Postgres

**Files:**
- Create: `internal/serve/postgres_integration_test.go`
- Modify: `docs/coverage/features.yml` (`cli.serve` gains `integration`)

**Interfaces:**
- Consumes: `NewDuckDBExecutor`, `duckdb.OpenPath`, testcontainers Postgres, pgx.
- Produces: a documented relationship between `pool.size`, `pg_connection_limit` and Postgres backends.

- [ ] **Step 1: Write the test**

Create `internal/serve/postgres_integration_test.go`. Start a Postgres, create a table, attach it read-only with `pg_connection_limit = 4`, run concurrent scans across a pool of 8, and read `pg_stat_activity` at the peak.

```go
// One DuckDB scan of an attached Postgres opens up to pg_connection_limit
// connections. With a pool, the worst case may be size x that, against a
// database that also carries the pipeline's writer. This test says which it
// is, so the README can state it rather than guess.
func TestIntegrationServePool_PostgresBackendsStayBounded(t *testing.T) {
	coverage.Covers(t, "cli.serve")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	// ... start container, seed a table of 200k rows, attach READ_ONLY with
	// pg_connection_limit = 4, build an executor of size 8 ...

	// Drive 8 concurrent scans, sampling pg_stat_activity while they run.
	peak := samplePeakBackends(t, direct, func() { runConcurrentScans(t, ex, 8) })

	t.Logf("peak Postgres backends: %d, with pool.size=8 and pg_connection_limit=4", peak)
	// The bound the README will state. If this fails, the README is wrong,
	// not the test: read the number and correct both.
	assert.That(t, peak <= 8*4)
}
```

Implement `samplePeakBackends` by polling
`SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() AND pid <> pg_backend_pid()`
every 20 ms on a direct pgx connection while the scans run, keeping the maximum.

- [ ] **Step 2: Run it and read the number**

Run: `go test -run '^TestIntegrationServePool' ./internal/serve/ -v`
Expected: PASS, and a log line giving the peak. The number matters more than the assertion: record it for the README and the PR.

- [ ] **Step 3: Document the relationship**

In `README.md`, in the `sqlflow serve` bullet list, replace the "Bound Postgres connections" bullet with what the test measured. State the real multiplier, `pool.size × pg_connection_limit` or whatever the number shows, and say to keep it under the database's `max_connections` less what the pipeline needs.

- [ ] **Step 4: Raise the coverage requirement and commit**

In `docs/coverage/features.yml`, change `cli.serve`'s `requires: [unit, release]` to `requires: [unit, integration, release]`.

```bash
go test -short -race ./internal/serve/
git add internal/serve/postgres_integration_test.go docs/coverage/features.yml README.md
git commit -m "serve: measure what a pool costs an attached Postgres"
```

---

### Task 9: Docs, coverage status, and the PR

**Files:**
- Modify: `README.md`, `CHANGELOG.md`, `docs/coverage/status/features.yml`

**Interfaces:**
- Consumes: everything above.

- [ ] **Step 1: Document the pool**

In `README.md`, in the `sqlflow serve` section, replace the bullet that begins "**One connection serves every request.**" with:

```markdown
- **A pool serves requests.** `serve.pool.size` sessions run at once, four by
  default. A request waits for a free session, and that wait counts toward
  the dataset's timeout, so an exhausted pool answers `504 query_timeout`.
  `queued_ms` in the response, and `sqlflow_serve_session_wait_seconds` in the
  metrics, say whether the pool is the limit.
- **A timeout stops reading, not always the query.** See Task 5, step 9,
  which writes this bullet and the `statement_timeout` guidance beside it.
```

Add a short `pool` and `metrics` block to the config example in the same section, and document `GET /metrics` in the routes table as unauthenticated and off by default.

- [ ] **Step 2: Changelog**

Under `## Unreleased` → `### Added`:

```markdown
- `sqlflow serve` answers requests from a pool of sessions rather than one
  connection. `serve.pool.size` sets how many run at once, four by default.
  Every session is pinned to UTC, and the config's `commands` run once, on a
  connection of their own. A response carries `queued_ms` beside
  `elapsed_ms`, so a busy pool is not reported as a slow query, and
  `/healthz` answers `busy` rather than `unavailable` when no session is
  free. With `serve.metrics.enabled`, `GET /metrics` serves six instruments,
  including the session wait that sizes the pool.
```

- [ ] **Step 3: Regenerate the coverage status**

The `cli.serve` row gains an integration level, so the committed status file changes. Run the unit and integration passes, then write the status from them:

```bash
mkdir -p .coverage
go test -short -race -json ./... > .coverage/go.json
go test -json -run '^TestIntegration' ./internal/serve/ > .coverage/go-integration.json
make coverage-write
git status --short docs/coverage
```

Expected: `docs/coverage/status/features.yml` gains `integration: covered` on `cli.serve`, and `matrix.md` follows. The release level is unchanged and comes from CI.

- [ ] **Step 4: Run everything**

```bash
go build ./... && go vet ./... && gofmt -l internal/ cmd/
go test -short -race ./...
go test -run '^TestIntegration' ./internal/serve/
uv run --locked pytest tests/tooling -q
```

Expected: all pass, `gofmt -l` prints nothing.

- [ ] **Step 5: Commit and hand off**

```bash
git add README.md CHANGELOG.md docs/coverage/
git commit -m "docs: the serve session pool"
```

Do not push or open the PR without the maintainer's go-ahead. Report: the branch, the commits, the Task 1 memory numbers, the Task 8 Postgres peak, and the unit and integration results.
