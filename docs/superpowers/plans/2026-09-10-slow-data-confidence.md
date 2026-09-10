# Slow-Moving Data Confidence Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prove, with tests that run in minutes and a soak that runs for hours, that a pipeline stays live and correct when its stream trickles, surges then stops, or never delivers at all.

**Architecture:** A one-row engine table, `sqlflow_progress`, written where the state transaction already commits, plus an in-memory snapshot of the same facts. The window predicate reads the table so a quiet stream closes its open windows. `/stats` and a new `/healthz` read the snapshot. Seven test cells cover the invariants; a `slow-soak` skill runs a scripted traffic profile against a real broker.

**Tech Stack:** Go 1.25, DuckDB via ADBC, franz-go `kgo`, `coder/websocket`, testcontainers Kafka for the one integration cell, bash and Python for the soak, the existing coverage registry.

**Spec:** `docs/superpowers/specs/2026-09-10-slow-data-confidence-design.md`

## Global Constraints

- Branch `feat/slow-data-confidence` in the main checkout at `/Users/danielmican/code/github.com/turbolytics/sql-flow`. Never `git checkout` elsewhere while a soak runs; run the soak from a scratchpad copy of its scripts.
- Every Go test starts with `coverage.Covers(t, "<feature id>")`. Ids used here: `core.consume_loop`, `state.durability`, `manager.tumbling_window`, `source.kafka`, `source.websocket`, `observability.metrics`, `cli.invocation`. A skipped test covers nothing; integration tests skip only under `-short`.
- The coverage matrix (`docs/coverage/matrix.*`) is regenerated from CI artifacts only. After any change under `internal/config` run `make schema`. No config struct changes are planned here.
- Prose follows Google Technical Writing One. No em dashes. No attribution lines in commits or the PR.
- `flush_interval_seconds` keeps its 30 second floor on the config path; tests set the Turbine's interval directly, which has no floor.
- The engine table names follow `sqlflow_offsets`: `sqlflow_progress`, excluded from state stats the same way.
- Timestamps in `sqlflow_progress` are wall clock in UTC, written by the engine, never by SQL.

---

### Task 1: The progress table and the in-memory snapshot

**Files:**
- Create: `internal/core/progress.go`
- Modify: `internal/core/turbine.go` (struct at ~line 199, `WithStateStore` at ~254, `commitState` at ~735, `processBatch` at ~789)
- Modify: `internal/core/stats.go:129` (the exclusion)
- Test: `internal/core/progress_test.go` (new)

**Interfaces:**
- Produces: `core.Progress{LastArrival, LastCommit time.Time; Messages int64}`; `core.ProgressStore` with `NewProgressStore(conn adbc.Connection)`, `Init(ctx) error`, `Record(ctx, p Progress) error`; `core.WithProgressStore(s progressSaver) TurbineOption`; `(*Turbine).Progress() Progress`.
- Consumed by: Task 3 (the predicate reads the table), Task 4 (`/stats` and `/healthz` read the snapshot), Task 6 (root.go wiring).

- [ ] **Step 1: Write the failing tests**

Create `internal/core/progress_test.go`:

```go
package core

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/turbolytics/sql-flow/internal/duckdb"
	"github.com/zeebo/assert"
)

// progressRecorder is the test double for the store: it keeps every record.
type progressRecorder struct {
	mu   sync.Mutex
	recs []Progress
}

func (r *progressRecorder) Record(_ context.Context, p Progress) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.recs = append(r.recs, p)
	return nil
}

func (r *progressRecorder) last() (Progress, int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.recs) == 0 {
		return Progress{}, 0
	}
	return r.recs[len(r.recs)-1], len(r.recs)
}

// A batch moves both clocks and the count. An idle tick moves the commit
// clock only. That distinction is what lets a predicate tell "quiet" from
// "stuck", so it is the first thing pinned.
func TestCoreConsumeLoop_ProgressRecordsArrivalOnBatchAndCommitOnIdle(t *testing.T) {
	coverage.Covers(t, "core.consume_loop")
	rec := &progressRecorder{}
	src := newBlockingSource(messages(3))
	tb := NewTurbine(src, &fakeHandler{}, &fakeSink{}, 1000, 30*time.Millisecond,
		&sync.Mutex{}, PipelineErrorPolicies{}, WithProgressStore(rec))
	done := make(chan struct{})
	go func() { _, _ = tb.ConsumeLoop(context.Background(), 0); close(done) }()

	deadline := time.After(5 * time.Second)
	var afterBatch Progress
	for {
		p, n := rec.last()
		if n >= 1 && p.Messages == 3 {
			afterBatch = p
			break
		}
		select {
		case <-deadline:
			t.Fatalf("no progress record with 3 messages; records=%d", n)
		default:
			time.Sleep(5 * time.Millisecond)
		}
	}
	assert.That(t, !afterBatch.LastArrival.IsZero())
	assert.That(t, !afterBatch.LastCommit.Before(afterBatch.LastArrival))

	// Now the source is silent. Idle ticks keep recording, and every one
	// of them carries the arrival clock unchanged.
	deadline = time.After(5 * time.Second)
	for {
		p, n := rec.last()
		if n >= 4 {
			assert.Equal(t, afterBatch.LastArrival, p.LastArrival)
			assert.That(t, p.LastCommit.After(afterBatch.LastCommit))
			assert.Equal(t, int64(3), p.Messages)
			break
		}
		select {
		case <-deadline:
			t.Fatalf("idle ticks did not record progress; records=%d", n)
		default:
			time.Sleep(5 * time.Millisecond)
		}
	}
	snap := tb.Progress()
	assert.Equal(t, int64(3), snap.Messages)
	close(src.release)
	<-done
}

// The store writes one row and rewrites it; it never grows. Read back
// through a second statement, the way the window manager will.
func TestStateDurability_ProgressStoreKeepsOneRow(t *testing.T) {
	coverage.Covers(t, "state.durability")
	ctx := context.Background()
	db, err := duckdb.OpenPath(ctx, "")
	assert.NoError(t, err)
	defer db.Close()
	conn, err := db.Open(ctx)
	assert.NoError(t, err)
	defer conn.Close()

	store := NewProgressStore(conn)
	assert.NoError(t, store.Init(ctx))
	assert.NoError(t, store.Init(ctx)) // idempotent, like the offsets table

	t0 := time.Date(2026, 9, 10, 12, 0, 0, 0, time.UTC)
	assert.NoError(t, store.Record(ctx, Progress{LastArrival: t0, LastCommit: t0, Messages: 1}))
	assert.NoError(t, store.Record(ctx, Progress{LastArrival: t0, LastCommit: t0.Add(time.Minute), Messages: 1}))

	stmt, err := conn.NewStatement()
	assert.NoError(t, err)
	defer stmt.Close()
	assert.NoError(t, stmt.SetSqlQuery(`SELECT count(*), max(messages), max(last_commit) - max(last_arrival) FROM sqlflow_progress`))
	reader, _, err := stmt.ExecuteQuery(ctx)
	assert.NoError(t, err)
	defer reader.Release()
	assert.That(t, reader.Next())
	rec := reader.Record()
	assert.Equal(t, int64(1), rec.Column(0).(interface{ Value(int) int64 }).Value(0))
	assert.Equal(t, int64(1), rec.Column(1).(interface{ Value(int) int64 }).Value(0))
}
```

Note: `db.Open(ctx)` is the ADBC database's connection constructor; if `internal/duckdb.OpenPath` returns a wrapper with a different method, use the one `internal/core/stats_test.go` uses to get a connection and keep the rest.

- [ ] **Step 2: Run them and watch them fail**

Run: `go test ./internal/core/ -run 'Progress' -v -count=1 2>&1 | head -20`
Expected: compile errors, `undefined: Progress`, `undefined: WithProgressStore`, `undefined: NewProgressStore`.

- [ ] **Step 3: Write the store**

Create `internal/core/progress.go`:

```go
package core

import (
	"context"
	"fmt"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
)

// progressTable is engine bookkeeping beside sqlflow_offsets: one row that
// says when the newest batch arrived, when state last committed, and how many
// messages have been consumed. The window predicate reads it to tell a quiet
// stream from a replay; /stats and /healthz read the in-memory copy.
const progressTable = "sqlflow_progress"

// Progress is the pipeline's liveness in three facts. Wall clock, UTC.
type Progress struct {
	LastArrival time.Time
	LastCommit  time.Time
	Messages    int64
}

// progressSaver is what the Turbine needs; ProgressStore is the DuckDB one.
type progressSaver interface {
	Record(ctx context.Context, p Progress) error
}

// ProgressStore keeps the one-row table current. It is created on the state
// connection when the pipeline has a state file, so its writes ride the same
// transaction as the offsets, and on a connection to the in-memory database
// otherwise.
type ProgressStore struct {
	conn adbc.Connection
}

func NewProgressStore(conn adbc.Connection) *ProgressStore {
	return &ProgressStore{conn: conn}
}

// Init creates the table and its single row if they are absent.
func (s *ProgressStore) Init(ctx context.Context) error {
	for _, q := range []string{
		`CREATE TABLE IF NOT EXISTS ` + progressTable + ` (
		    last_arrival TIMESTAMP,
		    last_commit  TIMESTAMP,
		    messages     BIGINT NOT NULL
		)`,
		`INSERT INTO ` + progressTable + ` (last_arrival, last_commit, messages)
		 SELECT NULL, NULL, 0 WHERE NOT EXISTS (SELECT 1 FROM ` + progressTable + `)`,
	} {
		if err := s.exec(ctx, q); err != nil {
			return fmt.Errorf("initialising %s: %w", progressTable, err)
		}
	}
	return nil
}

// Record rewrites the row. A zero LastArrival is left as it was, which is how
// an idle tick moves the commit clock without touching the arrival clock.
func (s *ProgressStore) Record(ctx context.Context, p Progress) error {
	q := fmt.Sprintf(`UPDATE %s SET last_commit = TIMESTAMP '%s', messages = %d`,
		progressTable, p.LastCommit.UTC().Format("2006-01-02 15:04:05.999999"), p.Messages)
	if !p.LastArrival.IsZero() {
		q += fmt.Sprintf(`, last_arrival = TIMESTAMP '%s'`,
			p.LastArrival.UTC().Format("2006-01-02 15:04:05.999999"))
	}
	if err := s.exec(ctx, q); err != nil {
		return fmt.Errorf("recording progress: %w", err)
	}
	return nil
}

func (s *ProgressStore) exec(ctx context.Context, q string) error {
	stmt, err := s.conn.NewStatement()
	if err != nil {
		return err
	}
	defer stmt.Close()
	if err := stmt.SetSqlQuery(q); err != nil {
		return err
	}
	_, err = stmt.ExecuteUpdate(ctx)
	return err
}
```

- [ ] **Step 4: Wire the Turbine**

In `internal/core/turbine.go`, add to the `Turbine` struct beside `offsets`:

```go
	// progress is the liveness record, see progress.go. Optional: a Turbine
	// built without it records nothing and Progress() reports zeros.
	progress progressSaver
	// snapshot is the in-memory copy of what progress last recorded, read by
	// /stats and /healthz without touching the database. Guarded by lock.
	snapshot Progress
	// batchSinceCommit is set by processBatch and cleared by commitState, so
	// commitState knows whether this commit follows an arrival or an idle tick.
	batchSinceCommit bool
```

Add the option after `WithStateStore`:

```go
// WithProgressStore records liveness into a store and into an in-memory
// snapshot on every commit and every idle tick.
func WithProgressStore(s progressSaver) TurbineOption {
	return func(t *Turbine) { t.progress = s }
}

// Progress reports the last recorded liveness facts. Zero values mean
// nothing has been recorded yet.
func (t *Turbine) Progress() Progress {
	t.lock.Lock()
	defer t.lock.Unlock()
	return t.snapshot
}
```

At the top of `commitState`, before the `if t.offsets == nil || t.stateTx == nil` guard, insert the recording, so a stateless pipeline records too:

```go
	if t.progress != nil {
		now := time.Now().UTC()
		t.lock.Lock()
		p := Progress{LastCommit: now, Messages: t.stats.MessagesConsumed()}
		if t.batchSinceCommit {
			p.LastArrival = now
			t.snapshot.LastArrival = now
		}
		t.snapshot.LastCommit = now
		t.snapshot.Messages = p.Messages
		t.batchSinceCommit = false
		t.lock.Unlock()
		if err := t.progress.Record(ctx, p); err != nil {
			t.logger.Warn("recording progress", zap.Error(err))
		}
	}
```

Check that `commitState`'s existing `t.lock.Lock()` comes after this block, not around it; the block takes and releases the lock itself. In `processBatch`, immediately after the handler `Invoke` succeeds and before the sink write, set the flag under the lock the code already holds there:

```go
	t.batchSinceCommit = true
```

If `processBatch` releases the lock before that point, take it again for the assignment. A stateless pipeline reaches `commitState` from `processBatch` and from the idle tick already; both now record.

In `internal/core/stats.go:129`, extend the exclusion:

```go
			if name == offsetsTable || name == batchTable || name == progressTable {
```

- [ ] **Step 5: Run the tests and the package**

Run: `go test ./internal/core/ -run 'Progress' -v -count=1 2>&1 | grep -E '^(--- |ok|FAIL)'`
Expected: both PASS.

Run: `go test ./internal/core/ -count=1 2>&1 | tail -2`
Expected: `ok`. The existing idle-tick tests still pass because the recorder is optional.

- [ ] **Step 6: Commit**

```bash
git add internal/core/progress.go internal/core/progress_test.go internal/core/turbine.go internal/core/stats.go
git commit -m "core: a progress table, and a snapshot, so liveness is a fact the engine keeps

sqlflow_progress holds one row: when the newest batch arrived, when state
last committed, and how many messages have been consumed. A batch moves
all three, an idle tick moves the commit clock only. The window predicate
will read the table; /stats and /healthz will read the snapshot."
```

---

### Task 2: Trickle and surge cells for the consume loop

**Files:**
- Test: `internal/core/slow_test.go` (new)

**Interfaces:**
- Consumes: `blockingSource` and `messages(n)` from `turbine_test.go:366-380`; `fakeHandler`, `fakeSink` (`turbine_test.go:72,134`); `offsetSaver`.
- Produces: `pacedSource`, a source that emits one batch per tick on a schedule, reused by Task 7's docs of the cells.

- [ ] **Step 1: Write the cells**

Create `internal/core/slow_test.go`:

```go
package core

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

// pacedSource delivers one message every `every`, n times, then stays open
// and silent until released. It is the trickle.
type pacedSource struct {
	ch      chan []Message
	release chan struct{}
	every   time.Duration
	n       int
}

func newPacedSource(n int, every time.Duration) *pacedSource {
	return &pacedSource{ch: make(chan []Message), release: make(chan struct{}), every: every, n: n}
}

func (s *pacedSource) Start() error { return nil }
func (s *pacedSource) Close() error { return nil }
func (s *pacedSource) Commit() error { return nil }
func (s *pacedSource) CommitMarks(*Marks) error { return nil }
func (s *pacedSource) Stream() <-chan []Message {
	go func() {
		defer close(s.ch)
		for i := 0; i < s.n; i++ {
			select {
			case <-time.After(s.every):
				s.ch <- []Message{{Value: []byte(`{"i":1}`), Topic: "t", Partition: 0, Offset: int64(i)}}
			case <-s.release:
				return
			}
		}
		<-s.release
	}()
	return s.ch
}

// markRecorder is an offsetSaver that keeps the newest saved mark.
type markRecorder struct {
	mu    sync.Mutex
	saves int
	last  *Marks
}

func (m *markRecorder) Save(_ context.Context, marks *Marks) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.saves++
	m.last = marks
	return nil
}

func (m *markRecorder) count() int { m.mu.Lock(); defer m.mu.Unlock(); return m.saves }

// noopTx satisfies stateTx for a pipeline that has offsets but no real
// transaction; commitState needs both to be non-nil to save marks.
type noopTx struct{}

func (noopTx) Commit(context.Context) error   { return nil }
func (noopTx) Rollback(context.Context) error { return nil }

func waitFor(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.After(timeout)
	for !cond() {
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for %s", what)
		default:
			time.Sleep(5 * time.Millisecond)
		}
	}
}

// C1. A trickle: one message every 40 ms, a batch size it will never reach,
// a 20 ms flush interval. Every message reaches the sink on the interval,
// not when the batch fills, and every flush saves a mark.
func TestCoreConsumeLoop_TrickleFlushesEveryMessageOnTheInterval(t *testing.T) {
	coverage.Covers(t, "core.consume_loop")
	src := newPacedSource(10, 40*time.Millisecond)
	sink := &fakeSink{}
	marks := &markRecorder{}
	tb := NewTurbine(src, &fakeHandler{}, sink, 1000, 20*time.Millisecond,
		&sync.Mutex{}, PipelineErrorPolicies{}, WithStateStore(marks, noopTx{}))
	done := make(chan struct{})
	go func() { _, _ = tb.ConsumeLoop(context.Background(), 0); close(done) }()

	// The first message lands within one interval plus scheduling slack,
	// long before the 40 ms cadence could have produced a second one.
	start := time.Now()
	waitFor(t, "the first row at the sink", 2*time.Second, func() bool {
		sink.mu.Lock()
		defer sink.mu.Unlock()
		return sink.rows >= 1
	})
	assert.That(t, time.Since(start) < 40*time.Millisecond+20*time.Millisecond+150*time.Millisecond)

	waitFor(t, "all ten rows", 5*time.Second, func() bool {
		sink.mu.Lock()
		defer sink.mu.Unlock()
		return sink.rows == 10
	})
	// Ten messages, at least ten flushes, since no batch ever held two.
	assert.That(t, marks.count() >= 10)
	close(src.release)
	<-done
}

// C2. A surge then silence: 5,000 messages in one burst, then nothing. The
// final partial batch reaches the sink within an interval of the last
// arrival, the saved mark is the last offset, and the silence writes nothing.
func TestCoreConsumeLoop_SurgeThenSilenceFlushesTheTailOnce(t *testing.T) {
	coverage.Covers(t, "core.consume_loop")
	burst := messages(5000)
	for i := range burst {
		burst[i].Offset = int64(i)
	}
	src := newBlockingSource(burst)
	sink := &fakeSink{}
	marks := &markRecorder{}
	tb := NewTurbine(src, &fakeHandler{}, sink, 1000, 30*time.Millisecond,
		&sync.Mutex{}, PipelineErrorPolicies{}, WithStateStore(marks, noopTx{}))
	done := make(chan struct{})
	go func() { _, _ = tb.ConsumeLoop(context.Background(), 0); close(done) }()

	waitFor(t, "5000 rows at the sink", 5*time.Second, func() bool {
		sink.mu.Lock()
		defer sink.mu.Unlock()
		return sink.rows == 5000
	})
	marks.mu.Lock()
	last := marks.last
	marks.mu.Unlock()
	assert.That(t, last != nil)
	var newest int64 = -1
	last.Each(func(_ string, _ int32, m Mark) { newest = m.Offset })
	assert.Equal(t, int64(4999), newest)

	// Five intervals of silence: the sink sees no further write.
	sink.mu.Lock()
	before := sink.writes
	sink.mu.Unlock()
	time.Sleep(150 * time.Millisecond)
	sink.mu.Lock()
	after := sink.writes
	sink.mu.Unlock()
	assert.Equal(t, before, after)
	close(src.release)
	<-done
}
```

If `fakeSink` has no `writes` counter, add one beside `rows` in `turbine_test.go:134` and increment it in its `WriteTable`. If `Marks.Each`'s signature differs from `(topic string, partition int32, m Mark)`, mirror the one in `internal/core/marks.go`.

- [ ] **Step 2: Run them**

Run: `go test ./internal/core/ -run 'Trickle|SurgeThenSilence' -v -count=1 2>&1 | grep -E '^(--- |ok|FAIL)|slow_test'`
Expected: both PASS on the current engine. These are not failing-first: they pin behaviour that exists so the soak and the docs can cite it. If either fails, that is a finding: stop, record the failure verbatim in the PR, and fix the engine before continuing.

- [ ] **Step 3: Commit**

```bash
git add internal/core/slow_test.go internal/core/turbine_test.go
git commit -m "core: a trickle flushes every message on the interval, a surge flushes its tail once

Two cells that pin liveness for slow data. A source that delivers one
message every 40 ms into a batch of 1,000 with a 20 ms interval gets
each row to the sink on the interval and saves a mark per flush. A burst
of 5,000 followed by silence flushes its partial tail within an interval,
saves the last offset, and writes nothing more."
```

---

### Task 3: Idle commits keep the state file flat (C3)

**Files:**
- Test: `internal/core/progress_test.go` (append)

**Interfaces:**
- Consumes: `duckdb.OpenPath`, `NewOffsetStore`, `NewProgressStore`, `WithStateStore`, `WithProgressStore`, `newIdleSource` (`turbine_test.go:861`). Read `turbine_test.go:599-640` for how `txConn` obtains a real transaction on a connection, and `root.go:206-235` for how the run command builds the state store on the same connection.

- [ ] **Step 1: Write the cell**

Append to `internal/core/progress_test.go`:

```go
// C3. Twenty idle ticks against a real state file. The commit clock moves
// on every tick, the arrival clock never does, and the file does not grow.
func TestStateDurability_IdleTicksDoNotGrowTheStateFile(t *testing.T) {
	coverage.Covers(t, "state.durability")
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state.db")
	db, err := duckdb.OpenPath(ctx, path)
	assert.NoError(t, err)
	defer db.Close()
	conn, err := db.Open(ctx)
	assert.NoError(t, err)
	defer conn.Close()

	offsets := NewOffsetStore(conn)
	assert.NoError(t, offsets.Init(ctx))
	progress := NewProgressStore(conn)
	assert.NoError(t, progress.Init(ctx))
	tx := beginTx(t, conn) // the helper turbine_test.go uses to open the state transaction

	src := newIdleSource()
	tb := NewTurbine(src, &fakeHandler{}, &fakeSink{}, 1000, 20*time.Millisecond,
		&sync.Mutex{}, PipelineErrorPolicies{},
		WithStateStore(offsets, tx), WithProgressStore(progress))
	done := make(chan struct{})
	go func() { _, _ = tb.ConsumeLoop(ctx, 0); close(done) }()

	sizeAfter := func(n int64) int64 {
		waitFor(t, "idle ticks", 5*time.Second, func() bool { return tb.Progress().Messages == 0 && !tb.Progress().LastCommit.IsZero() && ticks(tb) >= n })
		fi, err := os.Stat(path)
		assert.NoError(t, err)
		return fi.Size()
	}
	s1 := sizeAfter(1)
	s20 := sizeAfter(20)
	assert.That(t, tb.Progress().LastArrival.IsZero())
	// One checkpoint of slack: DuckDB may write a WAL frame on the first
	// commit. Twenty empty commits must not add another.
	assert.That(t, s20 <= s1+256*1024)
	close(src.release)
	<-done
}
```

`ticks(tb)` counts commits; add a `commits int64` counter to the Turbine under the lock, incremented in `commitState` after a successful commit, with an accessor `func (t *Turbine) commitCount() int64` in `turbine.go`. `beginTx` is whatever helper the state tests use to start the transaction on `conn`; if none exists, open it the way `root.go:228-235` does and wrap it in a `stateTx`. Add `"os"` and `"path/filepath"` to the imports.

- [ ] **Step 2: Run it**

Run: `go test ./internal/core/ -run 'IdleTicksDoNotGrow' -v -count=1 2>&1 | grep -E '^(--- |ok|FAIL)|size'`
Expected: PASS. If the file grows by more than the slack, that is a finding about empty commits and belongs in the PR before the docs claim otherwise.

- [ ] **Step 3: Commit**

```bash
git add internal/core/progress_test.go internal/core/turbine.go
git commit -m "core: twenty idle commits leave the state file where it was

The commit clock moves on every tick, the arrival clock never does, and
the file does not grow past one checkpoint of slack."
```

---

### Task 4: The window predicate closes on silence (C4)

**Files:**
- Modify: `dev/config/examples/tumbling.window.yml`, `dev/config/examples/kafka.stateful.window.yml`, `dev/config/examples/bluesky/bluesky.kafka.windowed.yml`, `dev/config/examples/bluesky/bluesky.postgres.windowed.yml`
- Test: `internal/managers/tumbling_test.go` (append)

**Interfaces:**
- Consumes: `newTestConn` (`tumbling_test.go:22`), `newTestTumbling` (`:133`), `recordingSink`, `core.NewProgressStore`.
- Produces: the two-branch predicate, used verbatim by the docs in Task 8.

- [ ] **Step 1: Write the failing test**

Append to `internal/managers/tumbling_test.go`:

```go
// The predicate the shipped examples use, with a five second grace so the
// test runs in seconds. Branch one is the stream clock (#234); branch two is
// the idleness bound, which reads the engine's progress row.
const idlePredicate = `
SELECT bucket, city, count FROM agg
WHERE bucket < (SELECT max(bucket) FROM agg) - INTERVAL '5' SECOND
   OR (SELECT now() - last_arrival FROM sqlflow_progress) > INTERVAL '5' SECOND`

const idleDelete = `
DELETE FROM agg
WHERE bucket < (SELECT max(bucket) FROM agg) - INTERVAL '5' SECOND
   OR (SELECT now() - last_arrival FROM sqlflow_progress) > INTERVAL '5' SECOND`

func setupAggWithProgress(t *testing.T) (adbc.Connection, *core.ProgressStore, func()) {
	t.Helper()
	conn, cleanup := newTestConn(t)
	exec(t, conn, `CREATE TABLE agg (bucket TIMESTAMP, city VARCHAR, count BIGINT)`)
	progress := core.NewProgressStore(conn)
	assert.NoError(t, progress.Init(context.Background()))
	return conn, progress, cleanup
}

// C4, half one. Rows land in one bucket, the stream goes quiet. Nothing
// newer ever arrives, so the stream clock alone would hold the window open
// forever. The idleness branch publishes it once, a grace after the last row.
func TestManagerTumblingWindow__QuietStreamClosesItsLastWindow(t *testing.T) {
	coverage.Covers(t, "manager.tumbling_window")
	ctx := context.Background()
	conn, progress, cleanup := setupAggWithProgress(t)
	defer cleanup()
	sink := &recordingSink{}
	m := newTestTumblingWith(conn, sink, idlePredicate, idleDelete, 500*time.Millisecond)

	exec(t, conn, `INSERT INTO agg VALUES (TIMESTAMP '2026-09-10 12:00:00', 'nyc', 3)`)
	assert.NoError(t, progress.Record(ctx, core.Progress{LastArrival: time.Now().UTC(), LastCommit: time.Now().UTC(), Messages: 3}))

	// Polls inside the grace publish nothing.
	assert.NoError(t, m.Poll(ctx))
	assert.Equal(t, 0, sink.rowCount())

	// Only the commit clock moves while the stream is quiet.
	time.Sleep(5500 * time.Millisecond)
	assert.NoError(t, progress.Record(ctx, core.Progress{LastCommit: time.Now().UTC(), Messages: 3}))
	assert.NoError(t, m.Poll(ctx))
	assert.Equal(t, 1, sink.rowCount())
	assert.NoError(t, m.Poll(ctx))
	assert.Equal(t, 1, sink.rowCount()) // deleted after publish, not published twice
}

// C4, half two, the #234 property kept. Three buckets arrive in one burst
// with arrivals continuous, so the idleness branch never fires; the stream
// clock closes the two older buckets once each and leaves the newest open.
func TestManagerTumblingWindow__ReplayStillClosesEachWindowOnce(t *testing.T) {
	coverage.Covers(t, "manager.tumbling_window")
	ctx := context.Background()
	conn, progress, cleanup := setupAggWithProgress(t)
	defer cleanup()
	sink := &recordingSink{}
	m := newTestTumblingWith(conn, sink, idlePredicate, idleDelete, 500*time.Millisecond)

	exec(t, conn, `INSERT INTO agg VALUES
		(TIMESTAMP '2026-09-10 12:00:00', 'nyc', 1),
		(TIMESTAMP '2026-09-10 12:00:10', 'nyc', 1),
		(TIMESTAMP '2026-09-10 12:00:20', 'nyc', 1)`)
	assert.NoError(t, progress.Record(ctx, core.Progress{LastArrival: time.Now().UTC(), LastCommit: time.Now().UTC(), Messages: 3}))

	assert.NoError(t, m.Poll(ctx))
	assert.Equal(t, 2, sink.rowCount())
	assert.NoError(t, m.Poll(ctx))
	assert.Equal(t, 2, sink.rowCount())
}
```

`newTestTumblingWith(conn, sink, collect, delete, poll)` is `newTestTumbling` with the SQL and interval as parameters; add it beside the original at `tumbling_test.go:133` and have the original call it. `exec` and `rowCount` are whatever the file already uses to run SQL and count published rows; if the names differ, use those. `Poll` is the manager's one-iteration method; if the manager only exposes `Start`, use `Start` with a context cancelled after one poll interval and adjust the assertions to "within one poll".

- [ ] **Step 2: Run them and watch half one fail**

Run: `go test ./internal/managers/ -run 'QuietStream|ReplayStill' -v -count=1 2>&1 | grep -E '^(--- |ok|FAIL)|Error'`
Expected: `QuietStreamClosesItsLastWindow` FAILS at the second `rowCount` assertion if the predicate is the stream clock alone; it PASSES with `idlePredicate` because the test carries the predicate itself. So the failing-first run is: temporarily use the shipped predicate (branch one only) and watch it hold the window open; then restore `idlePredicate` and watch it pass. Record both runs in the commit message.

- [ ] **Step 3: Change the shipped examples**

In each of the four example configs, the collect and delete predicates change from

```sql
WHERE bucket < (SELECT max(bucket) FROM <table>) - INTERVAL '60' SECOND
```

to

```sql
WHERE bucket < (SELECT max(bucket) FROM <table>) - INTERVAL '60' SECOND
   OR (SELECT now() - last_arrival FROM sqlflow_progress) > INTERVAL '60' SECOND
```

and the comment above them gains one paragraph:

```yaml
          # The second branch is the idleness bound. A stream that goes quiet
          # never moves its own clock, so without it the last window would
          # stay open until data a full grace newer arrived. sqlflow_progress
          # is the engine's one-row liveness table; last_arrival is when the
          # newest batch was written. After a grace of silence every open
          # window closes, including the newest.
```

Run: `go run ./cmd/sqlflow config validate dev/config/examples/tumbling.window.yml` and the other three.
Expected: `valid` for each.

- [ ] **Step 4: Commit**

```bash
git add internal/managers/tumbling_test.go dev/config/examples/
git commit -m "tumbling window: a quiet stream closes its last window

The stream-clock predicate from #234 holds a window open until data a
full grace newer arrives, so a stream that stops never closes its last
window. The predicate gains an idleness branch that reads the engine's
progress row: after a grace of silence every open window closes. Two
cells: a quiet stream publishes its bucket once, a replay still closes
each bucket once."
```

---

### Task 5: Progress on `/stats`, and `/healthz` (C7)

**Files:**
- Modify: `internal/cli/run/metrics.go` (`newHTTPMux`, ~line 30)
- Test: `internal/cli/run/health_test.go` (new)

**Interfaces:**
- Consumes: `core.Progress`, `(*core.Turbine).Progress()`.
- Produces: `progressFunc func() core.Progress`; `newHTTPMux(registry, stats, progress, interval, now)`; `/stats` gains `"progress"`; `/healthz`.

- [ ] **Step 1: Write the failing test**

Create `internal/cli/run/health_test.go`:

```go
package run

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/turbolytics/sql-flow/internal/core"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

// C7. Idle is healthy: the commit clock keeps moving while nothing arrives.
// Stuck is not: three intervals without a commit turns /healthz red, and the
// body says how old the last commit is.
func TestObservabilityMetrics_HealthzTellsIdleFromStuck(t *testing.T) {
	coverage.Covers(t, "observability.metrics")
	clock := time.Date(2026, 9, 10, 12, 0, 0, 0, time.UTC)
	now := func() time.Time { return clock }
	p := core.Progress{LastArrival: clock.Add(-time.Hour), LastCommit: clock.Add(-20 * time.Second), Messages: 42}
	mux := newHTTPMux(nil, nil, func() core.Progress { return p }, 30*time.Second, now)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	get := func(path string) (int, map[string]any) {
		resp, err := http.Get(srv.URL + path)
		assert.NoError(t, err)
		defer resp.Body.Close()
		var body map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&body)
		return resp.StatusCode, body
	}

	// Idle for an hour, committed 20 s ago: healthy.
	code, body := get("/healthz")
	assert.Equal(t, http.StatusOK, code)
	assert.Equal(t, "ok", body["status"])

	// /stats carries the ages.
	_, stats := get("/stats")
	prog := stats["progress"].(map[string]any)
	assert.Equal(t, float64(42), prog["messages"])
	assert.Equal(t, float64(3600), prog["arrival_age_seconds"])
	assert.Equal(t, float64(20), prog["commit_age_seconds"])

	// No commit for three intervals: stuck.
	p.LastCommit = clock.Add(-91 * time.Second)
	code, body = get("/healthz")
	assert.Equal(t, http.StatusServiceUnavailable, code)
	assert.Equal(t, "stuck", body["status"])
	assert.Equal(t, float64(91), body["commit_age_seconds"])

	// Never committed at all, just started: healthy until three intervals pass.
	p = core.Progress{}
	code, _ = get("/healthz")
	assert.Equal(t, http.StatusOK, code)
}
```

- [ ] **Step 2: Run it and watch it fail**

Run: `go test ./internal/cli/run/ -run Healthz -v -count=1 2>&1 | head -5`
Expected: compile error, `newHTTPMux` has too many arguments.

- [ ] **Step 3: Extend the mux**

In `internal/cli/run/metrics.go`, change the signature and body:

```go
// progressFunc reports the pipeline's liveness snapshot.
type progressFunc func() core.Progress

// newHTTPMux builds the server the pipeline exposes: Prometheus scraping, a
// JSON view of durable state and progress, and a health check.
//
// /healthz answers the one question a supervisor has: is this pipeline
// making progress? Idle is progress: the commit clock moves on every tick
// whether or not a message arrived. Three intervals without a commit is
// stuck, and the body says how long it has been. `now` is injectable for
// the test; `started` guards the window before the first commit.
func newHTTPMux(registry *prom.Registry, stats statsFunc, progress progressFunc, interval time.Duration, now func() time.Time) *http.ServeMux {
	mux := http.NewServeMux()
	started := now()
	if registry != nil {
		mux.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
	}
	ages := func() (core.Progress, float64, float64) {
		p := progress()
		arrival, commit := -1.0, -1.0
		if !p.LastArrival.IsZero() {
			arrival = now().Sub(p.LastArrival).Seconds()
		}
		if !p.LastCommit.IsZero() {
			commit = now().Sub(p.LastCommit).Seconds()
		} else {
			commit = now().Sub(started).Seconds()
		}
		return p, arrival, commit
	}
	if stats != nil || progress != nil {
		mux.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
			out := map[string]any{"state": nil}
			if stats != nil {
				state, err := stats()
				if err != nil {
					http.Error(w, fmt.Sprintf("collecting state stats: %v", err), http.StatusInternalServerError)
					return
				}
				out["state"] = state
			}
			if progress != nil {
				p, arrival, commit := ages()
				out["progress"] = map[string]any{
					"last_arrival": p.LastArrival, "last_commit": p.LastCommit, "messages": p.Messages,
					"arrival_age_seconds": arrival, "commit_age_seconds": commit,
				}
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(out)
		})
	}
	if progress != nil {
		mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
			_, _, commit := ages()
			w.Header().Set("Content-Type", "application/json")
			if commit > 3*interval.Seconds() {
				w.WriteHeader(http.StatusServiceUnavailable)
				_ = json.NewEncoder(w).Encode(map[string]any{"status": "stuck", "commit_age_seconds": commit, "interval_seconds": interval.Seconds()})
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"status": "ok", "commit_age_seconds": commit})
		})
	}
	return mux
}
```

Keep the existing comment about the state file lock above the function. Add `"time"` to the imports. Update the one call site in `root.go` (find it with `grep -n newHTTPMux internal/cli/run/root.go`) to pass `turbine.Progress`, `flushInterval`, and `time.Now`; the turbine is constructed at `root.go:351`, so the mux must be built after it or take a closure that reads a variable assigned later. Task 6 does the wiring; for this task, make the call compile by passing `nil, flushInterval, time.Now` and note it.

- [ ] **Step 4: Run the test**

Run: `go test ./internal/cli/run/ -run Healthz -v -count=1 2>&1 | grep -E '^(--- |ok|FAIL)'`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/cli/run/metrics.go internal/cli/run/health_test.go internal/cli/run/root.go
git commit -m "run: /stats reports progress, /healthz tells idle from stuck

Idle is healthy: the commit clock moves on every tick whether or not a
message arrived. Three intervals without a commit is stuck, 503, with
the age in the body."
```

---

### Task 6: Wire the run command, and pin the flush default (C6)

**Files:**
- Modify: `internal/cli/run/root.go` (state wiring ~lines 150-260, flush interval at 346, turbine at 351, mux call site)
- Test: `internal/cli/run/flush_interval_test.go` (new)
- Modify: `docs/coverage/invariants.yml` (`pipeline.progress.no_silent_stall`)

**Interfaces:**
- Consumes: everything from Tasks 1 and 5.
- Produces: `flushIntervalFor(seconds int) time.Duration`.

- [ ] **Step 1: Write the failing test**

Create `internal/cli/run/flush_interval_test.go`:

```go
package run

import (
	"testing"
	"time"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

// C6. The invariant registry said a zero interval removes the ticker and a
// low-traffic topic waits forever. It does not: the run command defaults
// zero to thirty seconds. This pins that, for absent, zero and negative.
func TestCliInvocation_FlushIntervalNeverZero(t *testing.T) {
	coverage.Covers(t, "cli.invocation")
	assert.Equal(t, 30*time.Second, flushIntervalFor(0))
	assert.Equal(t, 30*time.Second, flushIntervalFor(-5))
	assert.Equal(t, 45*time.Second, flushIntervalFor(45))
}
```

- [ ] **Step 2: Run it and watch it fail**

Run: `go test ./internal/cli/run/ -run FlushIntervalNeverZero -count=1 2>&1 | head -3`
Expected: `undefined: flushIntervalFor`.

- [ ] **Step 3: Extract the helper and wire progress**

In `root.go`, replace lines 346-349:

```go
			flushInterval := flushIntervalFor(conf.Pipeline.FlushIntervalSeconds)
```

and add, at package level:

```go
// flushIntervalFor is the one place the flush interval is decided. Absent,
// zero and negative all mean the default: a ticker the pipeline cannot lose,
// because without one a batch a low-traffic topic never fills waits forever.
func flushIntervalFor(seconds int) time.Duration {
	if seconds > 0 {
		return time.Duration(seconds) * time.Second
	}
	return 30 * time.Second
}
```

Wire the progress store. Where the run command opens the database (`root.go:156` for the in-memory case and the state-file branch around `:206`), obtain a connection the same way the offsets store does, then:

```go
			progress := core.NewProgressStore(progressConn)
			if err := progress.Init(context.Background()); err != nil {
				return err
			}
			turbineOpts = append(turbineOpts, core.WithProgressStore(progress))
```

In the state-file branch, `progressConn` is the same `conn` the offsets store uses at `:206`, so its writes ride the state transaction. In the in-memory branch, open one connection on the in-memory `db` for it. Then change the mux construction to pass `turbine.Progress`, `flushInterval` and `time.Now`. If the mux is built before the turbine, declare `var progressFn func() core.Progress` before and assign `progressFn = turbine.Progress` after construction, passing a closure that calls it when non-nil and returns a zero `core.Progress` otherwise.

- [ ] **Step 4: Correct the invariant**

In `docs/coverage/invariants.yml`, replace the `pipeline.progress.no_silent_stall` entry's `claim` and mark it enforced:

```yaml
  - id: pipeline.progress.no_silent_stall
    family: lifecycle
    class: liveness
    applies_to: pipeline
    claim: >
      A configuration cannot remove the flush ticker. flush_interval_seconds
      absent, zero or negative all run with the thirty second default, so a
      batch a low-traffic topic never fills still leaves on time. Pinned by
      TestCliInvocation_FlushIntervalNeverZero.
    verified_by: harness
    requires: []
    enforced: true
```

Check `enforced: true` is a value the registry accepts on this entry by running `make coverage-check` if the reports exist locally; if they do not, CI will say.

- [ ] **Step 5: Run the unit pass, then a real pipeline**

Run: `make test-go 2>&1 | tail -3`
Expected: green.

Run a shipped stateless example against the dev broker for thirty seconds and read the endpoints:

```bash
make start-backing-services >/dev/null 2>&1
(go run ./cmd/sqlflow run dev/config/examples/kafka.structured.mem.yml --metrics=prometheus > /tmp/slow-run.log 2>&1 &)
sleep 8
curl -s localhost:8000/healthz; echo
curl -s localhost:8000/stats | python3 -c "import json,sys; d=json.load(sys.stdin); print(d['progress'])"
pkill -f "sqlflow run dev/config/examples/kafka.structured.mem.yml"
```

Expected: `{"status":"ok","commit_age_seconds":<small>}` and a progress object whose `messages` is 0 and whose `arrival_age_seconds` is -1 with no traffic.

- [ ] **Step 6: Commit**

```bash
git add internal/cli/run/root.go internal/cli/run/flush_interval_test.go docs/coverage/invariants.yml
git commit -m "run: the progress store rides the state transaction, and the flush default is pinned

The registry said a zero interval stalls a low-traffic topic forever.
It does not, and now a test says so: absent, zero and negative all run
with the thirty second default. The invariant is corrected and marked
enforced."
```

---

### Task 7: Consumers survive idle (C5)

**Files:**
- Test: `internal/kafka/source_test.go` (append)
- Test: `internal/websocket/source_test.go` (append)

**Interfaces:**
- Consumes: `newTestClient(t, broker, topic, group, extra...)` (`source_test.go:17`), `brokerOrFail`, `produce`; `newServer` and `wsURL` (`websocket/source_test.go:20,53`), and the existing `TestSourceWebsocket_ReconnectsAfterDrop` at `:103`, which is the drop half of C5 and stays as is.

- [ ] **Step 1: Kafka idle past the session timeout**

Append to `internal/kafka/source_test.go`:

```go
// C5, Kafka. A trickle can leave a consumer silent for longer than its
// session timeout. franz-go heartbeats in the background, so the group
// membership survives and the next message is delivered without a rejoin
// storm. Six second session, eight seconds of silence.
func TestIntegrationSourceKafka_SurvivesIdleLongerThanTheSessionTimeout(t *testing.T) {
	coverage.Covers(t, "source.kafka")
	broker := brokerOrFail(t)
	topic := fmt.Sprintf("turbine-idle-%d", time.Now().UnixNano())

	producer, err := kgo.NewClient(kgo.SeedBrokers(broker), kgo.AllowAutoTopicCreation())
	assert.NoError(t, err)
	defer producer.Close()
	produce(t, producer, topic, 1)

	client := newTestClient(t, broker, topic, topic,
		kgo.SessionTimeout(6*time.Second),
		kgo.HeartbeatInterval(2*time.Second),
	)
	src, err := NewSource(client)
	assert.NoError(t, err)
	defer src.Close()
	stream := src.Stream()

	select {
	case batch := <-stream:
		assert.Equal(t, int64(0), batch[0].Offset)
	case <-time.After(30 * time.Second):
		t.Fatal("first message never arrived")
	}

	time.Sleep(8 * time.Second)
	produce(t, producer, topic, 1)

	select {
	case batch := <-stream:
		assert.Equal(t, int64(1), batch[0].Offset)
	case <-time.After(30 * time.Second):
		t.Fatal("the message after the idle gap never arrived")
	}
}
```

- [ ] **Step 2: WebSocket quiet server**

Append to `internal/websocket/source_test.go`:

```go
// C5, WebSocket. A server that accepts and then says nothing for ten seconds
// is a quiet stream, not a dead one. The source must still be listening
// when the frame finally comes.
func TestSourceWebsocket_SurvivesAQuietServer(t *testing.T) {
	coverage.Covers(t, "source.websocket")
	delivered := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer c.CloseNow()
		time.Sleep(10 * time.Second)
		_ = c.Write(r.Context(), websocket.MessageText, []byte(`{"late":true}`))
		<-delivered
	}))
	defer srv.Close()

	src, err := NewSource(wsURL(srv))
	assert.NoError(t, err)
	assert.NoError(t, src.Start())
	defer src.Close()

	select {
	case batch := <-src.Stream():
		assert.Equal(t, `{"late":true}`, string(batch[0].Value))
		close(delivered)
	case <-time.After(30 * time.Second):
		t.Fatal("the late frame never arrived")
	}
}
```

Use the same `websocket` import path the file already has (`github.com/coder/websocket`).

- [ ] **Step 3: Run both**

Run: `SQLFLOW_KAFKA_BROKERS=localhost:9092 go test ./internal/kafka/ -run SurvivesIdle -v -count=1 2>&1 | grep -E '^(--- |ok|FAIL)'`
Expected: PASS in about 12 seconds against the dev broker.

Run: `go test ./internal/websocket/ -run SurvivesAQuietServer -v -count=1 2>&1 | grep -E '^(--- |ok|FAIL)'`
Expected: PASS in about 10 seconds. If the source has a read deadline shorter than ten seconds and reconnects during the quiet, that is a finding: the test still passes if the reconnect lands before the frame, but log it in the PR.

- [ ] **Step 4: Commit**

```bash
git add internal/kafka/source_test.go internal/websocket/source_test.go
git commit -m "sources: a consumer survives idle past its session timeout, a WebSocket survives a quiet server

Two cells for the trickle: eight seconds of silence against a six
second Kafka session, and a server that accepts then says nothing for
ten seconds. The next message arrives in both."
```

---

### Task 8: The slow-soak skill

**Files:**
- Create: `.claude/skills/slow-soak/SKILL.md`, `profile.sh`, `sample.sh`, `verdict.py`, `slow.yml`

**Interfaces:**
- Consumes: the dev Kafka container `kafka1`, the `--with-http-debug` endpoint on `127.0.0.1:5000` queried through a sidecar the way `memory-soak/soak.sh` does, `/healthz` from Task 5.
- Produces: `soak-<label>/samples.csv` and `verdict.txt`.

- [ ] **Step 1: The pipeline under test**

Create `.claude/skills/slow-soak/slow.yml`. It keeps its own landed table and a tumbling window, so latency and window closes are both readable through the debug endpoint:

```yaml
commands:
  - name: tables
    sql: |
      CREATE TABLE IF NOT EXISTS landed (id BIGINT, sent_at TIMESTAMP, landed_at TIMESTAMP);
      CREATE TABLE IF NOT EXISTS agg (bucket TIMESTAMP, count BIGINT);
      CREATE TABLE IF NOT EXISTS published (bucket TIMESTAMP, count BIGINT, published_at TIMESTAMP);

pipeline:
  batch_size: 1000
  flush_interval_seconds: 30
  state:
    path: /conf/state/slow.db
  source:
    type: kafka
    kafka:
      brokers: [{{ SQLFLOW_KAFKA_BROKERS|default('kafka1:19092') }}]
      group_id: {{ SQLFLOW_GROUP_ID|default('slow-soak') }}
      auto_offset_reset: earliest
      topics:
        - "{{ SQLFLOW_TOPIC|default('slow-soak') }}"
  handler:
    type: handlers.InferredMemBatch
    sql: |
      INSERT INTO landed SELECT id, CAST(sent_at AS TIMESTAMP), now()::TIMESTAMP FROM batch;
      INSERT INTO agg
        SELECT time_bucket(INTERVAL '1 minute', CAST(sent_at AS TIMESTAMP)) AS bucket, count(*)
        FROM batch GROUP BY 1
      ON CONFLICT DO NOTHING;
      SELECT 1 WHERE false
  sink:
    type: noop

tables:
  sql:
    - name: agg
      sql: CREATE TABLE IF NOT EXISTS agg (bucket TIMESTAMP, count BIGINT)
      manager:
        tumbling_window:
          poll_interval_seconds: 10
          collect_closed_windows_sql: |
            SELECT bucket, count FROM agg
            WHERE bucket < (SELECT max(bucket) FROM agg) - INTERVAL '60' SECOND
               OR (SELECT now() - last_arrival FROM sqlflow_progress) > INTERVAL '60' SECOND
          delete_closed_windows_sql: |
            DELETE FROM agg
            WHERE bucket < (SELECT max(bucket) FROM agg) - INTERVAL '60' SECOND
               OR (SELECT now() - last_arrival FROM sqlflow_progress) > INTERVAL '60' SECOND
        sink:
          type: sqlcommand
          sqlcommand:
            sql: INSERT INTO published SELECT bucket, count, now()::TIMESTAMP FROM sqlflow_sink_batch
```

Validate it: `go run ./cmd/sqlflow config validate .claude/skills/slow-soak/slow.yml`. If a multi-statement handler is not accepted, split the `landed` insert into the handler and move the aggregate into a second `tables` entry fed by the window; the executor chooses whichever the validator accepts and records it in SKILL.md. If `ON CONFLICT` needs a key, give `agg` a primary key on `bucket` and use `ON CONFLICT (bucket) DO UPDATE SET count = agg.count + excluded.count`.

- [ ] **Step 2: The producer profile**

Create `.claude/skills/slow-soak/profile.sh`:

```bash
#!/usr/bin/env bash
# Feed a topic with a scripted profile from inside the Kafka container.
#
#   profile.sh <kafka-container> <topic> [short]
#
# Phases, full profile: surge 5 min at 1,000/s; silence 60 min; trickle 60
# min at one message per 90 s with two five-minute gaps and one pair sent in
# the same second; silence 30 min. "short" runs 5 min per phase at the same
# shapes. Every message carries its send time as sent_at.
set -euo pipefail
kafka=${1:?kafka container}; topic=${2:?topic}; mode=${3:-full}
if [ "$mode" = short ]; then surge=300; s1=300; trickle=300; s2=300; else surge=300; s1=3600; trickle=3600; s2=1800; fi
id=0
send() { # n messages now
  local n=$1 ts; ts=$(date -u +%Y-%m-%dT%H:%M:%S)
  docker exec -i "$kafka" bash -c "awk -v s=$id -v n=$n -v ts=$ts 'BEGIN{for(i=0;i<n;i++) printf \"{\\\"id\\\":%d,\\\"sent_at\\\":\\\"%s\\\"}\\n\", s+i, ts}' | kafka-console-producer --bootstrap-server localhost:19092 --topic $topic >/dev/null 2>&1"
  id=$((id + n))
  echo "$(date -u +%H:%M:%SZ) sent=$n total=$id"
}
echo "phase surge ${surge}s"; end=$((SECONDS + surge)); while [ $SECONDS -lt $end ]; do send 1000; sleep 1; done
echo "phase silence ${s1}s"; sleep "$s1"
echo "phase trickle ${trickle}s"; end=$((SECONDS + trickle)); gap1=$((SECONDS + trickle/3)); gap2=$((SECONDS + 2*trickle/3)); pair=$((SECONDS + trickle/2))
while [ $SECONDS -lt $end ]; do
  if [ $SECONDS -ge $gap1 ] && [ $SECONDS -lt $((gap1 + 300)) ]; then sleep 10; continue; fi
  if [ $SECONDS -ge $gap2 ] && [ $SECONDS -lt $((gap2 + 300)) ]; then sleep 10; continue; fi
  if [ $SECONDS -ge $pair ] && [ $SECONDS -lt $((pair + 90)) ]; then send 2; pair=$end; else send 1; fi
  sleep 90
done
echo "phase silence ${s2}s"; sleep "$s2"
echo "PROFILE_DONE total=$id"
```

- [ ] **Step 3: The sampler**

Create `.claude/skills/slow-soak/sample.sh`:

```bash
#!/usr/bin/env bash
# Sample a running slow-soak pipeline once a minute into samples.csv.
#
#   sample.sh <container-name> <out-dir> <minutes>
#
# Columns: t, produced (from the profile log), landed, max_latency_s,
# lag (newest produced offset minus committed), anon_mib, state_bytes,
# published_windows, healthz.
set -uo pipefail
name=${1:?container}; out=${2:?out dir}; minutes=${3:?minutes}
mkdir -p "$out"; csv="$out/samples.csv"
echo "t,produced,landed,max_latency_s,lag,anon_mib,state_bytes,published_windows,healthz" > "$csv"
q() { docker run --rm --network "container:$name" curlimages/curl:latest -s --max-time 8 "http://127.0.0.1:5000/debug?sql=$(python3 -c 'import sys,urllib.parse;print(urllib.parse.quote(sys.argv[1]))' "$1")" 2>/dev/null | python3 -c 'import json,sys; d=json.load(sys.stdin); r=d.get("rows") or d.get("data") or [[None]]; print(r[0][0] if r and r[0] else "")' 2>/dev/null; }
for ((i=0; i<minutes; i++)); do
  t=$(date -u +%H:%M:%SZ)
  produced=$(grep -o 'total=[0-9]*' "$out/profile.log" 2>/dev/null | tail -1 | cut -d= -f2)
  landed=$(q "SELECT count(*) FROM landed")
  lat=$(q "SELECT coalesce(max(epoch(landed_at - sent_at)),0) FROM landed WHERE landed_at > now() - INTERVAL '2' MINUTE")
  committed=$(q "SELECT coalesce(max(\"offset\"),-1) FROM sqlflow_offsets")
  lag=$(( ${produced:-0} - 1 - ${committed:-0} ))
  anon=$(docker exec "$name" sh -c 'awk "/^anon /{print int(\$2/1048576)}" /sys/fs/cgroup/memory.stat 2>/dev/null' )
  state=$(docker exec "$name" sh -c 'stat -c %s /conf/state/slow.db 2>/dev/null')
  pub=$(q "SELECT count(*) FROM published")
  hz=$(docker run --rm --network "container:$name" curlimages/curl:latest -s -o /dev/null -w '%{http_code}' --max-time 5 http://127.0.0.1:8000/healthz 2>/dev/null)
  echo "$t,${produced:-},${landed:-},${lat:-},${lag},${anon:-},${state:-},${pub:-},${hz:-}" | tee -a "$csv"
  sleep 60
done
```

The `/debug?sql=` response shape is whatever `internal/cli/run/debug.go` returns; the executor reads that file and fixes the one-line JSON extraction in `q` to match, and notes the shape in SKILL.md.

- [ ] **Step 4: The verdict**

Create `.claude/skills/slow-soak/verdict.py`:

```python
"""Judge a slow-soak samples.csv. Exit 1 on any failed rule, printing each."""
import csv, sys
rows = list(csv.DictReader(open(sys.argv[1])))
interval = int(sys.argv[2]) if len(sys.argv) > 2 else 30
fails = []
def f(x, d=0.0):
    try: return float(x)
    except (TypeError, ValueError): return d
if not rows: sys.exit("no samples")
# 1. latency: never more than the interval plus thirty seconds
worst = max(f(r["max_latency_s"]) for r in rows)
if worst > interval + 30: fails.append(f"latency {worst:.0f}s exceeds interval+30")
# 2. lag: after the surge, never more than one interval's worth of arrivals; at a trickle that is one message
tail = rows[len(rows)//4:]
if any(f(r["lag"], 0) > 1 for r in tail if r["produced"]): fails.append("offset lag above one interval after the surge")
# 3. memory: end of run within 10% of the pre-surge working set
first, last = f(rows[0]["anon_mib"]), f(rows[-1]["anon_mib"])
if first and last > first * 1.10: fails.append(f"working set {last:.0f} MiB ended more than 10% above start {first:.0f}")
# 4. state file: must not grow through a silence (any 30 consecutive samples with no landed change)
for i in range(30, len(rows)):
    win = rows[i-30:i+1]
    if len({r["landed"] for r in win}) == 1 and f(win[-1]["state_bytes"]) > f(win[0]["state_bytes"]) * 1.05:
        fails.append(f"state file grew through a silence around sample {i}"); break
# 5. windows: every bucket with landed rows is published by the end
if f(rows[-1]["published_windows"]) == 0: fails.append("no window was ever published")
# 6. health: never red
if any(r["healthz"] not in ("200", "") for r in rows): fails.append("healthz went red")
print("\n".join(fails) if fails else f"PASS over {len(rows)} samples; worst latency {worst:.0f}s, working set {first:.0f} to {last:.0f} MiB")
sys.exit(1 if fails else 0)
```

Rule 5 is deliberately coarse; the executor tightens it to "published count equals the number of distinct minute buckets landed" once the pipeline shape from Step 1 is settled.

- [ ] **Step 5: SKILL.md**

Create `.claude/skills/slow-soak/SKILL.md`:

```markdown
---
name: slow-soak
description: Use when a pipeline must be shown to stay live and correct under slow, bursty, or absent traffic for hours: latency on a trickle, the tail of a surge, idle commits, window closes on silence, and health reporting.
---

# Slow soak

Throughput proves nothing about a stream that trickles, stops, or never
starts. This runs a scripted profile against a real broker for hours and
judges seven things once a minute. One line per sample; everything else in
files.

## Run it

    cp -r .claude/skills/slow-soak /tmp/slow-soak && cd /tmp/slow-soak
    make start-backing-services
    docker run -d --name sqlflow-slow --network dev_default -p 8000:8000 \
      -v "$PWD":/conf -e SQLFLOW_TOPIC=slow-$(date +%s) \
      turbolytics/sql-flow:<tag> run /conf/slow.yml --metrics=prometheus --with-http-debug
    ./profile.sh kafka1 $SQLFLOW_TOPIC full > soak-full/profile.log 2>&1 &
    ./sample.sh sqlflow-slow soak-full 160
    python3 verdict.py soak-full/samples.csv 30

`short` in place of `full` runs five minutes a phase for a dry run; give
`sample.sh` 25 minutes for it. Copy the directory first: a checkout under a
running soak has bitten before.

## What a failure means

- Latency above the interval plus thirty: the flush ticker is not firing,
  or a flush is slow. Look at commit latency in /metrics.
- Lag above one interval after the surge: offsets are not committing per
  flush. Look at the state commit count.
- Working set ending above where it started: something held from the surge.
  Switch to memory-soak for the decomposition.
- State file growing through a silence: empty commits are not empty.
- No window published, or healthz red: read /stats progress; a stuck commit
  clock is the engine, a moving one with no windows is the predicate.
```

- [ ] **Step 6: Dry run**

From a scratchpad copy, run the `short` profile against the branch's image (`make sqlflow-image` gives `turbolytics/sql-flow:<git describe>`), sample for 25 minutes, and run the verdict. Expected: `PASS over 25 samples`. Fix whatever the dry run shows before the full run. Record the dry run's summary line in the commit message.

- [ ] **Step 7: Commit**

```bash
git add .claude/skills/slow-soak/
git commit -m "slow-soak: a scripted traffic profile, sampled once a minute, judged at the end

Surge, silence, trickle with gaps, silence. Latency, offset lag, working
set, state file size, window closes and /healthz, one line per sample,
seven rules at the end. Dry run: <summary line>."
```

---

### Task 9: PR, CI, coverage matrix, the full soak

**Files:**
- Modify: `docs/coverage/matrix.json`, `docs/coverage/matrix.md` (from CI artifacts only)

- [ ] **Step 1: Push and open the PR**

```bash
git push -u origin feat/slow-data-confidence
gh pr create --title "Slow-moving data: liveness proven under trickle, surge, and silence" --body-file - <<'EOF'
## Why

Every proof the engine carried was taken under load. Nothing showed what happens when a stream trickles, stops, or never starts. One of those was a known defect: the stream-clock window predicate from #234 holds a quiet stream's last window open until data a full grace newer arrives.

## What

- `sqlflow_progress`, a one-row engine table beside `sqlflow_offsets`: when the newest batch arrived, when state last committed, messages consumed. Written inside the state transaction; an idle tick moves the commit clock only.
- The window predicate gains an idleness branch that reads it. A quiet stream closes every open window a grace after its last row. A replay still closes each window once. The four shipped examples change.
- `/stats` reports progress ages; `/healthz` answers 200 while the commit clock is younger than three intervals and 503 with the age otherwise. Idle is healthy, stuck is not.
- The `flush_interval_seconds: 0` invariant was wrong: the run command already defaults it. Pinned by a test, registry corrected and marked enforced.
- Seven cells: trickle latency, surge tail, idle commits keep the file flat, quiet stream closes its window, replay stays whole, Kafka survives idle past its session timeout, WebSocket survives a quiet server, healthz tells idle from stuck.
- A `slow-soak` skill: scripted surge, silence, trickle, silence over about two and a half hours, seven rules judged at the end.

## Soak

<the full profile's verdict line and the samples summary, filled from Step 4>

Spec: `docs/superpowers/specs/2026-09-10-slow-data-confidence-design.md`.
EOF
```

- [ ] **Step 2: CI, then the matrix from the artifacts**

Run: `gh pr checks --watch`. Expected: green. Then:

```bash
run=$(gh run list --branch feat/slow-data-confidence --limit 1 --json databaseId --jq '.[0].databaseId')
rm -rf .coverage && mkdir -p .coverage
for a in report-unit report-integration report-release; do gh run download "$run" -n "$a" -D "/tmp/cov-$a"; done
cp /tmp/cov-report-unit/go.json /tmp/cov-report-integration/go-integration.json /tmp/cov-report-release/pytest.json .coverage/
make coverage-write
git add docs/coverage/matrix.json docs/coverage/matrix.md
git commit -m "coverage: matrix from the slow-data CI run"
git push
```

- [ ] **Step 3: Build the image and run the full soak**

From a scratchpad copy of `.claude/skills/slow-soak`, against the PR branch's image, run the full profile and sample for 160 minutes. Expected: `PASS over 160 samples`. Paste the verdict line and the min, max of `max_latency_s`, `anon_mib`, and `state_bytes` into the PR body.

- [ ] **Step 4: Merge when the user says so**

The user merges. Not before the soak verdict is in the PR.

---

### Task 10: Docs on turbolytics.io

**Files (turbolytics.io repo, a branch off main):**
- Modify: `src/content/docs/sqlflow/tutorials/tumbling-window-postgres.md`, `src/content/docs/sqlflow/tutorials/bluesky-firehose.md` (the predicate and its explanation)
- Modify: `src/content/docs/sqlflow/introduction/configuration.md` (after the `flush_interval_seconds` paragraph at ~line 110)
- Modify: `src/data/benchmark.ts`, `src/pages/products/sqlflow/benchmarks.astro` (a "Slow data" section)

- [ ] **Step 1: The predicate in the tutorials**

Wherever a tutorial shows the collect and delete predicates, replace them with the two-branch form from Task 4 and add, once, after the explanation of the stream clock:

```markdown
A stream that goes quiet never moves its own clock, so the stream clock alone would hold the last window open until data a full grace newer arrived. The second branch closes every open window after a grace period of silence. It reads `sqlflow_progress`, the engine's one-row liveness table, whose `last_arrival` is when the newest batch was written.
```

- [ ] **Step 2: Configuration page**

After the `flush_interval_seconds` paragraph, add:

````markdown
### Liveness

sqlflow keeps one row of liveness in `sqlflow_progress`: `last_arrival`, when the newest batch was written; `last_commit`, when state last committed; and `messages`. A batch moves all three. An idle tick moves `last_commit` only. Your SQL can read it, and the shipped tumbling window examples do.

`/stats` on port 8000 reports the same facts with their ages in seconds. `/healthz` returns 200 while `last_commit` is younger than three flush intervals, and 503 with the age otherwise:

```
{"status":"stuck","commit_age_seconds":91,"interval_seconds":30}
```

Idle is healthy. A pipeline that receives nothing still commits on every interval.
````

- [ ] **Step 3: Benchmarks page**

Add to `src/data/benchmark.ts`:

```ts
/** The slow-soak verdict: a scripted profile against a real broker. Fill from the run. */
export const slowSoak = {
  measured: '2026-09-1x',
  profile: 'five minutes at 1,000/s, an hour of silence, an hour at one message per 90 seconds with gaps, thirty minutes of silence',
  samples: '160, once a minute',
  worstLatency: '<s>',
  workingSet: '<start> to <end> MiB',
  stateFile: 'flat through both silences',
  windows: 'every bucket published within a grace of its last row',
  healthz: '200 at every sample',
} as const;
```

and a section in `benchmarks.astro` after "Memory under load", eyebrow "Slow data", heading "Live under a trickle. Live after a surge.", with one card per rule and the profile in the lede. Fill every value from the soak; leave no `<...>`.

- [ ] **Step 4: Build, test, commit, push held**

Run `npm run build && npm test`, the em dash check and the collapsed-space check from the benchmarks work. Commit on the branch; the push is the user's call.

---

## Self-review

**Spec coverage.** Progress table and snapshot: Task 1. Trickle and surge cells C1, C2: Task 2. Idle file growth C3: Task 3. Window predicate and C4 both halves, the four examples: Task 4. `/stats` progress and `/healthz`, C7: Task 5. Wiring, the corrected invariant, C6: Task 6. Idle survival C5: Task 7. The soak skill with its profile, sampler, verdict, dry run: Task 8. PR, matrix from CI, full soak: Task 9. Docs, all three surfaces: Task 10. Out of scope items have no task.

**Placeholders.** The `<...>` values in Task 9's PR body and Task 10's `slowSoak` are filled from the soak before the step runs, and both steps say so. Three points name a helper the executor must locate rather than invent (`beginTx`, `exec`, `rowCount`, `newTestTumblingWith`, the `/debug` JSON shape) and each says exactly which file to mirror. Nothing says "TBD".

**Type consistency.** `core.Progress{LastArrival, LastCommit, Messages}` in Tasks 1, 4, 5, 6. `progressSaver.Record(ctx, Progress) error` in Tasks 1 and 3. `newHTTPMux(registry, stats, progress, interval, now)` in Tasks 5 and 6. `flushIntervalFor(int) time.Duration` in Task 6 only. The predicate text is identical in Task 4's test, the four examples, Task 8's config and Task 10's docs, apart from the grace.

**One decision made in review.** The spec put progress inside the state transaction; a stateless pipeline has no transaction. Task 1 records at the top of `commitState`, before its early return, so both modes record, and Task 6 gives the stateless store its own connection on the in-memory database. The window predicate reads committed data in the state-file case and autocommitted data otherwise, which is the same fact either way.
