# Process contract implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Bound the shutdown with one configurable deadline that exits 15 when it passes, and make `/healthz` report starting, healthy, degraded or failed.

**Architecture:** `core.DrainBudget` is one deadline shared by the turbine's final batch, the managers' final poll and run's state syncs. The turbine classifies a drain that ran out of time as `system.lifecycle.drain_incomplete`, which `errs.ExitCode` maps to 15. A `health` struct in `internal/cli/run` collects failures and in-flight retries, and one pure function turns it plus the progress snapshot into a status. Both properties are proven in the conformance harness and enforced in the registry.

**Tech Stack:** Go 1.2x, cobra, zeebo/assert, the conformance harness in `internal/conformance`, the coverage registry under `docs/coverage`, pytest for tooling and release tests, Astro for the site.

**Spec:** `docs/superpowers/specs/2026-09-12-process-contract-design.md`

## Global Constraints

- No em dashes anywhere: code comments, docs, commit messages, YAML.
- No attribution lines in commits or PRs.
- Every Go test calls `coverage.Covers(t, ...)` first in its body.
- Comments and docs in Google Technical Writing One style. Plain declarative sentences.
- `uv run python`, never bare `python3`.
- Status files under `docs/coverage/status/` are regenerated from CI's report artifacts (`gh run download <id> -n report-unit -n report-integration -n report-release`, then `make coverage-write`), never from a local run.
- The worktree is `$S/sqlflow-161` on branch `feat/process-contract`, where `S=/private/tmp/claude-501/-Users-danielmican-code-github-com-turbolytics-turbolytics-io/3d504d24-b5b3-4502-848a-732b4e380d07/scratchpad`. Never edit `/Users/danielmican/code/github.com/turbolytics/sql-flow`.
- Site work happens in `/Users/danielmican/code/github.com/turbolytics/turbolytics.io` on a new branch `docs/process-contract` from `main`. The working tree there carries an unrelated dirty edit to `tumbling-window-postgres.md`; do not commit it.
- Run `gofmt -l internal/ cmd/` before every Go commit.

---

## File structure

sql-flow:

- Create `internal/core/drain.go`: `DrainBudget`, `DefaultDrainDeadline`, `WithDrainBudget`.
- Create `internal/core/drain_test.go`.
- Modify `internal/core/turbine.go`: the `drain` field, the `ctx.Done` branch, error stamps in `recordError`, the two new `Progress` fields.
- Modify `internal/core/progress.go`: `Progress` gains `LastError` and `Errors`.
- Modify `internal/errs/registry.go`, `internal/errs/exit.go`, `internal/errs/testdata/codes.golden`, `internal/errs/errs_test.go`.
- Modify `internal/managers/tumbling.go`: `WithDrainBudget`, final poll on the budget.
- Modify `internal/sinks/retry.go`, `internal/sinks/init.go`: `onSettle`, `RetryEvents`, `WithRetryEvents`.
- Modify `internal/config/config.go`, `internal/validate/schemas/config.json`, `internal/cli/testdata/config_example.golden`: `drain_deadline_seconds`.
- Create `internal/validate/drain.go` and `internal/validate/drain_test.go`: the `pipeline.drain_deadline` check.
- Modify `internal/validate/validate.go`: call the check.
- Create `internal/cli/run/health.go` and `internal/cli/run/health_status_test.go`.
- Modify `internal/cli/run/metrics.go`, `internal/cli/run/health_test.go`, `internal/cli/run/metrics_test.go`: the new mux parameter and statuses.
- Modify `internal/cli/run/root.go`, `internal/cli/run/managers.go`: build the budget, the health, wire both.
- Modify `internal/cli/exit_test.go`: the exit 15 mapping.
- Modify `internal/conformance/pipeline.go`, `internal/conformance/manager.go`: the two `drain.bounded` checks and the hanging sink.
- Modify `docs/coverage/invariants.yml`, `docs/coverage/features.yml`.
- Modify `tests/release/test_image.py`: `EXIT_DRAIN_INCOMPLETE`, the health release test.
- Modify `CHANGELOG.md`.

turbolytics.io:

- Create `src/content/docs/sqlflow/operations/running-in-production.md`.
- Modify `src/content/docs/sqlflow/introduction/configuration.md`, `src/content/docs/sqlflow/operations/handling-errors.md`, `tests/build.test.ts`.

---

### Task 1: The taxonomy code and exit code 15

**Files:**
- Modify: `internal/errs/registry.go`
- Modify: `internal/errs/exit.go`
- Modify: `internal/errs/testdata/codes.golden`
- Test: `internal/errs/errs_test.go`

**Interfaces:**
- Produces: `errs.CodeDrainIncomplete Code = "system.lifecycle.drain_incomplete"`, `errs.ExitDrainIncomplete = 15`.

- [ ] **Step 1: Write the failing test**

Append to `internal/errs/errs_test.go`:

```go
// A drain that ran out of time is its own exit code. It is retryable, because
// nothing unwritten was committed and the next start replays it; the code
// exists so an operator can see the tail was replayed rather than written.
func TestLifecycleExitCodes_DrainIncompleteExitsFifteen(t *testing.T) {
	coverage.Covers(t, "lifecycle.exit_codes")
	err := New(CodeDrainIncomplete, "drain deadline 1s reached")

	assert.Equal(t, ExitDrainIncomplete, ExitCode(err))
	assert.Equal(t, 15, ExitDrainIncomplete)
	assert.That(t, Retryable(ExitDrainIncomplete))

	def, ok := Lookup(CodeDrainIncomplete)
	assert.That(t, ok)
	assert.That(t, strings.Contains(def.Action, "drain_deadline_seconds"))
}
```

- [ ] **Step 2: Run it**

Run: `cd $S/sqlflow-161 && go test ./internal/errs/ -run DrainIncomplete`
Expected: FAIL, `undefined: CodeDrainIncomplete`.

- [ ] **Step 3: Add the code, the definition and the exit code**

In `internal/errs/registry.go`, after the `CodeBatchInternal` line:

```go
	// Lifecycle: the process stopping. A drain that ran out of time is not a
	// sink failure and not a user error. Nothing unwritten was committed, so
	// nothing is lost; the code says the tail was replayed rather than
	// written.
	CodeDrainIncomplete Code = "system.lifecycle.drain_incomplete"
```

In the `registry` map, after the `CodeBatchInternal` entry:

```go
	CodeDrainIncomplete: {
		CodeDrainIncomplete,
		"The drain deadline passed before the buffered batch or the closed windows were written.",
		"Nothing is lost: what was not written was not committed, and the next start replays it. Raise pipeline.drain_deadline_seconds if the sink needs longer, or check the sink.",
	},
```

In `internal/errs/exit.go`, after `ExitStateCorrupt`:

```go
	// ExitDrainIncomplete marks a stop that ran out of time. Retryable:
	// nothing unwritten was committed, and the next start replays it. The
	// code exists so an operator can see that the tail of the stream was
	// replayed rather than written.
	ExitDrainIncomplete = 15
```

Add `CodeDrainIncomplete: ExitDrainIncomplete,` to `exitCodes`.

- [ ] **Step 4: Update the golden and run the package**

Run: `cd $S/sqlflow-161 && UPDATE_GOLDEN=1 go test ./internal/errs/ -run RegistryIsAppendOnly && go test ./internal/errs/`
Expected: PASS. `git diff internal/errs/testdata/codes.golden` shows one added line and no removals.

- [ ] **Step 5: Commit**

```bash
git add internal/errs
git commit -m "errs: system.lifecycle.drain_incomplete exits 15"
```

---

### Task 2: `core.DrainBudget` and the bounded final batch

**Files:**
- Create: `internal/core/drain.go`
- Create: `internal/core/drain_test.go`
- Modify: `internal/core/turbine.go` (the struct, `NewTurbine`, the `ctx.Done` branch)

**Interfaces:**
- Produces: `core.NewDrainBudget(d time.Duration) *DrainBudget`, `(*DrainBudget).Context() context.Context`, `(*DrainBudget).Exceeded() bool`, `(*DrainBudget).Stop()`, `core.DefaultDrainDeadline = 30 * time.Second`, `core.WithDrainBudget(b *DrainBudget) TurbineOption`.
- Consumes: `errs.CodeDrainIncomplete` from Task 1.

- [ ] **Step 1: Write the failing tests**

Create `internal/core/drain_test.go`:

```go
package core

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/turbolytics/sql-flow/internal/errs"
	"github.com/zeebo/assert"
)

// The budget is one clock. Every caller after the first gets the same
// context, so the turbine's final batch and the managers' final poll spend
// the same seconds rather than each getting a fresh deadline.
func TestLifecycleDrain_BudgetIsOneClock(t *testing.T) {
	coverage.Covers(t, "lifecycle.drain")
	b := NewDrainBudget(50 * time.Millisecond)
	defer b.Stop()

	assert.That(t, !b.Exceeded())
	first := b.Context()
	second := b.Context()
	assert.That(t, first == second)

	<-first.Done()
	assert.That(t, b.Exceeded())
	assert.That(t, errors.Is(first.Err(), context.DeadlineExceeded))
}

// hangingSink blocks in Flush until its context ends. It is the sink a drain
// deadline exists for: one that neither succeeds nor fails on its own.
type hangingSink struct {
	mu      sync.Mutex
	flushes int
}

func (s *hangingSink) WriteTable(context.Context, arrow.Table) error { return nil }

func (s *hangingSink) Flush(ctx context.Context) error {
	s.mu.Lock()
	s.flushes++
	s.mu.Unlock()
	<-ctx.Done()
	return ctx.Err()
}

// A sink that never answers must not hold the process past the deadline.
// The loop returns inside the budget with the drain code, and the rows stay
// unwritten and uncommitted for the next start to replay.
func TestLifecycleDrain_DeadlineBoundsTheFinalBatch(t *testing.T) {
	coverage.Covers(t, "lifecycle.drain")
	src := newBlockingSource(messages(10))
	sink := &hangingSink{}
	h := &drainHandler{wrote: make(chan struct{})}
	budget := NewDrainBudget(200 * time.Millisecond)
	defer budget.Stop()

	tb := NewTurbine(src, h, sink, 1000, time.Hour, &sync.Mutex{},
		PipelineErrorPolicies{}, WithDrainBudget(budget))

	ctx, cancel := context.WithCancel(context.Background())
	defer close(src.release)

	var (
		err  error
		done = make(chan struct{})
	)
	go func() {
		defer close(done)
		_, err = tb.ConsumeLoop(ctx, 0)
	}()

	for i := 0; i < 10; i++ {
		<-h.wrote
	}
	started := time.Now()
	cancel()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the drain outlived its deadline by more than 5s")
	}

	assert.That(t, time.Since(started) < 2*time.Second)
	assert.Error(t, err)
	assert.Equal(t, errs.CodeDrainIncomplete, errs.CodeOf(err))
	assert.That(t, budget.Exceeded())
}

// A sink that fails for its own reason during the drain keeps its own code.
// Only running out of time is a drain failure.
func TestLifecycleDrain_ASinkFailureInsideTheDeadlineKeepsItsCode(t *testing.T) {
	coverage.Covers(t, "lifecycle.drain")
	src := newBlockingSource(messages(3))
	sink := &fakeSink{flushErr: errs.New(errs.CodeSinkWriteFailed, "rejected")}
	h := &drainHandler{wrote: make(chan struct{})}
	budget := NewDrainBudget(5 * time.Second)
	defer budget.Stop()

	tb := NewTurbine(src, h, sink, 1000, time.Hour, &sync.Mutex{},
		PipelineErrorPolicies{}, WithDrainBudget(budget))

	ctx, cancel := context.WithCancel(context.Background())
	defer close(src.release)

	var (
		err  error
		done = make(chan struct{})
	)
	go func() {
		defer close(done)
		_, err = tb.ConsumeLoop(ctx, 0)
	}()
	for i := 0; i < 3; i++ {
		<-h.wrote
	}
	cancel()
	<-done

	assert.Equal(t, errs.CodeSinkWriteFailed, errs.CodeOf(err))
	assert.That(t, !budget.Exceeded())
}
```

Check `fakeSink` in `internal/core/turbine_test.go` for the field that makes Flush fail. If it is not `flushErr`, use the field that exists.

- [ ] **Step 2: Run them**

Run: `cd $S/sqlflow-161 && go test ./internal/core/ -run 'LifecycleDrain_(Budget|Deadline|ASink)'`
Expected: FAIL, `undefined: NewDrainBudget`.

- [ ] **Step 3: Write `internal/core/drain.go`**

```go
package core

import (
	"context"
	"sync"
	"time"
)

// DefaultDrainDeadline bounds a shutdown that the config did not bound.
// Thirty seconds is Kubernetes' default terminationGracePeriodSeconds, so a
// pipeline with no setting at all still finishes or fails before the
// supervisor stops waiting.
const DefaultDrainDeadline = 30 * time.Second

// DrainBudget is one deadline for everything a shutdown does.
//
// After a SIGTERM four things reach DuckDB or a sink: the turbine's final
// batch, a state sync, each manager's final poll and a second sync. A deadline
// per step would let the shutdown take four deadlines; a supervisor gives it
// one. So the budget starts its clock on the first call to Context and hands
// the same context to every caller after that.
type DrainBudget struct {
	deadline time.Duration
	once     sync.Once
	ctx      context.Context
	cancel   context.CancelFunc
}

// NewDrainBudget makes a budget whose clock has not started.
func NewDrainBudget(deadline time.Duration) *DrainBudget {
	if deadline <= 0 {
		deadline = DefaultDrainDeadline
	}
	return &DrainBudget{deadline: deadline}
}

// Context starts the clock on the first call and returns the same context on
// every call after it.
func (b *DrainBudget) Context() context.Context {
	b.once.Do(func() {
		b.ctx, b.cancel = context.WithTimeout(context.Background(), b.deadline)
	})
	return b.ctx
}

// Exceeded reports whether the deadline has passed. False before the clock
// starts.
func (b *DrainBudget) Exceeded() bool {
	if b.ctx == nil {
		return false
	}
	return b.ctx.Err() != nil
}

// Deadline is what the budget was built with, for log lines and errors.
func (b *DrainBudget) Deadline() time.Duration { return b.deadline }

// Stop releases the timer. Safe before the clock starts.
func (b *DrainBudget) Stop() {
	if b.cancel != nil {
		b.cancel()
	}
}

// WithDrainBudget bounds the turbine's final batch. A turbine built without
// one gets DefaultDrainDeadline.
func WithDrainBudget(b *DrainBudget) TurbineOption {
	return func(t *Turbine) { t.drain = b }
}
```

Note `Exceeded` reads `b.ctx` without the `once`. `Context` is called from the loop goroutine before anyone asks `Exceeded`; if the race detector objects, guard both with a `sync.Mutex` instead of `sync.Once`.

- [ ] **Step 4: Wire it into the turbine**

In `internal/core/turbine.go`, add to the `Turbine` struct beside `errorPolicy`:

```go
	// drain bounds the final batch after a cancel. Shared with the managers
	// and run's state syncs, so one deadline covers the whole shutdown.
	drain *DrainBudget
```

In `NewTurbine`, after the options loop, default it:

```go
	if t.drain == nil {
		t.drain = NewDrainBudget(DefaultDrainDeadline)
	}
```

Replace the `ctx.Done` branch's drain with:

```go
		case <-ctx.Done():
			t.logger.Info("context done, draining the consumer loop")
			t.running = false
			// The source delivered this batch, but nothing has written it yet.
			// Returning without it drops the tail of every graceful shutdown.
			//
			// The drain runs on the budget's context, not the cancelled one:
			// every step below reaches DuckDB and the sink, and the cancelled
			// ctx would fail the exact work the drain exists to finish. The
			// budget bounds it instead, because a supervisor gives a stop a
			// fixed time and then kills the process with nothing recorded.
			if numBatchMessages > 0 {
				drainCtx := t.drain.Context()
				if err := t.processBatch(drainCtx, numBatchMessages); err != nil {
					if drainCtx.Err() != nil {
						err = errs.Wrap(errs.CodeDrainIncomplete, err,
							"drain deadline %s reached with %d messages buffered",
							t.drain.Deadline(), numBatchMessages)
					}
					t.recordError(ctx, err, phaseSinkFlush, "error draining the final batch")
					return nil, err
				}
			}
			t.logThroughput()
			return t.stats, nil
```

- [ ] **Step 5: Run the core package with the race detector**

Run: `cd $S/sqlflow-161 && go test -short -race ./internal/core/`
Expected: PASS, including `TestLifecycleDrain_CancelDrainsTheBufferedBatch`, which now runs under the default budget.

- [ ] **Step 6: Commit**

```bash
git add internal/core/drain.go internal/core/drain_test.go internal/core/turbine.go
git commit -m "core: the final batch after a cancel runs on a drain budget"
```

---

### Task 3: The manager's final poll on the budget

**Files:**
- Modify: `internal/managers/tumbling.go`
- Test: `internal/managers/tumbling_test.go`

**Interfaces:**
- Produces: `managers.WithDrainBudget(b *core.DrainBudget) TumblingOption`.
- Consumes: `core.DrainBudget` from Task 2.

- [ ] **Step 1: Write the failing test**

Append to `internal/managers/tumbling_test.go`, using the helpers already there (`newTestConn` or whatever the file names its connection helper, `seedWindows`, `countRows`, and the sink double the failed-flush tests use). Read `TestManagerTumblingWindow__FinalPollOnShutdownPublishesAClosedWindow` at line 444 first and copy its setup:

```go
// The final poll runs on the shutdown budget. A sink that never answers
// must not hold the process past it, and the windows stay in the table for
// the next start.
func TestManagerTumblingWindow__FinalPollStopsAtTheDrainDeadline(t *testing.T) {
	coverage.Covers(t, "manager.tumbling_window")
	conn := newTestConn(t)
	seedWindows(t, conn)
	sink := &hangingSink{}
	budget := core.NewDrainBudget(200 * time.Millisecond)
	defer budget.Stop()

	m := NewTumbling(conn, collectSQL, deleteSQL, time.Hour, sink, &sync.Mutex{},
		WithDrainBudget(budget))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- m.Start(ctx) }()
	cancel()

	select {
	case err := <-done:
		assert.Error(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("the final poll outlived the drain deadline")
	}
	assert.Equal(t, int64(2), countRows(t, conn, "windows"))
}
```

Define `hangingSink` in the test file the same way as Task 2's. Replace `newTestConn`, `collectSQL`, `deleteSQL`, `"windows"` and the seeded count with the names and values the file already uses.

- [ ] **Step 2: Run it**

Run: `cd $S/sqlflow-161 && go test ./internal/managers/ -run FinalPollStopsAtTheDrainDeadline`
Expected: FAIL, `undefined: WithDrainBudget`.

- [ ] **Step 3: Add the option and use it**

In `internal/managers/tumbling.go`, add a field to `Tumbling`:

```go
	// drain bounds the final poll. Shared with the turbine, so one deadline
	// covers the whole shutdown.
	drain *core.DrainBudget
```

Default it in `NewTumbling` after the options loop:

```go
	if m.drain == nil {
		m.drain = core.NewDrainBudget(core.DefaultDrainDeadline)
	}
```

Add the option beside `WithLogger`:

```go
// WithDrainBudget bounds the final poll after a cancel.
func WithDrainBudget(b *core.DrainBudget) TumblingOption {
	return func(m *Tumbling) { m.drain = b }
}
```

In `Start`, replace `m.Poll(context.Background())` in the `ctx.Done` branch with `m.Poll(m.drain.Context())`.

- [ ] **Step 4: Run the package**

Run: `cd $S/sqlflow-161 && go test -short -race ./internal/managers/`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/managers
git commit -m "managers: the final poll runs on the drain budget"
```

---

### Task 4: Config key, schema, and the validate warning

**Files:**
- Modify: `internal/config/config.go`
- Modify: `internal/validate/schemas/config.json`
- Modify: `internal/cli/testdata/config_example.golden`
- Create: `internal/validate/drain.go`
- Create: `internal/validate/drain_test.go`
- Modify: `internal/validate/validate.go`
- Modify: `internal/cli/run/metrics.go` (`drainDeadlineFor`)

**Interfaces:**
- Produces: `config.Pipeline.DrainDeadlineSeconds int` (`yaml:"drain_deadline_seconds,omitempty"`), `run.drainDeadlineFor(seconds int) time.Duration`, validate check id `pipeline.drain_deadline`.

- [ ] **Step 1: Add the key**

In `internal/config/config.go`, after `FlushIntervalSeconds`:

```go
	// Longest a shutdown may take after SIGTERM: the final batch, the
	// managers' final poll and the state syncs share it. Absent means 30.
	// When it passes the process exits 15 and the next start replays what
	// was not written.
	DrainDeadlineSeconds int `yaml:"drain_deadline_seconds,omitempty"`
```

In `internal/validate/schemas/config.json`, after the `flush_interval_seconds` property in the pipeline block:

```json
        "drain_deadline_seconds": {
          "type": "integer",
          "minimum": 1,
          "description": "Longest a shutdown may take after SIGTERM: the final batch, the\nmanagers' final poll and the state syncs share it. Absent means 30.\nWhen it passes the process exits 15 and the next start replays what\nwas not written."
        },
```

Check how `config example` orders keys (schema order or struct order) by regenerating the golden: `UPDATE_GOLDEN=1 go test ./internal/cli/ -run Example`. Read the diff. It must add only the new key with its comment.

- [ ] **Step 2: Add `drainDeadlineFor`**

In `internal/cli/run/metrics.go`, after `flushIntervalFor`:

```go
// drainDeadlineFor is the one place the drain deadline is decided. Absent,
// zero and negative all mean the default, for the same reason as the flush
// interval: a shutdown the config forgot to bound is still bounded.
func drainDeadlineFor(seconds int) time.Duration {
	if seconds > 0 {
		return time.Duration(seconds) * time.Second
	}
	return core.DefaultDrainDeadline
}
```

Add to `internal/cli/run/metrics_test.go`, next to the flush interval test:

```go
func TestLifecycleDrain_DeadlineDefaultsWhenAbsent(t *testing.T) {
	coverage.Covers(t, "lifecycle.drain")
	assert.Equal(t, core.DefaultDrainDeadline, drainDeadlineFor(0))
	assert.Equal(t, core.DefaultDrainDeadline, drainDeadlineFor(-5))
	assert.Equal(t, 45*time.Second, drainDeadlineFor(45))
}
```

- [ ] **Step 3: Write the failing validate test**

Create `internal/validate/drain_test.go`:

```go
package validate

import (
	"context"
	"testing"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

const drainConfig = `pipeline:
  batch_size: 1
  drain_deadline_seconds: %d
  source:
    type: kafka
    kafka:
      brokers: ["localhost:9092"]
      group_id: g
      auto_offset_reset: earliest
      topics: ["t"]
  handler:
    type: handlers.InferredMemBatch
    sql: SELECT 1
  sink:
    type: clickhouse
    clickhouse:
      dsn: clickhouse://localhost:9000
      table: t
    retry:
      deadline_seconds: 20
`

// hasCheck reports whether the report carries the check at that status.
func hasCheck(rep Report, id string, status Status) bool {
	for _, c := range rep.Checks {
		if c.ID == id && c.Status == status {
			return true
		}
	}
	return false
}

// A drain deadline shorter than a sink's retry deadline is a choice, not a
// fault: the operator wants the process out. They are told what it costs.
func TestValidateSchema_ShortDrainDeadlineWarns(t *testing.T) {
	coverage.Covers(t, "validate.schema")
	rep, err := Validate(context.Background(), Request{
		Path: "drain.yml", Config: fmt.Sprintf(drainConfig, 5)})
	assert.NoError(t, err)
	assert.That(t, rep.OK)

	var warned bool
	for _, d := range rep.Diagnostics {
		if d.Severity == SeverityWarning && d.Code == "user.config.invalid" {
			warned = true
			assert.That(t, strings.Contains(d.Message, "drain_deadline_seconds"))
			assert.That(t, strings.Contains(d.Message, "retry.deadline_seconds"))
		}
	}
	assert.That(t, warned)
	assert.That(t, hasCheck(rep, "pipeline.drain_deadline", StatusWarn))
}

func TestValidateSchema_LongDrainDeadlinePasses(t *testing.T) {
	coverage.Covers(t, "validate.schema")
	rep, err := Validate(context.Background(), Request{
		Path: "drain.yml", Config: fmt.Sprintf(drainConfig, 60)})
	assert.NoError(t, err)
	assert.That(t, rep.OK)
	assert.That(t, hasCheck(rep, "pipeline.drain_deadline", StatusPass))
}
```

Add `fmt` and `strings` to the imports. Read `report.go` for the exact `Status` and `Severity` constant names; if there is no warning status for a check, use `StatusPass` for the check and keep the warning diagnostic.

- [ ] **Step 4: Run it**

Run: `cd $S/sqlflow-161 && go test ./internal/validate/ -run DrainDeadline`
Expected: FAIL, no `pipeline.drain_deadline` check.

- [ ] **Step 5: Write the check**

Create `internal/validate/drain.go`:

```go
package validate

import (
	"fmt"

	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/core"
	"github.com/turbolytics/sql-flow/internal/errs"
	"github.com/turbolytics/sql-flow/internal/sinks"
	"gopkg.in/yaml.v3"
)

// checkDrainDeadline warns when a sink's retry ladder can outlive the drain.
//
// A short drain deadline is a legitimate choice: the operator wants the
// process out in five seconds whatever the sink is doing. It is a warning
// because that choice has a cost they may not have priced: a drain that hits
// a retrying sink exits 15 before the ladder finishes, and the tail replays
// on the next start.
func checkDrainDeadline(rendered []byte, rep *Report) {
	var conf config.Conf
	if err := yaml.Unmarshal(rendered, &conf); err != nil {
		// The schema check has already reported the parse failure.
		rep.SetCheck("pipeline.drain_deadline", StatusSkipped,
			"the config did not parse, so there was no pipeline to check")
		return
	}

	drain := core.DefaultDrainDeadline
	if conf.Pipeline.DrainDeadlineSeconds > 0 {
		drain = time.Duration(conf.Pipeline.DrainDeadlineSeconds) * time.Second
	}

	type ladder struct {
		name  string
		retry *config.SinkRetry
	}
	ladders := []ladder{{"pipeline.sink", conf.Pipeline.Sink.Retry}}
	if conf.Pipeline.OnError != nil && conf.Pipeline.OnError.DLQ != nil {
		ladders = append(ladders, ladder{"pipeline.on_error.dlq", conf.Pipeline.OnError.DLQ.Retry})
	}
	if conf.Tables != nil {
		for _, table := range conf.Tables.SQL {
			if table.Manager != nil {
				ladders = append(ladders, ladder{
					fmt.Sprintf("tables.sql[%s].manager.sink", table.Name),
					table.Manager.Sink.Retry})
			}
		}
	}

	status := StatusPass
	for _, l := range ladders {
		policy := sinks.RetryPolicyFrom(l.retry)
		if policy.Deadline <= drain {
			continue
		}
		status = StatusWarn
		rep.Add(diagnostic(errs.CodeConfigInvalid, SeverityWarning, fmt.Sprintf(
			"pipeline.drain_deadline_seconds is %s and %s retry.deadline_seconds is %s, "+
				"so a drain that hits a retrying sink exits 15 before the ladder finishes",
			drain, l.name, policy.Deadline), nil))
	}
	rep.SetCheck("pipeline.drain_deadline", status, "")
}
```

Add the `time` import. Check whether `internal/validate` may import `internal/sinks` without a cycle (`go build ./...` says). If it cycles, copy the four default constants into `internal/config` as `DefaultSinkRetryDeadlineSeconds` and read them from there instead, and make `sinks.RetryPolicyFrom` use the config constant so there is one source.

In `internal/validate/validate.go`, after `checkSchema(rendered, &rep)`:

```go
	checkDrainDeadline(rendered, &rep)
```

Read `report.go` to confirm whether a warning-status check keeps `rep.OK` true. `Finish` decides `OK`; if it counts warning checks as failures, the test in Step 3 tells you, and the check should report `StatusPass` with the warning diagnostic instead.

- [ ] **Step 6: Run the packages**

Run: `cd $S/sqlflow-161 && go build ./... && go test -short ./internal/validate/ ./internal/cli/ ./internal/cli/run/ ./internal/config/`
Expected: PASS. If `TestValidate...` in `internal/cli/validate_test.go` compares text output against a golden, regenerate it the same way and read the diff.

- [ ] **Step 7: Commit**

```bash
git add internal/config internal/validate internal/cli
git commit -m "config: pipeline.drain_deadline_seconds, and validate warns when a retry ladder outlives it"
```

---

### Task 5: Retry events from the ladder

**Files:**
- Modify: `internal/sinks/retry.go`
- Modify: `internal/sinks/init.go`
- Test: `internal/sinks/retry_test.go`

**Interfaces:**
- Produces: `sinks.RetryEvents{Retry func(sinkType string, attempt int, err error); Settle func(sinkType string)}`, `sinks.WithRetryEvents(e RetryEvents) Option`, `retrying.onSettle func()`.

- [ ] **Step 1: Write the failing test**

Append to `internal/sinks/retry_test.go`:

```go
// The health endpoint needs to know when a ladder is running and when it has
// stopped, whichever way it stopped. Settle fires once per flush that
// retried at all, after success or after the last failure.
func TestSinkRetry_SettleFiresAfterALadderEitherWay(t *testing.T) {
	coverage.Covers(t, "sink.retry")

	settled := 0
	retries := 0
	run := func(failures int) error {
		sink := &flakySink{failures: failures, err: errors.New("connection reset by peer")}
		r := newRetrying(sink, testPolicy())
		r.onRetry = func(int, error) { retries++ }
		r.onSettle = func() { settled++ }
		assert.NoError(t, r.WriteTable(context.Background(), nil))
		return r.Flush(context.Background())
	}

	// Succeeds on the second attempt: one retry, one settle.
	assert.NoError(t, run(1))
	assert.Equal(t, 1, retries)
	assert.Equal(t, 1, settled)

	// Never succeeds: the ladder is spent, and it still settles.
	assert.Error(t, run(99))
	assert.Equal(t, 2, settled)

	// Succeeds first time: no ladder ran, nothing to settle.
	assert.NoError(t, run(0))
	assert.Equal(t, 2, settled)
}
```

- [ ] **Step 2: Run it**

Run: `cd $S/sqlflow-161 && go test ./internal/sinks/ -run SettleFires`
Expected: FAIL, `r.onSettle undefined`.

- [ ] **Step 3: Add `onSettle`**

In `retrying`, after `onRetry`:

```go
	// onSettle reports that a ladder which retried at least once has
	// stopped, whether it delivered or gave up. The health endpoint clears
	// its "retrying" state on it.
	onSettle func()
```

Set `onSettle: func() {}` in `newRetrying`. In `Flush`, wrap the loop so that every return after the first retry settles:

```go
	retried := false
	defer func() {
		if retried {
			r.onSettle()
		}
	}()

	for attempt := 1; ; attempt++ {
		...
		r.onRetry(attempt, err)
		retried = true
		...
	}
```

Place `retried = true` on the line after `r.onRetry(attempt, err)`.

- [ ] **Step 4: Add the option in `init.go`**

```go
// RetryEvents is told when a sink's retry ladder runs. Retry fires per failed
// attempt that will be tried again; Settle fires once when a ladder that
// retried at all stops, whether it delivered or gave up.
type RetryEvents struct {
	Retry  func(sinkType string, attempt int, err error)
	Settle func(sinkType string)
}

// WithRetryEvents wires the ladder's events to a listener. The retry counter
// records regardless; this is for the health endpoint.
func WithRetryEvents(e RetryEvents) Option {
	return func(o *options) { o.retryEvents = e }
}
```

Add `retryEvents RetryEvents` to `options`. In `New`, after `r.onRetry = retryCounter(...)`:

```go
	if o.retryEvents.Retry != nil {
		counter := r.onRetry
		notify := o.retryEvents.Retry
		r.onRetry = func(attempt int, err error) {
			counter(attempt, err)
			notify(sink.Type, attempt, err)
		}
	}
	if o.retryEvents.Settle != nil {
		notify := o.retryEvents.Settle
		r.onSettle = func() { notify(sink.Type) }
	}
```

- [ ] **Step 5: Run the package**

Run: `cd $S/sqlflow-161 && go test -short -race ./internal/sinks/`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/sinks
git commit -m "sinks: the retry ladder reports when it settles"
```

---

### Task 6: Progress carries the error clock

**Files:**
- Modify: `internal/core/progress.go`
- Modify: `internal/core/turbine.go` (`recordError`, `Progress()`)
- Test: `internal/core/progress_test.go`

**Interfaces:**
- Produces: `core.Progress.LastError time.Time`, `core.Progress.Errors int64`.

- [ ] **Step 1: Write the failing test**

Append to `internal/core/progress_test.go`:

```go
// The health endpoint calls a pipeline degraded when it recorded an error
// inside the last interval, so the snapshot has to carry when that was.
func TestCoreConsumeLoop_ProgressRecordsTheLastError(t *testing.T) {
	coverage.Covers(t, "core.consume_loop")
	src := &fakeSource{batches: [][]Message{{{Value: []byte("bad")}, {Value: []byte("ok")}}}}
	h := &failingHandler{failWriteOn: "bad"}
	sink := &fakeSink{}
	tb := NewTurbine(src, h, sink, 2, time.Second, &sync.Mutex{},
		PipelineErrorPolicies{Policy: PolicyIgnore})

	before := time.Now().UTC()
	_, err := tb.ConsumeLoop(context.Background(), 0)
	assert.NoError(t, err)

	p := tb.Progress()
	assert.Equal(t, int64(1), p.Errors)
	assert.That(t, !p.LastError.Before(before))
}
```

- [ ] **Step 2: Run it**

Run: `cd $S/sqlflow-161 && go test ./internal/core/ -run ProgressRecordsTheLastError`
Expected: FAIL, `p.Errors undefined`.

- [ ] **Step 3: Add the fields**

In `internal/core/progress.go`:

```go
type Progress struct {
	LastArrival time.Time
	LastCommit  time.Time
	// LastError is when the loop last recorded an error, and Errors is how
	// many it has recorded. In memory only: they describe this process, not
	// the durable state, so the progress table does not carry them.
	LastError time.Time
	Errors    int64
	Messages  int64
}
```

Check that `ProgressStore.Record` builds its UPDATE from named fields rather than the whole struct, so the two new fields do not reach SQL. If it uses the struct wholesale, leave the SQL as it was and pass only the three columns it wrote before.

In `turbine.go`, add two atomics to the struct beside `commits`:

```go
	// lastErrorUnixNano and errorCount feed Progress without taking the
	// lock: recordError runs on paths that may already hold it.
	lastErrorUnixNano atomic.Int64
	errorCount        atomic.Int64
```

At the top of `recordError`:

```go
	t.lastErrorUnixNano.Store(time.Now().UnixNano())
	t.errorCount.Add(1)
```

In `Progress()`:

```go
func (t *Turbine) Progress() Progress {
	t.lock.Lock()
	p := t.snapshot
	t.lock.Unlock()
	if ns := t.lastErrorUnixNano.Load(); ns != 0 {
		p.LastError = time.Unix(0, ns).UTC()
	}
	p.Errors = t.errorCount.Load()
	return p
}
```

- [ ] **Step 4: Run the package**

Run: `cd $S/sqlflow-161 && go test -short -race ./internal/core/`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/core
git commit -m "core: Progress carries the last error and the error count"
```

---

### Task 7: The health status function and the endpoint

**Files:**
- Create: `internal/cli/run/health.go`
- Create: `internal/cli/run/health_status_test.go`
- Modify: `internal/cli/run/metrics.go`
- Modify: `internal/cli/run/health_test.go`
- Modify: `internal/cli/run/metrics_test.go` (every `newHTTPMux` call gains a `nil` health argument)
- Modify: `docs/coverage/features.yml`

**Interfaces:**
- Produces: `type health struct`, `newHealth() *health`, `(*health).Fail(err error)`, `(*health).Retry(sinkType string, attempt int, err error)`, `(*health).Settle(sinkType string)`, `(*health).Snapshot() healthSnapshot`, `type healthFunc func() healthSnapshot`, `healthStatus(p core.Progress, commitAge float64, snap healthSnapshot, interval time.Duration, now time.Time) (status, reason string, httpCode int)`.
- `newHTTPMux(registry, stats, collect, progress, health healthFunc, interval, now)`.

- [ ] **Step 1: Add the feature**

In `docs/coverage/features.yml`, after `lifecycle.exit_codes`:

```yaml
  - id: lifecycle.health
    description: /healthz reports starting, healthy, degraded or failed, with a reason.
    requires: [unit, release]
```

- [ ] **Step 2: Write the failing status test**

Create `internal/cli/run/health_status_test.go`:

```go
package run

import (
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/turbolytics/sql-flow/internal/core"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

// The four states, one table. The first matching rule wins: a recorded
// failure beats everything, then the commit clock, then a retry in flight,
// then a recent error, then a pipeline that has not committed yet.
func TestLifecycleHealth_StatusTable(t *testing.T) {
	coverage.Covers(t, "lifecycle.health")
	now := time.Date(2026, 9, 12, 12, 0, 0, 0, time.UTC)
	interval := 30 * time.Second
	recent := core.Progress{LastCommit: now.Add(-10 * time.Second)}

	cases := []struct {
		name       string
		p          core.Progress
		commitAge  float64
		snap       healthSnapshot
		status     string
		reason     string
		httpStatus int
	}{
		{"healthy", recent, 10, healthSnapshot{}, "healthy", "", http.StatusOK},
		{"starting", core.Progress{}, 5, healthSnapshot{}, "starting", "no commit yet", http.StatusOK},
		{"stuck is failed", recent, 91, healthSnapshot{}, "failed", "no commit for 91s", http.StatusServiceUnavailable},
		{"never committed past the grace", core.Progress{}, 120, healthSnapshot{}, "failed", "no commit for 120s", http.StatusServiceUnavailable},
		{"retrying", recent, 10, healthSnapshot{retrying: map[string]int{"clickhouse": 2}},
			"degraded", "sink clickhouse is retrying, attempt 2", http.StatusOK},
		{"recent error", core.Progress{LastCommit: recent.LastCommit, LastError: now.Add(-5 * time.Second), Errors: 3},
			10, healthSnapshot{}, "degraded", "3 errors recorded, last 5s ago", http.StatusOK},
		{"old error", core.Progress{LastCommit: recent.LastCommit, LastError: now.Add(-time.Hour), Errors: 3},
			10, healthSnapshot{}, "healthy", "", http.StatusOK},
		{"failure wins", recent, 10, healthSnapshot{failure: errors.New("[system.sink.write_failed] rejected")},
			"failed", "[system.sink.write_failed] rejected", http.StatusServiceUnavailable},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			status, reason, code := healthStatus(c.p, c.commitAge, c.snap, interval, now)
			assert.Equal(t, c.status, status)
			assert.Equal(t, c.reason, reason)
			assert.Equal(t, c.httpStatus, code)
		})
	}
}

// Fail keeps the first failure. A drain that then times out is a consequence,
// and the endpoint should name the cause.
func TestLifecycleHealth_FirstFailureIsKept(t *testing.T) {
	coverage.Covers(t, "lifecycle.health")
	h := newHealth()
	h.Fail(errors.New("first"))
	h.Fail(errors.New("second"))
	assert.Equal(t, "first", h.Snapshot().failure.Error())
}

func TestLifecycleHealth_RetryAndSettle(t *testing.T) {
	coverage.Covers(t, "lifecycle.health")
	h := newHealth()
	h.Retry("clickhouse", 1, errors.New("reset"))
	h.Retry("clickhouse", 2, errors.New("reset"))
	assert.Equal(t, 2, h.Snapshot().retrying["clickhouse"])
	h.Settle("clickhouse")
	assert.Equal(t, 0, len(h.Snapshot().retrying))
}
```

- [ ] **Step 3: Run it**

Run: `cd $S/sqlflow-161 && go test ./internal/cli/run/ -run LifecycleHealth`
Expected: FAIL, `undefined: healthStatus`.

- [ ] **Step 4: Write `internal/cli/run/health.go`**

```go
package run

import (
	"fmt"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/turbolytics/sql-flow/internal/core"
)

// health is what the process knows about itself that the progress snapshot
// does not: a failure that has stopped the loop, and a sink whose retry
// ladder is running.
type health struct {
	mu       sync.Mutex
	failure  error
	retrying map[string]int
}

func newHealth() *health {
	return &health{retrying: map[string]int{}}
}

// Fail records the failure that is stopping the process. The first one is
// kept: a drain that then runs out of time is a consequence, and the endpoint
// should name the cause.
func (h *health) Fail(err error) {
	if err == nil {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.failure == nil {
		h.failure = err
	}
}

// Retry records a ladder attempt in flight for one sink type.
func (h *health) Retry(sinkType string, attempt int, _ error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.retrying[sinkType] = attempt
}

// Settle clears the ladder for one sink type.
func (h *health) Settle(sinkType string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.retrying, sinkType)
}

// healthSnapshot is one read of health, safe to hand to the status function.
type healthSnapshot struct {
	failure  error
	retrying map[string]int
}

func (h *health) Snapshot() healthSnapshot {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := healthSnapshot{failure: h.failure, retrying: make(map[string]int, len(h.retrying))}
	for k, v := range h.retrying {
		out.retrying[k] = v
	}
	return out
}

// healthFunc reads the process's health. Nil means there is none to read.
type healthFunc func() healthSnapshot

// healthStatus is the table behind /healthz. The first matching rule wins.
//
// 200 means "do not restart" and 503 means "restart, or let it exit". A
// retrying sink and a recent error are both 200: restarting a pipeline whose
// sink is on its second attempt turns a blip into a rebalance.
func healthStatus(p core.Progress, commitAge float64, snap healthSnapshot,
	interval time.Duration, now time.Time) (status, reason string, httpCode int) {

	if snap.failure != nil {
		return "failed", snap.failure.Error(), http.StatusServiceUnavailable
	}
	if commitAge > float64(stuckIntervals)*interval.Seconds() {
		return "failed", fmt.Sprintf("no commit for %.0fs", commitAge), http.StatusServiceUnavailable
	}
	if len(snap.retrying) > 0 {
		sinks := make([]string, 0, len(snap.retrying))
		for s := range snap.retrying {
			sinks = append(sinks, s)
		}
		sort.Strings(sinks)
		return "degraded", fmt.Sprintf("sink %s is retrying, attempt %d",
			sinks[0], snap.retrying[sinks[0]]), http.StatusOK
	}
	if !p.LastError.IsZero() && now.Sub(p.LastError) <= interval {
		return "degraded", fmt.Sprintf("%d errors recorded, last %.0fs ago",
			p.Errors, now.Sub(p.LastError).Seconds()), http.StatusOK
	}
	if p.LastCommit.IsZero() {
		return "starting", "no commit yet", http.StatusOK
	}
	return "healthy", "", http.StatusOK
}
```

- [ ] **Step 5: Change the mux**

In `internal/cli/run/metrics.go`, add `health healthFunc` after `progress progressFunc` in `newHTTPMux`'s parameters. Replace the `/healthz` handler:

```go
	if progress != nil {
		mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
			p, _, commit := ages()
			var snap healthSnapshot
			if health != nil {
				snap = health()
			}
			status, reason, code := healthStatus(p, commit, snap, interval, now())

			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(code)
			body := map[string]any{
				"status":             status,
				"commit_age_seconds": commit,
				"interval_seconds":   interval.Seconds(),
			}
			if reason != "" {
				body["reason"] = reason
			}
			_ = json.NewEncoder(w).Encode(body)
		})
	}
```

Add `"last_error": p.LastError` and `"errors": p.Errors` to the `/stats` progress map.

Thread `health` through `newMeterProvider` as a new parameter after `progress`, and pass it to `newHTTPMux`. Update every caller: `health_test.go` (two), `metrics_test.go` (seven), `root.go` (one, in Task 8).

- [ ] **Step 6: Update `health_test.go`**

In `TestObservabilityMetrics_HealthzTellsIdleFromStuck`: change `coverage.Covers` to `"lifecycle.health"`, the expected first status from `"ok"` to `"healthy"`, the stuck status from `"stuck"` to `"failed"` with `assert.Equal(t, "no commit for 91s", body["reason"])`, the fresh-pipeline status to `"starting"`, and the final never-committed status to `"failed"`. Add `nil` as the health argument. Leave `TestObservabilityMetrics_HealthzAbsentWithoutProgress` under `observability.metrics` with the extra `nil`.

Add a test that the mux reads the health:

```go
func TestLifecycleHealth_EndpointReportsAFailure(t *testing.T) {
	coverage.Covers(t, "lifecycle.health")
	h := newHealth()
	p := core.Progress{LastCommit: time.Now().Add(-time.Second)}
	mux := newHTTPMux(nil, nil, nil, func() core.Progress { return p }, h.Snapshot,
		30*time.Second, time.Now)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	h.Fail(errors.New("[system.sink.write_failed] rejected"))
	resp, err := http.Get(srv.URL + "/healthz")
	assert.NoError(t, err)
	defer resp.Body.Close()
	var body map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&body)
	assert.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
	assert.Equal(t, "failed", body["status"])
	assert.Equal(t, "[system.sink.write_failed] rejected", body["reason"])
}
```

- [ ] **Step 7: Run the package and the tooling**

Run: `cd $S/sqlflow-161 && go build ./... && go test -short -race ./internal/cli/run/ && uv run pytest tests/tooling -q`
Expected: Go PASS. Tooling may fail on the matrix page being stale; run `make coverage-page` and rerun.

- [ ] **Step 8: Commit**

```bash
git add internal/cli/run docs/coverage
git commit -m "run: /healthz reports starting, healthy, degraded or failed"
```

---

### Task 8: Wire the budget and the health in `run`

**Files:**
- Modify: `internal/cli/run/root.go`
- Modify: `internal/cli/run/managers.go`
- Test: `internal/cli/exit_test.go`

**Interfaces:**
- Consumes: everything above. `buildManagedTables` gains two parameters: `budget *core.DrainBudget` and `events sinks.RetryEvents`.

- [ ] **Step 1: Build the budget and the health**

In `root.go`, change the `RunE` signature to a named return, `RunE: func(cmd *cobra.Command, args []string) (runErr error) {`, so the deferred block can set it.

After `flushInterval := flushIntervalFor(...)`:

```go
			// One deadline for the whole shutdown, started when the signal
			// arrives. The turbine's final batch, the managers' final poll
			// and the two state syncs below all spend it.
			budget := core.NewDrainBudget(drainDeadlineFor(conf.Pipeline.DrainDeadlineSeconds))
			defer budget.Stop()

			// What /healthz knows that the progress snapshot does not.
			hs := newHealth()
			retryEvents := sinks.RetryEvents{Retry: hs.Retry, Settle: hs.Settle}
```

Pass `hs.Snapshot` to `newMeterProvider` as the health argument.

Add `sinks.WithRetryEvents(retryEvents)` to the pipeline `sinks.New` call and to the DLQ call in `newErrorPolicies` (give `newErrorPolicies` an `events sinks.RetryEvents` parameter). Add `core.WithDrainBudget(budget)` to the turbine options.

In `managers.go`, add `budget *core.DrainBudget` and `events sinks.RetryEvents` parameters to `buildManagedTables`, pass `sinks.WithRetryEvents(events)` to its `sinks.New` and `managers.WithDrainBudget(budget)` to `NewTumbling`.

- [ ] **Step 2: Report failures to the health and check the budget**

The manager goroutine already calls `failRun(err)` (from #270); add `hs.Fail(err)` beside it.

Change the deferred manager block to run its syncs on the budget and to set the return when the budget ran out:

```go
			defer func() {
				drainCtx := budget.Context()
				if err := turbine.SyncState(drainCtx); err != nil {
					l.Error("failed to sync state before final poll", zap.Error(err))
				}
				stopManagers()
				managerWG.Wait()
				if err := turbine.SyncState(drainCtx); err != nil {
					l.Error("failed to sync state after final poll", zap.Error(err))
				}
				if budget.Exceeded() && runErr == nil {
					runErr = errs.New(errs.CodeDrainIncomplete,
						"drain deadline %s reached before the managers' final poll finished",
						budget.Deadline())
					hs.Fail(runErr)
					l.Error("drain incomplete", zap.Error(runErr))
				}
			}()
```

Careful: `budget.Context()` here starts the clock if the loop never did, which is the case when the loop exited on `--max-msgs` rather than a signal. That is fine: a clean exit's syncs finish in milliseconds.

After `stats, err := turbine.ConsumeLoop(runCtx, maxMsgs)` and `stopSignals()`, when `err != nil`, add `hs.Fail(err)` before returning. Where the manager cause is returned, `hs.Fail(cause)` too.

- [ ] **Step 3: Add the exit code test**

Append to `internal/cli/exit_test.go`:

```go
// A drain that ran out of time exits 15, and a supervisor may restart it:
// nothing unwritten was committed.
func TestLifecycleExitCodes_DrainIncompleteIsRetryable(t *testing.T) {
	coverage.Covers(t, "lifecycle.exit_codes")
	err := errs.New(errs.CodeDrainIncomplete, "drain deadline 30s reached")
	code := errs.ExitCode(err)
	assert.Equal(t, errs.ExitDrainIncomplete, code)
	assert.That(t, errs.Retryable(code))
}
```

- [ ] **Step 4: Build, vet, test**

Run: `cd $S/sqlflow-161 && go build ./... && go vet ./... && gofmt -l internal/ cmd/ && go test -short -race ./internal/cli/... ./internal/core/ ./internal/managers/ ./internal/sinks/`
Expected: PASS, `gofmt -l` prints nothing.

- [ ] **Step 5: Run a stateful example end to end**

Use the local Kafka on the `sqlflow` docker network (see the memory file `sqlflow-local-verification-setup`). Build the binary, run `dev/config/examples/kafka.stateful.window.yml` with `SQLFLOW_STATE_PATH` in the scratchpad, produce a few messages, curl `:8000/healthz` and confirm `starting` then `healthy`, send SIGTERM and confirm exit 0 with a `draining` log line. If Kafka is not up, note it in the PR and rely on the release test.

- [ ] **Step 6: Commit**

```bash
git add internal/cli
git commit -m "run: one drain budget for the shutdown, and the health endpoint sees failures and retries"
```

---

### Task 9: Harness checks for `lifecycle.drain.bounded` and `manager.drain.bounded`

**Files:**
- Modify: `internal/conformance/pipeline.go`
- Modify: `internal/conformance/manager.go`
- Modify: `docs/coverage/invariants.yml`
- Test: the existing `TestPipeline*_Conformance` and `TestManagerTumblingWindow_Conformance` tests run the new checks.

**Interfaces:**
- Consumes: `core.WithDrainBudget`, `core.NewDrainBudget`, `managers.WithDrainBudget` (through the subject's `New`).
- The `recordingSink` gains `hang bool`: `Flush` blocks until the context ends and records `flush-failed`.
- `PipelineSubject` gains nothing; `runPipeline` gains a `faults.hang` field and a `drainBudget` it applies through `WithDrainBudget`.
- `ManagerSubject.New` gains a fourth parameter, `budget *core.DrainBudget`. Update `internal/managers/conformance_test.go` to pass `WithDrainBudget(budget)`.

- [ ] **Step 1: Register the invariants**

In `docs/coverage/invariants.yml`, change `lifecycle.drain.bounded`:

```yaml
  - id: lifecycle.drain.bounded
    family: lifecycle
    class: liveness
    applies_to: pipeline
    claim: >
      A drain finishes or fails inside pipeline.drain_deadline_seconds. A sink
      that never answers cannot hold the process past it: the loop returns
      system.lifecycle.drain_incomplete, commits nothing for the batch it
      could not write, and the next start replays it.
    verified_by: harness
    requires: []
    enforced: true
```

Add after `manager.failure.exits`:

```yaml
  - id: manager.drain.bounded
    family: lifecycle
    class: liveness
    applies_to: manager
    claim: >
      The final poll after a cancel finishes or fails inside the drain
      deadline. A sink that never answers cannot hold the process past it,
      and every closed window it did not deliver stays in the state table.
    verified_by: harness
    requires: []
    enforced: true
```

Update the header comment's count if it names a total, and update `internal/coverage/registry_test.go` and `tests/tooling/test_registries.py` if either asserts an invariant count.

- [ ] **Step 2: The hanging sink and the pipeline check**

In `pipeline.go`, add `hang bool` to `recordingSink` and to `faults`. In `recordingSink.Flush`, before the `fail` branch:

```go
	if hang {
		// The sink a drain deadline exists for: one that neither succeeds
		// nor fails until the caller gives up.
		<-ctx.Done()
		s.rec.record("flush-failed")
		return ctx.Err()
	}
```

Read `hang` under the mutex with `inner` and `fail`. In `runPipeline`, set `sink.hang = f.hang`, and when `f.hang` is set build the turbine with `core.WithDrainBudget(core.NewDrainBudget(drainBudget))` where:

```go
// drainBudget is the deadline the bounded-drain scenario runs under. Short
// enough that the check is fast, long enough that a machine under load still
// reaches the flush before the clock starts.
const drainBudget = 300 * time.Millisecond
```

Add the constant `drainBounded = "lifecycle.drain.bounded"` and the check:

```go
// checkDrainBounded cancels mid-run against a sink that never answers, and
// holds that the loop returned inside the deadline with the drain code and
// committed nothing.
func checkDrainBounded(t *testing.T, s PipelineSubject) error {
	t.Helper()
	started := time.Now()
	run := runPipeline(t, s, TriggerDrain, faults{hang: true})
	took := time.Since(started)

	if run.err == nil {
		return fmt.Errorf("drain.bounded: the sink never answered and the pipeline reported a clean stop")
	}
	if errs.CodeOf(run.err) != errs.CodeDrainIncomplete {
		return fmt.Errorf("drain.bounded: the drain ran out of time and reported %s; "+
			"want %s so an operator can see the tail was replayed", errs.CodeOf(run.err),
			errs.CodeDrainIncomplete)
	}
	if took > 10*drainBudget {
		return fmt.Errorf("drain.bounded: a %s deadline held the process for %s", drainBudget, took)
	}
	for _, e := range run.events {
		if e == "commit" || e == "save-offsets" {
			return fmt.Errorf("drain.bounded: the sink never took the batch and the "+
				"pipeline did %s; the next start would skip those rows", list(run.events))
		}
	}
	return nil
}
```

`runPipeline` must not fail the test when `run.err` is set for this scenario; read it to see how `err` is captured (it returns `outcome.err`, so this should already hold).

Add `bounded := verdict{invariant: drainBounded}` in `pipelineVerdicts`, run `checkDrainBounded` after `checkDrain`, and append `bounded` to the returned slice.

- [ ] **Step 3: The manager check**

In `manager.go`, change `New` to `func(t *testing.T, sink core.Sink, poll time.Duration, budget *core.DrainBudget) Manager`. Pass `core.NewDrainBudget(managerWait)` from `newManagerRun` for the existing checks so nothing changes for them.

Add `managerDrainBounded = "manager.drain.bounded"` to the constants. The pipeline file already defines `drainBounded`, `drainBudget` and `checkDrainBounded` in this package, so the manager's names differ:

```go
// checkManagerDrainBounded cancels Start against a sink that never answers
// and holds that it returns inside the deadline with every window still in
// the table.
func checkManagerDrainBounded(t *testing.T, s ManagerSubject) error {
	t.Helper()
	s.Seed(t, seededWindows)
	rec := &Recorder{}
	sink := newRecordingSink(rec, nil, noop.NewMeterProvider())
	sink.hang = true
	budget := core.NewDrainBudget(drainBudget)
	defer budget.Stop()
	m := s.New(t, sink.counted, time.Hour, budget)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- m.Start(ctx) }()
	cancel()

	select {
	case err := <-done:
		if err == nil {
			return fmt.Errorf("the sink never answered the final poll and Start returned nil")
		}
	case <-time.After(managerWait):
		return fmt.Errorf("a %s drain deadline held the final poll for %s", drainBudget, managerWait)
	}
	if left := s.Remaining(t); left != seededWindows {
		return fmt.Errorf("the final poll ran out of time and %d of %d windows are gone "+
			"from the state table", seededWindows-left, seededWindows)
	}
	return nil
}
```

Add `bounded := verdict{invariant: managerDrainBounded}` to `managerVerdicts`, run `checkManagerDrainBounded`, and append it to the returned slice. In `internal/managers/conformance_test.go`, add the `budget` parameter and `WithDrainBudget(budget)`.

- [ ] **Step 4: Run the harness and the registries**

Run: `cd $S/sqlflow-161 && go test -short -race ./internal/conformance/ ./internal/sinks/ ./internal/managers/ ./internal/coverage/ && uv run pytest tests/tooling -q && make coverage-page`
Expected: PASS. Commit the regenerated `docs/coverage/matrix.md` if it changed.

- [ ] **Step 5: Commit**

```bash
git add internal/conformance internal/managers docs/coverage
git commit -m "conformance: a drain finishes inside its deadline, for the loop and for the manager"
```

---

### Task 10: Release test for the health endpoint, changelog, PR

**Files:**
- Modify: `tests/release/test_image.py`
- Modify: `CHANGELOG.md`

- [ ] **Step 1: Add the constant and the test**

After `EXIT_SINK_UNREACHABLE = 12` add `EXIT_DRAIN_INCOMPLETE = 15`. Read how `TERMINAL_EXITS` is built and leave 15 out of it.

Copy `test_turbostats_endpoint_serves_the_bundle` as:

```python
@pytest.mark.covers("lifecycle.health")
def test_lifecycle_health_reports_healthy_once_committed(image, stack):
    """The shipped image answers /healthz with the four-state body.

    A pipeline that has consumed nothing is starting. After the first idle
    tick commits, it is healthy. Both answer 200: neither is a reason for a
    supervisor to restart it.
    """
    topic = f"healthz-{int(time.time())}"

    with container_writable_dir() as state_dir:
        container = DockerContainer(image) \
            .with_volume_mapping(settings.DEV_DIR, "/tmp/conf") \
            .with_volume_mapping(state_dir, "/state", "rw") \
            .with_env("SQLFLOW_KAFKA_BROKERS", "kafka:9092") \
            .with_env("SQLFLOW_STATE_PATH", "/state/state.db") \
            .with_env("SQLFLOW_TOPIC", topic) \
            .with_env("SQLFLOW_GROUP_ID", topic) \
            .with_exposed_ports(8000) \
            .with_network(stack.network) \
            .with_command("run /tmp/conf/config/examples/kafka.stateful.window.yml")
        container.start()
        try:
            wait_for_logs(container, "consumer loop starting", timeout=90)
            port = container.get_exposed_port(8000)
            first = requests.get(f"http://localhost:{port}/healthz", timeout=10)
            # The example's flush interval is 30s; the idle tick after it is
            # the first commit.
            deadline = time.time() + 90
            body = first.json()
            while body["status"] == "starting" and time.time() < deadline:
                time.sleep(2)
                body = requests.get(f"http://localhost:{port}/healthz", timeout=10).json()
        finally:
            container.stop()

    assert first.status_code == 200
    assert first.json()["status"] in ("starting", "healthy")
    assert body["status"] == "healthy", body
```

Check the example's `flush_interval_seconds` and set the polling deadline to three times it.

- [ ] **Step 2: Changelog**

Find the most recent version heading in `CHANGELOG.md`. If there is an unreleased section, add there; otherwise add `## Unreleased` above the newest heading with:

```markdown
## Unreleased

### Added
- `pipeline.drain_deadline_seconds` bounds the whole shutdown: the final
  batch, the managers' final poll and the state syncs share one deadline,
  30 seconds by default. When it passes the process exits 15
  (`system.lifecycle.drain_incomplete`); nothing unwritten was committed and
  the next start replays it.
- `/healthz` reports `starting`, `healthy`, `degraded` or `failed` with a
  `reason`. `degraded` means a sink retry ladder is running or the loop
  recorded an error inside the last flush interval. `failed` is HTTP 503; the
  other three are 200. The former `ok` and `stuck` bodies are `healthy` and
  `failed`.
- `sqlflow validate` warns when a sink's `retry.deadline_seconds` is longer
  than the drain deadline.
```

- [ ] **Step 3: Run everything local, push, open the PR**

Run: `cd $S/sqlflow-161 && make test-go`
Expected: PASS.

```bash
git add tests/release/test_image.py CHANGELOG.md
git commit -m "release: /healthz answers healthy from the image"
git push -u origin feat/process-contract
gh pr create --title "Process contract: a bounded drain that exits 15, and a four-state health endpoint" --body-file - <<'EOF'
Closes #161.

Design: docs/superpowers/specs/2026-09-12-process-contract-design.md

- `pipeline.drain_deadline_seconds`, default 30, bounds the whole shutdown through one `core.DrainBudget` shared by the turbine, the managers and run's state syncs. When it passes the process exits 15 with `system.lifecycle.drain_incomplete`. Nothing unwritten was committed.
- `/healthz` reports `starting`, `healthy`, `degraded` or `failed` with a `reason`. The HTTP codes for the two previous states do not change.
- `lifecycle.drain.bounded` is proven by the harness and enforced. `manager.drain.bounded` is added and enforced.
- `sqlflow validate` warns when a retry ladder can outlive the drain.

Exit codes, the config-error terminal exit and the SIGTERM drain were already on main; the spec's first table says what shipped when.
EOF
```

- [ ] **Step 4: Coverage status from CI**

After CI's first run on the PR: `gh run list --branch feat/process-contract --limit 1`, then `gh run download <id> -n report-unit -n report-integration -n report-release`, `make coverage-write`, commit `docs/coverage/status` and `docs/coverage/matrix.md` as `coverage: status from CI run <id>`, push. Repeat if `coverage-check` fails on a stale file after a rebase.

---

### Task 11: Site docs

**Files:**
- Create: `src/content/docs/sqlflow/operations/running-in-production.md`
- Modify: `src/content/docs/sqlflow/introduction/configuration.md`
- Modify: `src/content/docs/sqlflow/operations/handling-errors.md`
- Modify: `tests/build.test.ts`

Work in `/Users/danielmican/code/github.com/turbolytics/turbolytics.io` on `git checkout -b docs/process-contract main`. Do not stage the dirty `tumbling-window-postgres.md`.

- [ ] **Step 1: Add the route to the build test**

In `tests/build.test.ts`, after `'docs/sqlflow/operations/handling-errors/index.html',` add `'docs/sqlflow/operations/running-in-production/index.html',`. Run `npm test` and confirm it fails on the missing route.

- [ ] **Step 2: Write the page**

```markdown
---
title: "Running in Production"
order: 2
---

sqlflow is one process with one config file. A supervisor starts it, reads its exit code when it stops, and polls one HTTP endpoint while it runs. This page documents those three contracts.

## Exit codes

The exit code tells the supervisor whether to restart. It is a projection of the [error code](/docs/sqlflow/operations/handling-errors) the process logged: a code in the `user` class exits 10, and the system codes map to the rest.

| Exit | Meaning | Restart |
|------|---------|---------|
| 0 | Clean stop, including a drain on SIGTERM. | No |
| 1 | A failure sqlflow could not classify. | Yes |
| 10 | A user error: config, SQL, credentials, data. It fails the same way every time. | No |
| 11 | The source refused a connection or stopped answering. | Yes |
| 12 | The sink refused a connection or stopped answering after the retry ladder. | Yes |
| 13 | A declared resource limit was exceeded. | Yes |
| 14 | The state file exists but cannot be read. Preserve it and start from a new path. | No |
| 15 | The drain deadline passed with rows unwritten. Nothing was lost; the next start replays them. | Yes |

2 is cobra's usage error and is never a pipeline failure.

A supervisor that restarts on 10 or 14 loops forever into the same failure. Kubernetes has no per-code policy, so set `restartPolicy: Always` and alert on `CrashLoopBackOff`; systemd can express it directly:

```ini
[Service]
Restart=on-failure
RestartPreventExitStatus=10 14
```

## Health

Every `sqlflow run` serves `GET /healthz` on port 8000. The body has one `status` field and, when the status is not `healthy`, a `reason`.

| status | HTTP | Meaning |
|--------|------|---------|
| `starting` | 200 | No commit yet, inside the first three flush intervals. |
| `healthy` | 200 | A commit inside the last three intervals, no retry running, no error in the last interval. |
| `degraded` | 200 | A sink's retry ladder is running, or the pipeline recorded an error inside the last flush interval. |
| `failed` | 503 | No commit for three intervals, or the pipeline has failed and is stopping. |

200 means do not restart. 503 means restart, or let the process exit.

```bash
curl -s localhost:8000/healthz
{"status":"degraded","reason":"sink clickhouse is retrying, attempt 2","commit_age_seconds":4,"interval_seconds":30}
```

The commit clock moves on every batch and on every idle tick, so a topic that delivers nothing for an hour is `healthy`. That is deliberate: a pipeline on a trickle looks dead to anything watching message counts.

A Kubernetes liveness probe:

```yaml
livenessProbe:
  httpGet:
    path: /healthz
    port: 8000
  periodSeconds: 30
  failureThreshold: 3
```

`GET /stats` on the same port carries the raw ages and, for a pipeline with a state path, the state file's size and row counts.

## Stopping

SIGTERM starts a drain. The pipeline writes the batch it was holding, runs each table manager's final poll so windows that closed during the last interval are published, commits state, and exits 0.

`pipeline.drain_deadline_seconds` bounds the whole drain. The default is 30 seconds, which is also Kubernetes' default `terminationGracePeriodSeconds`. If the deadline passes, the process exits 15. The batch it could not write was never committed, so the next start replays it. Set the deadline below the supervisor's grace period, or the supervisor's SIGKILL lands first and leaves no record.

```yaml
pipeline:
  drain_deadline_seconds: 20
```

A second SIGTERM during the drain stops the process at once.

`sqlflow validate` warns when a sink's `retry.deadline_seconds` is longer than the drain deadline, because a drain that hits a retrying sink then exits 15 before the ladder finishes.
```

Check the exact `/stats` fields against `metrics.go` before claiming them. Link `sqlflow validate` to the local development page if it documents the command.

- [ ] **Step 3: Configuration page**

In `introduction/configuration.md`, add `drain_deadline_seconds: <integer>` after `flush_interval_seconds` in the pipeline block. After the batch size and flush interval section, add:

```markdown
### Drain deadline

`drain_deadline_seconds` bounds the shutdown after SIGTERM. It defaults to 30. [Running in production](/docs/sqlflow/operations/running-in-production#stopping) describes what happens when it passes.
```

In the every-option listing, add the key with its comment after `flush_interval_seconds`, matching the regenerated `config example` golden from Task 4.

- [ ] **Step 4: Handling errors page**

Change the RAISE bullet to link the new page: `A malformed message exits \`10\`; [Running in production](/docs/sqlflow/operations/running-in-production#exit-codes) lists every code.` Delete the paragraph starting "One case sits on the wrong side of that line" through the closing "Read the wrapped message" sentence, and replace it with: `A value the sink's driver cannot encode is a user error and is not retried. The error names the column and the value.`

- [ ] **Step 5: Build, test, commit, push, PR**

```bash
npm test
git add src/content/docs/sqlflow/operations/running-in-production.md src/content/docs/sqlflow/introduction/configuration.md src/content/docs/sqlflow/operations/handling-errors.md tests/build.test.ts
git commit -m "docs: running in production, the exit codes, the health states and the drain deadline"
git push -u origin docs/process-contract
gh pr create --title "docs: running in production" --body "Documents exit codes, the four /healthz states and pipeline.drain_deadline_seconds from sql-flow #161. Merge after the sql-flow PR ships in a release, and pin the version note at the bottom of the page to that tag."
```

Hold the merge until the sql-flow change is released. The user runs releases by hand.
