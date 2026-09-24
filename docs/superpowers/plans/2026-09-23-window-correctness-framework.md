# Window Correctness Framework Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make every fact the window decides on state its own provenance, decide over those facts in one table, and check the result over sequences of events rather than over single states.

**Architecture:** Two seams first, because nothing replays deterministically without them: a clock on the turbine and injected triggers for the flush tick and the manager poll. Then `delivering_since` joins the progress row so the manager derives every fact from one read on one clock, `Source` becomes a third dimension of the decision table, and each fact carries what it measured and which clock measured it. Finally a two-layer check: exhaustive enumeration over a pure model, scripted sequences against the real engine.

**Tech Stack:** Go 1.24 (toolchain go1.26), DuckDB via ADBC, `github.com/zeebo/assert`, zap, OpenTelemetry metrics.

**Spec:** `docs/superpowers/specs/2026-09-23-window-correctness-framework-design.md`

## Global Constraints

- **No behavior change.** A window must close at exactly the same moment before and after this work. Any difference is a defect the work found and lands as its own commit with its own test.
- **Never compare across clock domains.** Event time answers "has the stream moved past this bucket." The progress row answers "has the engine been idle, and could it have received anything." The manager's own `now()` belongs to neither and must not appear in a close decision.
- **Monotonic readings survive.** `time.Now()` keeps its monotonic reading; `.UTC()` strips it and must not be applied to a value a duration is later measured from.
- **Every test names what it covers.** `coverage.Covers(t, "<feature>")` in each new test, matching the surrounding file's usage.
- **Existing state files must open.** `sqlflow_progress` gains a column; a state database written by an older engine has to keep working.
- **Comment style:** a strong, specific lead sentence per paragraph. No paragraph opens with a continuation fragment.

---

## File Structure

**Created:**
- `internal/core/clock.go` — the turbine's clock seam and its fake.
- `internal/managers/facts.go` — `Fact`, `Clock`, and the provenance a decision returns.
- `internal/managers/model.go` — the pure model of buckets, watermark and row, with no DuckDB.
- `internal/managers/model_test.go` — exhaustive enumeration over the model.
- `internal/simulate/simulate.go` — the event alphabet, the scripted runner, the fake partitioned source and the group coordinator.
- `internal/simulate/single_worker_test.go` — scripted sequences against a real turbine and manager.
- `internal/simulate/multi_worker_test.go` — the same machinery, skipped against the multi-worker issue.

**Modified:**
- `internal/core/turbine.go` — clock seam, flush trigger, `delivering_since` written, engine-side hold removed.
- `internal/core/progress.go` — the new column, in `Progress`, `Init` and `Record`.
- `internal/managers/decide.go` — `Source` dimension, rows, `StateOf` signature, facts.
- `internal/managers/watermark.go` — poll trigger, reads the new column, logs facts.
- `internal/managers/sql.go` — the quiet query returns the delivering terms.
- `docs/coverage/invariants.yml` — one declared property.
- `docs/windows/decisions.md` — regenerated, now three dimensions and provenance.

---

### Task 1: A clock on the turbine

**Files:**
- Create: `internal/core/clock.go`
- Modify: `internal/core/turbine.go` (the `time.Now()` reads at :453, :523, :624, :629, :744, :749)
- Test: `internal/core/clock_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces: `core.WithClock(now func() time.Time) TurbineOption`; `(*Turbine).now() time.Time`.

- [ ] **Step 1: Write the failing test**

```go
package core

import (
	"testing"
	"time"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

// The turbine reads every instant through one function, so a test can place
// the pipeline anywhere on the clock without waiting for it.
func TestTurbineClock_ReadsEveryInstantThroughTheInjectedClock(t *testing.T) {
	coverage.Covers(t, "core.consume_loop")
	at := time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)
	tb := NewTurbine(&fakeSource{}, &fakeHandler{}, &fakeSink{}, 1, time.Second,
		&sync.Mutex{}, PipelineErrorPolicies{}, WithClock(func() time.Time { return at }))

	assert.Equal(t, at, tb.now())
	assert.Equal(t, at, tb.quietSince)
}

func TestTurbineClock_DefaultsToTheWallClock(t *testing.T) {
	coverage.Covers(t, "core.consume_loop")
	tb := NewTurbine(&fakeSource{}, &fakeHandler{}, &fakeSink{}, 1, time.Second,
		&sync.Mutex{}, PipelineErrorPolicies{})
	// A default clock keeps its monotonic reading; UTC() would strip it.
	assert.That(t, strings.Contains(tb.now().String(), "m=+"))
}
```

- [ ] **Step 2: Run it and watch it fail**

Run: `go test ./internal/core/ -run TestTurbineClock -count=1`
Expected: FAIL, `undefined: WithClock`.

- [ ] **Step 3: Add the seam**

`internal/core/clock.go`:

```go
package core

import "time"

// WithClock replaces the instant source the turbine reads. Production leaves
// it alone; a simulator sets it so a sequence of events can be placed on the
// clock rather than waited for.
//
// The function must keep the monotonic reading time.Now() carries: the
// turbine measures durations between two of its results, and a fake that
// returns UTC() values makes those durations wall-clock arithmetic, which is
// what quietSince exists to avoid.
func WithClock(now func() time.Time) TurbineOption {
	return func(t *Turbine) { t.clock = now }
}

// now is every instant the turbine reads.
func (t *Turbine) now() time.Time {
	if t.clock == nil {
		return time.Now()
	}
	return t.clock()
}
```

In `internal/core/turbine.go`, add the field beside `progressEvery`:

```go
	// clock is where every instant comes from; nil means time.Now. See
	// WithClock.
	clock func() time.Time
```

Then replace the six reads. `NewTurbine` sets `quietSince` before options run, so move that seeding after the option loop:

```go
	for _, opt := range opts {
		opt(t)
	}
	// After the options, so WithClock governs the first instant too.
	t.quietSince = t.now()
	t.stats.StartTime = t.now().UTC()
```

and inside the methods use `t.now()` in place of `time.Now()` at the recordProgress read (`now := t.now()`), the commitState read, the `ConsumeLoop` seeding, and the `processBatch` stamp.

- [ ] **Step 4: Run the tests**

Run: `go test ./internal/core/ -run TestTurbineClock -count=1 && go test ./internal/core/ ./internal/cli/run/ -count=1 -run 'TestStateDurability|TestCoreConsumeLoop|TestManagerWindow_'`
Expected: PASS. The existing durability tests are the no-behavior-change gate.

- [ ] **Step 5: Commit**

```bash
git add internal/core/clock.go internal/core/clock_test.go internal/core/turbine.go
git commit -m "core: every instant the turbine reads comes from one function"
```

---

### Task 2: Triggers instead of tickers

**Files:**
- Modify: `internal/core/turbine.go` (the flush ticker at :783), `internal/managers/watermark.go` (the poll ticker at :201)
- Test: `internal/core/trigger_test.go`, `internal/managers/trigger_test.go`

**Interfaces:**
- Consumes: Task 1's `WithClock`.
- Produces: `core.WithFlushTrigger(c <-chan time.Time) TurbineOption`; `managers.WithPollTrigger(c <-chan time.Time) Option`.

- [ ] **Step 1: Write the failing test**

```go
// An injected trigger is the only thing that fires an idle tick, so a test
// decides when one happens instead of waiting a flush interval for it.
func TestTurbineTrigger_AnInjectedTickDrivesTheIdleCommit(t *testing.T) {
	coverage.Covers(t, "core.consume_loop")
	trigger := make(chan time.Time, 1)
	rec := &progressRecorder{}
	src := newBlockingSource(nil) // delivers nothing
	tb := NewTurbine(src, &fakeHandler{}, &fakeSink{}, 1, time.Hour,
		&sync.Mutex{}, PipelineErrorPolicies{},
		WithProgressStore(rec), WithFlushTrigger(trigger))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _, _ = tb.ConsumeLoop(ctx, 0); close(done) }()

	trigger <- time.Now()
	assert.NoError(t, waitFor(func() bool { return len(rec.records()) > 0 }))

	cancel()
	<-done
}
```

- [ ] **Step 2: Run it and watch it fail**

Run: `go test ./internal/core/ -run TestTurbineTrigger -count=1`
Expected: FAIL, `undefined: WithFlushTrigger`.

- [ ] **Step 3: Add both seams**

In `internal/core/turbine.go`:

```go
// WithFlushTrigger replaces the flush ticker. A pipeline given one commits on
// idleness only when the channel fires, which is how a simulator owns the
// ordering of a sequence rather than sharing it with a ticker.
func WithFlushTrigger(c <-chan time.Time) TurbineOption {
	return func(t *Turbine) { t.flushTrigger = c }
}
```

and where the loop builds its ticker:

```go
		var flushC <-chan time.Time
		if t.flushTrigger != nil {
			flushC = t.flushTrigger
		} else if t.flushInterval > 0 {
			flushTicker := time.NewTicker(t.flushInterval)
			defer flushTicker.Stop()
			flushC = flushTicker.C
		}
```

In `internal/managers/watermark.go`, the same shape:

```go
// WithPollTrigger replaces the poll ticker, so a caller decides when the
// manager looks for closed buckets.
func WithPollTrigger(c <-chan time.Time) Option {
	return func(w *Watermark) { w.pollTrigger = c }
}
```

```go
	var pollC <-chan time.Time
	if w.pollTrigger != nil {
		pollC = w.pollTrigger
	} else {
		ticker := time.NewTicker(w.poll)
		defer ticker.Stop()
		pollC = ticker.C
	}
```

- [ ] **Step 4: Run the tests**

Run: `go test ./internal/core/ ./internal/managers/ -count=1`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/core/turbine.go internal/core/trigger_test.go internal/managers/watermark.go internal/managers/trigger_test.go
git commit -m "core, managers: a caller can own when a tick and a poll happen"
```

---

### Task 3: the source's answer in the progress row

> Landed as one column, `delivering_for_us BIGINT`, rather than the
> `delivering_since TIMESTAMPTZ` below. Three states are needed, not two — a
> source that cannot deliver and one that was never asked mean opposite things
> to a window — and a duration is what `core.Deliverer` answers, so it needs no
> clock to be read against and no second column to disambiguate.

**Files:**
- Modify: `internal/core/progress.go`, `internal/core/turbine.go` (`recordProgress`)
- Test: `internal/core/progress_test.go`

**Interfaces:**
- Consumes: Task 1's `t.now()`.
- Produces: `Progress.DeliveringSince time.Time`; the column `delivering_since TIMESTAMPTZ` in `sqlflow_progress`.

- [ ] **Step 1: Write the failing test**

```go
// The row says since when the source could deliver, on the same clock as the
// other two columns, so a reader subtracts row values and never its own now.
func TestStateDurability_TheRowCarriesWhenTheSourceCouldDeliver(t *testing.T) {
	coverage.Covers(t, "state.durability")
	at := time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)
	rec := &progressRecorder{}
	src := &deliveringSource{for_: 30 * time.Second, ok: true}
	tb := NewTurbine(src, &fakeHandler{}, &fakeSink{}, 1, time.Hour,
		&sync.Mutex{}, PipelineErrorPolicies{},
		WithProgressStore(rec), WithClock(func() time.Time { return at }))

	assert.NoError(t, tb.recordProgress(context.Background(), progressForced))

	last := rec.records()[len(rec.records())-1]
	assert.Equal(t, at.Add(-30*time.Second), last.DeliveringSince)
}

// A source that cannot deliver leaves the column empty rather than writing an
// instant a reader would treat as a resumption.
func TestStateDurability_ASourceThatCannotDeliverLeavesTheColumnEmpty(t *testing.T) {
	coverage.Covers(t, "state.durability")
	at := time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)
	rec := &progressRecorder{}
	src := &deliveringSource{ok: false}
	tb := NewTurbine(src, &fakeHandler{}, &fakeSink{}, 1, time.Hour,
		&sync.Mutex{}, PipelineErrorPolicies{},
		WithProgressStore(rec), WithClock(func() time.Time { return at }))

	assert.NoError(t, tb.recordProgress(context.Background(), progressForced))

	last := rec.records()[len(rec.records())-1]
	assert.That(t, last.DeliveringSince.IsZero())
}
```

with the double beside the other fakes in that file:

```go
// deliveringSource is a source whose Delivering answer the test sets.
type deliveringSource struct {
	blockingSource
	for_ time.Duration
	ok   bool
}

func (s *deliveringSource) Delivering() (time.Duration, bool) { return s.for_, s.ok }
```

- [ ] **Step 2: Run it and watch it fail**

Run: `go test ./internal/core/ -run TheRowCarriesWhenTheSourceCouldDeliver -count=1`
Expected: FAIL, `last.DeliveringSince undefined`.

- [ ] **Step 3: Carry the column**

In `internal/core/progress.go`, add to `Progress`:

```go
	// DeliveringSince is when the source last became able to deliver: a Kafka
	// consumer's assignment, a websocket's dial. Zero while it cannot. On the
	// same clock as the other two, so a reader subtracts row values only.
	DeliveringSince time.Time
```

`Init` gains the column, and adds it to a table an older engine wrote:

```go
		`CREATE TABLE IF NOT EXISTS ` + progressTable + ` (
		    last_arrival     TIMESTAMPTZ,
		    last_commit      TIMESTAMPTZ,
		    delivering_since TIMESTAMPTZ,
		    messages         BIGINT NOT NULL
		)`,
		// A state database written before this column exists opens without it.
		`ALTER TABLE ` + progressTable + ` ADD COLUMN IF NOT EXISTS delivering_since TIMESTAMPTZ`,
```

`Record` writes it, and clears it when the source cannot deliver:

```go
	if p.DeliveringSince.IsZero() {
		q += `, delivering_since = NULL`
	} else {
		q += fmt.Sprintf(`, delivering_since = TIMESTAMPTZ '%s'`, utcLiteral(p.DeliveringSince))
	}
```

In `recordProgress`, fill it from the source, replacing nothing yet:

```go
	// From the duration the source reports rather than an instant it holds:
	// a duration cannot arrive with its monotonic reading stripped.
	if d, ok := t.source.(Deliverer); ok {
		if deliveringFor, delivering := d.Delivering(); delivering {
			p.DeliveringSince = now.Add(-deliveringFor)
		}
	}
```

- [ ] **Step 4: Run the tests**

Run: `go test ./internal/core/ -count=1`
Expected: PASS, including the existing durability suite.

- [ ] **Step 5: Commit**

```bash
git add internal/core/progress.go internal/core/turbine.go internal/core/progress_test.go
git commit -m "core: the progress row says since when the source could deliver"
```

---

### Task 4: `Source` as a third dimension

**Files:**
- Modify: `internal/managers/decide.go`, `internal/managers/sql.go`, `internal/managers/watermark.go`, `internal/core/turbine.go` (remove `holdQuietWhileNotDelivering` and its two call sites)
- Test: `internal/managers/decide_test.go`, `internal/core/progress_test.go`

**Interfaces:**
- Consumes: Task 3's column.
- Produces: `managers.Source` (`SourceDelivering`, `SourceNotDelivering`); `StateOf(decl Declaration, newest time.Time, hasRows bool, previous time.Time, hadPrevious bool, quiet, deliveringFor time.Duration, delivering bool) State`.

- [ ] **Step 1: Write the failing test**

```go
// A source that cannot deliver holds every open bucket, as a row of the table
// rather than as a correction applied to the quiet before the table runs.
func TestManagerWindow_NotDeliveringIsARowOfTheTable(t *testing.T) {
	coverage.Covers(t, "managers.window")
	decl := Declaration{Size: time.Minute, Grace: time.Minute, IdleClose: 10 * time.Second}
	s := StateOf(decl, newest, true, previous, true, time.Hour, 0, false)

	assert.Equal(t, SourceNotDelivering, s.Source)
	assert.Equal(t, Hold, Decide(s))
	assert.Equal(t, "hold.not_delivering", watermarkRuleFor(s).Name)
}

// Quiet is bounded by the resumption, so a reconnect 30s ago cannot confirm
// the 60s bound however old the last arrival is.
func TestManagerWindow_QuietIsBoundedByTheResumption(t *testing.T) {
	coverage.Covers(t, "managers.window")
	decl := Declaration{Size: time.Minute, Grace: time.Minute, IdleClose: time.Minute}
	s := StateOf(decl, newest, true, previous, true, 2*time.Minute, 30*time.Second, true)

	assert.Equal(t, IdleUnconfirmed, s.Idle)
	assert.Equal(t, Hold, Decide(s))
}
```

- [ ] **Step 2: Run it and watch it fail**

Run: `go test ./internal/managers/ -run 'NotDeliveringIsARow|QuietIsBounded' -count=1`
Expected: FAIL, `undefined: SourceNotDelivering`, and `StateOf` arity.

- [ ] **Step 3: Extend the table**

In `internal/managers/decide.go`:

```go
// Source is whether the engine could have received anything at all.
type Source string

const (
	// SourceDelivering is a source holding what it needs to deliver: a
	// consumer with partitions, a connected websocket.
	SourceDelivering Source = "delivering"
	// SourceNotDelivering is a source that holds nothing: a consumer between
	// assignments, a websocket reconnecting. Time it cannot deliver is not
	// quiet, and no bucket closes on idleness across it.
	SourceNotDelivering Source = "not_delivering"
)

var sourceValues = []Source{SourceDelivering, SourceNotDelivering}
```

`State` gains `Source Source`, `String()` renders it, and `allStates` iterates the third axis:

```go
func allStates() []State {
	var out []State
	for _, d := range dataValues {
		for _, i := range idleValues {
			for _, s := range sourceValues {
				out = append(out, State{Data: d, Idle: i, Source: s})
			}
		}
	}
	return out
}
```

`StateOf` takes the two new arguments and bounds the quiet:

```go
func StateOf(decl Declaration, newest time.Time, hasRows bool, previous time.Time,
	hadPrevious bool, quiet, deliveringFor time.Duration, delivering bool) State {
	s := State{Idle: IdleOff, Source: SourceDelivering}
	if !delivering {
		s.Source = SourceNotDelivering
	}
	if decl.IdleClose > 0 {
		// The engine can only confirm quiet it could have heard: a source
		// that resumed 30s ago has confirmed 30s at most, whatever the last
		// arrival says.
		if delivering && deliveringFor < quiet {
			quiet = deliveringFor
		}
		if !delivering {
			quiet = 0
		}
		s.Idle = IdleUnconfirmed
		if quiet >= decl.IdleClose {
			s.Idle = IdleConfirmed
		}
	}
	// ... the Data switch is unchanged
	return s
}
```

`watermarkTable` gains one row and `close.idle` narrows:

```go
	{
		Name:     "hold.not_delivering",
		Data:     []Data{DataOpen, DataRipe},
		Idle:     []Idle{IdleConfirmed},
		Source:   []Source{SourceNotDelivering},
		Action:   Hold,
		Deciding: "source",
		Claim:    "The source could not deliver, so the stream being silent says nothing about the data and no bucket closes on idleness.",
	},
	{
		Name:     "close.idle",
		Data:     []Data{DataOpen, DataRipe},
		Idle:     []Idle{IdleConfirmed},
		Source:   []Source{SourceDelivering},
		Action:   CloseByIdle,
		Deciding: "idle",
		Claim:    "The engine committed idle_close_seconds after the newest arrival, on a source that could have delivered, so every open bucket closes up to the newest bucket's end.",
	},
```

`matches` gains the third clause. With 4 × 3 × 2 = 24 states, `checkTables` proves the partition; no row may be reachable only through the unreachable pairing of `not_delivering` with `confirmed`, which `StateOf` never produces but the table still has to cover.

In `internal/managers/sql.go`:

```go
// confirmedQuietSQL reads the three engine instants as two differences and a
// flag, so the manager subtracts row values and never its own clock.
func confirmedQuietSQL() string {
	return `SELECT epoch_us(last_commit) - epoch_us(last_arrival),
	               coalesce(epoch_us(last_commit) - epoch_us(delivering_since), 0),
	               delivering_since IS NOT NULL
	        FROM sqlflow_progress`
}
```

`nextWatermark` reads all three and passes them to `StateOf`. In `internal/core/turbine.go`, delete `holdQuietWhileNotDelivering` and its two call sites, because the fact now travels in the row.

- [ ] **Step 4: Run the tests**

Run: `go test ./internal/managers/ ./internal/core/ -count=1 && go test ./internal/cli/run/ -run TestManagerWindow_ -count=1`
Expected: PASS. `TestStateDurability_ASourceThatCannotDeliverConfirmsNoQuiet` and `TheDrainConfirmsNoQuietWhileTheSourceCannotDeliver` now pass through the row rather than the hold, which is the no-behavior-change gate for this task.

- [ ] **Step 5: Commit**

```bash
git add internal/managers/decide.go internal/managers/sql.go internal/managers/watermark.go internal/managers/decide_test.go internal/core/turbine.go internal/core/progress_test.go
git commit -m "managers: whether the source could deliver is a fact, not a correction"
```

---

### Task 5: Provenance on every fact

**Files:**
- Create: `internal/managers/facts.go`
- Modify: `internal/managers/decide.go` (`StateOf` returns facts), `internal/managers/watermark.go` (the close log line), `internal/managers/decide_test.go` (the rendered doc)
- Test: `internal/managers/facts_test.go`

**Interfaces:**
- Consumes: Task 4's `State`.
- Produces: `managers.Fact`, `managers.FactClock`, `FactsOf(decl Declaration, s State, quiet, deliveringFor time.Duration, newest, previous time.Time) []Fact`.

- [ ] **Step 1: Write the failing test**

```go
// Every fact says which clock measured it, so a comparison that mixes event
// time with the engine's clock is visible here rather than in a chart.
func TestManagerWindow_EveryFactNamesItsClock(t *testing.T) {
	coverage.Covers(t, "managers.window")
	decl := Declaration{Size: time.Minute, Grace: time.Minute, IdleClose: time.Minute}
	s := StateOf(decl, newest, true, previous, true, 90*time.Second, time.Hour, true)

	byName := map[string]Fact{}
	for _, f := range FactsOf(decl, s, 90*time.Second, time.Hour, newest, previous) {
		byName[f.Name] = f
	}

	assert.Equal(t, EngineClock, byName["idle"].Clock)
	assert.Equal(t, EngineClock, byName["source"].Clock)
	assert.Equal(t, EventTime, byName["data"].Clock)
	assert.Equal(t, time.Minute, byName["idle"].Limit)
	assert.Equal(t, 90*time.Second, byName["idle"].Measured)
}
```

- [ ] **Step 2: Run it and watch it fail**

Run: `go test ./internal/managers/ -run EveryFactNamesItsClock -count=1`
Expected: FAIL, `undefined: FactsOf`.

- [ ] **Step 3: Write the facts**

`internal/managers/facts.go`:

```go
package managers

import (
	"fmt"
	"time"
)

// FactClock is which clock measured a fact. Event time comes from the data,
// the engine clock from the progress row, and the two are never compared.
type FactClock string

const (
	EventTime   FactClock = "event_time"
	EngineClock FactClock = "engine"
	NoClock     FactClock = "none"
)

// Fact is one input to a decision with the evidence for its value.
type Fact struct {
	Name     string
	Value    string
	Measured time.Duration
	Limit    time.Duration
	Clock    FactClock
	From     string
}

// String renders a fact for a log line: idle=confirmed 90s against 60s, engine.
func (f Fact) String() string {
	if f.Clock == NoClock {
		return fmt.Sprintf("%s=%s", f.Name, f.Value)
	}
	return fmt.Sprintf("%s=%s %s against %s, %s", f.Name, f.Value, f.Measured, f.Limit, f.Clock)
}

// FactsOf is the evidence behind a state.
func FactsOf(decl Declaration, s State, quiet, deliveringFor time.Duration,
	newest, previous time.Time) []Fact {
	return []Fact{
		{Name: "data", Value: string(s.Data), Measured: newest.Sub(previous),
			Limit: decl.Grace, Clock: EventTime, From: "window table"},
		{Name: "idle", Value: string(s.Idle), Measured: quiet,
			Limit: decl.IdleClose, Clock: EngineClock, From: "sqlflow_progress"},
		{Name: "source", Value: string(s.Source), Measured: deliveringFor,
			Clock: EngineClock, From: "sqlflow_progress"},
	}
}
```

In `watermark.go`, the close log line carries them:

```go
		w.logger.Info("close decided",
			zap.String("rule", rule.Name),
			zap.String("claim", rule.Claim),
			zap.Stringers("facts", facts),
		)
```

In `decide_test.go`, the rendered `docs/windows/decisions.md` gains a facts section listing each fact, its clock and what it is measured against. Regenerate and commit the doc.

- [ ] **Step 4: Run the tests**

Run: `go test ./internal/managers/ -count=1 && git diff --stat docs/windows/decisions.md`
Expected: PASS, and the doc shows the three facts with their clocks.

- [ ] **Step 5: Commit**

```bash
git add internal/managers/facts.go internal/managers/facts_test.go internal/managers/decide.go internal/managers/watermark.go internal/managers/decide_test.go docs/windows/decisions.md
git commit -m "managers: a decision states what it decided on, and which clock said so"
```

---

### Task 6: Declare the property the ledger is missing

**Files:**
- Modify: `docs/coverage/invariants.yml`
- Test: `go test ./internal/coverage/ -count=1` (the ledger's own schema test)

**Interfaces:**
- Consumes: nothing.
- Produces: the invariant id `pipeline.window.counts_every_row`.

- [ ] **Step 1: Write the entry**

Add to the pipeline family, after `pipeline.state.with_offsets`:

```yaml
  # No invariant owned the window's output, which is why a local reproduction
  # of #183 published 12% fewer records than were produced while all 52 of
  # these passed. A rebalance breaks it today: two workers each hold part of a
  # bucket, both publish, and an upserting sink keeps the last writer.
  - id: pipeline.window.counts_every_row
    family: checkpoint
    class: safety
    applies_to: pipeline
    claim: >
      A bucket's published value counts exactly the rows produced for it,
      once. Rows the engine dropped under late_rows: drop are excluded and
      counted in window_late_rows_total; late_rows: reemit publishes a
      different contract and is out of scope.
    verified_by: harness
    requires: []
    tracked_by: "#NNN"
```

Replace `#NNN` with the multi-worker issue number once it is filed.

- [ ] **Step 2: Run the ledger's checks**

Run: `go test ./internal/coverage/ -count=1 && make coverage-matrix && git status --short docs/coverage`
Expected: PASS, and the new row renders as declared-unenforced.

`coverage-matrix`, not `coverage-page`. The page target writes `matrix.md`
alone and leaves the per-feature files in `docs/coverage/status/` untouched,
which the gate also reads: a declared invariant owes a row to every feature it
applies to. Running the page target alone leaves CI red with a diff naming
`pipeline.stateful.yml` and `pipeline.stateless.yml`.

- [ ] **Step 3: Commit**

```bash
git add docs/coverage/invariants.yml docs/coverage/matrix.md
git commit -m "coverage: declare the property that owns a window's output"
```

---

### Task 7: The pure model, checked exhaustively

**Files:**
- Create: `internal/managers/model.go`, `internal/managers/model_test.go`

**Interfaces:**
- Consumes: Tasks 4 and 5.
- Produces: `managers.Model`, `Model.Apply(e Event) []Publication`, `managers.Event`.

- [ ] **Step 1: Write the failing test**

```go
// Every sequence of events up to five long leaves the model publishing each
// row exactly once, whatever order the events arrive in.
func TestManagerWindow_EverySequenceCountsEveryRowOnce(t *testing.T) {
	coverage.Covers(t, "managers.window")
	alphabet := []Event{
		{Kind: Arrive, Rows: 2}, {Kind: IdleTick}, {Kind: Restart},
		{Kind: SourceLost}, {Kind: SourceBack}, {Kind: ClockStep, By: time.Minute},
	}
	for _, seq := range sequences(alphabet, 5) {
		m := NewModel(Declaration{Size: time.Minute, Grace: time.Minute, IdleClose: 10 * time.Second})
		produced, published := 0, 0
		for _, e := range seq {
			if e.Kind == Arrive {
				produced += e.Rows
			}
			for _, p := range m.Apply(e) {
				published += p.Rows
			}
		}
		published += m.Drain()
		assert.Equal(t, produced, published)
	}
}
```

- [ ] **Step 2: Run it and watch it fail**

Run: `go test ./internal/managers/ -run EverySequenceCountsEveryRowOnce -count=1`
Expected: FAIL, `undefined: NewModel`.

- [ ] **Step 3: Write the model**

`internal/managers/model.go` holds buckets as counts keyed by bucket start, the committed watermark, and the three engine instants, and advances them per event. `Apply` computes a `State` through the real `StateOf`, asks the real `Decide`, and publishes what the action collects. Nothing here touches DuckDB, so the enumeration is cheap: with six events and length five, 7,776 sequences run in well under a second.

`sequences` is a helper in the test file that enumerates the cartesian product up to the given length.

- [ ] **Step 4: Run the tests**

Run: `go test ./internal/managers/ -count=1`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/managers/model.go internal/managers/model_test.go
git commit -m "managers: the table's rules, checked over sequences rather than states"
```

---

### Task 8: Scripted sequences against the real engine

**Files:**
- Create: `internal/simulate/simulate.go`, `internal/simulate/single_worker_test.go`, `internal/simulate/multi_worker_test.go`

**Interfaces:**
- Consumes: Tasks 1, 2, 3, 4.
- Produces: `simulate.Run(t *testing.T, script []Step) Result`, `simulate.FakeSource`, `simulate.Coordinator`.

- [ ] **Step 1: Write the failing test**

```go
// A restart, a revoke and a clock step in one run still publish every row the
// producer sent, once.
func TestSimulate_ASingleWorkerCountsEveryRow(t *testing.T) {
	coverage.Covers(t, "core.consume_loop")
	r := Run(t, []Step{
		Produce{Partition: 0, Rows: 100},
		IdleTick{},
		StepClock{By: time.Minute},
		Produce{Partition: 0, Rows: 100},
		Restart{},
		Produce{Partition: 0, Rows: 100},
		IdleTick{}, IdleTick{},
		Drain{},
	})
	assert.Equal(t, r.Produced, r.Published)
	assert.Equal(t, 0, r.Duplicated)
}
```

- [ ] **Step 2: Run it and watch it fail**

Run: `go test ./internal/simulate/ -count=1`
Expected: FAIL, no such package.

- [ ] **Step 3: Build the runner**

`simulate.go` holds four things. `FakeSource` implements `core.Source`, `core.MarkCommitter`, `core.PartitionOwner` and `core.Deliverer`, and exposes `Assign`, `Revoke` and `Lose` as method calls. `Coordinator` holds the assignment, a committed offset per partition, and a generation that refuses a commit from a worker whose generation has moved on. `worker` pairs a real `core.Turbine` (built with `WithClock`, `WithFlushTrigger`, a real `ProgressStore` on a temp state file) with a real `managers.Watermark` (built with `WithPollTrigger`), and a recording sink. `Run` steps the script, driving each event through the seams, and returns produced, published and duplicated counts read from the sink.

- [ ] **Step 4: Run the tests**

Run: `go test ./internal/simulate/ -count=1 -race`
Expected: PASS.

- [ ] **Step 5: Add the multi-worker scenarios, skipped**

```go
// Two workers over two partitions, which today publishes a bucket short by
// one worker's share. Skipped until the multi-worker issue lands, so the
// ledger records the gap rather than implying coverage.
func TestSimulate_ScaleOutKeepsEveryRow(t *testing.T) {
	t.Skip("tracked by #NNN: a bucket split across workers publishes one share")
	// ... the script: Produce on both partitions, StartWorker, Revoke, Drain
}
```

- [ ] **Step 6: Commit**

```bash
git add internal/simulate/
git commit -m "simulate: sequences of real events against a real turbine and manager"
```

---

## Self-Review

**Spec coverage.** The `Source` dimension is Task 4; `delivering_since` is Task 3; provenance is Task 5; the declared property is Task 6; the two simulator layers are Tasks 7 and 8; the seams the spec puts first are Tasks 1 and 2. The spec's "what it cannot prove" section names the #183 integration test, which is deliberately not in this plan and stays in that ticket.

**Placeholders.** Two remain by necessity: `#NNN` in Tasks 6 and 8, which is the multi-worker issue number, to be filled once it is filed. Everything else carries its code.

**Type consistency.** `StateOf` gains `(quiet, deliveringFor time.Duration, delivering bool)` in Task 4 and is called with that signature in Tasks 5 and 7. `Fact` is defined in Task 5 and used in Task 5 only. `Progress.DeliveringSince` is defined in Task 3 and read by the SQL in Task 4. `WithClock`, `WithFlushTrigger` and `WithPollTrigger` are defined in Tasks 1 and 2 and consumed in Task 8.

**Risks worth naming before execution.** Task 4 is the one that can change behavior: it moves a correction out of the engine and into the table, and the existing durability tests are the gate. If `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` is unsupported by the pinned DuckDB, Task 3 falls back to reading `duckdb_columns()` and issuing a plain `ALTER TABLE` when the column is absent.
