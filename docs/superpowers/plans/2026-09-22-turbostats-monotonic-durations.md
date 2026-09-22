# TurboStats Monotonic Durations Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** The TurboStats bundle carries `process.uptime_seconds` and a top-level `idle_seconds`, both measured on Go's monotonic clock, so a receiver can judge start and work without trusting the instance's wall clock.

**Architecture:** A new leaf package, `internal/activity`, owns one process's start and its last work as offsets on the monotonic clock. `sqlflow run` and `sqlflow serve` create it at startup, the pipeline and the serve handler mark it when they do work, and `turbostats.Collect` reads both durations from it in one call.

**Tech Stack:** Go 1.26, the standard library's monotonic clock, `github.com/zeebo/assert`, the repo's `internal/coverage` markers.

**Spec:** `turbolytics/sql-flow-control`, `docs/superpowers/specs/2026-09-22-status-truth-table-design.md`, section "Phase 1: the engine reports durations". This is PR 1 of that spec's four.

## Global Constraints

- Both new fields are pointers with `omitempty`: a process a second old has an uptime of 0, and 0 must not read as absent.
- `uptime_seconds` and `idle_seconds` come from `time.Since` on a start that carries a monotonic reading. Never from subtracting two wall readings.
- `started_at` and `last_activity_at` stay on the wire, unchanged, for display.
- `idle_seconds` is at most `uptime_seconds` in every bundle.
- `last_activity_at` is at or before `sent_at` in every bundle.
- Activity is per process. Nothing that survives a restart feeds `last_activity_at` or `idle_seconds`.
- The document stays `v: 1`. Both fields are additive; a receiver that ignores them is unaffected.
- Every new test carries `coverage.Covers(t, "observability.turbostats")`, or `"observability.turbostats.serve"` in `internal/serve`.
- Prose follows the repo's CLAUDE.md: SQLFlow in prose, `sqlflow` for the command.

---

## File Structure

- Create `internal/activity/clock.go`: the `Clock` type. One responsibility: durations since start and since last work, on the monotonic clock.
- Create `internal/activity/clock_test.go`.
- Modify `turbostats/wire/bundle.go`: `Process.UptimeSeconds`, `Bundle.IdleSeconds`.
- Modify `turbostats/wire/bundle_test.go`.
- Modify `internal/turbostats/bundle.go`: `Static.Clock`.
- Modify `internal/turbostats/collect.go`: fill the two fields.
- Modify `internal/turbostats/collect_test.go`.
- Modify `internal/core/metrics.go`: `Metrics.Activity`.
- Modify `internal/core/turbine.go:711`: mark on each batch received.
- Modify `internal/core/flat_test.go`.
- Modify `internal/serve/metrics.go`: mark on each request.
- Modify `internal/serve/server.go`: hand the static's clock to the metrics.
- Modify `internal/serve/turbostats_test.go`.
- Modify `internal/cli/run/root.go:127,362,439`: start the clock, wire it.
- Modify `internal/cli/serve/serve.go:96,141`: start the clock.
- Modify `docs/superpowers/specs/2026-09-19-turbostats-contract-amendment-design.md`: the contract lines.
- Modify `CHANGELOG.md`.

---

### Task 1: The activity clock

**Files:**
- Create: `internal/activity/clock.go`
- Test: `internal/activity/clock_test.go`

**Interfaces:**
- Produces:
  - `func Start() *Clock`
  - `func Fake(elapsed func() time.Duration) *Clock`, for tests in other packages
  - `func (c *Clock) StartedAt() time.Time`
  - `func (c *Clock) Mark()`, safe on a nil `*Clock`
  - `func (c *Clock) Read() (uptime, idle time.Duration, worked bool)`

- [ ] **Step 1: Write the failing tests**

```go
package activity

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

// fake is a clock whose elapsed time the test sets.
func fake() (*Clock, *time.Duration) {
	elapsed := new(time.Duration)
	return Fake(func() time.Duration { return *elapsed }), elapsed
}

// t.UTC() strips the monotonic reading, and root.go once took its start
// that way. Start is the only production constructor, so it is the one
// place that has to keep it.
func TestClock_StartKeepsTheMonotonicReading(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	c := Start()
	assert.That(t, strings.Contains(c.start.String(), "m=+"))
	// StartedAt is for display, in UTC, and is allowed to lose it.
	assert.Equal(t, time.UTC, c.StartedAt().Location())
}

func TestClock_NoWorkYetIsNotIdle(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	c, elapsed := fake()
	*elapsed = 90 * time.Second

	uptime, idle, worked := c.Read()
	assert.Equal(t, 90*time.Second, uptime)
	assert.Equal(t, time.Duration(0), idle)
	assert.That(t, !worked)
}

func TestClock_IdleIsTimeSinceTheLastMark(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	c, elapsed := fake()
	*elapsed = 30 * time.Second
	c.Mark()
	*elapsed = 90 * time.Second

	uptime, idle, worked := c.Read()
	assert.Equal(t, 90*time.Second, uptime)
	assert.Equal(t, 60*time.Second, idle)
	assert.That(t, worked)
}

// A mark at the instant of start is still work. Zero is the sentinel for
// none, so the offset is stored plus one.
func TestClock_AMarkAtStartIsWork(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	c, elapsed := fake()
	c.Mark()
	*elapsed = 5 * time.Second

	_, idle, worked := c.Read()
	assert.That(t, worked)
	assert.Equal(t, 5*time.Second, idle)
}

// Both durations come from one reading, so idle never exceeds uptime, even
// when a caller truncates both to whole seconds.
func TestClock_IdleNeverExceedsUptime(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	c, elapsed := fake()
	for i := 0; i < 1000; i++ {
		*elapsed = time.Duration(i) * 997 * time.Millisecond
		if i%7 == 0 {
			c.Mark()
		}
		uptime, idle, _ := c.Read()
		assert.That(t, idle <= uptime)
		assert.That(t, idle/time.Second <= uptime/time.Second)
	}
}

func TestClock_MarkOnNilIsANoop(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	var c *Clock
	c.Mark()
}

// The pipeline marks from its consume loop while the reporter reads from
// its own goroutine. Run with -race.
func TestClock_MarkAndReadAreSafeTogether(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	c := Start()
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				c.Mark()
				_, _, _ = c.Read()
			}
		}()
	}
	wg.Wait()
	_, _, worked := c.Read()
	assert.That(t, worked)
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test -short -race ./internal/activity/`
Expected: FAIL, `undefined: Fake`, `undefined: Start`.

- [ ] **Step 3: Write the implementation**

```go
// Package activity measures how long a process has run, and how long since
// it last did work, on the monotonic clock.
//
// The wall clock is the wrong instrument for both. A gateway without a
// hardware clock boots near 1970 and then syncs, and NTP steps a fast clock
// back. A duration taken from two wall readings across either step is wrong
// by the step, for the life of the process. Go's monotonic reading does not
// step, so every duration here is time.Since on a start that carries one.
package activity

import (
	"sync/atomic"
	"time"
)

// Clock is one process's start and its most recent work.
type Clock struct {
	start time.Time
	// last is nanoseconds from start to the most recent Mark, plus one, so
	// zero means no work yet.
	last atomic.Int64
	// elapsed is time since start. time.Since in production.
	elapsed func() time.Duration
}

// Start takes the process start now.
//
// It takes no argument on purpose. A caller handing in a time could hand in
// one whose monotonic reading was stripped: t.UTC() strips it, and the run
// command once did exactly that.
func Start() *Clock {
	c := &Clock{start: time.Now()}
	c.elapsed = func() time.Duration { return time.Since(c.start) }
	return c
}

// Fake is a clock whose elapsed time the caller supplies. It is for tests in
// other packages, which cannot wait for the monotonic clock to move.
func Fake(elapsed func() time.Duration) *Clock {
	return &Clock{start: time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC), elapsed: elapsed}
}

// StartedAt is the start as wall time, for display. Never subtract it from
// another wall reading: that is the arithmetic this package exists to
// replace.
func (c *Clock) StartedAt() time.Time { return c.start.UTC() }

// Mark records work now. It is safe on a nil Clock, so a caller built without
// one does not have to check.
func (c *Clock) Mark() {
	if c == nil {
		return
	}
	c.last.Store(int64(c.elapsed()) + 1)
}

// Read returns the uptime, the time since the last Mark, and whether there
// has been one.
//
// Both come from a single elapsed reading. Two readings would let idle
// exceed uptime by the time between them, and a receiver that checks
// idle <= uptime would call the bundle contradictory.
func (c *Clock) Read() (uptime, idle time.Duration, worked bool) {
	uptime = c.elapsed()
	last := c.last.Load()
	if last == 0 {
		return uptime, 0, false
	}
	idle = uptime - time.Duration(last-1)
	if idle < 0 {
		// A Mark raced this Read and landed after the elapsed reading.
		idle = 0
	}
	return uptime, idle, true
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test -short -race ./internal/activity/`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/activity/
git commit -m "activity: a process's uptime and idle time on the monotonic clock

A duration from two wall readings is wrong by any step between them: a
clockless gateway that boots near 1970 and syncs reported a start 56
years old for the life of the process. Start is the only constructor, so
no caller can hand in a time whose monotonic reading t.UTC() stripped."
```

---

### Task 2: The wire fields

**Files:**
- Modify: `turbostats/wire/bundle.go` (the `Bundle` and `Process` structs)
- Test: `turbostats/wire/bundle_test.go`

**Interfaces:**
- Produces: `wire.Process.UptimeSeconds *int64` (`json:"uptime_seconds,omitempty"`), `wire.Bundle.IdleSeconds *int64` (`json:"idle_seconds,omitempty"`).

- [ ] **Step 1: Write the failing test**

Append to `turbostats/wire/bundle_test.go`:

```go
// A process a second old has an uptime of 0, and a receiver must not read
// that as an older engine that sends none. So both durations are pointers:
// absent means absent, and zero is a reading.
func TestBundle_ZeroDurationsArePresentAndNilIsAbsent(t *testing.T) {
	zero := int64(0)
	with, err := json.Marshal(Bundle{V: Version,
		Process: Process{UptimeSeconds: &zero}, IdleSeconds: &zero})
	assert.NoError(t, err)
	assert.That(t, strings.Contains(string(with), `"uptime_seconds":0`))
	assert.That(t, strings.Contains(string(with), `"idle_seconds":0`))

	without, err := json.Marshal(Bundle{V: Version})
	assert.NoError(t, err)
	assert.That(t, !strings.Contains(string(without), "uptime_seconds"))
	assert.That(t, !strings.Contains(string(without), "idle_seconds"))
}
```

If `strings` is not yet imported in that file, add it to the import block.

- [ ] **Step 2: Run the test to verify it fails**

Run: `go test -short ./turbostats/wire/ -run TestBundle_ZeroDurations`
Expected: FAIL, `unknown field UptimeSeconds in struct literal`.

- [ ] **Step 3: Add the fields**

In `turbostats/wire/bundle.go`, in `Bundle`, directly after `LastActivityAt`:

```go
	// IdleSeconds is how long since the process last did work, from its
	// monotonic clock. Absent before any work, and from engines older than
	// v2026.09.23. A receiver judging work reads this, not LastActivityAt:
	// a wall-clock step between two readings moves their difference, and
	// cannot move this.
	IdleSeconds *int64 `json:"idle_seconds,omitempty"`
```

In `Process`, directly after `StartedAt`:

```go
	// UptimeSeconds is how long this process has run, from its monotonic
	// clock. Absent from engines older than v2026.09.23. StartedAt is for
	// display: a gateway that boots near 1970 and then syncs keeps a 1970
	// StartedAt for the life of the process, and its uptime stays right.
	UptimeSeconds *int64 `json:"uptime_seconds,omitempty"`
```

The version named in both comments is the next engine release. If the release that ships this is named differently, change both comments before merging.

- [ ] **Step 4: Run the wire tests**

Run: `go test -short ./turbostats/wire/`
Expected: PASS, including the existing vector tests, which sign bytes and do not depend on these fields.

- [ ] **Step 5: Commit**

```bash
git add turbostats/wire/
git commit -m "wire: uptime_seconds and idle_seconds, from the monotonic clock

Additive fields; the document stays v1. Pointers, so a zero uptime in a
process's first second is a reading and not an older engine's absence."
```

---

### Task 3: Collect reports the durations

**Files:**
- Modify: `internal/turbostats/bundle.go` (the `Static` struct)
- Modify: `internal/turbostats/collect.go` (inside `Collect`, after the `Process` literal)
- Test: `internal/turbostats/collect_test.go`

**Interfaces:**
- Consumes: `activity.Clock`, `Fake`, `Mark`, `Read` from Task 1; the wire fields from Task 2.
- Produces: `turbostats.Static.Clock *activity.Clock`.

- [ ] **Step 1: Write the failing tests**

Append to `internal/turbostats/collect_test.go`, and add `"github.com/turbolytics/sql-flow/internal/activity"` to its imports:

```go
func TestCollect_ReportsUptimeAndIdleFromTheProcessClock(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	reader, _, _ := provider(t)
	elapsed := 30 * time.Second
	clock := activity.Fake(func() time.Duration { return elapsed })
	clock.Mark()
	elapsed = 90*time.Second + 400*time.Millisecond

	src := runSource(reader, nil)
	src.Static.Clock = clock
	b, err := Collect(context.Background(), src)
	assert.NoError(t, err)

	assert.Equal(t, int64(90), *b.Process.UptimeSeconds)
	assert.Equal(t, int64(60), *b.IdleSeconds)
}

func TestCollect_NoWorkYetOmitsIdleAndKeepsUptime(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	reader, _, _ := provider(t)
	src := runSource(reader, nil)
	src.Static.Clock = activity.Fake(func() time.Duration { return 0 })

	b, err := Collect(context.Background(), src)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), *b.Process.UptimeSeconds)
	assert.That(t, b.IdleSeconds == nil)
}

// A Static built without a clock, as older callers and tests build it,
// sends neither field rather than a zero that reads as a fresh process.
func TestCollect_NoClockSendsNoDurations(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	reader, _, _ := provider(t)
	b, err := Collect(context.Background(), runSource(reader, nil))
	assert.NoError(t, err)
	assert.That(t, b.Process.UptimeSeconds == nil)
	assert.That(t, b.IdleSeconds == nil)
}

// The contract a receiver relies on: idle is at most uptime in every
// bundle, so idle > uptime can only mean a broken producer.
func TestCollect_IdleNeverExceedsUptime(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	reader, _, _ := provider(t)
	var elapsed time.Duration
	clock := activity.Fake(func() time.Duration { return elapsed })
	src := runSource(reader, nil)
	src.Static.Clock = clock
	for i := 0; i < 200; i++ {
		elapsed = time.Duration(i) * 1337 * time.Millisecond
		if i%3 == 0 {
			clock.Mark()
		}
		b, err := Collect(context.Background(), src)
		assert.NoError(t, err)
		if b.IdleSeconds != nil {
			assert.That(t, *b.IdleSeconds <= *b.Process.UptimeSeconds)
		}
	}
}

// The other half of the contract. Collect reads the instruments before it
// stamps sent_at, and both are whole seconds, so activity recorded now can
// never land after the bundle that carries it. A receiver treats activity
// more than a second after sent_at as contradictory.
func TestCollect_ActivityIsNeverAfterSentAt(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	reader, m, _ := provider(t)
	ctx := context.Background()
	m.PipelineLastMessage.Record(ctx, time.Now().Unix())

	b, err := Collect(ctx, runSource(reader, nil))
	assert.NoError(t, err)
	assert.That(t, b.LastActivityAt != nil)
	assert.That(t, !b.LastActivityAt.After(b.SentAt))
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test -short ./internal/turbostats/ -run 'TestCollect_(Reports|NoWork|NoClock|IdleNever|ActivityIsNever)'`
Expected: FAIL, `src.Static.Clock undefined`.

- [ ] **Step 3: Add the field and fill the bundle**

In `internal/turbostats/bundle.go`, add the import `"github.com/turbolytics/sql-flow/internal/activity"` and extend `Static`:

```go
type Static struct {
	ID, Name, Version, Commit, ConfigHash string
	StartedAt                             time.Time
	// IntervalSeconds is the reporter's interval, and 0 without a reporter.
	IntervalSeconds int
	// Clock is the process's monotonic start and last work. Nil sends no
	// uptime_seconds or idle_seconds, which a receiver reads as an older
	// engine rather than as a process that just started.
	Clock *activity.Clock
}
```

In `internal/turbostats/collect.go`, directly after the `b := Bundle{...}` literal and before `if src.Pipeline != nil {`:

```go
	if s.Clock != nil {
		uptime, idle, worked := s.Clock.Read()
		up := int64(uptime / time.Second)
		b.Process.UptimeSeconds = &up
		if worked {
			i := int64(idle / time.Second)
			b.IdleSeconds = &i
		}
	}
```

- [ ] **Step 4: Run the package's tests**

Run: `go test -short -race ./internal/turbostats/`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/turbostats/
git commit -m "turbostats: report uptime and idle from the process clock

Collect reads both from one monotonic reading, so idle never exceeds
uptime. Two contract tests pin what a receiver relies on: idle <= uptime,
and last_activity_at <= sent_at."
```

---

### Task 4: The pipeline and the serve handler mark their work

**Files:**
- Modify: `internal/core/metrics.go` (the `Metrics` struct)
- Modify: `internal/core/turbine.go:711`
- Test: `internal/core/flat_test.go`
- Modify: `internal/serve/metrics.go` (the `metrics` struct and `observeRequest`)
- Modify: `internal/serve/server.go` (after `s.metrics, s.reader = m, reader`)
- Test: `internal/serve/turbostats_test.go`

**Interfaces:**
- Consumes: `activity.Clock`, `Start`, `Mark`, `Read` from Task 1; `Static.Clock` from Task 3.
- Produces: `core.Metrics.Activity *activity.Clock`.

- [ ] **Step 1: Write the failing tests**

Append to `internal/core/flat_test.go`, and add `"github.com/turbolytics/sql-flow/internal/activity"` to its imports:

```go
// The same batch that stamps pipeline_last_message_timestamp marks the
// activity clock, so idle_seconds and last_activity_at describe one event.
func TestCoreConsumeLoop_MarksTheActivityClock(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	src := &fakeSource{batches: [][]Message{messages(10)}}
	r := sdkmetric.NewManualReader()
	m, err := NewMetrics(sdkmetric.NewMeterProvider(sdkmetric.WithReader(r)))
	assert.NoError(t, err)
	m.Activity = activity.Start()
	tb := NewTurbine(src, &fakeHandler{}, &fakeSink{}, 10, time.Second,
		&sync.Mutex{}, PipelineErrorPolicies{}, WithMetrics(m))

	_, err = tb.ConsumeLoop(context.Background(), 0)
	assert.NoError(t, err)

	_, _, worked := m.Activity.Read()
	assert.That(t, worked)
}

// Metrics built without a clock, as every other test builds them, still
// consume: Mark is a no-op on nil.
func TestCoreConsumeLoop_RunsWithoutAnActivityClock(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	src := &fakeSource{batches: [][]Message{messages(10)}}
	tb, _ := meteredTurbine(t, src, &fakeSink{}, 10)
	_, err := tb.ConsumeLoop(context.Background(), 0)
	assert.NoError(t, err)
}
```

Append to `internal/serve/turbostats_test.go`, and add `"github.com/turbolytics/sql-flow/internal/activity"` to its imports:

```go
// A request answered marks the clock the bundle reads, end to end through
// the real server.
func TestServeTurbostats_ARequestSetsIdleSeconds(t *testing.T) {
	coverage.Covers(t, "observability.turbostats.serve")
	static := testStatic
	static.Clock = activity.Start()
	ts := newTestServerWith(t, testServe, WithTurbostats(static, true))

	r := ts.do(t, http.MethodGet, "/turbostats/v1", nil)
	var before wire.Bundle
	assert.NoError(t, json.Unmarshal([]byte(r.raw), &before))
	assert.That(t, before.Process.UptimeSeconds != nil)
	// The route is not a dataset request, so nothing has been served yet.
	assert.That(t, before.IdleSeconds == nil)

	assert.Equal(t, http.StatusOK, ts.get(t, "/v1/datasets/status").status)

	r = ts.do(t, http.MethodGet, "/turbostats/v1", nil)
	var after wire.Bundle
	assert.NoError(t, json.Unmarshal([]byte(r.raw), &after))
	assert.That(t, after.IdleSeconds != nil)
	assert.That(t, *after.IdleSeconds <= *after.Process.UptimeSeconds)
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test -short ./internal/core/ -run TestCoreConsumeLoop_MarksTheActivityClock && go test -short ./internal/serve/ -run TestServeTurbostats_ARequestSetsIdleSeconds`
Expected: FAIL, `m.Activity undefined`; the serve test fails on `after.IdleSeconds != nil`.

Before implementing the serve half, confirm the `/turbostats/v1` route does not itself call `observeRequest`. If it does, the first assertion fails: change it to `before.IdleSeconds != nil` and drop the "nothing has been served yet" comment, because the route's own request is then work.

- [ ] **Step 3: Implement**

In `internal/core/metrics.go`, add the import `"github.com/turbolytics/sql-flow/internal/activity"` and this field at the end of the `Metrics` struct:

```go
	// Activity is the process's monotonic clock, marked with each batch
	// received. Nil in tests and in any caller without a reporter; Mark is
	// a no-op on nil.
	Activity *activity.Clock
```

In `internal/core/turbine.go`, directly after `t.metrics.PipelineLastMessage.Record(ctx, time.Now().Unix())`:

```go
		t.metrics.Activity.Mark()
```

In `internal/serve/metrics.go`, add the import and this field at the end of the `metrics` struct:

```go
	// activity is the process's monotonic clock, marked with each request
	// answered. Nil when the server has no TurboStats static.
	activity *activity.Clock
```

and, in `observeRequest`, directly after `m.flatLastRequest.Record(ctx, time.Now().Unix())`:

```go
	m.activity.Mark()
```

In `internal/serve/server.go`, directly after `s.metrics, s.reader = m, reader`:

```go
	if s.turbostats != nil {
		m.activity = s.turbostats.Clock
	}
```

- [ ] **Step 4: Run both packages' tests**

Run: `go test -short -race ./internal/core/ ./internal/serve/`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/core/ internal/serve/
git commit -m "core, serve: mark the activity clock where the activity gauges record

One call beside each gauge record, so idle_seconds and last_activity_at
describe the same batch or request."
```

---

### Task 5: Start the clock in both commands

**Files:**
- Modify: `internal/cli/run/root.go:127` (the `startedAt` line), `:362` (the `Static` literal), `:439` (after `core.NewMetrics`)
- Modify: `internal/cli/serve/serve.go:96`, `:141`

**Interfaces:**
- Consumes: `activity.Start`, `Clock.StartedAt` from Task 1; `Static.Clock` from Task 3; `Metrics.Activity` from Task 4.

- [ ] **Step 1: Replace the run command's start**

In `internal/cli/run/root.go`, replace:

```go
			// Taken here, once. It is the TurboStats bundle's started_at, and
			// a restart is the control plane seeing that value change.
			startedAt := time.Now().UTC()
```

with:

```go
			// Taken here, once. Its wall time is the bundle's started_at, and
			// a restart is the control plane seeing that value change. The
			// durations the bundle carries come from its monotonic reading,
			// which time.Now().UTC() used to strip.
			clock := activity.Start()
```

In the `turbostats.Static` literal, replace `StartedAt:  startedAt,` with:

```go
				StartedAt:  clock.StartedAt(),
				Clock:      clock,
```

Directly after the `pipelineMetrics, err := core.NewMetrics(meterProvider)` error check:

```go
			pipelineMetrics.Activity = clock
```

Add `"github.com/turbolytics/sql-flow/internal/activity"` to the imports. If `time` is now unused in the file, the compiler says so; remove it only then.

- [ ] **Step 2: Replace the serve command's start**

In `internal/cli/serve/serve.go`, replace:

```go
	// Taken here, once. It is the bundle's started_at, and a restart is a
	// receiver noticing it changed.
	startedAt := time.Now()
```

with:

```go
	// Taken here, once. Its wall time is the bundle's started_at, and a
	// restart is a receiver noticing it changed. Uptime and idle come from
	// its monotonic reading.
	clock := activity.Start()
```

In the `turbostats.Static` literal, replace `StartedAt:  startedAt,` with:

```go
		StartedAt:  clock.StartedAt(),
		Clock:      clock,
```

Add the import. Remove `time` only if the compiler reports it unused.

- [ ] **Step 3: Build and run the full short suite**

Run: `go build ./... && go vet ./... && go test -short -race ./...`
Expected: PASS. Per the repo memory, run `go`, `make` and `docker` outside the sandbox.

- [ ] **Step 4: Check a real bundle by hand**

Run a pipeline with the route on and read it:

```bash
go run ./cmd/sqlflow run dev/config/examples/tumbling.window.yml --turbostats --max-msgs 0 &
sleep 3
curl -s localhost:8000/turbostats/v1 | python3 -m json.tool | grep -E 'uptime_seconds|idle_seconds|started_at'
kill %1
```

Expected: `uptime_seconds` present and a small integer; `idle_seconds` absent until the pipeline receives a message. If that example needs Kafka, start it with `make start-backing-services`, or pick any config under `dev/config/examples` that runs locally. The point is one real bundle with the fields.

- [ ] **Step 5: Commit**

```bash
git add internal/cli/
git commit -m "run, serve: start the activity clock, and stop stripping its reading

run took its start as time.Now().UTC(), which drops the monotonic
reading. Both commands now take it from activity.Start and wire the
clock to Collect and to the code that does work."
```

---

### Task 6: The contract and the changelog

**Files:**
- Modify: `docs/superpowers/specs/2026-09-19-turbostats-contract-amendment-design.md`
- Modify: `CHANGELOG.md` (under `## Unreleased`)

- [ ] **Step 1: Add the contract lines**

Open the amendment spec and find the section that documents the bundle's fields: search for `last_activity_at`. Directly after the paragraph that defines it, add:

```markdown
### Durations (added 2026-09-22)

Two fields carry durations from the process's monotonic clock. A receiver
judges start and work from these, and keeps `started_at` and
`last_activity_at` for display.

- `process.uptime_seconds`: seconds since the process started. Absent from
  older engines.
- `idle_seconds`: seconds since the process last did work. Absent before any
  work, and from older engines.

Every bundle holds three invariants:

- `last_activity_at` is at or before `sent_at`.
- `idle_seconds` is at most `process.uptime_seconds`.
- Activity is per process. It resets on restart. A field that survives a
  restart, such as a durable `sqlflow_progress` arrival time, must not feed
  `last_activity_at` or `idle_seconds`.
```

- [ ] **Step 2: Add the changelog entry**

Under `## Unreleased`, in the `### Added` subsection (create it above `### Changed` if it does not exist):

```markdown
- The TurboStats bundle carries `process.uptime_seconds` and `idle_seconds`,
  measured on the process's monotonic clock. A wall-clock step cannot move
  them, where it moves any difference of two wall readings: a gateway
  without a hardware clock that boots near 1970 and then syncs reported a
  start 56 years old for the life of the process. `sqlflow run` also stops
  stripping its start's monotonic reading. Both fields are additive and the
  document stays v1.
```

- [ ] **Step 3: Commit**

```bash
git add docs/superpowers/specs/2026-09-19-turbostats-contract-amendment-design.md CHANGELOG.md
git commit -m "docs: the duration fields and the invariants every bundle holds"
```

- [ ] **Step 4: Open the PR against main**

Push the branch and open the PR against `main`. The body names the defect (wall-clock durations; `.UTC()` stripping the monotonic reading), the fix, and the evidence: the contract tests and the hand-checked bundle from Task 5. No session links, no attribution lines.
