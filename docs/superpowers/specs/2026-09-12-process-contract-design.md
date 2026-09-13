# Process contract: a bounded drain and a health endpoint with four states

Issue #161. Verified against `main` at be9412c on 2026-09-12.

## What the issue asked for, and what already shipped

| Scope item | State on main |
| --- | --- |
| Documented exit codes by failure class | Done. `errs.ExitCode` maps the taxonomy to 0, 1, 10 to 14. `errs.Retryable` says which a supervisor may restart. Covered by `lifecycle.exit_codes` at unit and release. |
| A config error exits with a terminal code | Done. Class `user` exits 10, `Retryable(10)` is false. |
| Config from file and env, no writes outside the working directory | Done. |
| SIGTERM drains | Done in #159. The final batch flushes through `context.WithoutCancel`. `lifecycle.drain.on_cancel` is enforced. |
| A configurable drain deadline with a distinct exit code | Not done. The drain runs on `context.Background`. Only the sink retry ladder's own deadline bounds it, and a sink that blocks inside one attempt is not bounded at all. `lifecycle.drain.bounded` is `tracked_by: "#161"` with no evidence. |
| A health endpoint with starting, healthy, degraded and failed | Half done. `/healthz` on `:8000` answers `ok` or `stuck`. Nothing distinguishes a pipeline before its first commit, a pipeline whose sink is retrying, or a pipeline that has failed and is on its way out. |

This design covers the last two rows. Nothing else in the issue changes.

## The drain deadline

### The problem

A supervisor gives a process a fixed time to stop. Kubernetes calls it
`terminationGracePeriodSeconds` and sends SIGKILL when it passes. Today
sqlflow's shutdown has no bound of its own, so the operator cannot know
whether a stop finished, and a SIGKILL that lands mid-drain leaves no record
that the tail of the stream was not written.

The shutdown does four things after the signal, all on `context.Background`:

1. The turbine flushes the buffered batch and commits (`ConsumeLoop`, the
   `ctx.Done` branch).
2. Run syncs state so the managers' final poll sees a fresh clock.
3. Each manager polls once more and publishes the windows that closed.
4. Run syncs state again so those deletes are durable.

### The change

One budget for the whole shutdown, started when the signal arrives.

`core.DrainBudget` holds one deadline. Its `Context()` starts the clock on
the first call and returns the same context to every caller after that, so
the turbine's final batch, both state syncs and the managers' final poll
spend the same seconds. `Exceeded()` reports whether the deadline passed.

```go
type DrainBudget struct {
    deadline time.Duration
    once     sync.Once
    ctx      context.Context
    cancel   context.CancelFunc
}

func NewDrainBudget(deadline time.Duration) *DrainBudget
func (b *DrainBudget) Context() context.Context
func (b *DrainBudget) Exceeded() bool
func (b *DrainBudget) Stop()
```

The turbine takes `core.WithDrainBudget(b)`. In the `ctx.Done` branch the
final batch runs on `b.Context()` instead of `context.WithoutCancel(ctx)`. A
turbine built without one gets a budget of `core.DefaultDrainDeadline`, thirty
seconds, so a caller that does not care still gets a bound.

The manager takes `managers.WithDrainBudget(b)`. Its final poll runs on
`b.Context()` instead of `context.Background`.

Run builds the budget from config, hands it to the turbine and every manager,
and runs its two state syncs on `b.Context()`.

### When the deadline passes

The in-flight work fails with a context error. The retry ladder already stops
on a cancelled context and returns the sink's error. A DuckDB statement on a
cancelled context returns a cancellation. Either way:

- The batch's transaction rolls back. Offsets that were committed stay
  committed. The rows the drain could not write replay on the next start,
  which is the same at-least-once outcome as a crash.
- The failure is classified as `system.lifecycle.drain_incomplete`. Run
  returns it, and the process exits `15`.

The classification happens in the turbine, which is the only place that
knows the drain was in progress: when `processBatch` fails and
`b.Context().Err()` is non-nil, the error is wrapped with the new code. The
managers' final poll and the state syncs run inside run's deferred block, so
run checks `b.Exceeded()` after that block and returns the coded error when
the loop itself returned nil.

A second SIGTERM during the drain still kills the process at once. #159
restores default signal handling before the drain starts, and that stays.

### Exit code 15

```go
// ExitDrainIncomplete marks a stop that ran out of time. Retryable: nothing
// was lost, because nothing unwritten was committed, and the next start
// replays it. The code exists so an operator can see that the tail of the
// stream was replayed rather than written.
ExitDrainIncomplete = 15
```

`Retryable(15)` is true. The taxonomy gains one code in a new domain:

| Code | Summary | Action |
| --- | --- | --- |
| `system.lifecycle.drain_incomplete` | The drain deadline passed before the buffered batch or the closed windows were written. | Nothing is lost: what was not written was not committed, and the next start replays it. Raise `pipeline.drain_deadline_seconds` if the sink needs longer, or check the sink. |

`exitCodes` gains the entry. The golden file gains the line. The registry is
append-only, so this is an addition and nothing moves.

### Configuration

```yaml
pipeline:
  drain_deadline_seconds: 30   # default 30, minimum 1
```

The schema declares it as an integer with a minimum of 1. `flushIntervalFor`
has a sibling, `drainDeadlineFor`, that turns absent, zero and negative into
the default.

`sqlflow validate` warns when the drain deadline is shorter than any sink's
`retry.deadline_seconds`, counting the pipeline sink, the DLQ sink and every
manager sink. A short drain deadline is a legitimate choice, so it is a
warning: the operator is told that a drain which hits a retrying sink exits
15 before the ladder finishes. The check is `pipeline.drain_deadline` and it
reads the rendered config, which the schema check has already parsed.

### Evidence

`lifecycle.drain.bounded` loses `tracked_by` and gains `enforced: true`. The
pipeline harness proves it with a seventh verdict: the recording sink gets a
`hang` flag that makes `Flush` block until its context ends, the run uses a
budget of a few hundred milliseconds, and the check holds that

- `ConsumeLoop` returns inside a bound several times the budget,
- the error's code is `system.lifecycle.drain_incomplete`,
- the events end in `flush-failed` with no `commit` after it,
- the rows the sink delivered are zero.

The manager gets the same claim as `manager.drain.bounded`, family
`lifecycle`, class `liveness`, `applies_to: manager`, enforced. The manager
harness proves it with a hanging sink and a cancelled `Start`: `Start`
returns inside the bound with an error, and `Remaining` still holds every
seeded window.

`internal/cli/exit_test.go` gains a case for the mapping, and the coverage
registry tests keep the invariant count and kind set honest.

## Health

### The problem

`/healthz` has two states, and both are about one signal, the commit clock.
An operator asking "should I restart this" gets the right answer from it. An
operator asking "is it doing what I deployed" does not: a pipeline whose sink
is on its third retry, a pipeline that has not committed once since it
started, and a pipeline that has failed and is draining all answer `ok`.

### The change

One endpoint, one `status` field, four values, and a `reason` that says why.

| status | HTTP | when |
| --- | --- | --- |
| `starting` | 200 | No commit yet, and the server started less than three flush intervals ago. |
| `healthy` | 200 | A commit inside the last three intervals, no retry in progress, no error in the last interval. |
| `degraded` | 200 | A sink's retry ladder is in progress, or the loop recorded an error inside the last flush interval. |
| `failed` | 503 | No commit for three intervals. Or the loop returned an error, a manager stopped, or the drain deadline passed, and the process is on its way out. |

200 means "do not restart". 503 means "restart, or let it exit". Today's
`stuck` becomes `failed` with `reason: "no commit for 91s"`. The HTTP codes
for the two existing states do not change, so a liveness probe written
against `ok` and `stuck` keeps behaving; only the body's `status` string
changes.

Rules are evaluated in this order and the first match wins: a recorded
failure, then the commit age, then a retry in progress, then a recent error,
then no commit yet, then healthy.

```json
{"status":"degraded","reason":"sink clickhouse is retrying, attempt 2","commit_age_seconds":4.2}
{"status":"failed","reason":"[system.sink.write_failed] sink rejected the batch: ...","commit_age_seconds":12.0}
```

### Where the signals come from

`core.Progress` gains two fields the turbine already knows:

```go
type Progress struct {
    LastArrival time.Time
    LastCommit  time.Time
    LastError   time.Time
    Messages    int64
    Errors      int64
}
```

`recordError` stamps `LastError` and increments `Errors` under the lock. The
progress table on disk does not change; these two fields are in-memory only,
because they describe this process rather than the durable state.

Retry in progress comes from the ladder. `retrying` already calls `onRetry`
per failed attempt. It gains `onSettle`, called when `Flush` returns after at
least one retry, whether it succeeded or gave up. `sinks.New` takes a new
option:

```go
// RetryEvents is told when a sink's retry ladder is running.
type RetryEvents struct {
    Retry  func(sinkType string, attempt int, err error)
    Settle func(sinkType string)
}

func WithRetryEvents(e RetryEvents) Option
```

The existing counter stays wired; the option's callbacks run beside it.

`internal/cli/run/health.go` holds the state the endpoint reads:

```go
type health struct {
    mu       sync.Mutex
    failure  error
    retrying map[string]int   // sink type -> attempt, present while a ladder runs
}

func (h *health) Fail(err error)
func (h *health) Retry(sinkType string, attempt int, err error)
func (h *health) Settle(sinkType string)
func (h *health) Snapshot() (failure error, retrying map[string]int)
```

Run calls `Fail` when `ConsumeLoop` returns an error, when a manager's cause
fires, and when the drain budget is exceeded. It passes `Retry` and `Settle`
to every `sinks.New` call: the pipeline sink, the DLQ sink and each manager
sink.

`newHTTPMux` takes a `healthFunc func() healthSnapshot` alongside the
progress function. The snapshot bundles the failure and the retrying map;
the progress and its ages come from the existing `progressFunc`. The status
rules live in one pure function,
`healthStatus(snapshot, interval) (status, reason string, code int)`, so the
table above is one test.

### What does not change

`/stats` keeps its shape and gains the two new progress fields under
`progress`. `/metrics` and `/turbostats/v1` do not change. The endpoint stays
absent when there is no progress source, as today.

## Documentation

On turbolytics.io:

- New page `operations/running-in-production.md`, order 2. Sections: exit
  codes (the table with a retryable column), the health endpoint (the four
  states, a curl example, a Kubernetes liveness probe), stopping a pipeline
  (SIGTERM, the drain, `drain_deadline_seconds`, exit 15).
- `introduction/configuration.md` adds `drain_deadline_seconds` to the
  pipeline section and to the every-option listing.
- `operations/handling-errors.md` links the new page from the RAISE bullet
  that mentions exit `10`, and drops the paragraph that says an encode
  failure is retried. #269 fixed that after the paragraph was written.

In sql-flow: `CHANGELOG.md` gains entries for the deadline, the exit code and
the health states. The tumbling window docs on the site already say a refused
publish stops the process, from #270.

## Coverage

- `docs/coverage/invariants.yml`: `lifecycle.drain.bounded` enforced, no
  `tracked_by`; `manager.drain.bounded` added.
- `docs/coverage/features.yml`: `lifecycle.health`, "The health endpoint
  reports starting, healthy, degraded and failed", requires `[unit,
  release]`. The release test reads `/healthz` from the image and holds that
  a running pipeline answers `healthy` with HTTP 200.
- Status files are regenerated from CI's report artifacts after the PR's
  first run, never locally.

## Out of scope

- A readiness endpoint separate from liveness. One endpoint with a `status`
  string serves both, and nothing routes traffic to a pipeline.
- Health for the Python engine.
- Bounding the start. A source or sink that never answers during `Start` is
  #268's problem, and the probe already fails the start once for a sink.
