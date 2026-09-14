# Watermark manager implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the user-written window manager with an engine-owned watermark manager, built from a `window` declaration, that closes buckets against a persisted event-time watermark on its own connection and applies a late-row policy.

**Architecture:** `config.Window` replaces `config.TableManager`. `managers.Watermark` replaces `managers.Tumbling`: it generates its SQL from the declaration, keeps its watermark in `sqlflow_windows`, runs on a second DuckDB connection with autocommit off, and keeps the poll loop, final poll and drain budget from #270 and #273. `run` opens one connection per window. The harness's manager subject gains a late-row seed and an uncommitted-rows hook, and three invariants join the four.

**Tech Stack:** Go, DuckDB through ADBC, the conformance harness, the coverage registry, pytest tooling and release suites.

**Spec:** `docs/superpowers/specs/2026-09-13-windows-owned-by-the-engine-design.md`

## Global Constraints

- No em dashes. No attribution lines in commits. Every Go test calls `coverage.Covers` first.
- Durations in config are `_seconds` integers.
- No `now()` in any SQL the manager runs. The idle test compares Go's clock with `last_arrival` read from `sqlflow_progress`.
- `sqlflow_windows` carries no index. One row per window, updated in place.
- The manager's connection has autocommit off, and every poll ends its transaction, commit or rollback, before returning.
- Coverage status files come from CI's report artifacts, never from a local run.
- Worktree: `$S/sqlflow-watermark`, branch `feat/watermark`.

---

### Task 1: The declaration

**Files:** `internal/config/config.go`, `internal/validate/schemas/config.json` and `internal/cli/testdata/config_example.golden` via `make schema`, every file under `dev/config/examples` and `.claude/skills/slow-soak/slow.yml` that carries `manager:`, `internal/validate/drain.go`, `internal/validate/drain_test.go`.

**Produces:**

```go
// Window is a tumbling window the engine closes for the user.
type Window struct {
	TimeColumn       string `yaml:"time_column"`
	SizeSeconds      int    `yaml:"size_seconds" jsonschema:"minimum=1"`
	GraceSeconds     int    `yaml:"grace_seconds,omitempty" jsonschema:"minimum=0"`
	IdleCloseSeconds int    `yaml:"idle_close_seconds,omitempty" jsonschema:"minimum=0"`
	LateRows         string `yaml:"late_rows,omitempty" jsonschema:"enum=drop,enum=reemit"`
	PollIntervalSecs int    `yaml:"poll_interval_seconds,omitempty" jsonschema:"minimum=1"`
	EmitSQL          string `yaml:"emit_sql,omitempty"`
	Sink             Sink   `yaml:"sink"`
}
```

`TableSQL.Manager` becomes `TableSQL.Window *Window \`yaml:"window,omitempty"\``. `TableManager` and `TumblingWindow` are deleted.

- [ ] Add `Window`, delete the two old types, run `make schema` and `UPDATE_GOLDEN=1 go test ./internal/cli/ -run Example`.
- [ ] Convert the five examples and the soak config. Each keeps its handler and sink; the two predicates become the declaration, and the collect's projection becomes `emit_sql` over `closed`. Sizes: `tumbling.window.yml` and `kafka.stateful.window.yml` are hourly buckets (3600, grace 60 and 3600, idle 60 and 3600). The Bluesky ones are 60, grace 60, idle 60. `benchmark.stateful.mem.yml` as its comment says.
- [ ] `drain.go` reads `table.Window.Sink`. `drain_test.go`'s manager config becomes a `window` block.
- [ ] `go test ./internal/cli/ ./internal/config/ ./internal/validate/ ./internal/schema/` passes. Commit: `config: a window declaration replaces the manager block`.

### Task 2: The watermark store

**Files:** create `internal/managers/store.go`, `internal/managers/store_test.go`; modify `internal/core/stats.go` (exclude `sqlflow_windows` from user tables).

**Produces:**

```go
const windowsTable = "sqlflow_windows"

type Store struct{ conn adbc.Connection }
func NewStore(conn adbc.Connection) *Store
func (s *Store) Init(ctx context.Context) error                                   // CREATE TABLE IF NOT EXISTS, no index
func (s *Store) Load(ctx context.Context, name string) (watermark time.Time, ok bool, err error)
func (s *Store) Save(ctx context.Context, name string, watermark, closedAt time.Time) error // UPDATE, or INSERT when absent; does not commit
```

`core.EngineTables` becomes the one list of engine table names, used by `userTables` and by the store.

- [ ] Tests: init twice is safe; load of an unknown window is `ok == false`; save then load round-trips the instant across a reopened file; the table has no index (`duckdb_indexes()` empty); `CollectStateStats` does not list it.
- [ ] Commit: `managers: sqlflow_windows persists each window's watermark`.

### Task 3: The watermark manager

**Files:** create `internal/managers/watermark.go`, `internal/managers/watermark_test.go`, `internal/managers/sql.go`, `internal/managers/sql_test.go`; delete `internal/managers/tumbling.go` and `tumbling_test.go`; modify `internal/managers/window_leak_test.go`, `internal/managers/conformance_test.go` (Task 5 finishes it).

**Produces:**

```go
type LatePolicy string
const (LateReemit LatePolicy = "reemit"; LateDrop LatePolicy = "drop")

type Declaration struct {
	Table, TimeColumn string
	Size, Grace, IdleClose time.Duration
	Late LatePolicy
	EmitSQL string   // "" means SELECT * FROM closed
}

type Watermark struct{ ... }
func NewWatermark(conn adbc.Connection, d Declaration, poll time.Duration, sink core.Sink, opts ...Option) (*Watermark, error)
func WithLogger(*zap.Logger) Option
func WithDrainBudget(*core.DrainBudget) Option
func WithMeterProvider(metric.MeterProvider) Option
func WithClock(func() time.Time) Option
func (w *Watermark) Start(ctx context.Context) error   // as Tumbling.Start, with finalPoll
func (w *Watermark) Poll(ctx context.Context) error
func (w *Watermark) Watermark() (time.Time, bool)      // last persisted value, for tests and /stats
```

Generated SQL, all parameterised by table, time column, and literal timestamps rendered with `utcLiteral`:

- newest: `SELECT epoch_us(max(<col>)) FROM <table>`
- last arrival: `SELECT epoch_us(last_arrival) FROM sqlflow_progress`
- late count: `SELECT count(*) FROM <table> WHERE <col> + INTERVAL '<size>' SECOND <= TIMESTAMPTZ '<prev>'`
- late delete: `DELETE FROM <table> WHERE <col> + INTERVAL '<size>' SECOND <= TIMESTAMPTZ '<prev>'`
- collect: `WITH closed AS (SELECT * FROM <table> WHERE <col> + INTERVAL '<size>' SECOND <= TIMESTAMPTZ '<wm>') <emit>`
- delete: `DELETE FROM <table> WHERE <col> + INTERVAL '<size>' SECOND <= TIMESTAMPTZ '<wm>'`

Poll: begin (first statement opens the transaction), load previous, drop or keep late rows, read newest and last arrival, compute the watermark per the spec's two rules, return after a rollback when nothing moved and nothing is to re-emit, collect and emit through the sink, then delete, save, commit. Any error rolls back. `finalPoll` and `drainError` as in `tumbling.go` today.

Metrics through the provider: `window_watermark_seconds{window}` gauge, `window_closed_total{window}` counter, `window_late_rows_total{window,policy}` counter.

- [ ] `sql_test.go`: each generated statement is byte-exact for one declaration.
- [ ] `watermark_test.go`, against a real DuckDB with a second connection playing the pipeline: closes a bucket the stream moved past by the grace and not one it did not; idle close closes everything after `IdleClose` of silence and nothing before; the watermark persists across a new `Watermark` over the same store; it never regresses when newest drops after deletes; `drop` deletes late rows without a flush and counts them; `reemit` publishes them; uncommitted rows on the pipeline connection are not published; a failed flush leaves rows and watermark unchanged; a poll that finds nothing ends its transaction so the next poll sees new rows; Start, cancel, final poll, and drain deadline as the tumbling tests had them.
- [ ] Leak test: the scenario builds a `Declaration` on a `TIMESTAMPTZ` bucket (`to_timestamp(time_us / 1000000)` bucketed to the minute) instead of collect and delete SQL, on a second connection.
- [ ] Commit: `managers: the watermark manager closes windows against a persisted event-time watermark`.

### Task 4: Wiring in run

**Files:** `internal/cli/run/managers.go`, `internal/cli/run/root.go`, `internal/cli/run/row_roles_test.go`, `internal/cli/run/managers_test.go`.

- [ ] `buildManagedTables(ctx, conf, db *duckdb.DB, l, mp, budget, events)` opens `db.Connect` per window, disables autocommit on it, builds `NewWatermark`, and returns the managers plus a close func for the connections. The pipeline lock is no longer passed. `root.go` calls it with `db`, and closes the window connections before the state reader connection closes, for the same use-after-free reason.
- [ ] Every window's `Store.Init` runs on the pipeline connection before autocommit is turned off there, beside `ProgressStore.Init`, so the table exists under autocommit.
- [ ] Tests updated to the new signature. Commit: `run: one connection per window`.

### Task 5: The harness

**Files:** `internal/conformance/manager.go`, `internal/managers/conformance_test.go`, `docs/coverage/invariants.yml`, `docs/coverage/integrations/manager.tumbling_window.yml`, `docs/coverage/features.yml`, `internal/coverage/registry_test.go` if it names the integration.

**Produces:**

```go
type ManagerSubject struct {
	Integration string
	New         func(t *testing.T, sink core.Sink, poll time.Duration, budget *core.DrainBudget, late string) Manager
	Seed        func(t *testing.T, n int)              // n closed buckets, the stream idle past idle_close
	SeedLate    func(t *testing.T, n int)              // n rows for a bucket below the watermark
	Remaining   func(t *testing.T) int64
	Uncommitted func(t *testing.T, n int) (release func())  // optional
}
```

- [ ] Three checks: `checkWatermarkNeverRegresses` (poll, record, seed newer, build a second manager over the same store, poll, compare); `checkCommittedRowsOnly` (skip without Uncommitted; hold n rows open on the pipeline connection, poll, the sink saw none of them); `checkLatePolicyHolds` (drop: seed, poll, seed late, poll, flushes stay 1 and remaining 0; reemit: flushes 2).
- [ ] `invariants.yml`: `manager.watermark.never_regresses`, `manager.close.committed_rows_only`, `manager.late.policy_holds`, all enforced. The integration file is renamed `manager.watermark.yml` with `feature: manager.window`; `features.yml` renames the feature; the old status file is deleted and CI writes the new one.
- [ ] `make coverage-page`, tooling tests pass. Commit: `conformance: three invariants for the watermark manager`.

### Task 6: Validate

**Files:** create `internal/validate/window.go`, `internal/validate/window_test.go`; modify `internal/validate/validate.go`.

- [ ] Check `tables.window`: `manager:` present anywhere under `tables.sql` is an error whose message names `window:` and the keys; `time_column` appears in the table's `CREATE` followed by `TIMESTAMPTZ` (textual, case-insensitive); `emit_sql` mentions `closed`. Diagnostics carry the line of the `window` key.
- [ ] Commit: `validate: the window declaration is checked, and the manager block is refused with its replacement`.

### Task 7: Docs, metrics table, changelog

**Files:** `README.md` (Tumbling windows section and the metrics table), `internal/cli/run/metrics_test.go` (the exported series list), `CHANGELOG.md`.

- [ ] README: the section describes the declaration, the watermark, the late policy and the guarantees, in the voice of the rest of the file. The instrument count and table gain the three window series.
- [ ] Changelog under Unreleased: Added the window declaration and the watermark; Removed the `manager` block and both predicates.
- [ ] `make test-go` clean. Commit: `docs: windows are declared, and the engine closes them`.

### Task 8: End to end

- [ ] Local Kafka, the converted `kafka.stateful.window.yml`: produce, watch the window close on the stream clock, restart, confirm no republish.
- [ ] The Bluesky Kafka windowed example against the live firehose with `late_rows: drop`, sink into a Postgres table with a primary key on `(bucket, kind)`, for ten minutes: no conflict, one row per bucket and kind, then a restart mid-run with the state path, and still no conflict.
- [ ] PR #281 body updated with the results. Coverage status from CI.
