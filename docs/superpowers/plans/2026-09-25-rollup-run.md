# `sqlflow rollup run` Implementation Plan (Plan 2a)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `sqlflow rollup run` installs what a rollups file declares, takes a leader lock, and fills every pending table in chunks beside a live pipeline, and reports `/healthz` and metrics. `sqlflow rollup install` stops being hidden.

**Architecture:** `internal/rollup` gains the backfill: a pure chunk planner and a `BackfillStep` that fills one chunk in one transaction under the triggers' own bucket locks. A new package, `internal/rollup/daemon`, holds the process: it installs, competes for a session advisory lock per rollup, fills chunk by chunk while it leads, and serves `/metrics` and `/healthz` on `:8000` behind `--metrics prometheus`. The cobra command is a thin wrapper.

**Tech Stack:** Go 1.26, pgx/v5, cobra, zap, OpenTelemetry metric SDK with its Prometheus exporter, testcontainers-go Postgres module, zeebo/assert.

**Spec:** `docs/superpowers/specs/2026-09-24-rollup-daemon-design.md`

## Where this plan sits

| Plan | Scope | State |
|---|---|---|
| 1 | Config blocks, planner, state table, `sqlflow rollup install` (hidden) | Merged, #380 |
| **2a, this one** | Chunked backfill, `sqlflow rollup run` with the leader lock, `/healthz` and metrics; `install` unhidden | |
| 2b | The verify pass and `sqlflow rollup verify`, `internal/freshness` and the store identity, `degraded` in `/healthz` | |
| 3 | The TurboStats `freshness` list and `rollup` section, and the release test | |
| 4 | `sqlflow rollup test` | |
| 5 | A release, then the Bluesky demo, the Render template, and the parity proof | |

## Global Constraints

- One branch, `feat/rollup-run`, from main at `b200d4c`, carrying this plan, the spec amendments and the code, in one PR against main. Gate every push: `[ "$(gh pr view N --json state --jq .state)" = OPEN ]`.
- Postgres 15 or later. Integration tests run `postgres:18`.
- No new error codes. A database error is `system.rollup.internal`, a failed connect `system.rollup.unreachable`, a bad flag or empty DSN `user.config.invalid`.
- Lock keys, exact: the install lock `hashtextextended('sqlflow_rollup_install', 0)`; a bucket lock `hashtextextended('<table>:' || <bucket epoch seconds>, 0)`, the key the generated triggers take; a leader lock `hashtextextended('sqlflow_rollup_leader:' || <rollup name>, 0)`.
- A backfill chunk: READ COMMITTED, the install lock taken shared, then `SET LOCAL lock_timeout` to `installLockTimeout` (2 s). One UTC day of source time, halved until it takes at most `maxChunkLocks` = 1000 advisory locks, never narrower than its table's width. Newest first. Progress recorded in the same transaction.
- The daemon: interval 15 s by default; `failed` after 3 intervals with no database round trip; `/metrics` and `/healthz` on `:8000` only with `--metrics prometheus`, as `sqlflow run` does.
- Metric instruments, exact names: `rollup_backfill_buckets`, `rollup_backfill_chunk_duration` (unit `s`), `rollup_errors` with attribute `phase` (`install`, `backfill`, `lock`), `rollup_leader_acquired`. Attributes `rollup` and `table` where a table is involved.
- Unit tests are `TestCliRollupRun_*`; integration tests `TestIntegrationRollupRun_*`, skip under `-short`, start their own container. Every one calls `coverage.Covers(t, "cli.rollup_run")`.
- Run `go`, `make`, `docker` and `gh` outside the sandbox. One heredoc per shell command; commit messages go in a file and `git commit -F`.
- Prose follows the repo's `CLAUDE.md`. Comments explain why.

## Review Focus

1. **A backfill beside live writers** loses no write and never holds the pipeline longer than one lock timeout. Task 2, `TestIntegrationRollupRun_ChunkedBackfillLosesNoConcurrentWrite` and `TestIntegrationRollupRun_ABackfillChunkGivesUpOnAnOpenWrite`.
2. **A grain added after history exists**, whose buckets span several chunks, such as a week built from days, fills to exactly its source. Task 2, `TestIntegrationRollupRun_BackfillOfAGrainAddedLater`.
3. **A daemon stopped mid-backfill** exits 0 promptly, and the next one resumes where it stopped and ends exact. Task 4, `TestIntegrationRollupRun_ADaemonStoppedMidBackfillIsResumed`.
4. **Two daemons at once**, as during every Render deploy: one leads, and when the leader's session dies the other leads within two intervals. Task 4, `TestIntegrationRollupRun_OneDaemonLeadsAndTheOtherTakesOver`.
5. **A database that goes away and comes back:** `/healthz` reports `failed` after three intervals, then `healthy` again with no restart. Task 4, `TestIntegrationRollupRun_AnUnreachableDatabaseFailsHealthAndRecovers`.

## File Structure

```
internal/rollup/backfill.go                    chunk planner, PendingBackfills, BackfillStep
internal/rollup/backfill_test.go               planner unit tests
internal/rollup/backfill_integration_test.go   BackfillStep against Postgres
internal/rollup/daemon/health.go               healthStatus rules, the state /healthz reads
internal/rollup/daemon/health_test.go
internal/rollup/daemon/metrics.go              meter provider, instruments, HTTP server
internal/rollup/daemon/leader.go               tryLead
internal/rollup/daemon/daemon.go               Options, New, Run, the loop
internal/rollup/daemon/daemon_integration_test.go
internal/cli/rollup/run.go                     sqlflow rollup run
internal/cli/rollup/install.go                 unhidden
internal/cli/rollup/rollup.go                  register run, update Long
internal/cli/rollup/run_test.go
internal/cli/rollup/run_integration_test.go
README.md, CHANGELOG.md                        docs
docs/superpowers/specs/2026-09-24-rollup-daemon-design.md   amendments
```

---

### Task 1: The chunk planner

**Files:**
- Create: `internal/rollup/backfill.go`
- Create: `internal/rollup/backfill_test.go`

**Interfaces:**
- Consumes: `edge`, `edges`, `Table` (internal/rollup/sql.go); `config.Rollup.Ladder()`, `config.ParseServeDuration`.
- Produces (unexported, used by Task 2): `const maxChunkLocks = 1000`; `var binOrigin time.Time`; `func floorTo(t time.Time, span time.Duration) time.Time`; `func cascadeWidths(r config.Rollup, e edge) []time.Duration`; `func chunkLocks(span time.Duration, widths []time.Duration) int`; `func chunkSpan(r config.Rollup, e edge) time.Duration`; `func fromWidth(r config.Rollup, e edge) time.Duration`; `func findEdge(r config.Rollup, table string) (edge, bool)`.

- [ ] **Step 1: Write the failing tests**

Create `internal/rollup/backfill_test.go`:

```go
package rollup

import (
	"testing"
	"time"

	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

// secondsRollup is a source written every second: a day of it is far more
// buckets than one chunk may lock.
func secondsRollup() config.Rollup {
	return config.Rollup{
		Name:   "events",
		Source: config.RollupSource{Table: "events_1s", TimeColumn: "second", Grain: "1s"},
		Grains: map[string]config.RollupGrain{"5s": {From: "1s"}, "1m": {From: "5s"}},
		DimensionSets: []config.RollupDimensionSet{{
			Name: "events", Dimensions: []string{},
			Measures: map[string]config.RollupMeasure{"n": {Type: "sum", Column: "n"}},
		}},
	}
}

func TestCliRollupRun_AChunkIsADayWhenADayFitsTheLockBudget(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")

	r := exampleRollup(t)
	e, ok := findEdge(r, "posts_by_lang_5m")
	assert.True(t, ok)
	// 5m, 15m, 1h, 6h and 1d buckets of a day, each plus one at an edge.
	assert.Equal(t, 289+97+25+5+2, chunkLocks(24*time.Hour, cascadeWidths(r, e)))
	assert.Equal(t, 24*time.Hour, chunkSpan(r, e))
}

func TestCliRollupRun_AChunkHalvesUntilItFitsTheLockBudget(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")

	r := secondsRollup()
	e, ok := findEdge(r, "events_5s")
	assert.True(t, ok)
	// 90 minutes locks 1081 + 91; 45 minutes locks 541 + 46.
	assert.Equal(t, 45*time.Minute, chunkSpan(r, e))
	assert.That(t, chunkLocks(chunkSpan(r, e), cascadeWidths(r, e)) <= maxChunkLocks)
}

func TestCliRollupRun_TheCascadeIsEveryGrainBuiltFromTheTable(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")

	r := exampleRollup(t)
	e, _ := findEdge(r, "posts_by_lang_1h")
	assert.DeepEqual(t, []time.Duration{time.Hour, 6 * time.Hour, 24 * time.Hour}, cascadeWidths(r, e))
	e, _ = findEdge(r, "posts_total_1d")
	assert.DeepEqual(t, []time.Duration{24 * time.Hour}, cascadeWidths(r, e))
}

func TestCliRollupRun_FromWidthIsTheWidthOfTheRowsATableReads(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")

	r := exampleRollup(t)
	e, _ := findEdge(r, "posts_by_lang_5m")
	assert.Equal(t, time.Minute, fromWidth(r, e))
	e, _ = findEdge(r, "posts_total_1d")
	assert.Equal(t, 6*time.Hour, fromWidth(r, e))
	_, ok := findEdge(r, "posts_by_lang_7d")
	assert.False(t, ok)
}

// Bins start at the generated SQL's origin, so Go and Postgres agree on
// every boundary, before the origin too.
func TestCliRollupRun_FloorToBinsFromTheSQLOrigin(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")

	at := func(s string) time.Time { v, _ := time.Parse(time.RFC3339, s); return v }
	assert.Equal(t, at("2026-09-12T00:00:00Z"), floorTo(at("2026-09-12T13:07:00Z"), 24*time.Hour).UTC())
	assert.Equal(t, at("2026-09-12T12:00:00Z"), floorTo(at("2026-09-12T13:07:00Z"), 6*time.Hour).UTC())
	assert.Equal(t, at("1999-12-31T00:00:00Z"), floorTo(at("1999-12-31T13:00:00Z"), 24*time.Hour).UTC())
	// A week bins from 2000-01-01, a Saturday.
	assert.Equal(t, at("2026-09-12T00:00:00Z"), floorTo(at("2026-09-14T09:00:00Z"), 7*24*time.Hour).UTC())
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test -short ./internal/rollup/ -run 'TestCliRollupRun_(AChunk|TheCascade|FromWidth|FloorTo)'`
Expected: FAIL to compile with `undefined: findEdge`, `undefined: chunkLocks`, `undefined: chunkSpan`.

- [ ] **Step 3: Write the planner**

Create `internal/rollup/backfill.go`:

```go
package rollup

import (
	"time"

	"github.com/turbolytics/sql-flow/internal/config"
)

// maxChunkLocks bounds the advisory locks one backfill chunk takes: the
// buckets of its table it touches, and the buckets its upserts' triggers
// lock in every coarser grain built from it. The default lock table holds
// 6,400 locks for the whole server, and a chunk shares it with the
// pipeline's writes.
const maxChunkLocks = 1000

// binOrigin is the instant the generated SQL bins from, so a chunk boundary
// computed here and a bucket computed in Postgres agree.
var binOrigin = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)

// floorTo is the start of t's span-wide bin from binOrigin, as date_bin
// computes it, before the origin as well as after.
func floorTo(t time.Time, span time.Duration) time.Time {
	d := t.Sub(binOrigin)
	q := d / span
	if d < 0 && d%span != 0 {
		q--
	}
	return binOrigin.Add(q * span)
}

// cascadeWidths is e's width, then the width of each grain its upserts
// reach through the triggers: every grain built from e's grain, and every
// grain built from those. The ladder runs narrowest first, so a grain's
// from is decided before the grain.
func cascadeWidths(r config.Rollup, e edge) []time.Duration {
	fed := map[string]bool{e.Grain.Name: true}
	out := []time.Duration{e.Grain.Width}
	for _, g := range r.Ladder() {
		if fed[g.From] && !fed[g.Name] {
			fed[g.Name] = true
			out = append(out, g.Width)
		}
	}
	return out
}

// chunkLocks is the most advisory locks a chunk spanning span takes: at
// each width, the buckets the span covers, plus one it may straddle.
func chunkLocks(span time.Duration, widths []time.Duration) int {
	n := 0
	for _, w := range widths {
		n += int((span+w-1)/w) + 1
	}
	return n
}

// chunkSpan is how much source time one chunk of e's table covers: a UTC
// day, halved until the chunk fits maxChunkLocks. It never drops below the
// table's own width, where a chunk locks about one bucket per grain.
func chunkSpan(r config.Rollup, e edge) time.Duration {
	widths := cascadeWidths(r, e)
	span := 24 * time.Hour
	for span/2 >= e.Grain.Width && chunkLocks(span, widths) > maxChunkLocks {
		span /= 2
	}
	return span
}

// fromWidth is the width of the rows e's table is built from: the source
// grain's, or the finer rollup grain's.
func fromWidth(r config.Rollup, e edge) time.Duration {
	if e.Grain.From == r.Source.Grain {
		w, err := config.ParseServeDuration(r.Source.Grain)
		if err == nil {
			return w
		}
	}
	for _, g := range r.Ladder() {
		if g.Name == e.Grain.From {
			return g.Width
		}
	}
	return e.Grain.Width
}

// findEdge is the declared table named table.
func findEdge(r config.Rollup, table string) (edge, bool) {
	for _, e := range edges(r) {
		if e.Table == table {
			return e, true
		}
	}
	return edge{}, false
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test -short ./internal/rollup/ -run 'TestCliRollupRun_(AChunk|TheCascade|FromWidth|FloorTo)' -v`
Expected: PASS, 5 tests.

- [ ] **Step 5: Commit**

Write `$TMPDIR/msg.txt`:

```
rollup: plan backfill chunks within the lock budget

A chunk takes the triggers' lock on every bucket it touches, at its own
grain and at every grain its upserts cascade to. A day of 5-minute
buckets and everything above it is 418 locks; a day of 5-second buckets
is 18,722, three times the server's default lock table. The planner
halves a day until a chunk takes at most 1,000.
```

Run: `git add internal/rollup/backfill.go internal/rollup/backfill_test.go && git commit -q -F "$TMPDIR/msg.txt"`

---

### Task 2: `BackfillStep`

**Files:**
- Modify: `internal/rollup/backfill.go`
- Create: `internal/rollup/backfill_integration_test.go`

**Interfaces:**
- Consumes: Task 1's planner; `readState`, `querier` (postgres_state.go); `writeUpsert`, `bin`, `interval`, `quote` (sql.go, postgres.go); `installLockTimeout` (install.go); test helpers `startRollupPostgres`, `connectIn`, `execSQL`, `count`, `at`, `minute`, `writeMinutes`, `flushMinutes`, `assertGrainsEqualSource`, `loadExample`, `mustInstall`, `stateOf`, `withLockTimeout`.
- Produces:
  - `type Pending struct { Rollup config.Rollup; Table string; Done *time.Time }`.
  - `func PendingBackfills(ctx context.Context, conn *pgx.Conn, conf *config.RollupsConf) ([]Pending, error)`.
  - `type Step struct { Rollup, Table string; From, To time.Time; Buckets int64; Complete bool; Remaining time.Duration }`.
  - `func BackfillStep(ctx context.Context, conn *pgx.Conn, p Pending) (Step, error)`.
  - Unexported: `func touchedBuckets(ctx context.Context, q querier, r config.Rollup, e edge, lo, hi time.Time) ([]time.Time, error)`; `func lockBuckets(ctx context.Context, tx pgx.Tx, table string, buckets []time.Time) error`, which the tests call to hold a bucket's lock.

- [ ] **Step 1: Write the failing tests**

Create `internal/rollup/backfill_integration_test.go`:

```go
package rollup

// Backfill against a real Postgres: history written before the triggers
// existed, a grain added later, a chunk beside open and live writers.

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

// history writes one row per minute per language from from to to, straight
// into the source. Before an install no trigger sees them.
func history(t *testing.T, conn *pgx.Conn, from, to string) {
	t.Helper()
	execSQL(t, conn, `INSERT INTO posts_per_minute_by_lang (bucket, lang, posts)
SELECT g, l, (extract(minute FROM g)::int % 7) + 1
FROM generate_series('`+from+`'::timestamptz, '`+to+`'::timestamptz, interval '1 minute') AS g,
     unnest(ARRAY['en', 'ja', 'de']) AS l`)
}

// fillAll runs chunks until nothing is pending, and returns them in order.
func fillAll(t *testing.T, conn *pgx.Conn, conf *config.RollupsConf) []Step {
	t.Helper()
	var steps []Step
	for i := 0; i < 10_000; i++ {
		pending, err := PendingBackfills(context.Background(), conn, conf)
		assert.NoError(t, err)
		if len(pending) == 0 {
			return steps
		}
		step, err := BackfillStep(context.Background(), conn, pending[0])
		assert.NoError(t, err)
		steps = append(steps, step)
	}
	t.Fatal("the backfill never finished")
	return nil
}

func TestIntegrationRollupRun_BackfillFillsHistoryNewestDayFirst(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	srv := startRollupPostgres(t)
	history(t, srv.conn, "2026-09-10T00:00:00Z", "2026-09-12T23:59:00Z")
	mustInstall(t, srv.conn, loadExample(t))

	steps := fillAll(t, srv.conn, loadExample(t))
	// Two tables built from the source, three days each.
	assert.Equal(t, 6, len(steps))
	assert.Equal(t, "posts_by_lang_5m", steps[0].Table)
	assert.That(t, steps[0].From.Equal(at("2026-09-12T00:00:00Z")))
	assert.That(t, steps[0].To.Equal(at("2026-09-13T00:00:00Z")))
	assert.Equal(t, int64(288), steps[0].Buckets)
	assert.Equal(t, 48*time.Hour, steps[0].Remaining)
	assert.True(t, steps[2].Complete)
	assert.Equal(t, 0, len(stateOf(t, srv.conn, "posts").Backfill))
	assertGrainsEqualSource(t, srv.conn)
}

func TestIntegrationRollupRun_BackfillResumesWhereItStopped(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	srv := startRollupPostgres(t)
	history(t, srv.conn, "2026-09-10T00:00:00Z", "2026-09-12T23:59:00Z")
	mustInstall(t, srv.conn, loadExample(t))

	pending, err := PendingBackfills(context.Background(), srv.conn, loadExample(t))
	assert.NoError(t, err)
	_, err = BackfillStep(context.Background(), srv.conn, pending[0])
	assert.NoError(t, err)

	// A crash here loses nothing: the chunk and its progress committed
	// together.
	done := stateOf(t, srv.conn, "posts").Backfill["posts_by_lang_5m"]
	assert.That(t, done != nil && done.Equal(at("2026-09-12T00:00:00Z")))
	steps := fillAll(t, srv.conn, loadExample(t))
	assert.Equal(t, 5, len(steps))
	assert.That(t, steps[0].To.Equal(at("2026-09-12T00:00:00Z")))
	assertGrainsEqualSource(t, srv.conn)
}

// A week built from days spans several chunks, and 2026-09-12 starts a new
// week, so the history falls in two weeks. Each chunk re-merges a whole
// week from its days, whatever part of it the chunk covers.
func TestIntegrationRollupRun_BackfillOfAGrainAddedLater(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	srv := startRollupPostgres(t)
	mustInstall(t, srv.conn, loadExample(t))
	// The first install marks the 5m tables; over an empty source each
	// completes in one chunk. History written after it reaches every grain
	// through the triggers.
	fillAll(t, srv.conn, loadExample(t))
	history(t, srv.conn, "2026-09-10T13:00:00Z", "2026-09-12T23:59:00Z")

	withWeek := loadExample(t)
	withWeek.Rollups[0].Grains["7d"] = config.RollupGrain{From: "1d"}
	assert.DeepEqual(t, []string{"posts_by_lang_7d", "posts_total_7d"}, mustInstall(t, srv.conn, withWeek).Rollups[0].Plan.Backfill)

	for _, s := range fillAll(t, srv.conn, withWeek) {
		assert.That(t, s.Table == "posts_by_lang_7d" || s.Table == "posts_total_7d")
	}
	week := "date_bin(INTERVAL '168 hours', bucket, TIMESTAMPTZ '2000-01-01 00:00:00+00')"
	for table, want := range map[string]string{
		"posts_by_lang_7d": "SELECT " + week + " AS bucket, lang, sum(posts)::bigint AS posts FROM posts_per_minute_by_lang GROUP BY 1, 2",
		"posts_total_7d":   "SELECT " + week + " AS bucket, count(DISTINCT bucket) AS minutes, sum(posts)::bigint AS posts FROM posts_per_minute_by_lang GROUP BY 1",
	} {
		got := "SELECT " + tableColumns(table) + " FROM " + table
		assert.Equal(t, int64(0), count(t, srv.conn,
			"SELECT count(*) FROM (("+want+" EXCEPT "+got+") UNION ALL ("+got+" EXCEPT "+want+")) AS d"))
	}
	assert.Equal(t, int64(2), count(t, srv.conn, "SELECT count(*) FROM posts_total_7d"))
}

func TestIntegrationRollupRun_BackfillSkipsARetainedTable(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	srv := startRollupPostgres(t)
	mustInstall(t, srv.conn, loadExample(t))
	withWeek := loadExample(t)
	withWeek.Rollups[0].Grains["7d"] = config.RollupGrain{From: "1d"}
	mustInstall(t, srv.conn, withWeek)
	mustInstall(t, srv.conn, loadExample(t))

	pending, err := PendingBackfills(context.Background(), srv.conn, loadExample(t))
	assert.NoError(t, err)
	for _, p := range pending {
		assert.That(t, p.Table != "posts_by_lang_7d" && p.Table != "posts_total_7d")
	}
	// Still pending, so declaring the week again finds it unfilled.
	_, kept := stateOf(t, srv.conn, "posts").Backfill["posts_by_lang_7d"]
	assert.True(t, kept)
}

// The backfill's bucket lock is the trigger's: while a chunk holds one, a
// write to that bucket waits.
func TestIntegrationRollupRun_BackfillTakesTheTriggersBucketLock(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	srv := startRollupPostgres(t)
	ctx := context.Background()
	mustInstall(t, srv.conn, loadExample(t))

	holder := connectIn(t, srv.dsn, "UTC")
	tx, err := holder.Begin(ctx)
	assert.NoError(t, err)
	assert.NoError(t, lockBuckets(ctx, tx, "posts_by_lang_5m", []time.Time{at("2026-09-15T10:00:00Z")}))

	w := connectIn(t, srv.dsn, "UTC")
	execSQL(t, w, "SET lock_timeout = '200ms'")
	_, err = w.Exec(ctx, "INSERT INTO posts_per_minute_by_lang (bucket, lang, posts) VALUES ('2026-09-15T10:01:00Z', 'en', 5)")
	var pgErr *pgconn.PgError
	assert.That(t, errors.As(err, &pgErr))
	assert.Equal(t, "55P03", pgErr.Code)

	assert.NoError(t, tx.Rollback(ctx))
	execSQL(t, w, "INSERT INTO posts_per_minute_by_lang (bucket, lang, posts) VALUES ('2026-09-15T10:01:00Z', 'en', 5)")
}

// A write left open holds its buckets' locks. A chunk that needs one gives
// up after the lock timeout rather than holding a day of bucket locks, which
// the pipeline's next writes would queue behind.
func TestIntegrationRollupRun_ABackfillChunkGivesUpOnAnOpenWrite(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	srv := startRollupPostgres(t)
	ctx := context.Background()
	history(t, srv.conn, "2026-09-12T00:00:00Z", "2026-09-12T23:59:00Z")
	mustInstall(t, srv.conn, loadExample(t))
	withLockTimeout(t, 200*time.Millisecond)

	open := connectIn(t, srv.dsn, "UTC")
	execSQL(t, open, "BEGIN")
	execSQL(t, open, "INSERT INTO posts_per_minute_by_lang (bucket, lang, posts) VALUES ('2026-09-12T10:01:00Z', 'pt', 9)")

	pending, err := PendingBackfills(ctx, srv.conn, loadExample(t))
	assert.NoError(t, err)
	_, err = BackfillStep(ctx, srv.conn, pending[0])
	var pgErr *pgconn.PgError
	assert.That(t, errors.As(err, &pgErr))
	assert.Equal(t, "55P03", pgErr.Code)

	execSQL(t, open, "COMMIT")
	fillAll(t, srv.conn, loadExample(t))
	assertGrainsEqualSource(t, srv.conn)
}

// A chunk takes the lock of every bucket it re-merges, not only the coarser
// ones its upserts' triggers take. The holder holds one 5-minute bucket's
// lock and nothing else, so only the chunk's own lock can make it wait.
func TestIntegrationRollupRun_ABackfillChunkWaitsForItsBucketLock(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	srv := startRollupPostgres(t)
	ctx := context.Background()
	history(t, srv.conn, "2026-09-12T00:00:00Z", "2026-09-12T23:59:00Z")
	mustInstall(t, srv.conn, loadExample(t))
	withLockTimeout(t, 200*time.Millisecond)

	holder := connectIn(t, srv.dsn, "UTC")
	tx, err := holder.Begin(ctx)
	assert.NoError(t, err)
	assert.NoError(t, lockBuckets(ctx, tx, "posts_by_lang_5m", []time.Time{at("2026-09-12T10:00:00Z")}))

	pending, err := PendingBackfills(ctx, srv.conn, loadExample(t))
	assert.NoError(t, err)
	assert.Equal(t, "posts_by_lang_5m", pending[0].Table)
	_, err = BackfillStep(ctx, srv.conn, pending[0])
	var pgErr *pgconn.PgError
	assert.That(t, errors.As(err, &pgErr))
	assert.Equal(t, "55P03", pgErr.Code)

	assert.NoError(t, tx.Rollback(ctx))
	fillAll(t, srv.conn, loadExample(t))
	assertGrainsEqualSource(t, srv.conn)
}

// Writers upsert into the days being filled, through the pipeline's own
// sink, while the backfill runs. Neither waits past its lock timeout, and
// every grain equals its source after both finish.
func TestIntegrationRollupRun_ChunkedBackfillLosesNoConcurrentWrite(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	srv := startRollupPostgres(t)
	history(t, srv.conn, "2026-09-10T00:00:00Z", "2026-09-12T23:59:00Z")
	mustInstall(t, srv.conn, loadExample(t))

	start := at("2026-09-10T00:00:00Z")
	langs := []string{"en", "ja", "de", "pt"}
	var wg sync.WaitGroup
	errc := make(chan error, 64)
	for w := 0; w < 2; w++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed))
			for i := 0; i < 30; i++ {
				rows := make([]minute, 20)
				for j := range rows {
					rows[j] = minute{
						start.Add(time.Duration(rng.Intn(3*24*60)) * time.Minute),
						langs[rng.Intn(len(langs))], int32(1 + rng.Intn(50)),
					}
				}
				if err := flushMinutes(srv.dsn+"&lock_timeout=1000", rows); err != nil {
					errc <- err
				}
			}
		}(int64(w + 1))
	}
	fillAll(t, srv.conn, loadExample(t))
	wg.Wait()
	close(errc)
	for err := range errc {
		t.Errorf("a writer failed beside the backfill: %v", err)
	}
	assertGrainsEqualSource(t, srv.conn)
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test -run '^TestIntegrationRollupRun_(Backfill|ABackfillChunk|ChunkedBackfill)' ./internal/rollup/`
Expected: FAIL to compile with `undefined: PendingBackfills`, `undefined: BackfillStep`, `undefined: lockBuckets`, `undefined: Step`.

- [ ] **Step 3: Write `PendingBackfills` and `BackfillStep`**

In `internal/rollup/backfill.go`, replace the import block with:

```go
import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/errs"
)
```

and append:

```go
// Pending is one declared table that still needs filling.
type Pending struct {
	Rollup config.Rollup
	Table  string
	// Done is the start of the oldest chunk filled so far, and nil before
	// the first.
	Done *time.Time
}

// PendingBackfills lists the declared tables that still need filling: rollup
// by rollup in file order, each rollup's tables in the order edges walks
// them, narrowest first, so a table's from table tends to fill first. A
// retained table keeps its pending entry, but nothing declares it, so it is
// skipped until the file declares it again.
func PendingBackfills(ctx context.Context, conn *pgx.Conn, conf *config.RollupsConf) ([]Pending, error) {
	var out []Pending
	for _, r := range conf.Rollups {
		s, err := readState(ctx, conn, r.Name)
		if err != nil {
			return nil, backfillError(err, r.Name, "read state")
		}
		if s == nil {
			continue
		}
		for _, e := range edges(r) {
			if done, ok := s.Backfill[e.Table]; ok {
				out = append(out, Pending{Rollup: r, Table: e.Table, Done: done})
			}
		}
	}
	return out, nil
}

// Step is what one backfill chunk did.
type Step struct {
	Rollup string
	Table  string
	// From and To bound the chunk's source time, To exclusive.
	From, To time.Time
	// Buckets is how many of the table's buckets the chunk re-merged.
	Buckets int64
	// Complete is true when the chunk reached the oldest source row and the
	// table's pending entry is gone.
	Complete bool
	// Remaining is the source history older than the chunk, still to fill.
	Remaining time.Duration
}

// BackfillStep fills one chunk of p's table: the newest source time not yet
// filled, chunkSpan wide, so recent history is right first.
//
// The chunk reads which of the table's buckets its rows fall in, takes the
// lock the table's trigger takes on each, in bucket order, and re-merges
// exactly those buckets from the table they are built from. A chunk and a
// write on the same bucket serialize on that lock, and in READ COMMITTED the
// later one reads the earlier one's commit. A bucket that appears after the
// read belongs to the write that made it, whose trigger holds its lock, so
// the chunk leaves it alone. The upsert fires the coarser grains' triggers,
// which lock their own buckets the same way.
//
// It takes the install lock shared, so it never runs beside an install that
// is changing the table's objects, and then bounds every lock wait with
// lock_timeout: while a chunk waits for an open write, the pipeline's next
// writes to its locked buckets wait behind it. The chunk records its
// progress in its own transaction, so a crash repeats at most one chunk,
// and a repeat rewrites the same values.
func BackfillStep(ctx context.Context, conn *pgx.Conn, p Pending) (Step, error) {
	step := Step{Rollup: p.Rollup.Name, Table: p.Table}
	e, ok := findEdge(p.Rollup, p.Table)
	if !ok {
		return step, errs.New(errs.CodeRollupInternal, "rollup backfill: rollup %s declares no table %s", p.Rollup.Name, p.Table)
	}
	tx, err := conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return step, backfillError(err, p.Table, "begin")
	}
	// A no-op once Commit has run.
	defer func() { _ = tx.Rollback(ctx) }()

	for _, stmt := range []string{
		"SELECT pg_advisory_xact_lock_shared(hashtextextended('sqlflow_rollup_install', 0))",
		fmt.Sprintf("SET LOCAL lock_timeout = '%dms'", installLockTimeout.Milliseconds()),
	} {
		if _, err := tx.Exec(ctx, stmt); err != nil {
			return step, backfillError(err, p.Table, "prepare")
		}
	}

	t := quote(p.Rollup.Source.TimeColumn)
	var oldest, newest *time.Time
	if err := tx.QueryRow(ctx, "SELECT min("+t+"), max("+t+") FROM "+quote(p.Rollup.Source.Table)).Scan(&oldest, &newest); err != nil {
		return step, backfillError(err, p.Table, "read the source's range")
	}
	if newest == nil {
		// An empty source has no history to fill.
		step.Complete = true
		return step, finishChunk(ctx, tx, p, step)
	}

	// The oldest row the table reads is the source's oldest, binned to the
	// width of the rows the table is built from. A day built from 6-hour
	// rows reads the 6-hour row that holds the source's first minute.
	floor := floorTo(*oldest, fromWidth(p.Rollup, e))
	span := chunkSpan(p.Rollup, e)
	hi := floorTo(*newest, span).Add(span)
	if p.Done != nil {
		hi = *p.Done
	}
	lo := hi.Add(-span)
	step.From, step.To = lo, hi
	step.Complete = !lo.After(floor)
	if !step.Complete {
		step.Remaining = lo.Sub(floor)
	}

	buckets, err := touchedBuckets(ctx, tx, p.Rollup, e, lo, hi)
	if err != nil {
		return step, backfillError(err, p.Table, "read the chunk's buckets")
	}
	step.Buckets = int64(len(buckets))
	if len(buckets) > 0 {
		if err := lockBuckets(ctx, tx, e.Table, buckets); err != nil {
			return step, backfillError(err, p.Table, "lock the chunk's buckets")
		}
		var b strings.Builder
		join := fmt.Sprintf("\nJOIN unnest($1::timestamptz[]) AS touched(b)\n  ON f.%s >= touched.b AND f.%s < touched.b + %s",
			t, t, interval(e.Grain.Width))
		writeUpsert(&b, p.Rollup, e.Set, e.Grain, e.From, join)
		if _, err := tx.Exec(ctx, b.String(), buckets); err != nil {
			return step, backfillError(err, p.Table, "re-merge the chunk")
		}
	}
	return step, finishChunk(ctx, tx, p, step)
}

// touchedBuckets is every bucket of e's table that the rows of the table it
// is built from, in [lo, hi), fall in, oldest first: the order the triggers
// lock buckets in.
func touchedBuckets(ctx context.Context, q querier, r config.Rollup, e edge, lo, hi time.Time) ([]time.Time, error) {
	t := quote(r.Source.TimeColumn)
	rows, err := q.Query(ctx, fmt.Sprintf("SELECT DISTINCT %s AS b FROM %s WHERE %s >= $1 AND %s < $2 ORDER BY 1",
		bin(e.Grain.Width, t), quote(e.From), t, t), lo, hi)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[time.Time])
}

// lockBuckets takes the lock the table's trigger takes on each bucket, in
// the order given, in one round trip. The key is the trigger's: the table's
// name, a colon, and the bucket's epoch in seconds.
func lockBuckets(ctx context.Context, tx pgx.Tx, table string, buckets []time.Time) error {
	batch := &pgx.Batch{}
	for _, b := range buckets {
		batch.Queue("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", fmt.Sprintf("%s:%d", table, b.Unix()))
	}
	br := tx.SendBatch(ctx, batch)
	for range buckets {
		if _, err := br.Exec(); err != nil {
			_ = br.Close()
			return err
		}
	}
	return br.Close()
}

// finishChunk records the chunk's progress in the state row and commits, so
// the progress and the rows it describes land together. The guard leaves
// alone an entry an install removed meanwhile.
func finishChunk(ctx context.Context, tx pgx.Tx, p Pending, step Step) error {
	var err error
	if step.Complete {
		_, err = tx.Exec(ctx, "UPDATE sqlflow_rollup_state SET backfill = backfill - $2::text WHERE rollup = $1",
			p.Rollup.Name, p.Table)
	} else {
		_, err = tx.Exec(ctx, `UPDATE sqlflow_rollup_state
SET backfill = jsonb_set(backfill, ARRAY[$2::text], to_jsonb($3::timestamptz))
WHERE rollup = $1 AND backfill ? $2::text`, p.Rollup.Name, p.Table, step.From)
	}
	if err != nil {
		return backfillError(err, p.Table, "record progress")
	}
	if err := tx.Commit(ctx); err != nil {
		return backfillError(err, p.Table, "commit")
	}
	return nil
}

func backfillError(err error, table, step string) error {
	return errs.Wrap(errs.CodeRollupInternal, err, "rollup backfill %s: %s", table, step)
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test -run '^TestIntegrationRollupRun_(Backfill|ABackfillChunk|ChunkedBackfill)' ./internal/rollup/ -v`
Expected: PASS, 8 tests.

Run: `go test -short -race ./internal/rollup/`
Expected: PASS.

- [ ] **Step 5: Prove the bucket locks matter**

The lost-update race the locks prevent needs two transactions to interleave, which a random workload hits rarely, so `ChunkedBackfillLosesNoConcurrentWrite` alone cannot prove the locks. `BackfillTakesTheTriggersBucketLock` proves the key is the trigger's; `ABackfillChunkWaitsForItsBucketLock` proves the chunk takes it. Watch the second one fail without the locks: in `BackfillStep`, comment out the `lockBuckets` call and its error check, then run:

Run: `go test -run '^TestIntegrationRollupRun_ABackfillChunkWaitsForItsBucketLock$' ./internal/rollup/`
Expected: FAIL. With no lock of its own, the chunk never waits for the holder and returns no 55P03 error. (`ABackfillChunkGivesUpOnAnOpenWrite` would still fail with 55P03: the chunk's upsert cascades into the 15-minute trigger, which waits for the open write's lock regardless.)

Restore the call, and run the test again. Expected: PASS.

- [ ] **Step 6: Commit**

Write `$TMPDIR/msg.txt`:

```
rollup: fill pending tables in chunks under the triggers' bucket locks

A table install marks for backfill fills one chunk at a time, newest day
first. A chunk locks exactly the buckets its rows fall in, with the key
the trigger takes, and re-merges exactly those, so a bucket that appears
mid-chunk stays with the write whose trigger holds its lock. It takes the
install lock shared and bounds each lock wait, so an open write fails the
chunk rather than stalling the pipeline behind it. Progress commits with
the rows. A week added after its days exist fills to its source across
chunks, and two live writers beside the backfill lose nothing.
```

Run: `git add internal/rollup/backfill.go internal/rollup/backfill_integration_test.go && git commit -q -F "$TMPDIR/msg.txt"`

---

### Task 3: Health rules and metrics

**Files:**
- Create: `internal/rollup/daemon/health.go`
- Create: `internal/rollup/daemon/health_test.go`
- Create: `internal/rollup/daemon/metrics.go`

**Interfaces:**
- Consumes: `errs.CodeConfigInvalid`.
- Produces (package `daemon`, unexported, used by Task 4):
  - `const roleStarting, roleLeader, roleStandby = "starting", "leader", "standby"`.
  - `type snapshot struct { Role string; LastContact time.Time; PendingTables int; Filling string; Remaining time.Duration }`.
  - `type healthState struct` with methods `touch(now time.Time)`, `setRole(role string)`, `setPending(n int)`, `setFilling(table string, remaining time.Duration)`, `get() snapshot`; and `func newHealthState(now time.Time) *healthState`.
  - `const unreachableIntervals = 3`.
  - `func healthStatus(s snapshot, now time.Time, interval time.Duration) (status, reason string, code int)`.
  - `type instruments struct { backfillBuckets metric.Int64Counter; chunkDuration metric.Float64Histogram; errors metric.Int64Counter; leaderAcquired metric.Int64Counter }`; `func newInstruments(mp metric.MeterProvider) (*instruments, error)`.
  - `func newProvider(exporter string) (metric.MeterProvider, *prom.Registry, error)`.
  - `func phase(name string) metric.AddOption`.

- [ ] **Step 1: Write the failing tests**

Create `internal/rollup/daemon/health_test.go`:

```go
package daemon

import (
	"net/http"
	"testing"
	"time"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/turbolytics/sql-flow/internal/errs"
	"github.com/zeebo/assert"
)

func TestCliRollupRun_HealthzRulesInOrder(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")

	now := time.Date(2026, 9, 25, 12, 0, 0, 0, time.UTC)
	interval := 15 * time.Second
	for _, c := range []struct {
		name   string
		s      snapshot
		status string
		code   int
		reason string
	}{
		{"no round trip for three intervals", snapshot{Role: roleLeader, LastContact: now.Add(-46 * time.Second)},
			"failed", http.StatusServiceUnavailable, "no database round trip for 46s"},
		{"failed beats standby", snapshot{Role: roleStandby, LastContact: now.Add(-time.Hour)},
			"failed", http.StatusServiceUnavailable, "no database round trip for 3600s"},
		{"standby", snapshot{Role: roleStandby, LastContact: now},
			"standby", http.StatusOK, "another instance holds the leader lock"},
		{"backfilling", snapshot{Role: roleLeader, LastContact: now, PendingTables: 2, Filling: "posts_by_lang_5m", Remaining: 36 * time.Hour},
			"backfilling", http.StatusOK, "2 tables to fill; posts_by_lang_5m has 1.5 days of history left"},
		{"backfilling before its first chunk", snapshot{Role: roleLeader, LastContact: now, PendingTables: 1},
			"backfilling", http.StatusOK, "1 tables to fill"},
		{"starting", snapshot{Role: roleStarting, LastContact: now},
			"starting", http.StatusOK, "installing and taking the leader lock"},
		{"healthy", snapshot{Role: roleLeader, LastContact: now},
			"healthy", http.StatusOK, ""},
	} {
		t.Run(c.name, func(t *testing.T) {
			status, reason, code := healthStatus(c.s, now, interval)
			assert.Equal(t, c.status, status)
			assert.Equal(t, c.code, code)
			assert.Equal(t, c.reason, reason)
		})
	}
}

func TestCliRollupRun_TheHealthStateCountsFromItsStart(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")

	start := time.Date(2026, 9, 25, 12, 0, 0, 0, time.UTC)
	h := newHealthState(start)
	assert.Equal(t, roleStarting, h.get().Role)
	// Before the first round trip, the age counts from the process's start.
	status, _, _ := healthStatus(h.get(), start.Add(10*time.Second), 15*time.Second)
	assert.Equal(t, "starting", status)
	h.touch(start.Add(time.Minute))
	h.setRole(roleLeader)
	h.setPending(1)
	h.setFilling("posts_total_5m", 24*time.Hour)
	s := h.get()
	assert.Equal(t, start.Add(time.Minute), s.LastContact)
	assert.Equal(t, "posts_total_5m", s.Filling)
}

func TestCliRollupRun_TheMetricsFlagTakesPrometheusOrNothing(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")

	mp, registry, err := newProvider("")
	assert.NoError(t, err)
	assert.NotNil(t, mp)
	assert.That(t, registry == nil)

	_, registry, err = newProvider("Prometheus")
	assert.NoError(t, err)
	assert.NotNil(t, registry)

	_, _, err = newProvider("statsd")
	assert.Error(t, err)
	assert.Equal(t, errs.CodeConfigInvalid, errs.CodeOf(err))

	_, err = newInstruments(mp)
	assert.NoError(t, err)
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test -short ./internal/rollup/daemon/`
Expected: FAIL to compile with `undefined: snapshot`, `undefined: healthStatus`, `undefined: newProvider`.

- [ ] **Step 3: Write `health.go`**

Create `internal/rollup/daemon/health.go`:

```go
// Package daemon is `sqlflow rollup run`: the process that installs what a
// rollups file declares, leads the rollups it names, fills their pending
// tables, and reports its health.
package daemon

import (
	"fmt"
	"net/http"
	"sync"
	"time"
)

// What this process is doing about the leader lock.
const (
	roleStarting = "starting"
	roleLeader   = "leader"
	roleStandby  = "standby"
)

// unreachableIntervals is how many intervals without a database round trip
// make the daemon failed. Three, so one slow query cannot flap it.
const unreachableIntervals = 3

// snapshot is one read of the daemon's health.
type snapshot struct {
	Role string
	// LastContact is the last successful database round trip, and the
	// process's start before the first.
	LastContact time.Time
	// PendingTables is how many declared tables still need filling, as the
	// leader last read them.
	PendingTables int
	// Filling is the table the last chunk filled, and Remaining the source
	// history left in it.
	Filling   string
	Remaining time.Duration
}

// healthState is what the loop writes and /healthz reads.
type healthState struct {
	mu sync.Mutex
	s  snapshot
}

func newHealthState(start time.Time) *healthState {
	return &healthState{s: snapshot{Role: roleStarting, LastContact: start}}
}

func (h *healthState) touch(now time.Time) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.s.LastContact = now
}

func (h *healthState) setRole(role string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.s.Role = role
}

func (h *healthState) setPending(n int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.s.PendingTables = n
	if n == 0 {
		h.s.Filling, h.s.Remaining = "", 0
	}
}

func (h *healthState) setFilling(table string, remaining time.Duration) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.s.Filling, h.s.Remaining = table, remaining
}

func (h *healthState) get() snapshot {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.s
}

// healthStatus is the table behind /healthz, the rules `sqlflow run` uses:
// 200 means do not restart, and 503 means restart. The first matching rule
// wins. degraded arrives with verify, in Plan 2b.
func healthStatus(s snapshot, now time.Time, interval time.Duration) (status, reason string, code int) {
	if age := now.Sub(s.LastContact); age > unreachableIntervals*interval {
		return "failed", fmt.Sprintf("no database round trip for %.0fs", age.Seconds()), http.StatusServiceUnavailable
	}
	if s.Role == roleStandby {
		return "standby", "another instance holds the leader lock", http.StatusOK
	}
	if s.PendingTables > 0 {
		reason := fmt.Sprintf("%d tables to fill", s.PendingTables)
		if s.Filling != "" {
			reason += fmt.Sprintf("; %s has %.1f days of history left", s.Filling, s.Remaining.Hours()/24)
		}
		return "backfilling", reason, http.StatusOK
	}
	if s.Role == roleStarting {
		return "starting", "installing and taking the leader lock", http.StatusOK
	}
	return "healthy", "", http.StatusOK
}
```

- [ ] **Step 4: Write `metrics.go`**

Create `internal/rollup/daemon/metrics.go`:

```go
package daemon

import (
	"fmt"
	"strings"

	prom "github.com/prometheus/client_golang/prometheus"
	"github.com/turbolytics/sql-flow/internal/errs"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

// instruments are the daemon's OTel instruments, named as the spec's
// metrics table names them. The Prometheus exporter appends the unit and
// _total.
type instruments struct {
	backfillBuckets metric.Int64Counter
	chunkDuration   metric.Float64Histogram
	errors          metric.Int64Counter
	leaderAcquired  metric.Int64Counter
}

func newInstruments(mp metric.MeterProvider) (*instruments, error) {
	m := mp.Meter("sqlflow.rollup")
	var in instruments
	var err error
	if in.backfillBuckets, err = m.Int64Counter("rollup_backfill_buckets",
		metric.WithDescription("Buckets written by backfill")); err != nil {
		return nil, err
	}
	if in.chunkDuration, err = m.Float64Histogram("rollup_backfill_chunk_duration",
		metric.WithDescription("One backfill chunk's transaction"), metric.WithUnit("s")); err != nil {
		return nil, err
	}
	if in.errors, err = m.Int64Counter("rollup_errors",
		metric.WithDescription("Errors by phase: install, backfill or lock")); err != nil {
		return nil, err
	}
	if in.leaderAcquired, err = m.Int64Counter("rollup_leader_acquired",
		metric.WithDescription("Times this process became the leader")); err != nil {
		return nil, err
	}
	return &in, nil
}

// phase labels an error by the phase that raised it.
func phase(name string) metric.AddOption {
	return metric.WithAttributes(attribute.String("phase", name))
}

// newProvider builds the meter provider and, for the prometheus exporter,
// the registry /metrics serves. With no exporter the instruments record into
// a provider nothing reads, so the loop never checks for nil.
func newProvider(exporter string) (metric.MeterProvider, *prom.Registry, error) {
	switch strings.ToLower(strings.TrimSpace(exporter)) {
	case "":
		return sdkmetric.NewMeterProvider(), nil, nil
	case "prometheus":
		registry := prom.NewRegistry()
		exp, err := prometheus.New(prometheus.WithRegisterer(registry))
		if err != nil {
			return nil, nil, fmt.Errorf("prometheus exporter: %w", err)
		}
		return sdkmetric.NewMeterProvider(sdkmetric.WithReader(exp)), registry, nil
	default:
		return nil, nil, errs.New(errs.CodeConfigInvalid,
			"--metrics %q is not an exporter this version serves; use prometheus", exporter)
	}
}
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `go test -short -race ./internal/rollup/daemon/ -v`
Expected: PASS, 3 tests and 7 subtests.

- [ ] **Step 6: Commit**

Write `$TMPDIR/msg.txt`:

```
rollup: the daemon's health rules and metric instruments

healthz follows sqlflow run's rules: 503 only when a restart can help,
which is three intervals without a database round trip. standby,
backfilling and starting are 200, and a backfill's reason names the
tables left and the history left in the one filling. The instruments are
the spec's names; --metrics takes prometheus or nothing.
```

Run: `git add internal/rollup/daemon && git commit -q -F "$TMPDIR/msg.txt"`

---

### Task 4: The daemon

**Files:**
- Create: `internal/rollup/daemon/leader.go`
- Create: `internal/rollup/daemon/daemon.go`
- Create: `internal/rollup/daemon/daemon_integration_test.go`

**Interfaces:**
- Consumes: `rollup.Connect`, `rollup.Install`, `rollup.InstallReport`, `rollup.PendingBackfills`, `rollup.BackfillStep`, `rollup.Step`; Task 3's health and metrics.
- Produces:
  - `type Options struct { Version string; Logger *zap.Logger; Metrics string; Addr string; Interval time.Duration; OnListen func(net.Addr) }`.
  - `func New(conf *config.RollupsConf, dsn string, opts Options) (*Daemon, error)`.
  - `func (d *Daemon) Run(ctx context.Context) error`.
  - `func (d *Daemon) Health() (status, reason string)`.

- [ ] **Step 1: Write the failing tests**

Create `internal/rollup/daemon/daemon_integration_test.go`:

```go
package daemon

// The daemon against a real Postgres: it installs and fills history, one of
// two leads and hands over, it stops mid-backfill and the next resumes, and
// its health follows the database.

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

const examplePath = "../../../dev/config/rollups/bluesky.yml"

func startPostgres(t *testing.T) (string, *pgx.Conn) {
	t.Helper()
	ctx := context.Background()
	pg, err := tcpostgres.Run(ctx, "postgres:18",
		tcpostgres.WithDatabase("rollup"),
		tcpostgres.WithUsername("rollup"),
		tcpostgres.WithPassword("rollup"),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	t.Cleanup(func() { _ = pg.Terminate(context.Background()) })
	dsn, err := pg.ConnectionString(ctx, "sslmode=disable")
	assert.NoError(t, err)
	conn, err := pgx.Connect(ctx, dsn)
	assert.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	exec(t, conn, `CREATE TABLE posts_per_minute_by_lang (
  bucket TIMESTAMPTZ NOT NULL, lang TEXT NOT NULL, posts INTEGER NOT NULL,
  PRIMARY KEY (bucket, lang))`)
	return dsn, conn
}

func exec(t *testing.T, conn *pgx.Conn, sql string) {
	t.Helper()
	_, err := conn.Exec(context.Background(), sql)
	assert.NoError(t, err)
}

func history(t *testing.T, conn *pgx.Conn, from, to string) {
	t.Helper()
	exec(t, conn, `INSERT INTO posts_per_minute_by_lang (bucket, lang, posts)
SELECT g, l, (extract(minute FROM g)::int % 7) + 1
FROM generate_series('`+from+`'::timestamptz, '`+to+`'::timestamptz, interval '1 minute') AS g,
     unnest(ARRAY['en', 'ja', 'de']) AS l`)
}

func loadExample(t *testing.T) *config.RollupsConf {
	t.Helper()
	conf, err := config.LoadRollups(examplePath)
	assert.NoError(t, err)
	return conf
}

// assertExact fails when a daily table differs from its source.
func assertExact(t *testing.T, conn *pgx.Conn) {
	t.Helper()
	day := "date_bin(INTERVAL '24 hours', bucket, TIMESTAMPTZ '2000-01-01 00:00:00+00')"
	for table, want := range map[string]string{
		"posts_by_lang_1d": "SELECT " + day + " AS bucket, lang, sum(posts)::bigint AS posts FROM posts_per_minute_by_lang GROUP BY 1, 2",
		"posts_total_1d":   "SELECT " + day + " AS bucket, count(DISTINCT bucket) AS minutes, sum(posts)::bigint AS posts FROM posts_per_minute_by_lang GROUP BY 1",
	} {
		cols := "bucket, lang, posts"
		if strings.HasPrefix(table, "posts_total_") {
			cols = "bucket, minutes, posts"
		}
		got := "SELECT " + cols + " FROM " + table
		var diff int64
		assert.NoError(t, conn.QueryRow(context.Background(),
			fmt.Sprintf("SELECT count(*) FROM ((%s EXCEPT %s) UNION ALL (%s EXCEPT %s)) AS d", want, got, got, want)).Scan(&diff))
		if diff != 0 {
			t.Fatalf("%s differs from its source in %d rows", table, diff)
		}
	}
}

// running starts a daemon and returns a stop function that cancels it and
// returns what Run returned.
func running(t *testing.T, dsn string, opts Options) (*Daemon, func() error) {
	t.Helper()
	if opts.Interval == 0 {
		opts.Interval = 100 * time.Millisecond
	}
	d, err := New(loadExample(t), dsn, opts)
	assert.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- d.Run(ctx) }()
	var once sync.Once
	var result error
	stop := func() error {
		once.Do(func() {
			cancel()
			select {
			case result = <-done:
			case <-time.After(30 * time.Second):
				result = fmt.Errorf("the daemon did not stop within 30s")
			}
		})
		return result
	}
	t.Cleanup(func() { _ = stop() })
	return d, stop
}

func waitFor(t *testing.T, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		if ok() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("waited 60s for %s", what)
}

func status(d *Daemon) string {
	s, _ := d.Health()
	return s
}

func TestIntegrationRollupRun_TheDaemonInstallsAndFillsHistory(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	dsn, conn := startPostgres(t)
	history(t, conn, "2026-09-10T00:00:00Z", "2026-09-12T23:59:00Z")

	d, stop := running(t, dsn, Options{})
	waitFor(t, "healthy", func() bool { return status(d) == "healthy" })
	assertExact(t, conn)
	assert.NoError(t, stop())
}

func TestIntegrationRollupRun_OneDaemonLeadsAndTheOtherTakesOver(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	dsn, conn := startPostgres(t)
	a, _ := running(t, dsn, Options{})
	b, _ := running(t, dsn, Options{})
	waitFor(t, "one leader and one standby", func() bool {
		sa, sb := status(a), status(b)
		return (sa == "healthy" && sb == "standby") || (sa == "standby" && sb == "healthy")
	})
	leader, standby := a, b
	if status(a) == "standby" {
		leader, standby = b, a
	}

	// Kill the leader's session, as a network partition or a crash would.
	exec(t, conn, `SELECT pg_terminate_backend(pid) FROM pg_locks
WHERE locktype = 'advisory' AND granted AND pid <> pg_backend_pid()`)
	waitFor(t, "the standby to lead", func() bool { return status(standby) == "healthy" })
	waitFor(t, "the old leader to stand by", func() bool { return status(leader) == "standby" })
}

func TestIntegrationRollupRun_ADaemonStoppedMidBackfillIsResumed(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	dsn, conn := startPostgres(t)
	history(t, conn, "2026-08-01T00:00:00Z", "2026-09-12T23:59:00Z")

	first, stop := running(t, dsn, Options{})
	waitFor(t, "a backfill in progress", func() bool {
		_, reason := first.Health()
		return strings.Contains(reason, "days of history left")
	})
	stopped := time.Now()
	assert.NoError(t, stop())
	assert.That(t, time.Since(stopped) < 10*time.Second)

	second, _ := running(t, dsn, Options{})
	waitFor(t, "healthy", func() bool { return status(second) == "healthy" })
	assertExact(t, conn)
}

func TestIntegrationRollupRun_AnUnreachableDatabaseFailsHealthAndRecovers(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	dsn, conn := startPostgres(t)
	d, _ := running(t, dsn, Options{})
	waitFor(t, "healthy", func() bool { return status(d) == "healthy" })

	// Refuse every new connection, and end the daemon's.
	exec(t, conn, "ALTER DATABASE rollup ALLOW_CONNECTIONS false")
	exec(t, conn, "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = 'rollup' AND pid <> pg_backend_pid()")
	waitFor(t, "failed", func() bool { return status(d) == "failed" })

	exec(t, conn, "ALTER DATABASE rollup ALLOW_CONNECTIONS true")
	waitFor(t, "healthy again, with no restart", func() bool { return status(d) == "healthy" })
}

func TestIntegrationRollupRun_TheDaemonServesHealthzAndMetrics(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	dsn, conn := startPostgres(t)
	history(t, conn, "2026-09-12T00:00:00Z", "2026-09-12T23:59:00Z")
	addrc := make(chan net.Addr, 1)
	d, _ := running(t, dsn, Options{Metrics: "prometheus", Addr: "127.0.0.1:0", OnListen: func(a net.Addr) { addrc <- a }})
	addr := (<-addrc).String()
	waitFor(t, "healthy", func() bool { return status(d) == "healthy" })

	resp, err := http.Get("http://" + addr + "/healthz")
	assert.NoError(t, err)
	var body map[string]string
	assert.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	_ = resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "healthy", body["status"])

	resp, err = http.Get("http://" + addr + "/metrics")
	assert.NoError(t, err)
	text, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	assert.NoError(t, err)
	for _, series := range []string{"rollup_backfill_buckets_total", "rollup_backfill_chunk_duration_seconds", "rollup_leader_acquired_total"} {
		assert.That(t, strings.Contains(string(text), series))
	}
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test -run '^TestIntegrationRollupRun' ./internal/rollup/daemon/`
Expected: FAIL to compile with `undefined: Options`, `undefined: New`, `undefined: Daemon`.

- [ ] **Step 3: Write `leader.go`**

Create `internal/rollup/daemon/leader.go`:

```go
package daemon

import (
	"context"
	"slices"

	"github.com/jackc/pgx/v5"
	"github.com/turbolytics/sql-flow/internal/config"
)

// tryLead takes the leader lock of every rollup the file declares, in name
// order, on conn's session. It leads only with all of them: a daemon that
// holds some gives them back, so two daemons for one file never split its
// work. A session lock lasts as long as the session, so a leader that loses
// its connection loses its locks, and a standby can take them.
func tryLead(ctx context.Context, conn *pgx.Conn, conf *config.RollupsConf) (bool, error) {
	var names []string
	for _, r := range conf.Rollups {
		names = append(names, r.Name)
	}
	slices.Sort(names)
	for _, n := range names {
		var got bool
		if err := conn.QueryRow(ctx, "SELECT pg_try_advisory_lock(hashtextextended('sqlflow_rollup_leader:' || $1, 0))", n).Scan(&got); err != nil {
			return false, err
		}
		if !got {
			_, err := conn.Exec(ctx, "SELECT pg_advisory_unlock_all()")
			return false, err
		}
	}
	return true, nil
}
```

- [ ] **Step 4: Write `daemon.go`**

Create `internal/rollup/daemon/daemon.go`:

```go
package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5"
	prom "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/rollup"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
)

// defaultInterval is how often the daemon checks its leadership and its
// pending tables when it has nothing to fill.
const defaultInterval = 15 * time.Second

// defaultAddr is where `sqlflow run` serves /metrics and /healthz, so a
// scrape config carries over.
const defaultAddr = ":8000"

// chunkTimeout bounds one chunk once SIGTERM has arrived. A chunk finishes
// rather than rolls back, and lock_timeout already bounds its lock waits,
// so this only stops a chunk the database never answers.
const chunkTimeout = 5 * time.Minute

// Options configures a Daemon.
type Options struct {
	Version string
	Logger  *zap.Logger
	// Metrics is the exporter served at /metrics beside /healthz: empty for
	// none, which serves neither, or "prometheus".
	Metrics string
	// Addr is where /metrics and /healthz listen. Defaults to :8000.
	Addr string
	// Interval defaults to 15 seconds.
	Interval time.Duration
	// OnListen receives the bound address. Tests bind port 0.
	OnListen func(net.Addr)
}

// Daemon is one `sqlflow rollup run` process.
type Daemon struct {
	conf     *config.RollupsConf
	dsn      string
	opts     Options
	log      *zap.Logger
	interval time.Duration
	health   *healthState
	m        *instruments
	registry *prom.Registry
}

// New builds a daemon. It connects to nothing: Run does.
func New(conf *config.RollupsConf, dsn string, opts Options) (*Daemon, error) {
	mp, registry, err := newProvider(opts.Metrics)
	if err != nil {
		return nil, err
	}
	m, err := newInstruments(mp)
	if err != nil {
		return nil, err
	}
	d := &Daemon{
		conf: conf, dsn: dsn, opts: opts, log: opts.Logger, interval: opts.Interval,
		health: newHealthState(time.Now()), m: m, registry: registry,
	}
	if d.log == nil {
		d.log = zap.NewNop()
	}
	if d.interval <= 0 {
		d.interval = defaultInterval
	}
	if d.opts.Addr == "" {
		d.opts.Addr = defaultAddr
	}
	return d, nil
}

// Health is the status /healthz reports, and its reason.
func (d *Daemon) Health() (status, reason string) {
	status, reason, _ = healthStatus(d.health.get(), time.Now(), d.interval)
	return status, reason
}

// Run installs what the file declares, then competes for the leader lock
// and, while it leads, fills pending tables chunk by chunk. It returns nil
// once ctx ends and the chunk in progress has committed. It returns an error
// only when the first connection fails or install refuses the file: a
// supervisor's restart loop makes either visible, and neither heals by
// waiting.
func (d *Daemon) Run(ctx context.Context) error {
	if d.registry != nil {
		if err := d.serveHTTP(ctx); err != nil {
			return err
		}
	}
	work, err := rollup.Connect(ctx, d.dsn)
	if err != nil {
		d.m.errors.Add(ctx, 1, phase("install"))
		return err
	}
	defer func() {
		if work != nil {
			_ = work.Close(context.Background())
		}
	}()
	d.health.touch(time.Now())
	report, err := rollup.Install(ctx, work, d.conf, d.opts.Version)
	if err != nil {
		d.m.errors.Add(ctx, 1, phase("install"))
		return err
	}
	d.health.touch(time.Now())
	d.logInstall(report)

	lead := &session{}
	defer lead.close()
	var checked time.Time
	for {
		if time.Since(checked) >= d.interval {
			d.keepLead(ctx, lead)
			checked = time.Now()
		}
		more := false
		if lead.leading {
			work = d.keepWork(ctx, work)
			if work != nil {
				more = d.fillOne(ctx, work)
			}
		}
		if ctx.Err() != nil {
			return nil
		}
		if more {
			continue
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(d.interval):
		}
	}
}

// session is the connection that holds, or competes for, the leader locks.
type session struct {
	conn    *pgx.Conn
	leading bool
}

func (s *session) close() {
	if s.conn != nil {
		_ = s.conn.Close(context.Background())
	}
	s.conn, s.leading = nil, false
}

// keepLead keeps or takes the leader locks, once an interval. A failed
// ping means the session, and its locks with it, may be gone, so the daemon
// steps down and competes again on the next interval.
func (d *Daemon) keepLead(ctx context.Context, s *session) {
	if s.conn != nil {
		if err := s.conn.Ping(ctx); err != nil {
			if ctx.Err() == nil {
				d.log.Warn("lost the leader session; standing down", zap.Error(err))
				d.m.errors.Add(ctx, 1, phase("lock"))
			}
			s.close()
			d.health.setRole(roleStandby)
			return
		}
		d.health.touch(time.Now())
	} else {
		conn, err := rollup.Connect(ctx, d.dsn)
		if err != nil {
			if ctx.Err() == nil {
				d.log.Warn("cannot reach the database for the leader lock", zap.Error(err))
				d.m.errors.Add(ctx, 1, phase("lock"))
			}
			return
		}
		s.conn = conn
		d.health.touch(time.Now())
	}
	if s.leading {
		return
	}
	ok, err := tryLead(ctx, s.conn, d.conf)
	if err != nil {
		if ctx.Err() == nil {
			d.log.Warn("competing for the leader lock", zap.Error(err))
			d.m.errors.Add(ctx, 1, phase("lock"))
		}
		s.close()
		return
	}
	if !ok {
		d.health.setRole(roleStandby)
		d.health.setPending(0)
		return
	}
	s.leading = true
	d.m.leaderAcquired.Add(ctx, 1)
	d.health.setRole(roleLeader)
	d.log.Info("leading")
}

// keepWork reopens the work connection after the database dropped it.
func (d *Daemon) keepWork(ctx context.Context, work *pgx.Conn) *pgx.Conn {
	if work != nil && !work.IsClosed() {
		return work
	}
	conn, err := rollup.Connect(ctx, d.dsn)
	if err != nil {
		if ctx.Err() == nil {
			d.log.Warn("cannot reach the database to fill tables", zap.Error(err))
			d.m.errors.Add(ctx, 1, phase("backfill"))
		}
		return nil
	}
	d.health.touch(time.Now())
	return conn
}

// fillOne fills one chunk of the first pending table. It reports whether
// another chunk is due at once: false when nothing is pending, or when the
// chunk failed and the next try waits an interval.
func (d *Daemon) fillOne(ctx context.Context, work *pgx.Conn) bool {
	pending, err := rollup.PendingBackfills(ctx, work, d.conf)
	if err != nil {
		if ctx.Err() == nil {
			d.log.Warn("reading pending tables", zap.Error(err))
			d.m.errors.Add(ctx, 1, phase("backfill"))
		}
		return false
	}
	d.health.touch(time.Now())
	d.health.setPending(len(pending))
	if len(pending) == 0 {
		return false
	}

	// The chunk finishes even after SIGTERM: stopping waits for it, and
	// lock_timeout bounds how long it can wait for a lock.
	cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), chunkTimeout)
	defer cancel()
	p := pending[0]
	attrs := metric.WithAttributes(attribute.String("rollup", p.Rollup.Name), attribute.String("table", p.Table))
	start := time.Now()
	step, err := rollup.BackfillStep(cctx, work, p)
	if err != nil {
		d.log.Warn("backfill chunk failed; retrying next interval", zap.String("table", p.Table), zap.Error(err))
		d.m.errors.Add(cctx, 1, phase("backfill"))
		return false
	}
	d.health.touch(time.Now())
	d.m.chunkDuration.Record(cctx, time.Since(start).Seconds(), attrs)
	d.m.backfillBuckets.Add(cctx, step.Buckets, attrs)
	d.health.setFilling(step.Table, step.Remaining)
	d.log.Info("backfill chunk", zap.String("rollup", step.Rollup), zap.String("table", step.Table),
		zap.Time("from", step.From), zap.Time("to", step.To), zap.Int64("buckets", step.Buckets),
		zap.Bool("complete", step.Complete))
	return true
}

func (d *Daemon) logInstall(rep *rollup.InstallReport) {
	for _, r := range rep.Rollups {
		d.log.Info("installed", zap.String("rollup", r.Name), zap.Bool("adopted", r.Adopted),
			zap.Strings("created", r.Created), zap.Strings("backfill", r.Plan.Backfill),
			zap.Strings("restored", r.Plan.Restore), zap.Strings("dropped", r.Dropped),
			zap.Int("attempts", rep.Attempts))
		for _, t := range r.Plan.Retain {
			d.log.Warn("no longer declared; its table and triggers stay", zap.String("rollup", r.Name), zap.String("table", t.Table))
		}
	}
	for _, n := range rep.Undeclared {
		d.log.Warn("rollup no longer declared; its tables and triggers stay", zap.String("rollup", n))
	}
}

// serveHTTP serves /metrics and /healthz on Addr until ctx ends.
func (d *Daemon) serveHTTP(ctx context.Context) error {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(d.registry, promhttp.HandlerOpts{}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		status, reason, code := healthStatus(d.health.get(), time.Now(), d.interval)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		_ = json.NewEncoder(w).Encode(map[string]string{"status": status, "reason": reason})
	})
	ln, err := net.Listen("tcp", d.opts.Addr)
	if err != nil {
		return err
	}
	if d.opts.OnListen != nil {
		d.opts.OnListen(ln.Addr())
	}
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		_ = srv.Close()
	}()
	go func() {
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			d.log.Error("http server stopped", zap.Error(err))
		}
	}()
	d.log.Info("serving http", zap.String("addr", ln.Addr().String()), zap.Strings("routes", []string{"/metrics", "/healthz"}))
	return nil
}
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `go test -run '^TestIntegrationRollupRun' ./internal/rollup/daemon/ -v`
Expected: PASS, 5 tests.

Run: `go test -short -race ./internal/rollup/...`
Expected: PASS.

- [ ] **Step 6: Commit**

Write `$TMPDIR/msg.txt`:

```
rollup: the daemon installs, leads, fills and reports its health

Run installs what the file declares, then competes for a session lock
per rollup and leads only with all of them. The leader fills pending
tables chunk by chunk; a standby waits. A dead leader session hands over
within two intervals. SIGTERM lets the open chunk commit, and the next
daemon resumes from its recorded progress. healthz reports failed after
three intervals without the database and recovers without a restart.
```

Run: `git add internal/rollup/daemon && git commit -q -F "$TMPDIR/msg.txt"`

---

### Task 5: `sqlflow rollup run`, and `install` unhidden

**Files:**
- Create: `internal/cli/rollup/run.go`
- Modify: `internal/cli/rollup/install.go` (remove `Hidden`)
- Modify: `internal/cli/rollup/rollup.go` (register `run`, update `Short` and `Long`)
- Modify: `internal/cli/rollup/install_test.go` (the hidden test becomes a listing test)
- Create: `internal/cli/rollup/run_test.go`
- Create: `internal/cli/rollup/run_integration_test.go`

**Interfaces:**
- Consumes: `daemon.New`, `daemon.Options`, `(*daemon.Daemon).Run`; `config.LoadRollups`, `RollupsConf.CheckError`, `RollupsConf.PostgresDSN`; `logging.New`; `buildinfo.Version`; test helpers `run`, `withStore`, `readFile`, `example`.
- Produces: `sqlflow rollup run -c FILE [--metrics prometheus]`.

- [ ] **Step 1: Write the failing tests**

In `internal/cli/rollup/install_test.go`, replace `TestCliRollupRun_InstallIsHiddenUntilRunExists` with:

```go
// install and run are the daemon's two commands, listed now that run can
// backfill what install creates.
func TestCliRollupRun_InstallAndRunAreListed(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")

	out, _, err := run(t, "--help")
	assert.NoError(t, err)
	for _, name := range []string{"install", "run"} {
		cmd, _, err := NewCommand().Find([]string{name})
		assert.NoError(t, err)
		assert.False(t, cmd.Hidden)
		assert.That(t, strings.Contains(out, "\n  "+name+" "))
	}
}
```

Create `internal/cli/rollup/run_test.go`:

```go
package rollup

import (
	"testing"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/turbolytics/sql-flow/internal/errs"
	"github.com/zeebo/assert"
)

func TestCliRollupRun_RunRefusesAnEmptyDSN(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")

	_, _, err := run(t, "run", "-c", withStore(t, ""))
	assert.Error(t, err)
	assert.Equal(t, errs.CodeConfigInvalid, errs.CodeOf(err))
}

// A typo in the flag fails before anything connects.
func TestCliRollupRun_RunRefusesAnUnknownExporter(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")

	_, _, err := run(t, "run", "-c", withStore(t, "postgres://rollup@127.0.0.1:1/rollup"), "--metrics", "statsd")
	assert.Error(t, err)
	assert.Equal(t, errs.CodeConfigInvalid, errs.CodeOf(err))
}

func TestCliRollupRun_RunRequiresItsFile(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")

	_, _, err := run(t, "run")
	assert.Error(t, err)
}
```

Create `internal/cli/rollup/run_integration_test.go`:

```go
package rollup

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

// The command fills history, then stops cleanly on its context, as it does
// on SIGTERM.
func TestIntegrationRollupRun_RunFillsHistoryAndStopsCleanly(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	ctx := context.Background()
	pg, err := tcpostgres.Run(ctx, "postgres:18",
		tcpostgres.WithDatabase("rollup"),
		tcpostgres.WithUsername("rollup"),
		tcpostgres.WithPassword("rollup"),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	t.Cleanup(func() { _ = pg.Terminate(context.Background()) })
	dsn, err := pg.ConnectionString(ctx, "sslmode=disable")
	assert.NoError(t, err)
	conn, err := pgx.Connect(ctx, dsn)
	assert.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	for _, sql := range []string{
		`CREATE TABLE posts_per_minute_by_lang (
  bucket TIMESTAMPTZ NOT NULL, lang TEXT NOT NULL, posts INTEGER NOT NULL,
  PRIMARY KEY (bucket, lang))`,
		`INSERT INTO posts_per_minute_by_lang (bucket, lang, posts)
SELECT g, 'en', 1 FROM generate_series('2026-09-12T00:00:00Z'::timestamptz, '2026-09-12T23:59:00Z', interval '1 minute') AS g`,
	} {
		_, err := conn.Exec(ctx, sql)
		assert.NoError(t, err)
	}

	rctx, cancel := context.WithCancel(ctx)
	cmd := NewCommand()
	var out, stderr bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&stderr)
	cmd.SetArgs([]string{"run", "-c", withStore(t, dsn)})
	done := make(chan error, 1)
	go func() { done <- cmd.ExecuteContext(rctx) }()

	deadline := time.Now().Add(60 * time.Second)
	for {
		var total int64
		err := conn.QueryRow(ctx, "SELECT coalesce(sum(posts), 0) FROM posts_total_1d").Scan(&total)
		if err == nil && total == 1440 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("run did not fill the day: total %d, err %v", total, err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	cancel()
	select {
	case err := <-done:
		assert.NoError(t, err)
	case <-time.After(30 * time.Second):
		t.Fatal("run did not stop within 30s of its context ending")
	}
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test -short ./internal/cli/rollup/ -run 'TestCliRollupRun_(InstallAndRunAreListed|Run)'`
Expected: FAIL. `run` is not a subcommand, and `install` is hidden.

- [ ] **Step 3: Write the command**

Create `internal/cli/rollup/run.go`:

```go
package rollup

import (
	"context"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
	"github.com/turbolytics/sql-flow/internal/buildinfo"
	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/logging"
	"github.com/turbolytics/sql-flow/internal/rollup/daemon"
)

func newRunCommand() *cobra.Command {
	var configPath, metrics string
	cmd := &cobra.Command{
		Use:   "run",
		Short: "Keep the rollup tables a rollups file declares installed and filled",
		Long: "Install what the rollups file declares, then lead its rollups and fill every " +
			"table install marks for backfill, one chunk at a time, beside a live pipeline. " +
			"Several instances may run: one leads and the rest stand by. --metrics prometheus " +
			"serves /metrics and /healthz on :8000.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			logger, levelErr := logging.New()
			defer func() { _ = logger.Sync() }()
			if levelErr != nil {
				return levelErr
			}
			conf, err := config.LoadRollups(configPath)
			if err != nil {
				return err
			}
			if err := conf.CheckError(); err != nil {
				return err
			}
			dsn, err := conf.PostgresDSN()
			if err != nil {
				return err
			}
			d, err := daemon.New(conf, dsn, daemon.Options{
				Version: buildinfo.Version, Logger: logger.Named("sqlflow.rollup"), Metrics: metrics,
			})
			if err != nil {
				return err
			}

			// A supervisor stops the daemon with SIGTERM. The command's
			// context is the parent, so a test or an embedding caller can
			// stop it too.
			parent := cmd.Context()
			if parent == nil {
				parent = context.Background()
			}
			ctx, stop := signal.NotifyContext(parent, syscall.SIGINT, syscall.SIGTERM)
			defer stop()
			return d.Run(ctx)
		},
	}
	cmd.Flags().StringVarP(&configPath, "config", "c", "", "Path to the rollups file")
	cmd.Flags().StringVar(&metrics, "metrics", "", "Metrics exporter to enable (prometheus); serves /metrics and /healthz on :8000")
	_ = cmd.MarkFlagRequired("config")
	return cmd
}
```

In `internal/cli/rollup/install.go`, delete these four lines:

```go
		// Hidden until `sqlflow rollup run` exists to backfill what install
		// creates. A table created over existing history stays partial until
		// then.
		Hidden: true,
```

In `internal/cli/rollup/rollup.go`, in `NewCommand`, replace everything from the `Short:` line through the `cmd.AddCommand(...)` line with:

```go
		Short: "Declare rollup tables, keep them filled, and generate the serve datasets that read them",
		Long: "Generate, from a rollups file, the migration that creates rollup tables and the " +
			"triggers that keep them current, and the serve datasets that read them. install " +
			"applies the tables and triggers itself, and run keeps them installed and fills " +
			"their history. check fails when a committed copy of a generated file has drifted.",
	}
	cmd.AddCommand(newDDLCommand(), newServeCommand(), newCheckCommand(), newInstallCommand(), newRunCommand())
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test -short -race ./internal/cli/rollup/`
Expected: PASS.

Run: `go test -run '^TestIntegrationRollupRun' ./internal/cli/rollup/ -v`
Expected: PASS, 2 tests.

- [ ] **Step 5: Commit**

Write `$TMPDIR/msg.txt`:

```
cli: sqlflow rollup run, and install listed beside it

run installs what the rollups file declares, leads its rollups, and
fills every table install marks, stopping cleanly on SIGTERM. install
was hidden because nothing filled the tables it created over existing
history; run does, so both are listed. A typo in --metrics fails before
anything connects.
```

Run: `git add internal/cli/rollup && git commit -q -F "$TMPDIR/msg.txt"`

---

### Task 6: Docs, spec amendments, and the whole gate

**Files:**
- Modify: `README.md` (the `sqlflow rollup` section)
- Modify: `CHANGELOG.md` (`## Unreleased` → `### Added`)
- Modify: `docs/superpowers/specs/2026-09-24-rollup-daemon-design.md`

**Interfaces:**
- Consumes: everything above.
- Produces: documentation only.

- [ ] **Step 1: The README**

In `README.md`, replace the code block and table under `### \`sqlflow rollup\`` with:

````markdown
```
sqlflow rollup install -c rollups.yml
sqlflow rollup run     -c rollups.yml [--metrics prometheus]
sqlflow rollup ddl     -c rollups.yml [--backend postgres]
sqlflow rollup serve   -c rollups.yml [--dataset NAME]
sqlflow rollup check   -c rollups.yml --serve FILE [--migration FILE]
```

| Command | Does |
|---|---|
| `install` | Creates the tables, functions and triggers the file declares, in one transaction, and records what it applied in `sqlflow_rollup_state`. Refuses a change that would corrupt stored rows. Fills nothing. |
| `run` | Installs, then leads the file's rollups and fills every table `install` marks, one chunk at a time, beside a live pipeline. Several may run: one leads, the rest stand by. `--metrics prometheus` serves `/metrics` and `/healthz` on `:8000`. |
| `ddl` | Prints a migration: the same tables, functions and triggers, and a backfill in one transaction. For a team that applies SQL itself. Connects to nothing. |
| `serve` | Prints the `serve` datasets, to paste into a serve file's `datasets`. |
| `check` | Exits `10` when the serve file, or the migration when given, differs from what the declaration generates, or when a dataset could answer more rows than its `max_rows`. |

`install` and `run` connect to `store.postgres.dsn`:

```yaml
store:
  type: postgres
  postgres:
    dsn: "{{ SQLFLOW_POSTGRES_URI }}"
```

Run `install` in a deploy's entrypoints, before `sqlflow serve` prepares its
statements, and `run` as its own process. `install` locks each source for
its DDL only, a few milliseconds. `run` fills history newest day first, under
the same per-bucket locks as the triggers, so the pipeline keeps writing.
````

Replace the paragraph that begins `` `sqlflow rollup` writes and checks the files; applying the migration is your `` with:

```markdown
`ddl` and `check` suit a team that applies SQL through its own migration
runner. `install` and `run` do it for you. [`dev/config/rollups/bluesky.yml`](dev/config/rollups/bluesky.yml)
is a complete declaration, and `sqlflow validate` checks a rollups file
against its schema and rules.
```

Replace the bullet that begins `- **The migration blocks the source's writers**` with:

```markdown
- **`ddl`'s migration blocks the source's writers** until it commits, and its
  backfill reads the whole source table. `install` and `run` do not: the
  backfill runs in chunks under per-bucket locks.
```

- [ ] **Step 2: The changelog**

In `CHANGELOG.md`, under `## Unreleased` → `### Added`, add first:

```markdown
- `sqlflow rollup install` and `sqlflow rollup run` keep the rollup tables a
  rollups file declares, where `sqlflow rollup ddl` printed a migration to
  apply by hand.
  - `install` creates the tables, functions and triggers in one transaction,
    and refuses a change that would corrupt stored rows: a measure's type, a
    set's dimensions, a grain's `from`. A removed grain keeps its tables, so
    a rollback loses nothing.
  - `run` installs, takes a leader lock, and fills history in chunks, newest
    day first, under the triggers' own bucket locks, beside a live pipeline.
    A second instance stands by and takes over when the leader's session
    ends.
  - `rollups.yml` gains `store` and `turbostats` blocks. `run --metrics
    prometheus` serves `/metrics` and `/healthz` on `:8000`.
```

- [ ] **Step 3: The spec**

In `docs/superpowers/specs/2026-09-24-rollup-daemon-design.md`, under `### Backfill`, replace the bullet that begins `- Each chunk is one READ COMMITTED transaction.` with:

```markdown
- Each chunk is one READ COMMITTED transaction. It takes the install lock
  shared, so it never runs beside an install that is changing its table,
  and sets `lock_timeout` as install does: while a chunk waits for an open
  write, the pipeline's next writes to its locked buckets wait behind it.
  It reads which of the target's buckets its rows fall in, takes the lock
  the target's trigger takes on each, in bucket order, in one round trip,
  and re-merges exactly those buckets from the table the target is built
  from. A bucket that appears after the read belongs to the write that made
  it, whose trigger holds its lock. Re-merging a recomputed set instead
  would touch that bucket without its lock. The chunk records its progress
  in the state row. Its upsert fires the triggers of the coarser tables,
  which take their own locks.
- The last chunk is the one that reaches the source's oldest row, binned to
  the width of the rows the target is built from, so a day built from
  6-hour rows includes the 6-hour row that holds the source's first minute.
```

Under `### /healthz`, after the table, add:

```markdown
The `backfilling` reason names the tables left and the source history left
in the one filling. `degraded` arrives with the verify pass.
```

- [ ] **Step 4: Run the whole gate**

Run: `go build ./... && go vet ./... && test -z "$(gofmt -l .)" && go test -short -race ./...`
Expected: every package passes. `internal/handlers`' `TestInferredInvoke_DoesNotLeakNativeMemory` flaps under `-race` on macOS, on main as well; if it fails, re-run that package alone and note it.

Run: `go test -run '^TestIntegration' ./internal/rollup/... ./internal/cli/rollup/`
Expected: PASS.

Run: `uv run --locked pytest tests/tooling -q`
Expected: PASS.

- [ ] **Step 5: Commit**

Write `$TMPDIR/msg.txt`:

```
docs: sqlflow rollup install and run

The README's rollup section lists the two commands, the store block and
where each runs in a deploy. The changelog announces them. The spec
records how a chunk locks and re-merges exactly the buckets it read, and
aligns its last chunk to the rows the table is built from.
```

Run: `git add README.md CHANGELOG.md docs/superpowers/specs/2026-09-24-rollup-daemon-design.md && git commit -q -F "$TMPDIR/msg.txt"`

- [ ] **Step 6: Open the PR**

Write `$TMPDIR/pr.md`:

```
Plan 2a of `docs/superpowers/specs/2026-09-24-rollup-daemon-design.md`: `sqlflow rollup run`, which installs what a rollups file declares, leads its rollups, and fills every table `install` marks, beside a live pipeline. `install` is listed now that something fills what it creates.

- A backfill chunk covers a UTC day, halved until it takes at most 1,000 advisory locks. It locks exactly the buckets its rows fall in, with the triggers' own key, and re-merges exactly those, so a bucket that appears mid-chunk stays with the write whose trigger holds its lock. Progress commits with the rows.
- The daemon leads only with every rollup's session lock; a standby takes over when the leader's session ends.
- `--metrics prometheus` serves `/metrics` and `/healthz` on `:8000`. `/healthz` reports `failed` after three intervals without the database.

This PR carries the plan (`docs/superpowers/plans/2026-09-25-rollup-run.md`) and the spec amendments with the code.
```

Paste the title and body into the chat and wait for the maintainer's go. Then:

Run: `git push -u origin feat/rollup-run:feat/rollup-run && gh pr create --repo turbolytics/sql-flow --base main --head feat/rollup-run --title "rollup: sqlflow rollup run, backfill and the leader lock" --body-file "$TMPDIR/pr.md"`
Expected: a PR URL. Then `gh pr view feat/rollup-run --repo turbolytics/sql-flow --json state,baseRefName --jq '.state, .baseRefName'` prints `OPEN` and `main`.
