# Rollup TurboStats Implementation Plan (Plan 3)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `sqlflow rollup run` reports itself to a control plane: a top-level `freshness` section with each table's newest bucket, keyed by store and table, and a `rollup` section with each rollup's verify and drift totals, backfill progress, completeness and trigger cost. A rollups file that reports TurboStats declares at most 10 tables.

**Architecture:** `turbostats/wire` gains two optional sections, bounded by a config cap that the shape guard names and a size test prices. The collector takes a `RollupSource` whose function returns both sections, as `ServeSource` does for serve. `internal/rollup` gains two reads, the least complete closed bucket and the trigger functions' cost. The daemon keeps what its passes measured and counted, builds the sections from it, and runs the reporter `sqlflow run` and `sqlflow serve` already use.

**Tech Stack:** Go 1.26, pgx/v5, cobra, zap, OpenTelemetry metric SDK v1.46, testcontainers-go, zeebo/assert; Python release tests with testcontainers.

**Spec:** `docs/superpowers/specs/2026-09-24-rollup-daemon-design.md`

## Where this plan sits

| Plan | Scope | State |
|---|---|---|
| 1 | Config blocks, planner, state table, `sqlflow rollup install` | Merged, #380 |
| 2a | Chunked backfill, `sqlflow rollup run`, the leader lock, `/healthz` and metrics | Merged, #382 |
| 2b | Verify, `internal/freshness`, the store identity | Merged, #388 and #390 |
| **3, this one** | TurboStats `freshness` and `rollup` sections, completeness, trigger cost, the release test | |
| 4 | `sqlflow rollup test` | |
| 5 | A release, then the Bluesky demo, the Render template, and the parity proof | |

## Decisions made with the maintainer on 2026-09-25

The spec's per-table lists repeated the store in every freshness entry and listed completeness and trigger cost per table, about 590 bytes a table at worst. The repo's shape guard refuses any list whose width is not bounded by config, and a receiver refuses a bundle over 16 KiB. So:

- **Freshness** is one object per bundle: the store id and `observed_at` once, then one entry per table with its schema-qualified name, its grain and its newest bucket. No duration is derived: a receiver computes age from the bucket and grain, and can change that rule without a new contract.
- **Completeness and trigger cost** are per-rollup totals, not per-table lists.
- **The cap:** a rollups file whose `turbostats` block reports (`report_to` is set) declares at most 10 tables. Each rollup's source is reported beside its tables and does not count. Validate refuses an eleventh with a message that names the count and says to split the file.

## Global Constraints

- One branch, `feat/rollup-turbostats`, from main at `cfbd22d`. Push with `git push -u origin feat/rollup-turbostats`. Once the PR exists, gate every push: `[ "$(gh pr view N --repo turbolytics/sql-flow --json state --jq .state)" = OPEN ]`.
- The wire contract is additive within v1: new optional sections and fields only. Nothing existing moves or changes type.
- `config.MaxReportedRollupTables = 10`. The shape guard exempts `.Bundle.Freshness.Tables` and `.Bundle.Rollup.Rollups` by name, citing the cap.
- `/healthz` reports the process only. Nothing in this plan changes it.
- A standby reports `rollup.role` and nothing else. Only the leader reports `freshness`.
- The bundle carries no error message and no DSN: `last_error_code` and `last_error_at` only.
- Unit tests `TestCliRollupRun_*` or, in the TurboStats packages, `TestCollect_*`, `TestWire_*` and `TestBundle_*` as those files name theirs. Integration tests `TestIntegrationRollupRun_*`, skip under `-short`. Rollup tests call `coverage.Covers(t, "cli.rollup_run")`; collector tests call `coverage.Covers(t, "observability.turbostats")`. `turbostats/wire` tests import only the standard library, as its package does.
- A bundle change runs both shape guards: `go test ./internal/turbostats/` and `uv run --locked pytest tests/release -q` against a freshly built image.
- Run `go`, `make`, `docker`, `gh` and `uv` outside the sandbox. One heredoc per command; commit messages go in a file.
- Prose follows the repo's `CLAUDE.md`.

## Review Focus

1. **A standby never reports freshness or rollup totals**, and a leader that steps down stops reporting them. Task 5, `TestIntegrationRollupRun_TheLeaderReportsItsRollupsAndFreshness` asserts both roles.
2. **The widest legal rollup bundle** fits under the receiver's 16 KiB. Task 2, `TestCollect_AFullRollupBundleStaysUnderTheCeiling`.
3. **A server with `track_functions` off** reports no `triggers`, rather than zeros. Task 4, `TestIntegrationRollupRun_TriggerCostIsAbsentUntilTheServerTracksFunctions`.
4. **A rollup with no `count_buckets`**, and a table with no closed bucket yet, report no completeness rather than an error. Task 4, `TestIntegrationRollupRun_TheLeastCompleteClosedBucketIsReported`.
5. **The shipped image** reports both sections against a real Postgres. Task 6, `test_cli_rollup_run_reports_its_rollups_and_freshness`.

## File Structure

```
internal/config/rollup.go, rollup_check.go, rollup_check_test.go   the cap
turbostats/wire/bundle.go, bundle_test.go                            Freshness, Rollup and their parts
internal/turbostats/dimensional_test.go                              exemptions and the size test
internal/turbostats/bundle.go, collect.go, collect_test.go           RollupSource
internal/rollup/measure.go, measure_integration_test.go              LeastComplete, TriggerCost
internal/rollup/daemon/report.go                                     what the daemon reports, and the section
internal/rollup/daemon/daemon.go, metrics.go                         the reader, the reporter, the recording
internal/rollup/daemon/daemon_integration_test.go                    a receiver in Go
internal/cli/rollup/run.go                                           rendered text, hash, commit
tests/release/test_image.py                                          the image test
docs/coverage/features.yml                                           cli.rollup_run requires release
README.md, CHANGELOG.md, the spec                                    docs
```

---

### Task 1: The cap

**Files:** Modify `internal/config/rollup.go`, `internal/config/rollup_check.go`, `internal/config/rollup_check_test.go`.

**Interfaces:**
- Produces: `const MaxReportedRollupTables = 10`; `func (c *RollupsConf) TableCount() int`.

- [ ] **Step 1: Write the failing test** in `internal/config/rollup_check_test.go`:

```go
// A rollups file that reports TurboStats sends one freshness entry per
// table, so the file caps its tables. The demo's ten fit; an eleventh and a
// twelfth are refused with the count. A file that reports nothing is not
// capped.
func TestCliRollupRun_AReportingFileDeclaresAtMostTenTables(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")

	reporting := &TurboStats{ID: "rollups-01", ReportTo: "http://127.0.0.1:8080/v1/turbostats",
		Key: "sfc_AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8"}
	capped := func(c *RollupsConf) []Violation {
		var out []Violation
		for _, v := range c.Check() {
			if strings.Contains(v.Message, "at most") {
				out = append(out, v)
			}
		}
		return out
	}

	conf, err := LoadRollups("../../dev/config/rollups/bluesky.yml")
	assert.NoError(t, err)
	assert.Equal(t, 10, conf.TableCount())
	conf.TurboStats = reporting
	assert.Equal(t, 0, len(capped(conf)))

	conf.Rollups[0].Grains["7d"] = RollupGrain{From: "1d"}
	assert.Equal(t, 12, conf.TableCount())
	got := capped(conf)
	assert.Equal(t, 1, len(got))
	assert.DeepEqual(t, []string{"turbostats"}, got[0].Path)
	assert.That(t, strings.Contains(got[0].Message, "declares 12"))

	conf.TurboStats = nil
	assert.Equal(t, 0, len(capped(conf)))
}
```

Add `"strings"` and the coverage import if the file lacks them.

- [ ] **Step 2: Run it.** `go test -count=1 -short -run TestCliRollupRun_AReportingFileDeclaresAtMostTenTables ./internal/config/` — Expected: FAIL to compile, `conf.TableCount undefined`.

- [ ] **Step 3: Implement.** In `internal/config/rollup.go`:

```go
// MaxReportedRollupTables bounds the tables a rollups file declares when it
// reports TurboStats. Each table is one entry in the bundle's freshness
// section, and a bundle's width must be set by the file and bounded: a
// receiver refuses one over 16 KiB. Ten tables and their sources fit with
// room, and a larger deployment splits across files.
const MaxReportedRollupTables = 10

// TableCount is how many rollup tables the file declares: each dimension
// set at each declared grain.
func (c *RollupsConf) TableCount() int {
	n := 0
	for _, r := range c.Rollups {
		n += len(r.DimensionSets) * len(r.Ladder())
	}
	return n
}
```

In `rollup_check.go`, after the `TurboStats.Check` line:

```go
	if c.TurboStats.Enabled() && c.TableCount() > MaxReportedRollupTables {
		add([]string{"turbostats"}, "a rollups file that reports TurboStats declares at most %d tables, "+
			"and this one declares %d; split the rollups across files, each with its own `sqlflow rollup run`",
			MaxReportedRollupTables, c.TableCount())
	}
```

- [ ] **Step 4: Run** `go test -count=1 -short ./internal/config/` — Expected: PASS.

- [ ] **Step 5: Commit** `config: a rollups file that reports TurboStats declares at most ten tables`.

---

### Task 2: The wire sections and their guards

**Files:** Modify `turbostats/wire/bundle.go`, `turbostats/wire/bundle_test.go`, `internal/turbostats/dimensional_test.go`.

**Interfaces:**
- Produces: `Bundle.Freshness *Freshness`, `Bundle.Rollup *Rollup`; `type Freshness struct { StoreID, StoreIDKind string; ObservedAt time.Time; Tables []FreshTable }`; `type FreshTable struct { Table string; GrainSeconds int64; NewestBucketAt *time.Time }`; `type Rollup struct { Role string; Rollups []RollupEntry; LastErrorCode *string; LastErrorAt *time.Time }`; `type RollupEntry struct { Name, Strategy string; Backfill *RollupBackfill; VerifyBucketCount, DriftBucketCount int64; Completeness *RollupCompleteness; Triggers *RollupTriggers }`; `type RollupBackfill struct { TablesLeft int; HistoryLeftSeconds *int64 }`; `type RollupCompleteness struct { Table string; BucketAt time.Time; SourceBuckets, ExpectedBuckets int64 }`; `type RollupTriggers struct { Calls int64; TotalSeconds float64 }`.

- [ ] **Step 1: Write the failing wire tests** in `turbostats/wire/bundle_test.go`:

```go
// The rollup sections are absent from a process that runs no rollups.
func TestBundle_RollupSectionsAreAbsentUntilSet(t *testing.T) {
	raw := mustMarshal(t, Bundle{V: Version, Pipeline: &Pipeline{}})
	for _, key := range []string{`"freshness"`, `"rollup"`} {
		if strings.Contains(raw, key) {
			t.Fatalf("bundle carries %s: %s", key, raw)
		}
	}
}

// A standby reports its role and nothing else, and an empty table reports
// no newest bucket rather than a zero time.
func TestBundle_AStandbyAndAnEmptyTableSayOnlyWhatIsKnown(t *testing.T) {
	if got := mustMarshal(t, Rollup{Role: "standby"}); got != `{"role":"standby"}` {
		t.Fatalf("a standby's rollup section is %s", got)
	}
	got := mustMarshal(t, FreshTable{Table: "public.posts_1h", GrainSeconds: 3600})
	if got != `{"table":"public.posts_1h","grain_seconds":3600}` {
		t.Fatalf("an empty table is %s", got)
	}
}
```

`mustMarshal` is the file's helper and returns a string.

- [ ] **Step 2: Run** `go test -count=1 ./turbostats/wire/` — Expected: FAIL to compile, `undefined: Rollup`.

- [ ] **Step 3: Add the types** to `turbostats/wire/bundle.go`. In `Bundle`, after `Serve`:

```go
	// Freshness is present when the process measures a store's tables: the
	// rollup daemon's leader.
	Freshness *Freshness `json:"freshness,omitempty"`
	// Rollup is present when the process runs rollups.
	Rollup *Rollup `json:"rollup,omitempty"`
```

and after `ServeCache`:

```go
// Freshness is how recent the data in one store's tables is, as this
// process read it. A receiver keys a table by StoreID and Table, so a table
// two processes report is one table, and the newer ObservedAt wins.
type Freshness struct {
	// StoreID names the database without its host or credentials: "pg:"
	// and 16 hex characters of a hash of the server's system identifier and
	// the database's name, the same for every reporter.
	StoreID string `json:"store_id"`
	// StoreIDKind is "system", or "address" when the server refused its
	// system identifier and the id came from the address it answered on,
	// which another reporter may see differently.
	StoreIDKind string `json:"store_id_kind"`
	// ObservedAt is the database's clock when the tables were read.
	ObservedAt time.Time `json:"observed_at"`
	// Tables is one entry per table. The reporter's config bounds it: a
	// rollups file that reports declares at most ten tables, and each rollup
	// adds its source.
	Tables []FreshTable `json:"tables"`
}

// FreshTable is one table's newest bucket. A receiver computes its age as
// ObservedAt minus the bucket's end, its start plus GrainSeconds. A table
// still filling its open bucket reads negative, which is current.
type FreshTable struct {
	// Table is qualified by its schema.
	Table        string `json:"table"`
	GrainSeconds int64  `json:"grain_seconds"`
	// NewestBucketAt is the start of the newest bucket, absent for an empty
	// table.
	NewestBucketAt *time.Time `json:"newest_bucket_at,omitempty"`
}

// Rollup is what a rollup daemon reports about the rollups it manages.
type Rollup struct {
	// Role is "leader", "standby" or "starting". Only the leader reports
	// the rollups: a standby checks nothing.
	Role string `json:"role"`
	// Rollups is one entry per rollup the file declares. Every rollup
	// declares a table, so the cap on tables bounds it.
	Rollups []RollupEntry `json:"rollups,omitempty"`
	// The code and time of the last error the daemon counted. The message
	// stays in the process: it can carry a DSN.
	LastErrorCode *string    `json:"last_error_code,omitempty"`
	LastErrorAt   *time.Time `json:"last_error_at,omitempty"`
}

// RollupEntry is one rollup's totals since the process started.
type RollupEntry struct {
	Name string `json:"name"`
	// Strategy is how the store keeps the rollup current: "trigger" in v1.
	Strategy string `json:"strategy"`
	// Backfill is absent once every table of the rollup is filled.
	Backfill          *RollupBackfill `json:"backfill,omitempty"`
	VerifyBucketCount int64           `json:"verify_bucket_count"`
	DriftBucketCount  int64           `json:"drift_bucket_count"`
	// Completeness is absent for a rollup with no count_buckets measure,
	// and before any of its tables has a closed bucket.
	Completeness *RollupCompleteness `json:"completeness,omitempty"`
	// Triggers is absent unless the server tracks function calls, which
	// takes track_functions set to pl or all.
	Triggers *RollupTriggers `json:"triggers,omitempty"`
}

// RollupBackfill is how much of a rollup is still filling.
type RollupBackfill struct {
	TablesLeft int `json:"tables_left"`
	// HistoryLeftSeconds is the source history still to fill in the table
	// furthest behind, absent before that table's first chunk.
	HistoryLeftSeconds *int64 `json:"history_left_seconds,omitempty"`
}

// RollupCompleteness is the least complete of the newest closed buckets of
// a rollup's count_buckets tables: the source buckets it holds, against the
// number its width holds. "57 of 60" is an hour missing three minutes.
type RollupCompleteness struct {
	Table           string    `json:"table"`
	BucketAt        time.Time `json:"bucket_at"`
	SourceBuckets   int64     `json:"source_buckets"`
	ExpectedBuckets int64     `json:"expected_buckets"`
}

// RollupTriggers sums the calls and time of a rollup's trigger functions,
// from the server's function statistics.
type RollupTriggers struct {
	Calls        int64   `json:"calls"`
	TotalSeconds float64 `json:"total_seconds"`
}
```

- [ ] **Step 4: Run** `go test -count=1 ./turbostats/wire/` — Expected: PASS.

- [ ] **Step 5: The shape guard.** Run `go test -count=1 -run TestWire_NoFieldScalesWithCardinality ./internal/turbostats/` — Expected: FAIL, the two new lists. In `dimensional_test.go`, beside the labels exemption:

```go
				// A rollup daemon's tables and rollups come from its rollups
				// file, and config.MaxReportedRollupTables caps the tables of
				// a file that reports. Every rollup declares a table, so the
				// cap bounds both lists. TestCollect_AFullRollupBundleStaysUnderTheCeiling
				// prices the widest legal file.
				if at == ".Bundle.Freshness.Tables" || at == ".Bundle.Rollup.Rollups" {
					continue
				}
```

Run it again — Expected: PASS.

- [ ] **Step 6: The size test.** Append to `dimensional_test.go`:

```go
// The widest rollup bundle the config allows stays under 12 KiB: the most
// tables a reporting file may declare, each with the longest name Postgres
// allows under the longest schema, a source per rollup, a rollup per table,
// and every optional field at its widest. The receiver's limit is 16 KiB.
func TestCollect_AFullRollupBundleStaysUnderTheCeiling(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	big := int64(1) << 62
	at := time.Now().UTC()
	name := strings.Repeat("n", 63)
	table := strings.Repeat("s", 63) + "." + strings.Repeat("t", 63)
	code := "system.rollup.unreachable"
	n := config.MaxReportedRollupTables
	fresh := make([]wire.FreshTable, 0, 2*n)
	entries := make([]wire.RollupEntry, 0, n)
	for i := 0; i < n; i++ {
		fresh = append(fresh,
			wire.FreshTable{Table: table, GrainSeconds: big, NewestBucketAt: &at},
			wire.FreshTable{Table: table, GrainSeconds: big, NewestBucketAt: &at})
		entries = append(entries, wire.RollupEntry{
			Name: name, Strategy: "trigger",
			Backfill:          &wire.RollupBackfill{TablesLeft: n, HistoryLeftSeconds: &big},
			VerifyBucketCount: big, DriftBucketCount: big,
			Completeness: &wire.RollupCompleteness{Table: table, BucketAt: at, SourceBuckets: big, ExpectedBuckets: big},
			Triggers:     &wire.RollupTriggers{Calls: big, TotalSeconds: 1e18},
		})
	}
	b := wire.Bundle{
		V: wire.Version, SentAt: at, IntervalSeconds: 86400, LastActivityAt: &at,
		Instance: wire.Instance{
			ID: strings.Repeat("i", 64), Version: "v2026.09.21.12", Commit: strings.Repeat("c", 40),
			Arch: "linux/arm64", ConfigHash: "sha256:" + strings.Repeat("f", 64), Labels: widestLabels(t),
		},
		Process:   wire.Process{StartedAt: at, RSSBytes: big, Goroutines: 1 << 30},
		Freshness: &wire.Freshness{StoreID: "pg:" + strings.Repeat("f", 16), StoreIDKind: "address", ObservedAt: at, Tables: fresh},
		Rollup:    &wire.Rollup{Role: "starting", Rollups: entries, LastErrorCode: &code, LastErrorAt: &at},
		Exit:      &wire.Exit{Reason: "system.internal.unexpected", Code: 255},
	}
	raw, err := json.Marshal(b)
	assert.NoError(t, err)
	t.Logf("the widest rollup bundle is %d bytes", len(raw))
	assert.That(t, len(raw) < 12<<10)
}
```

Run `go test -count=1 ./internal/turbostats/` — Expected: PASS, with the size logged. If the widest bundle exceeds 12 KiB, ledger the measured size and choose between a lower cap and a higher ceiling under 16 KiB with the maintainer's rule in mind: bounded and priced.

- [ ] **Step 7: Commit** `turbostats: freshness and rollup sections, bounded by the rollups file's cap`.

---

### Task 3: The collector reads a rollup source

**Files:** Modify `internal/turbostats/bundle.go`, `internal/turbostats/collect.go`, `internal/turbostats/collect_test.go`.

**Interfaces:**
- Produces: aliases `Freshness`, `FreshTable`, `Rollup`, `RollupEntry`, `RollupBackfill`, `RollupCompleteness`, `RollupTriggers`; `type RollupSource struct { Section func() (*Rollup, *Freshness) }`; `Source.Rollup *RollupSource`.

- [ ] **Step 1: Write the failing test** in `collect_test.go`:

```go
// A rollup source fills both sections, and a process without one sends
// neither.
func TestCollect_ARollupSourceFillsBothSections(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	reader := sdkmetric.NewManualReader()
	_ = sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	at := time.Now().UTC()
	src := Source{Static: Static{Version: "v", StartedAt: at}, Reader: reader,
		Rollup: &RollupSource{Section: func() (*Rollup, *Freshness) {
			return &Rollup{Role: "leader", Rollups: []RollupEntry{{Name: "posts", Strategy: "trigger"}}},
				&Freshness{StoreID: "pg:0123456789abcdef", StoreIDKind: "system", ObservedAt: at,
					Tables: []FreshTable{{Table: "public.posts_1h", GrainSeconds: 3600}}}
		}}}
	b, err := Collect(context.Background(), src)
	assert.NoError(t, err)
	assert.Equal(t, "leader", b.Rollup.Role)
	assert.Equal(t, 1, len(b.Freshness.Tables))
	assert.That(t, b.Pipeline == nil && b.Serve == nil)

	src.Rollup = nil
	b, err = Collect(context.Background(), src)
	assert.NoError(t, err)
	assert.That(t, b.Rollup == nil && b.Freshness == nil)
}
```

- [ ] **Step 2: Run** `go test -count=1 -run TestCollect_ARollupSourceFillsBothSections ./internal/turbostats/` — Expected: FAIL to compile, `undefined: RollupSource`.

- [ ] **Step 3: Implement.** In `bundle.go`, add to the alias block:

```go
	Freshness          = wire.Freshness
	FreshTable         = wire.FreshTable
	Rollup             = wire.Rollup
	RollupEntry        = wire.RollupEntry
	RollupBackfill     = wire.RollupBackfill
	RollupCompleteness = wire.RollupCompleteness
	RollupTriggers     = wire.RollupTriggers
```

to `Source`:

```go
	// Rollup is set by `sqlflow rollup run`.
	Rollup *RollupSource
```

and:

```go
// RollupSource is what the rollup daemon reports. The daemon builds both
// sections from its last passes, because they read a database the
// instruments do not. Freshness is nil when the process measured nothing:
// a standby, or a leader before its first observe pass.
type RollupSource struct {
	Section func() (*Rollup, *Freshness)
}
```

In `Collect`, after the serve section:

```go
	if src.Rollup != nil {
		b.Rollup, b.Freshness = src.Rollup.Section()
	}
```

- [ ] **Step 4: Run** `go test -count=1 ./internal/turbostats/` — Expected: PASS.

- [ ] **Step 5: Commit** `turbostats: the collector reads a rollup source`.

---

### Task 4: Completeness and trigger cost

**Files:** Create `internal/rollup/measure.go`, `internal/rollup/measure_integration_test.go`.

**Interfaces:**
- Produces: `type Completeness struct { Table string; BucketAt time.Time; SourceBuckets, ExpectedBuckets int64 }`; `func LeastComplete(ctx context.Context, conn *pgx.Conn, r config.Rollup) (Completeness, bool, error)`; `func TriggerCost(ctx context.Context, conn *pgx.Conn, r config.Rollup) (calls int64, seconds float64, ok bool, err error)`.

- [ ] **Step 1: Write the failing tests** in `internal/rollup/measure_integration_test.go`:

```go
package rollup

import (
	"context"
	"testing"
	"time"

	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

// posts_total counts minutes. With three minutes of 22:00 missing, its
// newest closed hour holds 57 of 60, the least complete of its newest
// closed buckets. posts_by_lang counts nothing, so a rollup of it alone
// reports no completeness, and so does a table with no closed bucket.
func TestIntegrationRollupRun_TheLeastCompleteClosedBucketIsReported(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	srv := startRollupPostgres(t)
	ctx := context.Background()
	history(t, srv.conn, "2026-09-12T00:00:00Z", "2026-09-12T23:59:00Z")
	execSQL(t, srv.conn, `DELETE FROM posts_per_minute_by_lang WHERE bucket >= '2026-09-12T22:10:00Z' AND bucket < '2026-09-12T22:13:00Z'`)
	mustInstall(t, srv.conn, loadExample(t))
	fillAll(t, srv.conn, loadExample(t))

	c, ok, err := LeastComplete(ctx, srv.conn, loadExample(t).Rollups[0])
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, "posts_total_1h", c.Table)
	assert.That(t, c.BucketAt.Equal(at("2026-09-12T22:00:00Z")))
	assert.Equal(t, int64(57), c.SourceBuckets)
	assert.Equal(t, int64(60), c.ExpectedBuckets)

	byLang := loadExample(t).Rollups[0]
	byLang.DimensionSets = byLang.DimensionSets[:1]
	_, ok, err = LeastComplete(ctx, srv.conn, byLang)
	assert.NoError(t, err)
	assert.False(t, ok)
}

// track_functions is off by default, and pg_stat_user_functions then holds
// no rows for the triggers: the cost is unknown, not zero. A session that
// sets it to pl counts its writes' trigger calls.
func TestIntegrationRollupRun_TriggerCostIsAbsentUntilTheServerTracksFunctions(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	srv := startRollupPostgres(t)
	ctx := context.Background()
	mustInstall(t, srv.conn, loadExample(t))
	fillAll(t, srv.conn, loadExample(t))
	r := loadExample(t).Rollups[0]

	history(t, srv.conn, "2026-09-12T00:00:00Z", "2026-09-12T00:04:00Z")
	_, _, ok, err := TriggerCost(ctx, srv.conn, r)
	assert.NoError(t, err)
	assert.False(t, ok)

	w := connectIn(t, srv.dsn, "UTC")
	execSQL(t, w, "SET track_functions = 'pl'")
	execSQL(t, w, `INSERT INTO posts_per_minute_by_lang (bucket, lang, posts) VALUES ('2026-09-12T00:05:00Z', 'en', 1)`)
	deadline := time.Now().Add(20 * time.Second)
	for {
		calls, seconds, ok, err := TriggerCost(ctx, srv.conn, r)
		assert.NoError(t, err)
		if ok {
			assert.That(t, calls > 0 && seconds >= 0)
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("no trigger calls counted 20s after a tracked write")
		}
		time.Sleep(200 * time.Millisecond)
	}
}

var _ = config.Rollup{}
```

Delete the last line if `config` is used elsewhere in the file.

- [ ] **Step 2: Run** `go test -count=1 -run 'TestIntegrationRollupRun_(TheLeastCompleteClosedBucketIsReported|TriggerCostIsAbsentUntilTheServerTracksFunctions)$' ./internal/rollup/` — Expected: FAIL to compile, `undefined: LeastComplete`.

- [ ] **Step 3: Implement** `internal/rollup/measure.go`:

```go
package rollup

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/turbolytics/sql-flow/internal/config"
)

// Completeness is how many source buckets one closed bucket holds, against
// how many its width holds.
type Completeness struct {
	Table           string
	BucketAt        time.Time
	SourceBuckets   int64
	ExpectedBuckets int64
}

// LeastComplete reads the newest closed bucket of every table of r whose
// set counts source buckets, and returns the least complete. A bucket is
// closed once the table holds a newer one, so the open bucket, still
// filling, never reads as a gap. In a set with dimensions the bucket's
// fewest counted buckets stand for it. ok is false for a rollup with no
// count_buckets measure, and before any such table has closed a bucket.
func LeastComplete(ctx context.Context, conn *pgx.Conn, r config.Rollup) (Completeness, bool, error) {
	source, err := config.ParseServeDuration(r.Source.Grain)
	if err != nil {
		return Completeness{}, false, err
	}
	t := quote(r.Source.TimeColumn)
	var worst Completeness
	found := false
	for _, e := range edges(r) {
		measure := ""
		for _, name := range e.Set.MeasureNames() {
			if e.Set.Measures[name].Type == "count_buckets" {
				measure = name
				break
			}
		}
		if measure == "" {
			continue
		}
		var bucket *time.Time
		var present *int64
		err := conn.QueryRow(ctx, fmt.Sprintf(`WITH closed AS (
  SELECT max(%[1]s) AS b FROM %[2]s WHERE %[1]s < (SELECT max(%[1]s) FROM %[2]s))
SELECT closed.b, (SELECT min(%[3]s) FROM %[2]s WHERE %[1]s = closed.b) FROM closed`,
			t, quote(e.Table), quote(measure))).Scan(&bucket, &present)
		if err != nil {
			return Completeness{}, false, fmt.Errorf("rollup completeness %s: %w", e.Table, err)
		}
		if bucket == nil || present == nil {
			continue
		}
		c := Completeness{Table: e.Table, BucketAt: *bucket, SourceBuckets: *present,
			ExpectedBuckets: int64(e.Grain.Width / source)}
		if !found || c.SourceBuckets*worst.ExpectedBuckets < worst.SourceBuckets*c.ExpectedBuckets {
			worst, found = c, true
		}
	}
	return worst, found, nil
}

// TriggerCost sums the calls and time of r's trigger functions from
// pg_stat_user_functions. The server counts them only in sessions whose
// track_functions is pl or all, and holds no row otherwise, so ok false
// means unknown, not free.
func TriggerCost(ctx context.Context, conn *pgx.Conn, r config.Rollup) (calls int64, seconds float64, ok bool, err error) {
	var names []string
	for _, e := range edges(r) {
		names = append(names, "sqlflow_rollup_"+e.Table)
	}
	var c *int64
	var s *float64
	if err := conn.QueryRow(ctx, `SELECT sum(calls)::bigint, sum(total_time) / 1000
FROM pg_stat_user_functions WHERE funcname = ANY($1) AND schemaname = current_schema()`, names).Scan(&c, &s); err != nil {
		return 0, 0, false, fmt.Errorf("rollup trigger cost: %w", err)
	}
	if c == nil || s == nil {
		return 0, 0, false, nil
	}
	return *c, *s, true, nil
}
```

- [ ] **Step 4: Run** the Step 2 command — Expected: PASS.

- [ ] **Step 5: Commit** `rollup: the least complete closed bucket, and the triggers' cost`.

---

### Task 5: The daemon reports

**Files:** Create `internal/rollup/daemon/report.go`. Modify `internal/rollup/daemon/daemon.go`, `internal/rollup/daemon/metrics.go`, `internal/rollup/daemon/daemon_integration_test.go`, `internal/cli/rollup/run.go`.

**Interfaces:**
- Consumes: `freshness.Observe`, `freshness.StoreOf`; `rollup.LeastComplete`, `rollup.TriggerCost`; `turbostats.Collect`, `turbostats.Source`, `turbostats.RollupSource`, `turbostats.NewReporter`, `turbostats.StartReporter`, `turbostats.ExitReason`, `turbostats.Static`; `activity.Start`; `wire.ParseCredential`; `errs.CodeOf`, `errs.ExitCode`.
- Produces: `Options.TurboStats *config.TurboStats`, `Options.ConfigHash string`, `Options.Commit string`; `func (d *Daemon) CollectBundle(ctx context.Context) (turbostats.Bundle, error)`.

- [ ] **Step 1: Write the failing test** in `daemon_integration_test.go`:

```go
// receiver is a control plane on the loopback, where report_to allows
// plaintext. It keeps every bundle it is sent.
func receiver(t *testing.T) (string, func() []wire.Bundle) {
	t.Helper()
	var mu sync.Mutex
	var got []wire.Bundle
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var b wire.Bundle
		if err := json.NewDecoder(r.Body).Decode(&b); err == nil {
			mu.Lock()
			got = append(got, b)
			mu.Unlock()
		}
		w.Header().Set("Content-Type", wire.MediaType)
		_, _ = w.Write([]byte(`{"v":1,"commands":[]}`))
	}))
	t.Cleanup(srv.Close)
	return srv.URL + "/v1/turbostats", func() []wire.Bundle {
		mu.Lock()
		defer mu.Unlock()
		return append([]wire.Bundle(nil), got...)
	}
}

// The leader reports every table's newest bucket under one store id, and
// each rollup's totals. A second instance, standing by, reports its role and
// nothing else.
func TestIntegrationRollupRun_TheLeaderReportsItsRollupsAndFreshness(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	dsn, conn := startPostgres(t)
	history(t, conn, "2026-09-12T00:00:00Z", "2026-09-12T23:59:00Z")
	url, bundles := receiver(t)
	ts := &config.TurboStats{ID: "rollups-01", ReportTo: url,
		Key: "sfc_AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8", IntervalSeconds: 1}
	leader, _ := running(t, dsn, Options{TurboStats: ts, ConfigHash: "sha256:test"})
	waitFor(t, "healthy", func() bool { return status(leader) == "healthy" })

	var last wire.Bundle
	waitFor(t, "a leader's bundle with both sections", func() bool {
		for _, b := range bundles() {
			if b.Rollup != nil && b.Rollup.Role == "leader" && b.Freshness != nil &&
				len(b.Rollup.Rollups) == 1 && b.Rollup.Rollups[0].VerifyBucketCount > 0 {
				last = b
				return true
			}
		}
		return false
	})
	assert.Equal(t, "rollups-01", last.Instance.ID)
	assert.That(t, strings.HasPrefix(last.Freshness.StoreID, "pg:"))
	assert.Equal(t, "system", last.Freshness.StoreIDKind)
	assert.Equal(t, 11, len(last.Freshness.Tables))
	byName := map[string]wire.FreshTable{}
	for _, ft := range last.Freshness.Tables {
		byName[ft.Table] = ft
	}
	assert.Equal(t, int64(60), byName["public.posts_per_minute_by_lang"].GrainSeconds)
	hour := byName["public.posts_by_lang_1h"]
	assert.Equal(t, int64(3600), hour.GrainSeconds)
	assert.That(t, hour.NewestBucketAt != nil && hour.NewestBucketAt.Equal(time.Date(2026, 9, 12, 23, 0, 0, 0, time.UTC)))
	posts := last.Rollup.Rollups[0]
	assert.Equal(t, "posts", posts.Name)
	assert.Equal(t, "trigger", posts.Strategy)
	assert.Equal(t, int64(0), posts.DriftBucketCount)
	assert.That(t, posts.Backfill == nil)
	assert.That(t, posts.Completeness != nil && posts.Completeness.ExpectedBuckets > 0)
	assert.That(t, posts.Triggers == nil)

	standbyURL, standbyBundles := receiver(t)
	standbyTS := *ts
	standbyTS.ReportTo = standbyURL
	standby, _ := running(t, dsn, Options{TurboStats: &standbyTS, ConfigHash: "sha256:test"})
	waitFor(t, "standby", func() bool { return status(standby) == "standby" })
	waitFor(t, "a standby's bundle", func() bool {
		for _, b := range standbyBundles() {
			if b.Rollup != nil && b.Rollup.Role == "standby" {
				assert.That(t, b.Freshness == nil && len(b.Rollup.Rollups) == 0)
				return true
			}
		}
		return false
	})
}
```

Add imports `encoding/json`, `net/http/httptest`, `github.com/turbolytics/sql-flow/internal/config` if missing, and `github.com/turbolytics/sql-flow/turbostats/wire`.

- [ ] **Step 2: Run** `go test -count=1 -run TestIntegrationRollupRun_TheLeaderReportsItsRollupsAndFreshness$ ./internal/rollup/daemon/` — Expected: FAIL to compile, `unknown field TurboStats in struct literal of type Options`.

- [ ] **Step 3: The manual reader.** In `metrics.go`, `newProvider` attaches a manual reader always, as serve and run do, and returns it:

```go
// newProvider builds the meter provider, the manual reader the TurboStats
// bundle reads, and, for the prometheus exporter, the registry /metrics
// serves. The manual reader is always attached, so a daemon that reports
// without serving /metrics still has its totals.
func newProvider(exporter string) (metric.MeterProvider, *sdkmetric.ManualReader, *prom.Registry, error) {
	reader := sdkmetric.NewManualReader()
	opts := []sdkmetric.Option{sdkmetric.WithReader(reader)}
	switch strings.ToLower(strings.TrimSpace(exporter)) {
	case "":
		return sdkmetric.NewMeterProvider(opts...), reader, nil, nil
	case "prometheus":
		registry := prom.NewRegistry()
		exp, err := prometheus.New(prometheus.WithRegisterer(registry))
		if err != nil {
			return nil, nil, nil, fmt.Errorf("prometheus exporter: %w", err)
		}
		opts = append(opts, sdkmetric.WithReader(exp))
		return sdkmetric.NewMeterProvider(opts...), reader, registry, nil
	default:
		return nil, nil, nil, errs.New(errs.CodeConfigInvalid,
			"--metrics %q is not an exporter this version serves; use prometheus", exporter)
	}
}
```

Update the one caller and the metrics unit test to take four results.

- [ ] **Step 4: What the daemon reports.** Create `internal/rollup/daemon/report.go`:

```go
package daemon

import (
	"sync"
	"time"

	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/errs"
	"github.com/turbolytics/sql-flow/internal/freshness"
	"github.com/turbolytics/sql-flow/internal/turbostats"
)

// report is what the daemon's passes measured and counted, for TurboStats.
// The loop writes it; the reporter's goroutine reads it.
type report struct {
	mu        sync.Mutex
	store     *freshness.Store
	observed  time.Time
	tables    []turbostats.FreshTable
	byRollup  map[string]*rollupTotals
	lastCode  string
	lastAt    time.Time
}

// rollupTotals is one rollup's counts since the process started, and what
// its last observe pass read.
type rollupTotals struct {
	verified, drifted int64
	pending           map[string]bool
	historyLeft       map[string]time.Duration
	completeness      *turbostats.RollupCompleteness
	triggers          *turbostats.RollupTriggers
}

func newReport(conf *config.RollupsConf) *report {
	r := &report{byRollup: map[string]*rollupTotals{}}
	for _, ro := range conf.Rollups {
		r.byRollup[ro.Name] = &rollupTotals{pending: map[string]bool{}, historyLeft: map[string]time.Duration{}}
	}
	return r
}

func (r *report) setStore(s freshness.Store) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.store = &s
}

// setObserved replaces the tables an observe pass read, all at once, so a
// bundle never mixes two passes.
func (r *report) setObserved(at time.Time, tables []turbostats.FreshTable) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.observed, r.tables = at, tables
}

func (r *report) setMeasured(rollup string, c *turbostats.RollupCompleteness, tr *turbostats.RollupTriggers) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if t := r.byRollup[rollup]; t != nil {
		t.completeness, t.triggers = c, tr
	}
}

func (r *report) addVerified(rollup string, buckets, drifted int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if t := r.byRollup[rollup]; t != nil {
		t.verified += buckets
		t.drifted += drifted
	}
}

// setPending records the tables still filling, rollup by rollup, as the
// leader last read them. A table that left the list forgets its history.
func (r *report) setPending(pending map[string][]string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for name, t := range r.byRollup {
		now := map[string]bool{}
		for _, table := range pending[name] {
			now[table] = true
		}
		for table := range t.historyLeft {
			if !now[table] {
				delete(t.historyLeft, table)
			}
		}
		t.pending = now
	}
}

func (r *report) setHistoryLeft(rollup, table string, left time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if t := r.byRollup[rollup]; t != nil {
		t.historyLeft[table] = left
	}
}

func (r *report) failed(err error, at time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lastCode, r.lastAt = string(errs.CodeOf(err)), at
}

// section is the bundle's rollup and freshness sections. A process that
// does not lead reports its role and its last error, and nothing it has
// not measured as leader.
func (r *report) section(role string, rollups []config.Rollup) (*turbostats.Rollup, *turbostats.Freshness) {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := &turbostats.Rollup{Role: role}
	if r.lastCode != "" {
		code, at := r.lastCode, r.lastAt.UTC()
		out.LastErrorCode, out.LastErrorAt = &code, &at
	}
	if role != roleLeader {
		return out, nil
	}
	for _, ro := range rollups {
		t := r.byRollup[ro.Name]
		e := turbostats.RollupEntry{Name: ro.Name, Strategy: "trigger",
			VerifyBucketCount: t.verified, DriftBucketCount: t.drifted,
			Completeness: t.completeness, Triggers: t.triggers}
		if len(t.pending) > 0 {
			e.Backfill = &turbostats.RollupBackfill{TablesLeft: len(t.pending)}
			var most time.Duration
			for _, left := range t.historyLeft {
				if left > most {
					most = left
				}
			}
			if len(t.historyLeft) > 0 {
				s := int64(most / time.Second)
				e.Backfill.HistoryLeftSeconds = &s
			}
		}
		out.Rollups = append(out.Rollups, e)
	}
	if r.store == nil || r.observed.IsZero() {
		return out, nil
	}
	return out, &turbostats.Freshness{StoreID: r.store.ID, StoreIDKind: r.store.Kind,
		ObservedAt: r.observed.UTC(), Tables: append([]turbostats.FreshTable(nil), r.tables...)}
}
```

- [ ] **Step 5: Wire it into the daemon.** In `daemon.go`:
  - `Options` gains `TurboStats *config.TurboStats`, `ConfigHash string` and `Commit string`, each commented.
  - `Daemon` gains `reader *sdkmetric.ManualReader`, `report *report`, `clock *activity.Clock`, `static turbostats.Static`, `key ed25519.PrivateKey`.
  - `New` takes the reader from `newProvider`, builds `report` and `clock := activity.Start()`, parses `opts.TurboStats.Key` with `wire.ParseCredential` when `opts.TurboStats.Enabled()` and returns `errs.New(errs.CodeConfigInvalid, "turbostats.key is not a credential")` on failure, and fills `static` as serve does: `ID` and `IntervalSeconds` only when enabled, `Version`, `Commit`, `ConfigHash`, `StartedAt: clock.StartedAt()`, `Clock: clock`, `Labels: opts.TurboStats.LabelSet()`.
  - A method replaces every `d.m.errors.Add(ctx, 1, phase(p))`: `d.fail(ctx, p, err)` adds the counter and calls `d.report.failed(err, time.Now())`.
  - `logStore` also calls `d.report.setStore(s)`.
  - `observe` collects a `[]turbostats.FreshTable` from each `freshness.Observation` (`Table: o.Table`, `GrainSeconds: int64(o.Grain / time.Second)`, `NewestBucketAt: o.NewestBucketAt`) and, after the loop, when at least one table was read, calls `d.report.setObserved(latestObservedAt, tables)`. Per rollup it calls `rollup.LeastComplete` and `rollup.TriggerCost` and passes their results, nil when not ok, to `d.report.setMeasured`; an error goes through `d.fail(ctx, "observe", err)`.
  - `verify` calls `d.report.addVerified(v.Rollup, v.Buckets, v.DriftBuckets)` beside the instruments, and `d.clock.Mark()` at the end of a pass.
  - `fillOne` builds `map[rollup][]table` from the pending list and calls `d.report.setPending`. `fillChunk` calls `d.report.setHistoryLeft(step.Rollup, step.Table, step.Remaining)` after a chunk that did not complete, and `d.clock.Mark()` after every chunk.
  - `CollectBundle`:

```go
// CollectBundle is this process's TurboStats bundle.
func (d *Daemon) CollectBundle(ctx context.Context) (turbostats.Bundle, error) {
	return turbostats.Collect(ctx, turbostats.Source{
		Static: d.static,
		Reader: d.reader,
		Rollup: &turbostats.RollupSource{Section: func() (*turbostats.Rollup, *turbostats.Freshness) {
			return d.report.section(d.health.get().Role, d.conf.Rollups)
		}},
	})
}
```

  - `Run` becomes `func (d *Daemon) Run(ctx context.Context) (err error)`. After `serveHTTP`, when `d.opts.TurboStats.Enabled()`:

```go
		reporter, rerr := turbostats.NewReporter(turbostats.ReporterConfig{
			ReportTo: d.opts.TurboStats.ReportTo, Key: d.key, Interval: d.opts.TurboStats.Interval(),
			Collect: d.CollectBundle, Log: d.log.Named("turbostats"),
		})
		if rerr != nil {
			return rerr
		}
		stop := turbostats.StartReporter(ctx, reporter)
		// The last bundle says how the process ended. A crash never
		// reaches it, which is how a receiver tells the two apart.
		defer func() {
			final, cancel := context.WithTimeout(context.WithoutCancel(ctx), reportGrace)
			defer cancel()
			stop(final, turbostats.Exit{Reason: turbostats.ExitReason(err, ctx.Err()), Code: errs.ExitCode(err)})
		}()
```

    with `const reportGrace = 10 * time.Second` beside the other constants, commented: it bounds the final bundle, and the reporter's own timeout is 10 s.

- [ ] **Step 6: The CLI.** In `internal/cli/rollup/run.go`, render once and parse, so the bundle's config hash is of the text the daemon runs:

```go
			rendered, err := config.RenderTemplate(configPath, nil)
			if err != nil {
				return err
			}
			conf, err := config.ParseRollups(rendered)
			if err != nil {
				return err
			}
```

and pass `TurboStats: conf.TurboStats, ConfigHash: turbostats.HashConfig(rendered), Commit: buildinfo.Commit` in `daemon.Options`.

- [ ] **Step 7: Run** `go test -count=1 ./internal/rollup/daemon/ ./internal/cli/rollup/` — Expected: PASS, every Plan 2 test included.

- [ ] **Step 8: Commit** `daemon: report freshness and each rollup's totals to TurboStats`.

---

### Task 6: The image reports

**Files:** Modify `tests/release/test_image.py`, `docs/coverage/features.yml`.

- [ ] **Step 1: Write the release test** after `wait_for_posts`:

```python
ROLLUPS = """
store:
  type: postgres
  postgres:
    dsn: postgres://postgres:rollup@db:5432/postgres?sslmode=disable
turbostats:
  id: rollups-release
  report_to: http://127.0.0.1:8080/v1/turbostats
  key: sfc_AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8
  interval_seconds: 1
rollups:
  - name: posts
    source:
      table: posts_per_minute
      time_column: bucket
      grain: 1m
      dimensions: []
    grains:
      5m: {from: 1m}
      1h: {from: 5m}
    dimension_sets:
      - name: posts_total
        dimensions: []
        measures:
          posts: {type: sum, column: posts}
          minutes: {type: count_buckets}
"""


@pytest.mark.covers("cli.rollup_run", "observability.turbostats.reporter")
def test_cli_rollup_run_reports_its_rollups_and_freshness(image):
    """`sqlflow rollup run` in the shipped image fills a Postgres, verifies it,
    and reports both TurboStats sections.

    The daemon shares the receiver's network namespace, so it reports to the
    loopback that report_to requires for plaintext, and reaches the database
    by its alias on the same network.
    """
    network = Network().create()
    db = DockerContainer("postgres:18") \
        .with_env("POSTGRES_PASSWORD", "rollup") \
        .with_network(network) \
        .with_network_aliases("db")
    db.start()
    receiver = DockerContainer("python:3.12-alpine") \
        .with_network(network) \
        .with_command(["python", "-u", "-c", RECEIVER])
    receiver.start()
    try:
        deadline = time.time() + 60
        while db.exec(["pg_isready", "-U", "postgres"]).exit_code != 0:
            assert time.time() < deadline, "postgres never became ready"
            time.sleep(0.5)
        for sql in [
            "CREATE TABLE posts_per_minute (bucket TIMESTAMPTZ PRIMARY KEY, posts INTEGER NOT NULL)",
            "INSERT INTO posts_per_minute SELECT g, 1 FROM generate_series("
            "'2026-09-12T00:00:00Z'::timestamptz, '2026-09-12T23:59:00Z', interval '1 minute') AS g",
        ]:
            result = db.exec(["psql", "-U", "postgres", "-v", "ON_ERROR_STOP=1", "-c", sql])
            assert result.exit_code == 0, result.output

        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "rollups.yml")
            with open(path, "w") as f:
                f.write(ROLLUPS)
            os.chmod(tmp, 0o755)
            os.chmod(path, 0o644)
            daemon = DockerContainer(image) \
                .with_volume_mapping(tmp, "/conf") \
                .with_kwargs(network_mode=(
                    f"container:{receiver.get_wrapped_container().id}")) \
                .with_command("rollup run -c /conf/rollups.yml")
            daemon.start()
            try:
                bundle = wait_for_bundle(receiver, lambda b: (
                    b.get("rollup", {}).get("role") == "leader"
                    and "freshness" in b
                    and b["rollup"]["rollups"][0]["verify_bucket_count"] > 0), timeout=90)
            finally:
                daemon.stop()
    finally:
        receiver.stop()
        db.stop()
        network.remove()

    assert bundle["instance"]["id"] == "rollups-release"
    assert "pipeline" not in bundle and "serve" not in bundle
    fresh = bundle["freshness"]
    assert fresh["store_id"].startswith("pg:") and fresh["store_id_kind"] == "system"
    tables = {t["table"]: t for t in fresh["tables"]}
    assert set(tables) == {"public.posts_per_minute", "public.posts_total_5m", "public.posts_total_1h"}
    assert tables["public.posts_total_1h"]["grain_seconds"] == 3600
    assert tables["public.posts_total_1h"]["newest_bucket_at"] == "2026-09-12T23:00:00Z"
    posts = bundle["rollup"]["rollups"][0]
    assert posts["name"] == "posts" and posts["strategy"] == "trigger"
    assert posts["drift_bucket_count"] == 0
    assert posts["completeness"]["expected_buckets"] in (5, 60)
    # The shape guard's promise, in the artifact: nothing below a table or a
    # rollup entry repeats.
    for entry in fresh["tables"] + bundle["rollup"]["rollups"]:
        for value in entry.values():
            if isinstance(value, dict):
                assert all(not isinstance(v, (list, dict)) for v in value.values())
            else:
                assert not isinstance(value, list)


def wait_for_bundle(receiver, match, timeout):
    """Read the receiver's posts until a bundle matches."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        out, _ = receiver.get_logs()
        for line in out.decode().splitlines():
            if line.startswith("{"):
                body = json.loads(line)["body"]
                if match(body):
                    return body
        time.sleep(0.5)
    raise AssertionError(f"no matching bundle arrived in {timeout}s")
```

- [ ] **Step 2: Coverage.** In `docs/coverage/features.yml`, `cli.rollup_run` requires `[unit, integration, release]`. Run `make coverage-page` and commit what it regenerates.

- [ ] **Step 3: Run** `make sqlflow-image`, then `SQLFLOW_IMAGE=<the tag it prints> uv run --locked pytest tests/release -q -k "rollup or turbostats"` — Expected: PASS.

- [ ] **Step 4: Commit** `release: the image's rollup daemon reports both TurboStats sections`.

---

### Task 7: Docs, the spec, and the PR

- [ ] **Step 1: Spec.** In "TurboStats": replace the `freshness` example and bullets with the object shape above, the rollup example with per-rollup `completeness` and `triggers` objects and `backfill` as `tables_left` and `history_left_seconds`, and state the cap and why. In "Configuration", add rule 3: the cap. In "Tests", the release row names the new test. In "What breaks", a row: a list that grows with the file without a cap makes a bundle a receiver refuses, `TestCollect_AFullRollupBundleStaysUnderTheCeiling`.
- [ ] **Step 2: README** under the rollup section: `run` reports to TurboStats when the file has a `turbostats` block, what it sends, and the cap. **CHANGELOG** under Unreleased, Added.
- [ ] **Step 3: The whole suite:** `go build ./... && go test -count=1 -short ./... && go test -count=1 ./internal/rollup/... ./internal/freshness/ ./internal/cli/rollup/ ./internal/turbostats/ ./turbostats/... && uv run --locked pytest tests/tooling -q`, and the release run from Task 6.
- [ ] **Step 4: Commit** the docs, then paste the PR's title and body in the chat and wait for the maintainer's go before pushing.

---

## After execution

Filled in once the plan has run.
