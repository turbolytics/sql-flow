# `sqlflow rollup test` Implementation Plan (Plan 4)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `sqlflow rollup test -c rollups.yml --dsn DSN [--tests FILE] [--seed N] [--keep]` checks a rollups declaration on a real Postgres, in a schema it creates and drops: a seeded generated workload that crosses a daylight saving change, then fixture cases, with two invariants checked on every final state.

**Architecture:** `internal/config` gains the test file and its rules. `internal/rollup` gains a sandbox (a schema with a clone of each source, where `install` runs through `search_path`), the invariants (verify's comparison pointed at the source, and a boundary check), the generated workload, and the fixture runner. The cobra command wires them and exits with a new code, `user.config.rollup_test_failed`.

**Tech Stack:** Go 1.26, pgx/v5, cobra, gopkg.in/yaml, testcontainers-go Postgres module, zeebo/assert.

**Spec:** `docs/superpowers/specs/2026-09-24-rollup-daemon-design.md`, "`sqlflow rollup test`".

## Where this plan sits

| Plan | Scope | State |
|---|---|---|
| 1, 2a, 2b, 3 | install, run, verify, freshness, TurboStats | Merged: #380, #382, #388, #390, #394 |
| **4, this one** | `sqlflow rollup test` | |
| 5 | A release, then the Bluesky demo, the Render template, and the parity proof | |

## Global Constraints

- One branch, `feat/rollup-test`, from main at `1440080`. Push with `git push -u origin feat/rollup-test`; gate pushes to an open PR on its state.
- The command connects only to `--dsn`. It never reads `store.postgres.dsn`.
- The sandbox schema is `sqlflow_test_` and 16 hex characters from `crypto/rand`. Every source is cloned with `CREATE TABLE <schema>.<t> (LIKE <src> INCLUDING DEFAULTS INCLUDING IDENTITY INCLUDING GENERATED INCLUDING INDEXES)`, which keeps `NOT NULL`, defaults and indexes and leaves out `CHECK` constraints.
- `install` runs unchanged in the sandbox: the session's `search_path` is the sandbox schema alone.
- The generated workload, per rollup: 20 batches of 1 to 50 rows, source buckets from 2026-03-07T00:00Z to 2026-03-10T00:00Z, one row in ten rewriting an earlier batch's key, each batch in a session `TimeZone` of `UTC`, `America/New_York` or `Asia/Kolkata`. Integers 0 to 1,000; doubles -1,000 to 1,000 with fractions; text from a pool of five; timestamps inside the row's source bucket; `NULL` only in nullable dimension columns, one in ten. Any other type stops the run naming the column and its type.
- Every write is one statement in its own transaction, in the sink's upsert shape: `ON CONFLICT (<time column>, <source dimensions>) DO UPDATE SET` every other written column.
- Values bind as text cast to the column's type, `$n::<format_type>`, for the workload and the fixtures alike.
- The invariants: every table equals a from-scratch `GROUP BY` of its source at its own width, with verify's comparison; and every bucket lies on its grain's UTC boundary.
- New error code `user.config.rollup_test_failed` (`errs.CodeConfigRollupTestFailed`), a user code, so it exits 10.
- New coverage feature `cli.rollup_test`, requires `[unit, integration]`. Unit tests `TestCliRollupTest_*`, integration `TestIntegrationRollupTest_*`, each calling `coverage.Covers(t, "cli.rollup_test")`.
- Run `go`, `make`, `docker`, `gh` and `uv` outside the sandbox. One heredoc per command; commit messages go in a file.
- Prose follows the repo's `CLAUDE.md`.

## Rulings made while planning

- **The workload always runs, and the fixture cases run after it when `--tests` is given.** The spec's step 4 says the command "runs the generated workload and the fixture cases"; its later sentence "Without `--tests`, the command writes to each source" reads as the default, not as an exclusion. Running both checks more, and each case starts from empty tables anyway.
- **Invariant 1 reuses verify.** A from-scratch `GROUP BY` of the source is verify's statement with the edge's `from` set to the source, so the comparison rules are verify's by construction: `NULL` dimensions pair, and double sums match within 1e-9.
- **`--dsn` is parsed before `rollup.Connect`,** so a bad value reads "--dsn is not a Postgres connection string", not the store's message.
- **The rule that every written key is a source column runs against the clone,** since only the database knows every column. It reports its YAML path like the static rules.

## Review Focus

1. **A declaration with a nullable dimension** passes the workload: `NULL` keys pair and upsert. Task 3, `TestIntegrationRollupTest_ANullableDimensionPasses`.
2. **The Render template's `metrics_1m`**, with its `CHECK` constraint and a defaulted `updated_at`, passes the workload. Task 3, `TestIntegrationRollupTest_TheRenderTemplatePasses`.
3. **A fixture that expects the wrong rows** fails with the case, the table, the key, and both rows. Task 4, `TestIntegrationRollupTest_AWrongExpectationFailsWithADiff`.
4. **`--seed` reproduces a run**, and a different seed does not. Task 3, `TestIntegrationRollupTest_ASeedReproducesTheWorkload`.
5. **The sandbox is dropped** after a failing run, and kept with `--keep`. Task 5, `TestIntegrationRollupTest_TheCommandDropsItsSchemaUnlessKept`.

## File Structure

```
internal/errs/registry.go, testdata/codes.golden          user.config.rollup_test_failed
internal/config/rollup_tests.go, rollup_tests_test.go     the test file and its rules
internal/rollup/harness.go                                 Sandbox, CheckInvariants
internal/rollup/workload.go                                the generated workload
internal/rollup/fixtures.go                                fixture cases
internal/rollup/harness_integration_test.go                sandbox, invariants, workload, fixtures
internal/cli/rollup/test.go, test_test.go, test_integration_test.go   the command
dev/config/rollups/bluesky.test.yml                        the demo's fixture cases
docs/coverage/features.yml, status/features.yml, matrix.md cli.rollup_test
README.md, CHANGELOG.md, the spec                          docs
```

---

### Task 1: The code and the test file

**Files:** `internal/errs/registry.go`, `internal/errs/testdata/codes.golden`, create `internal/config/rollup_tests.go`, `internal/config/rollup_tests_test.go`, create `dev/config/rollups/bluesky.test.yml`.

**Interfaces — Produces:** `errs.CodeConfigRollupTestFailed`; `type RollupTests struct { Tests []RollupTestCase }`; `type RollupTestCase struct { Name, Rollup string; Writes [][]map[string]any; Expect map[string][]map[string]any }`; `func LoadRollupTests(path string) (*RollupTests, error)`; `func (t *RollupTests) Check(conf *RollupsConf) []Violation`; `func (t *RollupTests) CheckError(conf *RollupsConf) error`; `func RollupTableColumns(r Rollup) map[string][]string` (table → its columns in order).

- [ ] **Step 1: Failing tests.** In `internal/config/rollup_tests_test.go`:

```go
package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

func TestCliRollupTest_TheDemosCasesAreValid(t *testing.T) {
	coverage.Covers(t, "cli.rollup_test")
	conf, err := LoadRollups("../../dev/config/rollups/bluesky.yml")
	assert.NoError(t, err)
	tests, err := LoadRollupTests("../../dev/config/rollups/bluesky.test.yml")
	assert.NoError(t, err)
	assert.That(t, len(tests.Tests) >= 2)
	assert.Equal(t, 0, len(tests.Check(conf)))
}

// Each rule is reported at its YAML path.
func TestCliRollupTest_EachRuleReportsItsPath(t *testing.T) {
	coverage.Covers(t, "cli.rollup_test")
	conf, err := LoadRollups("../../dev/config/rollups/bluesky.yml")
	assert.NoError(t, err)
	path := filepath.Join(t.TempDir(), "bad.test.yml")
	assert.NoError(t, os.WriteFile(path, []byte(`tests:
  - name: ""
    rollup: nope
    writes: []
  - name: wrong table and a short row
    rollup: posts
    writes:
      - [{bucket: "2026-09-24T12:07:00Z", lang: en, posts: 5}]
    expect:
      posts_by_lang_2m:
        - {bucket: "2026-09-24T12:00:00Z"}
      posts_by_lang_5m:
        - {bucket: "2026-09-24T12:05:00Z", lang: en}
`), 0o644))
	tests, err := LoadRollupTests(path)
	assert.NoError(t, err)
	got := map[string]bool{}
	for _, v := range tests.Check(conf) {
		got[strings.Join(v.Path, ".")] = true
	}
	for _, want := range []string{"tests.0.name", "tests.0.rollup", "tests.0.writes",
		"tests.1.expect.posts_by_lang_2m", "tests.1.expect.posts_by_lang_5m.0"} {
		assert.That(t, got[want])
	}
}
```

Add `"strings"` to the imports. Create `dev/config/rollups/bluesky.test.yml` with the spec's case, and a second one for `count_buckets` across a missing minute:

```yaml
# Fixture cases for dev/config/rollups/bluesky.yml:
#
#   sqlflow rollup test -c dev/config/rollups/bluesky.yml --dsn "$TEST_DSN" \
#     --tests dev/config/rollups/bluesky.test.yml
tests:
  - name: a republished minute replaces its count at every grain
    rollup: posts
    writes:
      - [{bucket: "2026-09-24T12:07:00Z", lang: en, posts: 5}]
      - [{bucket: "2026-09-24T12:07:00Z", lang: en, posts: 9}]
    expect:
      posts_by_lang_5m:
        - {bucket: "2026-09-24T12:05:00Z", lang: en, posts: 9}
      posts_total_1h:
        - {bucket: "2026-09-24T12:00:00Z", posts: 9, minutes: 1}

  - name: minutes counts the source minutes present, across languages
    rollup: posts
    writes:
      - [{bucket: "2026-09-24T12:00:00Z", lang: en, posts: 1},
         {bucket: "2026-09-24T12:00:00Z", lang: ja, posts: 2},
         {bucket: "2026-09-24T12:02:00Z", lang: en, posts: 3}]
    expect:
      posts_total_5m:
        - {bucket: "2026-09-24T12:00:00Z", posts: 6, minutes: 2}
```

- [ ] **Step 2: Run** `go test -count=1 -short -run TestCliRollupTest ./internal/config/` — Expected: FAIL to compile, `undefined: LoadRollupTests`.

- [ ] **Step 3: The code.** In `internal/errs/registry.go`, beside `CodeConfigRollupChange`:

```go
	// A rollups test case or an invariant failed: a table held rows the
	// declaration's writes do not make.
	CodeConfigRollupTestFailed Code = "user.config.rollup_test_failed"
```

with a definition: summary "A rollups test case or an invariant failed.", action "Read the failures the command printed: the case, the table, the key, and the expected and actual rows. Fix the declaration or the expectation, and run it again with the printed --seed to reproduce the same writes." Then `UPDATE_GOLDEN=1 go test -count=1 -run TestErrorTaxonomy_RegistryIsAppendOnly ./internal/errs/`.

- [ ] **Step 4: The test file.** Create `internal/config/rollup_tests.go`:

```go
package config

import (
	"strconv"

	"github.com/turbolytics/sql-flow/internal/errs"
)

// RollupTests is a file of fixture cases for `sqlflow rollup test`.
type RollupTests struct {
	Tests []RollupTestCase `yaml:"tests"`
}

// RollupTestCase is one case: writes to a rollup's source, each one
// statement in its own transaction, and the rows listed tables must hold
// afterward. Unlisted tables are checked by the invariants only.
type RollupTestCase struct {
	Name   string                      `yaml:"name"`
	Rollup string                      `yaml:"rollup"`
	Writes [][]map[string]any          `yaml:"writes"`
	Expect map[string][]map[string]any `yaml:"expect"`
}

// LoadRollupTests renders a test file and decodes it strictly.
func LoadRollupTests(path string) (*RollupTests, error) {
	rendered, err := RenderTemplate(path, nil)
	if err != nil {
		return nil, err
	}
	var t RollupTests
	if err := decodeStrict(rendered, &t); err != nil {
		return nil, err
	}
	return &t, nil
}

// RollupTableColumns is every table r declares and its columns, in order:
// the time column, the set's dimensions, then the measures by name. It is
// the rule rollup.Table and the generator follow.
func RollupTableColumns(r Rollup) map[string][]string {
	out := map[string][]string{}
	for _, set := range r.DimensionSets {
		cols := append([]string{r.Source.TimeColumn}, set.Dimensions...)
		cols = append(cols, set.MeasureNames()...)
		for _, g := range r.Ladder() {
			out[set.Name+"_"+g.Name] = cols
		}
	}
	return out
}

// Check reports every rule a case breaks, at its YAML path: a name, a
// declared rollup, at least one write, expect tables the rollup declares,
// and expected rows that name every column of their table. That every
// written key is a source column is checked against the database.
func (t *RollupTests) Check(conf *RollupsConf) []Violation {
	var out []Violation
	add := func(path []string, format string, args ...any) {
		out = append(out, Violation{Code: errs.CodeConfigRollup, Path: path, Message: fmt.Sprintf(format, args...)})
	}
	rollups := map[string]Rollup{}
	for _, r := range conf.Rollups {
		rollups[r.Name] = r
	}
	for i, tc := range t.Tests {
		at := []string{"tests", strconv.Itoa(i)}
		if tc.Name == "" {
			add(append(at, "name"), "a test case needs a name")
		}
		if len(tc.Writes) == 0 {
			add(append(at, "writes"), "test case %q writes nothing", tc.Name)
		}
		r, ok := rollups[tc.Rollup]
		if !ok {
			add(append(at, "rollup"), "test case %q names rollup %q, which the rollups file does not declare", tc.Name, tc.Rollup)
			continue
		}
		tables := RollupTableColumns(r)
		for table, rows := range tc.Expect {
			cols, ok := tables[table]
			if !ok {
				add(append(at, "expect", table), "test case %q expects table %s, which rollup %s does not declare", tc.Name, table, r.Name)
				continue
			}
			for j, row := range rows {
				for _, c := range cols {
					if _, ok := row[c]; !ok {
						add(append(at, "expect", table, strconv.Itoa(j)), "test case %q: an expected %s row names no %s; name every column", tc.Name, table, c)
						break
					}
				}
			}
		}
	}
	return out
}

// CheckError is Check as one coded error, or nil.
func (t *RollupTests) CheckError(conf *RollupsConf) error { ... as RollupsConf.CheckError does ... }
```

Mirror `RollupsConf.CheckError` for the error's shape. Keys of the `expect` map are visited in sorted order so violations are stable.

- [ ] **Step 5: Run** `go test -count=1 -short ./internal/config/ ./internal/errs/` — Expected: PASS.

- [ ] **Step 6: Commit** `config: the rollups test file, its rules, and user.config.rollup_test_failed`.

---

### Task 2: The sandbox and the invariants

**Files:** create `internal/rollup/harness.go`, `internal/rollup/harness_integration_test.go`.

**Interfaces — Produces:** `type Sandbox struct { Schema string }`; `func OpenSandbox(ctx context.Context, conn *pgx.Conn, conf *config.RollupsConf, version string) (*Sandbox, error)`; `func (s *Sandbox) Close(ctx context.Context, conn *pgx.Conn, keep bool) error`; `type InvariantFailure struct { Rollup, Table, Invariant string; Buckets int64; Sample []DriftRow }`; `func CheckInvariants(ctx context.Context, conn *pgx.Conn, conf *config.RollupsConf) ([]InvariantFailure, error)`.

- [ ] **Step 1: Failing tests** in `harness_integration_test.go`, on `startRollupPostgres`: `TestIntegrationRollupTest_TheSandboxClonesTheSourceAndInstallsThere` (a source with a `CHECK (posts >= 0)`: the clone lacks the check and keeps the primary key; `sqlflow_rollup_state` and `posts_by_lang_5m` exist in the sandbox schema and not in `public`; `Close` with keep false drops the schema, with keep true leaves it); `TestIntegrationRollupTest_TheInvariantsFindAHandEditAndAnOffBoundaryBucket` (after writes through the triggers, no failure; `UPDATE posts_by_lang_1h SET posts = posts + 1` yields an equality failure naming the table; an inserted row at `12:07` in `posts_by_lang_5m` yields a boundary failure).

- [ ] **Step 2: Run** them — Expected: FAIL to compile.

- [ ] **Step 3: Implement** `harness.go`: `OpenSandbox` resolves each distinct source's schema before touching `search_path` (a missing source stops with "source table %s does not exist; run the team's migrations against --dsn first"), creates the schema, clones each source, runs `SET search_path TO <schema>`, then `Install`. `Close` runs `SET search_path TO DEFAULT` and, unless keep, `DROP SCHEMA <schema> CASCADE`. `CheckInvariants` walks every declared edge: invariant 1 is `VerifySince` over `newTarget(r, e2)` where `e2` is `e` with `From = r.Source.Table` and `Grain.From = r.Source.Grain`; invariant 2 is `SELECT count(*) FROM <t> WHERE <time> <> <bin(width, time)>`.

- [ ] **Step 4: Run** — Expected: PASS. **Step 5: Commit** `rollup: a sandbox schema for rollups tests, and the two invariants`.

---

### Task 3: The generated workload

**Files:** create `internal/rollup/workload.go`, `internal/rollup/workload_test.go`; extend `harness_integration_test.go`.

**Interfaces — Produces:** `type WorkloadReport struct { Rollup string; Batches, Rows int; Digest string }`; `func RunWorkload(ctx context.Context, conn *pgx.Conn, conf *config.RollupsConf, seed int64) ([]WorkloadReport, error)`; unexported `sourceColumns`, `generate(col, bucket, rng)`, `upsertSQL`.

- [ ] **Step 1: Failing tests.** Unit (`workload_test.go`): `TestCliRollupTest_EachTypeGeneratesItsValues` (integers in [0, 1000], doubles in [-1000, 1000] with a fraction, text from the pool, a timestamp inside its bucket) and `TestCliRollupTest_AnUnknownTypeStopsTheRun` (`bytea` returns an error naming the column and `bytea`). Integration: `TheDemoPassesTheWorkload` (invariants hold after it; 20 batches per rollup), `TheRenderTemplatePasses` (apply `render/migrations/0001_metrics_1m.sql`, load `render/rollups.yml`), `ANullableDimensionPasses` (a source whose dimension is nullable with a `NULLS NOT DISTINCT` unique index), `ASeedReproducesTheWorkload` (two sandboxes, the same seed, equal digests; another seed, a different one).

- [ ] **Step 2: Run** — FAIL to compile.

- [ ] **Step 3: Implement.** `sourceColumns` reads `pg_attribute` with `format_type` for the clone. The written columns: the time column, the source dimensions, every measure column, and every other `NOT NULL` column without a default, identity or generation. The digest is a sha256 over every statement's arguments in order. Rewrites choose keys from earlier batches only, so no statement upserts one key twice. A missing unique index on the key surfaces as the database's error with "the sink's upsert needs a unique index on (%s)".

- [ ] **Step 4: Run** — PASS. **Step 5: Commit** `rollup: the generated workload, seeded, across a daylight saving change`.

---

### Task 4: Fixture cases

**Files:** create `internal/rollup/fixtures.go`; extend `harness_integration_test.go`.

**Interfaces — Produces:** `type CaseFailure struct { Table, Key, Kind, Expected, Actual string }`; `func RunCase(ctx context.Context, conn *pgx.Conn, conf *config.RollupsConf, index int, tc config.RollupTestCase) ([]CaseFailure, error)`.

- [ ] **Step 1: Failing tests:** `TheDemosCasesPass`, `AWrongExpectationFailsWithADiff` (expect `posts: 8` where 9 lands: one failure, kind `differs`, key `bucket=2026-09-24T12:05:00Z, lang=en`, both rows rendered), `AWriteToAColumnTheSourceLacksIsRefusedAtItsPath` (`tests.0.writes.0.0.nope`).
- [ ] **Step 2: Run** — FAIL.
- [ ] **Step 3: Implement.** `RunCase` sets `TimeZone` to `UTC`, truncates every source clone and rollup table of the file, runs each write as one upsert statement, compares each expected table by its key columns (text through the column's type; a `sum` with `numeric: double` within 1e-9 relative), then runs `CheckInvariants` and returns their failures as kind `invariant`.
- [ ] **Step 4: Run** — PASS. **Step 5: Commit** `rollup: fixture cases for rollups tests`.

---

### Task 5: `sqlflow rollup test`

**Files:** create `internal/cli/rollup/test.go`, `test_test.go`, `test_integration_test.go`; modify `internal/cli/rollup/rollup.go`; `docs/coverage/features.yml`, `status/features.yml`, `matrix.md`.

- [ ] **Step 1: Failing tests.** Unit: the command is listed; `--dsn` is required; a bad `--dsn` reads "--dsn is not a Postgres connection string" as `user.config.invalid`; a test file breaking a rule exits `user.config.rollup` before connecting. Integration: `TheCommandPassesTheDemo` (prints `seed N`, each rollup's batches and rows, each case `ok`), `AFailingCaseExitsWithTheCodeAndTheDiff`, `TheCommandDropsItsSchemaUnlessKept`, `TheCommandNeverReadsTheStore` (a file whose `store.postgres.dsn` names a port nothing listens on still passes).
- [ ] **Step 2: Run** — FAIL.
- [ ] **Step 3: Implement** the command: load and check the file and the test file, parse `--dsn`, connect, open the sandbox with a deferred close honouring `--keep`, print the seed, run the workload and the invariants, then each case, print failures, and return `errs.New(errs.CodeConfigRollupTestFailed, "%d of %d checks failed", ...)` when any failed. Register it in `rollup.go`. Add `cli.rollup_test` to `features.yml` with `requires: [unit, integration]`, its status row, and `make coverage-page`.
- [ ] **Step 4: Run** `go test -count=1 ./internal/cli/rollup/ ./internal/rollup/ ./internal/config/` — PASS. **Step 5: Commit** `cli: sqlflow rollup test`.

---

### Task 6: Docs and the PR

- [ ] README: a `test` row in the rollup commands table and a paragraph with the command line. CHANGELOG under Unreleased, Added. Spec: the `Tests` row `RollupTestCommand` names the new tests; the rulings above where the spec's text reads otherwise.
- [ ] The whole suite: `go build ./... && go test -count=1 -short ./... && go test -count=1 ./internal/rollup/... ./internal/cli/rollup/ ./internal/config/ && uv run --locked pytest tests/tooling -q`.
- [ ] Commit, then paste the PR's title and body in the chat and wait for the maintainer's go.

---

## After execution

Filled in once the plan has run.
