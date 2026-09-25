# `sqlflow rollup install` Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `sqlflow rollup install` creates every rollup table, function and trigger a `rollups.yml` declares, in one transaction, records what it applied in `sqlflow_rollup_state`, and refuses a change that would corrupt stored rows.

**Architecture:** `rollups.yml` gains a `store` and a `turbostats` block. The generator in `internal/rollup` splits the objects out of the migration, so `install` and `ddl` build the same tables and triggers. A pure planner compares the declaration with the state row. `Install` checks the source and every existing table, plans, and applies, all in one transaction behind an advisory lock. The command stays hidden until Plan 2 ships `sqlflow rollup run`, which backfills what `install` marks.

**Tech Stack:** Go 1.26, cobra, pgx/v5, testcontainers-go Postgres module, zeebo/assert, invopop/jsonschema through `make schema`.

**Spec:** `docs/superpowers/specs/2026-09-24-rollup-daemon-design.md`

## Where this plan sits

The spec is five plans. Each ships working, tested software on its own.

| Plan | Scope | Spec sections |
|---|---|---|
| **1, this one** | The `store` and `turbostats` blocks, the objects split, the planner, the state table, `sqlflow rollup install` (hidden) | Configuration; The daemon: state table, reconcile, taking over existing objects |
| 2 | Chunked backfill, the leader lock, the observe and verify loop, `/healthz`, metrics, `internal/freshness` and the store identity, `sqlflow rollup run` and `verify`; unhide `install` | The daemon: leader lock, backfill, loop, verify, shutdown; Observability except TurboStats |
| 3 | The TurboStats `freshness` list and `rollup` section, and the release test | Observability: TurboStats |
| 4 | `sqlflow rollup test` | `sqlflow rollup test` |
| 5 | A release, then the Bluesky demo, the Render template, and the parity proof | Rollout |

## Global Constraints

- One PR, on a branch from `origin/main` named `feat/rollup-install`, opened against `main`. Check the PR is OPEN before pushing to it.
- Generated DDL targets Postgres 15 or later. Integration tests run `postgres:18`.
- New error codes, append-only: `user.config.rollup_change` (`errs.CodeConfigRollupChange`, exit 10), `system.rollup.internal` (`errs.CodeRollupInternal`, exit 1), `system.rollup.unreachable` (`errs.CodeRollupUnreachable`, exit 1). A database the file does not fit reports `user.config.rollup` (`errs.CodeConfigRollup`, exit 10).
- The state table is `sqlflow_rollup_state`. The install lock is `pg_advisory_xact_lock(hashtextextended('sqlflow_rollup_install', 0))`.
- Rollup tables and the state table go in the connection's `current_schema()`.
- `sqlflow rollup ddl`, `serve` and `check` print exactly what they print today. `internal/rollup/testdata/*.sql` and `*.serve.yml` must not change.
- `sqlflow rollup install` is `Hidden: true` in this plan.
- Unit tests are `TestCliRollupRun_*` and call `coverage.Covers(t, "cli.rollup_run")`. Integration tests are `TestIntegrationRollupRun_*`, call the same marker, skip under `-short`, and start their own container. Attribution is by marker only.
- Unit: `go test -short -race ./...`. Integration: `go test -run '^TestIntegration' ./internal/rollup/ ./internal/cli/rollup/`. Goldens regenerate with `UPDATE_GOLDEN=1`.
- Run `go`, `make` and `docker` outside the sandbox.
- Prose follows the repo's `CLAUDE.md`: SQLFlow in prose, `sqlflow` for the command. Comments explain why. Commit subjects are `area: what changed`, and the body names the reason and what breaks if the change is wrong.

## Review Focus

These are the inputs the spec implies and a person deploying this will meet first. Each has a test in the task named.

1. **A cosmetic edit to `rollups.yml`** (key order, a sum's `numeric: integer` written out, dimensions reordered) must never read as a change and stop a deploy. Task 3, `TestCliRollupRun_AnEditThatChangesNoRowIsNoChange`.
2. **Several entrypoints running `install` at once during a deploy** must all succeed and create each object once. Task 5, `TestIntegrationRollupRun_FourInstallsAtOnce`.
3. **A rollback to a `rollups.yml` without a grain** must succeed, and the grain's tables must keep their rows and stay current. Task 5, `TestIntegrationRollupRun_InstallKeepsARemovedGrain`.
4. **An unreachable store or a wrong password** must fail with `system.rollup.unreachable` and never print the password. Task 6, `TestCliRollupRun_InstallNamesAnUnreachableStoreWithoutItsPassword`.
5. **A source outside `public`, reached through `search_path`,** must get its rollup tables and state row in `current_schema()`, where the triggers resolve them. Task 5, `TestIntegrationRollupRun_InstallCreatesTablesInTheCurrentSchema`.

## File Structure

```
internal/config/rollup.go                 RollupsConf.Store, .TurboStats, RollupStore, RollupPostgres, PostgresDSN
internal/config/rollup_check.go           store and turbostats rules
internal/config/rollup_store_test.go      their tests
internal/validate/schemas/rollups.json    regenerated by make schema
internal/errs/registry.go                 three codes
internal/errs/testdata/codes.golden       regenerated
internal/rollup/sql.go                    yamlPath, edge, edges
internal/rollup/postgres.go               tableQuery, checkNames, writeObjects, PostgresObjects
internal/rollup/objects_test.go           PostgresObjects and edges tests
internal/rollup/applied.go                Applied, AppliedFrom
internal/rollup/plan.go                   Plan, PlanChange, sortedKeys
internal/rollup/plan_test.go              planner tests
internal/rollup/testdata/bluesky.applied.json   golden
internal/rollup/postgres_state.go         stateDDL, State, readState, readAllStates, writeState
internal/rollup/postgres_source.go        checkSource, columns, compareTable
internal/rollup/install.go                Install, InstallReport, RollupInstall
internal/rollup/install_test.go           nextState test
internal/rollup/install_integration_test.go   state, source, compare and install against Postgres
internal/rollup/connect.go                Connect
internal/cli/rollup/install.go            sqlflow rollup install
internal/cli/rollup/rollup.go             register it
internal/cli/rollup/install_test.go       unit tests
internal/cli/rollup/install_integration_test.go  the command against Postgres
docs/coverage/features.yml                cli.rollup_run
docs/superpowers/specs/2026-09-24-rollup-daemon-design.md   three clarifications
```

---

### Task 1: The `store` and `turbostats` blocks in `rollups.yml`

**Files:**
- Modify: `internal/config/rollup.go` (`RollupsConf`, new types, `PostgresDSN`)
- Modify: `internal/config/rollup_check.go` (`Check`)
- Create: `internal/config/rollup_store_test.go`
- Modify: `internal/validate/schemas/rollups.json` (regenerated)
- Modify: `docs/coverage/features.yml`

**Interfaces:**
- Consumes: `config.TurboStats` and its `Check(at []string) []Violation` from `internal/config/turbostats.go`.
- Produces: `config.RollupsConf.Store *RollupStore`, `config.RollupsConf.TurboStats *TurboStats`, `config.RollupStore{Type string; Postgres *RollupPostgres}`, `config.RollupPostgres{DSN string}`, and `func (c *RollupsConf) PostgresDSN() (string, error)`, which returns `errs.CodeConfigInvalid` when the DSN is empty.

- [ ] **Step 1: Declare the feature**

In `docs/coverage/features.yml`, after the `cli.rollup` entry, add:

```yaml
  # The daemon that manages rollup tables: install, run and verify. The
  # integration level joins with the first test against Postgres, and the
  # release level with the image test for `sqlflow rollup run`.
  - id: cli.rollup_run
    description: Installs, backfills, checks and reports on the rollup tables a rollups file declares.
    requires: [unit]
```

- [ ] **Step 2: Write the failing tests**

Create `internal/config/rollup_store_test.go`:

```go
package config

import (
	"strings"
	"testing"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/turbolytics/sql-flow/internal/errs"
	"github.com/zeebo/assert"
)

const postgresStore = "store:\n  type: postgres\n  postgres:\n    dsn: postgres://rollup@db:5432/rollup\n"

func TestCliRollupRun_AStoreWithADSNPasses(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")

	conf := parseRollups(t, postgresStore+validRollups)
	assert.Equal(t, 0, len(conf.Check()))
	dsn, err := conf.PostgresDSN()
	assert.NoError(t, err)
	assert.Equal(t, "postgres://rollup@db:5432/rollup", dsn)
}

func TestCliRollupRun_StoreRulesNameTheKey(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")

	for _, c := range []struct{ name, store, path, says string }{
		{"a store this version does not run", "store:\n  type: clickhouse\n", "store.type", "use postgres"},
		{"postgres without its block", "store:\n  type: postgres\n", "store.postgres", "is required"},
	} {
		t.Run(c.name, func(t *testing.T) {
			v := parseRollups(t, c.store+validRollups).Check()
			assert.Equal(t, 1, len(v))
			assert.Equal(t, errs.CodeConfigRollup, v[0].Code)
			assert.Equal(t, c.path, strings.Join(v[0].Path, "."))
			assert.That(t, strings.Contains(v[0].Message, c.says))
		})
	}
}

// validate renders an unset variable as an empty string and must pass the
// file, so an empty DSN is refused where a command connects instead.
func TestCliRollupRun_AnEmptyDSNValidatesAndCannotConnect(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")

	empty := parseRollups(t, "store:\n  type: postgres\n  postgres:\n    dsn: \"\"\n"+validRollups)
	assert.Equal(t, 0, len(empty.Check()))
	_, err := empty.PostgresDSN()
	assert.Error(t, err)
	assert.Equal(t, errs.CodeConfigInvalid, errs.CodeOf(err))
	assert.That(t, strings.Contains(err.Error(), "store.postgres.dsn"))

	// No store block at all is refused the same way.
	_, err = parseRollups(t, validRollups).PostgresDSN()
	assert.Error(t, err)
}

func TestCliRollupRun_TurboStatsRulesApplyAtTheirKey(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")

	v := parseRollups(t, "turbostats:\n  labels: {id: demo}\n"+validRollups).Check()
	assert.Equal(t, 1, len(v))
	assert.Equal(t, "turbostats.labels", strings.Join(v[0].Path, "."))
}
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `go test -short ./internal/config/ -run TestCliRollupRun`
Expected: FAIL to compile with `conf.PostgresDSN undefined`.

- [ ] **Step 4: Add the types**

In `internal/config/rollup.go`, add `"strings"` and `"github.com/turbolytics/sql-flow/internal/errs"` to the imports, and replace `RollupsConf` with:

```go
// RollupsConf is a whole rollups file. `sqlflow rollup` generates, from each
// rollup, a migration that creates its tables and the triggers that keep them
// current, and the serve datasets that read them. `sqlflow rollup install`
// applies the tables and triggers itself.
type RollupsConf struct {
	// Where the rollup tables live. `sqlflow rollup run`, `install` and
	// `verify` connect to it. ddl, serve, check and test never read it, so a
	// test run cannot reach this database.
	Store *RollupStore `yaml:"store,omitempty"`
	// Where this instance reports itself: the block `sqlflow run` and
	// `sqlflow serve` take.
	TurboStats *TurboStats `yaml:"turbostats,omitempty"`
	// The rollups this file declares.
	Rollups []Rollup `yaml:"rollups"`
}

// RollupStore names the database that holds the rollup tables.
type RollupStore struct {
	// The kind of database. postgres is the only one this version runs.
	Type string `yaml:"type" jsonschema:"enum=postgres"`
	// The Postgres the tables live in. Required when type is postgres.
	Postgres *RollupPostgres `yaml:"postgres,omitempty"`
}

// RollupPostgres is a Postgres store.
type RollupPostgres struct {
	// A libpq connection string or URL. It carries a password, so render it
	// from an environment variable. There is no default: a daemon without a
	// database fails at startup rather than writing somewhere else.
	DSN string `yaml:"dsn"`
}

// PostgresDSN is the store's connection string, or the error that stops a
// command that connects. validate accepts an empty dsn, because an unset
// template variable renders as an empty string there.
func (c *RollupsConf) PostgresDSN() (string, error) {
	if c.Store == nil || c.Store.Postgres == nil || strings.TrimSpace(c.Store.Postgres.DSN) == "" {
		return "", errs.New(errs.CodeConfigInvalid,
			"store.postgres.dsn is empty; run, install and verify connect to the database that holds the rollup tables")
	}
	return c.Store.Postgres.DSN, nil
}
```

- [ ] **Step 5: Add the rules**

In `internal/config/rollup_check.go`, in `Check`, insert this block after the `add` closure and before `if len(c.Rollups) == 0 {`:

```go
	// The store and the reporter come first, as they do in the file, so the
	// first violation an operator reads is the first one they would reach.
	if c.Store != nil {
		switch {
		case c.Store.Type != "postgres":
			add([]string{"store", "type"}, "store.type %q is not a store this version runs; use postgres", c.Store.Type)
		case c.Store.Postgres == nil:
			add([]string{"store", "postgres"}, "store.type is postgres, so store.postgres, which holds the dsn, is required")
		}
	}
	out = append(out, c.TurboStats.Check([]string{"turbostats"})...)
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `go test -short ./internal/config/ -run TestCliRollupRun`
Expected: PASS, 4 tests.

- [ ] **Step 7: Regenerate the schema**

Run: `make schema`
Expected: `regenerated internal/validate/schemas/config.json, serve.json and rollups.json`. `git diff --stat` shows `internal/validate/schemas/rollups.json` only: a `store` and a `turbostats` property.

- [ ] **Step 8: Run the packages that read the schema and the example**

Run: `go test -short ./internal/config/ ./internal/schema/ ./internal/validate/ ./internal/rollup/ ./internal/cli/rollup/`
Expected: PASS. The generator goldens do not change.

Run: `uv run --locked pytest tests/tooling -q`
Expected: PASS, so the registry still parses.

- [ ] **Step 9: Commit**

```bash
git add docs/coverage/features.yml internal/config/rollup.go internal/config/rollup_check.go \
  internal/config/rollup_store_test.go internal/validate/schemas/rollups.json
git commit -m "rollup: a store and turbostats block in rollups.yml

The daemon needs a database and a reporter, and nothing else in the
file says where either is. validate accepts an empty dsn, because an
unset template variable renders empty; the commands that connect refuse
it. If the rules are wrong, validate passes a file that install then
rejects."
```

---

### Task 2: Split the objects out of the migration

`install` applies the same tables, functions and triggers as `ddl`, without the migration's `LOCK TABLE` and one-transaction backfill. This task refactors `postgres.go` so both come from one code path, and adds `edges`, the table list every later task walks.

**Files:**
- Modify: `internal/rollup/postgres.go` (`writePostgresRollup`, `writeTable`; new `checkNames`, `writeObjects`, `tableQuery`, `PostgresObjects`)
- Modify: `internal/rollup/sql.go` (new `yamlPath`, `edge`, `edges`)
- Create: `internal/rollup/objects_test.go`

**Interfaces:**
- Consumes: `Table`, `selectList`, `keyColumns`, `groupBy`, `quote`, `writeTrigger` from `internal/rollup/sql.go` and `postgres.go`.
- Produces:
  - `func PostgresObjects(r config.Rollup) (string, error)`: one rollup's tables, unique indexes, functions and triggers. No lock and no backfill.
  - `func tableQuery(r config.Rollup, set config.RollupDimensionSet, g config.RollupLevel) string`: the `SELECT ... FROM <source> AS f GROUP BY ...` that `CREATE TABLE ... AS` takes column types from.
  - `type edge struct { SetIndex int; Set config.RollupDimensionSet; Grain config.RollupLevel; Table string; From string }` and `func edges(r config.Rollup) []edge`, set by set, each set's grains narrowest first.
  - `func yamlPath(path []string, keys ...string) []string`.

- [ ] **Step 1: Write the failing tests**

Create `internal/rollup/objects_test.go`:

```go
package rollup

import (
	"strings"
	"testing"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

func TestCliRollupRun_PostgresObjectsIsTheMigrationWithoutItsLockAndBackfill(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")

	conf := loadExample(t)
	objects, err := PostgresObjects(conf.Rollups[0])
	assert.NoError(t, err)

	assert.False(t, strings.Contains(objects, "LOCK TABLE"))
	assert.False(t, strings.Contains(objects, "\nINSERT INTO"))
	assert.False(t, strings.Contains(objects, "rollup_backfill = on"))
	assert.Equal(t, 10, strings.Count(objects, "CREATE TABLE IF NOT EXISTS"))
	assert.Equal(t, 10, strings.Count(objects, "CREATE OR REPLACE FUNCTION"))
	assert.Equal(t, 20, strings.Count(objects, "CREATE OR REPLACE TRIGGER"))

	// Byte for byte the migration's own statements, so install and a team
	// that applies `rollup ddl` build the same database.
	ddl, err := PostgresDDL(conf)
	assert.NoError(t, err)
	assert.That(t, strings.Contains(ddl, objects))
}

func TestCliRollupRun_PostgresObjectsRefusesANameTooLong(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")

	r := loadExample(t).Rollups[0]
	r.DimensionSets[1].Name = "posts_total_" + strings.Repeat("x", 40)
	_, err := PostgresObjects(r)
	assert.Error(t, err)
	assert.That(t, strings.Contains(err.Error(), "63"))
}

func TestCliRollupRun_EdgesNameEachTableAndWhatItIsBuiltFrom(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")

	es := edges(loadExample(t).Rollups[0])
	assert.Equal(t, 10, len(es))
	assert.Equal(t, "posts_by_lang_5m", es[0].Table)
	assert.Equal(t, "posts_per_minute_by_lang", es[0].From)
	assert.Equal(t, 0, es[0].SetIndex)

	from := map[string]string{}
	for _, e := range es {
		from[e.Table] = e.From
	}
	assert.Equal(t, "posts_by_lang_5m", from["posts_by_lang_15m"])
	assert.Equal(t, "posts_total_6h", from["posts_total_1d"])
	assert.Equal(t, 1, es[9].SetIndex)
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test -short ./internal/rollup/ -run TestCliRollupRun`
Expected: FAIL to compile with `undefined: PostgresObjects` and `undefined: edges`.

- [ ] **Step 3: Add `yamlPath`, `edge` and `edges`**

In `internal/rollup/sql.go`, add `"slices"` to the imports, and append:

```go
// yamlPath extends a path into the rollups file, the form validate
// resolves to a line.
func yamlPath(path []string, keys ...string) []string {
	return slices.Concat(path, keys)
}

// edge is one rollup table and the table it is built from.
type edge struct {
	// SetIndex is the dimension set's position in the file, for a message's
	// YAML path.
	SetIndex int
	Set      config.RollupDimensionSet
	Grain    config.RollupLevel
	Table    string
	// From is the source table for a grain built from the source grain, and
	// the finer rollup table otherwise.
	From string
}

// edges lists every table of r, set by set, each set's grains narrowest
// first: the order the triggers cascade in.
func edges(r config.Rollup) []edge {
	var out []edge
	ladder := r.Ladder()
	for i, set := range r.DimensionSets {
		for _, g := range ladder {
			from := r.Source.Table
			if g.From != r.Source.Grain {
				from = Table(set, g.From)
			}
			out = append(out, edge{SetIndex: i, Set: set, Grain: g, Table: Table(set, g.Name), From: from})
		}
	}
	return out
}
```

- [ ] **Step 4: Split `writePostgresRollup` and `writeTable`**

In `internal/rollup/postgres.go`, replace `writePostgresRollup` and `writeTable` with:

```go
// PostgresObjects writes one rollup's tables, unique indexes, functions and
// triggers: what `sqlflow rollup install` applies. It holds no lock on the
// source and fills nothing, because install fills in chunks from `run`
// instead of in the migration's one transaction.
func PostgresObjects(r config.Rollup) (string, error) {
	if err := checkNames(r); err != nil {
		return "", err
	}
	var b strings.Builder
	writeObjects(&b, r)
	return b.String(), nil
}

func writePostgresRollup(b *strings.Builder, r config.Rollup) error {
	if err := checkNames(r); err != nil {
		return err
	}

	fmt.Fprintf(b, "\n-- Rollup %s, kept from %s.\n", r.Name, r.Source.Table)
	b.WriteString("-- Blocks the source's writers until commit, so no write lands between the\n" +
		"-- triggers existing and the backfill reading.\n")
	fmt.Fprintf(b, "LOCK TABLE %s IN SHARE ROW EXCLUSIVE MODE;\n", quote(r.Source.Table))

	writeObjects(b, r)

	b.WriteString("\n-- Backfill the grains built from the source. Their inserts fire the triggers,\n" +
		"-- which fill every coarser grain once.\n")
	b.WriteString("SET LOCAL sqlflow.rollup_backfill = on;\n")
	for _, set := range r.DimensionSets {
		for _, g := range r.Ladder() {
			if g.From == r.Source.Grain {
				writeUpsert(b, r, set, g, r.Source.Table, "")
			}
		}
	}
	b.WriteString("SET LOCAL sqlflow.rollup_backfill = off;\n")
	return nil
}

// checkNames refuses a generated name Postgres would truncate. Two long
// names could truncate to one, and the second object would replace the
// first.
func checkNames(r config.Rollup) error {
	for _, set := range r.DimensionSets {
		for _, g := range r.Ladder() {
			name := "sqlflow_rollup_" + Table(set, g.Name) + "_ins"
			if len(name) > maxIdentifier {
				return errs.New(errs.CodeConfigRollup,
					"rollup %s: generated name %s is %d bytes, and Postgres truncates names past %d; shorten dimension set %s",
					r.Name, name, len(name), maxIdentifier, set.Name)
			}
		}
	}
	return nil
}

// writeObjects writes every table first, then every function and trigger, so
// no trigger names a table that does not exist yet.
func writeObjects(b *strings.Builder, r config.Rollup) {
	ladder := r.Ladder()
	for _, set := range r.DimensionSets {
		for _, g := range ladder {
			writeTable(b, r, set, g)
		}
	}
	for _, set := range r.DimensionSets {
		for _, g := range ladder {
			writeTrigger(b, r, set, g)
		}
	}
}

// tableQuery is the query that fills a table of set at grain g from the
// source. CREATE TABLE ... AS takes every column's type from it, so the
// generator never connects to learn them, and install builds the expected
// shape of an existing table the same way.
func tableQuery(r config.Rollup, set config.RollupDimensionSet, g config.RollupLevel) string {
	sourceGrain := config.RollupLevel{Name: g.Name, Width: g.Width, From: r.Source.Grain}
	cols := selectList(r, set, sourceGrain)
	names := append(keyColumns(r, set), set.MeasureNames()...)
	for i := range cols {
		cols[i] += " AS " + quote(names[i])
	}
	return fmt.Sprintf("SELECT %s\nFROM %s AS f\nGROUP BY %s",
		strings.Join(cols, ",\n       "), quote(r.Source.Table), groupBy(len(set.Dimensions)+1))
}

// writeTable creates a table from the query that fills it, so every column
// takes its type from the source without the generator connecting.
func writeTable(b *strings.Builder, r config.Rollup, set config.RollupDimensionSet, g config.RollupLevel) {
	table := Table(set, g.Name)
	fmt.Fprintf(b, "\nCREATE TABLE IF NOT EXISTS %s AS\n%s\nWITH NO DATA;\n", quote(table), tableQuery(r, set, g))
	fmt.Fprintf(b, "CREATE UNIQUE INDEX IF NOT EXISTS %s\n  ON %s (%s) NULLS NOT DISTINCT;\n",
		quote(table+"_key"), quote(table), quoteList(keyColumns(r, set)))
}
```

- [ ] **Step 5: Run the new tests and every generator golden**

Run: `go test -short ./internal/rollup/ ./internal/cli/rollup/`
Expected: PASS. `TestCliRollup_PostgresDDLMatchesTheGolden`, `TestCliRollup_MetricsDDLMatchesTheGolden` and `TestCliRollup_DDLPrintsTheMigration` pass unchanged, so the migration is byte for byte what it was. `git status` shows no change under `internal/rollup/testdata/`.

- [ ] **Step 6: Commit**

```bash
git add internal/rollup/postgres.go internal/rollup/sql.go internal/rollup/objects_test.go
git commit -m "rollup: the tables, functions and triggers apart from the migration

install applies what ddl generates, without the migration's LOCK TABLE
and one-transaction backfill, which hold the pipeline's writes for the
whole history. One code path writes both, and the goldens prove the
migration did not change. If it did, a team that applies ddl would build
a different database from one that runs install."
```

---

### Task 3: The planner

**Files:**
- Modify: `internal/errs/registry.go` (`CodeConfigRollupChange`)
- Modify: `internal/errs/testdata/codes.golden` (regenerated)
- Create: `internal/rollup/applied.go`
- Create: `internal/rollup/plan.go`
- Create: `internal/rollup/plan_test.go`
- Create: `internal/rollup/testdata/bluesky.applied.json` (golden)
- Modify: `docs/superpowers/specs/2026-09-24-rollup-daemon-design.md` (one row)

**Interfaces:**
- Consumes: `edges`, `yamlPath` (Task 2); `config.Violation{Code errs.Code; Path []string; Message string}`.
- Produces:
  - `errs.CodeConfigRollupChange` = `"user.config.rollup_change"`.
  - `type Applied struct { Source AppliedSource; Grains map[string]string; DimensionSets map[string]AppliedSet }`, with `AppliedSource{Table, TimeColumn, Grain string; Dimensions []string}`, `AppliedSet{Dimensions []string; Measures map[string]AppliedMeasure}` and `AppliedMeasure{Type, Column, Numeric string}`. JSON tags are snake_case, and the JSON is stored.
  - `func AppliedFrom(r config.Rollup) Applied`.
  - `type Plan struct { Backfill, Retain, Restore []string }`.
  - `func PlanChange(r config.Rollup, path []string, prev *Applied, retained []string, missing map[string]bool) (Plan, []config.Violation)`.
  - `func sortedKeys[V any](ms ...map[string]V) []string`.

- [ ] **Step 1: Add the code**

In `internal/errs/registry.go`, after `CodeConfigRollupDrift` in the `const` block, add:

```go
	// A rollups file changed in a way that would corrupt the rows its tables
	// already hold: a measure, a dimension set's dimensions, a grain's from,
	// or the source. Also a table that exists with other columns than the
	// file declares, because another declaration built it.
	CodeConfigRollupChange Code = "user.config.rollup_change"
```

In the `registry` map, after the `CodeConfigRollupDrift` entry, add:

```go
	CodeConfigRollupChange: {
		CodeConfigRollupChange,
		"A rollups file changed in a way that would corrupt the rows its tables already hold, or a table exists with other columns than the file declares.",
		"Declare a new dimension set or rollup for the new shape, and drop the old tables by hand once nothing reads them. The message names the change.",
	},
```

Run: `UPDATE_GOLDEN=1 go test ./internal/errs -run TestErrorTaxonomy_RegistryIsAppendOnly && go test ./internal/errs`
Expected: PASS. `git diff internal/errs/testdata/codes.golden` adds `user.config.rollup_change` and removes nothing.

- [ ] **Step 2: Write the failing tests**

Create `internal/rollup/plan_test.go`:

```go
package rollup

import (
	"encoding/json"
	"slices"
	"strings"
	"testing"

	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/turbolytics/sql-flow/internal/errs"
	"github.com/zeebo/assert"
)

// pathInFile is the example's only rollup.
var pathInFile = []string{"rollups", "0"}

func exampleRollup(t *testing.T) config.Rollup {
	t.Helper()
	return loadExample(t).Rollups[0]
}

func appliedOf(r config.Rollup) *Applied {
	a := AppliedFrom(r)
	return &a
}

func setMeasure(r *config.Rollup, set int, name string, m config.RollupMeasure) {
	r.DimensionSets[set].Measures[name] = m
}

// The state table stores this JSON and every later version reads it, so a
// change to its shape must be deliberate.
func TestCliRollupRun_TheAppliedDeclarationMatchesTheGolden(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")

	b, err := json.MarshalIndent(AppliedFrom(exampleRollup(t)), "", "  ")
	assert.NoError(t, err)
	golden(t, "testdata/bluesky.applied.json", string(b)+"\n")
}

func TestCliRollupRun_AFirstInstallFillsTheTablesBuiltFromTheSource(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")

	plan, v := PlanChange(exampleRollup(t), pathInFile, nil, nil, nil)
	assert.Equal(t, 0, len(v))
	assert.DeepEqual(t, []string{"posts_by_lang_5m", "posts_total_5m"}, plan.Backfill)
	assert.Equal(t, 0, len(plan.Retain))
	assert.Equal(t, 0, len(plan.Restore))
}

// A deploy must not stop on an edit that changes no stored row.
func TestCliRollupRun_AnEditThatChangesNoRowIsNoChange(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")

	metrics, err := config.LoadRollups(metricsPath)
	assert.NoError(t, err)
	r := metrics.Rollups[0]
	prev := appliedOf(r)

	// A sum's default numeric written out, and dimensions in another order.
	setMeasure(&r, 0, "value_count", config.RollupMeasure{Type: "sum", Column: "value_count", Numeric: "integer"})
	slices.Reverse(r.DimensionSets[0].Dimensions)
	slices.Reverse(r.Source.Dimensions)

	plan, v := PlanChange(r, pathInFile, prev, nil, map[string]bool{})
	assert.Equal(t, 0, len(v))
	assert.DeepEqual(t, Plan{}, plan)
}

func TestCliRollupRun_AnAddedGrainFillsFromTheGrainBelow(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")

	r := exampleRollup(t)
	prev := appliedOf(r)
	r.Grains["7d"] = config.RollupGrain{From: "1d"}

	plan, v := PlanChange(r, pathInFile, prev, nil, map[string]bool{"posts_by_lang_7d": true, "posts_total_7d": true})
	assert.Equal(t, 0, len(v))
	assert.DeepEqual(t, []string{"posts_by_lang_7d", "posts_total_7d"}, plan.Backfill)
}

// The second new grain fills when the first does: the first one's upserts
// fire its trigger.
func TestCliRollupRun_OfTwoNewGrainsOnlyTheFirstFills(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")

	r := exampleRollup(t)
	prev := appliedOf(r)
	r.Grains["2d"] = config.RollupGrain{From: "1d"}
	r.Grains["4d"] = config.RollupGrain{From: "2d"}
	missing := map[string]bool{
		"posts_by_lang_2d": true, "posts_total_2d": true,
		"posts_by_lang_4d": true, "posts_total_4d": true,
	}

	plan, v := PlanChange(r, pathInFile, prev, nil, missing)
	assert.Equal(t, 0, len(v))
	assert.DeepEqual(t, []string{"posts_by_lang_2d", "posts_total_2d"}, plan.Backfill)
}

func TestCliRollupRun_ANewSetFillsOnlyItsTableBuiltFromTheSource(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")

	r := exampleRollup(t)
	prev := appliedOf(r)
	r.DimensionSets = append(r.DimensionSets, config.RollupDimensionSet{
		Name: "posts_peak", Dimensions: []string{"lang"},
		Measures: map[string]config.RollupMeasure{"peak": {Type: "max", Column: "posts"}},
	})
	missing := map[string]bool{}
	for _, g := range []string{"5m", "15m", "1h", "6h", "1d"} {
		missing["posts_peak_"+g] = true
	}

	plan, v := PlanChange(r, pathInFile, prev, nil, missing)
	assert.Equal(t, 0, len(v))
	assert.DeepEqual(t, []string{"posts_peak_5m"}, plan.Backfill)
}

func TestCliRollupRun_ARemovedGrainIsRetainedAndRestoredWhenDeclaredAgain(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")

	full := exampleRollup(t)
	without := exampleRollup(t)
	delete(without.Grains, "1d")

	plan, v := PlanChange(without, pathInFile, appliedOf(full), nil, map[string]bool{})
	assert.Equal(t, 0, len(v))
	assert.DeepEqual(t, []string{"posts_by_lang_1d", "posts_total_1d"}, plan.Retain)
	assert.Equal(t, 0, len(plan.Backfill))

	// Its triggers never stopped, so declaring it again fills nothing.
	plan, v = PlanChange(full, pathInFile, appliedOf(without), plan.Retain, map[string]bool{})
	assert.Equal(t, 0, len(v))
	assert.DeepEqual(t, []string{"posts_by_lang_1d", "posts_total_1d"}, plan.Restore)
	assert.Equal(t, 0, len(plan.Backfill))
	assert.Equal(t, 0, len(plan.Retain))
}

// A table dropped by hand is created again holding nothing, so it fills.
func TestCliRollupRun_AMissingTableFills(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")

	r := exampleRollup(t)
	plan, v := PlanChange(r, pathInFile, appliedOf(r), nil, map[string]bool{"posts_by_lang_1h": true})
	assert.Equal(t, 0, len(v))
	assert.DeepEqual(t, []string{"posts_by_lang_1h"}, plan.Backfill)
}

func TestCliRollupRun_ChangesThatCorruptStoredRowsAreRefused(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")

	for _, c := range []struct {
		name string
		edit func(r *config.Rollup)
		path string
	}{
		{"a measure's type", func(r *config.Rollup) {
			setMeasure(r, 0, "posts", config.RollupMeasure{Type: "max", Column: "posts"})
		}, "rollups.0.dimension_sets.0.measures.posts"},
		{"a sum's numeric", func(r *config.Rollup) {
			setMeasure(r, 0, "posts", config.RollupMeasure{Type: "sum", Column: "posts", Numeric: "double"})
		}, "rollups.0.dimension_sets.0.measures.posts"},
		{"a measure's column", func(r *config.Rollup) {
			setMeasure(r, 0, "posts", config.RollupMeasure{Type: "sum", Column: "likes"})
		}, "rollups.0.dimension_sets.0.measures.posts"},
		{"an added measure", func(r *config.Rollup) {
			setMeasure(r, 1, "peak", config.RollupMeasure{Type: "max", Column: "posts"})
		}, "rollups.0.dimension_sets.1.measures.peak"},
		{"a removed measure", func(r *config.Rollup) {
			delete(r.DimensionSets[1].Measures, "minutes")
		}, "rollups.0.dimension_sets.1.measures"},
		{"a set's dimensions", func(r *config.Rollup) {
			r.DimensionSets[0].Dimensions = nil
		}, "rollups.0.dimension_sets.0.dimensions"},
		{"a grain's from", func(r *config.Rollup) {
			r.Grains["1h"] = config.RollupGrain{From: "5m"}
		}, "rollups.0.grains.1h.from"},
		{"the source", func(r *config.Rollup) {
			r.Source.TimeColumn = "minute"
		}, "rollups.0.source"},
	} {
		t.Run(c.name, func(t *testing.T) {
			r := exampleRollup(t)
			prev := appliedOf(r)
			c.edit(&r)

			plan, v := PlanChange(r, pathInFile, prev, nil, map[string]bool{})
			assert.Equal(t, 1, len(v))
			assert.Equal(t, errs.CodeConfigRollupChange, v[0].Code)
			assert.Equal(t, c.path, strings.Join(v[0].Path, "."))
			assert.DeepEqual(t, Plan{}, plan)
		})
	}
}
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `go test -short ./internal/rollup/ -run TestCliRollupRun`
Expected: FAIL to compile with `undefined: AppliedFrom` and `undefined: PlanChange`.

- [ ] **Step 4: Write `applied.go`**

Create `internal/rollup/applied.go`:

```go
package rollup

import (
	"slices"

	"github.com/turbolytics/sql-flow/internal/config"
)

// Applied is the part of a rollup its tables are built from: the source, the
// grains, and the dimension sets. The state table stores it as JSON, and
// every later version reads it back, so its field names are a contract.
//
// The serve block is left out. It changes what serve reads, never what a
// table holds.
type Applied struct {
	Source AppliedSource `json:"source"`
	// Each grain and the grain it is built from.
	Grains        map[string]string     `json:"grains"`
	DimensionSets map[string]AppliedSet `json:"dimension_sets"`
}

// AppliedSource is the source as applied.
type AppliedSource struct {
	Table      string   `json:"table"`
	TimeColumn string   `json:"time_column"`
	Grain      string   `json:"grain"`
	Dimensions []string `json:"dimensions"`
}

// AppliedSet is one dimension set as applied.
type AppliedSet struct {
	Dimensions []string                  `json:"dimensions"`
	Measures   map[string]AppliedMeasure `json:"measures"`
}

// AppliedMeasure is one measure as applied.
type AppliedMeasure struct {
	Type    string `json:"type"`
	Column  string `json:"column,omitempty"`
	Numeric string `json:"numeric,omitempty"`
}

// AppliedFrom normalizes r, so an edit that changes no stored row compares
// equal: key order, dimension order, and a sum's numeric written out as its
// default, integer. A reorder changes neither a key nor a value, since the
// upserts name their columns and ON CONFLICT matches an index by its set of
// columns.
func AppliedFrom(r config.Rollup) Applied {
	out := Applied{
		Source: AppliedSource{
			Table: r.Source.Table, TimeColumn: r.Source.TimeColumn, Grain: r.Source.Grain,
			Dimensions: sortedCopy(r.Source.Dimensions),
		},
		Grains:        map[string]string{},
		DimensionSets: map[string]AppliedSet{},
	}
	for name, g := range r.Grains {
		out.Grains[name] = g.From
	}
	for _, set := range r.DimensionSets {
		measures := map[string]AppliedMeasure{}
		for name, m := range set.Measures {
			numeric := m.Numeric
			if m.Type == "sum" && numeric == "" {
				numeric = "integer"
			}
			measures[name] = AppliedMeasure{Type: m.Type, Column: m.Column, Numeric: numeric}
		}
		out.DimensionSets[set.Name] = AppliedSet{Dimensions: sortedCopy(set.Dimensions), Measures: measures}
	}
	return out
}

// sortedCopy is never nil, so an empty list stores as [] and not null.
func sortedCopy(in []string) []string {
	out := append([]string{}, in...)
	slices.Sort(out)
	return out
}
```

- [ ] **Step 5: Write `plan.go`**

Create `internal/rollup/plan.go`:

```go
package rollup

import (
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"

	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/errs"
)

// Plan is what one install does to one rollup's tables beyond creating the
// missing ones and replacing every function and trigger.
type Plan struct {
	// Backfill lists the tables to fill from the table each is built from.
	// `sqlflow rollup run` fills them.
	Backfill []string
	// Retain lists the tables of a grain or dimension set the file no longer
	// declares. They keep their rows and their triggers, so a rollback to
	// the previous file loses nothing.
	Retain []string
	// Restore lists retained tables the file declares again. Their triggers
	// never stopped, so they need no backfill.
	Restore []string
}

// changeRemedy ends every refusal: the old tables keep what they hold, and
// the new shape gets its own.
const changeRemedy = "declare a new rollup or dimension set for the new shape"

// PlanChange compares a rollup as last applied, prev, with the rollup as
// declared, r, at path in the file.
//
// prev is nil when the database has no state row for the rollup: a first
// install, or tables a migration created, which the install adopts. Nothing
// says those tables are complete, so every table built from the source is
// filled, and its upserts fire the triggers that fill the rest.
//
// missing holds each declared table that does not exist yet, and retained
// the state row's retained tables. A change that would corrupt stored rows
// is a violation, and the plan is empty.
func PlanChange(r config.Rollup, path []string, prev *Applied, retained []string, missing map[string]bool) (Plan, []config.Violation) {
	var plan Plan
	es := edges(r)
	if prev == nil {
		for _, e := range es {
			if e.Grain.From == r.Source.Grain {
				plan.Backfill = append(plan.Backfill, e.Table)
			}
		}
		return plan, nil
	}
	if v := changes(r, path, *prev); len(v) > 0 {
		return Plan{}, v
	}

	kept := map[string]bool{}
	for _, t := range retained {
		kept[t] = true
	}
	declared := map[string]bool{}
	for _, e := range es {
		declared[e.Table] = true
		if kept[e.Table] {
			plan.Restore = append(plan.Restore, e.Table)
		}
		// A table whose from is also new fills when its from fills: the
		// from's upserts fire this table's trigger.
		if missing[e.Table] && !missing[e.From] {
			plan.Backfill = append(plan.Backfill, e.Table)
		}
	}
	for _, set := range sortedKeys(prev.DimensionSets) {
		for _, g := range sortedKeys(prev.Grains) {
			if t := set + "_" + g; !declared[t] && !kept[t] {
				plan.Retain = append(plan.Retain, t)
			}
		}
	}
	return plan, nil
}

// changes returns every difference between prev and r that stored rows
// cannot absorb. An added or removed grain or dimension set is not one: it
// adds tables or leaves them in place.
func changes(r config.Rollup, path []string, prev Applied) []config.Violation {
	next := AppliedFrom(r)
	var out []config.Violation
	add := func(p []string, format string, args ...any) {
		out = append(out, config.Violation{Code: errs.CodeConfigRollupChange, Path: p, Message: fmt.Sprintf(format, args...)})
	}

	if !sameSource(prev.Source, next.Source) {
		add(yamlPath(path, "source"), "rollup %s: the source changed, and every stored row was built from the old one; %s",
			r.Name, changeRemedy)
	}
	for _, g := range sortedKeys(next.Grains) {
		if from, ok := prev.Grains[g]; ok && from != next.Grains[g] {
			add(yamlPath(path, "grains", g, "from"), "rollup %s grain %s: from changed from %s to %s, and the grain's triggers read the old one; %s",
				r.Name, g, from, next.Grains[g], changeRemedy)
		}
	}
	for i, set := range r.DimensionSets {
		old, ok := prev.DimensionSets[set.Name]
		if !ok {
			continue
		}
		cur := next.DimensionSets[set.Name]
		spath := yamlPath(path, "dimension_sets", strconv.Itoa(i))
		if !slices.Equal(old.Dimensions, cur.Dimensions) {
			add(yamlPath(spath, "dimensions"), "rollup %s dimension set %s: dimensions changed from [%s] to [%s], which changes every stored row's key; %s",
				r.Name, set.Name, strings.Join(old.Dimensions, ", "), strings.Join(cur.Dimensions, ", "), changeRemedy)
		}
		for _, m := range sortedKeys(old.Measures, cur.Measures) {
			was, had := old.Measures[m]
			now, has := cur.Measures[m]
			switch {
			case had && !has:
				add(yamlPath(spath, "measures"), "rollup %s dimension set %s: measure %s was removed, and its column would go stale in every stored row; %s",
					r.Name, set.Name, m, changeRemedy)
			case !had && has:
				add(yamlPath(spath, "measures", m), "rollup %s dimension set %s: measure %s was added, and the stored tables have no column for it; %s",
					r.Name, set.Name, m, changeRemedy)
			case was != now:
				add(yamlPath(spath, "measures", m), "rollup %s dimension set %s: measure %s changed from %s to %s, and every stored row was merged the old way; %s",
					r.Name, set.Name, m, describe(was), describe(now), changeRemedy)
			}
		}
	}
	return out
}

func sameSource(a, b AppliedSource) bool {
	return a.Table == b.Table && a.TimeColumn == b.TimeColumn && a.Grain == b.Grain &&
		slices.Equal(a.Dimensions, b.Dimensions)
}

// describe writes a measure the way a message names it: sum(posts) as integer.
func describe(m AppliedMeasure) string {
	s := m.Type
	if m.Column != "" {
		s += "(" + m.Column + ")"
	}
	if m.Numeric != "" {
		s += " as " + m.Numeric
	}
	return s
}

// sortedKeys is the union of the maps' keys, sorted, so a plan and a message
// come out the same on every run.
func sortedKeys[V any](ms ...map[string]V) []string {
	seen := map[string]bool{}
	for _, m := range ms {
		for k := range m {
			seen[k] = true
		}
	}
	return slices.Sorted(maps.Keys(seen))
}
```

- [ ] **Step 6: Write the golden and check it**

Run: `UPDATE_GOLDEN=1 go test -short ./internal/rollup/ -run TestCliRollupRun_TheAppliedDeclarationMatchesTheGolden`
Then read `internal/rollup/testdata/bluesky.applied.json`. It must be exactly:

```json
{
  "source": {
    "table": "posts_per_minute_by_lang",
    "time_column": "bucket",
    "grain": "1m",
    "dimensions": [
      "lang"
    ]
  },
  "grains": {
    "15m": "5m",
    "1d": "6h",
    "1h": "15m",
    "5m": "1m",
    "6h": "1h"
  },
  "dimension_sets": {
    "posts_by_lang": {
      "dimensions": [
        "lang"
      ],
      "measures": {
        "posts": {
          "type": "sum",
          "column": "posts",
          "numeric": "integer"
        }
      }
    },
    "posts_total": {
      "dimensions": [],
      "measures": {
        "minutes": {
          "type": "count_buckets"
        },
        "posts": {
          "type": "sum",
          "column": "posts",
          "numeric": "integer"
        }
      }
    }
  }
}
```

- [ ] **Step 7: Run the tests to verify they pass**

Run: `go test -short ./internal/rollup/ -run TestCliRollupRun -v`
Expected: PASS, with all 8 refusal subtests.

- [ ] **Step 8: Bring the spec in line**

In `docs/superpowers/specs/2026-09-24-rollup-daemon-design.md`, replace:

```
| A measure added to an existing dimension set | Stop with `user.config.rollup_change`. Declare a new dimension set instead. |
```

with:

```
| A measure added to or removed from an existing dimension set | Stop with `user.config.rollup_change`. A removed measure's column would go stale in every stored row. Declare a new dimension set instead. |
```

- [ ] **Step 9: Commit**

```bash
git add internal/errs/registry.go internal/errs/testdata/codes.golden internal/rollup/applied.go \
  internal/rollup/plan.go internal/rollup/plan_test.go internal/rollup/testdata/bluesky.applied.json \
  docs/superpowers/specs/2026-09-24-rollup-daemon-design.md
git commit -m "rollup: plan an install from the declaration last applied

A changed measure, dimension list, grain from or source would leave
stored rows built one way beside rows built another, so the planner
refuses it with user.config.rollup_change. A removed grain or set keeps
its tables, so a rollback loses nothing. Reordered keys and a default
written out compare equal. If the normalization is wrong, a cosmetic
edit stops a deploy."
```

---

### Task 4: The state table and the database checks

**Files:**
- Create: `internal/rollup/postgres_state.go`
- Create: `internal/rollup/postgres_source.go`
- Create: `internal/rollup/install_integration_test.go`

**Interfaces:**
- Consumes: `Applied`, `AppliedFrom`, `sortedKeys` (Task 3); `edge`, `edges`, `tableQuery`, `yamlPath`, `PostgresObjects` (Task 2); test helpers `startRollupPostgres`, `execSQL`, `count`, `at` from `postgres_integration_test.go`, `exampleRollup` from `plan_test.go`.
- Produces:
  - `const stateDDL string`.
  - `type State struct { Rollup string; Declaration Applied; Version string; AppliedAt time.Time; Backfill map[string]*time.Time; Retained []string }`.
  - `type querier interface`, which `*pgx.Conn` and `pgx.Tx` satisfy.
  - `func readState(ctx, q querier, rollup string) (*State, error)`, which returns nil and no error when there is no row.
  - `func readAllStates(ctx, q querier) ([]State, error)`.
  - `func writeState(ctx, q querier, s State) error`.
  - `func checkSource(ctx, q querier, r config.Rollup, path []string) ([]config.Violation, error)`.
  - `func columns(ctx, q querier, relation string) (map[string]string, error)`, where `relation` is a quoted, optionally schema-qualified name.
  - `func compareTable(ctx, q querier, r config.Rollup, e edge, relation string, path []string) ([]config.Violation, error)`, which must run inside a transaction.

- [ ] **Step 1: Write the failing integration tests**

Create `internal/rollup/install_integration_test.go`:

```go
package rollup

// sqlflow rollup install against a real Postgres: the state row, the checks
// on the source and on existing tables, and the install itself.

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/turbolytics/sql-flow/internal/errs"
	"github.com/zeebo/assert"
)

func TestIntegrationRollupRun_TheStateRowRoundTrips(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	srv := startRollupPostgres(t)
	ctx := context.Background()
	execSQL(t, srv.conn, stateDDL)

	none, err := readState(ctx, srv.conn, "posts")
	assert.NoError(t, err)
	assert.That(t, none == nil)

	day := at("2026-09-20T00:00:00Z")
	want := State{
		Rollup: "posts", Declaration: AppliedFrom(exampleRollup(t)), Version: "test",
		Backfill: map[string]*time.Time{"posts_by_lang_5m": nil, "posts_total_5m": &day},
		Retained: []string{"posts_by_lang_7d"},
	}
	assert.NoError(t, writeState(ctx, srv.conn, want))

	got, err := readState(ctx, srv.conn, "posts")
	assert.NoError(t, err)
	assert.DeepEqual(t, want.Declaration, got.Declaration)
	assert.DeepEqual(t, want.Retained, got.Retained)
	assert.Equal(t, "test", got.Version)
	pending, ok := got.Backfill["posts_by_lang_5m"]
	assert.True(t, ok)
	assert.That(t, pending == nil)
	assert.That(t, got.Backfill["posts_total_5m"].Equal(day))

	all, err := readAllStates(ctx, srv.conn)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(all))
}

func TestIntegrationRollupRun_SourceChecksNameWhatTheTriggersNeed(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	srv := startRollupPostgres(t)
	check := func(r config.Rollup) []config.Violation {
		t.Helper()
		v, err := checkSource(context.Background(), srv.conn, r, pathInFile)
		assert.NoError(t, err)
		return v
	}

	assert.Equal(t, 0, len(check(exampleRollup(t))))

	gone := exampleRollup(t)
	gone.Source.Table = "posts_nowhere"
	v := check(gone)
	assert.Equal(t, 1, len(v))
	assert.Equal(t, errs.CodeConfigRollup, v[0].Code)
	assert.That(t, strings.Contains(v[0].Message, "does not exist"))

	unread := exampleRollup(t)
	unread.DimensionSets[0].Measures["likes"] = config.RollupMeasure{Type: "sum", Column: "likes"}
	v = check(unread)
	assert.Equal(t, 1, len(v))
	assert.That(t, strings.Contains(v[0].Message, "has no column likes"))

	// A key led by lang serves no range on bucket.
	execSQL(t, srv.conn, "ALTER TABLE posts_per_minute_by_lang DROP CONSTRAINT posts_per_minute_by_lang_pkey")
	execSQL(t, srv.conn, "ALTER TABLE posts_per_minute_by_lang ADD PRIMARY KEY (lang, bucket)")
	v = check(exampleRollup(t))
	assert.Equal(t, 1, len(v))
	assert.Equal(t, "rollups.0.source.time_column", strings.Join(v[0].Path, "."))
	assert.That(t, strings.Contains(v[0].Message, `CREATE INDEX ON "posts_per_minute_by_lang" ("bucket")`))
}

func TestIntegrationRollupRun_CompareTableNamesEachColumnThatDiffers(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	srv := startRollupPostgres(t)
	ctx := context.Background()
	r := exampleRollup(t)
	e := edges(r)[0]
	compare := func() []config.Violation {
		t.Helper()
		tx, err := srv.conn.Begin(ctx)
		assert.NoError(t, err)
		defer func() { _ = tx.Rollback(ctx) }()
		v, err := compareTable(ctx, tx, r, e, quote(e.Table), pathInFile)
		assert.NoError(t, err)
		return v
	}

	execSQL(t, srv.conn, "CREATE TABLE posts_by_lang_5m (bucket timestamptz, lang text, posts text, extra int)")
	v := compare()
	assert.Equal(t, 1, len(v))
	assert.Equal(t, errs.CodeConfigRollupChange, v[0].Code)
	assert.Equal(t, "rollups.0.dimension_sets.0", strings.Join(v[0].Path, "."))
	assert.That(t, strings.Contains(v[0].Message, "posts is text, not bigint"))
	assert.That(t, strings.Contains(v[0].Message, "extra is not declared"))

	execSQL(t, srv.conn, "DROP TABLE posts_by_lang_5m")
	objects, err := PostgresObjects(r)
	assert.NoError(t, err)
	execSQL(t, srv.conn, objects)
	assert.Equal(t, 0, len(compare()))
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test -run '^TestIntegrationRollupRun' ./internal/rollup/`
Expected: FAIL to compile with `undefined: stateDDL`, `undefined: checkSource` and `undefined: compareTable`.

- [ ] **Step 3: Write `postgres_state.go`**

Create `internal/rollup/postgres_state.go`:

```go
package rollup

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// stateDDL records what each install applied, one row per rollup. The name
// is fixed, so every version of install and run finds it.
const stateDDL = `CREATE TABLE IF NOT EXISTS sqlflow_rollup_state (
  rollup          TEXT        PRIMARY KEY,
  -- The rollup as last applied, without its serve block. See Applied.
  declaration     JSONB       NOT NULL,
  sqlflow_version TEXT        NOT NULL,
  applied_at      TIMESTAMPTZ NOT NULL,
  -- Each table still being filled, and the start of the oldest source day
  -- filled so far: null before the first chunk.
  backfill        JSONB       NOT NULL DEFAULT '{}',
  -- Tables a later declaration removed. Their triggers still run.
  retained        JSONB       NOT NULL DEFAULT '[]'
)`

const stateColumns = "rollup, declaration, sqlflow_version, applied_at, backfill, retained"

// State is one rollup's row of sqlflow_rollup_state.
type State struct {
	Rollup      string
	Declaration Applied
	Version     string
	AppliedAt   time.Time
	// Backfill maps each table still being filled to the start of the oldest
	// source day filled so far, and to nil before the first chunk.
	Backfill map[string]*time.Time
	Retained []string
}

// querier is a connection or a transaction.
type querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// readState returns the rollup's row, or nil when it has none.
func readState(ctx context.Context, q querier, rollup string) (*State, error) {
	s, err := scanState(q.QueryRow(ctx, "SELECT "+stateColumns+" FROM sqlflow_rollup_state WHERE rollup = $1", rollup))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	return s, err
}

// readAllStates returns every row, in rollup order.
func readAllStates(ctx context.Context, q querier) ([]State, error) {
	rows, err := q.Query(ctx, "SELECT "+stateColumns+" FROM sqlflow_rollup_state ORDER BY rollup")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []State
	for rows.Next() {
		s, err := scanState(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *s)
	}
	return out, rows.Err()
}

func scanState(row pgx.Row) (*State, error) {
	var s State
	var decl, backfill, retained []byte
	if err := row.Scan(&s.Rollup, &decl, &s.Version, &s.AppliedAt, &backfill, &retained); err != nil {
		return nil, err
	}
	for _, f := range []struct {
		raw []byte
		to  any
	}{{decl, &s.Declaration}, {backfill, &s.Backfill}, {retained, &s.Retained}} {
		if err := json.Unmarshal(f.raw, f.to); err != nil {
			return nil, fmt.Errorf("sqlflow_rollup_state row %s: %w", s.Rollup, err)
		}
	}
	return &s, nil
}

// writeState replaces the rollup's row. applied_at is the database's clock,
// so rows written from two hosts compare.
func writeState(ctx context.Context, q querier, s State) error {
	if s.Backfill == nil {
		s.Backfill = map[string]*time.Time{}
	}
	if s.Retained == nil {
		s.Retained = []string{}
	}
	decl, err := json.Marshal(s.Declaration)
	if err != nil {
		return err
	}
	backfill, err := json.Marshal(s.Backfill)
	if err != nil {
		return err
	}
	retained, err := json.Marshal(s.Retained)
	if err != nil {
		return err
	}
	_, err = q.Exec(ctx, "INSERT INTO sqlflow_rollup_state ("+stateColumns+`)
VALUES ($1, $2, $3, now(), $4, $5)
ON CONFLICT (rollup) DO UPDATE SET
  declaration = excluded.declaration, sqlflow_version = excluded.sqlflow_version,
  applied_at = excluded.applied_at, backfill = excluded.backfill, retained = excluded.retained`,
		s.Rollup, string(decl), s.Version, string(backfill), string(retained))
	return err
}
```

- [ ] **Step 4: Write `postgres_source.go`**

Create `internal/rollup/postgres_source.go`:

```go
package rollup

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/errs"
)

// checkSource reports what the triggers need from r's source and cannot
// find: the table, a column the file reads, or an index that leads with the
// time column. The trigger on the source reads it by a time range on every
// write, and without that index each write scans the whole history.
func checkSource(ctx context.Context, q querier, r config.Rollup, path []string) ([]config.Violation, error) {
	var out []config.Violation
	add := func(key, format string, args ...any) {
		out = append(out, config.Violation{
			Code: errs.CodeConfigRollup, Path: yamlPath(path, "source", key), Message: fmt.Sprintf(format, args...),
		})
	}

	src := quote(r.Source.Table)
	cols, err := columns(ctx, q, src)
	if err != nil {
		return nil, err
	}
	if len(cols) == 0 {
		add("table", "rollup %s: source table %s does not exist on the connection's search_path; the pipeline's migration creates it",
			r.Name, r.Source.Table)
		return out, nil
	}
	for _, c := range sourceColumns(r) {
		if _, ok := cols[c]; !ok {
			add("table", "rollup %s: source table %s has no column %s, which the rollups file reads", r.Name, r.Source.Table, c)
		}
	}

	var indexed bool
	if err := q.QueryRow(ctx, `SELECT EXISTS (
  SELECT 1 FROM pg_index i
  JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = i.indkey[0]
  WHERE i.indrelid = to_regclass($1) AND a.attname = $2)`, src, r.Source.TimeColumn).Scan(&indexed); err != nil {
		return nil, err
	}
	if !indexed {
		add("time_column", "rollup %s: no index on %s leads with %s, so the trigger on it would scan the whole table on every write; run CREATE INDEX ON %s (%s)",
			r.Name, r.Source.Table, r.Source.TimeColumn, src, quote(r.Source.TimeColumn))
	}
	return out, nil
}

// sourceColumns is every source column the triggers read, sorted.
func sourceColumns(r config.Rollup) []string {
	seen := map[string]bool{r.Source.TimeColumn: true}
	for _, d := range r.Source.Dimensions {
		seen[d] = true
	}
	for _, set := range r.DimensionSets {
		for _, m := range set.Measures {
			if m.Column != "" {
				seen[m.Column] = true
			}
		}
	}
	return sortedKeys(seen)
}

// columns maps each column of relation to its type as Postgres prints it.
// relation is a quoted name, optionally schema-qualified. A relation that
// does not exist has no columns.
func columns(ctx context.Context, q querier, relation string) (map[string]string, error) {
	rows, err := q.Query(ctx, `SELECT a.attname, format_type(a.atttypid, a.atttypmod)
FROM pg_attribute a
WHERE a.attrelid = to_regclass($1) AND a.attnum > 0 AND NOT a.attisdropped`, relation)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]string{}
	for rows.Next() {
		var name, typ string
		if err := rows.Scan(&name, &typ); err != nil {
			return nil, err
		}
		out[name] = typ
	}
	return out, rows.Err()
}

// compareTable reports a declared table that exists with other columns than
// the file declares for it: another declaration built it. The expected shape
// comes the way ddl gets it, from the generator's query WITH NO DATA, into a
// temporary table that the transaction drops, so it must run inside one.
func compareTable(ctx context.Context, q querier, r config.Rollup, e edge, relation string, path []string) ([]config.Violation, error) {
	tmp := quote("sqlflow_expect_" + e.Table)
	if _, err := q.Exec(ctx, "CREATE TEMP TABLE "+tmp+" ON COMMIT DROP AS\n"+tableQuery(r, e.Set, e.Grain)+"\nWITH NO DATA"); err != nil {
		return nil, err
	}
	want, err := columns(ctx, q, "pg_temp."+tmp)
	if err != nil {
		return nil, err
	}
	got, err := columns(ctx, q, relation)
	if err != nil {
		return nil, err
	}

	var diffs []string
	for _, c := range sortedKeys(want, got) {
		w, declared := want[c]
		g, stored := got[c]
		switch {
		case !stored:
			diffs = append(diffs, c+" is missing")
		case !declared:
			diffs = append(diffs, c+" is not declared")
		case w != g:
			diffs = append(diffs, fmt.Sprintf("%s is %s, not %s", c, g, w))
		}
	}
	if len(diffs) == 0 {
		return nil, nil
	}
	return []config.Violation{{
		Code: errs.CodeConfigRollupChange,
		Path: yamlPath(path, "dimension_sets", strconv.Itoa(e.SetIndex)),
		Message: fmt.Sprintf("rollup %s: table %s exists with other columns than the file declares (%s), so another declaration built it; %s, or drop the table by hand",
			r.Name, e.Table, strings.Join(diffs, "; "), changeRemedy),
	}}, nil
}
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `go test -run '^TestIntegrationRollupRun' ./internal/rollup/ -v`
Expected: PASS, 3 tests.

Run: `go test -short -race ./internal/rollup/`
Expected: PASS. The integration tests skip under `-short`.

- [ ] **Step 6: Commit**

```bash
git add internal/rollup/postgres_state.go internal/rollup/postgres_source.go internal/rollup/install_integration_test.go
git commit -m "rollup: the state table, and the checks an install runs first

The state row records what each install applied, so the next one can
tell a new grain from a changed one. The source must have the columns
the file reads and an index led by the time column, or the trigger
scans the history on every write. An existing table is compared with
the shape ddl would give it, so a table another declaration built is
refused instead of written into."
```

---

### Task 5: `Install`

**Files:**
- Modify: `internal/errs/registry.go` (`CodeRollupInternal`)
- Modify: `internal/errs/testdata/codes.golden` (regenerated)
- Create: `internal/rollup/install.go`
- Create: `internal/rollup/install_test.go`
- Modify: `internal/rollup/install_integration_test.go` (append)
- Modify: `docs/coverage/features.yml` (`cli.rollup_run` requires integration)
- Modify: `docs/superpowers/specs/2026-09-24-rollup-daemon-design.md` (two clarifications)

**Interfaces:**
- Consumes: everything from Tasks 2–4; test helpers `startRollupPostgres`, `connectIn`, `execSQL`, `applyDDL`, `exampleDDL`, `writeMinutes`, `minute`, `at`, `count`, `assertGrainsEqualSource`, `loadExample`, `exampleRollup`.
- Produces:
  - `errs.CodeRollupInternal` = `"system.rollup.internal"`.
  - `func Install(ctx context.Context, conn *pgx.Conn, conf *config.RollupsConf, version string) (*InstallReport, error)`.
  - `type InstallReport struct { Rollups []RollupInstall; Undeclared []string }`.
  - `type RollupInstall struct { Name string; Adopted bool; Created []string; Plan Plan }`.

- [ ] **Step 1: Add the code**

In `internal/errs/registry.go`, before the `// The last resort.` comment in the `const` block, add:

```go
	// The rollup daemon and install. A domain of its own, so a supervisor
	// can tell the rollup process's failures from a pipeline's.
	CodeRollupInternal Code = "system.rollup.internal"
```

In the `registry` map, add:

```go
	CodeRollupInternal: {
		CodeRollupInternal,
		"A rollup command failed on a database error it could not classify. Nothing it began was committed.",
		"Read the wrapped database error. Retry once the database is healthy; if it repeats, report it with the message.",
	},
```

Run: `UPDATE_GOLDEN=1 go test ./internal/errs -run TestErrorTaxonomy_RegistryIsAppendOnly && go test ./internal/errs`
Expected: PASS. The golden gains `system.rollup.internal`.

- [ ] **Step 2: Write the failing tests**

Create `internal/rollup/install_test.go`:

```go
package rollup

import (
	"testing"
	"time"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

func TestCliRollupRun_NextStateKeepsProgressAndMovesRetainedTables(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")

	day := time.Date(2026, 9, 20, 0, 0, 0, 0, time.UTC)
	prev := &State{
		Rollup:   "posts",
		Backfill: map[string]*time.Time{"posts_by_lang_5m": &day, "posts_by_lang_1d": nil},
		Retained: []string{"posts_by_lang_7d", "posts_total_7d"},
	}
	p := planned{
		r:    exampleRollup(t),
		prev: prev,
		plan: Plan{
			Backfill: []string{"posts_by_lang_1h"},
			Retain:   []string{"posts_by_lang_1d"},
			Restore:  []string{"posts_by_lang_7d"},
		},
	}

	s := nextState(p, "v2")
	assert.Equal(t, "v2", s.Version)
	// Progress survives an install; a table created now starts over.
	assert.That(t, s.Backfill["posts_by_lang_5m"].Equal(day))
	_, pending := s.Backfill["posts_by_lang_1h"]
	assert.True(t, pending)
	// A retained table is no longer filled: nothing declares it.
	_, filling := s.Backfill["posts_by_lang_1d"]
	assert.False(t, filling)
	assert.DeepEqual(t, []string{"posts_total_7d", "posts_by_lang_1d"}, s.Retained)
}
```

Append to `internal/rollup/install_integration_test.go`, adding `"math/rand"` and `"github.com/jackc/pgx/v5"` to its imports:

```go
func mustInstall(t *testing.T, conn *pgx.Conn, conf *config.RollupsConf) *InstallReport {
	t.Helper()
	rep, err := Install(context.Background(), conn, conf, "test")
	assert.NoError(t, err)
	return rep
}

func stateOf(t *testing.T, conn *pgx.Conn, rollup string) *State {
	t.Helper()
	s, err := readState(context.Background(), conn, rollup)
	assert.NoError(t, err)
	assert.That(t, s != nil)
	return s
}

func relationExists(t *testing.T, conn *pgx.Conn, relation string) bool {
	t.Helper()
	return count(t, conn, "SELECT count(*) FROM pg_class WHERE oid = to_regclass('"+relation+"')") == 1
}

func TestIntegrationRollupRun_InstallOnAnEmptyDatabase(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	srv := startRollupPostgres(t)

	got := mustInstall(t, srv.conn, loadExample(t)).Rollups[0]
	assert.False(t, got.Adopted)
	assert.Equal(t, 10, len(got.Created))
	assert.DeepEqual(t, []string{"posts_by_lang_5m", "posts_total_5m"}, got.Plan.Backfill)

	// The triggers are live: writes reach every grain.
	writeMinutes(t, srv.dsn, minute{at("2026-09-15T10:01:00Z"), "en", 5}, minute{at("2026-09-15T10:07:00Z"), "ja", 3})
	assertGrainsEqualSource(t, srv.conn)

	s := stateOf(t, srv.conn, "posts")
	assert.Equal(t, "test", s.Version)
	assert.Equal(t, 2, len(s.Backfill))

	// A second install finds nothing to do and keeps the pending backfills.
	again := mustInstall(t, srv.conn, loadExample(t)).Rollups[0]
	assert.Equal(t, 0, len(again.Created))
	assert.DeepEqual(t, Plan{}, again.Plan)
	assert.Equal(t, 2, len(stateOf(t, srv.conn, "posts").Backfill))
}

func TestIntegrationRollupRun_InstallAdoptsTheMigrationsObjects(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	srv := startRollupPostgres(t)
	rng := rand.New(rand.NewSource(2))
	langs := []string{"en", "ja", "de"}
	var rows []minute
	for i := 0; i < 500; i++ {
		rows = append(rows, minute{
			at("2026-09-12T22:00:00Z").Add(time.Duration(rng.Intn(2*24*60)) * time.Minute),
			langs[rng.Intn(len(langs))], int32(1 + rng.Intn(100)),
		})
	}
	writeMinutes(t, srv.dsn, rows...)
	applyDDL(t, srv.conn, exampleDDL(t))
	assertGrainsEqualSource(t, srv.conn)

	got := mustInstall(t, srv.conn, loadExample(t)).Rollups[0]
	assert.True(t, got.Adopted)
	assert.Equal(t, 0, len(got.Created))
	// Nothing says a migration's tables are complete, so run fills them.
	assert.DeepEqual(t, []string{"posts_by_lang_5m", "posts_total_5m"}, got.Plan.Backfill)

	// No stored value changed, and the replaced triggers keep every grain.
	assertGrainsEqualSource(t, srv.conn)
	writeMinutes(t, srv.dsn, minute{at("2026-09-13T01:02:00Z"), "en", 7})
	assertGrainsEqualSource(t, srv.conn)
}

func TestIntegrationRollupRun_InstallRefusesAMismatchedTableAndChangesNothing(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	srv := startRollupPostgres(t)
	execSQL(t, srv.conn, "CREATE TABLE posts_by_lang_5m (bucket timestamptz, lang text, posts text)")

	_, err := Install(context.Background(), srv.conn, loadExample(t), "test")
	assert.Error(t, err)
	assert.Equal(t, errs.CodeConfigRollupChange, errs.CodeOf(err))
	assert.Equal(t, errs.ExitUserError, errs.ExitCode(err))
	assert.That(t, strings.Contains(err.Error(), "posts is text, not bigint"))

	// One transaction: the state table and every other object rolled back.
	assert.False(t, relationExists(t, srv.conn, "posts_by_lang_15m"))
	assert.False(t, relationExists(t, srv.conn, "sqlflow_rollup_state"))
}

// Every entrypoint of a deploy runs install at once.
func TestIntegrationRollupRun_FourInstallsAtOnce(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	srv := startRollupPostgres(t)
	type session struct {
		conn *pgx.Conn
		conf *config.RollupsConf
	}
	sessions := make([]session, 4)
	for i := range sessions {
		sessions[i] = session{connectIn(t, srv.dsn, "UTC"), loadExample(t)}
	}

	errc := make(chan error, len(sessions))
	for _, s := range sessions {
		go func(s session) {
			_, err := Install(context.Background(), s.conn, s.conf, "test")
			errc <- err
		}(s)
	}
	for range sessions {
		assert.NoError(t, <-errc)
	}
	assert.Equal(t, int64(20), count(t, srv.conn, "SELECT count(*) FROM pg_trigger WHERE tgname LIKE 'sqlflow_rollup_%'"))
	assert.Equal(t, int64(1), count(t, srv.conn, "SELECT count(*) FROM sqlflow_rollup_state"))
}

// A rollback to the previous file must not lose a grain's history.
func TestIntegrationRollupRun_InstallKeepsARemovedGrain(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	srv := startRollupPostgres(t)
	mustInstall(t, srv.conn, loadExample(t))

	rollback := loadExample(t)
	delete(rollback.Rollups[0].Grains, "1d")
	got := mustInstall(t, srv.conn, rollback).Rollups[0]
	retained := []string{"posts_by_lang_1d", "posts_total_1d"}
	assert.DeepEqual(t, retained, got.Plan.Retain)
	assert.DeepEqual(t, retained, stateOf(t, srv.conn, "posts").Retained)

	// The retained tables keep their triggers, so they stay current.
	writeMinutes(t, srv.dsn, minute{at("2026-09-15T10:01:00Z"), "en", 5})
	assertGrainsEqualSource(t, srv.conn)

	// Declared again, they need no backfill.
	back := mustInstall(t, srv.conn, loadExample(t)).Rollups[0]
	assert.DeepEqual(t, retained, back.Plan.Restore)
	assert.Equal(t, 0, len(back.Plan.Backfill))
	assert.Equal(t, 0, len(stateOf(t, srv.conn, "posts").Retained))
}

func TestIntegrationRollupRun_InstallRefusesAChangedMeasureType(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	srv := startRollupPostgres(t)
	mustInstall(t, srv.conn, loadExample(t))

	changed := loadExample(t)
	changed.Rollups[0].DimensionSets[0].Measures["posts"] = config.RollupMeasure{Type: "max", Column: "posts"}
	_, err := Install(context.Background(), srv.conn, changed, "test")
	assert.Error(t, err)
	assert.Equal(t, errs.CodeConfigRollupChange, errs.CodeOf(err))
	assert.That(t, strings.Contains(err.Error(), "rollups.0.dimension_sets.0.measures.posts"))
	assert.Equal(t, "sum", stateOf(t, srv.conn, "posts").Declaration.DimensionSets["posts_by_lang"].Measures["posts"].Type)
}

func TestIntegrationRollupRun_InstallReportsAnUndeclaredRollup(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	srv := startRollupPostgres(t)
	mustInstall(t, srv.conn, loadExample(t))

	renamed := loadExample(t)
	renamed.Rollups[0].Name = "posts_v2"
	rep := mustInstall(t, srv.conn, renamed)
	assert.DeepEqual(t, []string{"posts"}, rep.Undeclared)
	assert.True(t, rep.Rollups[0].Adopted)
}

func TestIntegrationRollupRun_InstallRecreatesADroppedTable(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	srv := startRollupPostgres(t)
	mustInstall(t, srv.conn, loadExample(t))
	execSQL(t, srv.conn, "DROP TABLE posts_by_lang_1h")

	got := mustInstall(t, srv.conn, loadExample(t)).Rollups[0]
	assert.DeepEqual(t, []string{"posts_by_lang_1h"}, got.Created)
	assert.DeepEqual(t, []string{"posts_by_lang_1h"}, got.Plan.Backfill)
	_, pending := stateOf(t, srv.conn, "posts").Backfill["posts_by_lang_1h"]
	assert.True(t, pending)
}

// The triggers name tables unqualified, so they resolve them through the
// writer's search_path. install creates them where that path puts them first.
func TestIntegrationRollupRun_InstallCreatesTablesInTheCurrentSchema(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	srv := startRollupPostgres(t)
	execSQL(t, srv.conn, "CREATE SCHEMA app")
	execSQL(t, srv.conn, "ALTER TABLE posts_per_minute_by_lang SET SCHEMA app")
	conn := connectIn(t, srv.dsn, "UTC")
	execSQL(t, conn, "SET search_path = app")

	mustInstall(t, conn, loadExample(t))
	assert.True(t, relationExists(t, srv.conn, "app.posts_by_lang_5m"))
	assert.True(t, relationExists(t, srv.conn, "app.sqlflow_rollup_state"))
	assert.False(t, relationExists(t, srv.conn, "public.posts_by_lang_5m"))

	execSQL(t, conn, "INSERT INTO posts_per_minute_by_lang (bucket, lang, posts) VALUES ('2026-09-15T10:01:00Z', 'en', 5)")
	assert.Equal(t, int64(5), count(t, conn, "SELECT posts FROM posts_by_lang_1d"))
}
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `go test -short ./internal/rollup/ -run TestCliRollupRun_NextState`
Expected: FAIL to compile with `undefined: planned` and `undefined: nextState`.

- [ ] **Step 4: Write `install.go`**

Create `internal/rollup/install.go`:

```go
package rollup

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/errs"
)

// installLock serializes installs. Every entrypoint of a deploy can run
// install at once, and CREATE TABLE IF NOT EXISTS is not safe against itself.
const installLock = "SELECT pg_advisory_xact_lock(hashtextextended('sqlflow_rollup_install', 0))"

// InstallReport says what one install did.
type InstallReport struct {
	Rollups []RollupInstall
	// Undeclared names rollups the state table holds and the file no longer
	// declares. Their tables and triggers stay.
	Undeclared []string
}

// RollupInstall is one declared rollup's part of an install.
type RollupInstall struct {
	Name string
	// Adopted is true when the database had no state row for the rollup and
	// some of its tables existed already: a migration created them.
	Adopted bool
	// Created lists the tables this install created, in the order edges
	// walks them.
	Created []string
	Plan    Plan
}

// Install makes the database hold what conf declares: every table, function
// and trigger, and a state row per rollup that records what was applied and
// which tables await a backfill. It fills nothing; `sqlflow rollup run`
// does.
//
// It runs in one transaction on conn, because Postgres DDL is transactional:
// a violation or an error leaves the database as it was. The transaction
// takes the source's trigger lock for its DDL only, never for a backfill, so
// a pipeline's write waits milliseconds.
func Install(ctx context.Context, conn *pgx.Conn, conf *config.RollupsConf, version string) (*InstallReport, error) {
	if err := conf.CheckError(); err != nil {
		return nil, err
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		return nil, installError(err, "begin")
	}
	// A no-op once Commit has run.
	defer func() { _ = tx.Rollback(ctx) }()

	report, err := install(ctx, tx, conf, version)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, installError(err, "commit")
	}
	return report, nil
}

// planned is one rollup checked and planned, waiting for every rollup in the
// file to pass before anything is applied.
type planned struct {
	r       config.Rollup
	prev    *State
	missing map[string]bool
	plan    Plan
}

func install(ctx context.Context, tx pgx.Tx, conf *config.RollupsConf, version string) (*InstallReport, error) {
	// Unqualified CREATE TABLE lands in current_schema(), and the triggers
	// resolve the same names through the same search_path.
	var schema *string
	if err := tx.QueryRow(ctx, "SELECT current_schema()").Scan(&schema); err != nil {
		return nil, installError(err, "read current_schema()")
	}
	if schema == nil {
		return nil, errs.New(errs.CodeConfigInvalid,
			"rollup install: no schema on the connection's search_path exists, so there is nowhere to create the rollup tables")
	}
	for _, stmt := range []string{installLock, stateDDL} {
		if _, err := tx.Exec(ctx, stmt); err != nil {
			return nil, installError(err, "prepare")
		}
	}

	var violations []config.Violation
	var todo []planned
	for i, r := range conf.Rollups {
		path := []string{"rollups", strconv.Itoa(i)}
		v, err := checkSource(ctx, tx, r, path)
		if err != nil {
			return nil, installError(err, "check source "+r.Source.Table)
		}
		if len(v) > 0 {
			violations = append(violations, v...)
			continue
		}

		prev, err := readState(ctx, tx, r.Name)
		if err != nil {
			return nil, installError(err, "read state")
		}
		missing := map[string]bool{}
		for _, e := range edges(r) {
			relation := quote(*schema) + "." + quote(e.Table)
			cols, err := columns(ctx, tx, relation)
			if err != nil {
				return nil, installError(err, "read "+e.Table)
			}
			if len(cols) == 0 {
				missing[e.Table] = true
				continue
			}
			v, err := compareTable(ctx, tx, r, e, relation, path)
			if err != nil {
				return nil, installError(err, "compare "+e.Table)
			}
			violations = append(violations, v...)
		}

		var prevApplied *Applied
		var retained []string
		if prev != nil {
			prevApplied, retained = &prev.Declaration, prev.Retained
		}
		plan, v := PlanChange(r, path, prevApplied, retained, missing)
		violations = append(violations, v...)
		todo = append(todo, planned{r: r, prev: prev, missing: missing, plan: plan})
	}
	if len(violations) > 0 {
		return nil, violationError(violations)
	}

	report := &InstallReport{}
	declared := map[string]bool{}
	for _, p := range todo {
		declared[p.r.Name] = true
		script, err := PostgresObjects(p.r)
		if err != nil {
			return nil, err
		}
		if _, err := tx.Exec(ctx, script); err != nil {
			return nil, installError(err, "apply rollup "+p.r.Name)
		}
		if err := writeState(ctx, tx, nextState(p, version)); err != nil {
			return nil, installError(err, "write state")
		}
		ri := RollupInstall{Name: p.r.Name, Plan: p.plan}
		es := edges(p.r)
		for _, e := range es {
			if p.missing[e.Table] {
				ri.Created = append(ri.Created, e.Table)
			}
		}
		ri.Adopted = p.prev == nil && len(ri.Created) < len(es)
		report.Rollups = append(report.Rollups, ri)
	}

	states, err := readAllStates(ctx, tx)
	if err != nil {
		return nil, installError(err, "read state")
	}
	for _, s := range states {
		if !declared[s.Rollup] {
			report.Undeclared = append(report.Undeclared, s.Rollup)
		}
	}
	return report, nil
}

// nextState is the row an install writes: the declaration as applied now,
// every backfill still in progress, and the plan's changes to both lists.
func nextState(p planned, version string) State {
	s := State{Rollup: p.r.Name, Declaration: AppliedFrom(p.r), Version: version, Backfill: map[string]*time.Time{}}
	if p.prev != nil {
		for t, done := range p.prev.Backfill {
			s.Backfill[t] = done
		}
		for _, t := range p.prev.Retained {
			if !slices.Contains(p.plan.Restore, t) {
				s.Retained = append(s.Retained, t)
			}
		}
	}
	// A table created now holds nothing, whatever an earlier backfill did to
	// a table of that name.
	for _, t := range p.plan.Backfill {
		s.Backfill[t] = nil
	}
	// A retained table is no longer filled: nothing declares it.
	for _, t := range p.plan.Retain {
		delete(s.Backfill, t)
		s.Retained = append(s.Retained, t)
	}
	return s
}

// violationError folds every violation into one error, as CheckError does,
// with each one's code: a database check and a declaration change can both
// stop one install.
func violationError(vs []config.Violation) error {
	var b strings.Builder
	b.WriteString("rollup install changed nothing")
	for _, v := range vs {
		fmt.Fprintf(&b, "\n  %s: [%s] %s", strings.Join(v.Path, "."), v.Code, v.Message)
	}
	return errs.New(vs[0].Code, "%s", b.String())
}

func installError(err error, step string) error {
	return errs.Wrap(errs.CodeRollupInternal, err, "rollup install: %s", step)
}
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `go test -short -race ./internal/rollup/`
Expected: PASS, including `TestCliRollupRun_NextStateKeepsProgressAndMovesRetainedTables`.

Run: `go test -run '^TestIntegrationRollupRun' ./internal/rollup/ -v`
Expected: PASS, 12 tests.

Run: `go test -run '^TestIntegrationRollup_' ./internal/rollup/`
Expected: PASS. The trigger tests from the 2026-09-15 spec still pass.

- [ ] **Step 6: Require integration evidence**

In `docs/coverage/features.yml`, change the `cli.rollup_run` entry to:

```yaml
  # The daemon that manages rollup tables: install, run and verify. The
  # release level joins with the image test for `sqlflow rollup run`.
  - id: cli.rollup_run
    description: Installs, backfills, checks and reports on the rollup tables a rollups file declares.
    requires: [unit, integration]
```

- [ ] **Step 7: Bring the spec in line**

In `docs/superpowers/specs/2026-09-24-rollup-daemon-design.md`:

Replace:

```
- **A schema key.** Rollup tables go where the source is, through the
  connection's `search_path`, as today.
```

with:

```
- **A schema key.** Rollup tables and the state table go in the
  connection's `current_schema()`, the first schema on its `search_path`
  that exists. The triggers name tables unqualified, so they resolve them
  through the writer's `search_path`, as today.
```

In the exit-code table, after the `system.rollup.unreachable` row, add:

```
| `system.rollup.internal` | A database error a rollup command could not classify. The registry gives every domain a catch-all. |
```

- [ ] **Step 8: Commit**

```bash
git add internal/errs/registry.go internal/errs/testdata/codes.golden internal/rollup/install.go \
  internal/rollup/install_test.go internal/rollup/install_integration_test.go \
  docs/coverage/features.yml docs/superpowers/specs/2026-09-24-rollup-daemon-design.md
git commit -m "rollup: Install, in one transaction behind an advisory lock

Install checks the source and every existing table, plans each rollup
against its state row, and applies only when nothing is refused, so a
bad file leaves the database as it was. Four concurrent installs create
each object once, and a removed grain keeps its rows and triggers. If
the lock or the transaction is wrong, a deploy's entrypoints race and a
half-applied rollup reaches the pipeline's write path."
```

---

### Task 6: `sqlflow rollup install`

**Files:**
- Modify: `internal/errs/registry.go` (`CodeRollupUnreachable`)
- Modify: `internal/errs/testdata/codes.golden` (regenerated)
- Create: `internal/rollup/connect.go`
- Create: `internal/cli/rollup/install.go`
- Modify: `internal/cli/rollup/rollup.go` (register the command)
- Create: `internal/cli/rollup/install_test.go`
- Create: `internal/cli/rollup/install_integration_test.go`

**Interfaces:**
- Consumes: `rollup.Install`, `rollup.InstallReport` (Task 5); `config.LoadRollups`, `RollupsConf.PostgresDSN` (Task 1); `buildinfo.Version`; the test helpers `run`, `readFile` and `example` in `internal/cli/rollup/rollup_test.go`.
- Produces:
  - `errs.CodeRollupUnreachable` = `"system.rollup.unreachable"`.
  - `func Connect(ctx context.Context, dsn string) (*pgx.Conn, error)` in `internal/rollup`.
  - The hidden `sqlflow rollup install -c FILE` command. Plan 2 reuses `Connect`, and unhides the command when `run` ships.

- [ ] **Step 1: Add the code**

In `internal/errs/registry.go`, after `CodeRollupInternal` in the `const` block, add:

```go
	// The rollup store refused or dropped the connection. Retryable: the
	// database may come back.
	CodeRollupUnreachable Code = "system.rollup.unreachable"
```

In the `registry` map, add:

```go
	CodeRollupUnreachable: {
		CodeRollupUnreachable,
		"A rollup command could not connect to the database in store.postgres.dsn.",
		"Check that the database is up and reachable from this host, and that the dsn's host, port, user and password are right. The message names the host and the database, never the password.",
	},
```

Run: `UPDATE_GOLDEN=1 go test ./internal/errs -run TestErrorTaxonomy_RegistryIsAppendOnly && go test ./internal/errs`
Expected: PASS. The golden gains `system.rollup.unreachable`.

- [ ] **Step 2: Write the failing tests**

Create `internal/cli/rollup/install_test.go`:

```go
package rollup

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/turbolytics/sql-flow/internal/errs"
	"github.com/zeebo/assert"
)

// withStore writes the example with a store block holding dsn, and returns
// its path.
func withStore(t *testing.T, dsn string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "rollups.yml")
	text := fmt.Sprintf("store:\n  type: postgres\n  postgres:\n    dsn: %q\n", dsn) + readFile(t, example)
	assert.NoError(t, os.WriteFile(path, []byte(text), 0o644))
	return path
}

func TestCliRollupRun_InstallIsHiddenUntilRunExists(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")

	install, _, err := NewCommand().Find([]string{"install"})
	assert.NoError(t, err)
	assert.Equal(t, "install", install.Name())
	assert.True(t, install.Hidden)
}

func TestCliRollupRun_InstallRefusesAnEmptyDSN(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")

	_, _, err := run(t, "install", "-c", withStore(t, ""))
	assert.Error(t, err)
	assert.Equal(t, errs.CodeConfigInvalid, errs.CodeOf(err))
	assert.Equal(t, errs.ExitUserError, errs.ExitCode(err))
}

// A deploy log is read by more people than the database's owner.
func TestCliRollupRun_InstallNamesAnUnreachableStoreWithoutItsPassword(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")

	dsn := "postgres://rollup:s3cret-pw@127.0.0.1:1/rollup?sslmode=disable&connect_timeout=2"
	_, stderr, err := run(t, "install", "-c", withStore(t, dsn))
	assert.Error(t, err)
	assert.Equal(t, errs.CodeRollupUnreachable, errs.CodeOf(err))
	assert.Equal(t, errs.ExitInternal, errs.ExitCode(err))
	assert.That(t, strings.Contains(err.Error(), "127.0.0.1"))
	assert.False(t, strings.Contains(err.Error(), "s3cret-pw"))
	assert.False(t, strings.Contains(stderr, "s3cret-pw"))
}
```

Create `internal/cli/rollup/install_integration_test.go`:

```go
package rollup

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

func TestIntegrationRollupRun_InstallCommandCreatesTheTables(t *testing.T) {
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
	_, err = conn.Exec(ctx, `CREATE TABLE posts_per_minute_by_lang (
  bucket TIMESTAMPTZ NOT NULL, lang TEXT NOT NULL, posts INTEGER NOT NULL,
  PRIMARY KEY (bucket, lang))`)
	assert.NoError(t, err)

	path := withStore(t, dsn)
	out, _, err := run(t, "install", "-c", path)
	assert.NoError(t, err)
	assert.That(t, strings.Contains(out, "rollup posts: created posts_by_lang_5m\n"))
	assert.That(t, strings.Contains(out, "rollup posts: posts_by_lang_5m awaits a backfill\n"))

	out, _, err = run(t, "install", "-c", path)
	assert.NoError(t, err)
	assert.False(t, strings.Contains(out, "created"))
	assert.That(t, strings.Contains(out, "rollup posts: functions and triggers are current\n"))
}
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `go test -short ./internal/cli/rollup/ -run TestCliRollupRun`
Expected: FAIL to compile with `undefined: errs.CodeRollupUnreachable` if Step 1 is not done yet. With Step 1 done, all three tests fail, because `install` is not a subcommand yet.

- [ ] **Step 4: Write `Connect`**

Create `internal/rollup/connect.go`:

```go
package rollup

import (
	"context"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/turbolytics/sql-flow/internal/errs"
)

// connectTimeout bounds a connect, so a database that drops packets fails a
// deploy's entrypoint instead of hanging it.
const connectTimeout = 10 * time.Second

// Connect opens a connection to the rollup store. Its errors never carry the
// password: the DSN holds it, so neither the DSN nor pgx's parse error is
// repeated, and a connect error has it removed.
func Connect(ctx context.Context, dsn string) (*pgx.Conn, error) {
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, errs.New(errs.CodeConfigInvalid, "store.postgres.dsn is not a Postgres connection string or URL")
	}
	if cfg.ConnectTimeout <= 0 || cfg.ConnectTimeout > connectTimeout {
		cfg.ConnectTimeout = connectTimeout
	}
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		msg := err.Error()
		if cfg.Password != "" {
			msg = strings.ReplaceAll(msg, cfg.Password, "********")
		}
		return nil, errs.New(errs.CodeRollupUnreachable, "rollup store %s/%s: %s", cfg.Host, cfg.Database, msg)
	}
	return conn, nil
}
```

- [ ] **Step 5: Write the command**

Create `internal/cli/rollup/install.go`:

```go
package rollup

import (
	"context"
	"fmt"
	"io"

	"github.com/spf13/cobra"
	"github.com/turbolytics/sql-flow/internal/buildinfo"
	"github.com/turbolytics/sql-flow/internal/config"
	gen "github.com/turbolytics/sql-flow/internal/rollup"
)

func newInstallCommand() *cobra.Command {
	var configPath string
	cmd := &cobra.Command{
		Use:   "install",
		Short: "Create the rollup tables and triggers a rollups file declares, then exit",
		Long: "Create every rollup table, function and trigger the rollups file declares, in one " +
			"transaction, and record what was applied in sqlflow_rollup_state. A change that would " +
			"corrupt stored rows changes nothing and exits non-zero. install fills no table: " +
			"`sqlflow rollup run` backfills the tables it marks.",
		// Hidden until `sqlflow rollup run` exists to backfill what install
		// creates. A table created over existing history stays partial until
		// then.
		Hidden: true,
		Args:   cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			conf, err := config.LoadRollups(configPath)
			if err != nil {
				return err
			}
			// The file's rules before its database, so a typo is reported as
			// a typo and not as a connection failure.
			if err := conf.CheckError(); err != nil {
				return err
			}
			dsn, err := conf.PostgresDSN()
			if err != nil {
				return err
			}
			ctx := cmd.Context()
			if ctx == nil {
				ctx = context.Background()
			}
			conn, err := gen.Connect(ctx, dsn)
			if err != nil {
				return err
			}
			defer conn.Close(context.Background())

			report, err := gen.Install(ctx, conn, conf, buildinfo.Version)
			if err != nil {
				return err
			}
			printInstall(cmd.OutOrStdout(), report)
			return nil
		},
	}
	cmd.Flags().StringVarP(&configPath, "config", "c", "", "Path to the rollups file")
	_ = cmd.MarkFlagRequired("config")
	return cmd
}

// printInstall writes one line per thing the install did, so a deploy log
// shows what changed and what still needs `sqlflow rollup run`.
func printInstall(w io.Writer, rep *gen.InstallReport) {
	for _, r := range rep.Rollups {
		if r.Adopted {
			fmt.Fprintf(w, "rollup %s: adopted the tables a migration created\n", r.Name)
		}
		for _, t := range r.Created {
			fmt.Fprintf(w, "rollup %s: created %s\n", r.Name, t)
		}
		for _, t := range r.Plan.Retain {
			fmt.Fprintf(w, "rollup %s: %s is no longer declared; its table and triggers stay\n", r.Name, t)
		}
		for _, t := range r.Plan.Restore {
			fmt.Fprintf(w, "rollup %s: %s is declared again\n", r.Name, t)
		}
		for _, t := range r.Plan.Backfill {
			fmt.Fprintf(w, "rollup %s: %s awaits a backfill\n", r.Name, t)
		}
		fmt.Fprintf(w, "rollup %s: functions and triggers are current\n", r.Name)
	}
	for _, name := range rep.Undeclared {
		fmt.Fprintf(w, "rollup %s is no longer declared; its tables and triggers stay\n", name)
	}
}
```

In `internal/cli/rollup/rollup.go`, change the registration line in `NewCommand` to:

```go
	cmd.AddCommand(newDDLCommand(), newServeCommand(), newCheckCommand(), newInstallCommand())
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `go test -short -race ./internal/cli/rollup/ ./internal/rollup/`
Expected: PASS.

Run: `go test -run '^TestIntegrationRollupRun' ./internal/cli/rollup/ -v`
Expected: PASS, 1 test.

- [ ] **Step 7: Run the whole gate**

Run: `go build ./... && go vet ./... && test -z "$(gofmt -l .)" && go test -short -race ./...`
Expected: no output from `gofmt -l`, and every package passes.

Run: `go test -json -run '^TestIntegration' ./internal/rollup/ ./internal/cli/rollup/ | tail -3`
Expected: the final event's `"Action":"pass"`.

Run: `go run ./cmd/sqlflow rollup --help`
Expected: `install` is not listed. `go run ./cmd/sqlflow rollup install --help` still prints its usage.

- [ ] **Step 8: Commit**

```bash
git add internal/errs/registry.go internal/errs/testdata/codes.golden internal/rollup/connect.go \
  internal/cli/rollup/install.go internal/cli/rollup/rollup.go \
  internal/cli/rollup/install_test.go internal/cli/rollup/install_integration_test.go
git commit -m "cli: sqlflow rollup install, hidden until run can backfill

The command applies what the rollups file declares and prints what it
created, retained and left for a backfill. A connection failure reports
system.rollup.unreachable with the host and database and never the
password. It stays hidden: a table created over existing history is
partial until sqlflow rollup run fills it."
```

- [ ] **Step 9: Open the PR**

The PR goes out under the maintainer's name. Paste the title and body below into the chat and wait for their go before running this step.

```bash
git push -u origin feat/rollup-install:feat/rollup-install
gh pr create --repo turbolytics/sql-flow --base main --head feat/rollup-install \
  --title "rollup: sqlflow rollup install, the first step of the rollup daemon" \
  --body-file <(cat <<'EOF'
The first of five plans for `docs/superpowers/specs/2026-09-24-rollup-daemon-design.md`: `sqlflow rollup install` creates every rollup table, function and trigger a rollups file declares, in one transaction behind an advisory lock, and records what it applied in `sqlflow_rollup_state`.

- `rollups.yml` gains `store` and `turbostats`.
- The generator writes the objects apart from the migration. The `ddl` goldens do not change.
- A change that would corrupt stored rows is refused with `user.config.rollup_change`. A removed grain keeps its tables and triggers.
- The command is hidden until Plan 2 ships `sqlflow rollup run`, which backfills what install marks.

Evidence: 12 integration tests against Postgres 18, including four concurrent installs, adopting a migration's objects, and a rollback that keeps a grain.
EOF
)
gh pr view feat/rollup-install --repo turbolytics/sql-flow --json state,baseRefName --jq '.state, .baseRefName'
```

Expected: `OPEN` and `main`.

---

## After the final review

The whole-branch review of #380 found one Critical and four Important defects, three of them dictated by this plan. The code on #380 differs from the plan text above in these places. Plan 2 builds on the code, not on the text.

- **Lock order (Task 2, Task 5).** `install` locks every source `IN SHARE ROW EXCLUSIVE MODE`, in name order, before its objects script. Without it, install deadlocked beside a live writer in 34 of 40 runs. `PostgresObjects` still takes no lock of its own.
- **Lock timeout and retries (Task 5).** `installLockTimeout`, 2 seconds, is set with `SET LOCAL lock_timeout` after the install lock. `Install` makes up to three attempts on `55P03` or `40P01`.
- **Isolation (Task 5).** `installOnce` begins with `pgx.TxOptions{IsoLevel: pgx.ReadCommitted}`.
- **Source dimensions (Task 3).** `sameSource` compares table, time column and grain only.
- **Retained tables (Tasks 3–5).** `State.Retained` and `Plan.Retain` are `[]RetainedTable{Table, Set, Grain, From, Shape}`. `PlanChange` takes `[]RetainedTable` and refuses a retained table declared again in another shape. `nextState` keeps a retained table's pending backfill.
- **Coverage (Task 1).** Adding a feature also needs its row in `docs/coverage/status/features.yml`, not only `make coverage-page`.

A second review (Fable) found no Critical defects and two Important ones, fixed on #380:

- **Dependency lock order.** `lockOrder` locks a source that another declared rollup builds after that rollup's source, with name order as the tie break. `InstallReport.Attempts` records the retries a lock wait or a deadlock cost.
- **Dropped retained tables.** `install` removes the record and pending backfill of a retained table that no longer exists, and reports it in `RollupInstall.Dropped`.

It measured install at 19 to 34 ms, with the source locked for about 3 ms and no data row read, on a 1.3-million-row source.

Carried to Plan 2:

- Backfill fills declared tables only, and skips the pending entries of retained ones.
- `DriftIsReported` must disable the trigger after install: `CREATE OR REPLACE TRIGGER` re-enables a disabled one.
- Retry logic needs finer error codes than `system.rollup.internal`.
- Align the spec or the code on the backfill-target rule: the code marks a new table whose from table exists, and the spec says whose from table's backfill is complete.
