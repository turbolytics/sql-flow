# Watermark-Driven Close Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** The window manager closes buckets when the engine tells it the watermark moved, decides nothing on a clock, and never sweeps the table for late rows; the engine decides lateness at arrival; `allowed_lateness_seconds` replaces `late_rows`; `poll_interval_seconds` is gone.

**Architecture:** `core.Watermarks` gains a per-window signal (a kick channel and a recompute set) and a per-record lateness classification read from lock-free atomics. The consume loop refuses records late beyond lateness before the handler, remembers buckets a late-but-allowed record landed in, and after each commit kicks the windows whose watermark moved and publishes the recompute buckets. The manager becomes one `Pass` run on start, on kick and on drain: publish due buckets, republish recompute buckets as whole values, delete expired buckets, save the closed watermark. Every configuration, schema, example, template, ledger entry and document that named the removed keys is updated in the same change.

**Tech Stack:** Go 1.26, DuckDB via ADBC, Arrow, zeebo/assert, the coverage ledger (`docs/coverage/*.yml`, rendered by `make coverage-page`), the config JSON schema golden (`UPDATE_GOLDEN=1`), the simulator in `internal/simulate`, the model in `internal/managers/model.go`, the conformance harness in `internal/conformance`.

**Spec:** `docs/superpowers/specs/2026-09-26-watermark-driven-close-design.md`

## Global Constraints

- No clock reaches a window decision. The manager takes no clock, no ticker and no interval. The engine's clock decides partition idleness only, as it does after #393.
- The engine's bucket function is DuckDB's: `time_bucket(INTERVAL '<size>', ts)` with origin `2000-01-03 00:00:00 UTC`. Verified empirically on 2026-09-26: Go's `time.Truncate` (origin year 1) agrees for every size that divides a day and for week multiples, and disagrees at 25 h. The Go function uses DuckDB's origin explicitly and a test proves agreement over a grid that includes 25 h and 3 days.
- A record is refused as late only when it is late beyond lateness for **every** window on the pipeline. A record admitted for one window and late-beyond for another is deleted by the stricter window's purge on its next pass; no shipped example has two windows.
- Both signals are sent **after** the commit that made their rows visible. A batch that rolls back kicks nothing and publishes no recompute buckets.
- `late_rows` and `poll_interval_seconds` are removed, not deprecated. The window block is `additionalProperties: false`, so a config that sets either fails validation. The validate message for each names its replacement and this spec.
- Every removed key is removed from: `internal/config/config.go`, `internal/validate/schemas/config.json` (regenerated), `README.md`, `render/pipeline.yml`, `render/docker-compose.yml` (`SQLFLOW_WINDOW_POLL_SECONDS`, twice), `dev/config/examples/tumbling.window.yml`, `dev/config/examples/kafka.stateful.window.yml`, `dev/config/examples/mqtt.duckdb.agg.yml`, `dev/config/examples/logs.rollup.clickhouse.yml`, `dev/config/examples/bluesky/bluesky.postgres.windowed.yml`, `dev/config/examples/bluesky/bluesky.kafka.windowed.yml`, `dev/bench/bluesky/demo-10x.yml`, `dev/bench/bluesky/demo-10x-noop-sink.yml`, `dev/bench/bluesky/demo-10x-sqlcommand.yml`, `.claude/skills/slow-soak/slow.yml`.
- Test commands: `export GOCACHE=<scratchpad>/gocache GOFLAGS=-mod=readonly`; macOS has no `timeout`, use `perl -e 'alarm shift; exec @ARGV' 900 go test ...`; never let a pipe mask an exit code (`> file 2>&1; echo rc=$?`). Port-binding tests (`internal/serve`, `internal/cli/run` debug API, webhook) need `dangerouslyDisableSandbox`. `TestIntegration*` need Docker; leave them to CI.
- Commit after every task with a message that says why, in this repo's voice (see `git log`).

---

## File structure

| file | responsibility after this plan |
|---|---|
| `internal/core/bucket.go` (new) | `BucketStart(at, size)`: DuckDB's `time_bucket` in Go, origin 2000-01-03. |
| `internal/core/bucket_test.go` (new) | agreement with DuckDB over a grid of sizes and instants. |
| `internal/core/watermarks.go` | adds `Lateness` to `WindowSpec`; per-window asserted value as an atomic; `Classify(atNanos)`; `WindowSignal` per window. |
| `internal/core/turbine.go` | the lateness check after placement; recompute buckets accumulated per batch; kicks after commit; `WindowLateRows` counter. |
| `internal/core/metrics.go` | `WindowLateRows` counter `window_late_rows_total{window,outcome}`. |
| `internal/config/config.go` | `AllowedLatenessSecs`; `LateRows`, `PollIntervalSecs`, `ReemitOverwrites`, `ReemitOverwritesMessage` removed. |
| `internal/validate/window.go` | `time_column` must be `time_bucket(INTERVAL '<size>', event_time)` (error); lateness > 0 needs a replacing sink; the reemit rules removed. |
| `internal/validate/sinks.go` | the `ReemitOverwrites` call removed. |
| `internal/managers/watermark.go` | `Pass` replaces `Poll`; `Start` runs a pass on start, on kick, on drain; the ticker, `poll`, and the late-row branch removed. |
| `internal/managers/decide.go` | bucket table on `bucket ∈ {open, due, retained, expired}`; `LatePolicy` removed. |
| `internal/managers/sql.go` | `dueBetweenSQL`, `bucketSQL`, `expiredBeforeSQL`. |
| `internal/managers/model.go` | lateness in the model; late events; exact-value property. |
| `internal/cli/run/managers.go` | `windowDeclaration` maps lateness; `NewWatermark` gets the window's signal; the run-time refusal is lateness + append-only sink. |
| `internal/conformance/manager.go` | `New` loses `poll` and `late`; `Poll` is `Pass`; the two late checks removed; the drain check cancels before `Start`. |
| `internal/turbostats/collect.go`, `turbostats/wire/bundle.go` | `late_rows_dropped` from `outcome=refused`; `late_rows_recomputed` from `outcome=recomputed`; `late_rows_reemitted` removed. |
| `internal/simulate/*` | `Pass`, a replace-semantics window sink, the signal-driven runner, five new scenarios. |
| `docs/coverage/invariants.yml`, `docs/windows/decisions.md`, `README.md`, `CHANGELOG.md` | as the spec's ledger section. |

---

### Task 1: The bucket function, proven against DuckDB

**Files:**
- Create: `internal/core/bucket.go`
- Create: `internal/core/bucket_test.go`

**Interfaces:**
- Produces: `func BucketStart(at time.Time, size time.Duration) time.Time` and `func BucketEnd(at time.Time, size time.Duration) time.Time`. Every later task that needs a record's bucket calls these and nothing else.

- [ ] **Step 1: Write the failing test**

```go
package core

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/turbolytics/sql-flow/internal/duckdb"
	"github.com/zeebo/assert"
)

// The engine decides a record's lateness from its bucket, and the handler's
// SQL computes time_column with time_bucket. If the two disagree, lateness
// is decided against the wrong bucket. This proves BucketStart is
// time_bucket, over every size a window is likely to declare and the ones
// where a naive truncation is wrong: 25 hours and 3 days do not divide the
// span from Go's zero time to DuckDB's origin, and time.Truncate gets them
// wrong by hours.
func TestWindowBucket_AgreesWithDuckDBTimeBucket(t *testing.T) {
	coverage.Covers(t, "manager.window")
	ctx := context.Background()
	db, err := duckdb.OpenPath(ctx, "")
	assert.NoError(t, err)
	defer db.Close()
	conn, err := db.Connect(ctx)
	assert.NoError(t, err)
	defer conn.Close()

	instants := []time.Time{
		time.Date(2026, 9, 26, 12, 34, 56, 789000000, time.UTC),
		time.Date(2026, 9, 26, 0, 0, 0, 0, time.UTC),
		time.Date(2026, 9, 26, 23, 59, 59, 999999000, time.UTC),
		time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC), // EventTimeFloor
		time.Date(2026, 9, 26, 12, 35, 0, 0, time.UTC), // exactly on a minute
		time.Date(2026, 9, 26, 12, 34, 59, 999999000, time.UTC),
	}
	sizes := []int{1, 7, 60, 90, 300, 420, 3600, 5400, 7200, 86400, 90000, 172800, 259200, 604800}
	for _, size := range sizes {
		for _, at := range instants {
			q := fmt.Sprintf(`SELECT epoch_us(time_bucket(INTERVAL '%d seconds', TIMESTAMPTZ '%s'))`,
				size, at.Format("2006-01-02 15:04:05.999999-07:00"))
			stmt, err := conn.NewStatement()
			assert.NoError(t, err)
			assert.NoError(t, stmt.SetSqlQuery(q))
			rdr, _, err := stmt.ExecuteQuery(ctx)
			assert.NoError(t, err)
			var micros int64
			for rdr.Next() {
				micros = rdr.Record().Column(0).(*array.Int64).Value(0)
			}
			rdr.Release()
			stmt.Close()

			want := time.UnixMicro(micros).UTC()
			got := BucketStart(at, time.Duration(size)*time.Second)
			if !got.Equal(want) {
				t.Fatalf("size %ds at %s: BucketStart %s, time_bucket %s", size, at, got, want)
			}
			assert.Equal(t, want.Add(time.Duration(size)*time.Second), BucketEnd(at, time.Duration(size)*time.Second))
		}
	}
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `perl -e 'alarm shift; exec @ARGV' 600 go test -count=1 -run TestWindowBucket ./internal/core/`
Expected: FAIL, `undefined: BucketStart`.

- [ ] **Step 3: Implement**

```go
package core

import "time"

// bucketOrigin is where DuckDB's time_bucket aligns its buckets: 2000-01-03,
// a Monday, so that week-sized buckets start on Mondays. The engine buckets
// from the same origin so that the bucket it decides a record's lateness
// against is the bucket the handler's time_bucket puts the row in.
//
// Go's time.Truncate aligns to the zero time, year 1 -- also a Monday, which
// is why the two agree for any size that divides a day and for multiples of
// a week, and disagree for sizes like 25 hours or 3 days. Using DuckDB's
// origin explicitly makes them agree for every size.
var bucketOrigin = time.Date(2000, 1, 3, 0, 0, 0, 0, time.UTC)

// BucketStart is time_bucket(INTERVAL 'size', at): the start of the bucket
// of length size that holds at, aligned to bucketOrigin. at is after the
// origin for every event time the engine places (EventTimeFloor is 2020).
func BucketStart(at time.Time, size time.Duration) time.Time {
	since := at.Sub(bucketOrigin)
	return bucketOrigin.Add(since - since%size)
}

// BucketEnd is the end of the bucket holding at: BucketStart + size. A
// bucket is closed when its end is at or before the watermark.
func BucketEnd(at time.Time, size time.Duration) time.Time {
	return BucketStart(at, size).Add(size)
}
```

- [ ] **Step 4: Run it to verify it passes**

Run: `perl -e 'alarm shift; exec @ARGV' 600 go test -count=1 -run TestWindowBucket ./internal/core/`
Expected: PASS. If a size fails, the origin is wrong, not the size: do not restrict the size list.

- [ ] **Step 5: Commit**

```bash
git add internal/core/bucket.go internal/core/bucket_test.go
git commit -m "core: the engine's bucket is DuckDB's time_bucket, proven over a grid"
```

---

### Task 2: Configuration -- `allowed_lateness_seconds` in, `late_rows` and `poll_interval_seconds` out

**Files:**
- Modify: `internal/config/config.go` (the `Window` struct, `ReemitOverwrites`, `ReemitOverwritesMessage`)
- Modify: `internal/config/config_test.go` if it names `LateRows` or `PollIntervalSecs` (grep)
- Modify: `internal/managers/watermark.go` (`Declaration`, `validate`)
- Modify: `internal/core/watermarks.go` (`WindowSpec`)
- Modify: `internal/cli/run/managers.go` (`windowDeclaration`, `windowSpecs`)
- Regenerate: `internal/validate/schemas/config.json`, `internal/cli/testdata/config_example.golden`

**Interfaces:**
- Produces: `config.Window.AllowedLatenessSecs int`; `(Window) Lateness() time.Duration`; `managers.Declaration.Lateness time.Duration` (replaces `Late LatePolicy`); `core.WindowSpec.Lateness time.Duration`.
- Removes: `config.Window.LateRows`, `config.Window.PollIntervalSecs`, `config.Window.ReemitOverwrites`, `config.ReemitOverwritesMessage`, `managers.LatePolicy`, `managers.LateDrop`, `managers.LateReemit`, `managers.ParseLatePolicy`, `managers.Declaration.Late`.

This task will not compile on its own until Tasks 5 and 6 remove the uses of `LatePolicy`; do it first and let the compiler list the call sites, but **do not** fix call sites in packages a later task owns -- stub them with the minimum that compiles and leave a `// Task N` comment only where that task rewrites the file anyway. The packages this task must leave compiling: `internal/config`, `internal/schema`, `internal/cli`.

- [ ] **Step 1: Write the failing test**

In `internal/config/config_test.go` (create the file if none exists in that package for windows):

```go
// A window's lateness is one number, default zero, and the two keys it
// replaces are gone from the type: a config that sets them fails the schema
// rather than being silently accepted with a policy its author did not write.
func TestConfigValidation_WindowLatenessIsOneNumber(t *testing.T) {
	coverage.Covers(t, "config.validation")
	w := Window{TimeColumn: "bucket", SizeSeconds: 60}
	assert.Equal(t, time.Duration(0), w.Lateness())
	w.AllowedLatenessSecs = 300
	assert.Equal(t, 5*time.Minute, w.Lateness())

	typ := reflect.TypeOf(Window{})
	for _, gone := range []string{"LateRows", "PollIntervalSecs"} {
		if _, ok := typ.FieldByName(gone); ok {
			t.Fatalf("Window still has %s", gone)
		}
	}
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `perl -e 'alarm shift; exec @ARGV' 600 go test -count=1 -run TestConfigValidation_WindowLatenessIsOneNumber ./internal/config/`
Expected: FAIL, `w.Lateness undefined`.

- [ ] **Step 3: Change the config type**

In `internal/config/config.go`, replace the `LateRows` and `PollIntervalSecs` fields and their comments with:

```go
	// How long after a bucket closes its rows are kept and late rows for it
	// are still accepted. A late row within this republishes the bucket as a
	// whole value, so the sink must replace by key; validate refuses a sink
	// that appends. Absent or 0: a row for a closed bucket is refused before
	// the handler and counted in window_late_rows_total. Flink's
	// allowedLateness. Measured in event time, against the watermark, like
	// everything a window decides.
	AllowedLatenessSecs int `yaml:"allowed_lateness_seconds,omitempty" jsonschema:"minimum=0"`
```

Delete `ReemitOverwrites` and `ReemitOverwritesMessage`. Add:

```go
// Lateness is allowed_lateness_seconds as a duration; zero means late rows
// are refused.
func (w Window) Lateness() time.Duration {
	return time.Duration(w.AllowedLatenessSecs) * time.Second
}
```

Update the `Window` doc comment: "the engine keeps an event-time watermark, publishes every bucket the watermark has passed to the window's sink, keeps it for allowed_lateness_seconds so a late row can republish it whole, and deletes it."

- [ ] **Step 4: Change the manager's declaration**

In `internal/managers/watermark.go`: delete `LatePolicy`, `LateReemit`, `LateDrop`, `ParseLatePolicy`. In `Declaration`, replace `Late LatePolicy` with:

```go
	// Lateness is how long after a bucket closes its rows are kept and a
	// late row republishes it whole. Zero: the rows are deleted at close and
	// the engine refuses late rows before they arrive.
	Lateness time.Duration
```

In `validate()`, replace the `ParseLatePolicy` check with `case d.Lateness < 0: return errs.New(errs.CodeConfigInvalid, "window %s: allowed_lateness_seconds cannot be negative", d.Table)`.

- [ ] **Step 5: Change the engine's spec and run's mapping**

In `internal/core/watermarks.go`, `WindowSpec` gains `Lateness time.Duration` with the comment "how long a closed bucket's rows are kept; a record whose bucket ended more than this before the watermark is refused."

In `internal/cli/run/managers.go`, `windowDeclaration` sets `Lateness: table.Window.Lateness()` instead of `Late:`; `windowSpecs` adds `Lateness: d.Lateness`. Delete the `ReemitOverwrites` block in `buildManagedTables` (lines ~160-163); Task 6 adds its replacement.

- [ ] **Step 6: Regenerate the goldens and run the config packages**

```bash
UPDATE_GOLDEN=1 perl -e 'alarm shift; exec @ARGV' 600 go test -count=1 -run TestConfigSchema_CommittedFileMatchesTheTypes ./internal/schema/
UPDATE_GOLDEN=1 perl -e 'alarm shift; exec @ARGV' 600 go test -count=1 -run TestConfigValidation_ExampleMatchesPythonOutput ./internal/cli/
perl -e 'alarm shift; exec @ARGV' 600 go test -count=1 ./internal/config/ ./internal/schema/
```
Expected: the schema golden shows `allowed_lateness_seconds` added and `late_rows`, `poll_interval_seconds` removed from the window block; config and schema pass. `internal/cli` will not pass until Task 3 updates the examples; that is expected here.

- [ ] **Step 7: Commit**

```bash
git add internal/config internal/managers/watermark.go internal/core/watermarks.go internal/cli/run/managers.go internal/validate/schemas/config.json internal/cli/testdata/config_example.golden
git commit -m "config: allowed_lateness_seconds replaces late_rows; poll_interval_seconds is gone"
```

---

### Task 3: Validate -- the bucket derivation is required, lateness needs a replacing sink, and every shipped config is updated

**Files:**
- Modify: `internal/validate/window.go`
- Modify: `internal/validate/sinks.go` (delete the `ReemitOverwrites` block)
- Modify: `internal/validate/window_test.go`, and the fixtures in `internal/validate/drain_test.go`, `internal/validate/sinks_test.go` that carry `late_rows`
- Modify: every config in Global Constraints; `README.md` window section (lines ~1185-1280 and the metrics table row for `window_late_rows`)

**Interfaces:**
- Produces: `validate` errors `window.time_column_derivation`, `window.lateness_sink`, `window.removed_key`; the interval parser `intervalSeconds(literal string) (int, bool)`.

- [ ] **Step 1: Write the failing tests**

In `internal/validate/window_test.go`, add:

```go
// A windowing pipeline's time_column must be time_bucket over event_time
// with the window's own size, because the engine decides a record's lateness
// from that bucket and a different expression would put the row somewhere
// else. This was a warning; the engine could tolerate the disagreement when
// it only swept the table. It cannot now.
func TestConfigValidation_WindowTimeColumnMustBeTimeBucketOverEventTime(t *testing.T) {
	coverage.Covers(t, "config.validation")
	base := windowedConfig() // the helper the file already has; see existing tests

	good := strings.Replace(base,
		"SELECT time_bucket(INTERVAL '1 minute', event_time) AS bucket",
		"SELECT time_bucket(INTERVAL '60 seconds', event_time) AS bucket", 1)
	rep := validateString(t, good)
	assert.Equal(t, 0, len(windowDiagnostics(rep, SeverityError)))

	for name, bad := range map[string]string{
		"payload field":   strings.Replace(base, "event_time) AS bucket", "ts) AS bucket", 1),
		"wrong size":      strings.Replace(base, "INTERVAL '1 minute'", "INTERVAL '5 minutes'", 1),
		"date_trunc":      strings.Replace(base, "time_bucket(INTERVAL '1 minute', event_time)", "date_trunc('minute', event_time)", 1),
		"no derivation":   strings.Replace(base, "time_bucket(INTERVAL '1 minute', event_time) AS bucket", "now() AS bucket", 1),
	} {
		t.Run(name, func(t *testing.T) {
			rep := validateString(t, bad)
			errs := windowDiagnostics(rep, SeverityError)
			assert.That(t, len(errs) >= 1)
			assert.That(t, strings.Contains(errs[0].Message, "time_bucket(INTERVAL '60 seconds', event_time)"))
		})
	}
}

// Lateness above zero republishes a bucket as a whole value, which an
// append-only sink holds twice. Refused, not warned: Flink's contract is the
// same. A sink validate cannot classify -- sqlcommand, clickhouse -- is
// warned, because whether it replaces is the SQL's or the table engine's
// call.
func TestConfigValidation_LatenessNeedsAReplacingSink(t *testing.T) {
	coverage.Covers(t, "config.validation")
	withLateness := func(sink string) string {
		return strings.Replace(strings.Replace(windowedConfig(),
			"idle_close_seconds: 10", "idle_close_seconds: 10\n        allowed_lateness_seconds: 300", 1),
			windowedSink(), sink, 1)
	}
	rep := validateString(t, withLateness(postgresUpsertSink()))
	assert.Equal(t, 0, len(windowDiagnostics(rep, SeverityError)))

	rep = validateString(t, withLateness(kafkaSink()))
	errs := windowDiagnostics(rep, SeverityError)
	assert.That(t, len(errs) >= 1)
	assert.That(t, strings.Contains(errs[0].Message, "allowed_lateness_seconds"))

	rep = validateString(t, withLateness(sqlcommandSink()))
	assert.Equal(t, 0, len(windowDiagnostics(rep, SeverityError)))
	assert.That(t, len(windowDiagnostics(rep, SeverityWarning)) >= 1)
}

// The removed keys fail with a message naming the replacement, so a config
// written against the old schema learns what changed rather than "unknown
// key".
func TestConfigValidation_RemovedWindowKeysNameTheirReplacement(t *testing.T) {
	coverage.Covers(t, "config.validation")
	for key, want := range map[string]string{
		"late_rows: drop":           "allowed_lateness_seconds",
		"poll_interval_seconds: 10": "the engine closes a window the moment its watermark passes",
	} {
		cfg := strings.Replace(windowedConfig(), "idle_close_seconds: 10", "idle_close_seconds: 10\n        "+key, 1)
		rep := validateString(t, cfg)
		found := false
		for _, d := range rep.Diagnostics {
			if d.Severity == SeverityError && strings.Contains(d.Message, want) {
				found = true
			}
		}
		assert.That(t, found)
	}
}
```

Check the helpers `windowedConfig`, `validateString`, `windowDiagnostics`, `windowedSink`, `postgresUpsertSink`, `kafkaSink`, `sqlcommandSink` exist in the test file; add the sink snippet helpers if not, each returning the YAML of one `sink:` block at the window's indentation. `windowedConfig` must use `time_bucket(INTERVAL '1 minute', event_time) AS bucket` and `size_seconds: 60` and no `late_rows`.

- [ ] **Step 2: Run them to verify they fail**

Run: `perl -e 'alarm shift; exec @ARGV' 600 go test -count=1 -run 'TestConfigValidation_(WindowTimeColumn|LatenessNeeds|RemovedWindowKeys)' ./internal/validate/`
Expected: FAIL (the derivation is a warning today; lateness and the removed-key messages do not exist).

- [ ] **Step 3: Implement the rules**

In `internal/validate/window.go`:

Replace the `mentionsEventTime` warning (the one #385 added, "a windowing pipeline should read event_time") with an error, using this derivation check:

```go
// timeBucketOverEventTime matches `time_bucket(INTERVAL '<literal>', event_time) AS <col>`,
// the one form a windowing pipeline's time_column may take. The engine
// buckets a record with core.BucketStart, which is time_bucket with
// DuckDB's origin, so the handler must bucket the same way or lateness is
// decided against a bucket the row is not in.
var timeBucketOverEventTime = regexp.MustCompile(
	`(?is)time_bucket\s*\(\s*INTERVAL\s+'([^']+)'\s*,\s*event_time\s*\)\s+AS\s+"?([A-Za-z_][A-Za-z0-9_]*)"?`)

// intervalSeconds reads DuckDB's quoted interval literal for the units a
// window size is written in. false for a form it does not read, which
// validate reports rather than guesses at.
func intervalSeconds(literal string) (int, bool) {
	fields := strings.Fields(strings.ToLower(strings.TrimSpace(literal)))
	if len(fields) != 2 {
		return 0, false
	}
	n, err := strconv.Atoi(fields[0])
	if err != nil || n <= 0 {
		return 0, false
	}
	unit := strings.TrimSuffix(fields[1], "s")
	per := map[string]int{"second": 1, "minute": 60, "hour": 3600, "day": 86400, "week": 604800}[unit]
	if per == 0 {
		return 0, false
	}
	return n * per, true
}

// derivesTimeColumn reports whether handlerSQL computes column as
// time_bucket over event_time with a size of sizeSeconds.
func derivesTimeColumn(handlerSQL, column string, sizeSeconds int) bool {
	for _, m := range timeBucketOverEventTime.FindAllStringSubmatch(handlerSQL, -1) {
		if !strings.EqualFold(m[2], column) {
			continue
		}
		if secs, ok := intervalSeconds(m[1]); ok && secs == sizeSeconds {
			return true
		}
	}
	return false
}
```

In the per-window loop, where the #385 warning was:

```go
			if h := conf.Pipeline.Handler; !derivesTimeColumn(h.SQL, w.TimeColumn, w.SizeSeconds) {
				fail(fmt.Sprintf("tables.sql[%d] window: time_column %q must be computed as "+
					"time_bucket(INTERVAL '%d seconds', event_time) in the handler's SQL. The engine "+
					"decides a record's lateness from that bucket, and a row bucketed any other way "+
					"is not in the bucket the engine decided against. Tell the source where the "+
					"record's time is with event_time: {path, format} if it is in the payload",
					i, w.TimeColumn, w.SizeSeconds), position(node))
			}
```

For the structured handler, the #385 rule that the table declares `event_time TIMESTAMPTZ` becomes `fail` instead of a warning, same message shape.

Add the lateness rule, replacing the two `reemit` rules (delete the `appendsOnly` reemit warning block; keep `appendsOnly`):

```go
			if w.AllowedLatenessSecs > 0 {
				switch {
				case appendsOnly(w.Sink):
					fail(fmt.Sprintf("tables.sql[%d] window: allowed_lateness_seconds is %d and the %s "+
						"sink appends. A late row republishes its bucket as a whole value, which an "+
						"append-only sink then holds twice. Use a sink that replaces by key, or set "+
						"allowed_lateness_seconds to 0 so late rows are refused",
						i, w.AllowedLatenessSecs, w.Sink.Type), position(mappingKey(node, "allowed_lateness_seconds")))
				case w.Sink.Type == "sqlcommand" || w.Sink.Type == "clickhouse":
					rep.Add(diagnostic(errs.CodeConfigInvalid, SeverityWarning, fmt.Sprintf(
						"tables.sql[%d] window: allowed_lateness_seconds is %d, so a late row republishes "+
							"its bucket as a whole value. The %s sink must replace the row for (bucket, key) "+
							"rather than add to it, which is its SQL's or its table engine's to guarantee",
						i, w.AllowedLatenessSecs, w.Sink.Type), position(mappingKey(node, "allowed_lateness_seconds"))))
				}
			}
```

Add the removed-key rule, beside the existing `manager` one:

```go
	for i, table := range tableNodes(&root) {
		win := mappingValue(table, "window")
		if key := mappingKey(win, "late_rows"); key != nil {
			fail(fmt.Sprintf("tables.sql[%d] window: late_rows is gone. Set allowed_lateness_seconds "+
				"instead: 0 refuses a row for a closed bucket (what drop did), and a positive value "+
				"keeps a closed bucket that long and republishes it whole when a late row arrives "+
				"(what reemit tried to do, without the delta). See "+
				"docs/superpowers/specs/2026-09-26-watermark-driven-close-design.md", i), position(key))
		}
		if key := mappingKey(win, "poll_interval_seconds"); key != nil {
			fail(fmt.Sprintf("tables.sql[%d] window: poll_interval_seconds is gone. The engine closes a "+
				"window the moment its watermark passes a bucket's end; nothing polls. Remove the key. See "+
				"docs/superpowers/specs/2026-09-26-watermark-driven-close-design.md", i), position(key))
		}
	}
```

Update the `manager is gone` message: replace `late_rows` in its list with `allowed_lateness_seconds`. Update the function's doc comment to list the new faults. Add `"strconv"` to imports.

In `internal/validate/sinks.go`, delete the `if table.Window.ReemitOverwrites()` block.

- [ ] **Step 4: Update every shipped configuration**

For each file, remove the `late_rows:` and `poll_interval_seconds:` lines, remove or rewrite comments that describe `reemit`/`drop`/polling, and make the handler's bucket expression `time_bucket(INTERVAL '<size>', event_time)` where `<size>` is `'1 hour'` for `size_seconds: 3600` and `'1 minute'` for `60`:

| file | bucket expression becomes | source `event_time` |
|---|---|---|
| `dev/config/examples/tumbling.window.yml` | `time_bucket(INTERVAL '1 hour', event_time) AS bucket` | read the source block: Kafka keeps the record timestamp (no block); a payload timestamp gets `event_time: {path: <field>, format: <fmt>}` |
| `dev/config/examples/kafka.stateful.window.yml` | `time_bucket(INTERVAL '1 hour', event_time) AS bucket` | Kafka record timestamp; no block |
| `dev/config/examples/mqtt.duckdb.agg.yml` | `time_bucket(INTERVAL '1 minute', event_time) AS bucket` (replacing `time_bucket(INTERVAL 1 MINUTE, TRY_CAST(ts AS TIMESTAMPTZ))`) | `mqtt: event_time: {path: ts, format: rfc3339}` -- read the example's payload comment for ts's format and use it |
| `dev/config/examples/logs.rollup.clickhouse.yml` | `time_bucket(INTERVAL '1 hour', event_time) AS bucket` | per its source; remove `late_rows: reemit` and rewrite the "read it with sum(requests)" comments: with lateness absent the bucket is published once and read as-is |
| `dev/config/examples/bluesky/bluesky.postgres.windowed.yml` | `time_bucket(INTERVAL '1 minute', event_time) AS bucket` | `websocket: event_time: {path: time_us, format: unix_us}` |
| `dev/config/examples/bluesky/bluesky.kafka.windowed.yml` | `time_bucket(INTERVAL '1 minute', event_time) as bucket` | `kafka: event_time: {path: time_us, format: unix_us}` |
| `render/pipeline.yml` | `time_bucket(INTERVAL '1 minute', event_time) AS bucket` (line ~165 has `ts`) | per its source block |
| `render/docker-compose.yml` | -- | delete both `SQLFLOW_WINDOW_POLL_SECONDS: "1"` lines |
| `dev/bench/bluesky/demo-10x*.yml` (three) | `time_bucket(INTERVAL '1 minute', event_time) AS bucket` | `event_time: {path: time_us, format: unix_us}`; delete the `poll_interval_seconds   10 -> 1` comment lines |
| `.claude/skills/slow-soak/slow.yml` | as bluesky | as bluesky |

For the bluesky postgres example, add `allowed_lateness_seconds: 300` with a comment: the sink upserts by `(bucket, lang)`, so a late row republishes the minute whole and the row is replaced -- this is the one shipped example that exercises lateness.

- [ ] **Step 5: Update the README**

In `README.md`, the window YAML block (~line 1185): replace `late_rows: drop  # or reemit; required` and `poll_interval_seconds: 10  # optional` with `allowed_lateness_seconds: 0  # optional; see Late rows`. Rewrite the **Late rows** paragraph:

> **Late rows.** A record whose bucket ended at or before the watermark is late. The engine decides that when the record arrives, from its `event_time` and the window's size, before the handler runs -- the way Flink's window operator does. With `allowed_lateness_seconds` absent or `0` a late record is refused and counted in `window_late_rows_total{outcome="refused"}`; the window table never holds it. With a positive value a closed bucket's rows are kept that long, a late record within it is written, and the bucket is republished as a **whole value** -- `emit_sql` over every row it has -- so the sink must replace by key; `sqlflow validate` refuses an append-only sink and warns for one it cannot classify. Past `end + allowed_lateness_seconds` the rows are deleted and later records are refused. Nothing here polls: the engine signals the window's manager when the watermark moved or a late row landed, and the manager publishes.

Delete the `poll_interval_seconds` sentence (~line 1253). In the metrics table, `window_late_rows_total` attributes become `window`, `outcome`.

- [ ] **Step 6: Run validate, cli and config**

```bash
perl -e 'alarm shift; exec @ARGV' 900 go test -count=1 ./internal/validate/ ./internal/config/ ./internal/cli/ > "$TMPDIR/v.txt" 2>&1; echo rc=$?; grep -E '^(ok|FAIL|--- FAIL)|_test.go:[0-9]+:' "$TMPDIR/v.txt" | grep -v COVERS
```
Expected: all three pass; `TestConfigValidation_Examples` and `TestConfigValidation_ExampleConfigsBuildRealComponents` pass on the updated examples with **no** window warnings.

- [ ] **Step 7: Commit**

```bash
git add internal/validate README.md render dev .claude/skills/slow-soak/slow.yml
git commit -m "validate: time_column must be time_bucket over event_time; lateness needs a replacing sink; every shipped window updated"
```

---

### Task 4: The engine -- lateness at arrival, and the two signals

**Files:**
- Modify: `internal/core/watermarks.go` (asserted atomics, `Classify`, `WindowSignal`)
- Modify: `internal/core/metrics.go` (`WindowLateRows`)
- Modify: `internal/core/turbine.go` (the check in the message loop, recompute accumulation, kicks after commit)
- Create: `internal/core/lateness_test.go`
- Modify: `internal/core/watermark_commit_test.go` (kick and recompute after commit; nothing after a rollback)

**Interfaces:**
- Produces:
  ```go
  // Lateness is where one record stands against one window.
  type Lateness int
  const (
  	OnTime Lateness = iota   // the bucket is open
  	LateAllowed              // the bucket closed, within lateness: write it, recompute the bucket
  	LateRefused              // beyond lateness: never written
  )
  type LateBucket struct { Window string; Bucket time.Time }
  // Classify decides every window at once for one record. Refused is true
  // only when every window refuses. Recompute lists the windows that want
  // the bucket republished. Lock-free: reads the asserted watermarks as
  // atomics. No allocation unless some window is LateAllowed.
  func (w *Watermarks) Classify(atNanos int64) (refused bool, recompute []LateBucket)
  // Signal is the per-window signal the manager waits on.
  func (w *Watermarks) Signal(name string) *WindowSignal
  type WindowSignal struct { /* kick chan struct{}; mu sync.Mutex; recompute map[time.Time]struct{} */ }
  func (s *WindowSignal) Kick()                       // non-blocking; coalesces
  func (s *WindowSignal) Recompute(bucket time.Time)  // adds to the set; caller kicks after
  func (s *WindowSignal) Wait() <-chan struct{}
  func (s *WindowSignal) TakeRecompute() []time.Time  // drains the set, sorted
  ```
- `Metrics.WindowLateRows metric.Int64Counter` -- `window_late_rows_total`, attributes `window`, `outcome` ∈ `refused`, `recomputed`.

- [ ] **Step 1: Write the failing tests**

`internal/core/lateness_test.go`:

```go
package core

import (
	"testing"
	"time"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

// One record against one window, at every boundary. One-minute buckets,
// lateness of two minutes, watermark asserted at 12:05.
//
//	record's bucket   end     end+lateness   against W=12:05   verdict
//	------------------------------------------------------------------
//	12:05             12:06   12:08          end > W            on time
//	12:04             12:05   12:07          end == W           late, allowed
//	12:03             12:04   12:06          end+l > W          late, allowed
//	12:02             12:03   12:05          end+l == W         refused
//	12:00             12:01   12:03          end+l < W          refused
func TestWindowLateness_IsDecidedAgainstTheBucketsEnd(t *testing.T) {
	coverage.Covers(t, "manager.window")
	spec := WindowSpec{Name: "w", Size: time.Minute, Grace: time.Minute, Lateness: 2 * time.Minute}
	w := NewWatermarks([]WindowSpec{spec}, time.Now)
	w.Restore("w", time.Time{}, wmT0.Add(5*time.Minute))

	for _, c := range []struct {
		at      time.Duration
		refused bool
		recomp  int
	}{
		{5*time.Minute + 30*time.Second, false, 0},
		{4*time.Minute + 59*time.Second, false, 1},
		{3 * time.Minute, false, 1},
		{2*time.Minute + 59*time.Second, true, 0},
		{30 * time.Second, true, 0},
	} {
		refused, recompute := w.Classify(wmT0.Add(c.at).UnixNano())
		assert.Equal(t, c.refused, refused)
		assert.Equal(t, c.recomp, len(recompute))
		if c.recomp == 1 {
			assert.Equal(t, BucketStart(wmT0.Add(c.at), time.Minute), recompute[0].Bucket)
			assert.Equal(t, "w", recompute[0].Window)
		}
	}
}

// With no lateness there is no allowed band: a closed bucket's row is refused.
// And before anything is asserted nothing is late.
func TestWindowLateness_ZeroLatenessRefusesAtTheWatermark(t *testing.T) {
	coverage.Covers(t, "manager.window")
	w := NewWatermarks([]WindowSpec{{Name: "w", Size: time.Minute, Grace: time.Minute}}, time.Now)
	refused, _ := w.Classify(wmT0.UnixNano())
	assert.That(t, !refused)

	w.Restore("w", time.Time{}, wmT0.Add(5*time.Minute))
	refused, recompute := w.Classify(wmT0.Add(4*time.Minute).UnixNano()) // end 12:05 == W
	assert.That(t, refused)
	assert.Equal(t, 0, len(recompute))
	refused, _ = w.Classify(wmT0.Add(5*time.Minute).UnixNano()) // end 12:06 > W
	assert.That(t, !refused)
}

// Two windows: a record is refused only when every window refuses it. One
// that a strict window refuses and a loose window accepts is admitted, and
// the strict window's next purge deletes it.
func TestWindowLateness_RefusedOnlyWhenEveryWindowRefuses(t *testing.T) {
	coverage.Covers(t, "manager.window")
	strict := WindowSpec{Name: "strict", Size: time.Minute, Grace: 0}
	loose := WindowSpec{Name: "loose", Size: time.Hour, Grace: 0, Lateness: time.Hour}
	w := NewWatermarks([]WindowSpec{strict, loose}, time.Now)
	w.Restore("strict", time.Time{}, wmT0.Add(10*time.Minute))
	w.Restore("loose", time.Time{}, wmT0.Add(10*time.Minute))

	refused, recompute := w.Classify(wmT0.Add(time.Minute).UnixNano())
	assert.That(t, !refused)
	// loose: the hour bucket 12:00 ends 13:00 > 12:10: on time. strict refuses.
	assert.Equal(t, 0, len(recompute))
}

// A record that is unstamped is never late: it has no bucket.
func TestWindowLateness_AnUnstampedRecordIsNotLate(t *testing.T) {
	coverage.Covers(t, "manager.window")
	w := NewWatermarks([]WindowSpec{{Name: "w", Size: time.Minute}}, time.Now)
	w.Restore("w", time.Time{}, wmT0.Add(time.Hour))
	refused, recompute := w.Classify(0)
	assert.That(t, !refused)
	assert.Equal(t, 0, len(recompute))
}

// The signal: a kick coalesces, a recompute set drains sorted and once.
func TestWindowSignal_KicksCoalesceAndRecomputesDrainOnce(t *testing.T) {
	coverage.Covers(t, "manager.window")
	w := NewWatermarks([]WindowSpec{{Name: "w", Size: time.Minute}}, time.Now)
	s := w.Signal("w")
	s.Kick()
	s.Kick()
	s.Kick()
	<-s.Wait()
	select {
	case <-s.Wait():
		t.Fatal("three kicks before a wait are one kick")
	default:
	}
	s.Recompute(wmT0.Add(2 * time.Minute))
	s.Recompute(wmT0)
	s.Recompute(wmT0.Add(2 * time.Minute))
	got := s.TakeRecompute()
	assert.Equal(t, 2, len(got))
	assert.Equal(t, wmT0, got[0])
	assert.Equal(t, 0, len(s.TakeRecompute()))
}
```

Add to `internal/core/watermark_commit_test.go`:

```go
// The kick follows the commit, never precedes it: a manager woken by it
// reads the watermark the commit wrote. A late-but-allowed record's bucket
// reaches the recompute set at the same moment. A batch that rolls back
// kicks nothing and publishes no bucket.
func TestStateDurability_TheSignalsFollowTheCommit(t *testing.T) {
	coverage.Covers(t, "state.durability")
	ctx := context.Background()
	st := openWindowedState(t)
	clk := &wmClock{at: wmT0}
	w := NewWatermarks([]WindowSpec{{Name: "win", Size: time.Minute, Grace: time.Minute, Lateness: 10 * time.Minute}}, clk.now)
	saver := &failingSaver{WatermarkSaver: NewWatermarkStore(st.conn)}
	tb := NewTurbine(newIdleSource(), &fakeHandler{}, &fakeSink{}, 1000, time.Hour,
		&sync.Mutex{}, PipelineErrorPolicies{},
		WithStateStore(st.offs, st.tx), WithProgressStore(NewProgressStore(st.conn)),
		WithWindows(w, saver), WithWatermarkWriteInterval(0), WithClock(clk.now))
	sig := w.Signal("win")

	// A batch that moves the watermark: no kick until the commit.
	w.Observe("", 0, wmT0.Add(5*time.Minute).UnixNano())
	select {
	case <-sig.Wait():
		t.Fatal("kicked before the commit")
	default:
	}
	assert.NoError(t, tb.commitState(ctx, progressOnInterval))
	<-sig.Wait()
	at, _ := st.committedWatermark(t)
	assert.Equal(t, wmT0.Add(4*time.Minute), at)

	// A late-but-allowed record: its bucket is in the set after the commit,
	// with a kick.
	tb.noteLate([]LateBucket{{Window: "win", Bucket: wmT0}})
	assert.Equal(t, 0, len(sig.TakeRecompute()))
	assert.NoError(t, tb.commitState(ctx, progressOnInterval))
	<-sig.Wait()
	assert.Equal(t, []time.Time{wmT0}, sig.TakeRecompute())

	// A batch whose commit fails: no kick, no bucket.
	tb.noteLate([]LateBucket{{Window: "win", Bucket: wmT0.Add(time.Minute)}})
	w.Observe("", 0, wmT0.Add(9*time.Minute).UnixNano())
	saver.fails = 1
	assert.Error(t, tb.commitState(ctx, progressOnInterval))
	select {
	case <-sig.Wait():
		t.Fatal("kicked after a rollback")
	default:
	}
	assert.Equal(t, 0, len(sig.TakeRecompute()))
}
```

- [ ] **Step 2: Run them to verify they fail**

Run: `perl -e 'alarm shift; exec @ARGV' 600 go test -count=1 -run 'TestWindowLateness|TestWindowSignal|TestStateDurability_TheSignalsFollowTheCommit' ./internal/core/`
Expected: FAIL, `Classify`, `Signal`, `noteLate` undefined.

- [ ] **Step 3: Implement in `watermarks.go`**

Add the types and methods. The asserted value per window becomes an atomic beside `stored` (keep `stored` under the mutex as the source of truth for `Next`/`Commit`; mirror into the atomic in `Commit` and `Restore`):

```go
	// asserted mirrors stored, one atomic per spec in spec order, for
	// Classify: it runs per record on the consume loop and must not take mu.
	asserted []atomic.Int64
	signals  []*WindowSignal
```

In `NewWatermarks`: `asserted: make([]atomic.Int64, len(specs))`, `signals` built with `newWindowSignal()` each. In `Commit` and `Restore`, after updating `stored[name]`, `w.asserted[i].Store(nanos)` for the spec index (add a `specIndex map[string]int` built in `NewWatermarks`).

```go
// Lateness is where one record stands against one window. Decided at
// arrival from the record's bucket and the asserted watermark, as Flink's
// window operator decides it, and nowhere else: a row below the watermark is
// in the table only because this said it could be.
type Lateness int

const (
	OnTime      Lateness = iota
	LateAllowed          // the bucket closed, within lateness: written, and the bucket recomputed
	LateRefused          // beyond lateness: never written, counted
)

// LateBucket is a window and the bucket a late-but-allowed record landed in.
type LateBucket struct {
	Window string
	Bucket time.Time
}

// Classify decides every window at once for one record. refused is true only
// when every window refuses it, so a pipeline with two windows of different
// sizes keeps a record the looser one still wants; the stricter one's purge
// deletes it from that table on its next pass. recompute is the windows that
// admitted it late and want the bucket republished. A record with no event
// time has no bucket and is never late.
//
// Lock-free and allocation-free on the common path: the asserted watermarks
// are atomics, and recompute is nil until a window admits a late record.
func (w *Watermarks) Classify(atNanos int64) (refused bool, recompute []LateBucket) {
	if atNanos <= 0 || len(w.specs) == 0 {
		return false, nil
	}
	at := time.Unix(0, atNanos)
	refused = true
	for i, spec := range w.specs {
		asserted := w.asserted[i].Load()
		if asserted == 0 {
			refused = false
			continue
		}
		end := BucketEnd(at, spec.Size).UnixNano()
		switch {
		case end+int64(spec.Lateness) <= asserted:
			// refused by this window
		case end <= asserted:
			refused = false
			recompute = append(recompute, LateBucket{Window: spec.Name, Bucket: BucketStart(at, spec.Size)})
		default:
			refused = false
		}
	}
	return refused, recompute
}

// WindowSignal is what the engine tells one window's manager: that the
// watermark moved, and which closed buckets a late row landed in.
type WindowSignal struct {
	kick      chan struct{}
	mu        sync.Mutex
	recompute map[time.Time]struct{}
}

func newWindowSignal() *WindowSignal {
	return &WindowSignal{kick: make(chan struct{}, 1), recompute: map[time.Time]struct{}{}}
}

// Kick wakes the manager. Non-blocking, capacity one: two kicks before the
// manager wakes are one kick, because a pass reads the current state rather
// than the event.
func (s *WindowSignal) Kick() {
	select {
	case s.kick <- struct{}{}:
	default:
	}
}

// Wait is the channel a manager selects on.
func (s *WindowSignal) Wait() <-chan struct{} { return s.kick }

// Recompute records a bucket a late-but-allowed row landed in. A set, so a
// burst of late rows for one bucket is one republish. The caller kicks after
// the commit that made the row visible.
func (s *WindowSignal) Recompute(bucket time.Time) {
	s.mu.Lock()
	s.recompute[bucket] = struct{}{}
	s.mu.Unlock()
}

// TakeRecompute drains the set, oldest bucket first.
func (s *WindowSignal) TakeRecompute() []time.Time {
	s.mu.Lock()
	out := make([]time.Time, 0, len(s.recompute))
	for b := range s.recompute {
		out = append(out, b)
	}
	s.recompute = map[time.Time]struct{}{}
	s.mu.Unlock()
	sort.Slice(out, func(i, j int) bool { return out[i].Before(out[j]) })
	return out
}

// Signal is the window's signal, for the manager built over it.
func (w *Watermarks) Signal(name string) *WindowSignal {
	i, ok := w.specIndex[name]
	if !ok {
		return nil
	}
	return w.signals[i]
}
```

- [ ] **Step 4: Implement in `metrics.go` and `turbine.go`**

`metrics.go`: add `WindowLateRows metric.Int64Counter` after `MessagesUnplaceable` with the comment "Records late for a window, by outcome: refused (beyond allowed_lateness_seconds; never written) or recomputed (within it; written, and the bucket republished whole). The data-loss half is refused." Register it as `window_late_rows_total`, unit `{row}`, description "Rows late for a window: refused beyond allowed_lateness_seconds, or recomputed within it".

`turbine.go`:

Fields: `pendingLate []LateBucket` (this batch's late-but-allowed buckets; consume loop only), and cached attribute sets `lateRefusedAttrs`, `lateRecomputedAttrs map[string][]metric.AddOption` built lazily per window name (the first record for a window allocates once).

In the message loop, immediately after the placement check and **before** `notePlaced`:

```go
			if t.windows != nil {
				refused, late := t.windows.Classify(raw.EventAtNanos)
				if refused {
					// Late beyond allowed_lateness_seconds for every window:
					// the bucket it belongs to closed and will not be
					// republished, so the row has nowhere to go. Refused before
					// the handler, like an unplaceable record, and for the
					// same reason: a row the window's promise excludes must
					// not reach the table. Consumed, marked, counted.
					t.noteRefusedLate(ctx, raw)
					t.mark(raw)
					totalConsumed++
					t.stats.SetNumMessagesConsumed(totalConsumed)
					if maxMsgs > 0 && totalConsumed >= int64(maxMsgs) {
						hitMax = true
						break
					}
					continue
				}
				if len(late) > 0 {
					t.noteLate(late)
				}
			}
```

Helpers:

```go
// noteRefusedLate counts a record refused as late, per window that refused
// it, and logs the condition once per run.
func (t *Turbine) noteRefusedLate(ctx context.Context, m Message) {
	for _, spec := range t.windows.Specs() {
		t.metrics.WindowLateRows.Add(ctx, 1, t.lateAttrs(spec.Name, "refused")...)
	}
	if !t.lateRefusedLogged {
		t.lateRefusedLogged = true
		t.logger.Warn("refusing records late beyond allowed_lateness_seconds",
			zap.Time("event_time", time.Unix(0, m.EventAtNanos).UTC()))
	}
}

// noteLate remembers the buckets a late-but-allowed record landed in, for
// the commit to hand to the windows' signals. Consume loop only; no lock.
func (t *Turbine) noteLate(late []LateBucket) {
	t.pendingLate = append(t.pendingLate, late...)
}
```

`Specs()` is a new accessor on `Watermarks` returning `w.specs`. `lateAttrs(window, outcome)` returns a cached `[]metric.AddOption{metric.WithAttributes(attribute.String("window", window), attribute.String("outcome", outcome))}`.

In `commitState`, in **both** branches, immediately after `t.windows.Commit(moved)` (i.e. only on a successful commit):

```go
		t.signalWindows(ctx, moved)
```

```go
// signalWindows tells each window's manager what this commit changed: a kick
// for every window whose watermark moved, and the recompute buckets this
// batch's late rows landed in, followed by a kick. After the commit, never
// before, so a manager woken here reads what the commit wrote. Called only
// when the commit succeeded; a rollback leaves pendingLate for the replay,
// which re-derives it.
func (t *Turbine) signalWindows(ctx context.Context, moved map[string]time.Time) {
	if t.windows == nil {
		return
	}
	kicked := map[string]bool{}
	for name := range moved {
		t.windows.Signal(name).Kick()
		kicked[name] = true
	}
	for _, lb := range t.pendingLate {
		sig := t.windows.Signal(lb.Window)
		sig.Recompute(lb.Bucket)
		t.metrics.WindowLateRows.Add(ctx, 1, t.lateAttrs(lb.Window, "recomputed")...)
		if !kicked[lb.Window] {
			sig.Kick()
			kicked[lb.Window] = true
		}
	}
	t.pendingLate = t.pendingLate[:0]
}
```

On the rollback paths of `commitState` (progress refused, watermark write failed, offsets failed, commit failed) add `t.pendingLate = t.pendingLate[:0]` so a replay does not double-record; the replay re-classifies the same records.

The recompute counter is recorded at commit, not at arrival, so a rolled-back batch counts nothing -- the same rule the manager's counter used to follow.

- [ ] **Step 5: Run the core package**

Run: `perl -e 'alarm shift; exec @ARGV' 900 go test -count=1 ./internal/core/ > "$TMPDIR/c.txt" 2>&1; echo rc=$?; grep -E '^(ok|FAIL|--- FAIL)|_test.go:[0-9]+:' "$TMPDIR/c.txt" | grep -v COVERS`
Expected: PASS, including the benchmarks compiling. Then `-race` on the same package.

- [ ] **Step 6: Re-run the write-path benchmark**

Run: `go test ./internal/core/ -run '^$' -bench 'BenchmarkConsumeLoopWindowedWritePath' -benchmem -benchtime 300000x -count 3`
Expected: `windows_on` within noise of `windows_off` (both ~40 ns) and no new allocations. `Classify` is a division and two compares per window per record; if it shows, the atomics or the bucket arithmetic is wrong.

- [ ] **Step 7: Commit**

```bash
git add internal/core
git commit -m "core: lateness is decided at arrival, and the engine signals the manager after the commit"
```

---

### Task 5: The manager -- one pass, three occasions, no clock

**Files:**
- Modify: `internal/managers/watermark.go` (`Pass`, `Start`, constructor, options, metrics)
- Modify: `internal/managers/decide.go` (bucket table)
- Modify: `internal/managers/sql.go`
- Modify: `internal/managers/watermark_test.go`, `decide_test.go`, `helpers_test.go`, `window_leak_test.go`, `window_leakloop_test.go`, `sql_test.go`
- Regenerate: `docs/windows/decisions.md`

**Interfaces:**
- Produces: `func NewWatermark(conn adbc.Connection, d Declaration, sink core.Sink, signal *core.WindowSignal, opts ...Option) (*Watermark, error)`; `func (w *Watermark) Pass(ctx context.Context) error`; `func (w *Watermark) Start(ctx context.Context) error`.
- Removes: `Poll`, `WithPollTrigger`, `defaultPollInterval`, the `poll` argument, `LatePolicy` and friends (Task 2 began this), `WindowMetrics.Late`; adds `WindowMetrics.Recomputed` (`window_recomputes`, counter, "Buckets republished whole because a late row arrived within allowed_lateness_seconds").

- [ ] **Step 1: Write the failing tests**

Rewrite `internal/managers/watermark_test.go`'s late-row tests and add the new ones. Keep every test that is about closing on the assertion (`ClosesUpToTheAssertedWatermark`, `AnAssertionPastEveryBucketClosesEverything`, `AnUnassertedWindowNeverCloses`, `AnAssertionOverAnEmptyTableIsSaved`, `WatermarkNeverRegresses`, `ARestartResumesFromTheWatermark`, the lag tests, `UncommittedRowsAreNotPublished`, `AFailedFlushLeavesEverything`, `AnEmptyPollDoesNotFreezeTheView`, `EmitSQLShapesTheClosedRows`, `MetricsReportTheWatermark`, `FinalPollStopsAtTheDrainDeadline`), renaming `Poll` to `Pass` and dropping `decl.Late`. Delete `LateRowsFollowThePolicy`, `LateRowsAreCountedOnlyWhenTheCloseCommits`, `ReemitPublishesTheLateRowsAlone`. Add:

```go
// With lateness, a closed bucket's rows stay until the watermark passes
// end + lateness, and a recompute republishes the whole bucket: the sink's
// last value for it is the exact count, not the late rows alone.
//
//	step                         table          closed   published (last value for bucket 0)
//	----------------------------------------------------------------------------------------
//	buckets 0 (3 rows), 2 (1)    0:3, 2:1       -        -
//	assert 12:01, pass           0:3, 2:1       12:01    bucket 0 → 3        rows kept: lateness 5m
//	late row for 0, recompute    0:4, 2:1       12:01    bucket 0 → 4        the whole bucket, again
//	assert 12:07, pass           2:1            12:07    bucket 0 expired: 12:01 + 5m <= 12:07, deleted
func TestManagerWindow_ARecomputeRepublishesTheWholeBucket(t *testing.T) {
	coverage.Covers(t, "manager.window")
	ctx := context.Background()
	d := newTestDB(t, "")
	createWindowTable(t, d.pipeline)
	sink := &recordingSink{}
	decl := testDecl()
	decl.Lateness = 5 * time.Minute
	decl.EmitSQL = "SELECT bucket, sum(count)::INT AS total FROM closed GROUP BY ALL"
	w, sig := newTestWatermark(t, d, decl, sink)

	insertBucket(t, d.pipeline, 0, "NYC", 3)
	insertBucket(t, d.pipeline, 2, "NYC", 1)
	assertAt(t, d.pipeline, bucket(1))
	assert.NoError(t, w.Pass(ctx))
	assert.DeepEqual(t, [][]string{{"3"}}, lastColumn(sink.published()))
	assert.Equal(t, int64(2), countRows(t, d.pipeline, testTable)) // bucket 0 retained

	insertBucket(t, d.pipeline, 0, "late", 1)
	sig.Recompute(bucket(0))
	assert.NoError(t, w.Pass(ctx))
	assert.DeepEqual(t, [][]string{{"3"}, {"4"}}, lastColumn(sink.published()))

	assertAt(t, d.pipeline, bucket(7))
	assert.NoError(t, w.Pass(ctx))
	assert.Equal(t, int64(0), countRows(t, d.pipeline, testTable)) // 0 expired, 2 published+expired
}

// With no lateness the close deletes what it publishes, as before.
func TestManagerWindow_WithoutLatenessACloseDeletesWhatItPublishes(t *testing.T) {
	coverage.Covers(t, "manager.window")
	ctx := context.Background()
	d := newTestDB(t, "")
	createWindowTable(t, d.pipeline)
	sink := &recordingSink{}
	w, _ := newTestWatermark(t, d, testDecl(), sink)
	insertBucket(t, d.pipeline, 0, "NYC", 3)
	insertBucket(t, d.pipeline, 2, "NYC", 1)
	assertAt(t, d.pipeline, bucket(1))
	assert.NoError(t, w.Pass(ctx))
	assert.Equal(t, int64(1), countRows(t, d.pipeline, testTable))
}

// Start runs one pass on start, one on every kick, and one on the drain;
// nothing wakes it otherwise. A test with no signal for a minute sees no
// pass, then a kick sees one.
func TestManagerWindow_StartPassesOnStartKickAndDrain(t *testing.T) {
	coverage.Covers(t, "manager.window")
	d := newTestDB(t, "")
	createWindowTable(t, d.pipeline)
	sink := &recordingSink{}
	w, sig := newTestWatermark(t, d, testDecl(), sink)
	insertBucket(t, d.pipeline, 0, "NYC", 3)
	insertBucket(t, d.pipeline, 2, "NYC", 1)
	assertAt(t, d.pipeline, bucket(1))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Start(ctx) }()

	// The start pass publishes bucket 0.
	waitFor(t, "the start pass", 5*time.Second, func() bool { r, _ := sink.counts(); return r == 1 })
	insertBucket(t, d.pipeline, 3, "NYC", 1)
	assertAt(t, d.pipeline, bucket(3))
	time.Sleep(100 * time.Millisecond)
	rows, _ := sink.counts()
	assert.Equal(t, int64(1), rows) // nothing woke it
	sig.Kick()
	waitFor(t, "the kicked pass", 5*time.Second, func() bool { r, _ := sink.counts(); return r == 2 })

	insertBucket(t, d.pipeline, 5, "NYC", 1)
	assertAt(t, d.pipeline, bucket(5))
	cancel()
	assert.NoError(t, <-done)
	rows, _ = sink.counts()
	assert.Equal(t, int64(3), rows) // the drain pass
}

// A context already cancelled when Start is called runs the drain pass only:
// the shape a shutdown that raced startup has, and the one the conformance
// harness's drain check builds.
func TestManagerWindow_StartOnACancelledContextDrainsOnly(t *testing.T) {
	coverage.Covers(t, "manager.window")
	d := newTestDB(t, "")
	createWindowTable(t, d.pipeline)
	sink := &recordingSink{}
	w, _ := newTestWatermark(t, d, testDecl(), sink)
	insertBucket(t, d.pipeline, 0, "NYC", 3)
	insertBucket(t, d.pipeline, 2, "NYC", 1)
	assertAt(t, d.pipeline, bucket(1))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	assert.NoError(t, w.Start(ctx))
	_, flushes := sink.counts()
	assert.Equal(t, 1, flushes)
}
```

`newTestWatermark` returns `(*Watermark, *core.WindowSignal)`: it builds `core.NewWatermarks([]core.WindowSpec{{Name: testTable, Size: decl.Size, Grace: decl.Grace, IdleClose: decl.IdleClose, Lateness: decl.Lateness}}, time.Now)` and passes `.Signal(testTable)`. Add `lastColumn(rows [][]string) [][]string` returning each row's last cell, beside `firstColumn`.

Update `decide_test.go`: the examples become bucket examples over `(end, closed, hadClosed, asserted, lateness)`; the renderer's bucket section lists `open`, `due`, `retained`, `expired`; the table checker's expected messages change to the bucket table's names; the invariants section drops nothing (it is about the watermark) and adds one line for `window.lateness_decided_at_arrival`.

- [ ] **Step 2: Run them to verify they fail**

Run: `perl -e 'alarm shift; exec @ARGV' 600 go vet ./internal/managers/`
Expected: compile errors for `Pass`, the constructor signature, `Lateness`.

- [ ] **Step 3: Implement `sql.go`**

Replace `countClosedSQL`/`deleteClosedSQL`/`collectSQL` with:

```go
// dueBetween is the predicate for buckets that close in this pass: ended
// after the closed watermark and at or before the asserted one.
func (d Declaration) dueBetween(closed time.Time, hadClosed bool, asserted time.Time) string {
	if !hadClosed {
		return d.closedBefore(asserted)
	}
	return fmt.Sprintf("%s + INTERVAL '%d' SECOND > TIMESTAMPTZ '%s' AND %s",
		quoteIdent(d.TimeColumn), int64(d.Size/time.Second), core.UTCLiteral(closed), d.closedBefore(asserted))
}

// bucketIs is the predicate for one bucket's rows.
func (d Declaration) bucketIs(bucket time.Time) string {
	return fmt.Sprintf("%s = TIMESTAMPTZ '%s'", quoteIdent(d.TimeColumn), core.UTCLiteral(bucket))
}

// expiredBefore is the predicate for buckets past their lateness: ended at
// or before asserted − lateness. With no lateness that is every bucket the
// pass publishes, so the close deletes what it published, as it always did.
func (d Declaration) expiredBefore(asserted time.Time) string {
	return d.closedBefore(asserted.Add(-d.Lateness))
}

func (d Declaration) countSQL(where string) string {
	return fmt.Sprintf("SELECT count(*)::BIGINT FROM %s WHERE %s", quoteIdent(d.Table), where)
}

func (d Declaration) deleteSQL(where string) string {
	return fmt.Sprintf("DELETE FROM %s WHERE %s", quoteIdent(d.Table), where)
}

// collectSQL is what the sink receives: emit_sql with the closed relation
// spliced in front of it as a CTE, over the rows where selects.
func (d Declaration) collectSQL(where string) string {
	closed := fmt.Sprintf("%s AS (SELECT * FROM %s WHERE %s)", closedView, quoteIdent(d.Table), where)
	emit := strings.TrimSpace(d.EmitSQL)
	if emit == "" {
		emit = defaultEmitSQL
	}
	if len(emit) >= 5 && strings.EqualFold(emit[:4], "WITH") && isSpace(emit[4]) {
		return "WITH " + closed + ", " + strings.TrimSpace(emit[5:])
	}
	return "WITH " + closed + " " + emit
}
```

Keep `closedBefore`, `newestSQL`, `oldestSQL`. Update `sql_test.go` for the new names and add one assertion each for `dueBetween`, `bucketIs`, `expiredBefore` with `Lateness: 5*time.Minute`.

- [ ] **Step 4: Implement `decide.go`'s bucket table**

Replace `Bucket`, `BucketState`, `BucketAction`, the bucket table, `BucketStateOf`, `DecideBucket`, `bucketValues`, `policyValues`, `allBucketStates`:

```go
// Bucket is where one bucket stands against the closed watermark, the
// asserted one, and the window's lateness.
type Bucket string

const (
	// BucketOpen ends after the asserted watermark.
	BucketOpen Bucket = "open"
	// BucketDue ends after the closed watermark and at or before the asserted
	// one: it closes in this pass.
	BucketDue Bucket = "due"
	// BucketRetained ended at or before the closed watermark and its lateness
	// has not run out: published, kept for a late row to republish whole.
	BucketRetained Bucket = "retained"
	// BucketExpired ended at or before asserted − lateness: its rows go.
	BucketExpired Bucket = "expired"
)

type BucketState struct{ Bucket Bucket }

func (b BucketState) String() string { return fmt.Sprintf("bucket=%s", b.Bucket) }

type BucketAction string

const (
	Keep    BucketAction = "keep"
	Publish BucketAction = "publish"
	Retain  BucketAction = "retain"
	Purge   BucketAction = "purge"
)

// BucketStateOf reduces one bucket's end to its fact. A due bucket with no
// lateness is also expired; the pass publishes first, then purges, so the
// order of the two actions is the pass's and not the table's.
func BucketStateOf(decl Declaration, end, closed time.Time, hadClosed bool, asserted time.Time) BucketState {
	switch {
	case end.After(asserted):
		return BucketState{BucketOpen}
	case !end.Add(decl.Lateness).After(asserted) && (!hadClosed || !end.After(closed)):
		return BucketState{BucketExpired}
	case hadClosed && !end.After(closed):
		return BucketState{BucketRetained}
	default:
		return BucketState{BucketDue}
	}
}

var bucketTable = []bucketRule{
	{Name: "keep", Bucket: []Bucket{BucketOpen}, Action: Keep, Deciding: "bucket",
		Claim: "The bucket ends after the asserted watermark, so its rows stay."},
	{Name: "publish", Bucket: []Bucket{BucketDue}, Action: Publish, Deciding: "bucket",
		Claim: "The bucket ends between the closed watermark and the asserted one: emit_sql runs over its rows and the result is published. With allowed_lateness_seconds the rows stay for a late row to republish it whole; without, this pass purges them too."},
	{Name: "retain", Bucket: []Bucket{BucketRetained}, Action: Retain, Deciding: "bucket",
		Claim: "The bucket has been published and its lateness has not run out: its rows stay, and a late row the engine admits republishes it whole."},
	{Name: "purge", Bucket: []Bucket{BucketExpired}, Action: Purge, Deciding: "bucket",
		Claim: "The bucket ended at or before the asserted watermark less the lateness: nothing more can arrive for it, and its rows are deleted."},
}
```

`bucketRule` loses `Policy`. `allBucketStates` enumerates the four. The watermark table (`hold.unasserted`, `hold.behind`, `follow`) is unchanged.

- [ ] **Step 5: Implement `watermark.go`**

The struct loses `poll`, `pollTrigger`; gains `signal *core.WindowSignal`. Delete `WithPollTrigger`, `defaultPollInterval`, `LatePolicy` and friends (if Task 2 left stubs). `WindowMetrics`: replace `Late` with `Recomputed metric.Int64Counter` (`window_recomputes`).

```go
// NewWatermark builds the manager for one window on conn, which must be a
// connection of its own with autocommit off: every pass ends with a commit
// or a rollback on it. signal is the window's, from core.Watermarks; nil is
// allowed for a caller that drives Pass itself.
func NewWatermark(conn adbc.Connection, d Declaration, sink core.Sink, signal *core.WindowSignal, opts ...Option) (*Watermark, error)

// Start runs one pass, then one on every kick, then one on the drain. It has
// no clock: the engine kicks when the watermark moved or a late row landed,
// and the engine's own flush tick is the only timer in the design. A context
// already cancelled runs the drain pass only.
//
// A failed pass returns, as before: the sink ran its retry ladder, and a
// window the destination will not take is not retried every kick. A write
// conflict with the pipeline is the exception, and the next kick retries it.
func (w *Watermark) Start(ctx context.Context) error {
	w.logger.Info("starting watermark manager", zap.String("table", w.decl.Table))
	if ctx.Err() != nil {
		return w.finalPass()
	}
	if err := w.Pass(ctx); err != nil && ctx.Err() == nil && !isConflict(err) {
		return fmt.Errorf("watermark manager %s: %w", w.decl.Table, err)
	}
	var wake <-chan struct{}
	if w.signal != nil {
		wake = w.signal.Wait()
	}
	for {
		select {
		case <-wake:
			err := w.Pass(ctx)
			if err != nil && ctx.Err() == nil {
				if isConflict(err) {
					w.logger.Warn("pass conflicted with the pipeline, retrying on the next kick", zap.Error(err))
					continue
				}
				w.logger.Error("pass failed, stopping the manager", zap.Error(err))
				return fmt.Errorf("watermark manager %s: %w", w.decl.Table, err)
			}
			if ctx.Err() == nil {
				continue
			}
		case <-ctx.Done():
		}
		return w.finalPass()
	}
}
```

`finalPass` is `finalPoll` renamed. `Pass`:

```go
// Pass is one unit of the manager's work: publish every bucket the
// assertion closed, republish every bucket a late row landed in, delete
// every bucket past its lateness, and record where the window has closed up
// to. It ends its transaction before returning, committed or rolled back.
func (w *Watermark) Pass(ctx context.Context) (err error) {
	committed := false
	var candidate, settled time.Time
	var candidateKnown, settledKnown bool
	defer func() {
		if !committed {
			if rbErr := w.tx.Rollback(context.WithoutCancel(ctx)); rbErr != nil && err == nil {
				err = fmt.Errorf("rolling back: %w", rbErr)
			}
		}
		if candidateKnown && settledKnown {
			w.recordCloseLag(context.WithoutCancel(ctx), candidate, settled)
		}
	}()

	closed, hadClosed, err := w.store.Load(ctx, w.decl.Table)
	if err != nil {
		return err
	}
	if hadClosed {
		settled, settledKnown = closed, true
	}
	asserted, hasAsserted, err := core.LoadWatermark(ctx, w.conn, w.decl.Table)
	if err != nil {
		return fmt.Errorf("reading the asserted watermark: %w", err)
	}
	if hasAsserted {
		candidate, candidateKnown = asserted, true
	}

	// Observation only: the gauges. (newestSQL / oldestSQL block as today.)

	// The buckets a late row landed in since the last pass. Taken before the
	// decision, so a kick that carried only recomputes still does them.
	var recompute []time.Time
	if w.signal != nil {
		recompute = w.signal.TakeRecompute()
	}

	state := StateOf(asserted, hasAsserted, closed, hadClosed)
	rule := watermarkRuleFor(state)
	watermark, moved := rule.Action.Next(asserted, closed)
	if !moved && len(recompute) == 0 {
		return nil
	}
	if !moved {
		watermark = closed
	}

	// Publish what is due. Published before anything is deleted, so a flush
	// that fails leaves the rows for the next pass.
	if moved {
		due := w.decl.dueBetween(closed, hadClosed, watermark)
		n, _, err := queryInt64(ctx, w.conn, w.decl.countSQL(due))
		if err != nil {
			return fmt.Errorf("counting due rows: %w", err)
		}
		if n > 0 {
			if err := w.publish(ctx, due); err != nil {
				return err
			}
			w.metrics.Closed.Add(ctx, 1, w.attrs)
		}
	}
	// Republish, whole, every bucket a late row landed in.
	for _, b := range recompute {
		if err := w.publish(ctx, w.decl.bucketIs(b)); err != nil {
			return err
		}
		w.metrics.Recomputed.Add(ctx, 1, w.attrs)
	}
	// Purge what is past its lateness. With no lateness this is what the
	// pass just published.
	if _, err := execRows(ctx, w.conn, w.decl.deleteSQL(w.decl.expiredBefore(watermark))); err != nil {
		return fmt.Errorf("deleting expired rows: %w", err)
	}
	if err := w.store.Save(ctx, w.decl.Table, watermark, time.Now().UTC()); err != nil {
		return err
	}
	if err := w.tx.Commit(ctx); err != nil {
		return errs.Wrap(errs.CodeStateCommitFailed, err, "committing the pass")
	}
	committed = true
	settled, settledKnown = watermark, true
	w.metrics.Watermark.Record(ctx, watermark.Unix(), w.attrs)
	return nil
}
```

`publish(ctx, where string)` collects `collectSQL(where)`, writes and flushes, as today's `publish` does with its predicate parameter changed. `w.attrs` is the cached `metric.WithAttributes(attribute.String("window", table))`.

A recompute whose pass fails after the set was taken would lose the buckets: `TakeRecompute` happens inside the pass, so on failure re-add them -- in the deferred block, `if !committed && w.signal != nil { for _, b := range recompute { w.signal.Recompute(b) } }`. Declare `recompute` before the defer.

- [ ] **Step 6: Update the tests and helpers; regenerate the decisions page**

`helpers_test.go`: `newTestWatermark` as described; `testDecl()` drops `Late`. `window_leak_test.go` and `window_leakloop_test.go`: `NewWatermark(mconn, decl, sink, nil)`; `m.Poll` → `m.Pass`; the leak test's `declaration()` drops `Late`. `conformance_test.go` is Task 7's; leave it failing to compile until then only if the package still builds -- it will not, so update it in this task to the new `ManagerSubject` shape defined in Task 7 (both tasks land in one PR; do them together if that is simpler, and commit once).

```bash
UPDATE_GOLDEN=1 perl -e 'alarm shift; exec @ARGV' 600 go test -count=1 -run ThePublishedDecisions ./internal/managers/
perl -e 'alarm shift; exec @ARGV' 900 go test -count=1 ./internal/managers/ > "$TMPDIR/m.txt" 2>&1; echo rc=$?; grep -E '^(ok|FAIL|--- FAIL)|_test.go:[0-9]+:' "$TMPDIR/m.txt" | grep -v COVERS
```

- [ ] **Step 7: Commit**

```bash
git add internal/managers docs/windows/decisions.md
git commit -m "managers: one pass on start, on kick and on drain; no clock, no sweep, no interval"
```

---

### Task 6: The run wiring and the run-time refusal

**Files:**
- Modify: `internal/cli/run/managers.go` (`buildManagedTables`, the refusal)
- Modify: `internal/cli/run/window_refusal_test.go`
- Modify: `internal/cli/run/idle_close_test.go`, `managers_test.go`, `row_roles_test.go`, `shared_conn_test.go`, `metrics_test.go`, `progress_wiring_test.go` (drop `LateRows`, `PollIntervalSecs`; `Poll` → `Pass`; `newTestWindow` passes a signal or nil)

**Interfaces:**
- `buildManagedTables(ctx, conf, db, watermarks *core.Watermarks, l, mp, budget, events)` -- gains the tracker so each manager gets `watermarks.Signal(table.Name)`. `root.go` passes the `watermarks` from `windowOptions`.

- [ ] **Step 1: Write the failing test**

Rewrite `window_refusal_test.go`:

```go
// A config that skipped validate must not run the pairing validate refuses:
// lateness above zero with a sink that appends, which holds a republished
// bucket twice. run refuses it before it dials anything, with the exit code
// a config error gets.
func TestWindowLatenessWithAnAppendingSinkIsRefusedAtStartup(t *testing.T) {
	coverage.Covers(t, "manager.window")
	db, _ := rowsTestDB(t)
	conf := &config.Conf{Tables: &config.Tables{SQL: []config.TableSQL{{
		Name: "agg",
		Window: &config.Window{
			TimeColumn: "bucket", SizeSeconds: 60, AllowedLatenessSecs: 300,
			Sink: config.Sink{Type: "postgres", Postgres: &config.PostgresSink{
				DSN: "postgres://u:p@127.0.0.1:1/db", Table: "agg", Mode: "append",
			}},
		},
	}}}}
	_, closeConns, err := buildManagedTables(context.Background(), conf, db, nil, zap.NewNop(), nil, nil, sinks.RetryEvents{})
	closeConns()
	assert.Error(t, err)
	assert.Equal(t, errs.CodeConfigInvalid, errs.CodeOf(err))
	assert.Equal(t, errs.ExitUserError, errs.ExitCode(err))
	assert.That(t, strings.Contains(err.Error(), "allowed_lateness_seconds"))
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `perl -e 'alarm shift; exec @ARGV' 600 go vet ./internal/cli/run/`
Expected: compile errors.

- [ ] **Step 3: Implement**

In `managers.go`, add beside `windowSpecs`:

```go
// LatenessNeedsReplacingSink is the pairing run refuses for a config that
// never went through validate: lateness above zero and a sink known to
// append. validate refuses it too, with the same words.
func latenessNeedsReplacingSink(w *config.Window) bool {
	if w.AllowedLatenessSecs <= 0 {
		return false
	}
	switch w.Sink.Type {
	case "iceberg", "kafka":
		return true
	case "postgres":
		return w.Sink.Postgres != nil && w.Sink.Postgres.Mode == "append"
	}
	return false
}
```

In `buildManagedTables`, where the `ReemitOverwrites` block was:

```go
		if latenessNeedsReplacingSink(table.Window) {
			return nil, closeConns, errs.New(errs.CodeConfigInvalid,
				"table %q window: allowed_lateness_seconds is %d and the %s sink appends; a late row "+
					"republishes its bucket whole, which an appending sink holds twice. Use a sink that "+
					"replaces by key, or set allowed_lateness_seconds to 0",
				table.Name, table.Window.AllowedLatenessSecs, table.Window.Sink.Type)
		}
```

`NewWatermark(conn, windowDeclaration(table), sink, signalFor(watermarks, table.Name), opts...)` where `signalFor` returns nil for a nil tracker. Remove `time.Duration(table.Window.PollIntervalSecs)*time.Second`. In `root.go`, pass `watermarks` into `buildManagedTables`.

Update the tests: `newTestWindow` builds a `core.Watermarks` for `agg` and passes its signal (or `nil`); `idleCloseRig.poll` becomes `pass` calling `managed[0].Pass`; every `config.Window{...}` literal drops `LateRows` and `PollIntervalSecs`.

- [ ] **Step 4: Run the package (unsandboxed; it binds ports)**

Run: `perl -e 'alarm shift; exec @ARGV' 900 go test -count=1 -skip TestIntegration ./internal/cli/run/ > "$TMPDIR/r.txt" 2>&1; echo rc=$?; grep -E '^(ok|FAIL|--- FAIL)|_test.go:[0-9]+:' "$TMPDIR/r.txt" | grep -v COVERS`

- [ ] **Step 5: Commit**

```bash
git add internal/cli/run
git commit -m "run: each manager gets its window's signal; lateness with an appending sink is refused at startup"
```

---

### Task 7: The conformance harness

**Files:**
- Modify: `internal/conformance/manager.go`
- Modify: `internal/managers/conformance_test.go`

**Interfaces:**
- `Manager` interface: `Start(ctx) error`, `Pass(ctx) error`.
- `ManagerSubject.New func(t *testing.T, sink core.Sink, budget *core.DrainBudget) Manager`; `HoldCommit func(t, sink, budget, hold func()) Manager`; `Seed`, `Remaining`, `Uncommitted`, `Batch` unchanged; `SeedLate`, `Metered`, `SeedNewer`, `LateInstrument` removed.
- Removed checks: `checkLatePolicyHolds`, `checkLateCountedOnce` and their invariants (Task 11 removes them from the ledger).

- [ ] **Step 1: Change the harness**

In `manager.go`: the interface, the subject, `newManagerRun` (`s.New(t, sink.counted, budget)`), every `m.Poll` → `m.Pass`, delete `lateRows`, `checkLatePolicyHolds`, `checkLateCountedOnce`, `latePolicyHolds`, `lateCountedOnce`, and their verdicts. `checkManagerDrainBounded`: cancel the context **before** starting:

```go
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already stopping: Start runs the drain pass only, which is the case this check is about
	done := make(chan error, 1)
	started := time.Now()
	go func() { done <- m.Start(ctx) }()
```

Update the package comment's list of claims, and `manager.publish.eventually`'s check comment: "starts the loop over closed buckets and holds that they reach the sink on the start pass, without anything else happening."

- [ ] **Step 2: Change the subject**

In `internal/managers/conformance_test.go`: `New` and `HoldCommit` build with `NewWatermark(conn, decl, sink, nil, ...)`; delete `SeedLate`, `Metered`, `SeedNewer`, `LateInstrument`; `Seed` keeps `assertAt(t, d.pipeline, bucket(n))`.

- [ ] **Step 3: Run**

Run: `perl -e 'alarm shift; exec @ARGV' 900 go test -count=1 ./internal/conformance/ ./internal/managers/ -run 'Conformance|TestConformance' > "$TMPDIR/cf.txt" 2>&1; echo rc=$?; grep -E '^(ok|FAIL|--- FAIL)|manager.go:[0-9]+:' "$TMPDIR/cf.txt" | grep -v COVERS`
Expected: every remaining check passes; `publish.eventually` passes on the start pass.

- [ ] **Step 4: Commit**

```bash
git add internal/conformance internal/managers/conformance_test.go
git commit -m "conformance: the manager passes on start and on kick; the late sweep's checks go with the sweep"
```

---

### Task 8: The wire bundle's late rows

**Files:**
- Modify: `internal/turbostats/collect.go` (~lines 120-130 and 479)
- Modify: `turbostats/wire/bundle.go` (~lines 283-304)
- Modify: their tests (grep `LateRowsReemitted`, `lateReemitted`)

**Interfaces:**
- `wire.Pipeline.LateRowsDropped *int64` (unchanged name, now `outcome="refused"`); `LateRowsRecomputed *int64` (new, `outcome="recomputed"`); `LateRowsReemitted` removed. `omitempty` pointers, so an older receiver sees the new field absent and the removed one absent.

- [ ] **Step 1: Write the failing test**

In the collector's test, feed `window_late_rows` data points with `outcome=refused` (3) and `outcome=recomputed` (2) and assert `*p.LateRowsDropped == 3`, `*p.LateRowsRecomputed == 2`.

- [ ] **Step 2: Implement**

In `collect.go` at the `case "window_late_rows":`, read attribute `outcome` instead of `policy`; `refused` → `dim.lateDropped`, `recomputed` → `dim.lateRecomputed`. In `bundle.go`, rename the field and rewrite the comment: "LateRowsDropped counts rows refused before the handler because their bucket had closed more than allowed_lateness_seconds before the watermark; they are the data-loss half. LateRowsRecomputed counts rows admitted within lateness, each of which republished its bucket whole." Update `turbostats/wire/README` or schema docs if the field is listed.

- [ ] **Step 3: Run and commit**

Run: `perl -e 'alarm shift; exec @ARGV' 900 go test -count=1 ./internal/turbostats/... ./turbostats/...` (port-binding reporter tests need the sandbox off).

```bash
git add internal/turbostats turbostats
git commit -m "turbostats: late rows are refused or recomputed; reemitted is gone with the delta"
```

---

### Task 9: The simulator

**Files:**
- Modify: `internal/simulate/window.go` (the sink keeps the last value per bucket; `Poll` → `Pass`; the manager gets the signal; a signal-driven runner)
- Modify: `internal/simulate/simulate.go` (`Pass` step; `RunWindowed` option)
- Modify: `internal/simulate/window_test.go`, `event_time_test.go`; `README.md`

**Interfaces:**
- `type Pass struct{}` replaces `Poll`. `WindowResult` gains `LastValue map[time.Time]int64` (the sink's last published value per bucket) and `LateRefused int64` (from the engine's counter); `LateDropped` is removed (it was a residual).
- `RunWindowedDriven(t, owned, decl, script) WindowResult` runs the manager's `Start` in a goroutine so kicks drive it; scripts then use `AwaitPublished{Rows int64}` to wait.

- [ ] **Step 1: Write the failing tests**

In `event_time_test.go`, rewrite `ARowForAClosedBucketIsDropped` and add:

```go
// A row for a bucket the watermark has passed is late. With no lateness the
// engine refuses it before the handler: it never reaches the table, and the
// sink's value for the bucket is the one the close published.
//
//	step         event time   bucket   window table          asserted   engine
//	-------------------------------------------------------------------------
//	Produce 5    12:00:30     12:00    12:00: 5              11:59:30
//	Produce 4    12:05:00     12:05    12:00: 5, 12:05: 4    12:04
//	Pass                              12:05: 4              12:04      12:00 → 5 published, deleted
//	Produce 6    12:00:30     (none)   12:05: 4              12:04      end 12:01 <= 12:04: refused, 6 counted
func TestSimulate_ARowForAClosedBucketIsRefused(t *testing.T) {
	coverage.Covers(t, "manager.window")
	r := RunWindowed(t, []int32{0}, windowDecl(), []Step{
		Produce{Partition: 0, Rows: 5, At: base.Add(30 * time.Second)},
		Produce{Partition: 0, Rows: 4, At: base.Add(5 * time.Minute)},
		Pass{},
		Produce{Partition: 0, Rows: 6, At: base.Add(30 * time.Second)},
	})
	assert.Equal(t, 15, r.Produced)
	assert.Equal(t, int64(5), r.Published)
	assert.Equal(t, int64(4), r.StillOpen)
	assert.Equal(t, int64(6), r.LateRefused)
	assert.Equal(t, int64(5), r.LastValue[base])
}

// With lateness, a late row is written and the bucket republished whole: the
// sink's last value is the exact count, never the late rows alone.
//
//	step         event time   bucket   window table          asserted   sink's value for 12:00
//	-----------------------------------------------------------------------------------------
//	Produce 5    12:00:30     12:00    12:00: 5              11:59:30   -
//	Produce 4    12:05:00     12:05    12:00: 5, 12:05: 4    12:04      -
//	Pass                              12:00: 5, 12:05: 4    12:04      5     kept: lateness 10m
//	Produce 1    12:00:45     12:00    12:00: 6, 12:05: 4    12:04      5     late, allowed: recompute queued
//	Pass                              (same)                12:04      6     the whole bucket
//	Produce 2    12:00:50     12:00    12:00: 8, 12:05: 4    12:04      6
//	Pass                              (same)                12:04      8
func TestSimulate_ALateRowRecomputesTheBucket(t *testing.T) {
	coverage.Covers(t, "manager.window")
	decl := windowDecl()
	decl.Lateness = 10 * time.Minute
	r := RunWindowed(t, []int32{0}, decl, []Step{
		Produce{Partition: 0, Rows: 5, At: base.Add(30 * time.Second)},
		Produce{Partition: 0, Rows: 4, At: base.Add(5 * time.Minute)},
		Pass{},
		Produce{Partition: 0, Rows: 1, At: base.Add(45 * time.Second)},
		Pass{},
		Produce{Partition: 0, Rows: 2, At: base.Add(50 * time.Second)},
		Pass{},
	})
	assert.Equal(t, 12, r.Produced)
	assert.Equal(t, int64(8), r.LastValue[base])
	assert.Equal(t, int64(0), r.LateRefused)
	assert.Equal(t, int64(2), r.Recomputes)
}

// Beyond lateness a late row is refused even though the bucket is retained.
func TestSimulate_ALateRowBeyondLatenessIsRefused(t *testing.T) {
	coverage.Covers(t, "manager.window")
	decl := windowDecl()
	decl.Lateness = 2 * time.Minute
	r := RunWindowed(t, []int32{0}, decl, []Step{
		Produce{Partition: 0, Rows: 5, At: base.Add(30 * time.Second)},
		Produce{Partition: 0, Rows: 4, At: base.Add(5 * time.Minute)},
		Pass{}, // asserted 12:04; 12:00 ends 12:01; 12:01 + 2m = 12:03 <= 12:04: already expired
		Produce{Partition: 0, Rows: 1, At: base.Add(45 * time.Second)},
	})
	assert.Equal(t, int64(1), r.LateRefused)
	assert.Equal(t, int64(5), r.LastValue[base])
}

// Three late rows for one bucket, one recompute.
func TestSimulate_ABurstOfLateRowsIsOneRecompute(t *testing.T) {
	coverage.Covers(t, "manager.window")
	decl := windowDecl()
	decl.Lateness = 10 * time.Minute
	r := RunWindowed(t, []int32{0}, decl, []Step{
		Produce{Partition: 0, Rows: 5, At: base.Add(30 * time.Second)},
		Produce{Partition: 0, Rows: 4, At: base.Add(5 * time.Minute)},
		Pass{},
		Produce{Partition: 0, Rows: 1, At: base.Add(40 * time.Second)},
		Produce{Partition: 0, Rows: 1, At: base.Add(41 * time.Second)},
		Produce{Partition: 0, Rows: 1, At: base.Add(42 * time.Second)},
		Pass{},
	})
	assert.Equal(t, int64(8), r.LastValue[base])
	assert.Equal(t, int64(1), r.Recomputes)
}

// A late row committed, then a restart before the manager passed: the start
// pass republishes every retained bucket, so the exact value still reaches
// the sink.
func TestSimulate_ARecomputeSurvivesARestart(t *testing.T) {
	coverage.Covers(t, "manager.window")
	decl := windowDecl()
	decl.Lateness = 10 * time.Minute
	r := RunWindowed(t, []int32{0}, decl, []Step{
		Produce{Partition: 0, Rows: 5, At: base.Add(30 * time.Second)},
		Produce{Partition: 0, Rows: 4, At: base.Add(5 * time.Minute)},
		Pass{},
		Produce{Partition: 0, Rows: 1, At: base.Add(45 * time.Second)},
		Restart{},
		StartPass{}, // what Start does first
	})
	assert.Equal(t, int64(6), r.LastValue[base])
}

// The kick follows the commit: with the manager's loop running, a produce
// that moves the watermark publishes without any Pass step.
func TestSimulate_TheKickFollowsTheCommit(t *testing.T) {
	coverage.Covers(t, "manager.window")
	r := RunWindowedDriven(t, []int32{0}, windowDecl(), []Step{
		Produce{Partition: 0, Rows: 5, At: base.Add(30 * time.Second)},
		Produce{Partition: 0, Rows: 4, At: base.Add(5 * time.Minute)},
		AwaitPublished{Rows: 5},
	})
	assert.Equal(t, int64(5), r.Published)
	assert.Equal(t, int64(4), r.StillOpen)
}
```

Update `windowDecl()` to drop `Late`. In `window_test.go`, `Poll{}` → `Pass{}` throughout, and assertions on `LateDropped` become `LateRefused` where a refusal is expected (there are none in that file; `AFastPartitionHoldsForTheSlowOne` asserts `LateRefused == 0`).

- [ ] **Step 2: Implement**

`window.go`:
- `windowSink` keeps `last map[int64]int64` (bucket micros → last published `n`), updated per flush by summing the rows of each bucket in that flush and **replacing** the entry; `rows` total stays for `Published`. `counts()` unchanged; add `lastValues() map[time.Time]int64`.
- `RunWindowed` builds the manager with `managers.NewWatermark(db.manager, decl, r.window.sink, wm.Signal(decl.Table))` where `wm` is the tracker `sinkTurbine` builds -- so the tracker must be built before the manager: move `newWatermarks` to `RunWindowed` and hand it to `sinkTurbine` (a `Restart` rebuilds it and must rebuild the manager's signal too: give `windowRun` a `signal *core.WindowSignal` the manager holds, and have `Restart` copy the new tracker's signal into a stable wrapper -- simplest: `windowRun.signal` is a `*core.WindowSignal` created once by the run and passed to *both* the tracker (`core.NewWatermarksWithSignals(specs, clock, signals)`) and the manager; add that constructor variant in core, two lines).
- `WindowResult.LateRefused` reads the engine's `window_late_rows_total{outcome=refused}` through a `sdkmetric.ManualReader` on the turbine's `WithMetrics`; `Recomputes` reads the manager's `window_recomputes`. Remove `LateDropped`.
- `Pass` step: `r.window.manager.Pass(ctx)`. `StartPass` step: the same, documented as what `Start` does first.
- `RunWindowedDriven`: like `RunWindowed`, but runs `manager.Start(ctx)` in a goroutine after `r.start()` and cancels it in `stop()`; `AwaitPublished{Rows}` awaits `sink.counts()`.

`simulate.go`: rename the `Poll` type to `Pass`. `deliver`'s wait already waits for commits, which is when kicks are sent.

- [ ] **Step 3: README**

Replace the "Poll" references with `Pass`, the "manager collects late rows on a close" rule with: "Late rows never reach the table with no lateness -- the engine refuses them, and `LateRefused` counts them. With lateness, a late row is written and the bucket is republished whole on the next pass; `LastValue` is what the sink holds." Add the five new scenarios to the index and the recompute diagram to the worked examples.

- [ ] **Step 4: Run**

```bash
perl -e 'alarm shift; exec @ARGV' 900 go test -count=3 ./internal/simulate/ > "$TMPDIR/s.txt" 2>&1; echo rc=$?
perl -e 'alarm shift; exec @ARGV' 900 go test -race -count=2 ./internal/simulate/ > "$TMPDIR/sr.txt" 2>&1; echo rc=$?
```

- [ ] **Step 5: Commit**

```bash
git add internal/simulate internal/core/watermarks.go
git commit -m "simulate: late rows are refused or recomputed, and the sink holds the exact value"
```

---

### Task 10: The model

**Files:**
- Modify: `internal/managers/model.go`, `model_test.go`

**Interfaces:**
- `Event` gains `Late bool` (a row for the bucket just below the closed watermark) and `VeryLate bool` (a row for a bucket below `closed − lateness`); `Model` tracks `Refused int` and the sink's `last map[time.Time]int`.

- [ ] **Step 1: Write the failing test**

Extend `shapes` with `lateness time.Duration` and cross each of A--D with `0` and `time.Minute` (eight shapes). Extend `modelAlphabet` with `{Kind: Produce, Partition: 0, Rows: 1, Late: true}` and `{Kind: Produce, Partition: 0, Rows: 1, VeryLate: true}` (ten events, depth 4: 11,110 sequences per shape). In `checkSequence`, replace the counting property with:

```go
	// The exact-value property. For every bucket, the sink's last value is
	// the rows produced for it less the rows refused for it -- never a delta,
	// never a stale first publish. And a refused row is never in the table.
	for b, produced := range m.ProducedPerBucket {
		want := produced - m.RefusedPerBucket[b]
		if got := m.Last[b]; got != want {
			t.Fatalf("%v: bucket %s: sink holds %d, produced %d, refused %d",
				kinds(seq), b, got, produced, m.RefusedPerBucket[b])
		}
	}
	if m.Refused != sumRefused && s.lateness == 0 && anyLate(seq) {
		t.Fatalf("%v: %d late rows and none refused under lateness 0", kinds(seq), lateCount(seq))
	}
```

- [ ] **Step 2: Implement**

`Model.Apply` for `Produce`: compute `at` as today, then `refused, recompute := m.engine.Classify(at.UnixNano())`; if refused, `m.Refused++`, `m.RefusedPerBucket[bucket]++`, do not insert, do not observe; otherwise insert, observe, and for each `recompute` add the bucket to `m.recompute` set; commit. `Late: true` sets `at` to `m.closed − 1s` when `hadClosed` (else the clock); `VeryLate: true` sets `at` to `m.closed − lateness − size − 1s`. `poll()` becomes the pass: publish due (buckets with C < end ≤ W: `m.Last[b] = m.buckets[b]`), republish `m.recompute` buckets (`m.Last[b] = m.buckets[b]`), delete expired (`end + lateness <= W`), `C := W`. The late-row drop branch is removed. `Restart` rebuilds the engine as today; the recompute set is cleared (in memory) and the start pass republishes every retained bucket -- model that: after `Restart`, set `m.Last[b] = m.buckets[b]` for every retained bucket at the next `poll`.

- [ ] **Step 3: Run and commit**

Run: `perl -e 'alarm shift; exec @ARGV' 900 go test -count=1 -run TestManagerWindow ./internal/managers/`

```bash
git add internal/managers/model.go internal/managers/model_test.go
git commit -m "managers: the model decides lateness at arrival and holds the sink to the exact value"
```

---

### Task 11: The ledger, the decisions page, the changelog, the spec

**Files:**
- Modify: `docs/coverage/invariants.yml`
- Regenerate: `docs/coverage/matrix.md` (`make coverage-page`)
- Modify: `CHANGELOG.md`, `docs/superpowers/specs/2026-09-26-watermark-driven-close-design.md` (status)

- [ ] **Step 1: The ledger**

In `invariants.yml`:
- `manager.publish.eventually` claim: "A closed window reaches the sink on the manager's start pass or on the kick that follows the commit which closed it, without anything else happening. The manager has no clock; the engine tells it."
- Delete `manager.late.policy_holds` and `manager.late.counted_once` (and the comment above the second).
- Add, under the pipeline checkpoint claims:
  ```yaml
  - id: pipeline.window.lateness_decided_at_arrival
    family: checkpoint
    class: safety
    applies_to: pipeline
    claim: >
      A record whose bucket ended at or before the watermark less
      allowed_lateness_seconds is refused before the handler and counted; one
      within lateness is written and its bucket is republished as a whole
      value; the window table never holds a row the engine did not admit.
    verified_by: harness
    requires: []
    tracked_by: "the conformance harness has no windowed pipeline subject yet; internal/core, internal/managers/model.go and internal/simulate verify it"
  ```
  (`tracked_by` because `verified_by` must be `harness` and the windowed pipeline subject is the follow-up #393 recorded; the claim is declared and its evidence named.)

- [ ] **Step 2: Regenerate and check**

```bash
make coverage-page
uv run --locked pytest tests/tooling -q
git diff --stat docs/coverage
```

- [ ] **Step 3: CHANGELOG**

Under `Unreleased` → `Changed`, before the #393 entry:

> - **The window's manager no longer polls, and lateness is decided when a record arrives.** The engine tells the manager when a window's watermark moved; the manager publishes what closed, republishes what a late row landed in, and deletes what is past its lateness. It has no clock. `poll_interval_seconds` is gone: nothing is left for it to pace, and a config that sets it fails validation naming the change.
>   `late_rows` is gone too, replaced by `allowed_lateness_seconds` (default 0): Flink's `allowedLateness`. With 0, a record for a bucket that closed is refused before the handler and counted in `window_late_rows_total{outcome="refused"}` -- it never reaches the window table, which is what `drop` did after the fact. With a positive value a closed bucket's rows are kept that long, a late record within it is written, and the bucket is republished as a **whole value** -- `emit_sql` over every row it has -- so the sink must replace by key; validate refuses an append-only sink. This replaces `reemit`, which published the late rows alone as a delta that only an additive sink could use and that a replacing sink turned into loss. The sink now always receives the exact value.
>   `time_column` must be `time_bucket(INTERVAL '<size>', event_time)`, which validate now refuses rather than warns: the engine decides lateness from that bucket, and the six shipped windowed examples and the Render template are updated to it. The wire bundle's `late_rows_reemitted` is gone and `late_rows_recomputed` is new; `late_rows_dropped` now counts refusals.

- [ ] **Step 4: The spec's status**

Change `**Status:** proposal, approved in discussion on 2026-09-26.` to `**Status:** implemented in the PR that carries this plan.`

- [ ] **Step 5: The whole suite, then commit**

```bash
gofmt -l internal/
perl -e 'alarm shift; exec @ARGV' 1800 go test -count=1 -skip TestIntegration ./... > "$TMPDIR/all.txt" 2>&1; echo rc=$?; grep -E '^(FAIL|--- FAIL)' "$TMPDIR/all.txt"
perl -e 'alarm shift; exec @ARGV' 1800 go test -race -count=1 -skip TestIntegration ./internal/core/ ./internal/managers/ ./internal/simulate/ ./internal/cli/run/
```
The port-binding packages need the sandbox off. Then:

```bash
git add docs CHANGELOG.md
git commit -m "docs: the window closes on the engine's signal; the ledger, the changelog and the spec say so"
```

---

## Self-review

**Spec coverage.** Signal → Task 4 (engine) and 5 (manager). Lateness at arrival → Task 4. `allowed_lateness_seconds` replaces `late_rows` → Task 2, validate rules → Task 3, run-time refusal → Task 6. `poll_interval_seconds` removed everywhere in the constraints table → Tasks 2 and 3. Bucket agreement → Task 1, required by Task 3's derivation rule. Start pass / restart → Task 5 and Task 9's `ARecomputeSurvivesARestart`. Signals after the commit, none on rollback → Task 4's test. Ledger changes → Task 11. Wire → Task 8. Verification (model, simulator, conformance, unit) → Tasks 10, 9, 7, 1/4. The liveness table needs no task: it is unchanged behaviour. Multi-window rule → Task 4's `Classify` and its test.

**Placeholders.** Task 3's example table asks the implementer to read two source blocks whose payload fields this plan could not see; the rule for what to write is given. Task 9's tracker-signal wiring names a two-line constructor variant rather than showing it: `NewWatermarksWithSignals(specs, now, signals []*WindowSignal)` assigns `signals` instead of creating them. No TBDs.

**Type consistency.** `core.WindowSpec.Lateness` (Task 2) is read by `Classify` (Task 4) and set by `windowSpecs` (Task 2). `managers.Declaration.Lateness` (Task 2) is read by `expiredBefore` and `BucketStateOf` (Task 5) and set by `windowDeclaration` (Task 2). `core.WindowSignal` with `Kick`, `Wait`, `Recompute`, `TakeRecompute` (Task 4) is consumed by `Start`/`Pass` (Task 5), `buildManagedTables` (Task 6), and the simulator (Task 9). `Pass` replaces `Poll` in Tasks 5, 6, 7, 9. `noteLate([]LateBucket)` is defined in Task 4 and used by its test. `WindowMetrics.Recomputed` (Task 5) is read by the simulator's `Recomputes` (Task 9). `window_late_rows_total{window,outcome}` (Task 4) is read by Task 8 and Task 9.
