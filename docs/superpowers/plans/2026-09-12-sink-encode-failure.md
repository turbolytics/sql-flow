# Sink encode failure is permanent Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A value the sink's driver cannot encode fails once, carries a `user` code, and exits 10, instead of being retried and reported as `system.sink.unreachable`.

**Architecture:** Three layers, each a task. The error registry gains `user.sink.encode_failed`. The retry ladder in `internal/sinks/retry.go` stops deciding retryability by listing codes and decides by class: any `user` error and `system.sink.write_failed` are permanent, everything else is retried. The ClickHouse sink wraps the driver's `Append` error with the new code. A fourth task writes the retry rule into the README and the invariant manifest.

**Tech Stack:** Go 1.2x, `internal/errs` coded errors, `github.com/zeebo/assert`, `github.com/ClickHouse/clickhouse-go/v2` v2.48.0 (`lib/driver.Batch` is an interface, so the sink test needs no server).

**Spec:** `docs/superpowers/specs/2026-09-12-sink-encode-failure-design.md`

## Global Constraints

- The error registry is append-only. `TestErrorTaxonomy_RegistryIsAppendOnly` compares `internal/errs/registry.go` to `internal/errs/testdata/codes.golden`. Add, never rename or remove.
- An uncoded error stays retryable. A driver's timeout or reset arrives without a code, and that is what the ladder exists for.
- Every test calls `coverage.Covers(t, "<id>")` first, with an id the coverage registry already knows. Use `sink.retry`, `sink.clickhouse` and `error.taxonomy`. Do not invent an id.
- Prose follows Google Technical Writing One, per the repo's `CLAUDE.md`: active voice, one idea per sentence, no hedging. Code comments explain why, not what.
- Commit messages name the defect, the fix, and the evidence, and say what breaks if the change is wrong. No attribution trailers.
- Run tests with `go test ./internal/<pkg>/ -run '<Name>' -v`. The Go build cache under `~/Library/Caches/go-build` is outside the sandbox; if a build fails with `operation not permitted` on that path, rerun outside the sandbox.

---

### Task 1: Register `user.sink.encode_failed`

**Files:**
- Modify: `internal/errs/registry.go:44-45` (constants) and `internal/errs/registry.go:142-146` (definitions)
- Modify: `internal/errs/testdata/codes.golden`
- Test: `internal/errs/errs_test.go`

**Interfaces:**
- Produces: `errs.CodeSinkEncodeFailed errs.Code = "user.sink.encode_failed"`. Tasks 2 and 3 reference it by that name.

- [ ] **Step 1: Write the failing test**

Append to `internal/errs/errs_test.go`:

```go
// A value the sink's driver refuses is the user's to fix: the value is in the
// topic and a restart re-reads it. The exit code has to say terminal, or a
// supervisor loops on it.
func TestErrorTaxonomy_EncodeFailedIsAUserErrorThatExitsTerminal(t *testing.T) {
	coverage.Covers(t, "error.taxonomy")
	err := New(CodeSinkEncodeFailed, "dt_plain parsing time")

	assert.Equal(t, ClassUser, ClassOf(err))
	assert.Equal(t, "sink", CodeSinkEncodeFailed.Domain())
	assert.Equal(t, ExitUserError, ExitCode(err))
	assert.False(t, Retryable(ExitCode(err)))

	def, ok := Lookup(CodeSinkEncodeFailed)
	assert.True(t, ok)
	assert.True(t, strings.Contains(def.Action, "handler SQL"))
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `go test ./internal/errs/ -run 'TestErrorTaxonomy_EncodeFailedIsAUserErrorThatExitsTerminal' -v`
Expected: build failure, `undefined: CodeSinkEncodeFailed`.

- [ ] **Step 3: Add the constant and the definition**

In `internal/errs/registry.go`, after the `CodeSinkTypeUnsupported` constant (line 45):

```go
	// A value the sink's client could not encode for the destination column.
	// The value fails identically on every attempt, so the retry ladder does
	// not retry it (#233).
	CodeSinkEncodeFailed Code = "user.sink.encode_failed"
```

In the `registry` map, after the `CodeSinkTypeUnsupported` entry (line 146):

```go
	CodeSinkEncodeFailed: {
		CodeSinkEncodeFailed,
		"The sink's client could not encode a result value for the destination column. It fails the same way on every attempt and is not retried.",
		"Cast or format the column in the handler SQL to match the destination column's type. The message names the column and the value.",
	},
```

- [ ] **Step 4: Regenerate the golden file**

Run: `UPDATE_GOLDEN=1 go test ./internal/errs/ -run 'TestErrorTaxonomy_RegistryIsAppendOnly' -v`
Expected: `golden updated`. Then `git diff internal/errs/testdata/codes.golden` shows exactly one added line, `user.sink.encode_failed`, between `user.sink.invalid` and `user.sink.type_unsupported`. If any line was removed, stop: the registry lost a code.

- [ ] **Step 5: Run the whole package**

Run: `go test ./internal/errs/ -v`
Expected: PASS. `TestErrorTaxonomy_ExitCodeMapsEveryCode` and `TestErrorTaxonomy_EveryCodeIsWellFormed` cover the new entry without changes.

- [ ] **Step 6: Commit**

```bash
git add internal/errs/registry.go internal/errs/testdata/codes.golden internal/errs/errs_test.go
git commit -m "errs: a code for a value the sink cannot encode

A ClickHouse value the driver refuses in Append was returned uncoded, so
the retry ladder retried it and reported system.sink.unreachable (#233).
user.sink.encode_failed names the fault: a payload value the user's SQL
shaped, terminal, exit 10.

type_unsupported stays for a column whose Arrow type the sink cannot
convert. That is a schema property and fails for every row. encode_failed
is one value in a supported column. An operator reading type unsupported
for a badly formatted string looks at the wrong thing.

The registry is append-only; codes.golden gains one line.

What breaks if this is wrong: a supervisor restarts a pipeline into the
same bad value forever, because the exit code said retryable."
```

---

### Task 2: The retry ladder decides by class

**Files:**
- Modify: `internal/sinks/retry.go:161-175` (`retryable` and its comment)
- Test: `internal/sinks/retry_test.go`

**Interfaces:**
- Consumes: `errs.CodeSinkEncodeFailed` from Task 1. `errs.ClassOf(err) errs.Class` and `errs.ClassUser` already exist in `internal/errs`.
- Produces: `retryable(err error) bool` keeps its signature. Task 3 relies on the rule: any `user` code makes one attempt.

- [ ] **Step 1: Write the failing tests**

Append to `internal/sinks/retry_test.go`. The helpers `flakySink`, `newTestRetry` and `testPolicy` are defined at the top of that file.

```go
// A column type the sink cannot convert fails identically every attempt.
// Before the class rule, this code was not on retryable's list, so the ladder
// re-encoded the same batch four times and reported the destination as
// unreachable (#233).
func TestSinkRetry_DoesNotRetryAnUnsupportedType(t *testing.T) {
	coverage.Covers(t, "sink.retry")
	inner := &flakySink{failures: 99, err: errs.New(errs.CodeSinkTypeUnsupported, "time64 is not supported")}
	r, slept := newTestRetry(inner, testPolicy())

	err := r.Flush(context.Background())

	assert.Error(t, err)
	assert.Equal(t, 1, inner.attempts)
	assert.Equal(t, 0, len(*slept))
	assert.Equal(t, errs.CodeSinkTypeUnsupported, errs.CodeOf(err))
}

// A value the driver refused while building the batch never reached the
// destination, and sending it again changes nothing.
func TestSinkRetry_DoesNotRetryAnEncodeFailure(t *testing.T) {
	coverage.Covers(t, "sink.retry")
	inner := &flakySink{failures: 99, err: errs.New(errs.CodeSinkEncodeFailed, "dt_plain parsing time")}
	r, slept := newTestRetry(inner, testPolicy())

	err := r.Flush(context.Background())

	assert.Error(t, err)
	assert.Equal(t, 1, inner.attempts)
	assert.Equal(t, 0, len(*slept))
	assert.Equal(t, errs.CodeSinkEncodeFailed, errs.CodeOf(err))
}

// The rule is the class, not a list. Every user code the registry holds
// makes exactly one attempt, including ones added after this test.
func TestSinkRetry_NoUserCodeIsRetried(t *testing.T) {
	coverage.Covers(t, "sink.retry")
	for _, d := range errs.All() {
		if !d.Code.IsUser() {
			continue
		}
		inner := &flakySink{failures: 99, err: errs.New(d.Code, "x")}
		r, _ := newTestRetry(inner, testPolicy())

		err := r.Flush(context.Background())

		assert.Error(t, err)
		if inner.attempts != 1 {
			t.Errorf("%s: made %d attempts on a user error", d.Code, inner.attempts)
		}
		assert.Equal(t, d.Code, errs.CodeOf(err))
	}
}

// An uncoded error that never clears runs the whole ladder and ends as
// unreachable. Pinned so a tidy-up of the class rule cannot turn a driver
// timeout nobody classified into a terminal failure.
func TestSinkRetry_UncodedErrorRunsTheWholeLadder(t *testing.T) {
	coverage.Covers(t, "sink.retry")
	inner := &flakySink{failures: 99, err: errors.New("i/o timeout")}
	p := testPolicy()
	r, _ := newTestRetry(inner, p)

	err := r.Flush(context.Background())

	assert.Error(t, err)
	assert.Equal(t, p.MaxAttempts, inner.attempts)
	assert.Equal(t, errs.CodeSinkUnreachable, errs.CodeOf(err))
}
```

- [ ] **Step 2: Run the tests to verify the first three fail**

Run: `go test ./internal/sinks/ -run 'TestSinkRetry_(DoesNotRetryAnUnsupportedType|DoesNotRetryAnEncodeFailure|NoUserCodeIsRetried|UncodedErrorRunsTheWholeLadder)' -v`
Expected: `DoesNotRetryAnUnsupportedType` fails with attempts 5, not 1, and code `system.sink.unreachable`. `DoesNotRetryAnEncodeFailure` fails the same way. `NoUserCodeIsRetried` reports `user.sink.type_unsupported` and `user.sink.encode_failed` among others made 5 attempts. `UncodedErrorRunsTheWholeLadder` passes already.

- [ ] **Step 3: Replace `retryable`**

Replace lines 161-175 of `internal/sinks/retry.go` with:

```go
// retryable reports whether another attempt could plausibly succeed.
//
// The rule is by class, not by a list of codes. A list retries whatever it
// did not anticipate: user.sink.type_unsupported was never on it, so a column
// the sink could not convert ran the whole ladder and was reported as
// unreachable (#233).
//
//	Class user               never retried   the config, SQL or data is wrong,
//	                                         and fails identically every time
//	system.sink.write_failed never retried   the destination answered and refused
//	system.sink.unreachable  retried         the destination may come back
//	uncoded                  retried         a driver's timeout or reset arrives
//	                                         unclassified; the deadline bounds
//	                                         the cost of guessing wrong
//	any other system code    retried
//
// README "Sink retries" states the same table for operators. Change both.
func retryable(err error) bool {
	if errs.ClassOf(err) == errs.ClassUser {
		return false
	}
	return !errs.HasCode(err, errs.CodeSinkWriteFailed)
}
```

`errs.CodeOf` returns `system.internal.unexpected` for an uncoded error, so the uncoded case falls through to `true` without a special branch.

- [ ] **Step 4: Run the retry suites**

Run: `go test ./internal/sinks/ -run 'TestSinkRetry_' -v`
Expected: PASS, including the four new tests and the existing `DoesNotRetryARejectedWrite`, `DoesNotRetryAConfigError`, `BoundsNonRetryableStopsUnderAnyPolicy` and `RetriesAnUncodedError`.

- [ ] **Step 5: Run the sinks package**

Run: `go test ./internal/sinks/`
Expected: PASS. Live-integration tests skip when no ClickHouse or Kafka is running; a skip is fine, a FAIL is not.

- [ ] **Step 6: Commit**

```bash
git add internal/sinks/retry.go internal/sinks/retry_test.go
git commit -m "sinks: the retry ladder decides by error class, not by a list

retryable listed three codes it would not retry and retried everything
else. user.sink.type_unsupported was not on the list, so a column the
ClickHouse sink could not convert was re-encoded four times and then
reported as system.sink.unreachable. Nothing was unreachable, and the
exit code told a supervisor to restart into the same failure (#233).

Any user-class error now makes one attempt, and so does write_failed.
Unreachable and uncoded errors are retried as before; a driver timeout
arrives without a code and is exactly what the ladder exists for.

TestSinkRetry_NoUserCodeIsRetried walks the registry, so a user code
added later cannot fall back into the ladder. UncodedErrorRunsTheWholeLadder
pins the other direction.

What breaks if this is wrong: a transient fault some sink coded as user
stops being retried. No sink does that today, and the tests would show it."
```

---

### Task 3: The ClickHouse sink codes its encode failures

**Files:**
- Modify: `internal/sinks/clickhouse.go:255-303` (`appendTables`)
- Test: `internal/sinks/clickhouse_test.go`

**Interfaces:**
- Consumes: `errs.CodeSinkEncodeFailed` from Task 1; `appendTables(batch driver.Batch, types []column.Type, tables []arrow.Table) error` and `temporalFromString(colType column.Type, s string) (time.Time, bool)`, both already in `clickhouse.go`.
- Produces: nothing new. The `append row` error now carries `user.sink.encode_failed`.

- [ ] **Step 1: Write the failing tests**

Append to `internal/sinks/clickhouse_test.go`. The file already imports `context`, `fmt`, `testing`, `time`, `arrow`, `array`, `memory`, `config`, `coverage`, `errs` and `assert`. Add `"errors"` and `"github.com/ClickHouse/clickhouse-go/v2/lib/column"` and `"github.com/ClickHouse/clickhouse-go/v2/lib/driver"` to the import block.

```go
// stubBatch is a driver.Batch that refuses every Append. The driver's real
// batch does the same thing in memory and touches no connection until Send,
// so a fake is a faithful stand-in for the failure this tests.
type stubBatch struct {
	appendErr error
	aborted   bool
}

func (b *stubBatch) Abort() error                 { b.aborted = true; return nil }
func (b *stubBatch) Append(v ...any) error        { return b.appendErr }
func (b *stubBatch) AppendStruct(v any) error     { return b.appendErr }
func (b *stubBatch) Column(int) driver.BatchColumn { return nil }
func (b *stubBatch) Flush() error                 { return nil }
func (b *stubBatch) Send() error                  { return nil }
func (b *stubBatch) IsSent() bool                 { return false }
func (b *stubBatch) Rows() int                    { return 0 }
func (b *stubBatch) Columns() []column.Interface  { return nil }
func (b *stubBatch) Close() error                 { return nil }

// A value the driver refuses while building the batch is a user fault that
// fails the same way every attempt. Returned uncoded, the retry ladder
// re-encoded it four times and reported the destination unreachable (#233).
func TestSinkClickhouse_ADriverRefusedValueCarriesEncodeFailed(t *testing.T) {
	coverage.Covers(t, "sink.clickhouse")

	driverErr := errors.New(`clickhouse [AppendRow]: dt_plain parsing time "2026-09-01T12:00:00Z" as "2006-01-02 15:04:05": cannot parse "T12:00:00Z" as " "`)
	batch := &stubBatch{appendErr: driverErr}

	schema := arrow.NewSchema([]arrow.Field{{Name: "dt_plain", Type: arrow.BinaryTypes.String}}, nil)
	b := array.NewRecordBuilder(memory.NewGoAllocator(), schema)
	defer b.Release()
	b.Field(0).(*array.StringBuilder).Append("2026-09-01T12:00:00Z")
	rec := b.NewRecord()
	defer rec.Release()
	table := array.NewTableFromRecords(schema, []arrow.Record{rec})
	defer table.Release()

	err := appendTables(batch, []column.Type{"DateTime"}, []arrow.Table{table})

	assert.Error(t, err)
	if !errs.HasCode(err, errs.CodeSinkEncodeFailed) {
		t.Fatalf("code = %s, want %s", errs.CodeOf(err), errs.CodeSinkEncodeFailed)
	}
	// The driver names the column and quotes the value. That text is the
	// operator's only pointer to the bad row and has to survive the wrap.
	assert.True(t, errors.Is(err, driverErr))
}

// The value from #233 misses every layout the sink accepts, so it reaches the
// driver unchanged. This documents the precondition for the encode path; it
// is not a claim the value should fail. Accepting RFC 3339 is a separate
// type-matrix change.
func TestSinkClickhouse_ISO8601WithTAndZReachesTheDriver(t *testing.T) {
	coverage.Covers(t, "sink.clickhouse")
	_, ok := temporalFromString(column.Type("DateTime"), "2026-09-01T12:00:00Z")
	assert.False(t, ok)
}
```

- [ ] **Step 2: Run the tests to verify the first fails**

Run: `go test ./internal/sinks/ -run 'TestSinkClickhouse_(ADriverRefusedValueCarriesEncodeFailed|ISO8601WithTAndZReachesTheDriver)' -v`
Expected: `ADriverRefusedValueCarriesEncodeFailed` fails with `code = system.internal.unexpected, want user.sink.encode_failed`. `ISO8601WithTAndZReachesTheDriver` passes.

The table construction follows `clickhouseFixtureTable` at `clickhouse_test.go:326`, which uses the same `NewRecordBuilder` and `NewTableFromRecords` calls.

- [ ] **Step 3: Wrap the driver's error**

In `internal/sinks/clickhouse.go` `appendTables`, replace:

```go
				if err := batch.Append(row...); err != nil {
					reader.Release()
					return fmt.Errorf("clickhouse sink: append row: %w", err)
				}
```

with:

```go
				if err := batch.Append(row...); err != nil {
					reader.Release()
					// Append validates and buffers in memory; the driver sends
					// nothing until Send. A value it refuses here fails the
					// same way on every attempt, so this is coded permanent
					// rather than left for the ladder to guess at (#233).
					return errs.Wrap(errs.CodeSinkEncodeFailed, err, "clickhouse sink: encode row")
				}
```

`errs` is already imported in `clickhouse.go`. The `arrowValue` branch above it keeps `user.sink.type_unsupported` and its `fmt.Errorf` wrap, because `fmt.Errorf` with `%w` preserves the inner code and `errs.CodeOf` walks the chain.

- [ ] **Step 4: Run the ClickHouse unit tests**

Run: `go test ./internal/sinks/ -run 'TestSinkClickhouse_' -v`
Expected: PASS for the unit tests. The live tests (`InsertsRows`, `InsertsArrays`, `StringTemporalsAreNotShiftedByHostZone` and others that call `newLiveClickhouseSink`) skip with `clickhouse unavailable` unless a ClickHouse is up.

- [ ] **Step 5: Run the end-to-end check against a live ClickHouse, if one is available**

Start the dev stack: `make start-backing-services`. If it fails on port 5432, ignore it; ClickHouse and Kafka still come up. Then run the live sink tests:

Run: `go test ./internal/sinks/ -run 'TestSinkClickhouse_' -v 2>&1 | grep -E '^(--- |ok|FAIL)'`
Expected: every `TestSinkClickhouse_` test reports PASS, none SKIP. If they still skip, note it in the commit message as "live ClickHouse tests not run" and continue. Do not claim the live path passed.

- [ ] **Step 6: Run the sinks package**

Run: `go test ./internal/sinks/`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add internal/sinks/clickhouse.go internal/sinks/clickhouse_test.go
git commit -m "clickhouse sink: a value the driver refuses is coded permanent

batch.Append failed on an ISO 8601 timestamp bound for a DateTime column
and the sink returned the error with no code. The ladder retried it four
times and reported system.sink.unreachable for a server that was up (#233).

Append validates and buffers in memory. clickhouse-go v2.48.0's native and
HTTP batches both call block.Append and return; neither touches the
connection until Send. The value cannot succeed on a later attempt, so it
is wrapped with user.sink.encode_failed at the driver boundary. The
driver's message, which names the column and quotes the value, survives
the wrap.

PrepareBatch and Send keep sinkError, which is where bytes cross the
network and where unreachable is a real answer.

What breaks if this is wrong: Append had a failure mode a retry could fix,
and that batch is now reported terminal after one attempt."
```

---

### Task 4: Document the retry rule

**Files:**
- Modify: `README.md` (insert a `### Sink retries` subsection at the end of `## Sinks`, before `## Error policies` at line 537)
- Modify: `docs/coverage/invariants.yml:117-126` (`sink.error.classifies` claim)
- Modify: `docs/superpowers/specs/2026-09-12-sink-encode-failure-design.md` (Docs section)

**Interfaces:**
- Consumes: the rule from Task 2 and the code from Task 1. The README table must match the `retryable` doc comment line for line in meaning.

- [ ] **Step 1: Add the README subsection**

Insert immediately before `## Error policies` in `README.md`:

````markdown
### Sink retries

ClickHouse and Iceberg flushes retry when the destination is not answering.
Omit the block to accept the defaults. Set `max_attempts: 1` to turn retrying
off. The Kafka sink ignores this block: franz-go already retries a produce
with its own backoff.

```yaml
sink:
  type: clickhouse
  retry:
    max_attempts: 4           # total attempts, including the first
    initial_backoff_ms: 100   # doubles each attempt
    max_backoff_ms: 2000      # ceiling on the backoff
    deadline_seconds: 10      # bounds the whole ladder, not one attempt
```

The values shown are the defaults, from `internal/sinks/policy.go`.

Keep `deadline_seconds` below `pipeline.flush_interval_seconds`. The retry
runs inside the open state transaction, and a ladder that outlives the flush
interval freezes the window clock.

The ladder retries only what another attempt could change. The error code
decides:

| Error | Retried | Why |
| --- | --- | --- |
| Any `user.*` code, including `user.sink.encode_failed` and `user.sink.type_unsupported` | No | The config, SQL or a value is wrong. It fails identically every time. |
| `system.sink.write_failed` | No | The destination answered and refused the write. |
| `system.sink.unreachable` | Yes | The destination may come back. |
| An error with no code | Yes | A driver's timeout or reset arrives unclassified. The deadline bounds the cost. |
| Any other `system.*` code | Yes | |

A failure that is not retried keeps its own code and exit code. A retried
failure that outlasts the ladder is reported as `system.sink.unreachable`,
exit 12, and `sink_retry_count_total` counts each attempt after the first.

`user.sink.encode_failed` is the sink's client refusing a value before
anything reaches the network, such as a timestamp string the ClickHouse
driver's `DateTime` layout cannot parse. Cast or format the column in the
handler SQL. The message names the column and quotes the value.
````

- [ ] **Step 2: Update the invariant claim**

In `docs/coverage/invariants.yml`, replace the `sink.error.classifies` claim:

```yaml
    claim: >
      The sink's errors classify as unreachable, rejected, or a user fault,
      so the retry ladder retries only the first. A value the sink's client
      cannot encode is a user fault: it never reaches the network and fails
      the same way every attempt.
```

Run: `go test ./internal/coverage/ ./internal/sinks/ -run 'Coverage|Invariant|Conformance' 2>&1 | tail -5`
Expected: PASS or no matching tests. If a test parses `invariants.yml` and fails on the edit, the YAML is malformed; fix the indentation.

- [ ] **Step 3: Correct the spec's Docs section**

In `docs/superpowers/specs/2026-09-12-sink-encode-failure-design.md`, replace the Docs section with:

```markdown
### Docs

README gains a "Sink retries" subsection under Sinks. It documents the retry
block, which the README did not cover, and states the retry rule as a table
keyed by error code. The `retryable` doc comment carries the same table and
names the README section, so a change to one points at the other.

The `sink.error.classifies` invariant claim names the third class.

Release notes live in the annotated tag, not in `CHANGELOG.md`, which stops
at v1.0.0. The v1.2.1 tag message names the defect: an encode failure was
retried and then reported as unreachable, and any user-class sink error took
the same path. It names the new code and the retry rule.

Nothing renders `errs.All()` into documentation today, so the registry entry
is the code's only published description.
```

- [ ] **Step 4: Read the README section once as an operator**

Open `README.md` at the new subsection and check: every code named in the table exists in `internal/errs/testdata/codes.golden`; the exit codes 10 and 12 match `internal/errs/exit.go`; the metric name matches the `## Metrics` table at README line 716.

- [ ] **Step 5: Commit**

```bash
git add README.md docs/coverage/invariants.yml docs/superpowers/specs/2026-09-12-sink-encode-failure-design.md
git commit -m "docs: the sink retry ladder, keyed by error code

The README did not document sink.retry at all, and nothing stated which
failures the ladder retries. An operator reading system.sink.unreachable
after four attempts on a bad value had no way to learn the rule (#233).

Sink retries is a new README subsection: the config block, the deadline's
relation to the flush interval, and a table of what is retried by error
code. The retryable doc comment carries the same table and names the
section. The sink.error.classifies invariant names the user-fault class.

What breaks if this is wrong: the README and the code disagree about what
is retried, and the operator trusts the README."
```

---

### Task 5: Verify the whole change

**Files:** none modified.

- [ ] **Step 1: Run the full test suite**

Run: `go test ./... 2>&1 | grep -v '^ok' | grep -v 'no test files'`
Expected: no output. Anything printed is a FAIL or a build error to fix before finishing.

- [ ] **Step 2: Run vet**

Run: `go vet ./internal/errs/ ./internal/sinks/`
Expected: no output.

- [ ] **Step 3: Confirm the spec's acceptance by hand, if a ClickHouse is up**

Build: `make build` (or `go build -o bin/sqlflow ./cmd/sqlflow`; check the Makefile for the binary name). Create a table `sf_tz` with a `DateTime` column named `dt_plain` and a `String` column `label`. Run the issue's config from the spec with one message `{"label":"isoz","ts":"2026-09-01T12:00:00Z"}` on the topic.

Expected on stderr, once, with no `sink retry` log lines before it:

```
Error: [user.sink.encode_failed] clickhouse sink: encode row: clickhouse [AppendRow]: dt_plain parsing time "2026-09-01T12:00:00Z" as "2006-01-02 15:04:05": cannot parse "T12:00:00Z" as " "
```

And `echo $?` prints `10`.

If no ClickHouse is available, say so in the final report. Do not report the acceptance as passed.

- [ ] **Step 4: Report**

State which tests ran, which skipped, and whether the live acceptance ran. Paste the `go test ./...` summary line for `internal/sinks` and `internal/errs`.
