# Kafka Sink Invariants Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prove nine of the ten sink invariants for `sink.kafka`, by teaching the
conformance harness the at-least-once contract and fixing the two Kafka sink
defects that are not about delivery semantics.

**Architecture:** The conformance harness runs one sequence per sink subject and
judges each invariant as its own subtest, emitting a marker the coverage
generator reads. This plan extends that sequence with a pre-warm flush and three
new judgments, replaces its exactly-once read-back comparison with a subsequence
check, and fixes the Kafka sink's unbounded flush and uncoded error.

**Tech Stack:** Go 1.x, franz-go v1.20.7, testcontainers-go, toxiproxy 2.12.0,
Arrow v18, `zeebo/assert`.

**Spec:** `docs/superpowers/specs/2026-09-08-kafka-sink-invariants-design.md`

## Global Constraints

- **At-least-once is the contract.** A row reaches the destination one or more
  times and is never lost. Assertions must fail loss and reordering, never
  duplication.
- **Prose follows Google Technical Writing One**, per `CLAUDE.md`: active voice,
  one idea per sentence, no hedging. Comments explain why, not what.
- **Commit messages name the defect, the fix, and the evidence**, and state what
  breaks if the change is wrong.
- **No attribution lines** in commit messages.
- `kgo.RecordDeliveryTimeout` **rejects any value below 1s** — franz-go returns
  `record timeout 500ms is less than allowed 1s` at client construction.
- A skip is not coverage. When the harness skips a judgment,
  `integrations.yml` must carry the matching exemption with a `reason` and a
  `proven_by` naming a real test, or `scripts/coverage_matrix.py` reports a
  problem.
- Unit pass: `go test -short ./internal/...`. Full gate: `make test-go`
  (build, vet, gofmt, `go test ./...` — which **does** run the container-backed
  integration tests, so Docker must be up).

---

## Task 1: Remove `Batch()` from the sink interface

`Batch()` has no caller. `internal/conformance/pipeline.go:781` is the only call
site outside tests and it is a forwarder in the harness's own double. Removing
it is mechanical and the compiler finds every site.

**Files:**
- Modify: `internal/core/turbine.go:83` (interface)
- Modify: `internal/sinks/init.go:27`, `console.go:84`, `kafka.go:173`,
  `sqlcommand.go:49`, `clickhouse.go:149`, `iceberg.go:71`, `retry.go:75`
- Modify: `internal/local/console.go:34`
- Modify: `internal/conformance/pipeline.go:777-783`
- Test: `internal/conformance/conformance_test.go:233,347`,
  `internal/managers/tumbling_test.go:111`, `internal/core/turbine_test.go:123,225,569`,
  `internal/sinks/probe_test.go:27`, `internal/sinks/retry_test.go:38`
- Test (assertions to delete): `internal/sinks/structural_test.go:67`,
  `iceberg_test.go:320,328`, `clickhouse_test.go:88`, `kafka_test.go:155,163`,
  `sqlcommand_test.go:303,311`

**Interfaces:**
- Consumes: nothing.
- Produces: `core.Sink` becomes `WriteTable(ctx, arrow.Table) error`,
  `Flush(ctx) error`. Every later task depends on this shape.

- [ ] **Step 1: Confirm the method has no production caller**

Run: `rg -n "\.Batch\(\)" --type go . | grep -v "_test.go"`

Expected: exactly two lines, both in `internal/conformance/pipeline.go` — the
`recordingSink.Batch` definition and its forwarding call. If anything else
appears, stop: a caller exists and this task's premise is wrong.

- [ ] **Step 2: Delete the method from the interface**

In `internal/core/turbine.go`, remove the `Batch() (arrow.Table, error)` line
from the `Sink` interface so it reads:

```go
type Sink interface {
	WriteTable(ctx context.Context, batch arrow.Table) error
	Flush(ctx context.Context) error
}
```

- [ ] **Step 3: Run the build to enumerate every implementation**

Run: `go build ./... 2>&1 | head -40`

Expected: the build succeeds. Removing a method from an interface does not break
implementations — they simply have an extra method. This step confirms the
interface change compiles before the deletions begin.

- [ ] **Step 4: Delete every `Batch` method**

Delete the whole method, and its doc comment, from each of:
`internal/sinks/init.go`, `console.go`, `kafka.go`, `sqlcommand.go`,
`clickhouse.go`, `iceberg.go`, `retry.go`, `internal/local/console.go`,
`internal/conformance/pipeline.go`.

In `internal/sinks/kafka.go`, also delete the now-unused `batch arrow.Table`
field from `KafkaSink` and the `s.batch = batch` assignment in `WriteTable`.

In `internal/sinks/console.go`, delete the `batch` field and its assignment the
same way.

- [ ] **Step 5: Delete the test doubles' methods and the tests that only test `Batch`**

Delete the `Batch` method from the doubles at
`internal/conformance/conformance_test.go:233,347`,
`internal/managers/tumbling_test.go:111`,
`internal/core/turbine_test.go:123,225,569`,
`internal/sinks/probe_test.go:27`, `internal/sinks/retry_test.go:38`.

Delete these tests outright — each exists only to assert `Batch`'s return value:
`TestSinkIceberg_BatchIsTheLastWrite` (`iceberg_test.go`),
`TestSinkKafka_BatchIsTheLastWrite` (`kafka_test.go`), and the equivalent in
`sqlcommand_test.go` and `clickhouse_test.go`.

In `internal/sinks/structural_test.go`, `TestSinkNoop_DeliversNothingAndSaysSo`
keeps its other assertions; delete only these three lines:

```go
	batch, err := s.Batch()
	assert.NoError(t, err)
	assert.Nil(t, batch)
```

- [ ] **Step 6: Verify nothing references it**

Run: `rg -n "Batch\(\)" --type go . ; go build ./...`

Expected: no matches, and a clean build.

- [ ] **Step 7: Run the unit pass**

Run: `go test -short ./internal/...`

Expected: PASS for every package.

- [ ] **Step 8: Commit**

```bash
git add -A
git commit -F - <<'EOF'
refactor: delete Batch() from the sink interface

Batch() had no caller. The only call site outside tests was a forwarder in the
conformance harness's own double, and no non-test caller has ever existed in
the Go tree -- checked at 7d27a2b, cd6303b and d193f6a. The comment naming the
tumbling-window manager as its reader was wrong: that manager calls WriteTable
and Flush only.

The method came from the Python engine, where pipeline.py logged the rows a
sink still held when flush() raised. The Go rewrite carried the method and
dropped the caller.

Six sinks implemented it three different ways, and an invariant guarded the
disagreement. Deleting the method retires that invariant in the next commit.

If this is wrong, a caller exists that the compiler cannot see, which would
mean a sink reached through reflection.
EOF
```

---

## Task 2: Retire `sink.batch.reports_buffer`

`scripts/coverage_matrix.py:157-162` fails when an exemption names an invariant
`invariants.yml` does not declare, so the declaration and `sink.noop`'s
exemption must go in the same commit.

**Files:**
- Modify: `docs/coverage/invariants.yml:99-105`
- Modify: `docs/coverage/integrations.yml:83-85`
- Modify: `docs/coverage/matrix.md` (regenerated, not hand-edited)

**Interfaces:**
- Consumes: Task 1's deletion.
- Produces: the invariant id `sink.batch.reports_buffer` no longer exists.
  No later task may reference it.

- [ ] **Step 1: Delete the invariant declaration**

Remove this block from `docs/coverage/invariants.yml`:

```yaml
  - id: sink.batch.reports_buffer
    family: resilience
    class: safety
    applies_to: sink
    claim: Batch returns what is buffered, and nil when nothing is.
    verified_by: harness
    requires: []
```

- [ ] **Step 2: Delete the exemption that names it**

Remove this block from `sink.noop`'s `exempt` list in
`docs/coverage/integrations.yml`:

```yaml
      - invariant: sink.batch.reports_buffer
        reason: has no destination; discarding every row is the contract
        proven_by: TestSinkNoop_DeliversNothingAndSaysSo
```

- [ ] **Step 3: Run the registry validation**

Run: `go test -short ./internal/coverage/...`

Expected: PASS. This confirms no Go test still asserts the retired id.

- [ ] **Step 4: Regenerate the matrix**

Run: `make coverage-matrix`

Expected: `docs/coverage/matrix.md` loses the `sink.batch.reports_buffer` row,
and the summary sentence drops from 31 declared invariants to 30 and from 134
cells to 128.

- [ ] **Step 5: Confirm the row is gone**

Run: `rg -n "batch.reports_buffer" docs/ internal/ scripts/`

Expected: no matches.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -F - <<'EOF'
coverage: retire sink.batch.reports_buffer with the method it guarded

The invariant claimed Batch() returns what is buffered and nil when nothing is.
The previous commit deleted Batch(), so the claim has no subject.

sink.noop's exemption goes with it: coverage_matrix.py rejects an exemption
naming an invariant the registry does not declare, so leaving it would fail the
gate rather than the review.

The matrix drops from 31 invariants to 30 and from 134 cells to 128. If this is
wrong, a cell that was measuring something real is now unmeasured -- but every
one of its six cells read missing or exempt, so nothing was.
EOF
```

---

## Task 3: Judge read-back as at-least-once

The harness currently requires the destination to hold exactly one row. That is
exactly-once, and it fails a sink behaving to contract. Replace it with a
subsequence check, and prove the looser check still catches loss and reordering.

**Files:**
- Modify: `internal/conformance/conformance.go` (add `deliveredInOrder`)
- Test: `internal/conformance/conformance_test.go` (add three doubles and their
  tests)

**Interfaces:**
- Consumes: `Row` (`map[string]any`), `describe([]Row) string`,
  `format(any) string` — all existing in `conformance.go`.
- Produces: `func deliveredInOrder(got, want []Row) string` — returns `""` when
  the read-back honours at-least-once, otherwise a sentence naming the failure.
  Tasks 4 and 8 call it.

- [ ] **Step 1: Write the failing tests**

Add to `internal/conformance/conformance_test.go`:

```go
// At-least-once permits a repeat. Exact equality does not, which is why the
// harness used to fail a sink that delivered correctly twice.
func TestToolingConformanceDelivered_AllowsARepeat(t *testing.T) {
	coverage.Covers(t, "tooling.conformance")
	want := []Row{{"id": int64(1)}, {"id": int64(2)}}
	got := []Row{{"id": int64(1)}, {"id": int64(2)}, {"id": int64(2)}}

	assert.Equal(t, "", deliveredInOrder(got, want))
}

// Loss is the failure the contract does not permit.
func TestToolingConformanceDelivered_CatchesALostRow(t *testing.T) {
	coverage.Covers(t, "tooling.conformance")
	want := []Row{{"id": int64(1)}, {"id": int64(2)}, {"id": int64(3)}}
	got := []Row{{"id": int64(1)}, {"id": int64(3)}}

	assert.True(t, strings.Contains(deliveredInOrder(got, want), "never reached"))
	assert.True(t, strings.Contains(deliveredInOrder(got, want), "id=2"))
}

// And so is reordering. A row that arrives before one that preceded it is not
// a duplicate of anything, so a set comparison would miss it.
func TestToolingConformanceDelivered_CatchesAReorder(t *testing.T) {
	coverage.Covers(t, "tooling.conformance")
	want := []Row{{"id": int64(1)}, {"id": int64(2)}, {"id": int64(3)}}
	got := []Row{{"id": int64(1)}, {"id": int64(3)}, {"id": int64(2)}}

	assert.True(t, strings.Contains(deliveredInOrder(got, want), "out of order"))
}

// An empty destination loses everything, and the message must say so rather
// than blaming order.
func TestToolingConformanceDelivered_CatchesAnEmptyDestination(t *testing.T) {
	coverage.Covers(t, "tooling.conformance")
	want := []Row{{"id": int64(1)}}

	assert.True(t, strings.Contains(deliveredInOrder(nil, want), "never reached"))
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test -short ./internal/conformance/ -run TestToolingConformanceDelivered -v`

Expected: FAIL to compile with `undefined: deliveredInOrder`.

- [ ] **Step 3: Implement `deliveredInOrder`**

Add to `internal/conformance/conformance.go`:

```go
// deliveredInOrder judges a read-back against at-least-once delivery.
//
// sqlflow delivers at-least-once end to end: a row reaches the destination one
// or more times and is never lost. So this asks whether want appears in got as
// a subsequence. A repeat passes, because the pipeline commits offsets only
// after a flush returns nil and replays the batch when it does not. A missing
// row fails, and so does one that overtook a row written before it.
//
// It returns "" when the read-back honours the contract, and a sentence naming
// the failure otherwise.
func deliveredInOrder(got, want []Row) string {
	next := 0
	for _, w := range want {
		found := -1
		for j := next; j < len(got); j++ {
			if got[j]["id"] == w["id"] {
				found = j
				break
			}
		}
		if found >= 0 {
			next = found + 1
			continue
		}

		// Absent from the rest of the read-back. Either it never arrived, or
		// it arrived before a row that was written before it -- two different
		// defects, and the message has to send the reader to the right one.
		if indexOf(got, w) >= 0 {
			return "id=" + format(w["id"]) + " reached the destination before a " +
				"row written earlier; want " + describe(want) + " in order, got " +
				describe(got)
		}
		return "id=" + format(w["id"]) + " never reached the destination; want " +
			describe(want) + ", got " + describe(got)
	}
	return ""
}

func indexOf(rows []Row, want Row) int {
	for i, r := range rows {
		if r["id"] == want["id"] {
			return i
		}
	}
	return -1
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test -short ./internal/conformance/ -run TestToolingConformanceDelivered -v`

Expected: PASS, all four.

- [ ] **Step 5: Add the doubles that violate the contract**

Add to `internal/conformance/conformance_test.go`, beside the existing doubles:

```go
// losingSink delivers everything except its most recent row. At-least-once
// permits a repeat; it does not permit a row that never arrives.
type losingSink struct{ *memSink }

func (l *losingSink) Flush(context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.down {
		return errors.New("destination unreachable")
	}
	for i, t := range l.buffered {
		if i < len(l.buffered)-1 {
			l.delivered = append(l.delivered, ids(t)...)
		}
		t.Release()
	}
	l.buffered = nil
	return nil
}

// reorderingSink delivers its buffer backwards. Kafka orders within a
// partition, and a sink that requeues a failed row behind a later one breaks
// that without losing anything, so a set comparison would call it correct.
type reorderingSink struct{ *memSink }

func (r *reorderingSink) Flush(context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.down {
		return errors.New("destination unreachable")
	}
	for i := len(r.buffered) - 1; i >= 0; i-- {
		r.delivered = append(r.delivered, ids(r.buffered[i])...)
		r.buffered[i].Release()
	}
	r.buffered = nil
	return nil
}

// duplicatingSink delivers every row twice. It is the positive control: the
// harness must pass it, or the contract has been tightened by accident.
type duplicatingSink struct{ *memSink }

func (d *duplicatingSink) Flush(context.Context) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.down {
		return errors.New("destination unreachable")
	}
	for _, t := range d.buffered {
		rows := ids(t)
		d.delivered = append(d.delivered, rows...)
		d.delivered = append(d.delivered, rows...)
		t.Release()
	}
	d.buffered = nil
	return nil
}
```

- [ ] **Step 6: Run the unit pass**

Run: `go test -short ./internal/conformance/`

Expected: PASS. The doubles are not wired into a sequence yet, so nothing else
changes.

- [ ] **Step 7: Commit**

```bash
git add -A
git commit -F - <<'EOF'
conformance: judge a read-back as at-least-once, not exactly-once

sinkVerdicts required the destination to hold exactly one row. sqlflow delivers
at-least-once end to end -- turbine.go commits offsets only after a flush
returns nil and replays the batch when it does not, and the tumbling manager
keeps a window in DuckDB until its flush succeeds -- so a repeat is correct and
the assertion failed sinks that behave to contract.

deliveredInOrder asks whether the written rows appear as a subsequence of the
read-back. A repeat passes. A lost row fails, and so does one that overtook a
row written before it, with a different message for each so the reader lands in
the right file.

Three doubles come with it: one that loses a row, one that reorders, and one
that duplicates as the positive control.

If the subsequence check is too loose, a sink that silently drops rows passes
the matrix -- which is why the losing double is part of this commit rather than
a later one.
EOF
```

---

## Task 4: Use the at-least-once judgment in the sequence

**Files:**
- Modify: `internal/conformance/conformance.go:269-275` (`sinkVerdicts` tail)
- Modify: `docs/coverage/invariants.yml` (claims for `keeps_batch` and
  `preserves_order`)
- Test: `internal/conformance/conformance_test.go`

**Interfaces:**
- Consumes: `deliveredInOrder(got, want []Row) string` from Task 3.
- Produces: `sinkVerdicts` still returns `[]verdict`; its `keepsBatch` verdict
  is now judged by `deliveredInOrder`.

- [ ] **Step 1: Write the failing test**

Add to `internal/conformance/conformance_test.go`:

```go
// The contract's positive control, run through the whole harness rather than
// the comparison alone: a sink that delivers each row twice is conformant.
func TestToolingConformanceSinks_ADuplicatingSinkPasses(t *testing.T) {
	coverage.Covers(t, "tooling.conformance")
	vs := verdicts(t, subject(&duplicatingSink{memSink: newMemSink()}))

	assert.Equal(t, "", vs[keepsBatch].failure)
	assert.Equal(t, "", vs[buffersOnly].failure)
}

// And a sink that loses the row it could not deliver still fails.
func TestToolingConformanceSinks_ALosingSinkIsCaught(t *testing.T) {
	coverage.Covers(t, "tooling.conformance")
	v := verdicts(t, subject(&losingSink{memSink: newMemSink()}))[keepsBatch]

	assert.True(t, strings.Contains(v.failure, "never reached"))
}
```

- [ ] **Step 2: Run to verify the duplicating case fails**

Run: `go test -short ./internal/conformance/ -run "TestToolingConformanceSinks_(ADuplicatingSinkPasses|ALosingSinkIsCaught)" -v`

Expected: `ADuplicatingSinkPasses` FAILS, reporting that the destination holds
`rows [id=1, id=1]` when exactly one was wanted. That failure is the defect this
task fixes.

- [ ] **Step 3: Replace the exact-equality check**

In `internal/conformance/conformance.go`, replace:

```go
	got := s.ReadBack(t)
	if keeps.failure == "" && (len(got) != 1 || got[0]["id"] != int64(1)) {
		keeps.failure = "after a failed Flush and a successful retry the " +
			"destination holds " + describe(got) +
			"; want exactly the row the failed flush could not deliver"
	}
	return []verdict{buffers, keeps, depth}
```

with:

```go
	got := s.ReadBack(t)
	if keeps.failure == "" {
		if bad := deliveredInOrder(got, []Row{{"id": int64(1)}}); bad != "" {
			keeps.failure = "after a failed Flush and a successful retry, " + bad
		}
	}
	return []verdict{buffers, keeps, depth}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test -short ./internal/conformance/`

Expected: PASS, including both new tests and every existing double.

- [ ] **Step 5: Record the contract in the registry**

In `docs/coverage/invariants.yml`, replace the `sink.flush.keeps_batch` claim
with:

```yaml
    claim: >
      A failed Flush leaves every undelivered row buffered. The next Flush
      re-attempts them. Delivery is at-least-once, so a row that arrives twice
      holds the claim and a row that never arrives breaks it.
```

and the `sink.flush.preserves_order` claim with:

```yaml
    claim: >
      Rows reach the destination in WriteTable order, across a retry. Repeats
      are permitted, because delivery is at-least-once; a row that overtakes one
      written before it is not.
```

- [ ] **Step 6: Regenerate the matrix and run the unit pass**

Run: `make coverage-matrix && go test -short ./internal/...`

Expected: `matrix.md` shows the new claim text; all packages PASS.

- [ ] **Step 7: Commit**

```bash
git add -A
git commit -F - <<'EOF'
conformance: keeps_batch is judged against the contract it belongs to

The verdict now runs deliveredInOrder rather than requiring the destination to
hold exactly one row, and invariants.yml says so in the claim: at-least-once
means a repeat holds keeps_batch and a missing row breaks it.

Two tests hold the boundary. A sink that delivers every row twice passes; a
sink that loses the row it could not deliver fails with "never reached".

Writing the contract into the claim is what stops the next person loosening a
sink to satisfy an assertion that was stricter than the engine ever promised.
EOF
```

---

## Task 5: Make two subjects witness delivery order

`SinkSubject.ReadBack` is documented to return rows "in delivery order". The
ClickHouse and sqlcommand subjects sort by id instead, so a sink that delivered
`[C, B, A]` reads back `[A, B, C]` and passes. Task 8 judges `preserves_order`
and cannot until this is fixed.

**Files:**
- Modify: `internal/sinks/conformance_test.go:91,110-111` (ClickHouse DDL and
  read-back)
- Modify: `internal/sinks/conformance_local_test.go:95,107,117` (sqlcommand DDL,
  sink SQL and read-back)

**Interfaces:**
- Consumes: nothing.
- Produces: both subjects' `ReadBack` returns rows in arrival order.

- [ ] **Step 1: Give the ClickHouse conformance table an arrival column**

In `internal/sinks/conformance_test.go`, replace the DDL:

```go
	assert.NoError(t, direct.conn.Exec(ctx,
		"CREATE TABLE "+table+" (id Int64) ENGINE = MergeTree() ORDER BY id"))
```

with one that stamps arrival and orders by it:

```go
	// arrived orders the read-back by when a row landed rather than by its id.
	// ORDER BY id sorts, and a sink that delivered [3, 2, 1] would read back
	// [1, 2, 3] and satisfy preserves_order without ever preserving anything.
	// MergeTree's own ORDER BY stays on id: it is the storage key, not the
	// read order.
	assert.NoError(t, direct.conn.Exec(ctx,
		"CREATE TABLE "+table+" (id Int64, arrived DateTime64(9) DEFAULT now64(9)) "+
			"ENGINE = MergeTree() ORDER BY id"))
```

- [ ] **Step 2: Read it back in arrival order**

In the same file, change the read-back query:

```go
			rows, err := direct.conn.Query(context.Background(),
				"SELECT id FROM "+table+" ORDER BY arrived, id")
```

- [ ] **Step 3: Give the sqlcommand target the same treatment**

In `internal/sinks/conformance_local_test.go`, replace the target DDL:

```go
	exec(t, conn, "CREATE TABLE "+target+" (id BIGINT)")
```

with:

```go
	// seq orders the read-back by arrival. ORDER BY id sorts, which would let a
	// sink that delivered rows backwards pass preserves_order.
	exec(t, conn, "CREATE SEQUENCE "+seq+" START 1")
	exec(t, conn, "CREATE TABLE "+target+
		" (id BIGINT, arrived BIGINT DEFAULT nextval('"+seq+"'))")
```

Declare `seq` beside `target` and `gate`:

```go
	seq := fmt.Sprintf("conformance_seq_%d", stamp)
```

- [ ] **Step 4: Name the column the sink writes**

The sink's SQL now inserts into a table with two columns, so name the one it
fills. Replace:

```go
			s, err := NewSQLCommandSink(conn,
				"INSERT INTO "+target+" SELECT b.id FROM "+sinkBatchTable+" b, "+gate, nil)
```

with:

```go
			s, err := NewSQLCommandSink(conn,
				"INSERT INTO "+target+" (id) SELECT b.id FROM "+sinkBatchTable+" b, "+gate, nil)
```

- [ ] **Step 5: Read it back in arrival order**

```go
			ReadBack: func(t *testing.T) []conformance.Row {
				return queryIDs(t, conn, "SELECT id FROM "+target+" ORDER BY arrived")
			},
```

- [ ] **Step 6: Run the sqlcommand subject**

Run: `go test -short ./internal/sinks/ -run TestSinkSqlcommand_Conformance -v`

Expected: PASS, with its three invariant subtests still passing.

- [ ] **Step 7: Run the ClickHouse subject**

Run: `go test ./internal/sinks/ -run TestIntegrationSinkClickhouse_Conformance -v`

Expected: PASS. Needs Docker. If ClickHouse rejects `now64(9)` as a DEFAULT,
use `DateTime64(3) DEFAULT now64(3)` — millisecond resolution still orders three
rows written seconds apart.

- [ ] **Step 8: Commit**

```bash
git add -A
git commit -F - <<'EOF'
conformance: two subjects sorted their read-back instead of observing it

SinkSubject.ReadBack is documented to return rows in delivery order. The
ClickHouse and sqlcommand subjects both queried ORDER BY id, which sorts. A
sink that delivered [3, 2, 1] read back [1, 2, 3], so preserves_order would
have gone green on a sink that preserves nothing.

Each conformance table gains an arrival column -- now64 for ClickHouse, a
sequence default for DuckDB -- and the read-back orders by it. The subject owns
its DDL, so no sink changes.

Without this, the preserves_order cell the next commit adds would be a false
positive on two of the five columns.
EOF
```

---

## Task 6: Pre-warm the destination and judge `flush.empty_is_noop`

The sequence breaks the destination before it has ever been written to
successfully. A sink that only loses rows after its first successful flush is
invisible to it, and for Kafka the unresolved topic changes franz-go's behaviour
entirely.

**Files:**
- Modify: `internal/conformance/conformance.go` (`sinkVerdicts`, constants)
- Test: `internal/conformance/conformance_test.go` (`verdicts` helper count)

**Interfaces:**
- Consumes: `deliveredInOrder` from Task 3.
- Produces: `sinkVerdicts` returns four verdicts. The constant
  `emptyIsNoop = "sink.flush.empty_is_noop"` is added.

- [ ] **Step 1: Write the failing test**

Add to `internal/conformance/conformance_test.go`:

```go
// A Flush with nothing buffered must not reach the destination. A sink that
// writes on every flush turns an idle interval tick into a spurious write.
type eagerFlushSink struct{ *memSink }

func (e *eagerFlushSink) Flush(context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.down {
		return errors.New("destination unreachable")
	}
	if len(e.buffered) == 0 {
		// Nothing buffered, and it writes anyway.
		e.delivered = append(e.delivered, 0)
		return nil
	}
	for _, t := range e.buffered {
		e.delivered = append(e.delivered, ids(t)...)
		t.Release()
	}
	e.buffered = nil
	return nil
}

func TestToolingConformanceSinks_AnEagerEmptyFlushIsCaught(t *testing.T) {
	coverage.Covers(t, "tooling.conformance")
	v := verdicts(t, subject(&eagerFlushSink{memSink: newMemSink()}))[emptyIsNoop]

	assert.True(t, strings.Contains(v.failure, "nothing was buffered"))
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `go test -short ./internal/conformance/ -run TestToolingConformanceSinks_AnEagerEmptyFlushIsCaught -v`

Expected: FAIL to compile with `undefined: emptyIsNoop`.

- [ ] **Step 3: Add the constant**

In `internal/conformance/conformance.go`, extend the block:

```go
const (
	buffersOnly  = "sink.write.buffers_only"
	keepsBatch   = "sink.flush.keeps_batch"
	reportsDepth = "sink.buffer.reports_depth"
	emptyIsNoop  = "sink.flush.empty_is_noop"
)
```

- [ ] **Step 4: Add the pre-warm and the empty-flush judgment**

In `sinkVerdicts`, replace the opening — everything from `sink := s.New(t)` down
to the `buffers := verdict{...}` block — with:

```go
	sink := s.New(t)
	ctx := context.Background()

	// Step 0. A flush with nothing buffered must return nil and reach nothing.
	// The pipeline flushes on an interval whether or not a batch arrived, so a
	// sink that writes here writes on every idle tick.
	empty := verdict{invariant: emptyIsNoop}
	if err := sink.Flush(ctx); err != nil {
		empty.failure = "Flush with nothing buffered returned " + err.Error() +
			"; an idle flush interval must not fail the pipeline"
	} else if got := s.ReadBack(t); len(got) != 0 {
		empty.failure = "the destination holds " + describe(got) +
			" after a Flush when nothing was buffered"
	}

	// Step 1. One clean delivery, so every judgment below runs against a
	// destination in the state production runs against. The Kafka sink's
	// transport behaves differently before a topic has ever resolved, and a
	// sink that only loses rows after its first success is invisible to a
	// sequence that never has one.
	warm := s.Table(t, 1)
	if err := sink.WriteTable(ctx, warm); err != nil {
		warm.Release()
		t.Fatalf("conformance: WriteTable during the pre-warm: %v", err)
	}
	warm.Release()
	if err := sink.Flush(ctx); err != nil {
		t.Fatalf("conformance: the pre-warm Flush failed against a healthy "+
			"destination: %v", err)
	}

	row := s.Table(t, 2)
	defer row.Release()

	if err := sink.WriteTable(ctx, row); err != nil {
		t.Fatalf("conformance: WriteTable before any fault: %v", err)
	}

	// Judged before anything can go wrong, so a write-through sink is named
	// for what it did rather than for a downstream symptom.
	buffers := verdict{invariant: buffersOnly}
	if got := s.ReadBack(t); len(got) != 1 {
		buffers.failure = "the destination holds " + describe(got) +
			" after WriteTable and before any Flush; only Flush may deliver, " +
			"because the pipeline commits offsets on what Flush reports"
	}
```

- [ ] **Step 5: Update every downstream reference to the row id**

The judged row is now `id=2`, and the pre-warm row `id=1` is already delivered.
In the same function, change the final read-back check to expect both:

```go
	got := s.ReadBack(t)
	if keeps.failure == "" {
		if bad := deliveredInOrder(got, []Row{{"id": int64(1)}, {"id": int64(2)}}); bad != "" {
			keeps.failure = "after a failed Flush and a successful retry, " + bad
		}
	}
	return []verdict{empty, buffers, keeps, depth}
```

Also update the `Break == nil` early return to carry the new verdict:

```go
	if s.Break == nil {
		skip := "the subject has nothing to break; exempt " +
			s.Integration + " in integrations.yml"
		return []verdict{
			{invariant: emptyIsNoop, skipped: skip},
			{invariant: buffersOnly, skipped: skip},
			{invariant: keepsBatch, skipped: skip},
			{invariant: reportsDepth, skipped: skip},
		}
	}
```

- [ ] **Step 6: Update the verdict-count assertion**

In `internal/conformance/conformance_test.go`, change `verdicts`:

```go
	assert.Equal(t, 4, len(out))
```

- [ ] **Step 7: Run the conformance unit pass**

Run: `go test -short ./internal/conformance/ -v`

Expected: PASS. `writeThroughSink` still fails `buffersOnly` — it now delivers
the pre-warm row plus the judged row, so the read-back holds two rows where one
was expected.

- [ ] **Step 8: Run the local sink subjects**

Run: `go test -short ./internal/sinks/ -run "Conformance" -v`

Expected: PASS for console and sqlcommand, each now reporting four invariant
subtests.

- [ ] **Step 9: Commit**

```bash
git add -A
git commit -F - <<'EOF'
conformance: warm the destination before faulting it, and judge an empty flush

The sequence broke the destination before it had ever been written to
successfully. That is not the state production runs in, and it hid real
behaviour: franz-go watches a Produce context only while a topic is unknown, so
the Kafka sink took an abort path that a resolved topic never reaches.

Step 0 flushes an empty buffer and requires nothing to reach the destination,
which is sink.flush.empty_is_noop -- the pipeline flushes on an interval
whether or not a batch arrived, so a sink that writes here writes on every idle
tick. Step 1 delivers one row cleanly. Everything after runs warm.

A sink that only loses rows after its first successful flush was invisible to
the old sequence.
EOF
```

---

## Task 7: Judge `flush.honours_context`

**Files:**
- Modify: `internal/conformance/conformance.go`
- Test: `internal/conformance/conformance_test.go`

**Interfaces:**
- Consumes: the sequence from Task 6.
- Produces: five verdicts; constant `honoursContext = "sink.flush.honours_context"`.

- [ ] **Step 1: Write the failing test**

```go
// A sink that ignores its context blocks the drain. The pipeline hands the
// drain a context stripped of cancellation, so a sink that only stops when
// cancelled never stops at all -- but a sink that returns success while the
// destination is broken is worse, and this double is the one that hangs.
type deafSink struct{ *memSink }

func (d *deafSink) Flush(ctx context.Context) error {
	if err := d.memSink.Flush(ctx); err != nil {
		// Ignores ctx entirely: sleeps past any deadline, then fails anyway.
		time.Sleep(2 * flushTimeout)
		return err
	}
	return nil
}

func TestToolingConformanceSinks_ASinkThatIgnoresItsContextIsCaught(t *testing.T) {
	coverage.Covers(t, "tooling.conformance")
	v := verdicts(t, subject(&deafSink{memSink: newMemSink()}))[honoursContext]

	assert.True(t, strings.Contains(v.failure, "did not return"))
}
```

This test takes as long as `flushTimeout` allows, so keep `flushTimeout` at its
current 10s and let the double sleep past it.

- [ ] **Step 2: Run to verify it fails**

Run: `go test -short ./internal/conformance/ -run TestToolingConformanceSinks_ASinkThatIgnoresItsContextIsCaught -v`

Expected: FAIL to compile with `undefined: honoursContext`.

- [ ] **Step 3: Add the constant and the judgment**

Add `honoursContext = "sink.flush.honours_context"` to the constant block.

In `sinkVerdicts`, replace the broken-flush section:

```go
	s.Break(t)
	broken, cancel := context.WithTimeout(ctx, flushTimeout)
	err := sink.Flush(broken)
	cancel()
```

with a version that times the call and judges the context:

```go
	s.Break(t)

	// Bounded well inside flushTimeout, so a sink that ignores its deadline is
	// caught by the outer bound rather than hanging the suite.
	deadline := flushTimeout / 4
	broken, cancel := context.WithTimeout(ctx, deadline)

	honours := verdict{invariant: honoursContext}
	start := time.Now()
	err := sink.Flush(broken)
	took := time.Since(start)
	cancel()

	if took > flushTimeout {
		honours.failure = "Flush did not return within " + flushTimeout.String() +
			" against a context that expired after " + deadline.String() +
			"; the pipeline's drain cannot bound a sink that ignores its context"
	} else if err == nil {
		// Left to the keeps_batch verdict below, which explains why a flush
		// into a broken destination succeeding is a delivery defect.
		honours.skipped = "Flush returned nil against a broken destination"
	} else if !errors.Is(err, context.DeadlineExceeded) {
		honours.failure = "Flush returned " + err.Error() +
			", which does not wrap context.DeadlineExceeded; a caller cannot " +
			"tell a sink that gave up on time from one that failed"
	}
```

Add `"errors"` and `"time"` to the imports if absent.

- [ ] **Step 4: Carry the verdict through both return paths**

Add `honours` to the final `return []verdict{...}`, to the early return after a
failed retry, and add `{invariant: honoursContext, skipped: skip}` to the
`Break == nil` list. Update the `verdicts` helper count to 5.

- [ ] **Step 5: Run the tests**

Run: `go test -short ./internal/conformance/ -v`

Expected: PASS, including the new double.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -F - <<'EOF'
conformance: judge that a flush honours the context it was given

The pipeline's drain hands the sink a context and every other caller passes one
that can expire. A sink that ignores it cannot be stopped: the flush interval
elapsing, a cancelled run and a SIGTERM all reach the sink through that context
and nowhere else.

The verdict times the flush against a broken destination and requires it to
return inside the bound, wrapping context.DeadlineExceeded so a caller can tell
a sink that gave up on time from one that failed outright. A double that sleeps
past its deadline proves the check catches the hang.

If this is wrong, the matrix certifies a sink that hangs shutdown until the
supervisor kills the process.
EOF
```

---

## Task 8: Judge `no_hollow_success` and `preserves_order`

**Files:**
- Modify: `internal/conformance/conformance.go`
- Modify: `internal/sinks/conformance_kafka_test.go` (single-partition topic)
- Test: `internal/conformance/conformance_test.go`

**Interfaces:**
- Consumes: the sequence from Task 7, `deliveredInOrder` from Task 3.
- Produces: seven verdicts; constants
  `noHollowSuccess = "sink.flush.no_hollow_success"` and
  `preservesOrder = "sink.flush.preserves_order"`.

- [ ] **Step 1: Write the failing tests**

```go
// hollowSink reports success on its second failed flush. That is #221's defect
// reached by a second route: the first flush records the failure, the second
// finds an empty error list and returns nil having delivered nothing.
type hollowSink struct {
	*memSink
	flushes int
}

func (h *hollowSink) Flush(ctx context.Context) error {
	h.mu.Lock()
	h.flushes++
	second := h.flushes == 2 && h.down
	h.mu.Unlock()
	if second {
		return nil
	}
	return h.memSink.Flush(ctx)
}

func TestToolingConformanceSinks_AHollowSuccessIsCaught(t *testing.T) {
	coverage.Covers(t, "tooling.conformance")
	v := verdicts(t, subject(&hollowSink{memSink: newMemSink()}))[noHollowSuccess]

	assert.True(t, strings.Contains(v.failure, "returned nil"))
}

func TestToolingConformanceSinks_AReorderingSinkIsCaught(t *testing.T) {
	coverage.Covers(t, "tooling.conformance")
	v := verdicts(t, subject(&reorderingSink{memSink: newMemSink()}))[preservesOrder]

	assert.True(t, strings.Contains(v.failure, "before a row written earlier"))
}
```

- [ ] **Step 2: Run to verify they fail**

Run: `go test -short ./internal/conformance/ -run "AHollowSuccessIsCaught|AReorderingSinkIsCaught" -v`

Expected: FAIL to compile — `noHollowSuccess` and `preservesOrder` undefined.

- [ ] **Step 3: Add the constants and the second broken flush**

Add both constants to the block. After the `honours` judgment and before
`s.Heal(t)`, insert:

```go
	// Step 4. A second row arrives while the destination is still broken, so
	// the retry has to deliver two rows in the order they were written.
	third := s.Table(t, 3)
	if err := sink.WriteTable(ctx, third); err != nil {
		third.Release()
		t.Fatalf("conformance: WriteTable while broken: %v", err)
	}
	third.Release()

	if reports && depth.failure == "" {
		if n := reporter.BufferedRows(); n != 2 {
			depth.failure = "reports " + strconv.Itoa(n) +
				" buffered rows after a failed Flush and a second WriteTable; want 2"
		}
	}

	// Step 5. Flushing again into the same fault must fail again. A sink that
	// clears its error state after reporting returns nil here having delivered
	// nothing, and the pipeline commits offsets for rows that never landed.
	hollow := verdict{invariant: noHollowSuccess}
	secondBroken, cancelSecond := context.WithTimeout(ctx, deadline)
	secondErr := sink.Flush(secondBroken)
	cancelSecond()
	if secondErr == nil && buffers.failure == "" {
		hollow.failure = "a second Flush into the same broken destination " +
			"returned nil; Flush may return nil only when every row since the " +
			"last success was acknowledged"
	}
```

- [ ] **Step 4: Judge order on the final read-back**

Replace the final read-back block with one that judges both invariants:

```go
	got := s.ReadBack(t)
	want := []Row{{"id": int64(1)}, {"id": int64(2)}, {"id": int64(3)}}

	order := verdict{invariant: preservesOrder}
	if bad := deliveredInOrder(got, want); bad != "" {
		// The same read-back answers both questions, and the message decides
		// which cell the reader is sent to: a row that never arrived is a lost
		// batch, and a row that overtook one written earlier is an ordering
		// defect.
		if strings.Contains(bad, "never reached") {
			if keeps.failure == "" {
				keeps.failure = "after a failed Flush and a successful retry, " + bad
			}
		} else {
			order.failure = bad
		}
	}
	return []verdict{empty, buffers, keeps, depth, honours, hollow, order}
```

Add `"strings"` to the imports.

- [ ] **Step 5: Carry both verdicts through the other return paths**

Add `{invariant: noHollowSuccess, skipped: skip}` and
`{invariant: preservesOrder, skipped: skip}` to the `Break == nil` list, and
both verdicts to the early return after a failed retry. Update the `verdicts`
helper count to 7.

- [ ] **Step 6: Run the tests**

Run: `go test -short ./internal/conformance/ -v`

Expected: PASS. `duplicatingSink` still passes both new verdicts.

- [ ] **Step 7: Pin the Kafka conformance topic to one partition**

franz-go's default `UniformBytesPartitioner` switches partition every 64 KiB,
and Kafka orders within a partition only. In
`internal/sinks/conformance_kafka_test.go`, create the topic explicitly before
the subject runs, replacing reliance on `AllowAutoTopicCreation`:

```go
	// One partition, because Kafka orders within a partition and nothing else.
	// On an auto-created multi-partition topic preserves_order would be
	// untestable rather than false.
	admin := kadm.NewClient(mustDirectClient(t, direct))
	defer admin.Close()
	if _, err := admin.CreateTopic(ctx, 1, 1, nil, topic); err != nil {
		t.Fatalf("create the conformance topic: %v", err)
	}
```

Add a helper beside `consumeIDs`:

```go
// mustDirectClient dials the broker directly, bypassing the proxy, so topic
// administration is never subject to the fault under test.
func mustDirectClient(t *testing.T, brokers []string) *kgo.Client {
	t.Helper()
	c, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	assert.NoError(t, err)
	t.Cleanup(c.Close)
	return c
}
```

Add `"github.com/twmb/franz-go/pkg/kadm"` to the imports, and run
`go mod tidy` if it is not already a dependency.

- [ ] **Step 8: Run the Kafka subject**

Run: `go test ./internal/sinks/ -run TestIntegrationSinkKafka_Conformance -v`

Expected: needs Docker. Record the outcome — this is the first run of the full
sequence against the real sink, and Task 10 depends on knowing which verdicts
fail. `flush.honours_context` is expected to pass; the sink already wraps
`context.DeadlineExceeded`.

- [ ] **Step 9: Commit**

```bash
git add -A
git commit -F - <<'EOF'
conformance: judge no_hollow_success and preserves_order on the same sequence

A second row now arrives while the destination is still broken, and a second
flush runs into the same fault. That second flush returning nil is the defect
#221 fixed for ClickHouse, reached by another route: a sink that clears its
error state after reporting delivers nothing and tells the pipeline it
succeeded, and the offsets move past rows that never landed.

The retry then has to deliver both rows in the order they were written. One
read-back answers both questions, and the message routes the reader: a row that
never arrived fails keeps_batch, a row that overtook one written earlier fails
preserves_order.

The Kafka conformance topic is created with one partition, because Kafka orders
within a partition and franz-go's default partitioner switches every 64 KiB. On
a multi-partition topic the ordering cell would be meaningless rather than
green.
EOF
```

---

## Task 9: Bound the Kafka flush with `RecordDeliveryTimeout`

No production caller gives `Flush` a context that expires. `managerCtx` is
`context.WithCancel(context.Background())` (`internal/cli/run/root.go:350`) and
the drain uses `context.WithoutCancel(ctx)`, which strips the deadline.
Measured: a deadline-free flush against an unreachable broker was still blocked
after 25 seconds.

**Files:**
- Modify: `internal/sinks/kafka.go` (`NewKafkaSink`)
- Test: `internal/sinks/kafka_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces: `KafkaSink` clients carry a delivery timeout. The existing
  `extra ...kgo.Opt` parameter still overrides it, because extras are appended
  last.

- [ ] **Step 1: Write the failing test**

Add to `internal/sinks/kafka_test.go`:

```go
// A flush with no deadline must still return.
//
// Every production caller passes one: the tumbling manager's context is
// cancellable but has no deadline, and the pipeline's drain strips the deadline
// with context.WithoutCancel. If only the context could stop a flush, an outage
// would stop a windowed pipeline publishing with nothing logged, and shutdown
// would wait for the supervisor to kill it.
func TestSinkKafka_FlushReturnsWithoutADeadline(t *testing.T) {
	coverage.Covers(t, "sink.kafka")

	// franz-go rejects a record timeout below 1s, so this is the floor rather
	// than a round number.
	s, err := NewKafkaSink(config.KafkaSink{
		Brokers: []string{unreachableBroker},
		Topic:   "sink-test",
	}, kgo.RecordDeliveryTimeout(time.Second))
	assert.NoError(t, err)
	t.Cleanup(func() { s.Close() })

	table := newTestTable(t, []string{"nyc"}, []int64{1})
	defer table.Release()
	assert.NoError(t, s.WriteTable(context.Background(), table))

	err = flushWithin(t, s, context.Background(), 10*time.Second)
	assert.Error(t, err)

	// And the row is still owed, so the next attempt re-sends it.
	assert.Equal(t, 1, s.BufferedRows())
}

// The default must be set, or the test above only proves the option works.
func TestSinkKafka_HasADeliveryTimeoutByDefault(t *testing.T) {
	coverage.Covers(t, "sink.kafka")
	s := newUnreachableKafkaSink(t)

	table := newTestTable(t, []string{"nyc"}, []int64{1})
	defer table.Release()
	assert.NoError(t, s.WriteTable(context.Background(), table))

	// Longer than recordDeliveryTimeout, so a sink with no default hangs here.
	err := flushWithin(t, s, context.Background(), recordDeliveryTimeout+10*time.Second)
	assert.Error(t, err)
}
```

Add `"github.com/twmb/franz-go/pkg/kgo"` to the test imports.

- [ ] **Step 2: Run to verify they fail**

Run: `go test ./internal/sinks/ -run "TestSinkKafka_(FlushReturnsWithoutADeadline|HasADeliveryTimeoutByDefault)" -v -timeout 120s`

Expected: `FlushReturnsWithoutADeadline` fails to compile
(`undefined: recordDeliveryTimeout` in the second test). After adding only the
constant, `HasADeliveryTimeoutByDefault` FAILS with "Flush did not return
within 30s, so it ignored its context".

- [ ] **Step 3: Add the constant and the option**

In `internal/sinks/kafka.go`:

```go
// recordDeliveryTimeout bounds how long franz-go may hold a record the broker
// has not acknowledged.
//
// franz-go retries a produce indefinitely by default, and no caller bounds the
// wait: the tumbling manager's context is cancellable but carries no deadline,
// and the pipeline's drain strips the deadline with context.WithoutCancel. So
// a flush against a hung broker blocked until the run was cancelled -- an
// outage stopped a windowed pipeline publishing with nothing logged, and
// shutdown waited for SIGKILL.
//
// Shorter than a supervisor's usual 30s termination grace period, so the drain
// fails and reports rather than being killed mid-flush. Kafka's own
// delivery.timeout.ms default of two minutes is far past that.
const recordDeliveryTimeout = 20 * time.Second
```

and in `NewKafkaSink`, add it to `opts` before `securityOpts`:

```go
	opts := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.AllowAutoTopicCreation(),
		kgo.RecordDeliveryTimeout(recordDeliveryTimeout),
	}
```

Add `"time"` to the imports.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test ./internal/sinks/ -run "TestSinkKafka_(FlushReturnsWithoutADeadline|HasADeliveryTimeoutByDefault)" -v -timeout 120s`

Expected: PASS. The first returns in about a second, the second in about 20.

- [ ] **Step 5: Run the Kafka unit pass**

Run: `go test -short ./internal/sinks/ -v -run TestSinkKafka`

Expected: PASS, including the existing `FlushHonoursItsContext`, which still
sees `context.DeadlineExceeded` because its 200ms deadline expires long before
the delivery timeout.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -F - <<'EOF'
fix: a Kafka flush with no deadline never returned

Flush honours its context, and no production caller gives it one that expires.
managerCtx is context.WithCancel(context.Background()) and the pipeline's drain
uses context.WithoutCancel, which strips the deadline with the cancellation.
RecordDeliveryTimeout defaults to off, so franz-go retried forever underneath.
Measured: a deadline-free flush against an unreachable broker was still blocked
after 25 seconds.

The cost was worst on the windowed path. A tumbling manager blocked inside its
first Flush drops every later tick, so an outage stopped every window
publishing with nothing logged and the pipeline looked healthy. At SIGTERM the
final poll runs on context.Background(), which can neither be cancelled nor
time out, and shutdown waited for SIGKILL.

A 20s delivery timeout bounds it: the same flush now returns in one second with
the row still buffered, so the next attempt re-sends it. Shorter than a
supervisor's usual 30s grace period, so the drain reports instead of being
killed mid-write.

If this is wrong, a slow but healthy broker fails a flush that would have
landed -- recoverable, because the row stays buffered and the pipeline retries.
EOF
```

---

## Task 10: Classify the Kafka sink's errors

`Flush` returns a bare `fmt.Errorf`, so `errs.CodeOf` reads
`system.internal.unexpected`, the process exits 1 rather than 12, and the error
metric is labelled for a bug rather than a dead broker. Task 9's timeout also
introduces `kgo.ErrRecordTimeout`, which `isUnreachable` does not recognise.

**Files:**
- Modify: `internal/sinks/kafka.go` (`Flush`, add `kafkaSinkError`)
- Test: `internal/sinks/kafka_test.go`

**Interfaces:**
- Consumes: `sinkError(err error, format string, args ...any) error` from
  `internal/sinks/classify.go`, `errs.CodeSinkUnreachable`.
- Produces: `func kafkaSinkError(err error, format string, args ...any) error`.

- [ ] **Step 1: Write the failing test**

```go
// A dead broker must exit 12, not 1.
//
// errs.CodeOf falls back to system.internal.unexpected for an uncoded error, so
// a bare fmt.Errorf tells a supervisor the pipeline hit a bug and tells the
// error metric the same. Both are wrong, and both are what an operator reads
// first.
func TestSinkKafka_FlushCodesAnUnreachableBroker(t *testing.T) {
	coverage.Covers(t, "sink.kafka")
	s := newUnreachableKafkaSink(t)

	table := newTestTable(t, []string{"nyc"}, []int64{1})
	defer table.Release()
	assert.NoError(t, s.WriteTable(context.Background(), table))

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	err := flushWithin(t, s, ctx, 10*time.Second)
	assert.Error(t, err)
	assert.Equal(t, errs.CodeSinkUnreachable, errs.CodeOf(err))
	assert.Equal(t, errs.ExitSinkUnreachable, errs.ExitCode(err))
}

// A record that expired waiting for a broker that never answered is the same
// failure as a refused connection. isUnreachable knows syscall and net errors
// and has no reason to know franz-go's sentinel, so the sink classifies it.
func TestSinkKafka_ARecordTimeoutIsUnreachable(t *testing.T) {
	coverage.Covers(t, "sink.kafka")
	err := kafkaSinkError(kgo.ErrRecordTimeout, "kafka sink: %d rows", 1)

	assert.Equal(t, errs.CodeSinkUnreachable, errs.CodeOf(err))
}
```

Add `"github.com/turbolytics/sql-flow/internal/errs"` to the test imports.

- [ ] **Step 2: Run to verify they fail**

Run: `go test ./internal/sinks/ -run "TestSinkKafka_(FlushCodesAnUnreachableBroker|ARecordTimeoutIsUnreachable)" -v`

Expected: FAIL to compile with `undefined: kafkaSinkError`. After adding the
function, `FlushCodesAnUnreachableBroker` still FAILS, reporting
`system.internal.unexpected` where `system.sink.unreachable` was wanted.

- [ ] **Step 3: Add the classifier**

In `internal/sinks/kafka.go`:

```go
// kafkaSinkError codes a flush failure, teaching sinkError the one franz-go
// error it cannot recognise.
//
// isUnreachable matches syscall and net errors. A record that expired waiting
// for a broker that never answered carries neither, and coding it as a rejected
// write would exit 1 and label the metric for a bug. It is the same partition a
// refused connection is, reported by a timer instead of the kernel.
func kafkaSinkError(err error, format string, args ...any) error {
	if errors.Is(err, kgo.ErrRecordTimeout) || errors.Is(err, kgo.ErrClientClosed) {
		return errs.Wrap(errs.CodeSinkUnreachable, err, format, args...)
	}
	return sinkError(err, format, args...)
}
```

Add `"github.com/turbolytics/sql-flow/internal/errs"` to the imports.

- [ ] **Step 4: Use it in `Flush`**

Replace the final return in `Flush`:

```go
	return fmt.Errorf("kafka sink: %d of %d rows not acknowledged, first: %w",
		len(keep), len(pending), firstErr)
```

with:

```go
	return kafkaSinkError(firstErr, "kafka sink: %d of %d rows not acknowledged",
		len(keep), len(pending))
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `go test ./internal/sinks/ -run TestSinkKafka -v -timeout 120s`

Expected: PASS. `FlushHonoursItsContext` still passes because `errs.Wrap`
preserves the chain, so `errors.Is(err, context.DeadlineExceeded)` still holds.

- [ ] **Step 6: Run the full unit pass**

Run: `go test -short ./internal/...`

Expected: PASS for every package.

- [ ] **Step 7: Commit**

```bash
git add -A
git commit -F - <<'EOF'
fix: a Kafka flush failure carried no error code

Flush returned a bare fmt.Errorf, so errs.CodeOf fell back to
system.internal.unexpected. A dead broker exited 1 instead of 12, which tells a
supervisor the pipeline hit a bug, and the error metric carried the same label
where an operator reads it first. isUnreachable already returned true for the
underlying error, so the classification was available and unused. ClickHouse
was the only sink that called sinkError.

kafkaSinkError adds the one error franz-go raises that isUnreachable cannot
recognise. A record that expired waiting for a broker that never answered is
the same partition a refused connection is, reported by a timer rather than the
kernel, and coding it as a rejected write would exit 1 again -- which is the
error the previous commit's delivery timeout makes the common one.

errs.Wrap preserves the chain, so callers testing for context.DeadlineExceeded
still see it.
EOF
```

---

## Task 11: Run the sequence against every subject and settle the matrix

Tasks 6 to 8 added four judgments that run for all five sink subjects. This task
finds out what they say. The design deliberately makes no prediction: exemptions
written before the evidence are excuses.

**Files:**
- Modify: `docs/coverage/integrations.yml` (only if a subject genuinely cannot
  exercise a claim)
- Modify: `docs/coverage/matrix.md` (regenerated)

**Interfaces:**
- Consumes: everything above.
- Produces: a green `make coverage-matrix-check`.

- [ ] **Step 1: Run every local subject**

Run: `go test -short ./internal/sinks/ -run Conformance -v 2>&1 | grep -E "^(=== RUN|--- (PASS|FAIL|SKIP))"`

Expected: console and sqlcommand each report seven invariant subtests. Record
every FAIL and SKIP with its message.

- [ ] **Step 2: Run every container-backed subject**

Run: `go test ./internal/sinks/ -run "TestIntegrationSink" -v -timeout 900s`

Expected: ClickHouse, Kafka and Iceberg subjects run. Record every FAIL and SKIP.
Docker must be up.

- [ ] **Step 3: Triage each failure**

For each failing verdict, decide between two outcomes and write the reason down
before acting:

- **A real defect in that sink.** Fix the sink. The harness message names the
  invariant and what it observed.
- **The subject cannot exercise the claim.** Add an exemption to
  `integrations.yml` with a `reason` naming the property of the integration —
  never a property of the test — and a `proven_by` naming a test that proves
  the premise.

A failure is not exempted because fixing it is inconvenient. `sink.console` and
`sink.sqlcommand` were once excused from `keeps_batch` on the grounds that
nothing crosses a network, and that hid two live batch-loss bugs.

- [ ] **Step 4: Regenerate the matrix**

Run: `make coverage-matrix`

Expected: the `sink.kafka` column shows `empty_is_noop`, `honours_context`,
`no_hollow_success` and `preserves_order` proven at integration level, alongside
the three already green.

- [ ] **Step 5: Run the coverage gate**

Run: `make coverage-matrix-check`

Expected: PASS, with no problems reported from either registry.

- [ ] **Step 6: Run the full Go gate**

Run: `make test-go`

Expected: PASS. This runs build, vet, gofmt and the whole suite including the
container-backed tests.

- [ ] **Step 7: Commit**

```bash
git add -A
git commit -F - <<'EOF'
coverage: record what the extended sequence proves for every sink

The four judgments added above run for all five subjects, not only Kafka. This
commit carries the matrix they produce and any exemption the run justified.

Every exemption names a property of the integration and a test that proves the
premise. sink.console and sink.sqlcommand were once excused from keeps_batch
because nothing crosses a network -- an argument for skipping a retry ladder,
not for skipping the invariant a ladder depends on -- and it hid two live
batch-loss bugs.
EOF
```

---

## Task 12: Give the Kafka sink a `Probe`

A pipeline whose broker list is wrong starts clean and fails at the first flush.
`sinks.New` already probes any sink implementing `Prober`, so this is the sink
half only.

**Files:**
- Modify: `internal/sinks/kafka.go`
- Test: `internal/sinks/kafka_test.go`
- Test: `internal/sinks/probe_test.go` (registry agreement)

**Interfaces:**
- Consumes: `sinks.Prober` (`Probe(ctx context.Context) error`) from
  `internal/sinks/probe.go`.
- Produces: `func (s *KafkaSink) Probe(ctx context.Context) error`.

- [ ] **Step 1: Write the failing test**

```go
// A broker list that names nothing must fail the start.
//
// Without a probe the pipeline starts, logs "consumer loop starting", and
// discovers the broker at the first flush -- which with a long flush interval
// is minutes later, and a supervisor calls it healthy for all of them.
func TestSinkKafka_ProbeFailsAgainstAnUnreachableBroker(t *testing.T) {
	coverage.Covers(t, "sink.kafka")
	s := newUnreachableKafkaSink(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	assert.Error(t, s.Probe(ctx))
}

// And it must be found through the interface, or sinks.New never calls it.
func TestSinkKafka_ImplementsProber(t *testing.T) {
	coverage.Covers(t, "sink.kafka")
	var s core.Sink = newUnreachableKafkaSink(t)

	_, ok := s.(Prober)
	assert.True(t, ok)
}
```

Add `"github.com/turbolytics/sql-flow/internal/core"` to the test imports.

- [ ] **Step 2: Run to verify they fail**

Run: `go test -short ./internal/sinks/ -run "TestSinkKafka_(ProbeFails|ImplementsProber)" -v`

Expected: FAIL to compile — `s.Probe` undefined.

- [ ] **Step 3: Implement `Probe`**

In `internal/sinks/kafka.go`:

```go
// Probe checks the broker before the first batch arrives.
//
// Without it a wrong broker list produces a pipeline that starts normally and
// fails at the first flush. With a long flush interval that is minutes later,
// and a supervisor reports the pipeline healthy for every one of them.
//
// Ping asks for metadata over the seed brokers, which is the cheapest request
// that proves one answered.
func (s *KafkaSink) Probe(ctx context.Context) error {
	return s.client.Ping(ctx)
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test -short ./internal/sinks/ -run "TestSinkKafka_(ProbeFails|ImplementsProber)" -v`

Expected: PASS.

- [ ] **Step 5: Declare it in the registry**

In `docs/coverage/integrations.yml`, change `sink.kafka`:

```yaml
  - id: sink.kafka
    kind: sink
    implements: [Sink, Prober]
    feature: sink.kafka
    exempt: []
```

- [ ] **Step 6: Run the unit pass and the gate**

Run: `go test -short ./internal/... && make coverage-check`

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add -A
git commit -F - <<'EOF'
feat: the Kafka sink probes its broker before the first batch

A pipeline whose broker list is wrong started normally, logged "consumer loop
starting", and discovered the broker at the first flush. With a long flush
interval that is minutes later, and a supervisor called the pipeline healthy
for all of them. That is the failure ClickHouse's Probe exists to prevent, and
the console and sqlcommand exemption -- reaches nothing that can be absent --
never fit a sink that crosses a network.

sinks.New already probes any sink implementing Prober, so this is the sink half
alone. Ping asks for metadata over the seed brokers, the cheapest request that
proves one answered.

If this is wrong, a pipeline fails to start against a broker that would have
come back on its own -- reported as system.sink.unreachable, which exits 12 and
tells the supervisor to retry.
EOF
```

---

## Task 13: Judge `probe.fails_start` and `close.idempotent`

**Files:**
- Modify: `internal/conformance/conformance.go`
- Modify: `docs/coverage/integrations.yml` (exemptions for sinks with no
  `Probe` or no `Close`)
- Test: `internal/conformance/conformance_test.go`

**Interfaces:**
- Consumes: the sequence from Task 8.
- Produces: nine verdicts; constants
  `probeFailsStart = "sink.probe.fails_start"` and
  `closeIdempotent = "lifecycle.close.idempotent"`. `SinkSubject` gains no
  field: both capabilities are found by type assertion.

- [ ] **Step 1: Write the failing tests**

```go
// A prober that retries turns a fast, clear start-up failure into a slow one.
// Nothing has been consumed yet, so there is nothing to lose by failing now.
type slowProbeSink struct {
	*memSink
}

func (s *slowProbeSink) Probe(ctx context.Context) error {
	time.Sleep(2 * probeTimeout)
	return errors.New("destination unreachable")
}

func TestToolingConformanceSinks_ASlowProbeIsCaught(t *testing.T) {
	coverage.Covers(t, "tooling.conformance")
	v := verdicts(t, subject(&slowProbeSink{memSink: newMemSink()}))[probeFailsStart]

	assert.True(t, strings.Contains(v.failure, "did not fail within"))
}

// A probe that succeeds against a broken destination is worse than none: it
// certifies a destination that is not there.
type blindProbeSink struct{ *memSink }

func (b *blindProbeSink) Probe(context.Context) error { return nil }

func TestToolingConformanceSinks_AProbeThatCannotFailIsCaught(t *testing.T) {
	coverage.Covers(t, "tooling.conformance")
	v := verdicts(t, subject(&blindProbeSink{memSink: newMemSink()}))[probeFailsStart]

	assert.True(t, strings.Contains(v.failure, "returned nil"))
}

// Close runs on every shutdown path, and more than one of them can reach it.
type panicOnSecondCloseSink struct {
	*memSink
	closed bool
}

func (p *panicOnSecondCloseSink) Close() error {
	if p.closed {
		return errors.New("close of closed sink")
	}
	p.closed = true
	return nil
}

func TestToolingConformanceSinks_ASinkThatCannotCloseTwiceIsCaught(t *testing.T) {
	coverage.Covers(t, "tooling.conformance")
	v := verdicts(t, subject(&panicOnSecondCloseSink{memSink: newMemSink()}))[closeIdempotent]

	assert.True(t, strings.Contains(v.failure, "second Close"))
}

// A sink implementing neither is skipped, and the registry must exempt it.
func TestToolingConformanceSinks_ASinkWithNoProbeOrCloseIsSkipped(t *testing.T) {
	coverage.Covers(t, "tooling.conformance")
	vs := verdicts(t, subject(newMemSink()))

	assert.True(t, strings.Contains(vs[probeFailsStart].skipped, "integrations.yml"))
	assert.True(t, strings.Contains(vs[closeIdempotent].skipped, "integrations.yml"))
}
```

- [ ] **Step 2: Run to verify they fail**

Run: `go test -short ./internal/conformance/ -run "ASlowProbe|AProbeThatCannotFail|ACannotCloseTwice|NoProbeOrClose" -v`

Expected: FAIL to compile — `probeFailsStart`, `closeIdempotent` and
`probeTimeout` undefined.

- [ ] **Step 3: Add the constants and the phase**

In `internal/conformance/conformance.go`, add to the constant block:

```go
	probeFailsStart = "sink.probe.fails_start"
	closeIdempotent = "lifecycle.close.idempotent"
```

and beside `flushTimeout`:

```go
// probeTimeout bounds a Probe against a broken destination. probe() dials once
// and does not retry, because the supervisor's restart is already the retry, so
// a prober that takes longer than this is laddering where it should not.
const probeTimeout = 5 * time.Second
```

Add the phase at the end of `sinkVerdicts`, after the final read-back and before
the return. The destination is still healed, so break it again:

```go
	// Phase 2. A fresh sink, because the one above holds a delivered buffer
	// and the judgments below are about a sink that has not started.
	probes := verdict{invariant: probeFailsStart}
	closes := verdict{invariant: closeIdempotent}

	fresh := s.New(t)
	s.Break(t)
	defer s.Heal(t)

	if p, ok := fresh.(prober); !ok {
		probes.skipped = s.Integration + " implements no Prober; exempt it in " +
			"integrations.yml or implement Probe"
	} else {
		probeCtx, cancelProbe := context.WithTimeout(ctx, probeTimeout)
		start := time.Now()
		err := p.Probe(probeCtx)
		cancelProbe()

		if time.Since(start) >= probeTimeout {
			probes.failure = "Probe did not fail within " + probeTimeout.String() +
				" against a broken destination; a probe dials once and does not " +
				"retry, because the supervisor's restart is the retry"
		} else if err == nil {
			probes.failure = "Probe returned nil against a broken destination; " +
				"a probe that cannot fail certifies a destination that is not there"
		}
	}

	if c, ok := fresh.(io.Closer); !ok {
		closes.skipped = s.Integration + " implements no Close; exempt it in " +
			"integrations.yml"
	} else {
		_ = c.Close()
		if err := c.Close(); err != nil {
			closes.failure = "the second Close returned " + err.Error() +
				"; more than one shutdown path reaches Close, and the second " +
				"must not fail the exit status"
		}
	}

	return []verdict{empty, buffers, keeps, depth, honours, hollow, order, probes, closes}
```

Add the local interface beside `SinkSubject`, so the harness does not import
`internal/sinks`:

```go
// prober mirrors sinks.Prober. Declared here because internal/sinks imports
// this package in its tests, so importing it back would be a cycle in the test
// binary. Go interfaces are structural, so the two match without a dependency.
type prober interface {
	Probe(ctx context.Context) error
}
```

Add `"io"` to the imports.

- [ ] **Step 4: Carry both verdicts through the other return paths**

Add `{invariant: probeFailsStart, skipped: skip}` and
`{invariant: closeIdempotent, skipped: skip}` to the `Break == nil` list and to
the early return after a failed retry. Update the `verdicts` helper count to 9.

- [ ] **Step 5: Run the tests**

Run: `go test -short ./internal/conformance/ -v`

Expected: PASS.

- [ ] **Step 6: Run every subject and record the outcome**

Run: `go test -short ./internal/sinks/ -run Conformance -v` and
`go test ./internal/sinks/ -run TestIntegrationSink -v -timeout 900s`

Expected: Kafka and ClickHouse prove `probe.fails_start`; console, sqlcommand
and noop skip it. Every skip needs its exemption in the next step.

- [ ] **Step 7: Add the exemptions the skips require**

For each integration whose verdict skipped, add to `docs/coverage/integrations.yml`.
For example, for `sink.console`:

```yaml
      - invariant: lifecycle.close.idempotent
        reason: implements no Close; it holds an io.Writer it does not own
        proven_by: TestSinkConsole_ImplementsNoCloser
```

Write the `proven_by` test where one does not exist, following
`TestSinkConsole_ImplementsNoProber` in `internal/sinks/structural_test.go`:

```go
func TestSinkConsole_ImplementsNoCloser(t *testing.T) {
	coverage.Covers(t, "sink.console")
	var s core.Sink = NewConsoleSink()

	_, ok := s.(io.Closer)
	assert.False(t, ok)
}
```

- [ ] **Step 8: Run the gate**

Run: `make test-go && make coverage-matrix-check`

Expected: PASS, with the `sink.kafka` column showing nine proven cells and
`error.classifies` the only one missing.

- [ ] **Step 9: Commit**

```bash
git add -A
git commit -F - <<'EOF'
conformance: judge probe.fails_start and close.idempotent

A fresh sink, a broken destination, and two questions the delivery sequence
cannot ask. A Probe must fail, and fail fast: probe() dials once and does not
retry because the supervisor's restart is already the retry, so a prober that
ladders only delays the report. A probe that returns nil against a destination
that is not there is worse than none, and one double proves each is caught.

Close must survive a second call, because more than one shutdown path reaches
it and the second must not change the exit status.

Both are found by type assertion, so SinkSubject gains no field. A sink
implementing neither is skipped, and integrations.yml carries the exemption --
the harness skipping and the registry excusing are two statements that have to
agree, or the cell silently reads as covered.
EOF
```

---

## Self-Review

**Spec coverage.** Every section of the design maps to a task:

| Spec section | Task |
|---|---|
| Delete `Batch()`, retire the invariant | 1, 2 |
| At-least-once in the harness | 3, 4 |
| Two subjects cannot witness order | 5 |
| Pre-warm, `empty_is_noop` | 6 |
| `honours_context` | 7 |
| `no_hollow_success`, `preserves_order`, single-partition topic | 8 |
| `RecordDeliveryTimeout` | 9 |
| `sinkError` wrap | 10 |
| "What this design does not know" — the five-subject sweep | 11 |
| Kafka `Probe` | 12 |
| Phase 2, exemptions | 13 |

Out of scope in the spec and absent here: the `Reject` seam,
`sink.error.classifies`, the type axis, source invariants, `requires` fields,
the `Batch()` diagnostic, and making the delivery timeout configurable.

**Type consistency.** `deliveredInOrder(got, want []Row) string` is defined in
Task 3 and called in Tasks 4, 6 and 8 with that signature. `indexOf(rows []Row,
want Row) int` is defined and used in Task 3 only. `kafkaSinkError(err error,
format string, args ...any) error` is defined and used in Task 10.
`recordDeliveryTimeout` is defined in Task 9 and referenced by a Task 9 test.
`probeTimeout`, `prober` and the nine invariant constants are introduced before
first use. The verdict count rises 3 → 4 → 5 → 7 → 9 across Tasks 6, 7, 8 and
13, and each of those tasks updates the `verdicts` helper.

**Known risk, flagged rather than hidden.** Task 6 changes the row ids every
later assertion depends on: the judged row becomes `id=2` and the read-back
expects `[1, 2]`, then `[1, 2, 3]` after Task 8. A task executed out of order
will fail on the id, not on the invariant. Execute 6, 7 and 8 in sequence.

---

## Execution Handoff

**Plan complete and saved to `docs/superpowers/plans/2026-09-08-kafka-sink-invariants.md`. Two execution options:**

**1. Subagent-Driven (recommended)** - I dispatch a fresh subagent per task, review between tasks, fast iteration

**2. Inline Execution** - Execute tasks in this session using executing-plans, batch execution with checkpoints

**Which approach?**
