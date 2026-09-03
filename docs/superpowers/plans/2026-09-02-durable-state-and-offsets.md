# Durable State and DuckDB-Managed Offsets Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make SQLFlow a stateful stream processor: window state survives a crash, the Kafka offsets that produced that state are committed atomically with it inside DuckDB, and both are observable at runtime.

**Architecture:** DuckDB becomes the single source of truth. A pipeline may declare `pipeline.state.path`; the engine opens DuckDB on that file instead of in memory, and keeps a `sqlflow_offsets` table there alongside the user's tables. Each batch runs with ADBC autocommit disabled: the handler's writes to state and the offset upsert land in one transaction, committed after the external sink has flushed. On startup the engine reads offsets out of DuckDB and seeks Kafka to them, so Kafka's committed offsets become advisory (useful for lag dashboards, no longer authoritative).

**Observability is part of the deliverable, not a follow-up.** State you cannot see is worse than no state: an operator has no way to tell a healthy pipeline from one that is falling behind or growing without bound. Every state feature here lands with the instrument that makes it observable — on-disk size, rows per managed table, consumer lag, and the latency of the commit this design introduces. Because DuckDB takes an exclusive file lock, a *running* pipeline must serve its own stats over HTTP; a *stopped* one is read from the file by a CLI. Both are built here.

### The Prometheus surface after this work

Exported under the meter name `sqlflow`, served on the existing `/metrics` endpoint (`--metrics prometheus`, `:8000`). The exporter, registry and mux already exist in `internal/cli/run/metrics.go`; this adds instruments to them rather than building anything new. `Int64Gauge` is already in use by `sink_flush_num_rows`, so gauges are known to export correctly through the OTel Prometheus bridge.

| Instrument | Type | Attributes | Task | Answers |
|---|---|---|---|---|
| `message_count` | counter | — | exists | Throughput |
| `error_count` | counter | `phase` | exists | Failure rate, by stage |
| `source_read_latency` | histogram | — | exists | Is the source starving the pipeline |
| `batch_processing_latency` | histogram | — | exists | Handler cost per batch |
| `sink_flush_latency` | histogram | — | exists | Sink cost per batch |
| `sink_flush_count` | counter | — | exists | Batches completed |
| `sink_flush_num_rows` | gauge | — | exists | Rows per batch |
| **`consumer_lag`** | gauge | `topic`, `partition` | 5b | Am I falling behind |
| **`state_db_size_bytes`** | gauge | — | 5c | Is state growing without bound |
| **`state_table_rows`** | gauge | `table` | 5c | Which table is growing |
| **`state_commit_latency`** | histogram | — | 4 | What the durability guarantee costs per batch |
| **`state_commit_count`** | counter | — | 4 | Commits completed, to pair with the sink count |

`state_*` instruments record nothing when the pipeline has no `state.path`, rather than reporting zero — an absent series and a genuinely empty state are different facts, and a dashboard should be able to tell them apart.

**Tech Stack:** Go 1.25, DuckDB via Arrow ADBC driver manager (`drivermgr`), franz-go for Kafka, zap, `github.com/zeebo/assert` for tests.

**Spec:** This document. The design was settled in conversation on 2026-09-02; the findings it rests on are recorded under "Established facts" below.

## Established facts

Each was verified by execution on 2026-09-02, not read from source. Do not re-litigate these; do re-verify if behaviour surprises you.

1. **DuckDB is opened in memory today.** `internal/duckdb/open.go` passes no path. All window state is process-local.
2. **A crash mid-window loses the aggregate silently.** 2,000 events were folded into an open window, the process was `kill -9`ed, and afterwards the consumer group read `committed=2000, log-end=2000, LAG=0`. A restart consumed 0 messages. The aggregate is permanently short and every signal reports healthy.
3. **DuckDB ADBC accepts a `path` option.** `drivermgr.Driver.NewDatabase({"driver": …, "entrypoint": "duckdb_adbc_init", "path": "/tmp/x.db"})` creates and uses the file. The keys `uri`, `database` and `filename` were not needed.
4. **ADBC transactions work; raw `BEGIN` does not.** `conn.SetOption(adbc.OptionKeyAutoCommit, adbc.OptionValueDisabled)` returns nil and the connection implements `Commit(ctx)`/`Rollback(ctx)`. Issuing `BEGIN TRANSACTION` afterwards fails with `cannot start a transaction within a transaction`, because disabling autocommit already opened one. **Use the ADBC methods, never SQL transaction statements.**
5. **Offsets already commit only what was processed.** As of #154 the pipeline hands the Kafka source explicit marks. This plan moves the authority for those marks into DuckDB; it does not revisit the mark-tracking logic.
6. **DuckDB holds an exclusive file lock.** While one process has the state file open, a second process cannot open it *even read-only*: `IO Error: Could not set lock on file … Conflicting lock is held`. After the writer exits, the file opens normally read-only and read-write. This is why stats are served over HTTP by the running process and read from the file only when it is stopped.
7. **Consumer lag needs no new dependency.** `kgo.FetchPartition` carries `HighWatermark` (and `LogStartOffset`) on every fetch, so lag is `HighWatermark - lastProcessedOffset`, computed in the poll loop. Do not add `kadm` for this.
8. **A second connection to the same database gets snapshot isolation.** `db.Open(ctx)` called twice on one `adbc.Database` yields two connections. With the writer inside an open transaction (autocommit disabled) and an uncommitted row pending, the reader reported the old count; after `Commit` it reported the new one. This is what lets `/stats` read without dirty reads and without taking the pipeline's lock. **Stats therefore report the last committed state — exactly what would survive a crash at that moment**, which is the number an operator wants from a durability feature.

## Global Constraints

- **No new external dependencies.** DuckDB and franz-go are already present; nothing else may be added. `go.mod` must not gain a direct requirement.
- **Backward compatible.** A config without `pipeline.state.path` behaves exactly as today: in-memory DuckDB, autocommit on, Kafka-authoritative offsets. Every existing test must pass unchanged.
- **Go 1.25**, `CGO_ENABLED=1`. `go vet ./...` and `gofmt -l internal/ cmd/` must be clean before every commit.
- **TDD.** Every task writes a failing test first and watches it fail for the expected reason. A test that passes on first run is a plan failure — fix the test, not the claim.
- **Kafka-backed tests skip when no broker is reachable**, matching `internal/kafka/source_test.go`'s `brokerOrSkip`. They must not run in CI's unit job.
- **Delivery-guarantee vocabulary is fixed.** "Exactly-once" describes *state relative to offsets* only. External sinks remain at-least-once. Never write "exactly-once" unqualified.

---

## File Structure

| File | Responsibility |
|---|---|
| `internal/duckdb/open.go` (modify) | Gains `OpenPath(ctx, path)`; existing `Open` delegates to it with an empty path |
| `internal/config/config.go` (modify) | New `State` struct on `Pipeline`; `path` field |
| `internal/core/offsets.go` (create) | `OffsetStore`: schema creation, `Load`, `Save`. Owns the `sqlflow_offsets` table and nothing else |
| `internal/core/offsets_test.go` (create) | Round-trip and schema tests against a real DuckDB |
| `internal/core/marks.go` (create) | `Marks`: per-partition positions, forward-only `Advance`, sorted `Each`. Replaces `map[string]map[int32]Mark` everywhere |
| `internal/core/turbine.go` (modify) | Transactional batch: autocommit off, sink flush, offset save, `Commit`; rollback on failure |
| `internal/core/turbine_test.go` (modify) | Fakes for the transactional path; ordering assertions |
| `internal/core/metrics.go` (modify) | Four new instruments: state size, rows per table, consumer lag, commit latency/count |
| `internal/core/stats.go` (create) | `StateStats`: the JSON-shaped snapshot both the HTTP endpoint and the CLI render. Reads a DuckDB connection, knows nothing about HTTP |
| `internal/core/stats_test.go` (create) | Snapshot tests against a real state file |
| `internal/kafka/source.go` (modify) | `SeekTo(marks)`; records `HighWatermark` per partition for lag |
| `internal/kafka/source_test.go` (modify) | Live-broker seek and high-watermark tests |
| `internal/cli/run/metrics.go` (modify) | Already owns the Prometheus exporter, registry and mux; `/stats` is registered on the same mux |
| `internal/cli/stats/root.go` (create) | `sqlflow stats -c <config>`: opens the state file read-only, renders `StateStats`, explains the lock error when the pipeline is running |
| `internal/cli/stats/root_test.go` (create) | Offline read, and the locked-file message |
| `internal/cli/run/root.go` (modify) | Wires `state.path` → `OpenPath`, builds the `OffsetStore`, seeks before consuming, starts the HTTP server |
| `tests/release/test_image.py` (modify) | Crash-recovery acceptance test, plus a `/stats` assertion through the image |
| `README.md` (modify) | Guarantee statement, `pipeline.state`, the new metrics, and `sqlflow stats` |

---

## Task 0: A `Marks` type

**Files:**
- Modify: `internal/core/turbine.go`, `internal/core/turbine_test.go`, `internal/kafka/source.go`, `internal/kafka/source_test.go`
- Test: `internal/core/marks_test.go` (create), `internal/core/marks.go` (create)

**Interfaces:**
- Consumes: `Mark`.
- Produces:

```go
type Marks struct { /* unexported nested map */ }

func NewMarks() *Marks
func (m *Marks) Advance(topic string, partition int32, mark Mark)
func (m *Marks) Get(topic string, partition int32) (Mark, bool)
func (m *Marks) Len() int
func (m *Marks) Empty() bool
func (m *Marks) Each(fn func(topic string, partition int32, mark Mark))
```

Every later task uses `*Marks` where it would otherwise say `map[string]map[int32]Mark`: `MarkCommitter.CommitMarks(*Marks)`, `Source.SeekTo(*Marks)`, `OffsetStore.Save(ctx, *Marks)`, `OffsetStore.Load(ctx) (*Marks, error)`. Do this task first — retrofitting it later means editing six files twice.

Two behaviours move into the type rather than being repeated by callers:

- **`Advance` only moves forward.** The rule currently sits inline in `turbine.mark()`; a partition never goes backwards, so a redelivery cannot rewind a committed position.
- **`Each` iterates in sorted order**, topic then partition. Map iteration is random, and unsorted output makes `/stats` JSON and `sqlflow stats` diffs unreadable between runs.

- [ ] **Step 1: Write the failing test**

```go
package core

import (
	"fmt"
	"testing"

	"github.com/zeebo/assert"
)

// Advance carries the invariant that a partition only moves forward, so a
// redelivered message cannot rewind a position that has already been
// committed.
func TestMarks_AdvanceNeverGoesBackwards(t *testing.T) {
	m := NewMarks()
	m.Advance("events", 0, Mark{Offset: 10, LeaderEpoch: 1})
	m.Advance("events", 0, Mark{Offset: 4, LeaderEpoch: 1})

	got, ok := m.Get("events", 0)
	assert.That(t, ok)
	assert.Equal(t, int64(10), got.Offset)
}

func TestMarks_AdvanceMovesForward(t *testing.T) {
	m := NewMarks()
	m.Advance("events", 0, Mark{Offset: 4, LeaderEpoch: 1})
	m.Advance("events", 0, Mark{Offset: 10, LeaderEpoch: 2})

	got, _ := m.Get("events", 0)
	assert.Equal(t, int64(10), got.Offset)
	assert.Equal(t, int32(2), got.LeaderEpoch)
}

func TestMarks_TracksPartitionsIndependently(t *testing.T) {
	m := NewMarks()
	m.Advance("events", 0, Mark{Offset: 5})
	m.Advance("events", 1, Mark{Offset: 99})
	m.Advance("other", 0, Mark{Offset: 1})

	assert.Equal(t, 3, m.Len())
	p0, _ := m.Get("events", 0)
	p1, _ := m.Get("events", 1)
	assert.Equal(t, int64(5), p0.Offset)
	assert.Equal(t, int64(99), p1.Offset)
}

func TestMarks_EmptyAndMissing(t *testing.T) {
	m := NewMarks()
	assert.That(t, m.Empty())
	assert.Equal(t, 0, m.Len())

	_, ok := m.Get("nope", 0)
	assert.That(t, !ok)

	m.Advance("events", 0, Mark{Offset: 1})
	assert.That(t, !m.Empty())
}

// Sorted iteration keeps /stats output and CLI diffs stable between runs;
// map order would shuffle them.
func TestMarks_EachIteratesInSortedOrder(t *testing.T) {
	m := NewMarks()
	m.Advance("zeta", 0, Mark{Offset: 1})
	m.Advance("alpha", 2, Mark{Offset: 2})
	m.Advance("alpha", 0, Mark{Offset: 3})

	var order []string
	m.Each(func(topic string, partition int32, mark Mark) {
		order = append(order, fmt.Sprintf("%s/%d", topic, partition))
	})
	assert.Equal(t, []string{"alpha/0", "alpha/2", "zeta/0"}, order)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/core/ -run TestMarks -v`
Expected: FAIL — `undefined: NewMarks`.

- [ ] **Step 3: Write minimal implementation**

Create `internal/core/marks.go`. `Advance` creates the inner map on first use and compares against any existing mark. `Each` collects topics, `sort.Strings` them, then collects and sorts each topic's partitions before calling `fn`. Keep the map unexported so the invariant cannot be bypassed.

Then replace the nested map at all nine sites. `Turbine.marks` becomes `*Marks`, initialised in `NewTurbine`; `turbine.mark()` becomes a call to `t.marks.Advance(...)` and loses its inline comparison; `commitSource` checks `t.marks.Empty()`. In `kafka.Source.CommitMarks`, build the `kgo` offsets inside `marks.Each(...)`.

- [ ] **Step 4: Run test to verify it passes**

Run: `SQLFLOW_DUCKDB_LIB=/opt/homebrew/lib/libduckdb.dylib go test ./internal/... && go test ./internal/kafka/ -count=1`
Expected: PASS. `TestConsumeLoop_CommitsOnlyProcessedMarks` and `TestConsumeLoop_MarksTrackEachPartition` must still pass — they are the regression gate for #154 and this refactor must not weaken them.

- [ ] **Step 5: Commit**

```bash
gofmt -w internal/ && go vet ./...
git add internal/core/marks.go internal/core/marks_test.go internal/core/turbine.go internal/core/turbine_test.go internal/kafka/source.go internal/kafka/source_test.go
git commit -m "refactor(core): introduce a Marks type for per-partition positions"
```

---

## Task 1: Open DuckDB on a path

**Files:**
- Modify: `internal/duckdb/open.go`
- Test: `internal/duckdb/open_test.go` (create)

**Interfaces:**
- Consumes: nothing.
- Produces:

```go
// DB owns the ADBC database handle so more than one connection can be opened
// against the same file. Today's Open discards that handle, which makes a
// second connection impossible -- and a second connection is what lets stats
// be read without disturbing the writer (fact 8).
type DB struct { /* adbc.Database, path */ }

func OpenPath(ctx context.Context, path string) (*DB, error)
func (d *DB) Connect(ctx context.Context) (adbc.Connection, error)
func (d *DB) Path() string
func (d *DB) Close() error
```

An empty `path` is in-memory. `func Open(ctx context.Context) (adbc.Connection, error)` is retained for callers that want one connection and no handle: it calls `OpenPath(ctx, "")` then `Connect`. Every existing caller keeps compiling.

- [ ] **Step 1: Write the failing test**

```go
package duckdb

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/zeebo/assert"
)

func exec(t *testing.T, conn interface {
	NewStatement() (interface {
		SetSqlQuery(string) error
		ExecuteUpdate(context.Context) (int64, error)
		Close() error
	}, error)
}, sql string) {
	t.Helper()
}

// A pipeline that declares a state path must get a DuckDB backed by that file,
// so window state survives the process. In-memory state is lost on a crash
// while its offsets are already committed.
func TestOpenPath_PersistsAcrossConnections(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.db")

	db, err := OpenPath(context.Background(), path)
	assert.NoError(t, err)
	conn, err := db.Connect(context.Background())
	assert.NoError(t, err)

	stmt, err := conn.NewStatement()
	assert.NoError(t, err)
	assert.NoError(t, stmt.SetSqlQuery("CREATE TABLE agg (city VARCHAR, n INTEGER); INSERT INTO agg VALUES ('NYC', 7)"))
	_, err = stmt.ExecuteUpdate(context.Background())
	assert.NoError(t, err)
	stmt.Close()
	conn.Close()

	_, statErr := os.Stat(path)
	assert.NoError(t, statErr)

	// A second process opening the same file sees the committed row.
	db2, err := OpenPath(context.Background(), path)
	assert.NoError(t, err)
	defer db2.Close()
	conn2, err := db2.Connect(context.Background())
	assert.NoError(t, err)
	defer conn2.Close()

	stmt2, err := conn2.NewStatement()
	assert.NoError(t, err)
	defer stmt2.Close()
	assert.NoError(t, stmt2.SetSqlQuery("SELECT n FROM agg WHERE city = 'NYC'"))
	reader, _, err := stmt2.ExecuteQuery(context.Background())
	assert.NoError(t, err)
	defer reader.Release()

	got := int64(-1)
	for reader.Next() {
		rec := reader.Record()
		if rec.NumRows() > 0 {
			got = rec.Column(0).(interface{ Value(int) int32 }).Value(0) != 0 &&
				true == true // replaced below
		}
	}
	_ = got
}

// An empty path keeps today's in-memory behaviour, so existing configs are
// unaffected.
func TestOpenPath_EmptyPathIsInMemory(t *testing.T) {
	db, err := OpenPath(context.Background(), "")
	assert.NoError(t, err)
	defer db.Close()
	conn, err := db.Connect(context.Background())
	assert.NoError(t, err)
	defer conn.Close()

	stmt, err := conn.NewStatement()
	assert.NoError(t, err)
	defer stmt.Close()
	assert.NoError(t, stmt.SetSqlQuery("CREATE TABLE t (i INTEGER)"))
	_, err = stmt.ExecuteUpdate(context.Background())
	assert.NoError(t, err)
}
```

Replace the awkward scan in the first test with the helper style already used in `internal/managers/tumbling_test.go:58` (`countRows`), which reads an `*array.Int64` column directly. Copy that helper into this file rather than exporting it; it is four lines and cross-package test helpers are not worth the coupling. The assertion is `assert.Equal(t, int64(7), got)`.

- [ ] **Step 2: Run test to verify it fails**

Run: `SQLFLOW_DUCKDB_LIB=/opt/homebrew/lib/libduckdb.dylib go test ./internal/duckdb/ -run TestOpenPath -v`
Expected: FAIL — `undefined: OpenPath`.

- [ ] **Step 3: Write minimal implementation**

```go
// OpenPath returns a DuckDB connection over ADBC. A non-empty path opens that
// file, so tables created on the connection outlive the process; an empty path
// is in-memory, which is the historical behaviour.
// DB owns the ADBC database handle. Holding it is what allows a second
// connection against the same file, which is how stats are read without
// disturbing the writer: connections see snapshot-isolated committed state.
type DB struct {
	db   adbc.Database
	path string
}

// OpenPath opens a DuckDB database. A non-empty path is a file, so tables
// outlive the process; an empty path is in-memory, the historical behaviour.
func OpenPath(ctx context.Context, path string) (*DB, error) {
	opts := map[string]string{
		"driver":     LibPath(),
		"entrypoint": "duckdb_adbc_init",
	}
	if path != "" {
		opts["path"] = path
	}

	var drv drivermgr.Driver
	database, err := drv.NewDatabase(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize DuckDB driver: %w", err)
	}
	return &DB{db: database, path: path}, nil
}

// Connect opens one connection. Each connection has its own transaction
// state, so a reader never sees a writer's uncommitted batch.
func (d *DB) Connect(ctx context.Context) (adbc.Connection, error) {
	conn, err := d.db.Open(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to open DuckDB connection: %w", err)
	}
	return conn, nil
}

func (d *DB) Path() string { return d.path }

func (d *DB) Close() error { return d.db.Close() }

// Open returns a single in-memory DuckDB connection, for callers that want no
// handle of their own.
func Open(ctx context.Context) (adbc.Connection, error) {
	db, err := OpenPath(ctx, "")
	if err != nil {
		return nil, err
	}
	return db.Connect(ctx)
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `SQLFLOW_DUCKDB_LIB=/opt/homebrew/lib/libduckdb.dylib go test ./internal/duckdb/ -v`
Expected: PASS, both tests.

- [ ] **Step 5: Commit**

```bash
gofmt -w internal/duckdb/ && go vet ./internal/duckdb/
git add internal/duckdb/open.go internal/duckdb/open_test.go
git commit -m "feat(duckdb): open on a file path for durable state"
```

---

## Task 2: Config gains `pipeline.state.path`

**Files:**
- Modify: `internal/config/config.go` (the `Pipeline` struct, near `FlushIntervalSeconds` at line 177)
- Test: `internal/config/load_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces: `Pipeline.State *StateConf` where `type StateConf struct { Path string \`yaml:"path"\` }`. Nil when the block is absent.

- [ ] **Step 1: Write the failing test**

```go
// A pipeline may name a file for its DuckDB state. Absent, state stays in
// memory and is lost on a crash.
func TestLoad_StatePath(t *testing.T) {
	conf := loadString(t, `
pipeline:
  batch_size: 10
  state:
    path: /var/lib/sqlflow/state.db
  source:
    type: kafka
    kafka:
      brokers: [localhost:9092]
      group_id: g
      topics: [t]
  handler:
    type: handlers.InferredMemBatch
    sql: SELECT 1
  sink:
    type: noop
`)
	assert.That(t, conf.Pipeline.State != nil)
	assert.Equal(t, "/var/lib/sqlflow/state.db", conf.Pipeline.State.Path)
}

func TestLoad_StateAbsentIsNil(t *testing.T) {
	conf := loadString(t, `
pipeline:
  batch_size: 10
  source:
    type: kafka
    kafka:
      brokers: [localhost:9092]
      group_id: g
      topics: [t]
  handler:
    type: handlers.InferredMemBatch
    sql: SELECT 1
  sink:
    type: noop
`)
	assert.That(t, conf.Pipeline.State == nil)
}
```

`loadString` may not exist. If not, write it beside these tests: it writes the YAML to `t.TempDir()` and calls `Load(path, nil)`, mirroring `renderString` at `internal/config/load_test.go:12`. Parsing is strict, so an unknown key is an error — that is what makes the second test meaningful.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/config/ -run TestLoad_State -v`
Expected: FAIL — strict parsing rejects the unknown `state` key.

- [ ] **Step 3: Write minimal implementation**

```go
// StateConf points the pipeline's DuckDB at a file, so tables the handler
// writes -- window state above all -- survive a restart. Offsets are stored in
// the same database, which is what makes state and offsets recoverable
// together.
type StateConf struct {
	Path string `yaml:"path"`
}
```

and on `Pipeline`, beside `FlushIntervalSeconds`:

```go
	State *StateConf `yaml:"state,omitempty"`
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/config/ -v`
Expected: PASS. `TestLoad_AllExampleConfigs` must still pass — it walks every shipped config.

- [ ] **Step 5: Commit**

```bash
gofmt -w internal/config/ && go vet ./internal/config/
git add internal/config/config.go internal/config/load_test.go
git commit -m "feat(config): add pipeline.state.path"
```

---

## Task 2b: Teach the JSON Schema about `pipeline.state`

**Files:**
- Modify: `internal/cli/schemas/config.json`
- Test: `internal/cli/config_test.go`

**Interfaces:** none; this is the validation surface for Task 2's config field.

Found by Task 2's review, not by the original plan. `sqlflow config validate`
renders the config and checks it against this schema, and the `pipeline`
object sets `additionalProperties: false` while listing only `batch_size`,
`description`, `flush_interval_seconds`, `handler`, `name`, `on_error`,
`sink`, `source`. A config using `pipeline.state.path` therefore **runs but
fails validation** -- the same trap the README documents for webhook sources,
where the schema rejects what the engine accepts.

- [ ] **Step 1: Write the failing test**

Add to `internal/cli/config_test.go`, following the existing validate tests
in that file:

```go
// A config that `run` accepts must also pass `config validate`. The schema
// sets additionalProperties: false on pipeline, so a new field is invisible
// to validation until it is declared here too.
func TestConfigValidate_AcceptsStatePath(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.yml")
	body := `
pipeline:
  batch_size: 10
  state:
    path: /var/lib/sqlflow/state.db
  source:
    type: kafka
    kafka:
      brokers: [localhost:9092]
      group_id: g
      topics: [t]
  handler:
    type: handlers.InferredMemBatch
    sql: SELECT 1
  sink:
    type: noop
`
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}

	var out bytes.Buffer
	assert.NoError(t, validateConfig(path, &out))
}
```

Match the helper name and signature the neighbouring validate tests already
use; read them first rather than assuming `validateConfig`.

- [ ] **Step 2: Run test to verify it fails**

Run: `SQLFLOW_DUCKDB_LIB=/opt/homebrew/lib/libduckdb.dylib go test ./internal/cli/ -run TestConfigValidate_AcceptsStatePath -v`
Expected: FAIL, with a schema error naming `state` as an unexpected property.

- [ ] **Step 3: Write minimal implementation**

Add to the `pipeline` object's `properties` in `internal/cli/schemas/config.json`:

```json
"state": {
  "type": "object",
  "description": "Where the pipeline keeps its DuckDB state. Absent means in-memory, and state is lost on a crash.",
  "properties": {
    "path": {
      "type": "string",
      "description": "File backing the pipeline's DuckDB database. Window state and Kafka offsets are stored here and committed together."
    }
  },
  "required": ["path"],
  "additionalProperties": false
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `SQLFLOW_DUCKDB_LIB=/opt/homebrew/lib/libduckdb.dylib go test ./internal/cli/ -v`
Expected: PASS, and every pre-existing validate test still passes.

- [ ] **Step 5: Commit**

```bash
gofmt -w internal/ && go vet ./...
git add internal/cli/schemas/config.json internal/cli/config_test.go
git commit -m "fix(schema): accept pipeline.state so validate matches run"
```

---

## Task 3: The offset store

**Files:**
- Create: `internal/core/offsets.go`
- Test: `internal/core/offsets_test.go`

**Interfaces:**
- Consumes: `core.Mark` (`internal/core/turbine.go`), an `adbc.Connection`.
- Produces:

```go
type OffsetStore struct { /* conn */ }
func NewOffsetStore(conn adbc.Connection) *OffsetStore
func (s *OffsetStore) Init(ctx context.Context) error
func (s *OffsetStore) Save(ctx context.Context, marks *Marks) error
func (s *OffsetStore) Load(ctx context.Context) (*Marks, error)
```

`Init` is idempotent (`CREATE TABLE IF NOT EXISTS`). `Save` upserts one row per topic/partition. `Save` must **not** commit — the caller owns the transaction. Table:

```sql
CREATE TABLE IF NOT EXISTS sqlflow_offsets (
    topic        VARCHAR NOT NULL,
    partition    INTEGER NOT NULL,
    offset       BIGINT  NOT NULL,
    leader_epoch INTEGER NOT NULL,
    PRIMARY KEY (topic, partition)
)
```

- [ ] **Step 1: Write the failing test**

```go
package core

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/turbolytics/sql-flow/internal/duckdb"
	"github.com/zeebo/assert"
)

func newStateConn(t *testing.T, path string) adbc.Connection {
	t.Helper()
	if os.Getenv("SQLFLOW_DUCKDB_LIB") == "" {
		os.Setenv("SQLFLOW_DUCKDB_LIB", "/opt/homebrew/lib/libduckdb.dylib")
	}
	db, err := duckdb.OpenPath(context.Background(), path)
	assert.NoError(t, err)
	conn, err := db.Connect(context.Background())
	assert.NoError(t, err)
	t.Cleanup(func() { conn.Close(); db.Close() })
	return conn
}

// Offsets round-trip through DuckDB, which is what lets a restart resume from
// the position that produced the state currently in the database.
func TestOffsetStore_RoundTrip(t *testing.T) {
	conn := newStateConn(t, filepath.Join(t.TempDir(), "state.db"))
	s := NewOffsetStore(conn)
	assert.NoError(t, s.Init(context.Background()))

	in := NewMarks()
	in.Advance("events", 0, Mark{Offset: 41, LeaderEpoch: 7})
	in.Advance("events", 1, Mark{Offset: 8, LeaderEpoch: 7})
	assert.NoError(t, s.Save(context.Background(), in))

	out, err := s.Load(context.Background())
	assert.NoError(t, err)
	p0, ok := out.Get("events", 0)
	assert.That(t, ok)
	assert.Equal(t, int64(41), p0.Offset)
	assert.Equal(t, int32(7), p0.LeaderEpoch)
	p1, _ := out.Get("events", 1)
	assert.Equal(t, int64(8), p1.Offset)
}

// Saving the same partition again advances it rather than duplicating it.
func TestOffsetStore_SaveIsAnUpsert(t *testing.T) {
	conn := newStateConn(t, filepath.Join(t.TempDir(), "state.db"))
	s := NewOffsetStore(conn)
	assert.NoError(t, s.Init(context.Background()))

	first := NewMarks(); first.Advance("events", 0, Mark{Offset: 10, LeaderEpoch: 1})
	assert.NoError(t, s.Save(context.Background(), first))
	second := NewMarks(); second.Advance("events", 0, Mark{Offset: 99, LeaderEpoch: 2})
	assert.NoError(t, s.Save(context.Background(), second))

	out, err := s.Load(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 1, out.Len())
	got, _ := out.Get("events", 0)
	assert.Equal(t, int64(99), got.Offset)
	assert.Equal(t, int32(2), got.LeaderEpoch)
}

// A fresh state file has no offsets; the caller must treat that as "start
// where auto_offset_reset says", not as offset zero.
func TestOffsetStore_LoadEmptyIsEmptyNotZero(t *testing.T) {
	conn := newStateConn(t, filepath.Join(t.TempDir(), "state.db"))
	s := NewOffsetStore(conn)
	assert.NoError(t, s.Init(context.Background()))

	out, err := s.Load(context.Background())
	assert.NoError(t, err)
	assert.That(t, out.Empty())
}

// Init runs on every start, including against an existing state file.
func TestOffsetStore_InitIsIdempotent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	conn := newStateConn(t, path)
	s := NewOffsetStore(conn)
	assert.NoError(t, s.Init(context.Background()))
	assert.NoError(t, s.Init(context.Background()))
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `SQLFLOW_DUCKDB_LIB=/opt/homebrew/lib/libduckdb.dylib go test ./internal/core/ -run TestOffsetStore -v`
Expected: FAIL — `undefined: NewOffsetStore`.

- [ ] **Step 3: Write minimal implementation**

Create `internal/core/offsets.go`. Notes that will otherwise cost time:

- `offset` and `partition` are not reserved in DuckDB, but quote the column names anyway (`"offset"`) — it reads as a keyword to anyone scanning the SQL.
- Execute writes with `stmt.ExecuteUpdate`; read with `stmt.ExecuteQuery` and iterate the `array.RecordReader` exactly as `internal/managers/tumbling.go:132` does.
- Columns come back as `*array.String`, `*array.Int32`, `*array.Int64`, `*array.Int32`. Assert those types; a wrong cast panics at runtime rather than failing to compile.
- Use `INSERT INTO … ON CONFLICT (topic, partition) DO UPDATE SET "offset" = EXCLUDED."offset", leader_epoch = EXCLUDED.leader_epoch`, which the tumbling window example already relies on.

- [ ] **Step 4: Run test to verify it passes**

Run: `SQLFLOW_DUCKDB_LIB=/opt/homebrew/lib/libduckdb.dylib go test ./internal/core/ -run TestOffsetStore -v`
Expected: PASS, all four.

- [ ] **Step 5: Commit**

```bash
gofmt -w internal/core/ && go vet ./internal/core/
git add internal/core/offsets.go internal/core/offsets_test.go
git commit -m "feat(core): store Kafka offsets in DuckDB"
```

---

## Task 4: Transactional batch

**Files:**
- Modify: `internal/core/turbine.go` (`processBatch`, and the `Turbine` struct)
- Test: `internal/core/turbine_test.go`

**Interfaces:**
- Consumes: `OffsetStore` from Task 3.
- Produces: `NewTurbine` gains a `WithStateStore(store *OffsetStore, conn adbc.Connection)` option. When set, `processBatch` runs transactionally.

**Ordering — get this right, it is the whole point.** Within one batch:

1. `handler.Invoke` — user SQL runs, mutating state inside the open transaction.
2. `sink.WriteTable` + `sink.Flush` — the external sink is flushed **before** the transaction commits, so a crash here replays the batch. External sinks stay at-least-once and may duplicate.
3. `offsets.Save` — the marks go into `sqlflow_offsets`, still inside the transaction.
4. `conn.Commit(ctx)` — state and offsets become durable together.
5. `source.CommitMarks` — Kafka commit, now advisory, for lag dashboards.

Any failure before step 4 calls `conn.Rollback(ctx)` and returns the error; the batch is replayed on restart. A failure at step 5 is logged, not fatal: the authoritative offsets are already durable.

- [ ] **Step 1: Write the failing test**

```go
// txConn records the transaction calls the pipeline makes, so the ordering
// that makes state and offsets atomic can be asserted.
type txConn struct {
	adbc.Connection
	events *[]string
}

func (c *txConn) Commit(ctx context.Context) error {
	*c.events = append(*c.events, "commit")
	return nil
}

func (c *txConn) Rollback(ctx context.Context) error {
	*c.events = append(*c.events, "rollback")
	return nil
}

// The external sink must flush before the transaction commits: a crash
// between them replays the batch (a duplicate, which is at-least-once) rather
// than committing offsets for rows the sink never received.
func TestProcessBatch_FlushesSinkBeforeCommittingState(t *testing.T) {
	var events []string
	sink := &orderingSink{events: &events}
	store := &fakeOffsetStore{events: &events}
	conn := &txConn{events: &events}

	src := &markingSource{fakeSource: fakeSource{
		batches: [][]Message{kafkaMessages("events", 0, 0, 4)},
	}}
	tb := NewTurbine(src, &fakeHandler{}, sink, 4, time.Second, &sync.Mutex{},
		PipelineErrorPolicies{}, WithStateStore(store, conn))

	_, err := tb.ConsumeLoop(context.Background(), 0)
	assert.NoError(t, err)

	assert.Equal(t, []string{"flush", "save-offsets", "commit"}, events)
}

// A sink failure must roll the transaction back, leaving the offsets on disk
// where they were so the batch is replayed.
func TestProcessBatch_RollsBackWhenSinkFails(t *testing.T) {
	var events []string
	sink := &failingSink{events: &events}
	store := &fakeOffsetStore{events: &events}
	conn := &txConn{events: &events}

	src := &markingSource{fakeSource: fakeSource{
		batches: [][]Message{kafkaMessages("events", 0, 0, 4)},
	}}
	tb := NewTurbine(src, &fakeHandler{}, sink, 4, time.Second, &sync.Mutex{},
		PipelineErrorPolicies{}, WithStateStore(store, conn))

	_, err := tb.ConsumeLoop(context.Background(), 0)
	assert.Error(t, err)
	assert.Equal(t, []string{"flush-failed", "rollback"}, events)
}

// Without a state store the pipeline behaves exactly as before: no
// transaction calls at all.
func TestProcessBatch_NoStateStoreIsUnchanged(t *testing.T) {
	var events []string
	sink := &orderingSink{events: &events}
	src := &markingSource{fakeSource: fakeSource{
		batches: [][]Message{kafkaMessages("events", 0, 0, 4)},
	}}
	tb := newTestTurbine(src, &fakeHandler{}, sink, 4)

	_, err := tb.ConsumeLoop(context.Background(), 0)
	assert.NoError(t, err)
	assert.Equal(t, []string{"flush"}, events)
	assert.Equal(t, 1, len(src.marks))
}
```

Write `orderingSink`, `failingSink` and `fakeOffsetStore` beside these — each appends its label to the shared slice. `fakeOffsetStore` needs the same method set the real store exposes to the pipeline; extract a small `offsetSaver` interface (`Save(ctx, marks) error`) in `turbine.go` so the fake does not need a real DuckDB connection.

- [ ] **Step 2: Run test to verify it fails**

Run: `SQLFLOW_DUCKDB_LIB=/opt/homebrew/lib/libduckdb.dylib go test ./internal/core/ -run TestProcessBatch -v`
Expected: FAIL — `undefined: WithStateStore`.

- [ ] **Step 3: Write minimal implementation**

Add to `Turbine`: `offsets offsetSaver`, `stateConn interface{ Commit(context.Context) error; Rollback(context.Context) error }`. Add the option. In `processBatch`, wrap the existing body: after the flush succeeds, call `t.offsets.Save(ctx, t.marks)`, then `t.stateConn.Commit(ctx)`; on any error before that, `t.stateConn.Rollback(ctx)`. Leave every path unchanged when `t.offsets == nil`.

Autocommit is disabled once, at startup, in Task 6 — not per batch. `Commit(ctx)` on an ADBC connection with autocommit disabled begins the next transaction implicitly (fact 4).

- [ ] **Step 4: Run test to verify it passes**

Run: `SQLFLOW_DUCKDB_LIB=/opt/homebrew/lib/libduckdb.dylib go test ./internal/core/ -v`
Expected: PASS, including every pre-existing test in the package.

- [ ] **Step 5: Commit**

```bash
gofmt -w internal/core/ && go vet ./internal/core/
git add internal/core/turbine.go internal/core/turbine_test.go
git commit -m "feat(core): commit state and offsets in one transaction"
```

---

## Task 5: Seek Kafka to stored offsets

**Files:**
- Modify: `internal/kafka/source.go`
- Test: `internal/kafka/source_test.go`

**Interfaces:**
- Consumes: `core.Mark`.
- Produces: `func (k *Source) SeekTo(marks *core.Marks) error`. Called before `Start`. Empty marks is a no-op, leaving `auto_offset_reset` to decide.

Use `kgo.Client.SetOffsets(map[string]map[int32]kgo.EpochOffset)` with `Offset: mark.Offset + 1` and `Epoch: mark.LeaderEpoch` — the same `+1` convention as `CommitMarks`, since a mark names the last *processed* offset.

- [ ] **Step 1: Write the failing test**

```go
// A restart must resume from the offsets in DuckDB, not from wherever the
// consumer group happens to be, because the state file is the source of truth.
func TestSource_SeekToResumesFromStoredOffsets(t *testing.T) {
	broker := brokerOrSkip(t)
	topic := fmt.Sprintf("turbine-seek-%d", time.Now().UnixNano())
	client := newTestClient(t, broker, topic, topic)
	defer client.Close()

	produce(t, client, topic, 100)

	src, err := NewSource(client)
	assert.NoError(t, err)
	defer src.Close()

	// Resume as though offset 49 had been processed: the next record read
	// must be 50.
	resume := core.NewMarks()
	resume.Advance(topic, 0, core.Mark{Offset: 49, LeaderEpoch: 0})
	assert.NoError(t, src.SeekTo(resume))

	select {
	case batch := <-src.Stream():
		assert.That(t, len(batch) >= 1)
		assert.Equal(t, int64(50), batch[0].Offset)
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for a message")
	}
}

// No stored offsets means no seek, so auto_offset_reset still governs a first
// run against a fresh state file.
func TestSource_SeekToEmptyIsANoop(t *testing.T) {
	broker := brokerOrSkip(t)
	topic := fmt.Sprintf("turbine-seek-empty-%d", time.Now().UnixNano())
	client := newTestClient(t, broker, topic, topic)
	defer client.Close()

	produce(t, client, topic, 3)

	src, err := NewSource(client)
	assert.NoError(t, err)
	defer src.Close()

	assert.NoError(t, src.SeekTo(nil))

	select {
	case batch := <-src.Stream():
		assert.Equal(t, int64(0), batch[0].Offset)
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for a message")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/kafka/ -run TestSource_SeekTo -v`
Expected: FAIL — `undefined: SeekTo`. If no broker is reachable the test skips; start one with `make start-backing-services` before claiming a result.

- [ ] **Step 3: Write minimal implementation**

```go
// SeekTo resumes consumption from positions recorded in the state database.
// The stored mark is the last offset processed, so consumption resumes at the
// next one, exactly as CommitMarks commits it.
func (k *Source) SeekTo(marks *core.Marks) error {
	if marks == nil || marks.Empty() {
		return nil
	}
	offsets := make(map[string]map[int32]kgo.EpochOffset, marks.Len())
	marks.Each(func(topic string, partition int32, m core.Mark) {
		if offsets[topic] == nil {
			offsets[topic] = map[int32]kgo.EpochOffset{}
		}
		offsets[topic][partition] = kgo.EpochOffset{Epoch: m.LeaderEpoch, Offset: m.Offset + 1}
	})
	k.client.SetOffsets(offsets)
	return nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/kafka/ -count=1 -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
gofmt -w internal/kafka/ && go vet ./internal/kafka/
git add internal/kafka/source.go internal/kafka/source_test.go
git commit -m "feat(kafka): seek to offsets stored in DuckDB"
```

---

## Task 5b: Consumer lag

**Files:**
- Modify: `internal/kafka/source.go` (the `EachRecord` loop, ~line 130), `internal/core/metrics.go`, `internal/core/turbine.go`
- Test: `internal/kafka/source_test.go`

**Interfaces:**
- Consumes: nothing new.
- Produces: `core.Message` gains `HighWatermark int64`. `Metrics` gains `ConsumerLag metric.Int64Gauge`, recorded per topic/partition with those attributes.

Lag is `HighWatermark - Offset` for the last record the pipeline processed. Fact 7: the watermark is on the fetch, so this costs nothing. Record it in `mark()`, where the last processed offset is already known.

- [ ] **Step 1: Write the failing test**

```go
// Lag is only meaningful against the broker's high watermark, which arrives
// on the fetch itself. Without it, an operator cannot tell a healthy pipeline
// from one falling behind.
func TestSource_MessagesCarryHighWatermark(t *testing.T) {
	broker := brokerOrSkip(t)
	topic := fmt.Sprintf("turbine-hwm-%d", time.Now().UnixNano())
	client := newTestClient(t, broker, topic, topic)
	defer client.Close()

	produce(t, client, topic, 10)

	src, err := NewSource(client)
	assert.NoError(t, err)
	defer src.Close()

	select {
	case batch := <-src.Stream():
		assert.That(t, len(batch) >= 1)
		// Ten records were produced, so the high watermark is 10 and the
		// first record sits 9 behind it.
		assert.Equal(t, int64(10), batch[0].HighWatermark)
		assert.Equal(t, int64(0), batch[0].Offset)
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for a message")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/kafka/ -run TestSource_MessagesCarryHighWatermark -v`
Expected: FAIL — `batch[0].HighWatermark undefined`.

- [ ] **Step 3: Write minimal implementation**

In `core.Message` add `HighWatermark int64` with a comment that it is the partition's high watermark at fetch time, zero for sources without one. In the source, the record loop is inside `fetches.EachRecord`, which does not expose the partition — switch to `fetches.EachPartition(func(p kgo.FetchTopicPartition) { ... })` so `p.HighWatermark` is in scope, and append records from `p.Records`. Keep the resulting `[]core.Message` ordering identical.

In `metrics.go`, add:

```go
	m.ConsumerLag, err = meter.Int64Gauge(
		"consumer_lag",
		metric.WithDescription("Messages between the last one processed and the partition's high watermark"),
		metric.WithUnit("messages"),
	)
	if err != nil {
		return nil, fmt.Errorf("consumer_lag: %w", err)
	}
```

In `turbine.mark()`, after recording the mark:

```go
	if m.HighWatermark > 0 {
		t.metrics.ConsumerLag.Record(context.Background(), m.HighWatermark-m.Offset-1,
			metric.WithAttributes(
				attribute.String("topic", m.Topic),
				attribute.Int("partition", int(m.Partition)),
			))
	}
```

`-1` because a mark names the last *processed* offset: having processed offset 9 with a watermark of 10 is lag zero.

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/kafka/ -count=1 -v && SQLFLOW_DUCKDB_LIB=/opt/homebrew/lib/libduckdb.dylib go test ./internal/core/ -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
gofmt -w internal/ && go vet ./...
git add internal/kafka/source.go internal/kafka/source_test.go internal/core/metrics.go internal/core/turbine.go
git commit -m "feat(metrics): report consumer lag from the fetch high watermark"
```

---

## Task 5c: State stats — the snapshot both consumers render

**Files:**
- Create: `internal/core/stats.go`, `internal/core/stats_test.go`
- Modify: `internal/core/metrics.go`

**Interfaces:**
- Consumes: `OffsetStore`, an `adbc.Connection`.
- Produces:

```go
type TableStat struct {
	Name string `json:"name"`
	Rows int64  `json:"rows"`
}

type OffsetStat struct {
	Topic       string `json:"topic"`
	Partition   int32  `json:"partition"`
	Offset      int64  `json:"offset"`
	LeaderEpoch int32  `json:"leader_epoch"`
}

type StateStats struct {
	Path      string       `json:"path"`
	SizeBytes int64        `json:"size_bytes"`
	Tables    []TableStat  `json:"tables"`
	Offsets   []OffsetStat `json:"offsets"`
}

func CollectStateStats(ctx context.Context, conn adbc.Connection, path string) (*StateStats, error)
```

`Tables` excludes `sqlflow_offsets` and the transient `batch` table — those are engine bookkeeping, not user state. Row counts come from `duckdb_tables()`; sizes from `os.Stat(path)`. `Metrics` gains `StateSizeBytes` and `StateTableRows` gauges recorded from the same struct, so the Prometheus numbers and the JSON can never disagree.

**The connection passed here is a dedicated reader**, obtained from `DB.Connect` and never the one the pipeline writes on. Per fact 8 it sees only committed state, so `count(*)` here cannot read a half-written batch and cannot block the writer. Do not take the pipeline's DuckDB mutex in this function; it is the wrong lock and holding it would let a scraper stall throughput.

- [ ] **Step 1: Write the failing test**

```go
// The stats snapshot is what an operator reads to answer "is my state growing
// without bound, and how far behind am I?" It must report the user's tables
// and the stored offsets, and must not leak engine bookkeeping tables.
func TestCollectStateStats(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.db")
	conn := newStateConn(t, path)

	s := NewOffsetStore(conn)
	assert.NoError(t, s.Init(context.Background()))
	saved := NewMarks()
	saved.Advance("events", 0, Mark{Offset: 41, LeaderEpoch: 7})
	assert.NoError(t, s.Save(context.Background(), saved))

	exec(t, conn, "CREATE TABLE agg (city VARCHAR, n INTEGER)")
	exec(t, conn, "INSERT INTO agg VALUES ('NYC', 1), ('SF', 2), ('LA', 3)")

	stats, err := CollectStateStats(context.Background(), conn, path)
	assert.NoError(t, err)

	assert.Equal(t, path, stats.Path)
	assert.That(t, stats.SizeBytes > 0)

	assert.Equal(t, 1, len(stats.Tables))
	assert.Equal(t, "agg", stats.Tables[0].Name)
	assert.Equal(t, int64(3), stats.Tables[0].Rows)

	assert.Equal(t, 1, len(stats.Offsets))
	assert.Equal(t, "events", stats.Offsets[0].Topic)
	assert.Equal(t, int64(41), stats.Offsets[0].Offset)
}

// Stats read a dedicated connection, so an in-flight batch is invisible until
// it commits. Without this the endpoint would report rows that a rollback
// then erased, and a durability feature would be reporting numbers that never
// survived anything.
func TestCollectStateStats_DoesNotSeeUncommittedWrites(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	db, err := duckdb.OpenPath(context.Background(), path)
	assert.NoError(t, err)
	defer db.Close()

	writer, err := db.Connect(context.Background())
	assert.NoError(t, err)
	defer writer.Close()
	reader, err := db.Connect(context.Background())
	assert.NoError(t, err)
	defer reader.Close()

	assert.NoError(t, NewOffsetStore(writer).Init(context.Background()))
	exec(t, writer, "CREATE TABLE agg (city VARCHAR)")
	exec(t, writer, "INSERT INTO agg VALUES ('NYC')")

	// The writer goes transactional and adds a row it has not committed.
	po, ok := writer.(adbc.PostInitOptions)
	assert.That(t, ok)
	assert.NoError(t, po.SetOption(adbc.OptionKeyAutoCommit, adbc.OptionValueDisabled))
	exec(t, writer, "INSERT INTO agg VALUES ('SF')")

	stats, err := CollectStateStats(context.Background(), reader, path)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), stats.Tables[0].Rows)

	// After the commit the reader sees it.
	assert.NoError(t, writer.(interface {
		Commit(context.Context) error
	}).Commit(context.Background()))

	stats, err = CollectStateStats(context.Background(), reader, path)
	assert.NoError(t, err)
	assert.Equal(t, int64(2), stats.Tables[0].Rows)
}

// An empty state file reports zero tables rather than failing, so `stats` is
// useful on a pipeline that has not processed anything yet.
func TestCollectStateStats_EmptyState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	conn := newStateConn(t, path)
	assert.NoError(t, NewOffsetStore(conn).Init(context.Background()))

	stats, err := CollectStateStats(context.Background(), conn, path)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(stats.Tables))
	assert.Equal(t, 0, len(stats.Offsets))
}
```

`exec` is the helper at `internal/managers/tumbling_test.go:41`; copy it into `internal/core` rather than exporting it.

- [ ] **Step 2: Run test to verify it fails**

Run: `SQLFLOW_DUCKDB_LIB=/opt/homebrew/lib/libduckdb.dylib go test ./internal/core/ -run TestCollectStateStats -v`
Expected: FAIL — `undefined: CollectStateStats`.

- [ ] **Step 3: Write minimal implementation**

List tables with `SELECT table_name FROM duckdb_tables() WHERE schema_name = 'main' ORDER BY table_name`, skipping `sqlflow_offsets` and `batch`. Count each with `SELECT count(*) FROM "<name>"` — quote the identifier, a user table may be named anything. Read offsets through `OffsetStore.Load` rather than a second query, so there is one definition of that schema.

Sort `Offsets` by topic then partition; map iteration order is random and unsorted JSON makes diffs unreadable.

- [ ] **Step 4: Run test to verify it passes**

Run: `SQLFLOW_DUCKDB_LIB=/opt/homebrew/lib/libduckdb.dylib go test ./internal/core/ -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
gofmt -w internal/core/ && go vet ./internal/core/
git add internal/core/stats.go internal/core/stats_test.go internal/core/metrics.go
git commit -m "feat(core): collect state stats for metrics and the stats endpoint"
```

---

## Task 5d: Serve `/stats`, and record state gauges

**Files:**
- Modify: `internal/cli/run/metrics.go` (it already builds the OTel Prometheus exporter, owns the registry and the mux, and registers `/metrics` at line 35 — `/stats` joins it there), `internal/core/turbine.go` (`StatusLoop`)

**Interfaces:**
- Consumes: `CollectStateStats`.
- Produces: the existing metrics-server constructor gains a `stats func() (*core.StateStats, error)` parameter and registers `/stats` on the same mux. Do **not** create a second server or a second port.

`/stats` returns the `StateStats` JSON plus the counters the pipeline already tracks:

```json
{
  "pipeline": "nyc-taxi-clickhouse",
  "messages_consumed": 1000,
  "num_errors": 0,
  "state": {"path": "/state/state.db", "size_bytes": 274432,
            "tables": [{"name": "agg", "rows": 24}],
            "offsets": [{"topic": "events", "partition": 0, "offset": 999, "leader_epoch": 7}]}
}
```

A pipeline with no `state.path` returns `"state": null` rather than erroring — the endpoint is still useful for counters.

`StatusLoop` already ticks every five seconds (`turbine.go:215`). Reuse it: collect stats on that tick and record `StateSizeBytes` and `StateTableRows`. Do not add a second ticker.

**Concurrency.** Collection runs on the dedicated reader connection from Task 5c, so it needs neither the pipeline's mutex nor any coordination with the writer. It does need its own mutex: `StatusLoop` and any number of concurrent `/stats` requests share that one reader connection, and a single ADBC connection is not safe for concurrent use. Guard the reader, not the pipeline.

- [ ] **Step 1: Write the failing test**

```go
package run

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/turbolytics/sql-flow/internal/core"
	"github.com/zeebo/assert"
)

// An operator cannot read the state file while the pipeline holds it -- DuckDB
// takes an exclusive lock -- so the running process has to serve its own
// stats.
func TestServeHTTP_StatsEndpoint(t *testing.T) {
	want := &core.StateStats{
		Path: "/state/state.db", SizeBytes: 4096,
		Tables:  []core.TableStat{{Name: "agg", Rows: 24}},
		Offsets: []core.OffsetStat{{Topic: "events", Partition: 0, Offset: 999, LeaderEpoch: 7}},
	}
	srv := serveHTTP("127.0.0.1:0", nil, func() (*core.StateStats, error) { return want, nil })
	defer srv.Close()

	rec := httptest.NewRecorder()
	srv.Handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/stats", nil))
	assert.Equal(t, http.StatusOK, rec.Code)

	var got map[string]any
	assert.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
	state := got["state"].(map[string]any)
	assert.Equal(t, "/state/state.db", state["path"])
	assert.Equal(t, float64(24), state["tables"].([]any)[0].(map[string]any)["rows"])
}

// A pipeline without a state path still answers, with a null state block.
func TestServeHTTP_StatsWithoutState(t *testing.T) {
	srv := serveHTTP("127.0.0.1:0", nil, func() (*core.StateStats, error) { return nil, nil })
	defer srv.Close()

	rec := httptest.NewRecorder()
	srv.Handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/stats", nil))
	assert.Equal(t, http.StatusOK, rec.Code)

	var got map[string]any
	assert.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
	assert.That(t, got["state"] == nil)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/cli/run/ -run TestServeHTTP -v`
Expected: FAIL — `undefined: serveHTTP`.

- [ ] **Step 3: Write minimal implementation**

Build an `http.ServeMux`, register the Prometheus handler at `/metrics` when `reg != nil`, and `/stats` always. Return the `*http.Server` without calling `ListenAndServe` — the test drives `Handler` directly and the caller starts it in a goroutine.

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/cli/run/ -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
gofmt -w internal/cli/ && go vet ./...
git add internal/cli/run/metrics.go internal/cli/run/root.go internal/core/turbine.go
git commit -m "feat(run): serve /stats and record state gauges"
```

---

## Task 5e: `sqlflow stats` for a stopped pipeline

> **Scope note.** This is the weakest task in the plan: it only works when the
> pipeline is *not* running, which is when you are least likely to be
> debugging. `/metrics` and `/stats` cover the live case, and a stopped
> pipeline's state file can be read with the `duckdb` CLI directly. Cut this
> first if scope needs cutting; the README line pointing at `duckdb` costs
> nothing.

**Files:**
- Create: `internal/cli/stats/root.go`, `internal/cli/stats/root_test.go`
- Modify: `internal/cli/root.go` (register the command)

**Interfaces:**
- Consumes: `config.Load`, `duckdb.OpenPath`, `CollectStateStats`.
- Produces: `sqlflow stats -c <config> [--json]`.

Reads `pipeline.state.path` from the config and opens that file. **The lock is the interesting case:** if the pipeline is running, the open fails with `Could not set lock on file`. Detect that substring and replace it with something actionable rather than passing DuckDB's error through.

- [ ] **Step 1: Write the failing test**

```go
// Reading a stopped pipeline's state is the offline half of observability.
func TestStats_ReadsAStoppedPipelinesState(t *testing.T) {
	dir := t.TempDir()
	statePath := filepath.Join(dir, "state.db")

	db, err := duckdb.OpenPath(context.Background(), statePath)
	assert.NoError(t, err)
	conn, err := db.Connect(context.Background())
	assert.NoError(t, err)
	store := core.NewOffsetStore(conn)
	assert.NoError(t, store.Init(context.Background()))
	saved := core.NewMarks()
	saved.Advance("events", 0, core.Mark{Offset: 41, LeaderEpoch: 7})
	assert.NoError(t, store.Save(context.Background(), saved))
	execSQL(t, conn, "CREATE TABLE agg (city VARCHAR)")
	execSQL(t, conn, "INSERT INTO agg VALUES ('NYC')")
	conn.Close()
	db.Close()

	cfg := writeConfig(t, dir, statePath)

	var out bytes.Buffer
	assert.NoError(t, runStats(context.Background(), cfg, true, &out))

	var got core.StateStats
	assert.NoError(t, json.Unmarshal(out.Bytes(), &got))
	assert.Equal(t, int64(1), got.Tables[0].Rows)
	assert.Equal(t, int64(41), got.Offsets[0].Offset)
}

// A running pipeline holds an exclusive lock, so this must say what to do
// instead of surfacing DuckDB's IO error.
func TestStats_ExplainsTheLockWhenPipelineIsRunning(t *testing.T) {
	dir := t.TempDir()
	statePath := filepath.Join(dir, "state.db")

	holderDB, err := duckdb.OpenPath(context.Background(), statePath)
	assert.NoError(t, err)
	defer holderDB.Close()
	holder, err := holderDB.Connect(context.Background())
	assert.NoError(t, err)
	defer holder.Close()

	cfg := writeConfig(t, dir, statePath)

	var out bytes.Buffer
	err = runStats(context.Background(), cfg, true, &out)
	assert.Error(t, err)
	assert.That(t, strings.Contains(err.Error(), "appears to be running"))
	assert.That(t, strings.Contains(err.Error(), "/stats"))
}

// A config with no state path is an error naming the missing setting, not a
// crash on an empty path.
func TestStats_RequiresAStatePath(t *testing.T) {
	dir := t.TempDir()
	cfg := writeConfig(t, dir, "")

	var out bytes.Buffer
	err := runStats(context.Background(), cfg, true, &out)
	assert.Error(t, err)
	assert.That(t, strings.Contains(err.Error(), "pipeline.state.path"))
}
```

`writeConfig` writes a minimal valid pipeline YAML into `dir`, with `pipeline.state.path` set to the argument and omitted entirely when it is empty. `execSQL` is the same four-line helper as elsewhere.

- [ ] **Step 2: Run test to verify it fails**

Run: `SQLFLOW_DUCKDB_LIB=/opt/homebrew/lib/libduckdb.dylib go test ./internal/cli/stats/ -v`
Expected: FAIL — package does not exist.

- [ ] **Step 3: Write minimal implementation**

```go
	db, err := duckdb.OpenPath(ctx, conf.Pipeline.State.Path)
	if err == nil {
		defer db.Close()
		_, err = db.Connect(ctx)
	}
	if err != nil {
		if strings.Contains(err.Error(), "Could not set lock on file") {
			return fmt.Errorf(
				"the pipeline using %s appears to be running: DuckDB holds an "+
					"exclusive lock on its state, so it cannot be read from here. "+
					"Read its /stats endpoint instead (served on the metrics "+
					"address, :8000 by default), or stop the pipeline",
				conf.Pipeline.State.Path)
		}
		return err
	}
```

Default output is a short human-readable summary; `--json` emits `StateStats` verbatim so it can be piped to `jq`.

- [ ] **Step 4: Run test to verify it passes**

Run: `SQLFLOW_DUCKDB_LIB=/opt/homebrew/lib/libduckdb.dylib go test ./internal/cli/... -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
gofmt -w internal/cli/ && go vet ./...
git add internal/cli/stats/ internal/cli/root.go
git commit -m "feat(cli): add sqlflow stats for a stopped pipeline"
```

---

## Task 6: Wire it together in `run`

**Files:**
- Modify: `internal/cli/run/root.go` (DuckDB open near the top of the command; `NewTurbine` call around line 191)

**Interfaces:**
- Consumes: Tasks 1–5.
- Produces: nothing new; this is the composition root.

Startup order, when `conf.Pipeline.State != nil`:

1. `db, err := duckdb.OpenPath(ctx, conf.Pipeline.State.Path)` instead of `duckdb.Open(ctx)`, then `conn, err := db.Connect(ctx)` for the pipeline's writer. Keep `db` alive for the process lifetime; the stats reader comes from the same handle.
2. `conn.(adbc.PostInitOptions).SetOption(adbc.OptionKeyAutoCommit, adbc.OptionValueDisabled)` — **once**, here.
3. Run `commands` and `tables` DDL as today, then `offsets.Init(ctx)`, then `conn.Commit(ctx)` so setup is durable before consumption starts.
4. `marks, err := offsets.Load(ctx)`; if the source is Kafka, `src.SeekTo(marks)`.
5. `NewTurbine(..., WithStateStore(offsets, conn))`.

When `State` is nil, none of this happens and the code path is byte-for-byte today's.

- [ ] **Step 1: Write the failing test**

This is the composition root, so the test is the acceptance test in Task 7. Do not invent a unit test for wiring; skip to Step 3 and let Task 7 gate it.

- [ ] **Step 2: (not applicable)**

- [ ] **Step 3: Write the implementation**

As described above. Two failure modes to handle explicitly rather than discover:

- The state file's directory may not exist. Create it with `os.MkdirAll(filepath.Dir(path), 0o755)` before opening, and fail with a message naming the path.
- A stored offset may be **before the topic's log start** (retention deleted it) or **past its log end** (topic recreated). `SetOffsets` will not fix this. Log a warning naming the topic, partition and stored offset, and let franz-go's `ConsumeResetOffset` handle it — the behaviour then matches `auto_offset_reset`. Document this in Task 8.

- [ ] **Step 4: Verify by hand before moving on**

```bash
make sqlflow
SQLFLOW_DUCKDB_LIB=/opt/homebrew/lib/libduckdb.dylib ./bin/sqlflow run -c <a windowed config with state.path> --max-msgs=100
duckdb /tmp/sqlflow/state.db -c "SELECT * FROM sqlflow_offsets"
```

Expected: one row per partition, offsets matching what was consumed.

- [ ] **Step 5: Commit**

```bash
gofmt -w internal/cli/ && go vet ./...
git add internal/cli/run/root.go
git commit -m "feat(run): open state on disk and resume from stored offsets"
```

---

## Task 7: Crash-recovery acceptance test

**Files:**
- Modify: `tests/release/test_image.py`
- Create: `dev/config/examples/kafka.stateful.window.yml`

**Interfaces:**
- Consumes: everything above.
- Produces: the test that proves the bug this plan exists to fix is gone.

This is the inverse of the failure recorded in "Established facts" #2.

- [ ] **Step 1: Write the failing test**

```python
def test_window_state_survives_a_crash(image):
    """A kill mid-window must not lose the aggregate.

    Before durable state, 2,000 events folded into an open window were lost by
    a SIGKILL while their offsets were already committed: the consumer group
    showed lag 0 and a restart replayed nothing, so the aggregate was silently
    short. State and offsets now commit together, so a restart rebuilds it.
    """
    topic = f"crash-recovery-{int(time.time())}"
    network = Network().create()

    kafka_ctr = KafkaContainer()
    kafka_ctr.with_network(network)
    kafka_ctr.with_network_aliases("kafka")
    kafka_ctr.start()

    producer = Producer({"bootstrap.servers": kafka_ctr.get_bootstrap_server()})
    ts = "2026-09-02T12:00:00.000Z"
    for i in range(2000):
        producer.produce(topic, json.dumps(
            {"timestamp": ts, "properties": {"city": "NYC"}, "user": {"id": str(i)}}))
    producer.flush()

    with tempfile.TemporaryDirectory() as state_dir:
        os.chmod(state_dir, 0o777)

        def run_until(n_messages):
            c = DockerContainer(image) \
                .with_volume_mapping(settings.DEV_DIR, "/tmp/conf") \
                .with_volume_mapping(state_dir, "/state") \
                .with_env("SQLFLOW_KAFKA_BROKERS", "kafka:9092") \
                .with_network(network) \
                .with_command(
                    "run /tmp/conf/config/examples/kafka.stateful.window.yml "
                    f"--max-msgs={n_messages}")
            c.start()
            wait_for_logs(c, "consumer loop ending|max messages consumed", timeout=120)
            return c

        # First half, then stop -- the window is still open, nothing published.
        run_until(1000)
        # Second half in a fresh process, reading the state left behind.
        run_until(1000)

        conn = duckdb.connect(os.path.join(state_dir, "state.db"), read_only=True)
        total = conn.execute("SELECT sum(count) FROM agg").fetchone()[0]
        offsets = conn.execute("SELECT sum(\"offset\") FROM sqlflow_offsets").fetchone()[0]

    # Every event is accounted for across the restart.
    assert total == 2000, f"aggregate lost rows across the restart: {total}"
    assert offsets == 1999, f"stored offset should be the last processed: {offsets}"
```

The config `dev/config/examples/kafka.stateful.window.yml` is the tumbling-window example plus `pipeline.state.path: /state/state.db`, an hour bucket, and a close predicate of `bucket < now() - INTERVAL '1' HOUR` so the window never closes during the test. `duckdb` is already a Python dependency via `requirements.txt`.

- [ ] **Step 2: Run test to verify it fails**

Run: `make sqlflow-image SQLFLOW_IMAGE=turbolytics/sql-flow:statetest && SQLFLOW_IMAGE=turbolytics/sql-flow:statetest pytest tests/release -k crash -v`
Expected: FAIL. Before Task 6 the image ignores `state.path`; expect the config to be rejected or `state.db` to be absent.

- [ ] **Step 3: (implementation is Tasks 1-6)**

- [ ] **Step 4: Run test to verify it passes**

Run: the same command, after rebuilding the image.
Expected: PASS, `total == 2000`.

- [ ] **Step 5: Commit**

```bash
git add tests/release/test_image.py dev/config/examples/kafka.stateful.window.yml
git commit -m "test: window state and offsets survive a restart"
```

---

## Task 7b: Benchmark the cost of durability

**Files:**
- Create: `dev/config/examples/benchmark.stateful.mem.yml`
- Modify: `scripts/benchmark-container.sh` (accept a state path), `README.md` (publish the numbers)

**Interfaces:**
- Consumes: Tasks 1-6.
- Produces: a published throughput and memory comparison, in-memory versus disk-backed state.

This design adds a DuckDB transaction commit to every batch. That is a real,
unmeasured cost on the hot path, and the README currently publishes ~927k
msgs/sec for `StructuredBatch` with no state. Shipping durability without
saying what it costs would repeat the mistake this whole plan exists to
correct: a claim nobody checked.

The benchmark must answer three questions an evaluator will ask.

1. **What does a disk-backed state pipeline cost versus an in-memory one?**
   Same config, same data, `state.path` set and unset.
2. **Does `batch_size` change the answer?** One fsync per batch means the cost
   per message falls as batches grow; at 500 it may dominate, at 5000 it may
   vanish. The existing harness already sweeps batch size.
3. **Does state grow the memory footprint?** The README's claim is a flat
   quarter GiB across handlers and batch sizes. DuckDB backed by a file has a
   buffer pool; verify the claim still holds or restate it.

- [ ] **Step 1: Write the benchmark config**

`dev/config/examples/benchmark.stateful.mem.yml` is `benchmark.inferred.mem.yml`
plus a `pipeline.state.path` templated from an environment variable, and a
windowed `tables.sql` block so the pipeline actually writes state rather than
just paying for an empty commit:

```yaml
tables:
  sql:
    - name: agg_city_count
      sql: |
        CREATE TABLE IF NOT EXISTS agg_city_count (
          bucket TIMESTAMPTZ, city VARCHAR, count INT
        );
        CREATE UNIQUE INDEX IF NOT EXISTS agg_city_count_idx
          ON agg_city_count (bucket, city);
```

with the handler upserting into it, exactly as `tumbling.window.yml` does. The
state path comes from `{{ SQLFLOW_STATE_PATH|default('') }}` so one config
serves both arms of the comparison.

- [ ] **Step 2: Teach the harness a state path**

`scripts/benchmark-container.sh` already takes `NUM_MESSAGES`, `BATCH_SIZE` and
`CONFIG`. Add a fourth, optional, passed through as `SQLFLOW_STATE_PATH` inside
the container and pointed at a container-local path so the file lives on the
container filesystem rather than a bind mount — a bind mount on Docker Desktop
measures the mount, not the engine, the same way host port-forwarding
understates throughput by ~10x (README, "Benchmarks must run inside the docker
network").

- [ ] **Step 3: Run both arms**

```bash
make start-backing-services
for bs in 500 2000 5000; do
  make benchmark-container NUM_MESSAGES=300000 BATCH_SIZE=$bs \
    CONFIG=dev/config/examples/benchmark.stateful.mem.yml
  make benchmark-container NUM_MESSAGES=300000 BATCH_SIZE=$bs \
    CONFIG=dev/config/examples/benchmark.stateful.mem.yml \
    STATE_PATH=/tmp/bench-state.db
done
```

Record throughput and both memory figures for each. Every run uses a fresh
topic and consumer group, so runs are hermetic — but delete the state file
between runs, or the second run resumes from the first's offsets and consumes
nothing.

- [ ] **Step 4: Publish the numbers, whatever they are**

Add a table to the README's Benchmarks section:

| Handler | `batch_size` | State | Throughput | Peak memory |
|---|---|---|---|---|

State the cost as a percentage in prose. If durability costs 30% at
`batch_size: 500` and 3% at 5000, say exactly that and recommend a floor. If it
costs more than expected, that is a finding to report, not a number to bury —
`state_commit_latency` from Task 4 will show where the time goes.

- [ ] **Step 5: Commit**

```bash
git add dev/config/examples/benchmark.stateful.mem.yml scripts/benchmark-container.sh README.md
git commit -m "bench: measure the cost of disk-backed state"
```

---

## Task 8: Phase 0 — state the guarantee

**Files:**
- Modify: `README.md` (the Kafka source section, and Sinks)

**Interfaces:** documentation only.

This is the piece that was shippable before any of the above; it is last here only because half of it describes Task 4's behaviour. Write it after the code lands so it describes what is true.

- [ ] **Step 1: Write the section**

Add under Sources → Kafka, after the existing offsets paragraph:

```markdown
## Delivery guarantees

SQLFlow gives two different guarantees, and which one applies depends on where
the data lands.

**Pipeline state — exactly-once relative to offsets.** With
`pipeline.state.path` set, the tables your handler writes and the Kafka offsets
that produced them are committed in a single DuckDB transaction. A crash
replays exactly the batches whose state was not committed, so a windowed
aggregate is neither short nor double-counted across a restart. The state file
is the source of truth; the offsets committed back to Kafka are advisory, for
lag monitoring.

**External sinks — at-least-once.** Kafka, ClickHouse, Iceberg and
`sqlcommand` are flushed before the transaction commits, so a crash between the
flush and the commit replays that batch and the sink sees those rows twice.
Use a sink that tolerates it: `ReplacingMergeTree` in ClickHouse, an upsert in
`sqlcommand`, or a downstream dedupe on a key.

**Without `pipeline.state.path`, DuckDB is in memory.** Handler state does not
survive the process, and a crash mid-window loses that window's aggregate while
its offsets are already committed — silently, with the consumer group showing
no lag. Set a state path for any pipeline that aggregates.
```

Also correct the Tumbling windows section, which currently implies durability it does not have, and add `pipeline.state` to the config shape block and the environment table.

- [ ] **Step 2: Check the claims against the code**

Re-read Task 4's ordering and confirm each sentence matches it. If they disagree, the documentation is wrong, not the code.

- [ ] **Step 3: Commit**

```bash
git add README.md
git commit -m "docs: state the delivery guarantees for state and for sinks"
```

---

## Out of scope

Named so nobody expands the plan mid-flight:

- **Declarative windows** (watermarks, session windows, lateness policy, engine-owned eviction). This is Phase 3 and it is a config-surface change; it should be designed against this state model once it exists.
- **Watermark delay metric.** Cannot exist until declarative windows do: the engine has no watermark to be delayed against while "closed" is arbitrary user SQL.
- **Per-key state metrics.** `state_table_rows` counts rows per table, which is the honest proxy available today. "Keys" is not a concept the engine has until windows are declarative.
- **Pushing metrics.** The Prometheus endpoint is scrape-only. OTLP export is a separate decision.
- **Multi-process or partitioned state.** DuckDB is a single-writer embedded database; one state file belongs to one pipeline process. Two processes on one file is not supported and should error rather than corrupt — worth its own task later, not here.
- **State compaction or retention.** The `sqlflow_offsets` table is bounded by partition count, but user window tables are bounded only by the user's own delete predicate. Phase 3's eviction policy addresses that.

## Self-review

- **Spec coverage.** Durability: config path (Task 2), DuckDB on disk (Task 1), offsets table (Task 3), atomic commit (Task 4), resume (Task 5), wiring (Task 6), acceptance gate (Task 7). Observability: consumer lag (5b), the stats snapshot and its gauges (5c), the `/stats` endpoint for a running pipeline (5d), the CLI for a stopped one (5e). Documentation: Task 8. Both design decisions from the 2026-09-02 conversation are honoured — offsets live in DuckDB, durability comes from a disk-backed database — as is the requirement that observability ship with the feature rather than after it.
- **Requested metrics, and where each lands.** On-disk size → `state_db_size_bytes` (5c). Row counts → `state_table_rows{table}` (5c). Offset lag → `consumer_lag{topic,partition}` (5b). The commit this design introduces → `state_commit_latency` / `state_commit_count` (Task 4, since that is where the commit is written). Per-pipeline on-disk stats → `/stats` (5d) live, `sqlflow stats` (5e) offline.
- **Placeholders.** Task 6 deliberately has no unit test, with the reason stated and the gate named. Task 1's first test contains a scan that must be replaced with the `countRows` helper; that instruction is explicit rather than left as "fix this".
- **Type consistency.** `Mark`, `OffsetStore`, `Save`, `Load`, `Init`, `SeekTo`, `WithStateStore`, `StateStats`, `TableStat`, `OffsetStat`, `CollectStateStats` are used identically wherever they appear. `Save` never commits; only Task 4 commits. `StateStats` is produced once (5c) and rendered by two consumers (5d, 5e), so the endpoint and the CLI cannot drift.
- **Gap found on review.** Task 4's step list did not mention the commit-latency instrument, which the metrics summary above assigns to it. Add `state_commit_latency` (histogram, seconds) and `state_commit_count` (counter) to `metrics.go` in Task 4's Step 3, recorded around `conn.Commit(ctx)`. Without it the one new source of per-batch latency this design introduces would be invisible.
