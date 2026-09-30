# Durable Counts Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** An event the webhook answered 200 for is in Kafka, a retry lands on its original's partition, and a one-minute count in Postgres is exact after a worker crashes, loses its disk, joins or leaves.

**Architecture:** Four engine changes and one fix the spec missed, each behind its own config key or scoped to windowed Kafka pipelines, so every existing config behaves as before:

1. The Kafka sink keys records by a column.
2. The webhook answers after the batch holding its body has flushed.
3. A windowed Kafka pipeline commits each partition's low watermark: the lowest offset still feeding a retained bucket. Kafka becomes the durable copy of window state.
4. That commit carries each window's closed watermark as commit metadata, so a worker that replays from it refuses what the original worker had already finalized.
5. A `partition_owned` window drops a revoked partition's rows instead of publishing them.

A tests-only package, `internal/metering`, runs the metering stack as real `sqlflow run` processes against Kafka and Postgres, and lands first so the PR records what `main` does before any fix.

**Tech Stack:** Go 1.26, franz-go v1.21.7 (`kgo.PreCommitFnContext`, `kgo.OnOffsetsFetched`), DuckDB 1.5.2 via ADBC, Arrow, testcontainers (Kafka `confluentinc/confluent-local:7.5.0`, Postgres 18 through `internal/pgtest`), zeebo/assert, the config JSON schema golden (`make schema`).

**Spec:** `docs/superpowers/specs/2026-09-29-metering-durable-counts-design.md`, including its "Revisions from planning" section, which this plan adds in Task 0.

## Global Constraints

- Defaults do not change behavior. `ack` defaults to `on_receive`. A Kafka sink with no `key` produces unkeyed records. A window without `partition_owned` keeps today's revocation behavior. The low-watermark commit applies only to a pipeline that windows and reads Kafka.
- Nothing on the consume loop's per-record path takes a mutex or allocates in the common case. The loop's budget is about 40 ns per record (`BenchmarkConsumeLoopWindowedWritePath`). Per-record work is accumulated in a slice and handed over once per batch, as `notePlaced` does.
- A record's bucket is `core.BucketStart(time.Unix(0, EventAtNanos), size)`. validate already requires `time_column` to be `time_bucket(INTERVAL '<size>', event_time)`, so this is the bucket the handler writes the row into.
- A bucket is retained while `bucket_end + lateness > closed`, where `closed` is the window manager's committed closed watermark. That is the manager's delete predicate (`expiredBefore`). The engine's own asserted watermark is never used to expire offset records, because it runs ahead of what the manager has published.
- New state tables carry no index. DuckDB never frees rows deleted from an indexed table (#268).
- Every config change runs `make schema` and commits `internal/validate/schemas/config.json`.
- Test commands: `export GOCACHE=$TMPDIR/gocache GOFLAGS=-mod=readonly`. macOS has no `timeout`; use `perl -e 'alarm shift; exec @ARGV' 900 go test ...`. Never let a pipe mask an exit code: `> $TMPDIR/out.txt 2>&1; echo rc=$?`. The webhook tests bind localhost ports and need `dangerouslyDisableSandbox`. `TestIntegration*` needs Docker.
- Commit messages name the defect, the fix and the evidence, in this repo's voice (`git log`). Push after each task with an explicit refspec: `git push origin metering/durable-counts`. Check `git branch --show-current` first.
- Prose in comments and docs follows CLAUDE.md: SQLFlow in prose, `sqlflow` in code.

## Review Focus

These are the inputs the spec implies but no task's happy-path test reaches. Each has its test in the named task:

1. **A webhook sender that hangs up while waiting for the flush.** Its body may still be flushed. The next request must not receive its answer, and nothing may leak. Test: `TestSourceWebhook_AfterFlushHangupDoesNotShiftAnswers` (Task 3).
2. **Records a revoked partition had prefetched, arriving after the drop.** They must not re-enter the window. Test: `TestTurbine_SkipsRecordsFromADroppedPartition` (Task 7).
3. **A replayed record older than anything the original worker still held.** It must be refused, not published alone over the full count. Test: scenario 11 and `TestTurbine_RefusesBelowTheReplayFloor` (Task 6).
4. **A partition revoked and later reassigned to the same process.** The process must not rewind to the position it loaded at startup. Test: `TestOffsetSeeker_ForgetsARevokedPartition` (Task 4).
5. **A batch that rolls back after offset records were noted.** The records must stay pending for the next commit, not be lost or written twice. Test: `TestWindowOffsets_RollbackKeepsPendingRecords` (Task 5).

---

## File structure

| file | responsibility after this plan |
|---|---|
| `internal/metering/harness_test.go` (new) | Kafka and Postgres containers, the `sqlflow` binary, config rendering, worker processes, workload and expected totals. |
| `internal/metering/scenarios_test.go` (new) | Scenarios 1 to 11. |
| `internal/config/config.go` | `KafkaSink.Key`, `WebhookSource.Ack`, `Window.PartitionOwned`, `Conf.CheckWebhookAck`, `Conf.CheckPartitionOwned`. |
| `internal/sinks/kafka.go`, `internal/sinks/rows.go` | keyed records; `tableRowKeys`. |
| `internal/webhook/source.go` | `ack: after_flush`; `Settle`. |
| `internal/core/turbine.go` | `Settler`; settle after commit; low-watermark commit; replay floor check; drop requests; skip revoked partitions. |
| `internal/core/marks.go` | `Marks.Forget`. |
| `internal/core/offsets.go` | `OffsetStore.Delete`. |
| `internal/core/windowoffsets.go` (new) | `WindowOffsets` tracker and `WindowOffsetStore` (`sqlflow_window_offsets`). |
| `internal/core/watermarks.go` | `WindowSpec.PartitionOwned`; `WindowSignal.SetClosed`/`Closed`, `LockPass`/`UnlockPass`. |
| `internal/core/partitiondrop.go` (new) | `PartitionDropper` and the DuckDB implementation. |
| `internal/kafka/seek.go` | `OffsetSeeker.Forget`. |
| `internal/kafka/source.go`, `internal/kafka/metadata.go` (new) | `CommitMarksWithMetadata`; the fetched-metadata relay. |
| `internal/sources/init.go` | registers the metadata relay on the client. |
| `internal/managers/watermark.go` | publishes the closed watermark on the signal; holds the pass lock during a pass. |
| `internal/cli/run/root.go`, `internal/cli/run/managers.go` | wiring for all of the above. |
| `internal/validate/sinks.go`, `internal/validate/window.go`, `internal/validate/webhook.go` (new) | the new rules. |
| `docs/coverage/invariants.yml`, `README.md`, `CHANGELOG.md`, `dev/config/examples/metering/*.yml` | Task 8. |

---

### Task 0: Record the planning revisions in the spec

**Files:**
- Modify: `docs/superpowers/specs/2026-09-29-metering-durable-counts-design.md`
- Create: `docs/superpowers/plans/2026-09-30-metering-durable-counts.md` (this file)

Reading the code turned up six points where the spec is wrong or silent. The spec gets a section that states each one, so the plan and the spec agree before any code is written.

- [x] **Step 1: Append the revisions section to the spec**

Add this section before `## Risks`:

```markdown
## Revisions from planning (2026-09-30)

Reading the code for the plan changed six things. Each is stated here so the
plan and the spec agree.

1. **Attribution is per record, and always exact.** The engine already
   computes each record's bucket from its event time, with the same function
   the handler's `time_bucket` uses (`core.BucketStart`). The offset record is
   keyed by that bucket and the record's own partition. The rule that
   attributed every partition to every bucket when the output lacked
   `kafka_partition` is gone.
2. **Offset records expire on the manager's closed watermark, not the
   engine's.** The engine's watermark runs ahead of what the manager has
   published. With lateness 0, a bucket is due and unpublished at the moment
   the engine's watermark passes it. Expiring its record then would commit
   past a bucket no sink has seen. The manager reports its closed watermark
   on the window's signal after each commit.
3. **Decision 5: the commit carries each window's closed watermark as Kafka
   commit metadata.** A worker that replays from the low watermark with no
   state of its own has no watermark. A late event the original worker
   refused, sitting after the low watermark, would be admitted into an
   expired minute and published alone, replacing the full count. The new
   owner reads the metadata when it is assigned the partition and refuses a
   record whose bucket ended at or before `closed - lateness`. franz-go
   supports both halves: `kgo.PreCommitFnContext` and `kgo.OnOffsetsFetched`.
   Scenario 11 tests it.
4. **Revoked partitions are forgotten.** The offset seeker kept the marks it
   loaded at startup and reapplied them whenever a partition came back, so a
   partition revoked and later reassigned rewound to a stale position. The
   seeker now forgets a released or lost partition. A windowed pipeline also
   drops the partition's marks, so it stops committing a partition another
   member owns.
5. **A drop waits for the window manager.** A manager pass in flight when
   the partition is revoked could publish the partition's partial rows after
   the callback returned. The drop takes the window's pass lock, so no pass
   runs while the rows are deleted. Records the consumer had prefetched from
   the partition are skipped when they arrive.
6. **A null key stops the pipeline.** A sink write error is fatal whatever
   the error policy says, so a row with a null key fails with
   `user.sink.encode_failed` and the pipeline stops. The ingest handler
   filters rows with no customer before the sink.

The scenario table changes too:

- Scenarios 4 and 5 run one worker. With three, the killed worker's
  partitions move to the others after the 45-second session timeout, and the
  scenario becomes scenario 3.
- Scenario 9 runs in process: a real webhook source and turbine with a sink
  whose flush fails. A failed flush against a real broker needs a fault
  proxy.
- Scenario 11 is new: a worker refuses a late event and loses its disk
  while a later minute is still open, and a fresh worker replays past the
  late event. Expected on `main`: fails, because the open minute's rows are
  lost, as in scenario 5. With decision 3 alone it fails differently: the
  late event's minute is published with that event alone. With decision 5
  it passes.
```

Add a row to the scenario table:

```markdown
| 11 | A late event refused, then the worker's disk lost with a later minute open: the fresh worker replays past it | fails on `main`: the open minute undercounts; fails with decision 3 alone: the late event's minute is overwritten; passes with decision 5 |
```

- [x] **Step 2: Commit the spec and this plan**

```bash
git branch --show-current   # metering/durable-counts
git add docs/superpowers/specs/2026-09-29-metering-durable-counts-design.md docs/superpowers/plans/2026-09-30-metering-durable-counts.md
git commit -m "plan: durable counts, and six spec revisions the code forced

Reading the turbine, the Kafka source and the manager for the plan found
that offset records must expire on the manager's closed watermark, that a
fresh worker replaying from the low watermark can publish a lone late event
over a full count, that the seeker rewinds a reassigned partition to its
startup position, and that a manager pass can publish a revoked
partition's rows mid-drop. The spec now says how each is handled."
git push origin metering/durable-counts
```

---

### Task 1: The metering harness and scenarios, run against the current engine

**Files:**
- Create: `internal/metering/harness_test.go`
- Create: `internal/metering/scenarios_test.go`

**Interfaces:**
- Produces: `type Features struct{ Ack, Key, PartitionOwned bool }` and `var current Features`. Tasks 2, 3 and 7 each set one field to true in `current`. Every scenario reads `current`, so the same test runs before and after each fix.

The package is tests only, like `internal/rendertemplate`. Workers are real `sqlflow run` processes, so `kill -9` is real and a lost disk is a new state path.

- [ ] **Step 1: Confirm `md5_number`'s type in the pinned DuckDB**

Run: `echo "SELECT typeof(md5_number('x'))" | duckdb`. If no DuckDB CLI is installed, add a throwaway test in `internal/core` that opens `duckdb.OpenPath(ctx, "")`, runs the query, and prints the result. Delete the test afterwards.

Expected: `UHUGEINT`. If it prints `HUGEINT`, declare `id_hash HUGEINT` in the window table below instead.

- [ ] **Step 2: Write the harness**

`internal/metering/harness_test.go`:

```go
// Package metering holds the usage-metering stack to exact counts: webhook
// ingest into Kafka keyed by customer, three count workers in one consumer
// group, a one-minute window with allowed lateness, and a Postgres upsert
// keyed by (minute, customer, meter, kafka_partition). The stack is
// processes, not goroutines, because the failures it exists for are a
// process killed mid-minute and a disk that did not come back.
//
// Tests only. The scenarios read current, so each fix turns its feature on
// here and the same scenario that failed before it must pass after it.
package metering

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"testing"
	"text/template"
	"time"

	"github.com/jackc/pgx/v5"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/turbolytics/sql-flow/internal/pgtest"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/zeebo/assert"
)

// Features are the engine changes a scenario's configs use. Task 1 lands
// with none; each fix turns its own on.
type Features struct {
	Ack            bool // ingest: webhook ack: after_flush
	Key            bool // ingest: kafka sink key: customer
	PartitionOwned bool // count: window partition_owned: true
}

var current = Features{}

var sharedPostgres = pgtest.New(pgtest.Options{User: "metering", Password: "metering"})

func TestMain(m *testing.M) {
	code := sharedPostgres.Run(m)
	if brokerCtr != nil {
		_ = brokerCtr.Terminate(context.Background())
	}
	os.Exit(code)
}

// brokerImage matches internal/kafka's, so a machine that ran those tests
// has the image.
const brokerImage = "confluentinc/confluent-local:7.5.0"

var (
	brokerOnce sync.Once
	brokerAddr string
	brokerErr  error
	brokerCtr  *tckafka.KafkaContainer
)

func broker(t *testing.T) string {
	t.Helper()
	if testing.Short() {
		t.Skip("integration test: -short runs the unit pass only")
	}
	if addr := os.Getenv("SQLFLOW_KAFKA_BROKERS"); addr != "" {
		return addr
	}
	brokerOnce.Do(func() {
		ctx := context.Background()
		brokerCtr, brokerErr = tckafka.Run(ctx, brokerImage)
		if brokerErr != nil {
			return
		}
		var addrs []string
		addrs, brokerErr = brokerCtr.Brokers(ctx)
		if brokerErr == nil && len(addrs) == 0 {
			brokerErr = fmt.Errorf("kafka container reported no brokers")
		}
		if brokerErr == nil {
			brokerAddr = addrs[0]
		}
	})
	if brokerErr != nil {
		t.Fatalf("starting kafka: %v", brokerErr)
	}
	return brokerAddr
}

var (
	binOnce sync.Once
	binPath string
	binErr  error
)

// sqlflowBinary builds this checkout's sqlflow once per test binary. The
// scenarios test the engine the branch builds, never an installed one.
func sqlflowBinary(t *testing.T) string {
	t.Helper()
	binOnce.Do(func() {
		dir, err := os.MkdirTemp("", "sqlflow-metering-bin")
		if err != nil {
			binErr = err
			return
		}
		binPath = filepath.Join(dir, "sqlflow")
		cmd := exec.Command("go", "build", "-o", binPath, "github.com/turbolytics/sql-flow/cmd/sqlflow")
		cmd.Env = append(os.Environ(), "CGO_ENABLED=1")
		out, err := cmd.CombinedOutput()
		if err != nil {
			binErr = fmt.Errorf("go build: %v\n%s", err, out)
		}
	})
	if binErr != nil {
		t.Fatal(binErr)
	}
	return binPath
}

// stack is one scenario's world: its own topic, group and database.
type stack struct {
	t        *testing.T
	bin      string
	broker   string
	topic    string
	group    string
	dsn      string
	pg       *pgx.Conn
	dir      string
	features Features
	// partitions is the topic's partition count.
	partitions int32
}

func newStack(t *testing.T, f Features) *stack {
	t.Helper()
	b := broker(t)
	dsn, _ := sharedPostgres.Database(t)
	ctx := context.Background()
	pg, err := pgx.Connect(ctx, dsn)
	assert.NoError(t, err)
	t.Cleanup(func() { _ = pg.Close(context.Background()) })
	_, err = pg.Exec(ctx, `SET TIME ZONE 'UTC'`)
	assert.NoError(t, err)
	_, err = pg.Exec(ctx, `CREATE TABLE usage_per_minute (
		minute          TIMESTAMPTZ NOT NULL,
		customer        TEXT        NOT NULL,
		meter           TEXT        NOT NULL,
		kafka_partition INTEGER     NOT NULL,
		quantity        BIGINT      NOT NULL,
		PRIMARY KEY (minute, customer, meter, kafka_partition))`)
	assert.NoError(t, err)

	name := strings.ToLower(strings.ReplaceAll(t.Name(), "/", "-"))
	s := &stack{
		t:          t,
		bin:        sqlflowBinary(t),
		broker:     b,
		topic:      fmt.Sprintf("usage-%s-%d", name, time.Now().UnixNano()),
		group:      fmt.Sprintf("count-%s-%d", name, time.Now().UnixNano()),
		dsn:        dsn,
		pg:         pg,
		dir:        t.TempDir(),
		features:   f,
		partitions: 6,
	}
	s.createTopic()
	return s
}

func (s *stack) createTopic() {
	s.t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(s.broker))
	assert.NoError(s.t, err)
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	req := kmsg.NewPtrCreateTopicsRequest()
	rt := kmsg.NewCreateTopicsRequestTopic()
	rt.Topic, rt.NumPartitions, rt.ReplicationFactor = s.topic, s.partitions, 1
	req.Topics = append(req.Topics, rt)
	req.TimeoutMillis = 20000
	resp, err := req.RequestWith(ctx, cl)
	assert.NoError(s.t, err)
	assert.Equal(s.t, int16(0), resp.Topics[0].ErrorCode)
}

// event is one metered quantity, after ingest unnests it: one row per meter.
type event struct {
	ID       string `json:"id"`
	Customer string `json:"customer"`
	Meter    string `json:"meter"`
	Quantity int64  `json:"quantity"`
	TSMillis int64  `json:"ts_ms"`
}

// workload is a fixed event set: customers x perCustomer events, each with
// three meters, spread evenly over span from start. The seed makes a
// failure reproducible.
func workload(seed int64, customers, perCustomer int, start time.Time, span time.Duration) []event {
	r := rand.New(rand.NewSource(seed))
	var out []event
	n := customers * perCustomer
	for i := 0; i < n; i++ {
		c := fmt.Sprintf("user_%05d", i%customers)
		at := start.Add(time.Duration(int64(span) * int64(i) / int64(n)))
		id := fmt.Sprintf("evt_%d_%d", seed, i)
		for _, m := range []struct {
			meter string
			q     int64
		}{{"requests", 1}, {"input_tokens", 100 + r.Int63n(900)}, {"output_tokens", 50 + r.Int63n(400)}} {
			out = append(out, event{ID: id, Customer: c, Meter: m.meter, Quantity: m.q, TSMillis: at.UnixMilli()})
		}
	}
	return out
}

type totalKey struct {
	Minute   time.Time
	Customer string
	Meter    string
}

// expected is the exact answer: the sum of quantity per (minute, customer,
// meter) over distinct (id, meter). A duplicate in events counts once.
func expected(events []event) map[totalKey]int64 {
	seen := map[[2]string]bool{}
	out := map[totalKey]int64{}
	for _, e := range events {
		k := [2]string{e.ID, e.Meter}
		if seen[k] {
			continue
		}
		seen[k] = true
		minute := time.UnixMilli(e.TSMillis).UTC().Truncate(time.Minute)
		out[totalKey{minute, e.Customer, e.Meter}] += e.Quantity
	}
	return out
}

// produce writes events to the topic keyed by customer, as a keyed ingest
// does, synchronously.
func (s *stack) produce(events []event) {
	s.t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(s.broker), kgo.ProducerLinger(5*time.Millisecond))
	assert.NoError(s.t, err)
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	recs := make([]*kgo.Record, len(events))
	for i, e := range events {
		v, err := json.Marshal(e)
		assert.NoError(s.t, err)
		recs[i] = &kgo.Record{Topic: s.topic, Key: []byte(e.Customer), Value: v}
	}
	assert.NoError(s.t, cl.ProduceSync(ctx, recs...).FirstErr())
}

// totals reads Postgres summed over partitions, which is what a reader of
// the stack sees.
func (s *stack) totals() map[totalKey]int64 {
	s.t.Helper()
	rows, err := s.pg.Query(context.Background(),
		`SELECT minute, customer, meter, sum(quantity)::BIGINT FROM usage_per_minute GROUP BY 1, 2, 3`)
	assert.NoError(s.t, err)
	defer rows.Close()
	out := map[totalKey]int64{}
	for rows.Next() {
		var k totalKey
		var q int64
		assert.NoError(s.t, rows.Scan(&k.Minute, &k.Customer, &k.Meter, &q))
		k.Minute = k.Minute.UTC()
		out[k] = q
	}
	assert.NoError(s.t, rows.Err())
	return out
}

// awaitExact waits until Postgres holds exactly want, then holds it for
// settle: a partial republish landing after the right answer is the
// failure the rebalance scenarios exist for, so reaching it once is not
// enough. On timeout it fails with the first differences.
func (s *stack) awaitExact(want map[totalKey]int64, timeout, settle time.Duration) {
	s.t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		got := s.totals()
		if diff := diffTotals(want, got); len(diff) == 0 {
			hold := time.Now().Add(settle)
			for time.Now().Before(hold) {
				time.Sleep(500 * time.Millisecond)
				if d := diffTotals(want, s.totals()); len(d) > 0 {
					s.t.Fatalf("exact, then not: %d keys changed after the right answer, first: %s",
						len(d), strings.Join(first(d, 10), "; "))
				}
			}
			return
		} else if time.Now().After(deadline) {
			s.t.Fatalf("totals not exact after %s: %d keys differ, first: %s\nworker logs: %s",
				timeout, len(diff), strings.Join(first(diff, 10), "; "), s.dir)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// awaitAny waits until Postgres holds any row: some window has published.
func (s *stack) awaitAny(timeout time.Duration) {
	s.t.Helper()
	deadline := time.Now().Add(timeout)
	for len(s.totals()) == 0 {
		if time.Now().After(deadline) {
			s.t.Fatalf("nothing published within %s; worker logs: %s", timeout, s.dir)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func diffTotals(want, got map[totalKey]int64) []string {
	var out []string
	for k, w := range want {
		if g := got[k]; g != w {
			out = append(out, fmt.Sprintf("%s %s %s want %d got %d", k.Minute.Format(time.RFC3339), k.Customer, k.Meter, w, g))
		}
	}
	for k, g := range got {
		if _, ok := want[k]; !ok {
			out = append(out, fmt.Sprintf("%s %s %s want nothing got %d", k.Minute.Format(time.RFC3339), k.Customer, k.Meter, g))
		}
	}
	sort.Strings(out)
	return out
}

func first(s []string, n int) []string {
	if len(s) < n {
		return s
	}
	return s[:n]
}

const countConfig = `
tables:
  sql:
    - name: usage_window
      sql: |
        CREATE TABLE IF NOT EXISTS usage_window (
          minute TIMESTAMPTZ,
          customer TEXT,
          meter TEXT,
          kafka_partition INTEGER,
          id_hash UHUGEINT,
          quantity BIGINT
        );
      window:
        time_column: minute
        size_seconds: 60
        grace_seconds: 5
        idle_close_seconds: 5
        allowed_lateness_seconds: 60
{{- if .Features.PartitionOwned }}
        partition_owned: true
{{- end }}
        emit_sql: |
          SELECT minute, customer, meter, kafka_partition, sum(quantity)::BIGINT AS quantity
          FROM (
            SELECT minute, customer, meter, kafka_partition, id_hash, any_value(quantity) AS quantity
            FROM closed
            GROUP BY ALL
          )
          GROUP BY ALL
        sink:
          type: postgres
          postgres:
            dsn: "{{ .DSN }}"
            table: usage_per_minute
            mode: upsert
            key: [minute, customer, meter, kafka_partition]
pipeline:
  name: count
  batch_size: 500
  flush_interval_seconds: 1
  state:
    path: {{ .StatePath }}
  source:
    type: kafka
    kafka:
      brokers: ["{{ .Broker }}"]
      group_id: {{ .Group }}
      auto_offset_reset: earliest
      topics: ["{{ .Topic }}"]
      event_time:
        path: ts_ms
        format: unix_ms
  handler:
    type: handlers.InferredMemBatch
    sql: |
      INSERT INTO usage_window
      SELECT
        time_bucket(INTERVAL '60 seconds', event_time) AS minute,
        customer,
        meter,
        kafka_partition,
        md5_number(id) AS id_hash,
        quantity
      FROM batch
  sink:
    type: noop
`

const ingestConfig = `
pipeline:
  name: ingest
  batch_size: {{ .BatchSize }}
  flush_interval_seconds: {{ .FlushSeconds }}
  source:
    type: webhook
    webhook:
      addr: "{{ .Addr }}"
{{- if .Features.Ack }}
      ack: after_flush
{{- end }}
  handler:
    type: handlers.InferredMemBatch
    sql: |
      SELECT e.id, e.customer, e.meter, e.quantity, e.ts_ms
      FROM (SELECT unnest(events) AS e FROM batch)
      WHERE e.id IS NOT NULL AND e.customer IS NOT NULL
  sink:
    type: kafka
    kafka:
      brokers: ["{{ .Broker }}"]
      topic: {{ .Topic }}
{{- if .Features.Key }}
      key: customer
{{- end }}
`

// worker is one sqlflow run process.
type worker struct {
	name string
	cmd  *exec.Cmd
	log  string
	done chan error
}

func (s *stack) render(name, tmpl string, vars map[string]any) string {
	s.t.Helper()
	vars["Features"] = s.features
	vars["Broker"] = s.broker
	vars["Topic"] = s.topic
	var buf bytes.Buffer
	assert.NoError(s.t, template.Must(template.New(name).Parse(tmpl)).Execute(&buf, vars))
	path := filepath.Join(s.dir, name+".yml")
	assert.NoError(s.t, os.WriteFile(path, buf.Bytes(), 0o644))
	return path
}

// startCount starts a count worker whose state lives at state, a name under
// the scenario's directory. A new name is a lost disk.
func (s *stack) startCount(name, state string) *worker {
	s.t.Helper()
	cfg := s.render(name, countConfig, map[string]any{
		"DSN": s.dsn, "Group": s.group, "StatePath": filepath.Join(s.dir, state+".duckdb"),
	})
	return s.start(name, cfg)
}

// startIngest starts the webhook ingest and returns its URL.
func (s *stack) startIngest(name string, batchSize, flushSeconds int) (*worker, string) {
	s.t.Helper()
	addr := freeAddr(s.t)
	cfg := s.render(name, ingestConfig, map[string]any{
		"Addr": addr, "BatchSize": batchSize, "FlushSeconds": flushSeconds,
	})
	w := s.start(name, cfg)
	url := "http://" + addr + "/events"
	waitHTTP(s.t, "http://"+addr+"/healthz", 60*time.Second)
	return w, url
}

func (s *stack) start(name, cfg string) *worker {
	s.t.Helper()
	logPath := filepath.Join(s.dir, name+".log")
	f, err := os.Create(logPath)
	assert.NoError(s.t, err)
	cmd := exec.Command(s.bin, "run", cfg)
	cmd.Stdout, cmd.Stderr = f, f
	assert.NoError(s.t, cmd.Start())
	w := &worker{name: name, cmd: cmd, log: logPath, done: make(chan error, 1)}
	go func() { w.done <- cmd.Wait(); f.Close() }()
	s.t.Cleanup(func() { w.kill9() })
	return w
}

// kill9 is a crash: no drain, no final pass, no commit.
func (w *worker) kill9() {
	if w.cmd.ProcessState == nil {
		_ = w.cmd.Process.Signal(syscall.SIGKILL)
		<-w.done
	}
}

// stop is a graceful leave: SIGTERM and wait for the drain.
func (w *worker) stop(t *testing.T) {
	t.Helper()
	_ = w.cmd.Process.Signal(syscall.SIGTERM)
	select {
	case <-w.done:
	case <-time.After(60 * time.Second):
		t.Fatalf("%s did not exit within 60s of SIGTERM; log: %s", w.name, w.log)
	}
}

func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	assert.NoError(t, err)
	addr := ln.Addr().String()
	assert.NoError(t, ln.Close())
	return addr
}

func waitHTTP(t *testing.T, url string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if resp, err := httpClient.Get(url); err == nil {
			resp.Body.Close()
			if resp.StatusCode == 200 {
				return
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("%s did not answer 200 within %s", url, timeout)
}

// recent is the start of a workload: whole minutes in the past, inside the
// engine's placement window and far enough back that every minute can close.
func recent(minutesAgo int) time.Time {
	return time.Now().UTC().Truncate(time.Minute).Add(-time.Duration(minutesAgo) * time.Minute)
}

// httpClient bounds every request: an after_flush 200 waits up to a flush
// interval, and a sender under a killed ingest must not hang the test.
var httpClient = &http.Client{Timeout: 30 * time.Second}
```

- [ ] **Step 3: Write the scenarios**

`internal/metering/scenarios_test.go`:

```go
package metering

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/zeebo/assert"
)

// Every scenario feeds a fixed event set whose answer is computed here, and
// waits for Postgres to hold exactly that answer summed over partitions.
// A count that is off by one fails.

const exactWithin = 3 * time.Minute

// 1. Steady state: three workers up throughout.
func TestIntegrationMetering_SteadyState(t *testing.T) {
	coverage.Covers(t, "source.kafka")
	s := newStack(t, current)
	events := workload(1, 200, 20, recent(8), 5*time.Minute)
	for i := 0; i < 3; i++ {
		s.startCount(name("count", i), name("state", i))
	}
	s.produce(events)
	s.awaitExact(expected(events), exactWithin, 10*time.Second)
}

// 2. A worker joins mid-stream, forcing a rebalance while minutes are open.
func TestIntegrationMetering_WorkerJoins(t *testing.T) {
	coverage.Covers(t, "source.kafka")
	s := newStack(t, current)
	events := workload(2, 200, 20, recent(8), 5*time.Minute)
	half := len(events) / 2
	s.startCount("count-0", "state-0")
	s.startCount("count-1", "state-1")
	s.produce(events[:half])
	time.Sleep(10 * time.Second) // rows are in open windows now
	s.startCount("count-2", "state-2")
	s.produce(events[half:])
	s.awaitExact(expected(events), exactWithin, 20*time.Second)
}

// 3. A worker leaves gracefully mid-stream.
func TestIntegrationMetering_WorkerLeaves(t *testing.T) {
	coverage.Covers(t, "source.kafka")
	s := newStack(t, current)
	events := workload(3, 200, 20, recent(8), 5*time.Minute)
	half := len(events) / 2
	w0 := s.startCount("count-0", "state-0")
	s.startCount("count-1", "state-1")
	s.startCount("count-2", "state-2")
	s.produce(events[:half])
	time.Sleep(10 * time.Second)
	w0.stop(t)
	s.produce(events[half:])
	s.awaitExact(expected(events), exactWithin, 20*time.Second)
}

// 4. kill -9, state kept. One worker, so the only recovery path is the
// restart on the same disk; with three, the partitions move after the
// session timeout and this becomes scenario 3.
func TestIntegrationMetering_CrashStateKept(t *testing.T) {
	coverage.Covers(t, "source.kafka")
	s := newStack(t, current)
	events := workload(4, 200, 20, recent(8), 5*time.Minute)
	half := len(events) / 2
	w := s.startCount("count-0", "state-0")
	s.produce(events[:half])
	time.Sleep(10 * time.Second)
	w.kill9()
	s.startCount("count-0b", "state-0")
	s.produce(events[half:])
	s.awaitExact(expected(events), exactWithin, 10*time.Second)
}

// 5. kill -9, state lost: the restart has a new state path.
func TestIntegrationMetering_CrashStateLost(t *testing.T) {
	coverage.Covers(t, "source.kafka")
	s := newStack(t, current)
	events := workload(5, 200, 20, recent(8), 5*time.Minute)
	half := len(events) / 2
	w := s.startCount("count-0", "state-0")
	s.produce(events[:half])
	time.Sleep(10 * time.Second)
	w.kill9()
	s.startCount("count-0b", "state-new")
	s.produce(events[half:])
	s.awaitExact(expected(events), exactWithin, 10*time.Second)
}

// 6. Every request is sent twice, as a client retries a lost 200, through
// the webhook and the Kafka sink. The duplicate must land on its original's
// partition to be deduplicated there.
func TestIntegrationMetering_RetriedDuplicates(t *testing.T) {
	coverage.Covers(t, "source.webhook", "sink.kafka")
	s := newStack(t, current)
	events := workload(6, 100, 10, recent(8), 5*time.Minute)
	_, url := s.startIngest("ingest", 50, 1)
	for i := 0; i < 3; i++ {
		s.startCount(name("count", i), name("state", i))
	}
	for _, b := range batches(events, 25) {
		for attempt := 0; attempt < 2; attempt++ {
			code := postEvents(t, url, b)
			assert.Equal(t, http.StatusOK, code)
		}
	}
	s.awaitExact(expected(events), exactWithin, 10*time.Second)
}

// 7. Late events within allowed_lateness_seconds republish their minute.
func TestIntegrationMetering_LateWithinLateness(t *testing.T) {
	coverage.Covers(t, "source.kafka")
	s := newStack(t, current)
	start := recent(8)
	events := workload(7, 100, 10, start, 5*time.Minute)
	for i := 0; i < 3; i++ {
		s.startCount(name("count", i), name("state", i))
	}
	s.produce(events)
	// 30 s late for minute 3: its end is 90 s before the newest event, and
	// the watermark is newest - grace, so it closed and is within 60 s of
	// lateness only if it arrives before the stream moves on. Sent while
	// the stream is still at minute 4.
	late := workload(70, 100, 1, start.Add(3*time.Minute+30*time.Second), time.Second)
	s.produce(late)
	s.awaitExact(expected(append(events, late...)), exactWithin, 10*time.Second)
}

// 8. Kill ingest while requests are in flight. Every event answered 200
// must be in Kafka.
func TestIntegrationMetering_IngestKilledBeforeFlush(t *testing.T) {
	coverage.Covers(t, "source.webhook")
	s := newStack(t, current)
	w, url := s.startIngest("ingest", 1000, 2)
	var (
		mu       sync.Mutex
		answered []string
	)
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				e := event{ID: name(name("evt", g), i), Customer: "user_1", Meter: "requests", Quantity: 1,
					TSMillis: time.Now().UnixMilli()}
				if code := postEventsNoFail([]event{e}, url); code == http.StatusOK {
					mu.Lock()
					answered = append(answered, e.ID)
					mu.Unlock()
				}
			}
		}(g)
	}
	time.Sleep(3 * time.Second)
	w.kill9()
	close(stop)
	wg.Wait()
	inKafka := s.readIDs()
	var missing []string
	for _, id := range answered {
		if !inKafka[id] {
			missing = append(missing, id)
		}
	}
	if len(missing) > 0 {
		t.Fatalf("%d of %d events answered 200 are not in Kafka, first: %v", len(missing), len(answered), first(missing, 10))
	}
	t.Logf("%d events answered 200, all in Kafka", len(answered))
}

// 10. Rebalance under load on one hot partition. Exact totals, and the time
// to reach them against the same load with no rebalance.
func TestIntegrationMetering_RebalanceUnderLoad(t *testing.T) {
	coverage.Covers(t, "source.kafka")
	for _, join := range []bool{false, true} {
		join := join
		t.Run(map[bool]string{false: "control", true: "join"}[join], func(t *testing.T) {
			s := newStack(t, current)
			// 80% of events for one customer: one partition carries most of
			// the load, which is the partition whose replay costs the most.
			hot := workload(10, 1, 160000, recent(6), 4*time.Minute)
			cold := workload(11, 400, 100, recent(6), 4*time.Minute)
			events := append(hot, cold...)
			s.startCount("count-0", "state-0")
			s.startCount("count-1", "state-1")
			t0 := time.Now()
			s.produce(events[:len(events)/2])
			if join {
				s.startCount("count-2", "state-2")
			}
			s.produce(events[len(events)/2:])
			s.awaitExact(expected(events), 5*time.Minute, 20*time.Second)
			t.Logf("rebalance_under_load join=%v events=%d exact_after=%s", join, len(events), time.Since(t0))
		})
	}
}

// 11. A worker refuses a late event, then loses its disk while a later
// minute is still open. The fresh worker replays from the open minute's
// records, which sit before the late event in the log, so it reads the late
// event again. The minute it was late for must keep its count.
func TestIntegrationMetering_ReplayPastARefusedLateEvent(t *testing.T) {
	coverage.Covers(t, "source.kafka")
	s := newStack(t, current)
	start := recent(10)
	early := workload(12, 50, 10, start, 2*time.Minute)                    // minutes 0 and 1
	later := workload(13, 50, 10, start.Add(5*time.Minute), 3*time.Minute) // minutes 5 to 7
	w := s.startCount("count-0", "state-0")
	s.produce(early)
	// Minute 1's events carry the watermark past minute 0's end, so minute 0
	// publishes: the worker is consuming.
	s.awaitAny(90 * time.Second)
	s.produce(later)
	// At once, while minute 7 is open: minute 0 ended about seven minutes
	// before the watermark, far past 60 s of lateness, so the worker refuses
	// it. Same customer as early[0], so same partition as that customer's
	// minute 7 records, which the low watermark holds the commit behind.
	straggler := []event{{ID: "straggler", Customer: early[0].Customer, Meter: "requests", Quantity: 1,
		TSMillis: start.Add(10 * time.Second).UnixMilli()}}
	s.produce(straggler)
	// One flush and commit, and less than idle_close_seconds, so minute 7 is
	// still open when the worker dies.
	time.Sleep(3 * time.Second)
	w.kill9()
	s.startCount("count-0b", "state-new")
	s.awaitExact(expected(append(early, later...)), exactWithin, 20*time.Second)
}

func name(prefix string, i int) string { return prefix + "-" + itoa(i) }

func itoa(i int) string { return fmt.Sprintf("%d", i) }

func batches(events []event, n int) [][]event {
	var out [][]event
	for len(events) > 0 {
		k := min(n, len(events))
		out = append(out, events[:k])
		events = events[k:]
	}
	return out
}

func postEvents(t *testing.T, url string, events []event) int {
	t.Helper()
	code := postEventsNoFail(events, url)
	if code == 0 {
		t.Fatalf("posting to %s failed", url)
	}
	return code
}

// postEventsNoFail returns 0 when the request did not complete, which is
// what a sender sees when ingest dies under it.
func postEventsNoFail(events []event, url string) int {
	body, _ := json.Marshal(map[string]any{"events": events})
	resp, err := httpClient.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		return 0
	}
	resp.Body.Close()
	return resp.StatusCode
}

// readIDs reads the topic to its end with a fresh consumer.
func (s *stack) readIDs() map[string]bool {
	s.t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(s.broker), kgo.ConsumeTopics(s.topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	assert.NoError(s.t, err)
	defer cl.Close()
	ids := map[string]bool{}
	idle := 0
	for idle < 3 {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		f := cl.PollFetches(ctx)
		cancel()
		if f.NumRecords() == 0 {
			idle++
			continue
		}
		idle = 0
		f.EachRecord(func(r *kgo.Record) {
			var e event
			if json.Unmarshal(r.Value, &e) == nil {
				ids[e.ID] = true
			}
		})
	}
	return ids
}
```

Scenario 9 (a failed flush answers 503) lives in `internal/webhook` and is written in Task 3; it needs `ack`, which does not exist yet.

- [ ] **Step 4: Compile and run the unit pass**

Run: `go vet ./internal/metering/ && go test -short ./internal/metering/`
Expected: vet clean; every test skips.

- [ ] **Step 5: Run the scenarios against the current engine and record what each did**

Run (needs Docker; takes about 15 minutes):

```bash
perl -e 'alarm shift; exec @ARGV' 2400 go test -run '^TestIntegrationMetering' -parallel 3 -timeout 40m -v ./internal/metering/ > $TMPDIR/metering-baseline.txt 2>&1; echo rc=$?
grep -E '^(=== RUN|--- (PASS|FAIL)|.*rebalance_under_load)' $TMPDIR/metering-baseline.txt
```

Expected, the spec's predictions:

| # | Scenario | Prediction on `main` |
|---|---|---|
| 1 | SteadyState | PASS |
| 2 | WorkerJoins | FAIL: undercount or overwrite |
| 3 | WorkerLeaves | FAIL: undercount |
| 4 | CrashStateKept | PASS |
| 5 | CrashStateLost | FAIL: undercount |
| 6 | RetriedDuplicates | FAIL: a retry on another partition doubles |
| 7 | LateWithinLateness | PASS |
| 8 | IngestKilledBeforeFlush | FAIL: answered events missing from Kafka |
| 10 | RebalanceUnderLoad/join | FAIL, as 2 |
| 11 | ReplayPastARefusedLateEvent | FAIL: minute 7 undercounts, as 5 |

A scenario that fails for a reason other than the predicted one, such as a config the engine refuses or a harness timeout, is a harness bug. Fix it before recording. A prediction that turns out wrong is recorded as wrong, not adjusted.

- [ ] **Step 6: Commit, and record the results in the PR**

```bash
git add internal/metering/
git commit -m "metering: the durable-counts scenarios, run against the engine as it is

Eleven scenarios run the metering stack as real sqlflow processes against
Kafka and Postgres and require exact totals per minute, customer and meter.
On this commit: <paste the PASS/FAIL line per scenario>. The failing ones
are the defects #414 fixes; each fix turns its feature on in the harness."
git push origin metering/durable-counts
gh pr edit 414 --body-file <(gh pr view 414 --json body -q .body; printf '\n\n## Baseline, before any fix\n\n<the table from step 5, with the actual result per row>\n')
```

---

### Task 2: `key` on the Kafka sink

**Files:**
- Modify: `internal/config/config.go:57-65`
- Modify: `internal/sinks/kafka.go`
- Modify: `internal/sinks/rows.go`
- Modify: `internal/validate/sinks.go:70-100`
- Modify: `internal/metering/harness_test.go` (`current.Key = true`)
- Test: `internal/sinks/kafka_test.go`, `internal/validate/sinks_test.go`

**Interfaces:**
- Produces: `config.KafkaSink.Key string`; `func tableRowKeys(table arrow.Table, column string) ([][]byte, error)` in package sinks.

- [ ] **Step 1: Write the failing sink tests**

Append to `internal/sinks/kafka_test.go`:

```go
// customerTable is a two-row batch: customer "a" then "b", with n 7 and 8.
func customerTable(t *testing.T, customers []*string) arrow.Table {
	t.Helper()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "customer", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "n", Type: arrow.PrimitiveTypes.Int64},
	}, nil)
	b := array.NewRecordBuilder(memory.NewGoAllocator(), schema)
	defer b.Release()
	for i, c := range customers {
		if c == nil {
			b.Field(0).AppendNull()
		} else {
			b.Field(0).(*array.StringBuilder).Append(*c)
		}
		b.Field(1).(*array.Int64Builder).Append(int64(7 + i))
	}
	rec := b.NewRecord()
	t.Cleanup(rec.Release)
	tbl := array.NewTableFromRecords(schema, []arrow.Record{rec})
	t.Cleanup(tbl.Release)
	return tbl
}

func strp(s string) *string { return &s }

// A retry has the same customer as its original, and the key is what
// sends both to one partition, where a window can deduplicate them.
func TestSinkKafka_KeyIsTheColumnsText(t *testing.T) {
	coverage.Covers(t, "sink.kafka")
	s, err := NewKafkaSink(config.KafkaSink{Brokers: []string{unreachableBroker}, Topic: "t", Key: "customer"})
	assert.NoError(t, err)
	defer s.Close()

	assert.NoError(t, s.WriteTable(context.Background(), customerTable(t, []*string{strp("a"), strp("b")})))
	assert.Equal(t, 2, len(s.pending))
	assert.Equal(t, "a", string(s.pending[0].key))
	assert.Equal(t, "b", string(s.pending[1].key))
	assert.That(t, strings.Contains(string(s.pending[0].value), `"customer":"a"`))
}

func TestSinkKafka_KeyOnANumberIsItsDecimalText(t *testing.T) {
	coverage.Covers(t, "sink.kafka")
	s, err := NewKafkaSink(config.KafkaSink{Brokers: []string{unreachableBroker}, Topic: "t", Key: "n"})
	assert.NoError(t, err)
	defer s.Close()
	assert.NoError(t, s.WriteTable(context.Background(), customerTable(t, []*string{strp("a")})))
	assert.Equal(t, "7", string(s.pending[0].key))
}

// A null key has no partition, and an unkeyed record defeats the key.
func TestSinkKafka_NullKeyFailsTheBatch(t *testing.T) {
	coverage.Covers(t, "sink.kafka")
	s, err := NewKafkaSink(config.KafkaSink{Brokers: []string{unreachableBroker}, Topic: "t", Key: "customer"})
	assert.NoError(t, err)
	defer s.Close()
	err = s.WriteTable(context.Background(), customerTable(t, []*string{strp("a"), nil}))
	assert.Error(t, err)
	assert.Equal(t, errs.CodeSinkEncodeFailed, errs.CodeOf(err))
	assert.Equal(t, 0, len(s.pending))
}

func TestSinkKafka_MissingKeyColumnFailsTheBatch(t *testing.T) {
	coverage.Covers(t, "sink.kafka")
	s, err := NewKafkaSink(config.KafkaSink{Brokers: []string{unreachableBroker}, Topic: "t", Key: "tenant"})
	assert.NoError(t, err)
	defer s.Close()
	err = s.WriteTable(context.Background(), customerTable(t, []*string{strp("a")}))
	assert.Equal(t, errs.CodeSinkEncodeFailed, errs.CodeOf(err))
}

func TestSinkKafka_NoKeyLeavesRecordsUnkeyed(t *testing.T) {
	coverage.Covers(t, "sink.kafka")
	s := newUnreachableKafkaSink(t)
	assert.NoError(t, s.WriteTable(context.Background(), customerTable(t, []*string{strp("a")})))
	assert.Nil(t, s.pending[0].key)
}
```

Add imports: `strings`, `github.com/apache/arrow-go/v18/arrow`, `.../arrow/array`, `.../arrow/memory`.

Append an integration test proving the key reaches the broker and pins the partition. Use the `sinkBrokerImage` and container setup from `conformance_kafka_test.go`, with `KAFKA_NUM_PARTITIONS` set to `"6"`:

```go
func TestIntegrationSinkKafka_KeyedRecordsShareAPartition(t *testing.T) {
	coverage.Covers(t, "sink.kafka")
	if testing.Short() {
		t.Skip("integration test: -short runs the unit pass only")
	}
	ctx := context.Background()
	broker, err := tckafka.Run(ctx, sinkBrokerImage,
		testcontainers.WithEnv(map[string]string{"KAFKA_NUM_PARTITIONS": "6"}))
	assert.NoError(t, err)
	t.Cleanup(func() { _ = broker.Terminate(context.Background()) })
	brokers, err := broker.Brokers(ctx)
	assert.NoError(t, err)

	topic := fmt.Sprintf("keyed-%d", time.Now().UnixNano())
	s, err := NewKafkaSink(config.KafkaSink{Brokers: brokers, Topic: topic, Key: "customer"})
	assert.NoError(t, err)
	defer s.Close()
	for i := 0; i < 20; i++ {
		assert.NoError(t, s.WriteTable(ctx, customerTable(t, []*string{strp("a"), strp("b")})))
	}
	assert.NoError(t, s.Flush(ctx))

	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	assert.NoError(t, err)
	defer cl.Close()
	partitionOf := map[string]map[int32]bool{}
	for n := 0; n < 40; {
		pctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		f := cl.PollFetches(pctx)
		cancel()
		assert.NoError(t, pctx.Err())
		f.EachRecord(func(r *kgo.Record) {
			n++
			if partitionOf[string(r.Key)] == nil {
				partitionOf[string(r.Key)] = map[int32]bool{}
			}
			partitionOf[string(r.Key)][r.Partition] = true
		})
	}
	assert.Equal(t, 1, len(partitionOf["a"]))
	assert.Equal(t, 1, len(partitionOf["b"]))
}
```

- [ ] **Step 2: Run them to see them fail**

Run: `go test -short ./internal/sinks/ -run 'TestSinkKafka_(Key|NullKey|MissingKey|NoKey)'`
Expected: compile failure, `unknown field Key in struct literal` and `s.pending[0].key undefined`.

- [ ] **Step 3: Add the config field**

In `internal/config/config.go`, `KafkaSink`:

```go
	// A column of the handler's output. Its value, as text, becomes each
	// record's key, so rows with the same value land on one partition: a
	// retried event lands beside its original, where a window can
	// deduplicate it. Absent, records carry no key. A row whose value is
	// null fails the batch.
	Key string `yaml:"key,omitempty"`
```

- [ ] **Step 4: Implement `tableRowKeys` and the keyed sink**

Append to `internal/sinks/rows.go`:

```go
// tableRowKeys returns column's value for every row of table, as text, in
// the order tableRowsAsJSON returns the rows. A null is an error: a keyed
// sink exists so one key always lands on one partition, and a null has no
// partition to land on.
func tableRowKeys(table arrow.Table, column string) ([][]byte, error) {
	if table == nil {
		return nil, nil
	}
	idx := table.Schema().FieldIndices(column)
	if len(idx) == 0 {
		return nil, errs.New(errs.CodeSinkEncodeFailed,
			"kafka sink: key column %q is not in the handler's output", column)
	}
	reader := array.NewTableReader(table, 0)
	defer reader.Release()
	var keys [][]byte
	for reader.Next() {
		rec := reader.Record()
		col := rec.Column(idx[0])
		for i := 0; i < int(rec.NumRows()); i++ {
			if col.IsNull(i) {
				return nil, errs.New(errs.CodeSinkEncodeFailed,
					"kafka sink: key column %q is null in row %d", column, len(keys))
			}
			keys = append(keys, []byte(col.ValueStr(i)))
		}
	}
	return keys, nil
}
```

In `internal/sinks/kafka.go`:

```go
// kafkaRow is one encoded row and its key, nil when the sink has none.
type kafkaRow struct {
	key, value []byte
}

type KafkaSink struct {
	client *kgo.Client
	topic  string
	// key is the column each record is keyed by; empty for none.
	key string

	mu sync.Mutex
	// pending holds the encoded rows that Flush has not yet had acknowledged.
	pending []kafkaRow
}
```

`NewKafkaSink` returns `&KafkaSink{client: client, topic: conf.Topic, key: conf.Key}`.

`WriteTable`:

```go
func (s *KafkaSink) WriteTable(ctx context.Context, batch arrow.Table) error {
	rows, err := tableRowsAsJSON(batch)
	if err != nil {
		return err
	}
	var keys [][]byte
	if s.key != "" {
		if keys, err = tableRowKeys(batch, s.key); err != nil {
			return err
		}
		if len(keys) != len(rows) {
			return errs.New(errs.CodeSinkInternal, "kafka sink: %d keys for %d rows", len(keys), len(rows))
		}
	}
	add := make([]kafkaRow, len(rows))
	for i := range rows {
		add[i].value = rows[i]
		if keys != nil {
			add[i].key = keys[i]
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.pending = append(s.pending, add...)
	return nil
}
```

In `Flush`, change the element type of `pending` and `keep` to `kafkaRow`, and produce `&kgo.Record{Topic: s.topic, Key: row.key, Value: row.value}`.

- [ ] **Step 5: Run the sink tests**

Run: `go test -short ./internal/sinks/ -run 'TestSinkKafka'`
Expected: PASS, including the existing Kafka sink tests.

- [ ] **Step 6: Write the failing validate test**

A key column the handler's SQL never names is a typo that fails every batch at runtime. validate cannot resolve `SELECT *`, so this is a warning. Append to `internal/validate/sinks_test.go`:

```go
const keyedKafkaPipeline = `
pipeline:
  batch_size: 1
  source:
    type: webhook
  handler:
    type: handlers.InferredMemBatch
    sql: SELECT id, customer FROM batch
  sink:
    type: kafka
    kafka:
      brokers: ["localhost:9092"]
      topic: t
      key: %s
`

func TestValidateSchema_KafkaKeyTheHandlerNeverNamesWarns(t *testing.T) {
	coverage.Covers(t, "validate.schema")
	rep, err := Validate(context.Background(), Request{Path: "k.yml", Config: fmt.Sprintf(keyedKafkaPipeline, "tenant")})
	assert.NoError(t, err)
	assert.That(t, rep.OK)
	diags := sinkDiagnostics(rep)
	assert.Equal(t, 1, len(diags))
	assert.Equal(t, SeverityWarning, diags[0].Severity)
	assert.That(t, strings.Contains(diags[0].Message, `key "tenant"`))
}

func TestValidateSchema_KafkaKeyTheHandlerNamesPasses(t *testing.T) {
	coverage.Covers(t, "validate.schema")
	rep, err := Validate(context.Background(), Request{Path: "k.yml", Config: fmt.Sprintf(keyedKafkaPipeline, "customer")})
	assert.NoError(t, err)
	assert.Equal(t, 0, len(sinkDiagnostics(rep)))
}
```

Run: `go test -short ./internal/validate/ -run KafkaKey`
Expected: FAIL. Before `make schema` the first case fails on the schema check's unknown `key`. After it, the warning is missing.

- [ ] **Step 7: Regenerate the schema and add the rule**

Run: `make schema`

Change `checkSink`'s signature to take the handler SQL, `checkSink(where string, s config.Sink, handlerSQL string, node *yaml.Node, ...)`, and pass `conf.Pipeline.Handler.SQL` from both call sites. Add the case:

```go
	case "kafka":
		if s.Kafka != nil && s.Kafka.Key != "" && !mentionsIdentifier(handlerSQL, s.Kafka.Key) {
			warn(fmt.Sprintf("%s: key %q is not named in the handler's SQL. Every row is keyed by that "+
				"column, and a batch without it fails at the sink", where, s.Kafka.Key),
				position(mappingValue(node, "kafka")))
		}
```

Add to the same file:

```go
// mentionsIdentifier reports whether sql names ident as a whole word. A
// SELECT * names nothing, which is why callers warn rather than fail.
func mentionsIdentifier(sql, ident string) bool {
	return regexp.MustCompile(`(?i)\b` + regexp.QuoteMeta(ident) + `\b`).MatchString(sql)
}
```

- [ ] **Step 8: Run validate and the schema golden**

Run: `go test -short ./internal/validate/ ./internal/schema/ ./internal/config/`
Expected: PASS.

- [ ] **Step 9: Turn the feature on in the harness and run scenario 6**

Set `var current = Features{Key: true}` in `internal/metering/harness_test.go`.

Run: `go test -run 'TestIntegrationMetering_RetriedDuplicates' -v ./internal/metering/ > $TMPDIR/s6.txt 2>&1; echo rc=$?`
Expected: PASS. Ingest still answers before the flush, but nothing is killed here, so every retry reaches Kafka and lands on its original's partition.

- [ ] **Step 10: Commit**

```bash
git add internal/config/config.go internal/sinks/ internal/validate/ internal/metering/harness_test.go
git commit -m "sink/kafka: key records by a column, so a retry lands on its original's partition

KafkaSink.Flush produced records with no key, so the partitioner spread a
retried event and its original across partitions, and a per-partition
window counted both. key: <column> keys each record by that column's text.
A null key fails the batch with user.sink.encode_failed. Evidence: the
keyed-partition integration test, and metering scenario 6 passes."
git push origin metering/durable-counts
```

---

### Task 3: `ack: after_flush` on the webhook source

**Files:**
- Modify: `internal/config/config.go` (`WebhookSource`, `Conf`)
- Modify: `internal/webhook/source.go`
- Modify: `internal/sources/init.go` (webhook builder)
- Modify: `internal/core/turbine.go`
- Create: `internal/validate/webhook.go`
- Modify: `internal/validate/validate.go:54-58`, `internal/cli/run/root.go` (run-time refusal)
- Modify: `internal/metering/harness_test.go` (`current.Ack = true`)
- Test: `internal/webhook/source_test.go`, `internal/webhook/turbine_test.go` (new), `internal/core/turbine_test.go`, `internal/validate/webhook_test.go` (new)

**Interfaces:**
- Produces in `core`:
  ```go
  // Settler is a source that answers its sender once the pipeline is
  // finished with what it delivered.
  type Settler interface {
  	// Settle resolves the oldest n messages the source delivered: err nil
  	// when their batch flushed and committed, non-nil when it did not.
  	Settle(n int, err error)
  }
  var ErrStoppedBeforeFlush = errors.New("the pipeline stopped before this message was flushed")
  ```
- Produces in `webhook`: `func WithAckAfterFlush() Option`; `func (s *Source) Settle(n int, err error)`.
- Produces in `config`: `WebhookSource.Ack string`, consts `WebhookAckOnReceive = "on_receive"`, `WebhookAckAfterFlush = "after_flush"`, `func (w *WebhookSource) AfterFlush() (bool, error)`, `func (c *Conf) CheckWebhookAck() error`.

How the pieces fit:

- A handler in after-flush mode appends a completion channel to a FIFO before it sends. It holds `sendMu` across the append and the send, so FIFO order is channel order.
- The turbine counts every message it takes from the stream (`t.unsettled`). After a batch's flush, state commit and source commit, it calls `Settle(t.unsettled, nil)`. When the loop returns an error, it settles what is outstanding with that error.
- `Settle` pops n channels and sends on each. The channels are buffered, so a sender that hung up never blocks the turbine.

- [ ] **Step 1: Write the failing webhook tests**

Append to `internal/webhook/source_test.go`:

```go
// With after_flush, a 200 means the pipeline flushed the body. It is not
// sent when the body reaches the queue.
func TestSourceWebhook_AfterFlushAnswersOnSettle(t *testing.T) {
	coverage.Covers(t, "source.webhook")
	s, err := NewSource(WithAckAfterFlush())
	assert.NoError(t, err)
	defer s.Close()
	srv := httptest.NewServer(s.Handler())
	defer srv.Close()

	answered := make(chan *http.Response, 1)
	go func() { answered <- post(t, srv.URL+"/events", []byte(`{"a":1}`), "", "") }()

	<-s.Stream()
	select {
	case <-answered:
		t.Fatal("answered before the pipeline settled the message")
	case <-time.After(250 * time.Millisecond):
	}
	s.Settle(1, nil)
	resp := <-answered
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, `{"status":"flushed"}`, readBody(t, resp))
}

// A flush that failed answers 503, so the sender retries.
func TestSourceWebhook_AfterFlushFailedSettleIs503(t *testing.T) {
	coverage.Covers(t, "source.webhook")
	s, err := NewSource(WithAckAfterFlush())
	assert.NoError(t, err)
	defer s.Close()
	srv := httptest.NewServer(s.Handler())
	defer srv.Close()

	answered := make(chan *http.Response, 1)
	go func() { answered <- post(t, srv.URL+"/events", []byte(`{"a":1}`), "", "") }()
	<-s.Stream()
	s.Settle(1, errors.New("broker down"))
	resp := <-answered
	assert.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
	assert.Equal(t, `{"detail":"Not flushed"}`, readBody(t, resp))
}

// Settle resolves in delivery order: the first body the pipeline took is
// the first answered.
func TestSourceWebhook_AfterFlushSettlesInDeliveryOrder(t *testing.T) {
	coverage.Covers(t, "source.webhook")
	s, err := NewSource(WithAckAfterFlush())
	assert.NoError(t, err)
	defer s.Close()
	srv := httptest.NewServer(s.Handler())
	defer srv.Close()

	codes := make(map[string]chan int)
	for _, body := range []string{"first", "second"} {
		ch := make(chan int, 1)
		codes[body] = ch
		go func(b string) {
			resp := post(t, srv.URL+"/events", []byte(b), "", "")
			resp.Body.Close()
			ch <- resp.StatusCode
		}(body)
		got := <-s.Stream()
		assert.Equal(t, body, string(got[0].Value))
	}
	s.Settle(1, nil)
	assert.Equal(t, http.StatusOK, <-codes["first"])
	select {
	case <-codes["second"]:
		t.Fatal("the second body was answered by the first settle")
	case <-time.After(250 * time.Millisecond):
	}
	s.Settle(1, errors.New("x"))
	assert.Equal(t, http.StatusServiceUnavailable, <-codes["second"])
}

// A sender that hangs up after its body is queued leaves its place in the
// FIFO. The next sender gets its own answer, not the departed one's.
func TestSourceWebhook_AfterFlushHangupDoesNotShiftAnswers(t *testing.T) {
	coverage.Covers(t, "source.webhook")
	s, err := NewSource(WithAckAfterFlush())
	assert.NoError(t, err)
	defer s.Close()
	srv := httptest.NewServer(s.Handler())
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL+"/events", strings.NewReader("gone"))
	go func() { _, _ = http.DefaultClient.Do(req) }()
	<-s.Stream()
	cancel()
	time.Sleep(100 * time.Millisecond)

	second := make(chan int, 1)
	go func() {
		resp := post(t, srv.URL+"/events", []byte("stays"), "", "")
		resp.Body.Close()
		second <- resp.StatusCode
	}()
	<-s.Stream()
	s.Settle(1, errors.New("the departed sender's batch failed"))
	s.Settle(1, nil)
	assert.Equal(t, http.StatusOK, <-second)
}

// Close answers every waiting sender 503: nothing will settle them.
func TestSourceWebhook_AfterFlushCloseReleasesWaiters(t *testing.T) {
	coverage.Covers(t, "source.webhook")
	s, err := NewSource(WithAckAfterFlush())
	assert.NoError(t, err)
	srv := httptest.NewServer(s.Handler())
	defer srv.Close()

	answered := make(chan int, 1)
	go func() {
		resp := post(t, srv.URL+"/events", []byte("x"), "", "")
		resp.Body.Close()
		answered <- resp.StatusCode
	}()
	<-s.Stream()
	assert.NoError(t, s.Close())
	assert.Equal(t, http.StatusServiceUnavailable, <-answered)
}

// on_receive stays the default and answers as the body is queued.
func TestSourceWebhook_OnReceiveIsTheDefault(t *testing.T) {
	coverage.Covers(t, "source.webhook")
	s, err := NewSource()
	assert.NoError(t, err)
	defer s.Close()
	srv := httptest.NewServer(s.Handler())
	defer srv.Close()
	resp := post(t, srv.URL+"/events", []byte("x"), "", "")
	assert.Equal(t, `{"status":"received"}`, readBody(t, resp))
	s.Settle(5, nil) // nothing waits; must not panic
}
```

Add imports `context` and `errors`.

- [ ] **Step 2: Run them to see them fail**

Run (unsandboxed, since the tests bind ports): `go test ./internal/webhook/ -run AfterFlush`
Expected: compile failure, `undefined: WithAckAfterFlush`.

- [ ] **Step 3: Implement after-flush in the webhook source**

In `internal/webhook/source.go`, add to `Source`:

```go
	// afterFlush answers a delivery once the pipeline has flushed it rather
	// than once it is queued. pending holds one channel per queued delivery,
	// in channel order; sendMu makes the append and the send one step, so
	// that order holds under concurrent handlers.
	afterFlush bool
	sendMu     sync.Mutex
	pendingMu  sync.Mutex
	pending    []chan error
```

```go
// WithAckAfterFlush answers a delivery after the batch holding it has
// flushed: a 200 then means the body is in the sink. Without it the source
// answers when the body is queued, which a crash before the flush turns into
// a lost event the sender was told it had delivered.
func WithAckAfterFlush() Option {
	return func(s *Source) { s.afterFlush = true }
}

// Settle resolves the oldest n deliveries. The channels are buffered, so a
// sender that has hung up never blocks the pipeline.
func (s *Source) Settle(n int, err error) {
	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()
	if n > len(s.pending) {
		n = len(s.pending)
	}
	for _, ch := range s.pending[:n] {
		ch <- err
	}
	s.pending = s.pending[n:]
}

// unqueue removes ch, which the caller appended last and never sent. A
// failed send is always the newest entry, because sendMu is held from the
// append to the send.
func (s *Source) unqueue(ch chan error) {
	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()
	if n := len(s.pending); n > 0 && s.pending[n-1] == ch {
		s.pending = s.pending[:n-1]
	}
}
```

Replace the send at the end of `receiveEvents` (from `s.mu.RLock()` to the end):

```go
	msgs := []core.Message{{Value: body, EventAtNanos: s.stamp(body)}}

	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		writeJSON(w, http.StatusServiceUnavailable, `{"detail":"Source is closed"}`)
		return
	}
	if !s.afterFlush {
		defer s.mu.RUnlock()
		select {
		case s.streamChan <- msgs:
			writeJSON(w, http.StatusOK, `{"status":"received"}`)
		case <-s.done:
			writeJSON(w, http.StatusServiceUnavailable, `{"detail":"Source is closed"}`)
		case <-r.Context().Done():
		}
		return
	}

	settled := make(chan error, 1)
	s.sendMu.Lock()
	s.pendingMu.Lock()
	s.pending = append(s.pending, settled)
	s.pendingMu.Unlock()
	var sent bool
	select {
	case s.streamChan <- msgs:
		sent = true
	case <-s.done:
	case <-r.Context().Done():
	}
	if !sent {
		s.unqueue(settled)
	}
	s.sendMu.Unlock()
	// Released before the wait: Close takes the write lock, and it must
	// not wait on a flush that may never come.
	s.mu.RUnlock()
	if !sent {
		if r.Context().Err() == nil {
			writeJSON(w, http.StatusServiceUnavailable, `{"detail":"Source is closed"}`)
		}
		return
	}

	select {
	case err := <-settled:
		s.answerSettled(w, err)
	case <-s.done:
		// Settled and closing can both be ready; a body that was flushed
		// says so rather than asking for a retry.
		select {
		case err := <-settled:
			s.answerSettled(w, err)
		default:
			writeJSON(w, http.StatusServiceUnavailable, `{"detail":"Source is closed"}`)
		}
	case <-r.Context().Done():
	}
}

func (s *Source) answerSettled(w http.ResponseWriter, err error) {
	if err != nil {
		writeJSON(w, http.StatusServiceUnavailable, `{"detail":"Not flushed"}`)
		return
	}
	writeJSON(w, http.StatusOK, `{"status":"flushed"}`)
}
```

In `Close`, after `close(s.done)`, nothing else is needed: every waiter selects on `s.done`.

- [ ] **Step 4: Run the webhook tests**

Run (unsandboxed): `go test ./internal/webhook/`
Expected: PASS, including every existing test.

- [ ] **Step 5: Write the failing turbine tests**

Append to `internal/core/turbine_test.go`:

```go
// settlingSource records every Settle call.
type settlingSource struct {
	fakeSource
	mu      sync.Mutex
	settles []settleCall
}

type settleCall struct {
	n   int
	err error
}

func (s *settlingSource) Settle(n int, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.settles = append(s.settles, settleCall{n, err})
}

func (s *settlingSource) calls() []settleCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]settleCall(nil), s.settles...)
}

// A batch is settled after its flush and its commits, once, with the number
// of messages it took from the stream.
func TestTurbine_SettlesAfterTheFlush(t *testing.T) {
	src := &settlingSource{fakeSource: fakeSource{batches: [][]Message{
		{{Value: []byte("a")}}, {{Value: []byte("b")}}, {{Value: []byte("c")}},
	}}}
	events := []string{}
	sink := &orderingSink{events: &events}
	tb := newTestTurbine(src, &fakeHandler{}, sink, 3)
	_, err := tb.ConsumeLoop(context.Background(), 3)
	assert.NoError(t, err)
	assert.Equal(t, []settleCall{{3, nil}}, src.calls())
	assert.Equal(t, "flush", events[len(events)-1])
}

// A flush that fails settles the batch with the error, so the sender
// hears 503 and retries.
func TestTurbine_SettlesAFailedFlushWithItsError(t *testing.T) {
	src := &settlingSource{fakeSource: fakeSource{batches: [][]Message{{{Value: []byte("a")}}}}}
	events := []string{}
	sink := &orderingSink{events: &events, fail: true}
	tb := newTestTurbine(src, &fakeHandler{}, sink, 1)
	_, err := tb.ConsumeLoop(context.Background(), 1)
	assert.Error(t, err)
	calls := src.calls()
	assert.Equal(t, 1, len(calls))
	assert.Equal(t, 1, calls[0].n)
	assert.Error(t, calls[0].err)
}

// Messages the loop never reached are never settled. Close answers them.
func TestTurbine_DoesNotSettleWhatItNeverTook(t *testing.T) {
	src := &settlingSource{fakeSource: fakeSource{batches: [][]Message{
		{{Value: []byte("a")}, {Value: []byte("b")}, {Value: []byte("c")}},
	}}}
	events := []string{}
	tb := newTestTurbine(src, &fakeHandler{}, &orderingSink{events: &events}, 10)
	_, err := tb.ConsumeLoop(context.Background(), 2)
	assert.NoError(t, err)
	assert.Equal(t, []settleCall{{2, nil}}, src.calls())
}
```

`orderingSink` (`internal/core/turbine_test.go:581`) appends `"flush"` on success and `"flush-failed"` on failure.

Run: `go test -short ./internal/core/ -run 'TestTurbine_(Settle|DoesNotSettle)'`
Expected: FAIL, no Settle calls recorded.

- [ ] **Step 6: Settle in the turbine**

In `internal/core/turbine.go`, add the `Settler` interface and `ErrStoppedBeforeFlush` from this task's Interfaces block beside `MarkCommitter`. Import `errors`. Add the field:

```go
	// unsettled is how many messages the loop took from the stream since the
	// last settle; a Settler source hears it after the batch commits.
	unsettled int
```

```go
// settle tells a Settler source the loop is finished with what it took:
// flushed and committed when err is nil.
func (t *Turbine) settle(err error) {
	if t.unsettled == 0 {
		return
	}
	if s, ok := t.source.(Settler); ok {
		s.Settle(t.unsettled, err)
	}
	t.unsettled = 0
}
```

Increment `t.unsettled++` beside every `totalConsumed++` in `ConsumeLoop`. There are four: the unplaceable branch, the refused-late branch, the write-error branch and the written branch.

In `processBatch`, call `t.settle(nil)` after the `commitSource` block and before `batch.Release()`. On a failure, the error return is enough: the defer below settles it.

In the idle-tick branch of `ConsumeLoop`, after `commitState` succeeds, call `t.settle(nil)`. It settles messages an error policy dropped with no batch after them, so their senders do not wait for the next delivery.

In `ConsumeLoop`, add a new defer directly after the one that calls `t.source.Close()`. Go runs defers last-in first-out, so this one runs before the source closes, and a waiter gets the loop's error rather than the source's closing 503. Leave the `marks.Reset` defer as it is.

```go
	// Before the source closes: a sender still waiting on its flush hears
	// why it will not come.
	defer func() {
		if err != nil {
			t.settle(err)
			return
		}
		t.settle(ErrStoppedBeforeFlush)
	}()
```

- [ ] **Step 7: Run the turbine tests**

Run: `go test -short -race ./internal/core/`
Expected: PASS.

- [ ] **Step 8: Write scenario 9, in process**

`internal/webhook/turbine_test.go`:

```go
package webhook

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/turbolytics/sql-flow/internal/core"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

type countingHandler struct{ n int }

func (h *countingHandler) Init(context.Context) error { h.n = 0; return nil }
func (h *countingHandler) Write([]byte) error         { h.n++; return nil }
func (h *countingHandler) Invoke(context.Context) (arrow.Table, error) {
	return nil, nil
}
func (h *countingHandler) RowsRead() int64 { return int64(h.n) }

type failingSink struct{}

func (failingSink) WriteTable(context.Context, arrow.Table) error { return nil }
func (failingSink) Flush(context.Context) error               { return errors.New("broker down") }

type okSink struct{}

func (okSink) WriteTable(context.Context, arrow.Table) error { return nil }
func (okSink) Flush(context.Context) error               { return nil }

// Scenario 9: a real webhook source and turbine. A flush that fails answers
// 503; one that succeeds answers 200.
func TestSourceWebhook_AfterFlushThroughTheTurbine(t *testing.T) {
	coverage.Covers(t, "source.webhook")
	for _, tc := range []struct {
		name string
		sink core.Sink
		want int
	}{{"flush fails", failingSink{}, http.StatusServiceUnavailable}, {"flush succeeds", okSink{}, http.StatusOK}} {
		t.Run(tc.name, func(t *testing.T) {
			// ConsumeLoop calls Start, which binds; port 0 keeps that bind
			// harmless beside the test server.
			s, err := NewSource(WithAckAfterFlush(), WithAddr("127.0.0.1:0"))
			assert.NoError(t, err)
			srv := httptest.NewServer(s.Handler())
			defer srv.Close()
			tb := core.NewTurbine(s, &countingHandler{}, tc.sink, 1, time.Second, &sync.Mutex{}, core.PipelineErrorPolicies{})
			go func() { _, _ = tb.ConsumeLoop(context.Background(), 0) }()
			resp := post(t, srv.URL+"/events", []byte(`{"a":1}`), "", "")
			resp.Body.Close()
			assert.Equal(t, tc.want, resp.StatusCode)
			_ = s.Close()
		})
	}
}
```

Run (unsandboxed): `go test ./internal/webhook/ -run ThroughTheTurbine`
Expected: PASS.

- [ ] **Step 9: Config, source registry, validate and the run-time refusal**

`internal/config/config.go`, in `WebhookSource`:

```go
	// When a delivery is answered. on_receive, the default, answers once the
	// body is queued. after_flush answers once the batch holding it has
	// flushed to the sink, so a 200 means the body is in the sink; a failed
	// flush answers 503. after_flush is refused on a pipeline that windows:
	// a window holds a row for minutes, and a request cannot wait that long.
	Ack string `yaml:"ack,omitempty" jsonschema:"enum=on_receive,enum=after_flush"`
```

```go
const (
	WebhookAckOnReceive  = "on_receive"
	WebhookAckAfterFlush = "after_flush"
)

// AfterFlush reports whether deliveries are answered after the flush. A nil
// receiver is the absent block.
func (w *WebhookSource) AfterFlush() (bool, error) {
	if w == nil || w.Ack == "" || w.Ack == WebhookAckOnReceive {
		return false, nil
	}
	if w.Ack == WebhookAckAfterFlush {
		return true, nil
	}
	return false, errs.New(errs.CodeSourceInvalid, "webhook source: ack must be on_receive or after_flush, got %q", w.Ack)
}

// CheckWebhookAck refuses after_flush on a pipeline that windows. validate
// and run both call it.
func (c *Conf) CheckWebhookAck() error {
	if c.Pipeline.Source.Type != "webhook" {
		return nil
	}
	after, err := c.Pipeline.Source.Webhook.AfterFlush()
	if err != nil || !after || !c.HasWindow() {
		return err
	}
	return errs.New(errs.CodeConfigInvalid,
		"webhook source: ack after_flush on a pipeline that windows. A window holds a row until its "+
			"bucket closes, which is minutes, and the request waits for the flush. Ingest into Kafka "+
			"with after_flush and window in a second pipeline that reads the topic")
}
```

`internal/sources/init.go`, webhook builder: resolve `after, err := c.Webhook.AfterFlush()`, and append `webhook.WithAckAfterFlush()` to the options when it is true. Log `ack` in the "initializing webhook source" line.

`internal/validate/webhook.go`:

```go
package validate

import (
	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/errs"
	"gopkg.in/yaml.v3"
)

// checkWebhookAck refuses ack: after_flush on a pipeline that windows.
func checkWebhookAck(rendered []byte, rep *Report) {
	var root yaml.Node
	if yaml.Unmarshal(rendered, &root) != nil {
		return
	}
	var conf config.Conf
	if root.Decode(&conf) != nil {
		return
	}
	if err := conf.CheckWebhookAck(); err != nil {
		rep.Add(diagnostic(errs.CodeConfigInvalid, SeverityError, err.Error(), position(sourceNode(&root))))
		rep.SetCheck("source.webhook.ack", StatusFail, "")
		return
	}
	rep.SetCheck("source.webhook.ack", StatusPass, "")
}
```

Call `checkWebhookAck(rendered, &rep)` after `checkMqtt` in `validate.go`.

In `internal/cli/run/root.go`, find where `conf.Pipeline.CheckMQTT()` is called (`grep -n CheckMQTT internal/cli/run/root.go`) and call `conf.CheckWebhookAck()` beside it, returning its error.

`internal/validate/webhook_test.go`:

```go
package validate

import (
	"context"
	"strings"
	"testing"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

func TestValidateSchema_AfterFlushOnAWindowingPipelineFails(t *testing.T) {
	coverage.Covers(t, "validate.schema")
	cfg := strings.Replace(windowedConfig, "%s", "bucket TIMESTAMPTZ", 1)
	cfg = strings.Replace(cfg, "%s", "", 1)
	cfg = strings.Replace(cfg, `    type: kafka
    kafka:
      brokers: ["localhost:9092"]
      group_id: g
      auto_offset_reset: earliest
      topics: ["t"]
      event_time:
        path: ts
        format: rfc3339`, `    type: webhook
    webhook:
      ack: after_flush
      event_time:
        path: ts
        format: rfc3339`, 1)
	rep, err := Validate(context.Background(), Request{Path: "w.yml", Config: cfg})
	assert.NoError(t, err)
	assert.That(t, !rep.OK)
	assert.Equal(t, StatusFail, checkStatus(t, rep, "source.webhook.ack"))
}

func TestValidateSchema_AfterFlushWithoutAWindowPasses(t *testing.T) {
	coverage.Covers(t, "validate.schema")
	rep, err := Validate(context.Background(), Request{Path: "i.yml", Config: `
pipeline:
  batch_size: 50
  source:
    type: webhook
    webhook:
      ack: after_flush
  handler:
    type: handlers.InferredMemBatch
    sql: SELECT * FROM batch
  sink:
    type: noop
`})
	assert.NoError(t, err)
	assert.That(t, rep.OK)
	assert.Equal(t, StatusPass, checkStatus(t, rep, "source.webhook.ack"))
}
```

The first test's `strings.Replace` must find the Kafka block of `windowedConfig` verbatim. Read `internal/validate/window_test.go:1-40` and match it exactly.

Run: `make schema && go test -short ./internal/validate/ ./internal/config/ ./internal/schema/ ./internal/sources/`
Expected: PASS.

- [ ] **Step 10: Turn the feature on and run scenarios 6 and 8**

Set `var current = Features{Key: true, Ack: true}`.

Run: `go test -run 'TestIntegrationMetering_(RetriedDuplicates|IngestKilledBeforeFlush)' -v ./internal/metering/ > $TMPDIR/s68.txt 2>&1; echo rc=$?`
Expected: both PASS. Record the p50 and p99 of a 200's latency from scenario 6. Add a timing around each `postEvents` there and log both.

- [ ] **Step 11: Commit**

```bash
git add internal/ docs/ 
git commit -m "webhook: ack after_flush, so a 200 means the body is in the sink

receiveEvents answered 200 once the body was on a channel of one, before the
turbine had flushed it, at any batch size. A crash in between lost an event
the sender had been told was delivered. ack: after_flush answers after the
batch holding the body flushed and committed, and 503 when the flush
failed. validate and run refuse it on a pipeline that windows. Evidence:
metering scenarios 6 and 8, and the in-process scenario 9."
git push origin metering/durable-counts
```

---

### Task 4: The seeker forgets a revoked partition

**Files:**
- Modify: `internal/kafka/seek.go`
- Modify: `internal/kafka/source.go` (`NewSource`)
- Modify: `internal/core/marks.go`
- Test: `internal/kafka/seek_test.go`, `internal/core/marks_test.go`

**Interfaces:**
- Produces: `func (s *OffsetSeeker) Forget(parts map[string][]int32)`, and `func (m *Marks) Forget(topic string, partitions []int32)`. Task 7 calls `Marks.Forget`.

- [ ] **Step 1: Write the failing tests**

Append to `internal/kafka/seek_test.go`:

```go
// A partition revoked and later reassigned must not rewind to the position
// this process loaded at startup: another member has moved it since.
func TestOffsetSeeker_ForgetsARevokedPartition(t *testing.T) {
	coverage.Covers(t, "source.kafka")
	marks := core.NewMarks()
	marks.Advance("t", 0, core.Mark{Offset: 10})
	marks.Advance("t", 1, core.Mark{Offset: 20})
	s := NewOffsetSeeker()
	s.SetMarks(marks)

	s.Forget(map[string][]int32{"t": {1}})

	fetched := map[string]map[int32]kgo.Offset{"t": {0: kgo.NewOffset().At(99), 1: kgo.NewOffset().At(500)}}
	adjusted, err := s.Adjust(context.Background(), fetched)
	assert.NoError(t, err)
	assert.Equal(t, int64(11), adjusted["t"][0].EpochOffset().Offset)
	assert.Equal(t, int64(500), adjusted["t"][1].EpochOffset().Offset)
	// The caller's marks are untouched: SetMarks copied them.
	_, still := marks.Get("t", 1)
	assert.That(t, still)
}
```

Append to `internal/core/marks_test.go`:

```go
func TestMarks_Forget(t *testing.T) {
	m := NewMarks()
	m.Advance("t", 0, Mark{Offset: 1})
	m.Advance("t", 1, Mark{Offset: 2})
	m.Forget("t", []int32{1, 7})
	_, ok := m.Get("t", 1)
	assert.That(t, !ok)
	_, ok = m.Get("t", 0)
	assert.That(t, ok)
}
```

Run: `go test -short ./internal/kafka/ ./internal/core/ -run 'Forget'`
Expected: compile failure, `Forget undefined`.

- [ ] **Step 2: Implement**

`internal/core/marks.go`:

```go
// Forget drops partitions this process no longer holds, so nothing commits a
// position for a partition another member now owns.
func (m *Marks) Forget(topic string, partitions []int32) {
	parts, ok := m.m[topic]
	if !ok {
		return
	}
	for _, p := range partitions {
		delete(parts, p)
	}
}
```

`internal/kafka/seek.go`. `SetMarks` copies, so `Forget` never mutates the caller's marks:

```go
func (s *OffsetSeeker) SetMarks(marks *core.Marks) {
	cp := core.NewMarks()
	if marks != nil {
		cp.Reset(marks)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.marks = cp
}

// Forget drops the stored positions of partitions this consumer no longer
// holds. Its state for them is no longer the latest: another member has
// consumed and committed since, and reapplying the startup position on a
// later reassignment would rewind the partition.
func (s *OffsetSeeker) Forget(parts map[string][]int32) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.marks == nil {
		return
	}
	for topic, ps := range parts {
		s.marks.Forget(topic, ps)
	}
}
```

In `NewSource`, after the options loop:

```go
	// The seeker's positions describe partitions as this process last held
	// them; once one leaves, the group's committed offset is the truth.
	if s.seeker != nil && s.partitions != nil {
		s.partitions.Subscribe(nil, s.seeker.Forget, s.seeker.Forget)
	}
```

- [ ] **Step 3: Run the tests**

Run: `go test -short ./internal/kafka/ ./internal/core/`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add internal/kafka/ internal/core/marks.go internal/core/marks_test.go
git commit -m "kafka: forget a revoked partition's startup position

The offset seeker applied the marks it loaded at startup on every join, so
a partition revoked and later reassigned rewound to where this process had
been before another member consumed it. Released and lost partitions now
leave the seeker. Evidence: TestOffsetSeeker_ForgetsARevokedPartition."
git push origin metering/durable-counts
```

---

### Task 5: Offset records and the low-watermark commit

**Files:**
- Create: `internal/core/windowoffsets.go`
- Create: `internal/core/windowoffsets_test.go`
- Modify: `internal/core/watermarks.go` (`WindowSignal`)
- Modify: `internal/managers/watermark.go` (`Pass`)
- Modify: `internal/core/turbine.go`
- Modify: `internal/cli/run/root.go`, `internal/cli/run/managers.go`
- Test: `internal/managers/watermark_test.go`, `internal/core/turbine_test.go`

**Interfaces:**
- Consumes: `BucketStart`, `BucketEnd` (`internal/core/bucket.go`); `WindowSpec`; `Marks`.
- Produces:
  ```go
  const WindowOffsetsTable = "sqlflow_window_offsets"
  type OffsetRecord struct { Window string; Bucket time.Time; Topic string; Partition int32; Mark Mark }
  func NewWindowOffsets(specs []WindowSpec) *WindowOffsets
  func (o *WindowOffsets) Note(m Message)             // consume loop, per placed record
  func (o *WindowOffsets) Merge()                     // once per batch, before commitState
  func (o *WindowOffsets) Expire(window string, closed time.Time)
  func (o *WindowOffsets) Drop(topic string, partitions []int32)
  func (o *WindowOffsets) Low(processed *Marks) *Marks
  func (o *WindowOffsets) Closed() map[string]time.Time  // what Expire was last given, per window
  func (o *WindowOffsets) Load(recs []OffsetRecord)
  func (o *WindowOffsets) Pending() WindowOffsetsDelta
  func (o *WindowOffsets) Saved()
  type WindowOffsetsDelta struct { Added []OffsetRecord; Expired map[string]time.Time; Dropped map[string][]int32 }
  type WindowOffsetStore struct{ conn adbc.Connection; specs []WindowSpec }
  func NewWindowOffsetStore(conn adbc.Connection, specs []WindowSpec) *WindowOffsetStore
  func (s *WindowOffsetStore) Init(ctx) error
  func (s *WindowOffsetStore) Load(ctx) ([]OffsetRecord, error)
  func (s *WindowOffsetStore) Save(ctx, d WindowOffsetsDelta) error
  func WithWindowOffsets(o *WindowOffsets, s *WindowOffsetStore) TurbineOption  // s nil without a state path
  func (s *WindowSignal) SetClosed(t time.Time)
  func (s *WindowSignal) Closed() (time.Time, bool)
  ```

- [ ] **Step 1: Write the tracker's failing tests**

`internal/core/windowoffsets_test.go`:

```go
package core

import (
	"testing"
	"time"

	"github.com/zeebo/assert"
)

var minuteSpec = WindowSpec{Name: "w", Size: time.Minute, Lateness: time.Minute}

func msgAt(p int32, off int64, at time.Time) Message {
	return Message{Topic: "t", Partition: p, Offset: off, LeaderEpoch: 3, EventAtNanos: at.UnixNano()}
}

var woT0 = time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC)

// The commit for a partition is the one before the lowest offset still
// feeding a retained bucket, so a worker that starts without the rows
// replays them.
func TestWindowOffsets_LowIsBeforeTheOldestRetainedRecord(t *testing.T) {
	o := NewWindowOffsets([]WindowSpec{minuteSpec})
	o.Note(msgAt(0, 100, woT0.Add(10*time.Second)))
	o.Note(msgAt(0, 101, woT0.Add(70*time.Second)))
	o.Note(msgAt(0, 102, woT0.Add(20*time.Second))) // out of order, same first bucket
	o.Merge()
	processed := NewMarks()
	processed.Advance("t", 0, Mark{Offset: 102})
	low := o.Low(processed)
	m, _ := low.Get("t", 0)
	assert.Equal(t, int64(99), m.Offset)
	assert.Equal(t, int32(3), m.LeaderEpoch)
}

// A bucket past its lateness against the manager's closed watermark drops
// its record, and the commit moves to the next retained bucket.
func TestWindowOffsets_ExpireMovesLowForward(t *testing.T) {
	o := NewWindowOffsets([]WindowSpec{minuteSpec})
	o.Note(msgAt(0, 100, woT0.Add(10*time.Second)))
	o.Note(msgAt(0, 150, woT0.Add(70*time.Second)))
	o.Merge()
	// Bucket 12:00 ends 12:01; with 60 s of lateness it expires at closed 12:02.
	o.Expire("w", woT0.Add(2*time.Minute))
	processed := NewMarks()
	processed.Advance("t", 0, Mark{Offset: 150})
	m, _ := o.Low(processed).Get("t", 0)
	assert.Equal(t, int64(149), m.Offset)
}

// One second short of expiry keeps the record: the predicate is the
// manager's, bucket_end + lateness <= closed.
func TestWindowOffsets_ExpireIsTheManagersPredicate(t *testing.T) {
	o := NewWindowOffsets([]WindowSpec{minuteSpec})
	o.Note(msgAt(0, 100, woT0.Add(10*time.Second)))
	o.Merge()
	o.Expire("w", woT0.Add(2*time.Minute-time.Second))
	processed := NewMarks()
	processed.Advance("t", 0, Mark{Offset: 100})
	m, _ := o.Low(processed).Get("t", 0)
	assert.Equal(t, int64(99), m.Offset)
}

// A partition with nothing retained commits what it processed.
func TestWindowOffsets_NothingRetainedCommitsProcessed(t *testing.T) {
	o := NewWindowOffsets([]WindowSpec{minuteSpec})
	processed := NewMarks()
	processed.Advance("t", 4, Mark{Offset: 7, LeaderEpoch: 2})
	m, _ := o.Low(processed).Get("t", 4)
	assert.Equal(t, Mark{Offset: 7, LeaderEpoch: 2}, m)
}

// Attribution is the record's own partition and its own bucket.
func TestWindowOffsets_PartitionsAreIndependent(t *testing.T) {
	o := NewWindowOffsets([]WindowSpec{minuteSpec})
	o.Note(msgAt(0, 5, woT0))
	o.Note(msgAt(1, 900, woT0))
	o.Merge()
	processed := NewMarks()
	processed.Advance("t", 0, Mark{Offset: 10})
	processed.Advance("t", 1, Mark{Offset: 950})
	low := o.Low(processed)
	m0, _ := low.Get("t", 0)
	m1, _ := low.Get("t", 1)
	assert.Equal(t, int64(4), m0.Offset)
	assert.Equal(t, int64(899), m1.Offset)
}

// A batch that rolls back leaves its records pending: the replay notes the
// same records, and the next successful commit writes them once.
func TestWindowOffsets_RollbackKeepsPendingRecords(t *testing.T) {
	o := NewWindowOffsets([]WindowSpec{minuteSpec})
	o.Note(msgAt(0, 5, woT0))
	o.Merge()
	assert.Equal(t, 1, len(o.Pending().Added))
	// Rolled back: Saved is not called. The replay notes the same record.
	o.Note(msgAt(0, 5, woT0))
	o.Merge()
	assert.Equal(t, 1, len(o.Pending().Added))
	o.Saved()
	assert.Equal(t, 0, len(o.Pending().Added))
}

// A record with no event time has no bucket and no record.
func TestWindowOffsets_IgnoresRecordsWithoutEventTimeOrPosition(t *testing.T) {
	o := NewWindowOffsets([]WindowSpec{minuteSpec})
	o.Note(Message{Topic: "t", Partition: 0, Offset: 1})
	o.Note(Message{EventAtNanos: woT0.UnixNano()})
	o.Merge()
	assert.Equal(t, 0, len(o.Pending().Added))
}

// Drop forgets a partition's records and says so for the store.
func TestWindowOffsets_Drop(t *testing.T) {
	o := NewWindowOffsets([]WindowSpec{minuteSpec})
	o.Note(msgAt(2, 5, woT0))
	o.Merge()
	o.Saved()
	o.Drop("t", []int32{2})
	processed := NewMarks()
	processed.Advance("t", 2, Mark{Offset: 9})
	m, _ := o.Low(processed).Get("t", 2)
	assert.Equal(t, int64(9), m.Offset)
	assert.Equal(t, []int32{2}, o.Pending().Dropped["t"])
}
```

Run: `go test -short ./internal/core/ -run TestWindowOffsets`
Expected: compile failure, `undefined: NewWindowOffsets`.

- [ ] **Step 2: Implement the tracker**

`internal/core/windowoffsets.go`:

```go
package core

import (
	"time"
)

// WindowOffsetsTable holds, per window, bucket and partition, the lowest
// offset that wrote a row into the bucket. It is what lets the committed
// offset stay at or below every row a retained bucket holds, so Kafka is the
// durable copy of window state: a worker that starts without the rows, on a
// new disk or after a rebalance, replays from there and rebuilds each bucket
// whole.
const WindowOffsetsTable = "sqlflow_window_offsets"

// OffsetRecord is one row of that table.
type OffsetRecord struct {
	Window    string
	Bucket    time.Time
	Topic     string
	Partition int32
	// Mark is the lowest offset seen for the bucket and the leader epoch it
	// was read under.
	Mark Mark
}

type offsetKey struct {
	window    string
	bucket    int64
	topic     string
	partition int32
}

// WindowOffsetsDelta is what the store must write to match the tracker.
type WindowOffsetsDelta struct {
	Added   []OffsetRecord
	Expired map[string]time.Time
	Dropped map[string][]int32
}

// WindowOffsets tracks the lowest offset feeding each retained bucket.
//
// Note runs per record on the consume loop, so it touches only a slice of
// this batch's first sightings, scanned linearly: a batch spans few
// partitions and buckets. Merge folds that slice into the map once per
// batch. Everything else runs on the consume loop too, between batches, so
// nothing here takes a lock.
type WindowOffsets struct {
	specs  []WindowSpec
	recs   map[offsetKey]Mark
	batch  []OffsetRecord
	delta  WindowOffsetsDelta
	closed map[string]time.Time
}

func NewWindowOffsets(specs []WindowSpec) *WindowOffsets {
	return &WindowOffsets{
		specs:  specs,
		recs:   map[offsetKey]Mark{},
		closed: map[string]time.Time{},
		delta:  WindowOffsetsDelta{Expired: map[string]time.Time{}, Dropped: map[string][]int32{}},
	}
}

// Note records a placed record against its bucket in every window. Offsets
// rise within a partition, so the first sighting of a key in a batch is its
// lowest in that batch, and a key already merged holds a lower one still.
func (o *WindowOffsets) Note(m Message) {
	if !m.HasMetadata() || m.EventAtNanos <= 0 {
		return
	}
	at := time.Unix(0, m.EventAtNanos).UTC()
	for _, spec := range o.specs {
		bucket := BucketStart(at, spec.Size)
		seen := false
		for i := range o.batch {
			r := &o.batch[i]
			if r.Partition == m.Partition && r.Bucket.Equal(bucket) && r.Window == spec.Name && r.Topic == m.Topic {
				seen = true
				break
			}
		}
		if !seen {
			o.batch = append(o.batch, OffsetRecord{
				Window: spec.Name, Bucket: bucket, Topic: m.Topic, Partition: m.Partition,
				Mark: Mark{Offset: m.Offset, LeaderEpoch: m.LeaderEpoch},
			})
		}
	}
}

// Merge folds the batch's sightings in. A key already held keeps its lower
// offset. A new key is pending for the store until Saved.
func (o *WindowOffsets) Merge() {
	for _, r := range o.batch {
		k := offsetKey{r.Window, r.Bucket.UnixNano(), r.Topic, r.Partition}
		if cur, ok := o.recs[k]; ok && cur.Offset <= r.Mark.Offset {
			continue
		}
		o.recs[k] = r.Mark
		o.delta.Added = append(o.delta.Added, r)
	}
	o.batch = o.batch[:0]
}

// Expire drops the records of window's buckets past their lateness against
// the manager's closed watermark: bucket_end + lateness <= closed, the
// predicate the manager deletes the rows by.
func (o *WindowOffsets) Expire(window string, closed time.Time) {
	var spec WindowSpec
	for _, s := range o.specs {
		if s.Name == window {
			spec = s
		}
	}
	if spec.Name == "" || !closed.After(o.closed[window]) {
		return
	}
	o.closed[window] = closed
	o.delta.Expired[window] = closed
	limit := closed.Add(-spec.Lateness).UnixNano()
	for k := range o.recs {
		if k.window == window && k.bucket+int64(spec.Size) <= limit {
			delete(o.recs, k)
		}
	}
	kept := o.delta.Added[:0]
	for _, r := range o.delta.Added {
		if r.Window == window && r.Bucket.Add(spec.Size).UnixNano() <= limit {
			continue
		}
		kept = append(kept, r)
	}
	o.delta.Added = kept
}

// Drop forgets every record of the partitions.
func (o *WindowOffsets) Drop(topic string, partitions []int32) {
	drop := map[int32]bool{}
	for _, p := range partitions {
		drop[p] = true
	}
	for k := range o.recs {
		if k.topic == topic && drop[k.partition] {
			delete(o.recs, k)
		}
	}
	kept := o.delta.Added[:0]
	for _, r := range o.delta.Added {
		if r.Topic == topic && drop[r.Partition] {
			continue
		}
		kept = append(kept, r)
	}
	o.delta.Added = kept
	o.delta.Dropped[topic] = append(o.delta.Dropped[topic], partitions...)
}

// Low is the position to commit for every partition processed holds: the
// one before the lowest offset still feeding a retained bucket, or processed's
// own where nothing retained holds rows from the partition. Never above
// processed.
func (o *WindowOffsets) Low(processed *Marks) *Marks {
	low := NewMarks()
	processed.Each(func(topic string, partition int32, mark Mark) {
		best := mark
		for k, m := range o.recs {
			if k.topic == topic && k.partition == partition && m.Offset-1 < best.Offset {
				best = Mark{Offset: m.Offset - 1, LeaderEpoch: m.LeaderEpoch}
			}
		}
		low.Advance(topic, partition, best)
	})
	return low
}

// Closed is the closed watermark Expire last saw per window.
func (o *WindowOffsets) Closed() map[string]time.Time { return o.closed }

// Load seeds the tracker from the store at start. Nothing loaded is pending.
func (o *WindowOffsets) Load(recs []OffsetRecord) {
	for _, r := range recs {
		o.recs[offsetKey{r.Window, r.Bucket.UnixNano(), r.Topic, r.Partition}] = r.Mark
	}
}

// Pending is what the store has not yet written.
func (o *WindowOffsets) Pending() WindowOffsetsDelta { return o.delta }

// Saved clears Pending, once the transaction that wrote it has committed.
func (o *WindowOffsets) Saved() {
	o.delta = WindowOffsetsDelta{Expired: map[string]time.Time{}, Dropped: map[string][]int32{}}
}
```

`Low` scans every record per partition. The record count is bounded by buckets retained × partitions × windows, about 18 for the metering stack, and `Low` runs once per commit. Do not index it.

Run: `go test -short ./internal/core/ -run TestWindowOffsets`
Expected: PASS.

- [ ] **Step 3: Write the store's failing test**

Append to `internal/core/windowoffsets_test.go`, with a connection helper the store and dropper tests share:

```go
// memConn is an in-memory DuckDB connection, as the watermark store's own
// test opens one.
func memConn(t *testing.T) adbc.Connection {
	t.Helper()
	ctx := context.Background()
	db, err := duckdb.OpenPath(ctx, "")
	assert.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	conn, err := db.Connect(ctx)
	assert.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	return conn
}

// The store round-trips records, deletes expired buckets with the tracker's
// predicate, and deletes a dropped partition.
func TestWindowOffsetStore_RoundTrip(t *testing.T) {
	coverage.Covers(t, "state.durability")
	conn := memConn(t)
	ctx := context.Background()
	store := NewWindowOffsetStore(conn, []WindowSpec{minuteSpec})
	assert.NoError(t, store.Init(ctx))

	o := NewWindowOffsets([]WindowSpec{minuteSpec})
	o.Note(msgAt(0, 100, woT0))
	o.Note(msgAt(0, 150, woT0.Add(time.Minute)))
	o.Note(msgAt(1, 7, woT0.Add(time.Minute)))
	o.Merge()
	assert.NoError(t, store.Save(ctx, o.Pending()))
	o.Saved()

	o.Expire("w", woT0.Add(2*time.Minute)) // drops bucket 12:00
	o.Drop("t", []int32{1})
	assert.NoError(t, store.Save(ctx, o.Pending()))

	got, err := store.Load(ctx)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(got))
	assert.Equal(t, int64(150), got[0].Mark.Offset)
	assert.Equal(t, int32(0), got[0].Partition)
	assert.That(t, got[0].Bucket.Equal(woT0.Add(time.Minute)))
}
```

Add imports `context`, `github.com/apache/arrow-adbc/go/adbc`, `github.com/turbolytics/sql-flow/internal/coverage` and `github.com/turbolytics/sql-flow/internal/duckdb` to the test file.

Run: `go test -short ./internal/core/ -run TestWindowOffsetStore`
Expected: compile failure.

- [ ] **Step 4: Implement the store**

Append to `internal/core/windowoffsets.go`, imports `context`, `fmt`, `strings`, `adbc`, `arrow/array`:

```go
// WindowOffsetStore keeps sqlflow_window_offsets on the pipeline's
// connection, written in the batch's transaction beside the rows the records
// describe. No index: rows are deleted as buckets expire, and DuckDB never
// frees rows deleted from an indexed table (#268).
type WindowOffsetStore struct {
	conn  adbc.Connection
	specs []WindowSpec
}

func NewWindowOffsetStore(conn adbc.Connection, specs []WindowSpec) *WindowOffsetStore {
	return &WindowOffsetStore{conn: conn, specs: specs}
}

// Init creates the table if it is absent. Run it under autocommit, before
// the pipeline turns autocommit off.
func (s *WindowOffsetStore) Init(ctx context.Context) error {
	return s.exec(ctx, `CREATE TABLE IF NOT EXISTS `+WindowOffsetsTable+` (
	    window_name  VARCHAR     NOT NULL,
	    bucket       TIMESTAMPTZ NOT NULL,
	    topic        VARCHAR     NOT NULL,
	    partition    INTEGER     NOT NULL,
	    "offset"     BIGINT      NOT NULL,
	    leader_epoch INTEGER     NOT NULL
	)`)
}

// Load reads every record. A key written twice keeps its lower offset.
func (s *WindowOffsetStore) Load(ctx context.Context) ([]OffsetRecord, error) {
	stmt, err := s.conn.NewStatement()
	if err != nil {
		return nil, err
	}
	defer stmt.Close()
	if err := stmt.SetSqlQuery(`SELECT window_name, epoch_us(bucket), topic, partition, min("offset"),
	        arg_min(leader_epoch, "offset")
	    FROM ` + WindowOffsetsTable + ` GROUP BY window_name, bucket, topic, partition`); err != nil {
		return nil, err
	}
	reader, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		return nil, errs.Wrap(errs.CodeStateCorrupt, err, "reading %s from the state file", WindowOffsetsTable)
	}
	defer reader.Release()
	var out []OffsetRecord
	for reader.Next() {
		rec := reader.Record()
		names := rec.Column(0).(*array.String)
		buckets := rec.Column(1).(*array.Int64)
		topics := rec.Column(2).(*array.String)
		parts := rec.Column(3).(*array.Int32)
		offs := rec.Column(4).(*array.Int64)
		epochs := rec.Column(5).(*array.Int32)
		for i := 0; i < int(rec.NumRows()); i++ {
			out = append(out, OffsetRecord{
				Window:    strings.Clone(names.Value(i)),
				Bucket:    time.UnixMicro(buckets.Value(i)).UTC(),
				Topic:     strings.Clone(topics.Value(i)),
				Partition: parts.Value(i),
				Mark:      Mark{Offset: offs.Value(i), LeaderEpoch: epochs.Value(i)},
			})
		}
	}
	return out, reader.Err()
}

// Save writes a delta. It does not commit; the caller owns the transaction.
func (s *WindowOffsetStore) Save(ctx context.Context, d WindowOffsetsDelta) error {
	for window, closed := range d.Expired {
		var spec WindowSpec
		for _, sp := range s.specs {
			if sp.Name == window {
				spec = sp
			}
		}
		// bucket + size + lateness <= closed, as the tracker expires.
		limit := closed.Add(-spec.Lateness).Add(-spec.Size)
		if err := s.exec(ctx, fmt.Sprintf(`DELETE FROM %s WHERE window_name = '%s' AND bucket <= TIMESTAMPTZ '%s'`,
			WindowOffsetsTable, escapeSQLString(window), utcLiteral(limit))); err != nil {
			return fmt.Errorf("expiring offset records for %s: %w", window, err)
		}
	}
	for topic, parts := range d.Dropped {
		if len(parts) == 0 {
			continue
		}
		if err := s.exec(ctx, fmt.Sprintf(`DELETE FROM %s WHERE topic = '%s' AND partition IN (%s)`,
			WindowOffsetsTable, escapeSQLString(topic), joinInt32(parts))); err != nil {
			return fmt.Errorf("dropping offset records: %w", err)
		}
	}
	if len(d.Added) == 0 {
		return nil
	}
	var b strings.Builder
	b.WriteString(`INSERT INTO ` + WindowOffsetsTable + ` VALUES `)
	for i, r := range d.Added {
		if i > 0 {
			b.WriteString(", ")
		}
		fmt.Fprintf(&b, `('%s', TIMESTAMPTZ '%s', '%s', %d, %d, %d)`, escapeSQLString(r.Window),
			utcLiteral(r.Bucket), escapeSQLString(r.Topic), r.Partition, r.Mark.Offset, r.Mark.LeaderEpoch)
	}
	return s.exec(ctx, b.String())
}

func joinInt32(ps []int32) string {
	out := make([]string, len(ps))
	for i, p := range ps {
		out[i] = fmt.Sprint(p)
	}
	return strings.Join(out, ", ")
}

func (s *WindowOffsetStore) exec(ctx context.Context, q string) error {
	stmt, err := s.conn.NewStatement()
	if err != nil {
		return err
	}
	defer stmt.Close()
	if err := stmt.SetSqlQuery(q); err != nil {
		return err
	}
	_, err = stmt.ExecuteUpdate(ctx)
	return err
}
```

Insert only: a key re-added after a rollback is inserted again, and `Load` takes the minimum per key. That avoids an index and an upsert. Rows are removed by expiry and drop, so duplicates never accumulate past retention.

`utcLiteral` is the helper `watermarks.go` uses. If it lives under another name, use that one; `grep -n "func utcLiteral\|func UTCLiteral" internal/core/`.

Run: `go test -short ./internal/core/ -run 'TestWindowOffset'`
Expected: PASS.

- [ ] **Step 5: Publish the closed watermark on the signal**

A failing test first. Append to `internal/managers/watermark_test.go`. The helpers are the package's own (`helpers_test.go`):

```go
// The engine expires offset records on the manager's closed watermark, and
// learns it from the signal after each committed pass.
func TestManagerWindow_PassPublishesClosedOnTheSignal(t *testing.T) {
	coverage.Covers(t, "manager.window")
	ctx := context.Background()
	d := newTestDB(t, "")
	createWindowTable(t, d.pipeline)
	w, sig := newTestWatermark(t, d, testDecl(), &recordingSink{})
	insertBucket(t, d.pipeline, 0, "NYC", 3)
	_, ok := sig.Closed()
	assert.That(t, !ok)

	assertAt(t, d.pipeline, bucket(1))
	assert.NoError(t, w.Pass(ctx))
	got, ok := sig.Closed()
	assert.That(t, ok)
	assert.That(t, got.Equal(bucket(1)))
}
```

In `internal/core/watermarks.go`, add to `WindowSignal`:

```go
	// closed is the manager's committed closed watermark, in nanoseconds;
	// zero before its first pass. The engine expires offset records on it,
	// never on its own asserted watermark, which runs ahead of what has been
	// published.
	closed atomic.Int64
```

```go
// SetClosed records the manager's committed closed watermark. Monotonic.
func (s *WindowSignal) SetClosed(t time.Time) {
	n := t.UnixNano()
	for {
		cur := s.closed.Load()
		if n <= cur || s.closed.CompareAndSwap(cur, n) {
			return
		}
	}
}

// Closed is the manager's committed closed watermark, if it has one.
func (s *WindowSignal) Closed() (time.Time, bool) {
	n := s.closed.Load()
	if n == 0 {
		return time.Time{}, false
	}
	return time.Unix(0, n).UTC(), true
}
```

In `managers.Watermark.Pass`, right after the store load (`closed, hadClosed, err := w.store.Load(...)`):

```go
	if hadClosed && w.signal != nil {
		w.signal.SetClosed(closed)
	}
```

After `committed = true`:

```go
	if w.signal != nil {
		w.signal.SetClosed(watermark)
	}
```

Run: `go test -short ./internal/managers/ ./internal/core/`
Expected: PASS.

- [ ] **Step 6: Write the failing turbine tests**

Append to `internal/core/turbine_test.go`:

```go
// With windows over a Kafka-like source, the committed position is the low
// watermark, not the processed one.
func TestTurbine_CommitsTheLowWatermark(t *testing.T) {
	spec := WindowSpec{Name: "w", Size: time.Minute, Lateness: time.Minute}
	w := NewWatermarks([]WindowSpec{spec}, nil)
	src := &markingSource{fakeSource: fakeSource{batches: [][]Message{{
		{Topic: "t", Partition: 0, Offset: 10, EventAtNanos: woT0.UnixNano()},
		{Topic: "t", Partition: 0, Offset: 11, EventAtNanos: woT0.Add(70 * time.Second).UnixNano()},
	}}}}
	tb := newWindowedTurbine(src, &fakeHandler{}, &fakeSink{}, 2, w, WithWindowOffsets(NewWindowOffsets([]WindowSpec{spec}), nil))
	_, err := tb.ConsumeLoop(context.Background(), 2)
	assert.NoError(t, err)
	last := src.marks[len(src.marks)-1]
	m, _ := last.Get("t", 0)
	assert.Equal(t, int64(9), m.Offset)
}

// Once the manager reports a closed watermark past a bucket's lateness, the
// next commit moves past that bucket's rows.
func TestTurbine_CommitMovesWhenTheManagerCloses(t *testing.T) {
	spec := WindowSpec{Name: "w", Size: time.Minute, Lateness: time.Minute}
	w := NewWatermarks([]WindowSpec{spec}, nil)
	w.Signal("w").SetClosed(woT0.Add(2 * time.Minute))
	src := &markingSource{fakeSource: fakeSource{batches: [][]Message{{
		{Topic: "t", Partition: 0, Offset: 10, EventAtNanos: woT0.UnixNano()},
		{Topic: "t", Partition: 0, Offset: 11, EventAtNanos: woT0.Add(150 * time.Second).UnixNano()},
	}}}}
	tb := newWindowedTurbine(src, &fakeHandler{}, &fakeSink{}, 2, w, WithWindowOffsets(NewWindowOffsets([]WindowSpec{spec}), nil))
	_, err := tb.ConsumeLoop(context.Background(), 2)
	assert.NoError(t, err)
	m, _ := src.marks[len(src.marks)-1].Get("t", 0)
	assert.Equal(t, int64(10), m.Offset)
}
```

Add the constructor both tests use. Options go in at construction because `NewTurbine` runs `watchSource` after applying them, and a subscription made there must see the windows. `assertWatermarks` calls the saver whenever a watermark moves, so it cannot be nil; `benchSaver` (`bench_watermarks_test.go`) is the no-op one this package already has.

```go
// newWindowedTurbine builds a turbine over windows, with every option applied
// at construction so watchSource and the subscriptions see them.
func newWindowedTurbine(src Source, h Handler, sink Sink, batch int, w *Watermarks, opts ...TurbineOption) *Turbine {
	opts = append([]TurbineOption{WithWindows(w, &benchSaver{})}, opts...)
	return NewTurbine(src, h, sink, batch, time.Second, &sync.Mutex{}, PipelineErrorPolicies{}, opts...)
}
```

The turbine is built without `WithEventTimePlacement`, so no record is refused for its clock. The tracker has asserted nothing yet, so `Classify` admits both records.

Run: `go test -short ./internal/core/ -run 'TestTurbine_Commit(sTheLowWatermark|MovesWhen)'`
Expected: compile failure, `undefined: WithWindowOffsets`.

- [ ] **Step 7: Wire the tracker into the turbine**

In `internal/core/turbine.go`, add fields:

```go
	// windowOffsets tracks the lowest offset feeding each retained bucket, so
	// the source commit stays below every row a window still holds;
	// windowOffsetStore persists it in the state transaction. Nil without a
	// window over a source with positions.
	windowOffsets     *WindowOffsets
	windowOffsetStore *WindowOffsetStore
```

```go
// WithWindowOffsets commits each partition's low watermark rather than its
// processed position. store is nil without a state path: the tracker still
// holds the commit back, and a restart replays from Kafka.
func WithWindowOffsets(o *WindowOffsets, store *WindowOffsetStore) TurbineOption {
	return func(t *Turbine) {
		t.windowOffsets = o
		t.windowOffsetStore = store
	}
}
```

In `ConsumeLoop`, directly after `t.notePlaced(raw.Topic, raw.Partition, raw.EventAtNanos)`:

```go
				if t.windowOffsets != nil {
					t.windowOffsets.Note(raw)
				}
```

In `processBatch`, after `t.flushObservations()`:

```go
	if t.windowOffsets != nil {
		t.windowOffsets.Merge()
	}
```

In `commitState`, before `moved, watermarkErr := t.assertWatermarks(ctx, write)`:

```go
	t.expireWindowOffsets()
```

```go
// expireWindowOffsets drops the records of buckets the manager has closed
// past their lateness.
func (t *Turbine) expireWindowOffsets() {
	if t.windowOffsets == nil || t.windows == nil {
		return
	}
	for _, spec := range t.windows.Specs() {
		if closed, ok := t.windows.Signal(spec.Name).Closed(); ok {
			t.windowOffsets.Expire(spec.Name, closed)
		}
	}
}
```

In the state-transaction path of `commitState`, after `t.offsets.Save(ctx, t.marks)` succeeds:

```go
	if t.windowOffsetStore != nil {
		if err := t.windowOffsetStore.Save(ctx, t.windowOffsets.Pending()); err != nil {
			if rbErr := t.stateTx.Rollback(context.WithoutCancel(ctx)); rbErr != nil {
				t.logger.Error("rollback after failed offset-record save", zap.Error(rbErr))
			}
			t.dropPendingLate()
			return errs.Wrap(errs.CodeStateCommitFailed, err, "saving offset records")
		}
	}
```

After `t.stateTx.Commit` succeeds, call `if t.windowOffsets != nil { t.windowOffsets.Saved() }`. On the no-state path, call `Saved()` unconditionally, since nothing persists.

In `commitSource`:

```go
func (t *Turbine) commitSource() error {
	if mc, ok := t.source.(MarkCommitter); ok {
		if t.marks.Empty() {
			return nil
		}
		if t.windowOffsets != nil {
			return mc.CommitMarks(t.windowOffsets.Low(t.marks))
		}
		return mc.CommitMarks(t.marks)
	}
	return t.source.Commit()
}
```

Update `commitSource`'s comment: with windows the commit is the low watermark, and why.

Run: `go test -short -race ./internal/core/`
Expected: PASS.

- [ ] **Step 8: Wire it in run**

In `internal/cli/run/root.go`, where the state path is set up (`offsets.Init` and `offsets.Load`, before autocommit is disabled), when `conf.HasWindow()` and the source type is `kafka`:

```go
				if conf.HasWindow() && conf.Pipeline.Source.Type == "kafka" {
					windowOffsetStore = core.NewWindowOffsetStore(conn, windowSpecs(conf))
					if err := windowOffsetStore.Init(context.Background()); err != nil {
						return err
					}
					if storedWindowOffsets, err = windowOffsetStore.Load(context.Background()); err != nil {
						return err
					}
				}
```

Declare `windowOffsetStore *core.WindowOffsetStore` and `storedWindowOffsets []core.OffsetRecord` beside `storedMarks`. After the state block, for any windowed Kafka pipeline, with or without a state path:

```go
			if conf.HasWindow() && conf.Pipeline.Source.Type == "kafka" {
				wo := core.NewWindowOffsets(windowSpecs(conf))
				wo.Load(storedWindowOffsets)
				turbineOpts = append(turbineOpts, core.WithWindowOffsets(wo, windowOffsetStore))
			}
```

`windowSpecs` is in `managers.go`. It builds the specs from the config and is what `NewWatermarks` receives, so the names match the signals.

- [ ] **Step 9: Run the unit pass and scenarios 4, 5 and 11**

Run: `go test -short -race ./internal/core/ ./internal/managers/ ./internal/cli/... > $TMPDIR/u5.txt 2>&1; echo rc=$?`
Expected: rc=0.

Run: `go test -run 'TestIntegrationMetering_(CrashStateKept|CrashStateLost|ReplayPastARefusedLateEvent)' -v ./internal/metering/ > $TMPDIR/s5.txt 2>&1; echo rc=$?`
Expected:
- Scenario 4 PASS.
- Scenario 5 PASS: the fresh worker replays from the low watermark and rebuilds the open minutes.
- Scenario 11 FAIL, and differently from Task 1's run: minute 7 is now exact, and minute 0 for the straggler's customer and meter reads 1. The fresh worker admitted the straggler into an expired minute and published it alone. That is the defect Task 6 fixes.

Record the scenario 11 failure line. If minute 0 is exact and nothing else differs, the replay did not reach the straggler: check that it landed on the same partition as that customer's minute 7 records.

- [ ] **Step 10: Commit**

```bash
git add internal/
git commit -m "core: commit each partition's low watermark on a windowed Kafka pipeline

commitSource committed the processed position, which passes rows that live
only in an open window. A worker whose disk was gone started from the group
offset and never saw them: metering scenario 5 undercounted. The engine now
records the lowest offset feeding each retained bucket, in the batch's
transaction, expires records on the manager's closed watermark, and commits
the position before the lowest. Evidence: scenario 5 passes. Scenario 11 now
fails as the spec revision predicted; the next commit fixes it."
git push origin metering/durable-counts
```

---

### Task 6: The replay floor, carried in commit metadata

**Files:**
- Create: `internal/kafka/metadata.go`
- Modify: `internal/kafka/source.go`
- Modify: `internal/sources/init.go`
- Modify: `internal/core/turbine.go`
- Test: `internal/kafka/metadata_test.go` (new), `internal/core/turbine_test.go`

**Interfaces:**
- Produces in `core`:
  ```go
  // MetadataCommitter is a MarkCommitter that commits a string with each
  // position and reports what the group last committed for a partition when
  // this consumer is assigned it.
  type MetadataCommitter interface {
  	CommitMarksWithMetadata(marks *Marks, metadata string) error
  	OnCommittedMetadata(fn func(topic string, partition int32, metadata string))
  }
  func EncodeReplayFloor(closed map[string]time.Time) string
  func DecodeReplayFloor(s string) (map[string]time.Time, bool)
  ```
- Produces in `kafka`: `type CommittedMetadata struct{...}`, `func NewCommittedMetadata() *CommittedMetadata`, `func (c *CommittedMetadata) ClientOptions() []kgo.Opt`, `func (c *CommittedMetadata) Subscribe(fn func(string, int32, string))`, `func WithCommittedMetadata(c *CommittedMetadata) Option`.

The metadata is `sqlflow1 ` followed by JSON `{"closed":{"<window>":"<RFC3339Nano>"}}`. The prefix lets a reader skip metadata some other tool wrote.

- [ ] **Step 1: Write the failing codec and turbine tests**

Append to `internal/core/windowoffsets_test.go`:

```go
func TestReplayFloor_RoundTrip(t *testing.T) {
	in := map[string]time.Time{"w": woT0, "v": woT0.Add(time.Minute)}
	out, ok := DecodeReplayFloor(EncodeReplayFloor(in))
	assert.That(t, ok)
	assert.That(t, out["w"].Equal(woT0))
	assert.That(t, out["v"].Equal(woT0.Add(time.Minute)))
	_, ok = DecodeReplayFloor("")
	assert.That(t, !ok)
	_, ok = DecodeReplayFloor(`{"closed":{}}`) // someone else's metadata
	assert.That(t, !ok)
}
```

Append to `internal/core/turbine_test.go`:

```go
// metadataSource reports committed metadata for a partition on assignment,
// as the Kafka source does from the offset fetch.
type metadataSource struct {
	markingSource
	fn   func(string, int32, string)
	meta string
}

func (s *metadataSource) CommitMarksWithMetadata(m *Marks, meta string) error {
	s.meta = meta
	return s.CommitMarks(m)
}
func (s *metadataSource) OnCommittedMetadata(fn func(string, int32, string)) { s.fn = fn }

// A worker replaying a partition it has no state for refuses a record the
// previous owner had finalized: its bucket ended at or before closed minus
// lateness.
func TestTurbine_RefusesBelowTheReplayFloor(t *testing.T) {
	spec := WindowSpec{Name: "w", Size: time.Minute, Lateness: time.Minute}
	w := NewWatermarks([]WindowSpec{spec}, nil)
	src := &metadataSource{markingSource: markingSource{fakeSource: fakeSource{batches: [][]Message{{
		{Topic: "t", Partition: 0, Offset: 10, EventAtNanos: woT0.Add(10 * time.Second).UnixNano(), Value: []byte("old")},
		{Topic: "t", Partition: 0, Offset: 11, EventAtNanos: woT0.Add(5 * time.Minute).UnixNano(), Value: []byte("new")},
	}}}}}
	h := &recordingHandler{}
	tb := newWindowedTurbine(src, h, &fakeSink{}, 2, w, WithWindowOffsets(NewWindowOffsets([]WindowSpec{spec}), nil))
	// The previous owner had closed through 12:03; bucket 12:00 expired at 12:02.
	src.fn("t", 0, EncodeReplayFloor(map[string]time.Time{"w": woT0.Add(3 * time.Minute)}))
	_, err := tb.ConsumeLoop(context.Background(), 2)
	assert.NoError(t, err)
	assert.Equal(t, []string{"new"}, h.values())
}

// The commit carries this worker's closed watermarks.
func TestTurbine_CommitCarriesTheClosedWatermarks(t *testing.T) {
	spec := WindowSpec{Name: "w", Size: time.Minute, Lateness: time.Minute}
	w := NewWatermarks([]WindowSpec{spec}, nil)
	w.Signal("w").SetClosed(woT0)
	src := &metadataSource{markingSource: markingSource{fakeSource: fakeSource{batches: [][]Message{{
		{Topic: "t", Partition: 0, Offset: 10, EventAtNanos: woT0.Add(5 * time.Minute).UnixNano()},
	}}}}}
	tb := newWindowedTurbine(src, &fakeHandler{}, &fakeSink{}, 1, w, WithWindowOffsets(NewWindowOffsets([]WindowSpec{spec}), nil))
	_, err := tb.ConsumeLoop(context.Background(), 1)
	assert.NoError(t, err)
	got, ok := DecodeReplayFloor(src.meta)
	assert.That(t, ok)
	assert.That(t, got["w"].Equal(woT0))
}
```

`recordingHandler` is a `fakeHandler` that keeps the values written. If `turbine_test.go` has none, add one:

```go
type recordingHandler struct {
	fakeHandler
	mu   sync.Mutex
	vals []string
}

func (h *recordingHandler) Write(b []byte) error {
	h.mu.Lock()
	h.vals = append(h.vals, string(b))
	h.mu.Unlock()
	return h.fakeHandler.Write(b)
}

func (h *recordingHandler) values() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.vals...)
}
```

`src.fn` is set when the turbine subscribes during construction, so it is non-nil by the time the test calls it. If `watchSource` subscribes only for a `PartitionOwner`, subscribe to metadata independently of that switch.

Run: `go test -short ./internal/core/ -run 'ReplayFloor|CommitCarries'`
Expected: compile failure.

- [ ] **Step 2: Implement the codec and the floor check**

Append to `internal/core/windowoffsets.go` (imports `encoding/json`, `strings`):

```go
// replayFloorPrefix marks commit metadata this engine wrote.
const replayFloorPrefix = "sqlflow1 "

// EncodeReplayFloor is the commit metadata for a windowed pipeline: each
// window's closed watermark. A worker that replays a partition from the low
// watermark has no watermark of its own. Without this, a late event the
// previous owner refused would land in an expired bucket and be published
// alone over the full count.
func EncodeReplayFloor(closed map[string]time.Time) string {
	type body struct {
		Closed map[string]time.Time `json:"closed"`
	}
	b, _ := json.Marshal(body{Closed: closed})
	return replayFloorPrefix + string(b)
}

// DecodeReplayFloor reads EncodeReplayFloor's output. Anything else, such as
// metadata another tool committed, is not a floor.
func DecodeReplayFloor(s string) (map[string]time.Time, bool) {
	rest, ok := strings.CutPrefix(s, replayFloorPrefix)
	if !ok {
		return nil, false
	}
	var body struct {
		Closed map[string]time.Time `json:"closed"`
	}
	if json.Unmarshal([]byte(rest), &body) != nil || body.Closed == nil {
		return nil, false
	}
	return body.Closed, true
}
```

In `internal/core/turbine.go`, add the `MetadataCommitter` interface from this task's Interfaces block, and fields:

```go
	// floorInbox receives replay floors from the source's goroutine;
	// floorsWaiting says it holds any, so the loop checks with one atomic
	// load. replayFloors is the loop's own copy, closed nanos per spec index.
	floorMu       sync.Mutex
	floorInbox    map[partitionKey][]int64
	floorsWaiting atomic.Bool
	replayFloors  map[partitionKey][]int64
```

Subscribe in `WithWindowOffsets`, which only a windowed Kafka pipeline sets. Add inside its returned function:

```go
		if mc, ok := t.source.(MetadataCommitter); ok {
			mc.OnCommittedMetadata(t.noteCommittedMetadata)
		}
```

```go
// noteCommittedMetadata runs on the source's goroutine when this consumer is
// assigned a partition. The loop picks the floor up before its next record.
func (t *Turbine) noteCommittedMetadata(topic string, partition int32, metadata string) {
	closed, ok := DecodeReplayFloor(metadata)
	if !ok || t.windows == nil {
		return
	}
	specs := t.windows.Specs()
	floor := make([]int64, len(specs))
	for i, spec := range specs {
		if c, ok := closed[spec.Name]; ok {
			floor[i] = c.UnixNano()
		}
	}
	t.floorMu.Lock()
	if t.floorInbox == nil {
		t.floorInbox = map[partitionKey][]int64{}
	}
	t.floorInbox[partitionKey{topic, partition}] = floor
	t.floorMu.Unlock()
	t.floorsWaiting.Store(true)
}

func (t *Turbine) takeFloors() {
	if !t.floorsWaiting.Load() {
		return
	}
	t.floorMu.Lock()
	inbox := t.floorInbox
	t.floorInbox = nil
	t.floorsWaiting.Store(false)
	t.floorMu.Unlock()
	if t.replayFloors == nil {
		t.replayFloors = map[partitionKey][]int64{}
	}
	for k, v := range inbox {
		t.replayFloors[k] = v
	}
}

// belowReplayFloor is true when every window would refuse the record under
// the previous owner's closed watermark: its bucket ended at or before
// closed minus lateness. The same all-windows rule as Classify.
func (t *Turbine) belowReplayFloor(m Message) bool {
	floor, ok := t.replayFloors[partitionKey{m.Topic, m.Partition}]
	if !ok || m.EventAtNanos <= 0 {
		return false
	}
	at := time.Unix(0, m.EventAtNanos).UTC()
	for i, spec := range t.windows.Specs() {
		if floor[i] == 0 || BucketEnd(at, spec.Size).UnixNano()+int64(spec.Lateness) > floor[i] {
			return false
		}
	}
	return true
}
```

In `ConsumeLoop`, call `t.takeFloors()` once per source batch, before the per-message loop. In the per-message loop, inside `if t.windows != nil {`, before `Classify`:

```go
				if len(t.replayFloors) > 0 && t.belowReplayFloor(raw) {
					t.noteRefusedLate(ctx, raw)
					t.mark(raw)
					totalConsumed++
					t.unsettled++
					t.stats.SetNumMessagesConsumed(totalConsumed)
					if maxMsgs > 0 && totalConsumed >= int64(maxMsgs) {
						t.logger.Info("max messages consumed, stopping consumer loop")
						hitMax = true
						break
					}
					continue
				}
```

In `commitSource`, the windowed branch becomes:

```go
		if t.windowOffsets != nil {
			low := t.windowOffsets.Low(t.marks)
			if mc2, ok := t.source.(MetadataCommitter); ok {
				return mc2.CommitMarksWithMetadata(low, EncodeReplayFloor(t.windowOffsets.Closed()))
			}
			return mc.CommitMarks(low)
		}
```

Run: `go test -short -race ./internal/core/`
Expected: PASS.

- [ ] **Step 3: Write the Kafka metadata integration test**

`internal/kafka/metadata_test.go`:

```go
package kafka

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/turbolytics/sql-flow/internal/core"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

// A committed position carries the metadata, and the next consumer in the
// group is told it when assigned the partition.
func TestIntegrationSourceKafka_CommitMetadataReachesTheNextOwner(t *testing.T) {
	coverage.Covers(t, "source.kafka")
	broker := brokerOrFail(t)
	t.Parallel()
	topic := fmt.Sprintf("turbine-commit-meta-%d", time.Now().UnixNano())
	createTopic(t, broker, topic, 1)

	first := NewCommittedMetadata()
	client := newTestClient(t, broker, topic, topic, first.ClientOptions()...)
	produce(t, client, topic, 5)
	src, err := NewSource(client, WithCommittedMetadata(first))
	assert.NoError(t, err)
	stream := src.Stream()
	<-stream // the group is joined and the partition held
	marks := core.NewMarks()
	marks.Advance(topic, 0, core.Mark{Offset: 2})
	assert.NoError(t, src.CommitMarksWithMetadata(marks, "sqlflow1 {\"closed\":{}}"))
	assert.NoError(t, src.Close())

	second := NewCommittedMetadata()
	got := make(chan string, 1)
	second.Subscribe(func(tp string, p int32, meta string) {
		if tp == topic && p == 0 {
			got <- meta
		}
	})
	client2 := newTestClient(t, broker, topic, topic, second.ClientOptions()...)
	src2, err := NewSource(client2, WithCommittedMetadata(second))
	assert.NoError(t, err)
	defer src2.Close()
	src2.Stream()
	select {
	case meta := <-got:
		assert.Equal(t, "sqlflow1 {\"closed\":{}}", meta)
	case <-time.After(60 * time.Second):
		t.Fatal("the second consumer was never told the committed metadata")
	}
}
```

`produce` is in `source_test.go`. Check its signature before use.

Run: `go test -run TestIntegrationSourceKafka_CommitMetadata ./internal/kafka/`
Expected: compile failure.

- [ ] **Step 4: Implement the Kafka side**

`internal/kafka/metadata.go`:

```go
package kafka

import (
	"context"
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// CommittedMetadata relays the metadata the group last committed for each
// partition this consumer is assigned, read from the offset fetch that
// follows every assignment. Built before the client, as PartitionEvents is,
// because the callback is a client option.
type CommittedMetadata struct {
	mu   sync.Mutex
	subs []func(topic string, partition int32, metadata string)
}

func NewCommittedMetadata() *CommittedMetadata { return &CommittedMetadata{} }

func (c *CommittedMetadata) ClientOptions() []kgo.Opt {
	return []kgo.Opt{kgo.OnOffsetsFetched(c.onFetched)}
}

func (c *CommittedMetadata) Subscribe(fn func(topic string, partition int32, metadata string)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.subs = append(c.subs, fn)
}

// onFetched reads both response shapes: v8 and later nest topics under
// groups, and earlier versions list them at the top.
func (c *CommittedMetadata) onFetched(_ context.Context, _ *kgo.Client, resp *kmsg.OffsetFetchResponse) error {
	c.mu.Lock()
	subs := append([]func(string, int32, string){}, c.subs...)
	c.mu.Unlock()
	emit := func(topic string, partition int32, meta *string) {
		if meta == nil || *meta == "" {
			return
		}
		for _, fn := range subs {
			fn(topic, partition, *meta)
		}
	}
	for _, t := range resp.Topics {
		for _, p := range t.Partitions {
			emit(t.Topic, p.Partition, p.Metadata)
		}
	}
	for _, g := range resp.Groups {
		for _, t := range g.Topics {
			for _, p := range t.Partitions {
				emit(t.Topic, p.Partition, p.Metadata)
			}
		}
	}
	return nil
}
```

Before relying on the field names, check them against `kmsg` v1.13.1:

```bash
grep -n "type OffsetFetchResponse\b\|type OffsetFetchResponseTopicPartition\b\|type OffsetFetchResponseGroupTopicPartition\b" -A 20 $(go env GOMODCACHE)/github.com/twmb/franz-go/pkg/kmsg@v1.13.1/generated.go | grep -n "Metadata\|Topic \|Partition "
```

In `internal/kafka/source.go`:

```go
	metadata *CommittedMetadata
```

```go
// WithCommittedMetadata gives the source the metadata relay registered on its
// client, so the pipeline hears what the group committed for a partition
// when this consumer is assigned it.
func WithCommittedMetadata(c *CommittedMetadata) Option {
	return func(s *Source) { s.metadata = c }
}

// OnCommittedMetadata implements core.MetadataCommitter.
func (k *Source) OnCommittedMetadata(fn func(topic string, partition int32, metadata string)) {
	if k.metadata != nil {
		k.metadata.Subscribe(fn)
	}
}

// CommitMarksWithMetadata is CommitMarks with a string on every partition.
func (k *Source) CommitMarksWithMetadata(marks *core.Marks, metadata string) error {
	return k.commitMarks(marks, &metadata)
}
```

Rename the body of `CommitMarks` to `commitMarks(marks *core.Marks, metadata *string) error`, have `CommitMarks` call `k.commitMarks(marks, nil)`, and set the context:

```go
	ctx, cancel := context.WithTimeout(context.Background(), commitTimeout)
	defer cancel()
	if metadata != nil {
		ctx = kgo.PreCommitFnContext(ctx, func(req *kmsg.OffsetCommitRequest) error {
			for ti := range req.Topics {
				for pi := range req.Topics[ti].Partitions {
					req.Topics[ti].Partitions[pi].Metadata = metadata
				}
			}
			return nil
		})
	}
```

Add `var _ core.MetadataCommitter = (*Source)(nil)` beside the other assertions.

In `internal/sources/init.go`, build `meta := tkafka.NewCommittedMetadata()` beside `partitions`, append `meta.ClientOptions()...` to the client options, and pass `tkafka.WithCommittedMetadata(meta)` to `NewSource`.

Run: `go test -run 'TestIntegrationSourceKafka_CommitMetadata' ./internal/kafka/ > $TMPDIR/k6.txt 2>&1; echo rc=$?`
Expected: PASS. If the metadata arrives empty, `PreCommitFnContext` did not reach `CommitOffsetsSync`'s request. Read `consumer_group.go` around `commitContextFn` to see which call path honors it, and use that path.

- [ ] **Step 5: Run scenarios 5 and 11**

Run: `go test -run 'TestIntegrationMetering_(CrashStateLost|ReplayPastARefusedLateEvent)' -v ./internal/metering/ > $TMPDIR/s6b.txt 2>&1; echo rc=$?`
Expected: both PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/
git commit -m "core: carry the closed watermark in the Kafka commit, and refuse below it on replay

With the low-watermark commit, a worker that replayed a partition with no
state of its own had no watermark: a late event the previous owner refused,
sitting past the low watermark, landed in an expired minute and was
published alone over the full count (metering scenario 11). The commit now
carries each window's closed watermark as metadata, the next owner reads it
from the offset fetch, and refuses a record whose bucket ended at or before
closed minus lateness. Evidence: scenarios 5 and 11 pass."
git push origin metering/durable-counts
```

---

### Task 7: `partition_owned`: drop a revoked partition's rows

**Files:**
- Modify: `internal/config/config.go` (`Window`, `Conf`)
- Modify: `internal/core/watermarks.go` (`WindowSpec`, `WindowSignal` pass lock)
- Create: `internal/core/partitiondrop.go`
- Modify: `internal/core/offsets.go` (`Delete`)
- Modify: `internal/core/turbine.go`
- Modify: `internal/managers/watermark.go` (hold the pass lock)
- Modify: `internal/cli/run/managers.go`, `internal/cli/run/root.go`
- Modify: `internal/validate/window.go`
- Modify: `internal/metering/harness_test.go` (`current.PartitionOwned = true`)
- Test: `internal/core/turbine_test.go`, `internal/core/partitiondrop_test.go` (new), `internal/validate/window_test.go`

**Interfaces:**
- Consumes: `Marks.Forget` (Task 4); `WindowOffsets.Drop` (Task 5).
- Produces:
  ```go
  // WindowSpec gains:
  PartitionOwned bool
  // WindowSignal gains:
  func (s *WindowSignal) LockPass()
  func (s *WindowSignal) UnlockPass()
  // core:
  type PartitionDropper interface {
  	DropPartitions(ctx context.Context, topic string, partitions []int32) error
  }
  func NewDuckDBPartitionDropper(conn adbc.Connection, tables []string) *DuckDBPartitionDropper
  func WithPartitionDropper(d PartitionDropper) TurbineOption
  func (s *OffsetStore) Delete(ctx context.Context, topic string, partitions []int32) error
  // config:
  Window.PartitionOwned bool `yaml:"partition_owned,omitempty"`
  func (c *Conf) CheckPartitionOwned() error
  ```

How a revocation is handled:

1. franz-go calls the relay's revoked callback. The turbine's `released` wrapper sends a drop request to the consume loop and waits.
2. The loop services the request between batches: it processes the partial batch, if any, so every processed position is committed with its low watermark. Then, holding each owned window's pass lock and the turbine lock, it deletes the partition's rows, offset records and stored mark in one transaction. It also forgets the partition in `marks`, `committed` and the replay floors, and adds it to the revoked set.
3. The callback returns and calls `t.windows.Released`.
4. Records from a revoked partition that arrive later, prefetched before the revocation, are skipped without being marked.

- [ ] **Step 1: Write the failing turbine tests**

Append to `internal/core/turbine_test.go`:

```go
// recordingDropper records drops, in order.
type recordingDropper struct {
	mu    sync.Mutex
	drops []string
}

func (d *recordingDropper) DropPartitions(_ context.Context, topic string, ps []int32) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.drops = append(d.drops, fmt.Sprintf("%s%v", topic, ps))
	return nil
}

// ownerSource is a markingSource that reports partitions, lets the test
// revoke them, and delivers what the test puts on its stream.
type ownerSource struct {
	markingSource
	stream                   chan []Message
	assigned, released, lost func(map[string][]int32)
}

func newOwnerSource() *ownerSource { return &ownerSource{stream: make(chan []Message, 1)} }

func (s *ownerSource) Stream() <-chan []Message { return s.stream }

func (s *ownerSource) OnPartitions(a, r, l func(map[string][]int32)) {
	s.assigned, s.released, s.lost = a, r, l
}

var ownedSpec = WindowSpec{Name: "w", Size: time.Minute, Lateness: time.Minute, PartitionOwned: true}

// A revocation deletes the partition's rows before the tracker releases it,
// and the callback returns only after the delete.
func TestTurbine_RevocationDropsBeforeRelease(t *testing.T) {
	src := newOwnerSource()
	d := &recordingDropper{}
	w := NewWatermarks([]WindowSpec{ownedSpec}, nil)
	tb := newWindowedTurbine(src, &fakeHandler{}, &fakeSink{}, 100, w, WithPartitionDropper(d))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _, _ = tb.ConsumeLoop(ctx, 0) }()

	src.assigned(map[string][]int32{"t": {0, 1}})
	src.released(map[string][]int32{"t": {1}}) // blocks until dropped
	d.mu.Lock()
	assert.Equal(t, []string{"t[1]"}, d.drops)
	d.mu.Unlock()
}

// A record prefetched from a partition before its revocation is skipped
// when it arrives: its rows would otherwise re-enter the window.
func TestTurbine_SkipsRecordsFromADroppedPartition(t *testing.T) {
	src := newOwnerSource()
	h := &recordingHandler{}
	w := NewWatermarks([]WindowSpec{ownedSpec}, nil)
	tb := newWindowedTurbine(src, h, &fakeSink{}, 1, w, WithPartitionDropper(&recordingDropper{}))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { _, _ = tb.ConsumeLoop(ctx, 0); close(done) }()

	src.assigned(map[string][]int32{"t": {0, 1}})
	src.released(map[string][]int32{"t": {1}})
	src.stream <- []Message{
		{Topic: "t", Partition: 1, Offset: 5, EventAtNanos: woT0.UnixNano(), Value: []byte("stale")},
		{Topic: "t", Partition: 0, Offset: 9, EventAtNanos: woT0.UnixNano(), Value: []byte("ours")},
	}
	waitFor(t, "the handler to take the owned record", time.Second, func() bool { return len(h.values()) == 1 })
	assert.Equal(t, []string{"ours"}, h.values())
	cancel()
	<-done
}
```

`waitFor` is the helper in `internal/core/slow_test.go`. `newWindowedTurbine` comes from Task 5, and `recordingHandler` from Task 6. Add `fmt` to the test file's imports if it is not there.

Run: `go test -short ./internal/core/ -run 'Revocation|SkipsRecords'`
Expected: compile failure, `unknown field PartitionOwned`.

- [ ] **Step 2: Implement the drop in the turbine**

`internal/core/watermarks.go`: add `PartitionOwned bool` to `WindowSpec`, with a comment saying every row of the window belongs to one Kafka partition. Add to `WindowSignal`:

```go
	// pass is held by the manager for the length of a pass. A partition drop
	// takes it too, so no pass is publishing a revoked partition's rows while
	// they are deleted.
	pass sync.Mutex
```

```go
func (s *WindowSignal) LockPass()   { s.pass.Lock() }
func (s *WindowSignal) UnlockPass() { s.pass.Unlock() }
```

`internal/managers/watermark.go`, at the top of `Pass`:

```go
	if w.signal != nil {
		w.signal.LockPass()
		defer w.signal.UnlockPass()
	}
```

`internal/core/turbine.go`: add fields:

```go
	// dropper deletes a revoked partition's rows from every partition-owned
	// window; drops carries requests from the source's rebalance callback to
	// the consume loop, which owns the connection. loopDone is closed when
	// ConsumeLoop returns, so a callback never waits on a loop that is gone.
	dropper  PartitionDropper
	drops    chan dropRequest
	loopDone chan struct{}
	// revoked holds partitions dropped since their revocation; records the
	// consumer prefetched from them are skipped. Copy-on-write behind an
	// atomic pointer, so the per-record check takes no lock: after a
	// scale-out the set stays non-empty for the life of the process.
	// revokedMu serializes the writers only.
	revokedMu sync.Mutex
	revoked   atomic.Pointer[map[partitionKey]bool]
```

```go
type dropRequest struct {
	parts map[string][]int32
	done  chan error
}

// PartitionDropper deletes a partition's rows from every partition-owned
// window, and its stored positions, inside the pipeline's open transaction.
type PartitionDropper interface {
	DropPartitions(ctx context.Context, topic string, partitions []int32) error
}

// WithPartitionDropper makes a revocation drop the partition's rows rather
// than let them close.
func WithPartitionDropper(d PartitionDropper) TurbineOption {
	return func(t *Turbine) { t.dropper = d }
}
```

In `NewTurbine`, initialize `drops: make(chan dropRequest)` and `loopDone: make(chan struct{})`. In `ConsumeLoop`, `defer close(t.loopDone)` as the first defer.

`watchSource`, the `PartitionOwner` case:

```go
	case PartitionOwner:
		t.partitionsRelayed = true
		s.OnPartitions(t.assigned, t.released, t.lost)
```

```go
func (t *Turbine) assigned(parts map[string][]int32) {
	t.updateRevoked(parts, false)
	t.windows.Assigned(parts)
}

// updateRevoked adds or removes partitions from the revoked set, copying it,
// so a reader holding the old map never sees it change.
func (t *Turbine) updateRevoked(parts map[string][]int32, revoked bool) {
	t.revokedMu.Lock()
	defer t.revokedMu.Unlock()
	next := map[partitionKey]bool{}
	if cur := t.revoked.Load(); cur != nil {
		for k := range *cur {
			next[k] = true
		}
	}
	for topic, ps := range parts {
		for _, p := range ps {
			if revoked {
				next[partitionKey{topic, p}] = true
			} else {
				delete(next, partitionKey{topic, p})
			}
		}
	}
	t.revoked.Store(&next)
}

func (t *Turbine) released(parts map[string][]int32) {
	t.requestDrop(parts)
	t.windows.Released(parts)
}

func (t *Turbine) lost(parts map[string][]int32) {
	t.requestDrop(parts)
	t.windows.Lost(parts)
}

// requestDrop runs on the source's rebalance goroutine. It blocks until the
// consume loop has dropped the partitions, which holds the rebalance until
// no process but the new owner can write their rows.
func (t *Turbine) requestDrop(parts map[string][]int32) {
	if t.dropper == nil {
		return
	}
	req := dropRequest{parts: parts, done: make(chan error, 1)}
	select {
	case t.drops <- req:
	case <-t.loopDone:
		return
	}
	select {
	case err := <-req.done:
		if err != nil {
			t.logger.Error("dropping revoked partitions failed; the pipeline is stopping", zap.Error(err))
		}
	case <-t.loopDone:
	}
}
```

In `ConsumeLoop`'s select, add a case:

```go
		case req := <-t.drops:
			if numBatchMessages > 0 {
				if err := t.processBatch(batchCtx, numBatchMessages); err != nil {
					req.done <- err
					return nil, t.drainError(err, numBatchMessages)
				}
				numBatchMessages = 0
			}
			err := t.dropPartitions(batchCtx, req.parts)
			req.done <- err
			if err != nil {
				t.recordError(ctx, err, phaseStateCommit, "error dropping revoked partitions")
				return nil, err
			}
			continue
```

```go
// dropPartitions deletes revoked partitions' rows, offset records and stored
// positions, and commits, holding every owned window's pass lock so no pass
// publishes those rows meanwhile.
func (t *Turbine) dropPartitions(ctx context.Context, parts map[string][]int32) error {
	var owned []*WindowSignal
	for _, spec := range t.windows.Specs() {
		if spec.PartitionOwned {
			owned = append(owned, t.windows.Signal(spec.Name))
		}
	}
	for _, s := range owned {
		s.LockPass()
	}
	defer func() {
		for _, s := range owned {
			s.UnlockPass()
		}
	}()

	t.lock.Lock()
	defer t.lock.Unlock()
	fail := func(err error) error {
		if t.stateTx != nil {
			_ = t.stateTx.Rollback(context.WithoutCancel(ctx))
		}
		return err
	}
	for topic, ps := range parts {
		if err := t.dropper.DropPartitions(ctx, topic, ps); err != nil {
			return fail(err)
		}
		if del, ok := t.offsets.(offsetDeleter); ok {
			if err := del.Delete(ctx, topic, ps); err != nil {
				return fail(err)
			}
		}
		if t.windowOffsets != nil {
			t.windowOffsets.Drop(topic, ps)
		}
		t.marks.Forget(topic, ps)
		t.committed.Forget(topic, ps)
		for _, p := range ps {
			delete(t.replayFloors, partitionKey{topic, p})
		}
	}
	if t.windowOffsetStore != nil {
		if err := t.windowOffsetStore.Save(ctx, t.windowOffsets.Pending()); err != nil {
			return fail(err)
		}
	}
	if t.stateTx != nil {
		if err := t.stateTx.Commit(ctx); err != nil {
			return fail(errs.Wrap(errs.CodeStateCommitFailed, err, "committing the partition drop"))
		}
	}
	if t.windowOffsets != nil {
		t.windowOffsets.Saved()
	}
	t.updateRevoked(parts, true)
	return nil
}
```

The loop has committed everything before it services a drop: it processes the partial batch first, and every commit saves the tracker's pending records. So `Pending()` here holds only this drop's `Dropped` entry. The whole drop commits as one transaction: window rows, the stored position, and the offset records.

`offsetDeleter` is an optional interface, so the ordering tests' `fakeOffsetStore` needs no change:

```go
// offsetDeleter is an offset store that can forget partitions.
type offsetDeleter interface {
	Delete(ctx context.Context, topic string, partitions []int32) error
}
```

Append to `internal/core/offsets.go`:

```go
// Delete removes the stored positions of partitions this process no longer
// holds. It does not commit.
func (s *OffsetStore) Delete(ctx context.Context, topic string, partitions []int32) error {
	stmt, err := s.conn.NewStatement()
	if err != nil {
		return err
	}
	defer stmt.Close()
	if err := stmt.SetSqlQuery(fmt.Sprintf(`DELETE FROM %s WHERE topic = '%s' AND partition IN (%s)`,
		offsetsTable, escapeSQLString(topic), joinInt32(partitions))); err != nil {
		return err
	}
	_, err = stmt.ExecuteUpdate(ctx)
	return err
}
```

At the top of the per-message loop, before the placement check:

```go
			if rv := t.revoked.Load(); rv != nil && len(*rv) > 0 &&
				(*rv)[partitionKey{raw.Topic, raw.Partition}] {
				continue
			}
```

- [ ] **Step 3: Run the turbine tests**

Run: `go test -short -race ./internal/core/ ./internal/managers/`
Expected: PASS.

- [ ] **Step 4: The DuckDB dropper, with a test**

`internal/core/partitiondrop.go`:

```go
package core

import (
	"context"
	"fmt"

	"github.com/apache/arrow-adbc/go/adbc"
)

// DuckDBPartitionDropper deletes a partition's rows from every
// partition-owned window table, on the pipeline's connection, inside its open
// transaction. The turbine deletes the stored position and the offset records
// beside it.
type DuckDBPartitionDropper struct {
	conn   adbc.Connection
	tables []string
}

func NewDuckDBPartitionDropper(conn adbc.Connection, tables []string) *DuckDBPartitionDropper {
	return &DuckDBPartitionDropper{conn: conn, tables: tables}
}

func (d *DuckDBPartitionDropper) DropPartitions(ctx context.Context, topic string, partitions []int32) error {
	in := joinInt32(partitions)
	for _, table := range d.tables {
		q := fmt.Sprintf(`DELETE FROM %s WHERE kafka_partition IN (%s)`, quoteIdent(table), in)
		stmt, err := d.conn.NewStatement()
		if err != nil {
			return err
		}
		if err := stmt.SetSqlQuery(q); err != nil {
			stmt.Close()
			return err
		}
		_, err = stmt.ExecuteUpdate(ctx)
		stmt.Close()
		if err != nil {
			return fmt.Errorf("dropping partitions %s from %s: %w", in, topic, err)
		}
	}
	return nil
}
```

The topic is not in the predicate: `CheckPartitionOwned` requires exactly one topic.

`internal/core/partitiondrop_test.go`:

```go
package core

import (
	"context"
	"testing"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

func TestDuckDBPartitionDropper_DeletesOnlyThePartition(t *testing.T) {
	coverage.Covers(t, "source.kafka")
	ctx := context.Background()
	conn := memConn(t)
	for _, q := range []string{
		`CREATE TABLE w (minute TIMESTAMPTZ, kafka_partition INTEGER, n INTEGER)`,
		`INSERT INTO w VALUES (TIMESTAMPTZ '2026-09-30 12:00:00+00', 0, 1), (TIMESTAMPTZ '2026-09-30 12:00:00+00', 1, 2)`,
	} {
		stmt, err := conn.NewStatement()
		assert.NoError(t, err)
		assert.NoError(t, stmt.SetSqlQuery(q))
		_, err = stmt.ExecuteUpdate(ctx)
		assert.NoError(t, err)
		stmt.Close()
	}
	assert.NoError(t, NewDuckDBPartitionDropper(conn, []string{"w"}).DropPartitions(ctx, "t", []int32{1}))

	offsets := NewOffsetStore(conn)
	assert.NoError(t, offsets.Init(ctx))
	marks := NewMarks()
	marks.Advance("t", 0, Mark{Offset: 1})
	marks.Advance("t", 1, Mark{Offset: 2})
	assert.NoError(t, offsets.Save(ctx, marks))
	assert.NoError(t, offsets.Delete(ctx, "t", []int32{1}))
	loaded, err := offsets.Load(ctx)
	assert.NoError(t, err)
	_, has1 := loaded.Get("t", 1)
	_, has0 := loaded.Get("t", 0)
	assert.That(t, !has1 && has0)

	stmt, err := conn.NewStatement()
	assert.NoError(t, err)
	defer stmt.Close()
	assert.NoError(t, stmt.SetSqlQuery(`SELECT count(*)::BIGINT FROM w WHERE kafka_partition = 1`))
	rdr, _, err := stmt.ExecuteQuery(ctx)
	assert.NoError(t, err)
	defer rdr.Release()
	assert.That(t, rdr.Next())
	assert.Equal(t, int64(0), rdr.Record().Column(0).(*array.Int64).Value(0))
}
```

Import `github.com/apache/arrow-go/v18/arrow/array` for the count.

Run: `go test -short ./internal/core/ -run TestDuckDBPartitionDropper`
Expected: PASS.

- [ ] **Step 5: Config, validate and run wiring**

`internal/config/config.go`, in `Window`:

```go
	// Every row of this window belongs to one Kafka partition, carried in a
	// kafka_partition column. When the group revokes a partition, its rows
	// are deleted without being published, and the new owner recounts them
	// from the committed offset, so one process writes each (bucket,
	// partition) key. Requires a Kafka source with one topic, kafka_partition
	// in the window table, and a postgres upsert sink keyed by
	// kafka_partition among its columns.
	PartitionOwned bool `yaml:"partition_owned,omitempty"`
```

```go
// CheckPartitionOwned refuses a partition_owned window whose pipeline cannot
// honor it. validate and run both call it; validate also checks the SQL.
func (c *Conf) CheckPartitionOwned() error {
	if c.Tables == nil {
		return nil
	}
	for _, table := range c.Tables.SQL {
		w := table.Window
		if w == nil || !w.PartitionOwned {
			continue
		}
		src := c.Pipeline.Source
		if src.Type != "kafka" || src.Kafka == nil || len(src.Kafka.Topics) != 1 {
			return errs.New(errs.CodeConfigInvalid,
				"table %q window: partition_owned needs a kafka source with exactly one topic; "+
					"a row is identified by kafka_partition alone", table.Name)
		}
		if w.Sink.Type != "postgres" || w.Sink.Postgres == nil || w.Sink.Postgres.Mode != "upsert" ||
			!slices.Contains(w.Sink.Postgres.Key, "kafka_partition") {
			return errs.New(errs.CodeConfigInvalid,
				"table %q window: partition_owned needs a postgres upsert sink with kafka_partition in "+
					"its key, so the new owner's recount replaces the row the old owner wrote", table.Name)
		}
	}
	return nil
}
```

In `internal/validate/window.go`, inside the per-window loop:

```go
			if w.PartitionOwned {
				if err := conf.CheckPartitionOwned(); err != nil {
					fail(err.Error(), position(mappingKey(node, "partition_owned")))
				}
				if !mentionsIdentifier(table.SQL, "kafka_partition") {
					fail(fmt.Sprintf("tables.sql[%d] window: partition_owned needs a kafka_partition "+
						"column in the table's CREATE", i), position(node))
				}
				if !mentionsIdentifier(conf.Pipeline.Handler.SQL, "kafka_partition") {
					fail(fmt.Sprintf("tables.sql[%d] window: partition_owned needs the handler's SQL to "+
						"write kafka_partition", i), position(handlerNode(&root)))
				}
			}
```

`mentionsIdentifier` is defined in `sinks.go` (Task 2), in the same package.

Add validate tests to `internal/validate/window_test.go`: one config that passes, and one failure each for a webhook source, a sink key without `kafka_partition`, and a handler that does not write it.

`internal/cli/run/managers.go`: in `windowSpecs`, set `PartitionOwned: table.Window.PartitionOwned`. In `internal/cli/run/root.go`, call `conf.CheckPartitionOwned()` beside `CheckWebhookAck`. When any window is partition-owned:

- Collect the owned tables' names.
- Without a state path, `t.offsets` and `t.windowOffsetStore` are nil, so the drop deletes window rows only. That is correct: nothing on disk would outlive the process.
- Append `core.WithPartitionDropper(core.NewDuckDBPartitionDropper(conn, owned))`.

`NewTurbine` calls `watchSource` during construction, so `WithPartitionDropper` must be in `turbineOpts` before `NewTurbine` runs. It is, because run builds the options first.

Run: `make schema && go test -short -race ./internal/... > $TMPDIR/u7.txt 2>&1; echo rc=$?`
Expected: rc=0.

- [ ] **Step 6: Turn the feature on and run every scenario**

Set `var current = Features{Key: true, Ack: true, PartitionOwned: true}`.

Run:

```bash
perl -e 'alarm shift; exec @ARGV' 2400 go test -run '^TestIntegrationMetering' -parallel 3 -timeout 40m -v ./internal/metering/ > $TMPDIR/metering-after.txt 2>&1; echo rc=$?
grep -E '^(--- (PASS|FAIL)|.*rebalance_under_load)' $TMPDIR/metering-after.txt
```

Expected: every scenario PASS. Record both `rebalance_under_load` lines; the difference between `join` and `control` is the rebalance's replay cost.

- [ ] **Step 7: Commit**

```bash
git add internal/
git commit -m "window: partition_owned drops a revoked partition's rows instead of publishing them

After a rebalance the old owner closed the rows it held for a revoked
partition by the other partitions' progress, and its partial count raced the
new owner's full recount for the same key: metering scenarios 2, 3 and 10
lost counts. With partition_owned, the revocation callback holds the
rebalance until the consume loop has deleted the partition's rows, offset
records and stored position in one transaction, under the window's pass
lock. Prefetched records from the partition are skipped. Evidence: all
eleven scenarios pass; rebalance cost <join - control> at <rate>."
git push origin metering/durable-counts
```

---

### Task 8: Docs, examples, ledger and the PR

**Files:**
- Create: `dev/config/examples/metering/ingest.yml`, `dev/config/examples/metering/count.yml`
- Modify: `README.md` (the window and source sections), `CHANGELOG.md`
- Modify: `docs/coverage/invariants.yml`

- [ ] **Step 1: Write the examples**

Copy the harness's two templates into the example files with real values. The broker is `{{ SQLFLOW_KAFKA_BROKERS|default('kafka:9092') }}`, the DSN is `{{ SQLFLOW_POSTGRES_URI|default(...) }}` as the Bluesky example does, and the state path is `/var/lib/sqlflow/count.duckdb`. Every non-obvious key gets a comment, in the style of `bluesky.postgres.windowed.yml`:

- `ack: after_flush`: why, and the latency it costs.
- `key: customer`: a retry lands on its original's partition.
- `partition_owned: true`: what happens on a revocation.
- the `emit_sql` distinct over `id_hash`: an exact dedupe within retention.
- `allowed_lateness_seconds: 60`: the dedupe horizon is window, grace and lateness together.

Run: `go run ./cmd/sqlflow config validate dev/config/examples/metering/ingest.yml && go run ./cmd/sqlflow config validate dev/config/examples/metering/count.yml`
Expected: both valid. Run `go test -short ./internal/cli/` if a test globs the examples.

- [ ] **Step 2: Add the invariants**

Append to `docs/coverage/invariants.yml`, under checkpoint:

```yaml
  - id: source.webhook.ack_after_flush
    family: checkpoint
    class: safety
    applies_to: source
    claim: >
      With ack after_flush, a delivery answered 200 is in the sink: the answer
      is sent after the batch holding it flushed and committed, and a failed
      flush answers 503.
    verified_by: harness
    requires: []
    tracked_by: "internal/metering scenario 8 and internal/webhook verify it; no conformance subject yet"

  - id: pipeline.window.replays_lost_state
    family: checkpoint
    class: safety
    applies_to: pipeline
    claim: >
      A windowed Kafka pipeline commits no position past a row a retained
      bucket holds, so a worker that starts without the rows rebuilds each
      bucket whole from Kafka, and refuses what the previous owner finalized.
    verified_by: harness
    requires: []
    tracked_by: "internal/metering scenarios 5 and 11 verify it; no conformance subject yet"

  - id: pipeline.window.partition_owned_single_writer
    family: checkpoint
    class: safety
    applies_to: pipeline
    claim: >
      With partition_owned, a revoked partition's rows are deleted without
      being published before the rebalance completes, so one process writes
      each (bucket, partition) key.
    verified_by: harness
    requires: []
    tracked_by: "internal/metering scenarios 2, 3 and 10 verify it; no conformance subject yet"
```

Run: `make coverage-page`. Per this repo's notes, use it to check the gate, and do not regenerate status files in the sandbox.
Expected: the gate passes; the three invariants render as declared-unenforced.

- [ ] **Step 3: README and CHANGELOG**

README: document `ack`, the Kafka sink `key`, and `partition_owned` beside the existing webhook, Kafka sink and window sections, one short paragraph each, in the CLAUDE.md style. State the low-watermark commit and the replay floor in the window section in two sentences. Neither has a key; both apply to every windowed Kafka pipeline.

CHANGELOG, under Unreleased:

```markdown
### Added
- Webhook source `ack: after_flush`: a 200 means the body reached the sink. A failed flush answers 503.
- Kafka sink `key: <column>`: records are keyed by the column's text.
- Window `partition_owned: true`: a revoked partition's rows are dropped, and the new owner recounts them.

### Fixed
- A windowed Kafka pipeline committed offsets past rows held only in an open window. A worker that lost its disk, or took a partition over in a rebalance, undercounted. It now commits each partition's low watermark and carries each window's closed watermark as commit metadata.
- A partition revoked and later reassigned rewound to the position the process loaded at startup.
```

- [ ] **Step 4: Run everything once more**

```bash
go vet ./... > $TMPDIR/vet.txt 2>&1; echo rc=$?
go test -short -race ./... > $TMPDIR/unit.txt 2>&1; echo rc=$?
perl -e 'alarm shift; exec @ARGV' 3600 go test -run '^TestIntegration' -timeout 60m ./internal/core/ ./internal/kafka/ ./internal/sinks/ ./internal/webhook/ ./internal/metering/ > $TMPDIR/integ.txt 2>&1; echo rc=$?
```

Expected: rc=0 for all three. Run the windowed write-path benchmark before and after the branch:

```bash
git stash -u 2>/dev/null; git checkout fedeb83 -- internal/core/turbine.go
go test -run '^$' -bench 'BenchmarkConsumeLoopWindowedWritePath' -count 5 ./internal/core/ > $TMPDIR/bench-before.txt
git checkout metering/durable-counts -- internal/core/turbine.go; git stash pop 2>/dev/null
go test -run '^$' -bench 'BenchmarkConsumeLoopWindowedWritePath' -count 5 ./internal/core/ > $TMPDIR/bench-after.txt
```

If checking out one file breaks compilation on the old revision, run the "before" benchmark in a separate worktree at `fedeb83` instead. Report ns/op before and after. An increase over 10% is a finding to fix before the PR leaves draft, not a number to explain away.

- [ ] **Step 5: Commit, push and update the PR**

```bash
git add dev/config/examples/metering/ README.md CHANGELOG.md docs/coverage/invariants.yml
git commit -m "docs: the metering examples, the three new keys, and their invariants"
git push origin metering/durable-counts
```

Update PR #414's description with:

- the baseline table from Task 1 and the final table, side by side;
- the rebalance cost from scenario 10;
- the `after_flush` p50 and p99 from scenario 6;
- the benchmark before and after;
- the six spec revisions, linking the spec section.

Leave the PR in draft. Danny decides when it is ready.
