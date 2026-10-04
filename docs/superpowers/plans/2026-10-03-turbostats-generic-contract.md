# TurboStats generic contract Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the runtime-neutral fields to the TurboStats contract, publish the contract as JSON Schema reflected from the Go types, and make SQLFlow send every new field that applies to it.

**Architecture:** The contract is the Go types in `turbostats/wire`. This plan adds fields and two types there, generates `bundle.schema.json` and `response.schema.json` from them with the `internal/schema` pattern the config schemas use, and embeds both in a standard-library-only package that the control plane will serve. The collector in `internal/turbostats` fills the fields SQLFlow can measure; the engine gains two instruments: the last delivered write, and rows dropped under `IGNORE`.

**Tech Stack:** Go, `github.com/invopop/jsonschema` v0.14.0 (generation), `github.com/santhosh-tekuri/jsonschema/v6` (validation), OpenTelemetry metric SDK, pytest and testcontainers for the release test.

**Spec:** `docs/superpowers/specs/2026-10-03-turbostats-kafka-connect-design.md`

**Scope:** This is the first of three plans. It covers the contract, its schema and SQLFlow, in this repository. The Kafka Connect reporter and the control plane get their own plans. The control plane plan serves the embedded schema; this plan only produces it.

## Global Constraints

- Every change is additive. `wire.MediaType` and `wire.Version` do not change.
- `turbostats/wire` and every package under it import only the standard library.
- No field, type or constant names Debezium or Kafka Connect.
- Absent and zero are different facts. A field whose zero is a reading is a pointer or a struct that is always present; a field whose zero would be false is `omitempty`.
- Schema `$id`s, exactly: `https://control.turbolytics.io/v1/turbostats/bundle.schema.json` and `https://control.turbolytics.io/v1/turbostats/response.schema.json`.
- The schema forbids no unknown property and carries no `enum`.
- Go struct literals in new code put each field on its own line.
- Prose follows `CLAUDE.md`: Google Technical Writing One; SQLFlow in prose, `sqlflow` for the command.
- Commit messages name the defect, the fix and the evidence, and say what breaks if the change is wrong. No session links.
- Run `go`, `make`, `docker` and `gh` outside the sandbox.
- A change to the bundle's shape needs `pytest tests/release` (via `make test-release`), never "not required".
- Each task ends with the whole short suite: `go test -short ./...`.

## Review Focus

The five conditions most likely to bite a user that no task's main tests exercise, and where each is pinned:

1. **An engine that predates these fields reports to a receiver that validates with the new schema.** Its bundle must validate. Pinned in Task 8: `TestSchema_AnOlderEnginesBundleValidates`.
2. **An engine newer than the schema adds a field or a state value.** Today's schema must accept it, or a deployed validator rejects a newer engine. Pinned in Task 8: `TestSchema_ANewerEnginesBundleValidates`.
3. **A periodic flush with nothing pending.** It must not move `last_sink_write_at`, or an idle pipeline that delivers nothing reads as fresh. Pinned in Task 5: `TestCounting_AFlushWithNothingPendingIsNotAWrite`.
4. **The final bundle after a fatal error.** It must say `failed`, not `stopped`, or a crash reads as a clean shutdown. Pinned in Task 4: `TestReporter_AFailedExitSaysFailed`.
5. **A process with no `GOMEMLIMIT`.** `heap_limit_bytes` must be absent, not `math.MaxInt64`, or a receiver computes a near-zero heap use and never warns. Pinned in Task 3: `TestGoMemory_NoLimitIsAbsent`.

---

### Task 1: The wire types

**Files:**
- Modify: `turbostats/wire/bundle.go`
- Modify: `internal/turbostats/bundle.go` (aliases)
- Modify: `internal/turbostats/dimensional_test.go:436-487` (widest bundle)
- Test: `turbostats/wire/bundle_test.go`

**Interfaces:**
- Produces:
  - `wire.Instance` fields `Runtime`, `RuntimeVersion`, `ReporterVersion string`.
  - `wire.Process` fields `Host string`, `MemoryLimitBytes int64`, `Memory *Memory`; `Goroutines` becomes `omitempty`.
  - `wire.Memory{Runtime string; RetainedBytes, LiveBytes, HeapLimitBytes, GCCount int64}`.
  - `wire.Pipeline` fields `State string`, `StartedAt *time.Time`, `RestartCount *int64`, `SourceConnected *bool`, `LastSinkWriteAt *time.Time`, `ErrorRowsDropped *int64`, `SourceWireBytes *int64`, `SinkWireBytes *int64`, `Backfill *Backfill`.
  - `wire.Backfill{State string; BlocksStream bool; ElapsedSeconds *int64; Unit string; UnitsTotal, UnitsLeft *int; RowsRead *int64}`.
  - Constants `wire.RuntimeSQLFlow = "sqlflow"`, `wire.MemoryRuntimeGo = "go"`, `wire.StateRunning = "running"`, `wire.StateStopped = "stopped"`, `wire.StateFailed = "failed"`, `wire.BackfillNone = "none"`.
  - Test helper `widestBundle(t *testing.T) wire.Bundle` in `internal/turbostats/dimensional_test.go`.

- [ ] **Step 1: Write the failing tests**

Append to `turbostats/wire/bundle_test.go`:

```go
// An engine that predates the generic fields sends none of them, and a
// reader must see unknown, not a pipeline that never restarted, never
// dropped a row and has nothing to backfill.
func TestPipeline_AnOlderEngineDecodesAsUnknown(t *testing.T) {
	var b Bundle
	if err := json.Unmarshal([]byte(`{"v":1,"pipeline":{"message_count":1}}`), &b); err != nil {
		t.Fatal(err)
	}
	p := b.Pipeline
	if p.State != "" || p.StartedAt != nil || p.RestartCount != nil ||
		p.SourceConnected != nil || p.LastSinkWriteAt != nil ||
		p.ErrorRowsDropped != nil || p.SourceWireBytes != nil ||
		p.SinkWireBytes != nil || p.Backfill != nil {
		t.Fatalf("an older engine's pipeline decoded with readings: %+v", p)
	}
}

// Zero restarts and a lost source are readings. Both must reach the wire.
func TestPipeline_ZeroRestartsAndALostSourceArePresent(t *testing.T) {
	zero := int64(0)
	lost := false
	raw := mustMarshal(t, Pipeline{
		RestartCount:    &zero,
		SourceConnected: &lost,
	})
	for _, want := range []string{`"restart_count":0`, `"source_connected":false`} {
		if !strings.Contains(raw, want) {
			t.Errorf("want %s in %s", want, raw)
		}
	}
}

// A source that can backfill but has not says so, with nothing else: no
// progress exists yet, and zeros would claim some.
func TestBackfill_NoneCarriesOnlyItsState(t *testing.T) {
	got := mustMarshal(t, Backfill{State: BackfillNone})
	if got != `{"state":"none","blocks_stream":false}` {
		t.Fatalf("a backfill that has not run is %s", got)
	}
}

// A JVM has no goroutines. Absent says so; zero would be false.
func TestProcess_GoroutinesIsOmittable(t *testing.T) {
	if raw := mustMarshal(t, Process{}); strings.Contains(raw, "goroutines") {
		t.Fatalf("a process with no goroutines sends the field: %s", raw)
	}
	if raw := mustMarshal(t, Process{Goroutines: 7}); !strings.Contains(raw, `"goroutines":7`) {
		t.Fatalf("a Go process lost its goroutines: %s", raw)
	}
}

// A runtime that has not collected yet has collected zero times. gc_count
// is a reading from the first report.
func TestMemory_ZeroCollectionsArePresent(t *testing.T) {
	got := mustMarshal(t, Memory{Runtime: "jvm"})
	if got != `{"runtime":"jvm","gc_count":0}` {
		t.Fatalf("a fresh runtime's memory is %s", got)
	}
}

// The runtime fields are absent from an engine that does not set them.
func TestInstance_RuntimeFieldsAreAbsentUntilSet(t *testing.T) {
	without := mustMarshal(t, Instance{ID: "gw-1"})
	for _, absent := range []string{"runtime", "reporter_version"} {
		if strings.Contains(without, absent) {
			t.Errorf("an unset %s is present: %s", absent, without)
		}
	}
	with := mustMarshal(t, Instance{
		ID:              "prod-connect/inventory-cdc/0",
		Runtime:         "kafka-connect",
		RuntimeVersion:  "3.8.0",
		ReporterVersion: "0.1.0",
	})
	for _, want := range []string{`"runtime":"kafka-connect"`,
		`"runtime_version":"3.8.0"`, `"reporter_version":"0.1.0"`} {
		if !strings.Contains(with, want) {
			t.Errorf("want %s in %s", want, with)
		}
	}
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test ./turbostats/wire/ -run 'TestPipeline_AnOlderEngine|TestPipeline_ZeroRestarts|TestBackfill_|TestProcess_Goroutines|TestMemory_|TestInstance_Runtime'`
Expected: FAIL to compile with `unknown field RestartCount` and `undefined: Backfill`.

- [ ] **Step 3: Add the fields and types**

In `turbostats/wire/bundle.go`, replace the comments on `Instance.ID` and `Instance.Name` with:

```go
	// ID names one stream of reports, and the reporter guarantees it is
	// unique within its org. Two streams under one ID are a reporter bug: a
	// receiver flags the collision and never namespaces the ID, because only
	// the reporter knows what makes it unique. A Kafka Connect task reports
	// as <cluster>/<connector>/<task>. Empty until a reporter is configured,
	// which requires it.
```

```go
	// Name is the logical pipeline. Instances that share a name are parts of
	// one pipeline, such as a connector's tasks or a pipeline's replicas. It
	// is pipeline.name under run and serve.name under serve.
```

Then, in `Instance`, after `HandlerType`:

```go
	// Runtime is the engine that runs the pipeline: sqlflow, or
	// kafka-connect for a connector the Kafka Connect reporter describes.
	// It is an open vocabulary: a reader that meets a name it does not know
	// keeps the report. Absent from engines that predate the field.
	Runtime string `json:"runtime,omitempty"`
	// RuntimeVersion is that engine's version, when Version names something
	// else. A Kafka Connect connector sends its plugin's version as Version
	// and Kafka's here. SQLFlow sends none: Version already says it.
	RuntimeVersion string `json:"runtime_version,omitempty"`
	// ReporterVersion is the reporter's own version, when the reporter is
	// not the engine. SQLFlow reports itself and sends none.
	ReporterVersion string `json:"reporter_version,omitempty"`
```

In `Process`, after `ID`:

```go
	// Host is where this process runs, in the operator's terms: the
	// hostname, or a Kafka Connect worker's ID. A receiver shows it to say
	// where a stream ran and where it moved. It identifies nothing, since
	// two containers can share a hostname; ID does that. Absent when the
	// host would not say.
	Host string `json:"host,omitempty"`
```

Replace the `Goroutines` line with:

```go
	// Goroutines is absent from a runtime that has none, such as a JVM. A
	// live Go process always runs at least one, so zero cannot be confused
	// with absent, as with RSSBytes.
	Goroutines int `json:"goroutines,omitempty"`
	// MemoryLimitBytes is the container's memory limit, from the cgroup.
	// Absent when no limit is set or the process cannot read it. RSSBytes
	// approaching it predicts the kernel killing the process.
	MemoryLimitBytes int64 `json:"memory_limit_bytes,omitempty"`
	// Memory is the managed runtime's memory, in terms every garbage
	// collected runtime shares. Absent from engines that predate it.
	Memory *Memory `json:"memory,omitempty"`
```

After the `Process` type, add:

```go
// Memory is a garbage-collected runtime's view of its own memory.
//
// It exists so a receiver judges a Go process and a JVM with one rule. On
// Go, RetainedBytes and LiveBytes copy GoRetainedBytes and GoHeapBytes, the
// way last_activity_at copies its section timestamps; the Go fields stay the
// source.
//
// LiveBytes is the leak signal. Heap in use rises and falls with every
// collection; what a collection finds live rising across hours is a leak.
// RSSBytes less RetainedBytes is memory the runtime cannot see: DuckDB and
// Arrow on SQLFlow, direct buffers on a JVM.
//
// GC time is not here. A JVM reports wall-clock pause time and Go reports
// CPU time, and one field would mean two things.
type Memory struct {
	// Runtime is "go" or "jvm", an open vocabulary.
	Runtime string `json:"runtime"`
	// RetainedBytes is what the runtime holds from the operating system:
	// Go's mapped memory less released heap pages, or a JVM's committed
	// heap and non-heap.
	RetainedBytes int64 `json:"retained_bytes,omitempty"`
	// LiveBytes is what the last collection found live. Absent before the
	// first collection.
	LiveBytes int64 `json:"live_bytes,omitempty"`
	// HeapLimitBytes is the runtime's heap ceiling: GOMEMLIMIT or -Xmx.
	// Absent when unlimited. LiveBytes approaching it predicts an
	// out-of-memory failure.
	HeapLimitBytes int64 `json:"heap_limit_bytes,omitempty"`
	// GCCount is collections since the process started, a counter. Compare
	// it only within one process: collectors count cycles differently, so
	// its rate means something and its value across runtimes does not.
	GCCount int64 `json:"gc_count"`
}

// The runtime and memory-runtime names SQLFlow sends.
const (
	RuntimeSQLFlow  = "sqlflow"
	MemoryRuntimeGo = "go"
)

// Pipeline states. A reader treats a value it does not know as unknown,
// never as running.
const (
	StateRunning = "running"
	StateStopped = "stopped"
	StateFailed  = "failed"
)

// BackfillNone is the state of a source that can backfill and is not.
const BackfillNone = "none"

// Backfill is bounded work inside an unbounded pipeline: a CDC snapshot, a
// replay from the earliest offset, a rebuild of state. It is not a pipeline;
// it has no source, sink or identity of its own.
//
// It is present for a source that can backfill, from the first report to
// the last, with State none until one runs, so a process's field set never
// changes. A completed backfill keeps its final values until the next one
// starts.
//
// Its rows count toward the pipeline's totals as well: phase splits one
// measurement, so RowsRead is message_count's backfill share. They never
// enter event lag, because a backfilled row's age says when it was last
// written, not how far behind the stream the pipeline runs.
type Backfill struct {
	// State is none, running, paused, completed or aborted.
	State string `json:"state"`
	// BlocksStream says the stream waits for this backfill: true for an
	// initial or blocking snapshot, false for one interleaved with the
	// stream.
	BlocksStream bool `json:"blocks_stream"`
	// ElapsedSeconds is how long the current or last backfill has run,
	// paused time included.
	ElapsedSeconds *int64 `json:"elapsed_seconds,omitempty"`
	// Unit is what UnitsTotal and UnitsLeft count: table, partition.
	Unit string `json:"unit,omitempty"`
	// UnitsTotal and UnitsLeft follow the rollup daemon's tables_left: both
	// count what remains, so progress reads the same way in both places.
	UnitsTotal *int `json:"units_total,omitempty"`
	UnitsLeft  *int `json:"units_left,omitempty"`
	// RowsRead is rows read, summed over units. A source may update it in
	// steps, so a receiver's stall threshold exceeds one step.
	RowsRead *int64 `json:"rows_read,omitempty"`
}
```

Replace the comment line `// Pipeline carries the consume loop's totals since Process.StartedAt.` with:

```go
// Pipeline carries the consume loop's totals since Pipeline.StartedAt. On
// SQLFlow that equals Process.StartedAt; a runtime that restarts a pipeline
// inside a running process moves it, and its counters restart with it.
```

In `Pipeline`, in the `EventLagBasis` comment, after the sentence that lists `arrival`, add: `source_commit_time, the source database's commit time of the change, to processing;`.

In `Pipeline`, before `MessageCount`, add:

```go
	// State is starting, running, paused, stopped or failed. It is the one
	// field that can say a pipeline failed while its process stays healthy.
	// SQLFlow sends running while it reports, and stopped or failed in its
	// final bundle. Absent from engines that predate it.
	State string `json:"state,omitempty"`
	// StartedAt is the epoch of every pipeline counter. A receiver
	// subtracts two reports only when their StartedAt agrees.
	StartedAt *time.Time `json:"started_at,omitempty"`
	// RestartCount counts pipeline starts after the first since the
	// process started. A counter, because a pipeline can restart several
	// times between two reports and StartedAt shows only the last.
	RestartCount *int64 `json:"restart_count,omitempty"`
	// SourceConnected says whether the source holds its connection now.
	// Absent when the source cannot tell. It separates a quiet source from
	// a lost one, which LastMessageAt alone cannot.
	SourceConnected *bool `json:"source_connected,omitempty"`
	// LastSinkWriteAt is the last write the destination acknowledged:
	// output freshness, where LastMessageAt is input. A pipeline that reads
	// and cannot write shows the first fresh and this stale. Absent until
	// the first acknowledged write.
	LastSinkWriteAt *time.Time `json:"last_sink_write_at,omitempty"`
	// ErrorRowsDropped is rows discarded under a tolerant error policy,
	// neither delivered nor diverted to a DLQ. It is silent data loss, like
	// LateRowsDropped, and is never summed with another outcome. An engine
	// that counts it always sends it, zero included.
	ErrorRowsDropped *int64 `json:"error_rows_dropped,omitempty"`
	// SourceWireBytes and SinkWireBytes are bytes on the network, framing
	// and compression included, where MessagePayloadBytes is payload. Each
	// is absent when the client cannot report it.
	SourceWireBytes *int64 `json:"source_wire_bytes,omitempty"`
	SinkWireBytes   *int64 `json:"sink_wire_bytes,omitempty"`
	// Backfill is present for a source that can backfill. See Backfill.
	Backfill *Backfill `json:"backfill,omitempty"`
```

In `internal/turbostats/bundle.go`, add to the alias block:

```go
	Memory   = wire.Memory
	Backfill = wire.Backfill
```

- [ ] **Step 4: Run the wire tests**

Run: `go test ./turbostats/wire/`
Expected: PASS.

- [ ] **Step 5: Extend the widest-bundle guard**

In `internal/turbostats/dimensional_test.go`, move the bundle literal out of `TestCollect_AFullBundleStaysUnderTheCeiling` into a helper, and add every field the literal missed, the new ones and the event lag, Go memory, process ID and uptime fields:

```go
// widestBundle is a bundle with every field at its widest value. The size
// guard prices it, and the schema test validates it, so a field added to
// the contract and forgotten here fails neither.
func widestBundle(t *testing.T) wire.Bundle {
	t.Helper()
	big := int64(1) << 62
	n := 1 << 30
	at := time.Now().UTC()
	// The longest code in the taxonomy is shorter than this; a code is a
	// bounded string and this is the widest one worth pricing.
	code := "system.internal.unexpected"
	secs := 1e9
	basis := "kafka_log_append_time"
	connected := true
	return wire.Bundle{
		V:               wire.Version,
		SentAt:          at,
		IntervalSeconds: 86400,
		LastActivityAt:  &at,
		IdleSeconds:     &big,
		Instance: wire.Instance{
			ID:              strings.Repeat("i", 64),
			Name:            strings.Repeat("n", 64),
			Version:         "v2026.09.21.12",
			Commit:          strings.Repeat("c", 40),
			Arch:            "linux/arm64",
			ConfigHash:      "sha256:" + strings.Repeat("f", 64),
			SourceType:      "websocket",
			SinkType:        "clickhouse",
			HandlerType:     "inferred_disk",
			Runtime:         "kafka-connect",
			RuntimeVersion:  "3.8.0-ccs",
			ReporterVersion: "v2026.10.03.12",
			Labels:          widestLabels(t),
		},
		Process: wire.Process{
			ID:               strings.Repeat("a", 32),
			Host:             strings.Repeat("h", 64),
			StartedAt:        at,
			UptimeSeconds:    &big,
			RSSBytes:         big,
			Goroutines:       n,
			GoRetainedBytes:  big,
			GoHeapBytes:      big,
			MemoryLimitBytes: big,
			Memory: &wire.Memory{
				Runtime:        "jvm",
				RetainedBytes:  big,
				LiveBytes:      big,
				HeapLimitBytes: big,
				GCCount:        big,
			},
		},
		Pipeline: &wire.Pipeline{
			State:                "starting",
			StartedAt:            &at,
			RestartCount:         &big,
			SourceConnected:      &connected,
			MessageCount:         big,
			MessagePayloadBytes:  &big,
			HandlerRowsRead:      big,
			ErrorCount:           big,
			SinkFlushCount:       big,
			SinkRowsAccepted:     big,
			SinkRowsWritten:      big,
			StateCommitCount:     big,
			StateDBSizeBytes:     &big,
			LastMessageAt:        &at,
			LastSinkWriteAt:      &at,
			ErrorRowsDropped:     &big,
			SourceWireBytes:      &big,
			SinkWireBytes:        &big,
			SinkRetryCount:       &big,
			LagMaxMessages:       &big,
			LagTotalMessages:     &big,
			LagPartitions:        &n,
			LagObservedAt:        &at,
			LateRowsDropped:      &big,
			LateRowsRecomputed:   &big,
			WindowClosedCount:    &big,
			WindowLagSeconds:     &big,
			WindowNewestBucketAt: &at,
			SourceErrorCount:     &big,
			HandlerErrorCount:    &big,
			SinkErrorCount:       &big,
			StateErrorCount:      &big,
			DLQRows:              &big,
			LastErrorCode:        &code,
			LastErrorAt:          &at,
			RecvWaitSeconds:      &secs,
			EventLagSeconds:      &secs,
			EventLagMaxSeconds:   &secs,
			EventLagObservedAt:   &at,
			EventLagBasis:        &basis,
			Duration: &wire.PipelineDurations{
				Batch:     widestDuration(),
				SinkFlush: widestDuration(),
			},
			Backfill: &wire.Backfill{
				State:          "completed",
				BlocksStream:   true,
				ElapsedSeconds: &big,
				Unit:           "partition",
				UnitsTotal:     &n,
				UnitsLeft:      &n,
				RowsRead:       &big,
			},
		},
		Serve: &wire.Serve{
			RequestCount:      big,
			RequestErrorCount: big,
			SessionsInUse:     n,
			SessionsTotal:     n,
			LastRequestAt:     &at,
			Cache: &wire.ServeCache{
				HitCount:      big,
				MissCount:     big,
				SharedCount:   big,
				EvictionCount: big,
				Bytes:         big,
				Entries:       n,
			},
			Duration: &wire.ServeDurations{Request: widestDuration()},
		},
		Exit: &wire.Exit{
			Reason: "system.internal.unexpected",
			Code:   255,
		},
	}
}
```

Then reduce the test body to:

```go
func TestCollect_AFullBundleStaysUnderTheCeiling(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	raw, err := json.Marshal(widestBundle(t))
	assert.NoError(t, err)
	t.Logf("a bundle with every field at its widest is %d bytes", len(raw))
	assert.That(t, len(raw) < 8<<10)
}
```

- [ ] **Step 6: Run the guards**

Run: `go test ./internal/turbostats/ -run 'TestWire_NoFieldScalesWithCardinality|TestCollect_AFullBundleStaysUnderTheCeiling' -v`
Expected: PASS. The log line reports more than the 4,221 bytes it reported before, and under 8,192: the guard now prices fields it missed. `TestWire_NoFieldScalesWithCardinality` passes without a new exemption, because `Memory` and `Backfill` hold no map or slice.

- [ ] **Step 7: Run the short suite and commit**

Run: `go test -short ./...`
Expected: PASS.

```bash
git add turbostats/wire/bundle.go turbostats/wire/bundle_test.go internal/turbostats/bundle.go internal/turbostats/dimensional_test.go
git commit -m "wire: the contract describes a pipeline's lifetime, backfill and memory for any runtime

A failed Kafka Connect task on a live worker reads as up: nothing in the
bundle can say a pipeline failed while its process is healthy. pipeline.state,
a pipeline counter epoch and a restart counter say it. Backfill, output
freshness, error-policy drops, wire bytes and a runtime-neutral memory object
complete the generic fields; goroutines becomes omittable so a JVM need not
send a false zero.

Every field is additive and the document stays v1. The widest-bundle guard
now prices every field, including the event lag and Go memory fields it
missed, so a forgotten field fails it.

If a pointer field became a value, an older engine would decode as a
pipeline with zero restarts and nothing dropped."
```

---

### Task 2: The JSON Schema

**Files:**
- Create: `turbostats/wire/schema/schema.go`
- Create: `turbostats/wire/schema/bundle.schema.json`, `turbostats/wire/schema/response.schema.json` (generated)
- Create: `internal/schema/turbostats.go`
- Create: `internal/schema/turbostats_test.go`
- Modify: `Makefile:93-104` (`schema` target)
- Modify: `docs/superpowers/specs/2026-10-03-turbostats-kafka-connect-design.md` (file names, vectors claim)

**Interfaces:**
- Consumes: the `wire` types from Task 1.
- Produces:
  - Package `github.com/turbolytics/sql-flow/turbostats/wire/schema` with `const BundleID, ResponseID string` and `var Bundle, Response []byte`.
  - `schema.GenerateTurboStatsBundle(wireDir string) ([]byte, error)` and `schema.GenerateTurboStatsResponse(wireDir string) ([]byte, error)` in `internal/schema`.

- [ ] **Step 1: Create the embed package with placeholder files**

The package must compile before the generator can write into it, so the JSON files start as empty schemas.

```bash
mkdir -p turbostats/wire/schema
printf '{}\n' > turbostats/wire/schema/bundle.schema.json
printf '{}\n' > turbostats/wire/schema/response.schema.json
```

Create `turbostats/wire/schema/schema.go`:

```go
// Package schema is the TurboStats contract as JSON Schema, for a reporter
// or receiver that is not written in Go.
//
// The documents are generated from the types in turbostats/wire by `make
// schema`, and a golden test fails when they fall behind. The Go types are
// the contract; these are an artifact of them.
//
// The control plane serves them at their IDs, from the version of this
// module it builds against, so the schema it serves is the schema of what
// it accepts. Like wire, this package imports only the standard library.
//
// The schema checks shape. It cannot check the contract's rules about how a
// process's reports relate over time: that a field set never changes, that
// counters only rise within one pipeline.started_at, that backfill is
// present from the first report. Those live in the specs and the tests.
package schema

import _ "embed"

// The IDs are the URLs the documents are published at. They carry the
// contract's version, as the media type and the ingest route do, so a v2
// contract gets new URLs and v1's stay.
const (
	BundleID   = "https://control.turbolytics.io/v1/turbostats/bundle.schema.json"
	ResponseID = "https://control.turbolytics.io/v1/turbostats/response.schema.json"
)

// Bundle is the schema of one report.
//
//go:embed bundle.schema.json
var Bundle []byte

// Response is the schema of the body of every 2xx answer to a report.
//
//go:embed response.schema.json
var Response []byte
```

- [ ] **Step 2: Write the failing tests**

Create `internal/schema/turbostats_test.go`:

```go
package schema

import (
	"bytes"
	"encoding/json"
	"os"
	"testing"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/turbolytics/sql-flow/turbostats/wire"
	tsschema "github.com/turbolytics/sql-flow/turbostats/wire/schema"
	"github.com/zeebo/assert"
)

// wireDir is where the reflector reads the contract's doc comments.
const wireDir = "../../turbostats/wire"

const (
	bundleGoldenPath   = "../../turbostats/wire/schema/bundle.schema.json"
	responseGoldenPath = "../../turbostats/wire/schema/response.schema.json"
)

// The TurboStats schemas are generated, so the committed files are build
// artifacts and these are their golden tests. Regenerate with `make schema`.
func TestTurboStatsSchema_CommittedFilesMatchTheTypes(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	for _, tt := range []struct {
		path string
		gen  func(string) ([]byte, error)
	}{
		{bundleGoldenPath, GenerateTurboStatsBundle},
		{responseGoldenPath, GenerateTurboStatsResponse},
	} {
		generated, err := tt.gen(wireDir)
		assert.NoError(t, err)
		if os.Getenv("UPDATE_GOLDEN") == "1" {
			assert.NoError(t, os.WriteFile(tt.path, generated, 0o644))
			continue
		}
		committed, err := os.ReadFile(tt.path)
		assert.NoError(t, err)
		if !bytes.Equal(committed, generated) {
			t.Fatalf("%s is stale. Run `make schema`.", tt.path)
		}
	}
}

// The embedded documents are the committed files. A package that embedded
// something else would serve a schema no test checked.
func TestTurboStatsSchema_TheEmbeddedFilesAreTheCommittedOnes(t *testing.T) {
	committed, err := os.ReadFile(bundleGoldenPath)
	assert.NoError(t, err)
	assert.That(t, bytes.Equal(committed, tsschema.Bundle))
	committed, err = os.ReadFile(responseGoldenPath)
	assert.NoError(t, err)
	assert.That(t, bytes.Equal(committed, tsschema.Response))
}

// Every reader ignores unknown sections and fields, and a new one is an
// additive v1 change. A schema that forbade one would make a deployed
// validator reject a newer engine.
func TestTurboStatsSchema_NoObjectForbidsUnknownFields(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	for _, doc := range turboStatsDocs(t) {
		walkJSON(doc, func(node map[string]any) {
			if v, ok := node["additionalProperties"]; ok && v == false {
				t.Errorf("an object forbids unknown fields: %v", node)
			}
		})
	}
}

// A new state, basis or runtime name is an additive v1 change. An enum
// would make an old validator reject it.
func TestTurboStatsSchema_CarriesNoEnum(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	for _, doc := range turboStatsDocs(t) {
		walkJSON(doc, func(node map[string]any) {
			if _, ok := node["enum"]; ok {
				t.Errorf("a field carries an enum: %v", node)
			}
		})
	}
}

// Buckets are summed across a fleet, which works only while every process
// sends the same number of them. The count comes from the code.
func TestTurboStatsSchema_BucketsHaveTheContractsLength(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	want := float64(len(wire.DurationBounds) + 1)
	found := 0
	walkJSON(turboStatsDocs(t)[0], func(node map[string]any) {
		props, ok := node["properties"].(map[string]any)
		if !ok {
			return
		}
		buckets, ok := props["buckets"].(map[string]any)
		if !ok {
			return
		}
		found++
		assert.Equal(t, want, buckets["minItems"])
		assert.Equal(t, want, buckets["maxItems"])
	})
	// Batch, sink flush and serve request.
	assert.Equal(t, 3, found)
}

// required follows the json tags. A field without omitempty is required,
// and goroutines stopped being required so a JVM need not send it.
func TestTurboStatsSchema_RequiredFollowsTheTags(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	doc := turboStatsDocs(t)[0]
	assert.DeepEqual(t, []any{"v", "sent_at", "instance", "process"}, doc["required"])
	process := doc["properties"].(map[string]any)["process"].(map[string]any)
	assert.DeepEqual(t, []any{"started_at"}, process["required"])
}

// turboStatsDocs is the bundle schema, then the response schema, decoded.
func turboStatsDocs(t *testing.T) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, gen := range []func(string) ([]byte, error){GenerateTurboStatsBundle, GenerateTurboStatsResponse} {
		raw, err := gen(wireDir)
		assert.NoError(t, err)
		var doc map[string]any
		assert.NoError(t, json.Unmarshal(raw, &doc))
		out = append(out, doc)
	}
	return out
}

// walkJSON calls fn on every object in a decoded JSON document.
func walkJSON(v any, fn func(map[string]any)) {
	switch node := v.(type) {
	case map[string]any:
		fn(node)
		for _, child := range node {
			walkJSON(child, fn)
		}
	case []any:
		for _, child := range node {
			walkJSON(child, fn)
		}
	}
}
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `go test ./internal/schema/ -run TestTurboStatsSchema`
Expected: FAIL to compile with `undefined: GenerateTurboStatsBundle`.

- [ ] **Step 4: Write the generator**

Create `internal/schema/turbostats.go`:

```go
package schema

import (
	"fmt"

	"github.com/invopop/jsonschema"
	"github.com/turbolytics/sql-flow/turbostats/wire"
	tsschema "github.com/turbolytics/sql-flow/turbostats/wire/schema"
)

// wirePkg is the import path the reflector keys the contract's doc
// comments by.
const wirePkg = "github.com/turbolytics/sql-flow/turbostats/wire"

// GenerateTurboStatsBundle reflects wire.Bundle into the bundle schema.
//
// wireDir is the path to turbostats/wire, which the reflector reads doc
// comments from: the contract's prose becomes the schema's descriptions.
func GenerateTurboStatsBundle(wireDir string) ([]byte, error) {
	return generateWire(wireDir, &wire.Bundle{}, tsschema.BundleID)
}

// GenerateTurboStatsResponse reflects wire.Response into the response
// schema.
func GenerateTurboStatsResponse(wireDir string) ([]byte, error) {
	return generateWire(wireDir, &wire.Response{}, tsschema.ResponseID)
}

func generateWire(wireDir string, v any, id string) ([]byte, error) {
	r := &jsonschema.Reflector{
		// Every reader ignores unknown sections and fields. The config
		// schemas reject unknown keys; this one must not, or a validator
		// deployed today rejects the engine released tomorrow.
		AllowAdditionalProperties: true,
		// One document, read by people and by reporters in other languages.
		ExpandedStruct: true,
		DoNotReference: true,
	}
	if err := r.AddGoComments(wirePkg, wireDir); err != nil {
		return nil, fmt.Errorf("reading contract doc comments from %s: %w", wireDir, err)
	}
	s := r.Reflect(v)
	s.ID = jsonschema.ID(id)
	s.Version = "https://json-schema.org/draft/2020-12/schema"
	pinBuckets(s)
	return encode(s)
}

// pinBuckets fixes every duration's buckets at the contract's length.
//
// The length is read from wire.DurationBounds rather than written here, the
// way the config generator reads its enums from the registries. A copied
// number would drift the day a boundary is added.
func pinBuckets(s *jsonschema.Schema) {
	n := uint64(len(wire.DurationBounds) + 1)
	walkSchema(s, func(node *jsonschema.Schema) {
		if node.Properties == nil {
			return
		}
		b, ok := node.Properties.Get("buckets")
		if !ok {
			return
		}
		if _, isDuration := node.Properties.Get("sum_seconds"); !isDuration {
			return
		}
		b.MinItems = &n
		b.MaxItems = &n
	})
}

// walkSchema calls fn on s and every schema beneath it.
func walkSchema(s *jsonschema.Schema, fn func(*jsonschema.Schema)) {
	if s == nil {
		return
	}
	fn(s)
	if s.Properties != nil {
		for p := s.Properties.Oldest(); p != nil; p = p.Next() {
			walkSchema(p.Value, fn)
		}
	}
	walkSchema(s.Items, fn)
	walkSchema(s.AdditionalProperties, fn)
}
```

The ordered map is `github.com/pb33f/ordered-map/v2`, whose iteration is `Oldest()` and `Next()`. If the compiler rejects that, read `$(go env GOMODCACHE)/github.com/pb33f/ordered-map/v2@*/orderedmap.go` for the iteration method and use it.

- [ ] **Step 5: Generate the files and run the tests**

Run: `UPDATE_GOLDEN=1 go test ./internal/schema/ -run TestTurboStatsSchema_CommittedFilesMatchTheTypes && go test ./internal/schema/ -run TestTurboStatsSchema -v`
Expected: PASS for all six tests. Open `turbostats/wire/schema/bundle.schema.json` and confirm `"$id"` is the bundle URL, `commands` is `{"items": true, "type": "array"}` (invopop renders `json.RawMessage` as any value; checked on 2026-10-03), and `sent_at` has `"format": "date-time"`.

- [ ] **Step 6: Mutation check**

Set `AllowAdditionalProperties: false` in `generateWire` and run `go test ./internal/schema/ -run TestTurboStatsSchema_NoObjectForbidsUnknownFields`. Expected: FAIL. Restore it. Then change `uint64(len(wire.DurationBounds) + 1)` to `uint64(len(wire.DurationBounds))` and run `go test ./internal/schema/ -run TestTurboStatsSchema_BucketsHaveTheContractsLength`. Expected: FAIL. Restore it.

- [ ] **Step 7: Add the generator to `make schema`**

In `Makefile`, in the `schema` target, change the first `UPDATE_GOLDEN=1 go test ./internal/schema/ -run '...'` line's pattern to add `|TestTurboStatsSchema_CommittedFilesMatchTheTypes`, and add an echo line:

```make
	@echo "regenerated turbostats/wire/schema/bundle.schema.json and response.schema.json"
```

Change the target's leading comment from `# Regenerates the config JSON Schema from the Go types.` to `# Regenerates the config and TurboStats JSON Schemas from the Go types.`

Run: `make schema && git status --short`
Expected: no change to any committed schema.

- [ ] **Step 8: Correct the spec**

In the spec's "JSON Schema" section, replace `turbostats/wire/schema/bundle.json` and `response.json` with `bundle.schema.json` and `response.schema.json`, so the file names match their URLs. In "What validates against it", replace the first bullet with:

```markdown
- The contract's Go tests validate every bundle the engine builds, the
  widest bundle the contract allows, an older engine's bundle and a newer
  engine's bundle. `testdata/vectors.json` is not one of them: its body is
  `{"v":1}`, a signing vector rather than a bundle, and the schema rejects it.
```

In the spec's Testing section, replace `validation of \`vectors.json\` and a collected bundle against the schema` with `validation of every bundle the engine builds against the schema`.

- [ ] **Step 9: Run the short suite and commit**

Run: `go test -short ./...`
Expected: PASS.

```bash
git add turbostats/wire/schema internal/schema/turbostats.go internal/schema/turbostats_test.go Makefile docs/superpowers/specs/2026-10-03-turbostats-kafka-connect-design.md
git commit -m "schema: the TurboStats contract is published as JSON Schema reflected from the wire types

A reporter not written in Go had no way to check its bundles against the
contract. The bundle and response schemas are now generated from the wire
types, embedded in turbostats/wire/schema for the control plane to serve,
and held to the types by a golden test, as the config schemas are.

The schema forbids no unknown field and carries no enum, because both are
additive v1 changes; tests fail if either appears. Bucket length comes from
wire.DurationBounds. A mutation of each rule fails its test.

If the schema forbade unknown fields, every deployed validator would reject
the next engine that adds one."
```

---

### Task 3: SQLFlow's process fields

**Files:**
- Modify: `internal/turbostats/gomem.go`
- Create: `internal/turbostats/cgroup.go`
- Create: `internal/turbostats/host.go`
- Modify: `internal/turbostats/collect.go:35-65`
- Test: `internal/turbostats/cgroup_test.go`, `internal/turbostats/collect_test.go`

**Interfaces:**
- Consumes: `wire.Memory`, `wire.RuntimeSQLFlow`, `wire.MemoryRuntimeGo` from Task 1.
- Produces:
  - `turbostats.GoMem{Retained, HeapLive, GCCycles, HeapLimit int64}` and `turbostats.GoMemory() GoMem` (replaces the two-value form).
  - `turbostats.MemoryLimit(root string) (int64, bool)` and `const cgroupRoot = "/sys/fs/cgroup"`.
  - `hostname() string` (package-private).

- [ ] **Step 1: Write the failing tests**

Create `internal/turbostats/cgroup_test.go`:

```go
package turbostats

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

func writeCgroup(t *testing.T, rel, content string) string {
	t.Helper()
	root := t.TempDir()
	path := filepath.Join(root, rel)
	assert.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
	assert.NoError(t, os.WriteFile(path, []byte(content), 0o644))
	return root
}

// A container's limit predicts the kernel killing it, and no limit is not a
// limit of zero.
func TestMemoryLimit_ReadsCgroupV2AndV1(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	for name, tt := range map[string]struct {
		rel, content string
		want         int64
		ok           bool
	}{
		"v2 limit":     {"memory.max", "536870912\n", 536870912, true},
		"v2 unlimited": {"memory.max", "max\n", 0, false},
		"v1 limit":     {"memory/memory.limit_in_bytes", "536870912\n", 536870912, true},
		// cgroup v1 spells "unlimited" as the largest page-aligned int64.
		"v1 unlimited": {"memory/memory.limit_in_bytes", "9223372036854771712\n", 0, false},
		"garbage":      {"memory.max", "lots\n", 0, false},
	} {
		t.Run(name, func(t *testing.T) {
			got, ok := MemoryLimit(writeCgroup(t, tt.rel, tt.content))
			assert.Equal(t, tt.ok, ok)
			assert.Equal(t, tt.want, got)
		})
	}
}

// A host with no cgroup filesystem, such as a laptop, has no limit to read.
func TestMemoryLimit_NoCgroupIsNoLimit(t *testing.T) {
	_, ok := MemoryLimit(t.TempDir())
	assert.That(t, !ok)
}
```

Append to `internal/turbostats/collect_test.go` (add `"os"` and `"runtime/debug"` to its imports):

```go
// Every SQLFlow bundle says which engine sent it and where it runs, and its
// memory object copies the Go fields, so a receiver reads one rule for Go
// and for a JVM.
func TestCollect_CarriesTheRuntimeHostAndMemory(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	reader, _, _ := provider(t)
	runtime.GC()

	b, err := Collect(context.Background(), runSource(reader, nil))
	assert.NoError(t, err)

	assert.Equal(t, wire.RuntimeSQLFlow, b.Instance.Runtime)
	want, err := os.Hostname()
	assert.NoError(t, err)
	assert.Equal(t, want, b.Process.Host)

	m := b.Process.Memory
	assert.That(t, m != nil)
	assert.Equal(t, wire.MemoryRuntimeGo, m.Runtime)
	assert.Equal(t, b.Process.GoRetainedBytes, m.RetainedBytes)
	assert.Equal(t, b.Process.GoHeapBytes, m.LiveBytes)
	assert.That(t, m.GCCount > 0)
}

// GOMEMLIMIT reaches the bundle as the heap ceiling.
func TestGoMemory_ReadsTheHeapLimit(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	old := debug.SetMemoryLimit(512 << 20)
	defer debug.SetMemoryLimit(old)
	assert.Equal(t, int64(512<<20), GoMemory().HeapLimit)
}

// No GOMEMLIMIT is no ceiling. The runtime spells that math.MaxInt64, and
// sending it would make every heap look empty against its limit.
func TestGoMemory_NoLimitIsAbsent(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	old := debug.SetMemoryLimit(math.MaxInt64)
	defer debug.SetMemoryLimit(old)
	assert.Equal(t, int64(0), GoMemory().HeapLimit)

	reader, _, _ := provider(t)
	b, err := Collect(context.Background(), runSource(reader, nil))
	assert.NoError(t, err)
	raw, err := json.Marshal(b)
	assert.NoError(t, err)
	assert.That(t, !strings.Contains(string(raw), "heap_limit_bytes"))
}
```

Add `"math"` to the imports as well.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test ./internal/turbostats/ -run 'TestMemoryLimit_|TestCollect_CarriesTheRuntimeHostAndMemory|TestGoMemory_'`
Expected: FAIL to compile with `undefined: MemoryLimit` and `GoMemory().HeapLimit undefined`.

- [ ] **Step 3: Implement**

Replace the body of `internal/turbostats/gomem.go` after the package clause with:

```go
import (
	"math"
	"runtime/debug"
	"runtime/metrics"
)

// goMemorySamples are the runtime metrics GoMemory reads. runtime/metrics
// rather than runtime.ReadMemStats, because ReadMemStats stops the world, and
// Collect runs on every report and every GET /turbostats/v1.
var goMemorySamples = []string{
	"/memory/classes/total:bytes",
	"/memory/classes/heap/released:bytes",
	"/gc/heap/live:bytes",
	"/gc/cycles/total:gc-cycles",
}

// GoMem is what the Go runtime says about its memory. A field is zero when
// the runtime does not report it, which the bundle sends as absent.
type GoMem struct {
	// Retained is what the runtime holds from the operating system.
	Retained int64
	// HeapLive is the heap the last collection found live.
	HeapLive int64
	// GCCycles is collections completed since the process started.
	GCCycles int64
	// HeapLimit is GOMEMLIMIT, and zero when none is set.
	HeapLimit int64
}

// GoMemory reads the Go runtime's memory.
func GoMemory() GoMem {
	s := make([]metrics.Sample, len(goMemorySamples))
	for i, name := range goMemorySamples {
		s[i].Name = name
	}
	metrics.Read(s)

	total, released := uint64Of(s[0]), uint64Of(s[1])
	m := GoMem{
		HeapLive:  int64(uint64Of(s[2])),
		GCCycles:  int64(uint64Of(s[3])),
		HeapLimit: heapLimit(),
	}
	if total > released {
		m.Retained = int64(total - released)
	}
	return m
}

// heapLimit is GOMEMLIMIT. A negative argument reads the limit without
// changing it, and math.MaxInt64 is the runtime's spelling of no limit.
func heapLimit() int64 {
	if l := debug.SetMemoryLimit(-1); l != math.MaxInt64 {
		return l
	}
	return 0
}

// uint64Of is a sample's value, or zero when this runtime does not know the
// metric. A Go release that renamed one would otherwise panic the collector.
func uint64Of(s metrics.Sample) uint64 {
	if s.Value.Kind() != metrics.KindUint64 {
		return 0
	}
	return s.Value.Uint64()
}
```

Create `internal/turbostats/cgroup.go`:

```go
package turbostats

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// cgroupRoot is where Linux mounts the cgroup filesystem. A container with
// its own cgroup namespace sees its own cgroup here.
const cgroupRoot = "/sys/fs/cgroup"

// MemoryLimit is the memory limit of the cgroup under root, and false when
// there is none or it cannot be read: no cgroup filesystem, an unlimited
// cgroup, or a file this does not understand.
//
// It reads a file per call rather than once at startup, because an
// orchestrator can resize a running container.
func MemoryLimit(root string) (int64, bool) {
	// cgroup v2: one file, "max" when unlimited.
	if raw, err := os.ReadFile(filepath.Join(root, "memory.max")); err == nil {
		return parseLimit(raw)
	}
	// cgroup v1.
	if raw, err := os.ReadFile(filepath.Join(root, "memory", "memory.limit_in_bytes")); err == nil {
		return parseLimit(raw)
	}
	return 0, false
}

// parseLimit reads a limit file. cgroup v1 reports "unlimited" as the
// largest page-aligned int64, so anything at or above 2^62 is no limit.
func parseLimit(raw []byte) (int64, bool) {
	s := strings.TrimSpace(string(raw))
	if s == "max" {
		return 0, false
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || n <= 0 || n >= 1<<62 {
		return 0, false
	}
	return n, true
}
```

Create `internal/turbostats/host.go`:

```go
package turbostats

import (
	"os"
	"sync"
)

// hostname is read once: it does not change while a process runs, and
// Collect runs on every report.
var hostname = sync.OnceValue(func() string {
	h, err := os.Hostname()
	if err != nil {
		return ""
	}
	return h
})
```

In `internal/turbostats/collect.go`, replace `goRetained, goHeap := GoMemory()` with:

```go
	mem := GoMemory()
	// Absent rather than zero when there is no limit to read.
	limit, _ := MemoryLimit(cgroupRoot)
```

Add `Runtime: wire.RuntimeSQLFlow,` to the `Instance` literal after `HandlerType`, and replace the `Process` literal with:

```go
		Process: Process{
			StartedAt:        s.StartedAt.UTC().Truncate(time.Second),
			Host:             hostname(),
			RSSBytes:         rss,
			MemoryLimitBytes: limit,
			Goroutines:       runtime.NumGoroutine(),
			GoRetainedBytes:  mem.Retained,
			GoHeapBytes:      mem.HeapLive,
			Memory: &Memory{
				Runtime:        wire.MemoryRuntimeGo,
				RetainedBytes:  mem.Retained,
				LiveBytes:      mem.HeapLive,
				HeapLimitBytes: mem.HeapLimit,
				GCCount:        mem.GCCycles,
			},
		},
```

- [ ] **Step 4: Run the tests**

Run: `go test ./internal/turbostats/ -v -run 'TestMemoryLimit_|TestCollect_CarriesTheRuntimeHostAndMemory|TestGoMemory_|TestCollect_ARealisticRunBundleStaysUnderItsCeiling'`
Expected: PASS. The realistic run bundle logs about 1,400 bytes, under its 2 KiB ceiling.

- [ ] **Step 5: Run the short suite and commit**

Run: `go test -short ./...`
Expected: PASS.

```bash
git add internal/turbostats/gomem.go internal/turbostats/cgroup.go internal/turbostats/host.go internal/turbostats/collect.go internal/turbostats/cgroup_test.go internal/turbostats/collect_test.go
git commit -m "turbostats: SQLFlow reports its runtime, host, container limit and the generic memory object

A receiver judging a fleet of SQLFlow processes and Kafka Connect workers
needs one memory rule for both, and an OOM kill needs the container's limit
to be predicted. Collect now sends instance.runtime, process.host, the cgroup
memory limit and process.memory, whose retained and live bytes copy the Go
fields, with GOMEMLIMIT and the collection count.

No GOMEMLIMIT is absent rather than math.MaxInt64; cgroup v1's unlimited
sentinel and v2's max are absent rather than zero. Tests pin each.

If the unlimited sentinels reached the wire, every heap would read as empty
against its limit and no out-of-memory warning would ever fire."
```

---

### Task 4: SQLFlow's pipeline lifetime

**Files:**
- Modify: `internal/turbostats/collect.go:78-112`
- Modify: `internal/turbostats/reporter.go:206`
- Test: `internal/turbostats/collect_test.go`, `internal/turbostats/reporter_test.go`

**Interfaces:**
- Consumes: `wire.StateRunning`, `wire.StateStopped`, `wire.StateFailed` from Task 1.
- Produces: `pipelineSection(ctx, flat, floats, hist, dim, sentAt, startedAt time.Time, src)`; the final bundle's `Pipeline.State` is `stopped` or `failed`.

- [ ] **Step 1: Write the failing tests**

Append to `internal/turbostats/collect_test.go`:

```go
// A SQLFlow pipeline runs exactly as long as its process: running while it
// reports, one epoch, and no restarts.
func TestCollect_APipelineLivesAsLongAsItsProcess(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	reader, _, _ := provider(t)
	b, err := Collect(context.Background(), runSource(reader, nil))
	assert.NoError(t, err)

	p := b.Pipeline
	assert.Equal(t, wire.StateRunning, p.State)
	assert.That(t, p.StartedAt != nil)
	assert.Equal(t, b.Process.StartedAt, *p.StartedAt)
	assert.That(t, p.RestartCount != nil)
	assert.Equal(t, int64(0), *p.RestartCount)
	// SQLFlow cannot backfill yet, and says so by sending none.
	assert.That(t, p.Backfill == nil)
}
```

Append to `internal/turbostats/reporter_test.go`:

```go
// pipelineReporter collects a bundle with a running pipeline, as `sqlflow
// run` does.
func pipelineReporter(t *testing.T, url string) *Reporter {
	t.Helper()
	r, err := NewReporter(ReporterConfig{
		ReportTo: url,
		Key:      testKey(t),
		Interval: time.Hour,
		Collect: func(context.Context) (Bundle, error) {
			return Bundle{
				V:        Version,
				Instance: Instance{ID: "one"},
				Pipeline: &Pipeline{State: wire.StateRunning},
			}, nil
		},
		Log: zap.NewNop(),
	})
	assert.NoError(t, err)
	return r
}

func finalPipelineState(t *testing.T, exit Exit) string {
	t.Helper()
	rc := newReceiver()
	srv := httptest.NewServer(rc)
	defer srv.Close()
	pipelineReporter(t, srv.URL).Final(context.Background(), exit)
	rc.mu.Lock()
	body := rc.bodies[0]
	rc.mu.Unlock()
	var b Bundle
	assert.NoError(t, json.Unmarshal(body, &b))
	return b.Pipeline.State
}

// The last bundle says the pipeline stopped. A receiver reading state alone
// must never show a pipeline that shut down as running.
func TestReporter_ACleanExitSaysStopped(t *testing.T) {
	coverage.Covers(t, "observability.turbostats.reporter")
	assert.Equal(t, wire.StateStopped, finalPipelineState(t, Exit{Reason: "SIGTERM", Code: 0}))
}

// A pipeline that died says failed, or a crash reads as a clean shutdown.
func TestReporter_AFailedExitSaysFailed(t *testing.T) {
	coverage.Covers(t, "observability.turbostats.reporter")
	assert.Equal(t, wire.StateFailed,
		finalPipelineState(t, Exit{Reason: "system.sink.unreachable", Code: 1}))
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test ./internal/turbostats/ -run 'TestCollect_APipelineLivesAsLongAsItsProcess|TestReporter_ACleanExitSaysStopped|TestReporter_AFailedExitSaysFailed'`
Expected: FAIL. The collect test fails on `State`, `""` against `running`; both reporter tests fail with `running`.

- [ ] **Step 3: Implement**

In `internal/turbostats/collect.go`, change the call to:

```go
		p, err := pipelineSection(ctx, flat, floats, hist, dim, b.SentAt, b.Process.StartedAt, src.Pipeline)
```

Change the signature to:

```go
func pipelineSection(ctx context.Context, flat map[string]int64, floats map[string]float64,
	hist map[string]metricdata.HistogramDataPoint[float64], dim *dimensional,
	sentAt, startedAt time.Time, src *PipelineSource) (*Pipeline, error) {
```

At the top of the `&Pipeline{` literal, add:

```go
		// A SQLFlow pipeline starts with its process and never restarts
		// inside it, so its epoch is the process's and its restarts are
		// zero: a reading, not an unknown.
		State:        wire.StateRunning,
		StartedAt:    &startedAt,
		RestartCount: int64Ptr(0),
```

In `internal/turbostats/reporter.go`, after `b.Exit = exit`, add:

```go
	// The last bundle says how the pipeline ended, so a receiver that reads
	// state alone never shows a pipeline that shut down as running. A
	// non-zero exit code is a failure.
	if exit != nil && b.Pipeline != nil {
		b.Pipeline.State = wire.StateStopped
		if exit.Code != 0 {
			b.Pipeline.State = wire.StateFailed
		}
	}
```

Add the `wire` import to `reporter.go` if it lacks one.

- [ ] **Step 4: Run the tests**

Run: `go test ./internal/turbostats/`
Expected: PASS.

- [ ] **Step 5: Run the short suite and commit**

Run: `go test -short ./...`
Expected: PASS.

```bash
git add internal/turbostats/collect.go internal/turbostats/reporter.go internal/turbostats/collect_test.go internal/turbostats/reporter_test.go
git commit -m "turbostats: SQLFlow reports its pipeline's state, epoch and restarts

pipeline.state exists so a receiver can tell a failed pipeline from a
healthy process. SQLFlow now sends running while it reports, its process
start as the counter epoch, and zero restarts. The final bundle says
stopped after a clean exit and failed after a non-zero one.

If the final bundle kept running, a receiver reading state would show a
pipeline that crashed as healthy until its silence timed out."
```

---

### Task 5: Output freshness

**Files:**
- Modify: `internal/core/counting.go`
- Modify: `internal/cli/run/managers.go:308`
- Modify: `internal/turbostats/collect.go` (pipeline literal)
- Modify: `internal/cli/run/metrics_test.go:206-243`, `README.md:1446-1451`
- Test: `internal/core/counting_test.go`, `internal/turbostats/collect_test.go`

**Interfaces:**
- Consumes: `wire.Pipeline.LastSinkWriteAt` from Task 1.
- Produces: instrument `pipeline_last_sink_write_timestamp` (Int64Gauge, unix seconds, no attributes); `core.SinkRoleManager = "manager"`.

- [ ] **Step 1: Write the failing tests**

Append to `internal/core/counting_test.go` (add `"time"`, `"github.com/turbolytics/sql-flow/internal/coverage"` and `sdkmetric "go.opentelemetry.io/otel/sdk/metric"` to its imports):

```go
const lastWrite = "pipeline_last_sink_write_timestamp"

func meteredCounting(t *testing.T, inner Sink, role string) (Sink, *sdkmetric.ManualReader) {
	t.Helper()
	r := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(r))
	return NewCountingSink(inner, mp, "postgres", role), r
}

// The last write is the destination's acknowledgment, not the buffer's. A
// failed flush delivered nothing, and the retry that delivers is the write.
func TestCounting_RecordsTheLastWriteOnDeliveryOnly(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	inner := &countStubSink{failFlush: true}
	c, r := meteredCounting(t, inner, SinkRolePipeline)
	ctx := context.Background()

	assert.NoError(t, c.WriteTable(ctx, countIDTable(3)))
	assert.Error(t, c.Flush(ctx))
	assert.Equal(t, int64(0), flatValue(t, r, lastWrite))

	inner.failFlush = false
	before := time.Now().Unix()
	assert.NoError(t, c.Flush(ctx))
	got := flatValue(t, r, lastWrite)
	assert.That(t, got >= before && got <= time.Now().Unix())
}

// A periodic flush with nothing pending delivered nothing. Counting it would
// make a pipeline that writes nothing look fresh.
func TestCounting_AFlushWithNothingPendingIsNotAWrite(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	c, r := meteredCounting(t, &countStubSink{}, SinkRolePipeline)
	assert.NoError(t, c.Flush(context.Background()))
	assert.Equal(t, int64(0), flatValue(t, r, lastWrite))
}

// A windowed pipeline's output lands through its manager's sink, so that is
// a write too.
func TestCounting_AManagerSinkWriteIsAWrite(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	c, r := meteredCounting(t, &countStubSink{}, SinkRoleManager)
	ctx := context.Background()
	assert.NoError(t, c.WriteTable(ctx, countIDTable(1)))
	assert.NoError(t, c.Flush(ctx))
	assert.That(t, flatValue(t, r, lastWrite) > 0)
}

// The DLQ's writes are failures landing. Counting them would make a
// pipeline look fresh while every row it touched failed.
func TestCounting_TheDLQNeverMovesTheLastWrite(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	c, r := meteredCounting(t, &countStubSink{}, "dlq")
	ctx := context.Background()
	assert.NoError(t, c.WriteTable(ctx, countIDTable(2)))
	assert.NoError(t, c.Flush(ctx))
	assert.Equal(t, int64(0), flatValue(t, r, lastWrite))
}
```

Append to `internal/turbostats/collect_test.go`:

```go
// Output freshness reaches the bundle as a time, and is absent before the
// first delivered write.
func TestCollect_CarriesWhenTheSinkLastDelivered(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	reader, _, meter := provider(t)
	ctx := context.Background()

	b, err := Collect(ctx, runSource(reader, nil))
	assert.NoError(t, err)
	assert.That(t, b.Pipeline.LastSinkWriteAt == nil)

	g, err := meter.Int64Gauge("pipeline_last_sink_write_timestamp")
	assert.NoError(t, err)
	g.Record(ctx, 1757570000)
	b, err = Collect(ctx, runSource(reader, nil))
	assert.NoError(t, err)
	assert.That(t, b.Pipeline.LastSinkWriteAt != nil)
	assert.Equal(t, time.Unix(1757570000, 0).UTC(), *b.Pipeline.LastSinkWriteAt)
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test ./internal/core/ -run TestCounting_ && go test ./internal/turbostats/ -run TestCollect_CarriesWhenTheSinkLastDelivered`
Expected: FAIL to compile with `undefined: SinkRoleManager`, then the collect test fails on a nil `LastSinkWriteAt`.

- [ ] **Step 3: Implement**

In `internal/core/counting.go`, add `"time"` to the imports, and after `SinkRolePipeline`:

```go
// SinkRoleManager is the role of a window manager's sink, which delivers a
// windowed pipeline's output.
const SinkRoleManager = "manager"
```

Add a field to `counting`, after `flatWritten`:

```go
	// lastWrite records when rows were last delivered: the output's
	// freshness. Nil for the DLQ, whose writes are failures landing.
	lastWrite metric.Int64Gauge
```

In `NewCountingSink`, before the `BufferedRowReporter` check:

```go
	// A manager's sink delivers a windowed pipeline's output, so it counts.
	// The DLQ's does not, or a pipeline would look fresh while every row it
	// touched failed.
	if role == SinkRolePipeline || role == SinkRoleManager {
		if g, err := meter.Int64Gauge(
			"pipeline_last_sink_write_timestamp",
			metric.WithDescription("When a pipeline or window sink last delivered rows, as unix seconds"),
			metric.WithUnit("s"),
		); err == nil {
			c.lastWrite = g
		}
	}
```

In `Flush`, inside `if n > 0 {`, after the `flatWritten` block:

```go
		if c.lastWrite != nil {
			c.lastWrite.Record(ctx, time.Now().Unix())
		}
```

In `internal/cli/run/managers.go:308`, replace `sinks.WithSinkRole("manager")` with `sinks.WithSinkRole(core.SinkRoleManager)`. The file already imports `core` if `go build ./...` succeeds; otherwise add `"github.com/turbolytics/sql-flow/internal/core"`.

In `internal/turbostats/collect.go`, in the `&Pipeline{` literal, after `LastMessageAt`:

```go
		LastSinkWriteAt:     unixTime(flat["pipeline_last_sink_write_timestamp"]),
```

- [ ] **Step 4: Run the tests**

Run: `go test ./internal/core/ -run TestCounting && go test ./internal/turbostats/`
Expected: PASS.

- [ ] **Step 5: Hold the metrics endpoint to the docs**

Run: `go test ./internal/cli/run/ -run TestExportedSeriesNames`
If it fails with `pipeline_last_sink_write_timestamp_seconds` in the actual list, insert that name after `"pipeline_last_message_timestamp_seconds",` in `want`. If it passes, the test's pipeline never flushes rows and the gauge is never exported there; leave `want` alone.

In `README.md`, add a row to the **Pipeline:** table after `handler_checkpoints_skipped`:

```markdown
| `pipeline_last_sink_write_timestamp` | `pipeline_last_sink_write_timestamp_seconds` | gauge | — |
```

and after the paragraph that ends `memory grows until it closes.`, add:

```markdown
`pipeline_last_sink_write_timestamp` is when the pipeline or a window last
delivered rows, as acknowledged by the destination. A flush with nothing
pending does not move it, and neither does the DLQ. Read it beside
`pipeline_last_message_timestamp`: input that stays fresh while this goes
stale is a pipeline that reads and cannot write.
```

- [ ] **Step 6: Run the short suite and commit**

Run: `go test -short ./...`
Expected: PASS.

```bash
git add internal/core/counting.go internal/core/counting_test.go internal/cli/run/managers.go internal/turbostats/collect.go internal/turbostats/collect_test.go internal/cli/run/metrics_test.go README.md
git commit -m "core: the pipeline records when its output last landed

The bundle said when input arrived and never when output left, so a
pipeline that reads and cannot write looked fresh. The counting sink now
records pipeline_last_sink_write_timestamp on a flush that delivered rows,
for the pipeline's sink and window managers' sinks, and the bundle carries
it as pipeline.last_sink_write_at.

A failed flush, a flush with nothing pending and a DLQ write leave it
alone; a test pins each.

If an empty flush moved it, an idle pipeline that delivers nothing would
never page."
```

---

### Task 6: Rows dropped under IGNORE

**Files:**
- Modify: `internal/core/metrics.go:60-65, 340-349`
- Modify: `internal/core/turbine.go:1694, 1918-1924, 2310`
- Modify: `internal/turbostats/collect.go` (pipeline literal)
- Modify: `internal/cli/run/metrics_test.go`, `README.md`
- Test: `internal/core/flat_test.go`, `internal/turbostats/collect_test.go`

**Interfaces:**
- Consumes: `wire.Pipeline.ErrorRowsDropped` from Task 1.
- Produces: `Metrics.ErrorRowsDropped metric.Int64Counter`, instrument `error_rows_dropped`; `applyErrorPolicy(ctx, cause error, phase, message string, rows int64) error`.

- [ ] **Step 1: Write the failing tests**

Append to `internal/core/flat_test.go`:

```go
func ignoringTurbine(t *testing.T, src Source, h Handler, batchSize int) (*Turbine, *sdkmetric.ManualReader) {
	t.Helper()
	r := sdkmetric.NewManualReader()
	m, err := NewMetrics(sdkmetric.NewMeterProvider(sdkmetric.WithReader(r)))
	assert.NoError(t, err)
	tb := NewTurbine(src, h, &fakeSink{}, batchSize, time.Second, &sync.Mutex{},
		PipelineErrorPolicies{Policy: PolicyIgnore}, WithMetrics(m))
	return tb, r
}

// IGNORE discards a message the handler rejects. It is gone, and the count
// says so.
func TestErrorIgnore_CountsARejectedMessageAsDropped(t *testing.T) {
	coverage.Covers(t, "error.ignore")
	src := &fakeSource{batches: [][]Message{mixedMessages("bad", 4)}}
	tb, r := ignoringTurbine(t, src, &failingHandler{failWriteOn: "bad"}, 4)
	_, err := tb.ConsumeLoop(context.Background(), 0)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), flatValue(t, r, "error_rows_dropped"))
}

// IGNORE discards a whole batch when the handler's SQL fails, and every
// message in it is gone.
func TestErrorIgnore_CountsAFailedBatchAsDropped(t *testing.T) {
	coverage.Covers(t, "error.ignore")
	src := &fakeSource{batches: [][]Message{messages(4)}}
	tb, r := ignoringTurbine(t, src, &failingHandler{failInvokeOn: true}, 4)
	_, err := tb.ConsumeLoop(context.Background(), 0)
	assert.NoError(t, err)
	assert.Equal(t, int64(4), flatValue(t, r, "error_rows_dropped"))
}

// The DLQ diverts what it takes. Nothing is dropped.
func TestErrorDlq_DropsNothing(t *testing.T) {
	coverage.Covers(t, "error.dlq")
	r := sdkmetric.NewManualReader()
	m, err := NewMetrics(sdkmetric.NewMeterProvider(sdkmetric.WithReader(r)))
	assert.NoError(t, err)
	src := &fakeSource{batches: [][]Message{mixedMessages("bad", 4)}}
	tb := NewTurbine(src, &failingHandler{failWriteOn: "bad"}, &fakeSink{}, 4,
		time.Second, &sync.Mutex{},
		PipelineErrorPolicies{Policy: PolicyDLQ, DLQSink: &recordingSink{}}, WithMetrics(m))
	_, err = tb.ConsumeLoop(context.Background(), 0)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), flatValue(t, r, "error_rows_dropped"))
}
```

Append to `internal/turbostats/collect_test.go`:

```go
// An engine that counts dropped rows always sends the count, zero included:
// "nothing was dropped" is a reading.
func TestCollect_AlwaysSendsErrorRowsDropped(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	reader, m, _ := provider(t)
	ctx := context.Background()

	b, err := Collect(ctx, runSource(reader, nil))
	assert.NoError(t, err)
	assert.That(t, b.Pipeline.ErrorRowsDropped != nil)
	assert.Equal(t, int64(0), *b.Pipeline.ErrorRowsDropped)

	m.ErrorRowsDropped.Add(ctx, 4)
	b, err = Collect(ctx, runSource(reader, nil))
	assert.NoError(t, err)
	assert.Equal(t, int64(4), *b.Pipeline.ErrorRowsDropped)
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test ./internal/core/ -run 'TestErrorIgnore_Counts.*Dropped|TestErrorDlq_DropsNothing'`
Expected: FAIL: `TestErrorIgnore_CountsARejectedMessageAsDropped` gets 0, want 1. The collect test fails to compile on `m.ErrorRowsDropped`.

- [ ] **Step 3: Implement**

In `internal/core/metrics.go`, after the `PipelineRowsWritten` field:

```go
	// ErrorRowsDropped is rows the IGNORE policy discarded: neither
	// delivered nor diverted to a DLQ. Silent loss, counted where it
	// happens.
	ErrorRowsDropped metric.Int64Counter
```

In the counter table, after the `PipelineRowsWritten` entry:

```go
		{&m.ErrorRowsDropped, "error_rows_dropped", "rows",
			"Rows the IGNORE error policy discarded; neither delivered nor diverted"},
```

In `internal/core/turbine.go`, change `applyErrorPolicy`'s signature and its IGNORE case:

```go
// applyErrorPolicy decides what happens to a failed message or batch. It
// returns a non-nil error only when the pipeline should stop. Counting and
// logging belong to recordError, which every caller has already run. rows
// is how many rows the failure covers, which IGNORE discards.
func (t *Turbine) applyErrorPolicy(ctx context.Context, cause error, phase, message string, rows int64) error {

	switch t.errorPolicy.Policy {
	case PolicyIgnore:
		// Counted here and nowhere else: the DLQ diverts its rows and RAISE
		// stops the pipeline, so only IGNORE loses them.
		t.metrics.ErrorRowsDropped.Add(ctx, rows)
		return nil
```

Change the two callers:

```go
				if err := t.applyErrorPolicy(ctx, err, phaseHandlerWrite, string(raw.Value), 1); err != nil {
```

```go
		if policyErr := t.applyErrorPolicy(ctx, err, phaseHandlerInvoke, "Handler invocation failed", int64(numBatchMessages)); policyErr != nil {
```

`numBatchMessages` counts only messages the handler accepted: a rejected message `continue`s before the increment at `turbine.go:1714`, so the two paths never count one message twice.

In `internal/turbostats/collect.go`, in the `&Pipeline{` literal, after `SinkRetryCount`:

```go
		// Always sent, zero included: the engine counts every row IGNORE
		// discards.
		ErrorRowsDropped: int64Ptr(flat["error_rows_dropped"]),
```

- [ ] **Step 4: Run the tests**

Run: `go test ./internal/core/ -run 'TestError' && go test ./internal/turbostats/`
Expected: PASS.

- [ ] **Step 5: Hold the metrics endpoint to the docs**

Run: `go test ./internal/cli/run/ -run TestExportedSeriesNames`
If it fails with `error_rows_dropped_total` in the actual list, insert that name after `"error_count_total",` in `want`. If it passes, leave `want` alone.

In `README.md`, add a row to the **Pipeline:** table after `error_count`:

```markdown
| `error_rows_dropped` | `error_rows_dropped_total` | counter | — |
```

and after the paragraph added in Task 5:

```markdown
`error_rows_dropped` counts rows the `IGNORE` error policy discarded: a
message the handler rejected counts one, and a batch whose SQL failed counts
every message in it. They were neither delivered nor diverted, so a rising
count is data loss. The DLQ policy never moves it.
```

- [ ] **Step 6: Run the short suite and commit**

Run: `go test -short ./...`
Expected: PASS.

```bash
git add internal/core/metrics.go internal/core/turbine.go internal/core/flat_test.go internal/turbostats/collect.go internal/turbostats/collect_test.go internal/cli/run/metrics_test.go README.md
git commit -m "core: rows the IGNORE policy discards are counted

IGNORE drops a rejected message, or a whole batch whose SQL failed, and
nothing counted the rows: an error count says something failed, not how
much data went with it. applyErrorPolicy now counts the rows each failure
covers into error_rows_dropped, and the bundle always sends it.

A rejected message counts one and a failed batch counts its messages; the
rejected message never enters the batch count, so neither path counts a
message twice. The DLQ policy counts nothing.

If DLQ diversions counted, a pipeline routing failures safely would read
as losing data."
```

---

### Task 7: Reserved label names

**Files:**
- Modify: `internal/config/turbostats.go:38-70`
- Modify: `internal/validate/schemas/config.json`, `serve.json`, `rollups.json` (regenerated)
- Test: `internal/config/turbostats_test.go:128-150`

**Interfaces:**
- Consumes: the instance field names from Task 1.
- Produces: `runtime`, `runtime_version` and `reporter_version` refused as label keys.

- [ ] **Step 1: Write the failing test**

In `TestTurboStatsLabels_RefusesWhatBreaksTheShape`, add to `cases`:

```go
		"reserved runtime":          {"runtime": "jvm"},
		"reserved runtime version":  {"runtime_version": "3.8"},
		"reserved reporter version": {"reporter_version": "0.1"},
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `go test ./internal/config/ -run TestTurboStatsLabels_RefusesWhatBreaksTheShape`
Expected: FAIL on the three new cases.

- [ ] **Step 3: Implement**

In `internal/config/turbostats.go`, add the three names to `reservedLabels`:

```go
var reservedLabels = map[string]bool{
	"id": true, "name": true, "version": true, "commit": true, "arch": true,
	"config_hash": true, "source_type": true, "sink_type": true, "handler_type": true,
	"runtime": true, "runtime_version": true, "reporter_version": true,
}
```

In the `Labels` field's comment, replace `arch, config_hash, source_type, sink_type, handler_type.` with `arch, config_hash, source_type, sink_type, handler_type, runtime, runtime_version, reporter_version.`

- [ ] **Step 4: Regenerate the config schemas and run the tests**

The `Labels` comment is a schema description, so the config schemas change.

Run: `make schema && go test ./internal/config/ ./internal/schema/`
Expected: PASS. `git diff --stat internal/validate/schemas` shows the description change in the schemas that carry a `turbostats` block.

- [ ] **Step 5: Run the short suite and commit**

Run: `go test -short ./...`
Expected: PASS.

```bash
git add internal/config/turbostats.go internal/config/turbostats_test.go internal/validate/schemas
git commit -m "config: a label may not shadow the new instance fields

A label named runtime would put two different things under one name in a
receiver that flattens labels beside instance fields. runtime,
runtime_version and reporter_version join the reserved names, and
validate refuses them before a pipeline runs."
```

---

### Task 8: Every bundle validates against the schema

**Files:**
- Create: `internal/turbostats/schema_test.go`

**Interfaces:**
- Consumes: `tsschema.Bundle`, `tsschema.Response`, `tsschema.BundleID`, `tsschema.ResponseID` from Task 2; `widestBundle` from Task 1; `provider`, `runSource`, `static` from `collect_test.go`.

- [ ] **Step 1: Write the tests**

Create `internal/turbostats/schema_test.go`:

```go
package turbostats

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"github.com/santhosh-tekuri/jsonschema/v6"
	"github.com/turbolytics/sql-flow/internal/core"
	"github.com/turbolytics/sql-flow/internal/coverage"
	tsschema "github.com/turbolytics/sql-flow/turbostats/wire/schema"
	"github.com/zeebo/assert"
)

func compiled(t *testing.T, id string, doc []byte) *jsonschema.Schema {
	t.Helper()
	parsed, err := jsonschema.UnmarshalJSON(bytes.NewReader(doc))
	assert.NoError(t, err)
	c := jsonschema.NewCompiler()
	assert.NoError(t, c.AddResource(id, parsed))
	s, err := c.Compile(id)
	assert.NoError(t, err)
	return s
}

// asJSON is v as the JSON data model a validator reads.
func asJSON(t *testing.T, v any) any {
	t.Helper()
	raw, err := json.Marshal(v)
	assert.NoError(t, err)
	var out any
	assert.NoError(t, json.Unmarshal(raw, &out))
	return out
}

func validBundle(t *testing.T, v any) {
	t.Helper()
	if err := compiled(t, tsschema.BundleID, tsschema.Bundle).Validate(asJSON(t, v)); err != nil {
		t.Fatalf("the schema rejects a bundle the contract allows:\n%v", err)
	}
}

// Every bundle the engine builds is one the published schema accepts: a
// run, a serve and a rollup process, and the widest bundle the contract
// allows. A schema that rejected one would fail a reporter in another
// language that copied the engine exactly.
func TestSchema_EveryBundleTheEngineBuildsValidates(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	ctx := context.Background()

	reader, m, _ := provider(t)
	m.MessageCount.Add(ctx, 7)
	m.ErrorRowsDropped.Add(ctx, 1)
	stats := func(context.Context) (*core.StateStats, error) {
		return &core.StateStats{SizeBytes: 4096}, nil
	}
	run, err := Collect(ctx, runSource(reader, stats))
	assert.NoError(t, err)
	run.Exit = &Exit{
		Reason: "SIGTERM",
		Code:   0,
	}

	serveReader, _, _ := provider(t)
	serve, err := Collect(ctx, Source{
		Static: static,
		Reader: serveReader,
		Serve:  &ServeSource{},
	})
	assert.NoError(t, err)

	rollupReader, _, _ := provider(t)
	rollup, err := Collect(ctx, Source{
		Static: static,
		Reader: rollupReader,
		Rollup: &RollupSource{
			Section: func() (*Rollup, *Freshness) {
				return &Rollup{Role: "standby"}, nil
			},
		},
	})
	assert.NoError(t, err)

	for name, b := range map[string]Bundle{
		"run":    run,
		"serve":  serve,
		"rollup": rollup,
		"widest": widestBundle(t),
	} {
		t.Run(name, func(t *testing.T) { validBundle(t, b) })
	}
}

// A fleet mixes engine versions. A bundle from an engine that predates
// every field added since the first receiver must still validate.
func TestSchema_AnOlderEnginesBundleValidates(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	older := `{"v":1,"sent_at":"2026-09-01T00:00:00Z",
		"instance":{"version":"v1","commit":"abc","arch":"linux/arm64","config_hash":"sha256:00"},
		"process":{"started_at":"2026-09-01T00:00:00Z","goroutines":12},
		"pipeline":{"message_count":1,"handler_rows_read":1,"error_count":0,
			"sink_flush_count":0,"sink_rows_accepted":0,"sink_rows_written":0,
			"state_commit_count":0}}`
	var v any
	assert.NoError(t, json.Unmarshal([]byte(older), &v))
	assert.NoError(t, compiled(t, tsschema.BundleID, tsschema.Bundle).Validate(v))
}

// An engine newer than this schema adds a field, a section and a state
// value. Today's schema must accept all three, or a deployed validator
// rejects the next release.
func TestSchema_ANewerEnginesBundleValidates(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	doc := asJSON(t, widestBundle(t)).(map[string]any)
	doc["future_section"] = map[string]any{"x": 1}
	pipeline := doc["pipeline"].(map[string]any)
	pipeline["future_field"] = 2
	pipeline["state"] = "draining"
	pipeline["backfill"].(map[string]any)["state"] = "throttled"
	assert.NoError(t, compiled(t, tsschema.BundleID, tsschema.Bundle).Validate(doc))
}

// The schema is not vacuous. A document without the required sections is
// not a bundle; the signing vectors' {"v":1} body is one such document.
func TestSchema_ADocumentWithoutTheRequiredSectionsIsRejected(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	var v any
	assert.NoError(t, json.Unmarshal([]byte(`{"v":1}`), &v))
	assert.Error(t, compiled(t, tsschema.BundleID, tsschema.Bundle).Validate(v))
}

// A JVM sends no goroutines. Its bundle must validate.
func TestSchema_ABundleWithoutGoroutinesValidates(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	doc := asJSON(t, widestBundle(t)).(map[string]any)
	delete(doc["process"].(map[string]any), "goroutines")
	assert.NoError(t, compiled(t, tsschema.BundleID, tsschema.Bundle).Validate(doc))
}

// The response a control plane answers with, with and without commands.
func TestSchema_TheResponseValidates(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	s := compiled(t, tsschema.ResponseID, tsschema.Response)
	for _, body := range []string{
		`{"v":1,"commands":[]}`,
		`{"v":1,"commands":[{"id":"c1","verb":"restart"}],"later":true}`,
	} {
		var v any
		assert.NoError(t, json.Unmarshal([]byte(body), &v))
		assert.NoError(t, s.Validate(v))
	}
}
```

- [ ] **Step 2: Run the tests**

Run: `go test ./internal/turbostats/ -run TestSchema_ -v`
Expected: PASS for all six. If `TestSchema_EveryBundleTheEngineBuildsValidates` fails, the failure names the schema path; fix the generator or the type, never this test.

- [ ] **Step 3: Mutation check**

In `turbostats/wire/bundle.go`, temporarily remove `,omitempty` from `Goroutines`, run `make schema`, then run `go test ./internal/turbostats/ -run TestSchema_ABundleWithoutGoroutinesValidates`. Expected: FAIL with a missing `goroutines` property. Restore `,omitempty`, run `make schema`, and run it again. Expected: PASS, and `git status --short` shows no schema change.

- [ ] **Step 4: Run the short suite and commit**

Run: `go test -short ./... && git status --short`
Expected: PASS, and no schema file changed.

```bash
git add internal/turbostats/schema_test.go
git commit -m "turbostats: every bundle the engine builds validates against the published schema

The schema is generated from the types, but nothing proved a real bundle
satisfies it. Run, serve and rollup bundles, the widest bundle, an older
engine's bundle, a newer engine's bundle and a JVM-shaped bundle without
goroutines now validate; a document without the required sections does
not.

The goroutines check failed with omitempty removed and passed with it
restored. If the schema rejected an older or newer engine's bundle, a
receiver that validates would drop part of a mixed fleet."
```

---

### Task 9: The shipped image

**Files:**
- Modify: `tests/release/test_image.py:553-635`

**Interfaces:**
- Consumes: every SQLFlow field from Tasks 3 to 6, in the built image.

- [ ] **Step 1: Write the failing assertions**

In `test_turbostats_reporter_posts_signed_bundles`, give the pipeline container a memory limit. `with_kwargs` replaces its arguments, so both go in one call:

```python
            pipeline = DockerContainer(image) \
                .with_volume_mapping(tmp, "/conf") \
                .with_kwargs(
                    network_mode=f"container:{receiver.get_wrapped_container().id}",
                    mem_limit="512m") \
                .with_command("run /conf/pipeline.yml")
```

After `assert "serve" not in bundle`, add:

```python
    # The generic fields, as the shipped image sends them. The container's
    # limit reaches the bundle, so an OOM kill can be predicted.
    assert bundle["instance"]["runtime"] == "sqlflow"
    assert bundle["process"]["host"]
    assert bundle["process"]["memory_limit_bytes"] == 512 * 1024 * 1024
    memory = bundle["process"]["memory"]
    assert memory["runtime"] == "go"
    assert memory["retained_bytes"] > 0
    assert "gc_count" in memory

    pipeline = bundle["pipeline"]
    assert pipeline["state"] == "running"
    assert pipeline["started_at"] == bundle["process"]["started_at"]
    assert pipeline["restart_count"] == 0
    assert pipeline["error_rows_dropped"] == 0
    assert "backfill" not in pipeline
```

- [ ] **Step 2: Run the release test against the image before this branch**

Run the edited test against an image built from `main`:

```bash
git worktree add ../ts-main origin/main && (cd ../ts-main && make sqlflow-image SQLFLOW_IMAGE=sqlflow:ts-main)
SQLFLOW_IMAGE=sqlflow:ts-main uv run pytest tests/release -q -k test_turbostats_reporter_posts_signed_bundles
git worktree remove ../ts-main
```

Expected: FAIL with `KeyError: 'runtime'`. This proves the assertions detect an image without the fields.

- [ ] **Step 3: Run the release tests against this branch**

Run: `make test-release`
Expected: PASS, including `test_turbostats_reporter_posts_signed_bundles`, `test_turbostats_serve_bundle_carries_a_serve_section` and `test_cli_rollup_run_reports_its_rollups_and_freshness`.

- [ ] **Step 4: Commit**

```bash
git add tests/release/test_image.py
git commit -m "release: the shipped image sends the generic TurboStats fields

Unit tests prove Collect fills the fields, not that the image users pull
does. The reporter's release test now runs the pipeline under a 512 MiB
limit and asserts the runtime, host, container limit, memory object,
pipeline state, epoch, restarts and dropped rows in the first bundle.

Against an image built from main the test fails with KeyError: 'runtime';
against this branch it passes."
```

---

## After the last task

Push the branch and update PR #431's description with what the plan built. The next plans, each on its own branch against `main`:

1. **The Kafka Connect reporter.** It starts with the spec's "Verify before building" list, proved by execution against a real Connect worker and Postgres, before any reporter code.
2. **The control plane.** It serves `tsschema.Bundle` and `tsschema.Response` at their IDs, bumps its `sql-flow` dependency to pick up the new fields, and reads `pipeline.state` into its verdicts.
