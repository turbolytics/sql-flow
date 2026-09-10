# Row Accounting Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make an enrichment drop visible by counting rows at every stage of the pipeline, and by reporting what a joined reference table held at startup.

**Architecture:** Three cumulative counters replace an unsummable gauge. A decorator applied inside `sinks.New` counts rows for every sink the pipeline builds, incrementing the delivered counter only when `Flush` returns nil. A startup pass derives the tables the handler SQL joins from DuckDB's own AST and counts them.

**Tech Stack:** Go, OpenTelemetry metrics with the Prometheus exporter, DuckDB via ADBC, Apache Arrow. Tests use `github.com/zeebo/assert`.

**Spec:** `docs/superpowers/specs/2026-09-08-row-accounting-design.md`

## Global Constraints

- Instrument names carry no `_total` suffix. The Prometheus exporter appends the unit, then `_total` for counters, and skips the unit when the name already contains it.
- New counters are `Int64Counter`. New gauges are `Int64Gauge`. Every instrument sets `metric.WithDescription` and `metric.WithUnit`.
- A nil `metric.MeterProvider` must yield instruments that record nothing. Use `noop.NewMeterProvider()`, never a nil check at the call site.
- Losing an instrument is never worth failing a pipeline over. Constructor errors degrade to a no-op, matching `retryCounter` in `internal/sinks/metrics.go`.
- The meter name is `sqlflow` everywhere except `internal/webhook`, which uses `sqlflow.sources.http`.
- Exported series names are asserted as an exact set in `internal/cli/run/metrics_test.go`. Adding an instrument without updating that set fails the build. This is deliberate.
- Do not change `sink_flush_num_rows`, `error_count`'s unit, or `sink_retry_count`'s unit. Each change renames an exported series.
- Every commit message names the defect, the fix, and the evidence, per `CLAUDE.md`.

---

### Task 1: Pin the exported names, then repair the unit inconsistency

This task builds the safety net every later task leans on. It asserts the exact set of exported Prometheus series, so an instrument added later cannot go undocumented.

**Files:**
- Modify: `internal/core/metrics.go`
- Modify: `internal/sinks/metrics.go`
- Test: `internal/cli/run/metrics_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces: `run.exportedSeriesNames(t *testing.T, mp metric.MeterProvider, reg *prom.Registry) []string` — a test helper later tasks extend with new names.

- [ ] **Step 1: Write the failing test**

Append to `internal/cli/run/metrics_test.go`:

```go
// exportedNames registers every instrument the engine declares against a real
// Prometheus exporter and returns the series names it produces.
//
// The instrument name and the exported name differ: the exporter appends the
// unit, then _total for counters, and skips the unit when the name already
// contains it. Only the exported name is queryable, so only the exported name
// is worth asserting.
func exportedNames(t *testing.T) []string {
	t.Helper()

	reg := prom.NewRegistry()
	exp, err := prometheus.New(prometheus.WithRegisterer(reg))
	assert.NoError(t, err)
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(exp))

	m, err := core.NewMetrics(mp)
	assert.NoError(t, err)

	ctx := context.Background()
	m.MessageCount.Add(ctx, 1)
	m.ErrorCount.Add(ctx, 1)
	m.SourceReadLatency.Record(ctx, 1)
	m.SinkFlushLatency.Record(ctx, 1)
	m.SinkFlushNumRows.Record(ctx, 1)
	m.SinkFlushCount.Add(ctx, 1)
	m.BatchProcessingLatency.Record(ctx, 1)
	m.ConsumerLag.Record(ctx, 1)
	m.StateCommitLatency.Record(ctx, 1)
	m.StateCommitCount.Add(ctx, 1)
	m.StateSizeBytes.Record(ctx, 1)
	m.StateTableRows.Record(ctx, 1)

	wm, err := webhook.NewMetrics(mp)
	assert.NoError(t, err)
	wm.RequestCount.Add(ctx, 1)
	wm.RequestDuration.Record(ctx, 1)

	families, err := reg.Gather()
	assert.NoError(t, err)

	var names []string
	for _, f := range families {
		if f.GetName() == "target_info" {
			continue
		}
		names = append(names, f.GetName())
	}
	sort.Strings(names)
	return names
}

func TestExportedSeriesNames(t *testing.T) {
	want := []string{
		"batch_processing_latency_seconds",
		"consumer_lag_messages",
		"error_count_total",
		"message_count_messages_total",
		"sink_buffered_rows",
		"sink_flush_count_flushes_total",
		"sink_flush_latency_seconds",
		"sink_flush_num_rows",
		"source_read_latency_seconds",
		"state_commit_count_commits_total",
		"state_commit_latency_seconds",
		"state_db_size_bytes",
		"state_table_rows",
		"webhook_request_duration_seconds",
		"webhook_requests_total",
	}
	assert.DeepEqual(t, want, exportedNames(t))
}

// TestStateCommitLatencyUnitIsNameNeutral guards the unit edit in this task.
// state_commit_latency declared its unit as "s" where every other latency
// declares "seconds". The exporter normalizes UCUM "s" before appending it, so
// the series name is the same either way -- but a unit edit that did rename a
// series would break dashboards with no test failing.
func TestStateCommitLatencyUnitIsNameNeutral(t *testing.T) {
	names := exportedNames(t)
	found := false
	for _, n := range names {
		if n == "state_commit_latency_seconds" {
			found = true
		}
	}
	assert.True(t, found)
}
```

Add these imports to the file's import block:

```go
import (
	"context"
	"sort"
	"testing"

	prom "github.com/prometheus/client_golang/prometheus"
	"github.com/turbolytics/sql-flow/internal/core"
	"github.com/turbolytics/sql-flow/internal/webhook"
	"github.com/zeebo/assert"
	"go.opentelemetry.io/otel/exporters/prometheus"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)
```

- [ ] **Step 2: Run the test to verify it passes**

Run: `go test ./internal/cli/run/ -run 'TestExportedSeriesNames|TestStateCommitLatencyUnitIsNameNeutral' -v`
Expected: PASS. This test characterizes what ships today, including `sink_buffered_rows`, which Task 4 removes.

- [ ] **Step 3: Change the unit and add the comments**

In `internal/core/metrics.go`, change `state_commit_latency`'s unit:

```go
	if m.StateCommitLatency, err = meter.Float64Histogram(
		"state_commit_latency",
		metric.WithDescription("Latency of committing state and offsets together"),
		metric.WithUnit("seconds"),
	); err != nil {
		return nil, fmt.Errorf("state_commit_latency: %w", err)
	}
```

Add this comment directly above the `error_count` block in the same file:

```go
	// The unit stays "count", which the exporter drops as unitless, so this
	// exports as error_count_total. A descriptive unit would rename it to
	// error_count_errors_total and break every dashboard built on the current
	// name. Measured against the exporter, not inferred.
```

Add the same comment above the `sink_retry_count` block in `internal/sinks/metrics.go`, naming that series:

```go
	// The unit stays "count", which the exporter drops as unitless, so this
	// exports as sink_retry_count_total. A descriptive unit would rename it to
	// sink_retry_count_retries_total and break every dashboard built on the
	// current name. Measured against the exporter, not inferred.
```

- [ ] **Step 4: Run the tests to verify they still pass**

Run: `go test ./internal/cli/run/ ./internal/core/ ./internal/sinks/ -run 'TestExportedSeriesNames|TestStateCommitLatency'`
Expected: PASS. The unit change is name-neutral, so the asserted set is unchanged.

- [ ] **Step 5: Commit**

```bash
git add internal/core/metrics.go internal/sinks/metrics.go internal/cli/run/metrics_test.go
git commit -m "metrics: assert the exported series names, and align the state_commit_latency unit

The exported Prometheus name and the instrument name differ, and only the
exported name is queryable. Nothing asserted the mapping, so an OTel upgrade
that changed unit suffixing would rename every series in this repo with no
test failing.

state_commit_latency declared its unit as \"s\" where every other latency
declares \"seconds\". The exporter normalizes UCUM \"s\" before appending it,
so the series stays state_commit_latency_seconds; the test proves it rather
than assuming it.

error_count and sink_retry_count keep unit \"count\" with a comment saying
why: a descriptive unit renames error_count_total to error_count_errors_total.

If the assertion is wrong, the README documents series that do not exist."
```

---

### Task 2: RowsRead on the Handler interface, and handler_rows_read

**Files:**
- Modify: `internal/core/turbine.go` (the `Handler` interface, and `processBatch`)
- Modify: `internal/core/metrics.go`
- Modify: `internal/handlers/structured.go`
- Modify: `internal/handlers/inferred.go`
- Modify: `internal/handlers/inferred_disk.go`
- Modify: `internal/conformance/pipeline.go` (`passthroughHandler`)
- Test: `internal/core/turbine_test.go`
- Test: `internal/cli/run/metrics_test.go`

`internal/handlers/noop.go` is NOT modified. Its `Noop` type declares `Init() error` and `Invoke() (*arrow.Table, error)`, which do not match `core.Handler`, so it implements nothing and is dead code.

**Interfaces:**
- Consumes: `exportedNames` from Task 1.
- Produces: `core.Handler` gains `RowsRead() int64`. `core.Metrics` gains the field `HandlerRowsRead metric.Int64Counter`.

- [ ] **Step 1: Write the failing test**

Add to `internal/core/turbine_test.go`:

```go
// TestHandlerRowsReadIsRowsNotMessages pins the distinction the metric name
// makes. A handler whose Invoke fails produced no batch table, so the SQL ran
// over nothing, however many messages were written to it.
func TestHandlerRowsReadIsRowsNotMessages(t *testing.T) {
	h := &fakeHandler{}
	assert.Equal(t, int64(0), h.RowsRead())

	assert.NoError(t, h.Write([]byte(`{"a":1}`)))
	assert.NoError(t, h.Write([]byte(`{"a":2}`)))

	// Buffered but not yet invoked: the SQL has not run over anything.
	assert.Equal(t, int64(0), h.RowsRead())

	_, err := h.Invoke(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, int64(2), h.RowsRead())
}
```

Add `rowsRead int64` to the `fakeHandler` struct in the same file, and a method:

```go
func (h *fakeHandler) RowsRead() int64 { return h.rowsRead }
```

Set it inside `fakeHandler.Invoke`, immediately before the table is built, to the number of buffered messages that became rows. Set it to `0` on any path that returns a nil table or an error.

- [ ] **Step 2: Run the test to verify it fails**

Run: `go test ./internal/core/ -run TestHandlerRowsReadIsRowsNotMessages -v`
Expected: FAIL. `h.RowsRead` is not defined on the real handlers, so the package will not compile until Step 3 adds the method everywhere.

- [ ] **Step 3: Add the interface method and the implementations**

In `internal/core/turbine.go`, extend the interface:

```go
type Handler interface {
	Init(ctx context.Context) error
	Write(msg []byte) error
	Invoke(ctx context.Context) (arrow.Table, error)

	// RowsRead reports the rows the last Invoke ingested into the batch
	// table. It is the denominator of the enrichment ratio, so it must count
	// rows the SQL actually ran over: an Invoke that failed schema inference
	// produced no table and reports zero, however many messages were written.
	RowsRead() int64
}
```

In `internal/handlers/structured.go`, add the field to `StructuredBatchHandler`:

```go
	// rowsRead is the row count of the last Invoke, for handler_rows_read.
	rowsRead int64
```

and the method:

```go
func (h *StructuredBatchHandler) RowsRead() int64 { return h.rowsRead }
```

In `Invoke`, set `h.rowsRead = 0` as the first statement, and set `h.rowsRead = int64(len(raw))` only after the record has been built and ingested successfully, immediately before the query statement runs.

Apply the identical pattern to `InferredMemBatchHandler` in `internal/handlers/inferred.go`: field, method, `h.rowsRead = 0` first, and `h.rowsRead = int64(len(raw))` after `h.ingestStmt.ExecuteUpdate(ctx)` returns without error.

For `InferredDiskBatchHandler` in `internal/handlers/inferred_disk.go`, the rows come from the batch file rather than a slice. Add the same field and method, set `h.rowsRead = 0` at the top of `Invoke`, and set `h.rowsRead = int64(h.numWrote)` immediately after the `CREATE TABLE ... read_json_auto` exec succeeds.

In `internal/conformance/pipeline.go`, add to `passthroughHandler`:

```go
	rowsRead int64
```

```go
func (h *passthroughHandler) RowsRead() int64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.rowsRead
}
```

and in its `Invoke`, set `h.rowsRead = n` after the `n == 0` early return, so an empty batch reports zero.

Add the instrument to `internal/core/metrics.go`, beside `MessageCount`:

```go
	HandlerRowsRead metric.Int64Counter
```

```go
	if m.HandlerRowsRead, err = meter.Int64Counter(
		"handler_rows_read",
		metric.WithDescription("Rows the handler ingested into the batch table, which the pipeline SQL ran over"),
		metric.WithUnit("rows"),
	); err != nil {
		return nil, fmt.Errorf("handler_rows_read: %w", err)
	}
```

Record it in `processBatch` in `internal/core/turbine.go`, directly after the `t.handler.Invoke(ctx)` call and its lock release, before the error branch:

```go
	// Recorded before the error branch: a failed Invoke reports zero, which is
	// the honest number, and skipping the record would leave the ratio's
	// denominator frozen through every failing batch.
	t.metrics.HandlerRowsRead.Add(ctx, t.handler.RowsRead())
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test ./internal/core/ ./internal/handlers/ ./internal/conformance/ -run 'TestHandlerRowsRead|TestInvoke'`
Expected: PASS.

- [ ] **Step 5: Add the new series name to the assertion**

In `internal/cli/run/metrics_test.go`, add `m.HandlerRowsRead.Add(ctx, 1)` to `exportedNames`, and add `"handler_rows_read_total"` to the `want` slice in `TestExportedSeriesNames`, keeping the slice sorted.

Run: `go test ./internal/cli/run/ -run TestExportedSeriesNames -v`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/core internal/handlers internal/conformance internal/cli/run
git commit -m "metrics: count the rows the SQL ran over, from the handler itself

The enrichment ratio needs a denominator, and nothing counted the rows that
reached the batch table. message_count counts messages, which is a different
number the moment a message is rejected.

Counting accepted handler.Write calls was the cheaper option and is wrong on a
path that exists today: when inferSchema or buildRecord fails, Invoke produces
no table at all, so the SQL ran over nothing while the count had already moved.
RowsRead on the Handler interface reports zero there instead.

handlers.Noop is untouched. Its signatures do not match core.Handler, so it
implements nothing.

If this is wrong, the ratio's denominator overstates and a real drop looks
smaller than it is."
```

---

### Task 3: The counting decorator, in isolation

**Files:**
- Create: `internal/core/counting.go`
- Create: `internal/core/counting_test.go`

**Interfaces:**
- Consumes: `core.Sink`, `core.BufferedRowReporter` — both in the same package.
- Produces: `core.NewCountingSink(inner Sink, mp metric.MeterProvider, sinkType, role string) Sink`.

**Why `internal/core` and not `internal/sinks`.** The conformance harness must
apply this decorator (Task 5), and the harness lives in `internal/conformance`.
Five test files in **package `sinks`** — not `sinks_test` — import
`internal/conformance`, so a `conformance → sinks` import cycles the `sinks`
test binary. Verified with `go list -deps` and the package declarations on
2026-09-09. Both packages already import `internal/core`, which makes it the
only home that serves both callers.

- [ ] **Step 1: Write the failing test**

Create `internal/core/counting_test.go`:

```go
package core

import (
	"context"
	"errors"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/zeebo/assert"
	"go.opentelemetry.io/otel/metric/noop"
)

// stubSink records what it was given and fails its flush on demand.
type stubSink struct {
	rows      int64
	failFlush bool
	buffered  int
}

func (s *stubSink) WriteTable(_ context.Context, batch arrow.Table) error {
	if batch != nil {
		s.rows += batch.NumRows()
	}
	return nil
}

func (s *stubSink) Flush(context.Context) error {
	if s.failFlush {
		return errors.New("stub: sink is down")
	}
	return nil
}

func (s *stubSink) BufferedRows() int { return s.buffered }

// plainSink implements core.Sink and nothing else.
type plainSink struct{}

func (plainSink) WriteTable(context.Context, arrow.Table) error { return nil }
func (plainSink) Flush(context.Context) error                   { return nil }

// TestCountingCountsDeliveredRowsOnce is the invariant
// sink.rows.counted_on_delivery in miniature. A failed flush counts nothing,
// and the retry that delivers those rows counts them exactly once.
func TestCountingCountsDeliveredRowsOnce(t *testing.T) {
	inner := &stubSink{failFlush: true}
	c := NewCountingSink(inner, noop.NewMeterProvider(), "console", "pipeline").(*countingBuffered)

	ctx := context.Background()
	assert.NoError(t, c.WriteTable(ctx, idTable(3)))
	assert.Equal(t, int64(3), c.pendingRows())

	assert.Error(t, c.Flush(ctx))
	assert.Equal(t, int64(3), c.pendingRows())

	inner.failFlush = false
	assert.NoError(t, c.Flush(ctx))
	assert.Equal(t, int64(0), c.pendingRows())
}

// TestCountingForwardsBufferedRows guards the trap the retry wrapper fell
// into: a decorator that does not forward an optional interface silently
// removes it from every sink it wraps.
func TestCountingForwardsBufferedRows(t *testing.T) {
	inner := &stubSink{buffered: 7}
	c := NewCountingSink(inner, noop.NewMeterProvider(), "console", "pipeline")

	reporter, ok := c.(BufferedRowReporter)
	assert.True(t, ok)
	assert.Equal(t, 7, reporter.BufferedRows())
}

// TestCountingDoesNotInventBufferedRows is the other half: a sink that reports
// no depth must not appear to report one just because it was wrapped.
func TestCountingDoesNotInventBufferedRows(t *testing.T) {
	c := NewCountingSink(plainSink{}, noop.NewMeterProvider(), "noop", "pipeline")
	_, ok := c.(BufferedRowReporter)
	assert.False(t, ok)
}

// TestCountingNilBatchIsNoop matches sink.flush.empty_is_noop.
func TestCountingNilBatchIsNoop(t *testing.T) {
	c := NewCountingSink(&stubSink{}, noop.NewMeterProvider(), "console", "pipeline").(*countingBuffered)
	assert.NoError(t, c.WriteTable(context.Background(), nil))
	assert.Equal(t, int64(0), c.pendingRows())
}
```

Add this helper to the same file:

```go
// idTable builds an n-row table with a single int64 column.
func idTable(n int64) arrow.Table {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	b := array.NewRecordBuilder(memory.NewGoAllocator(), schema)
	defer b.Release()
	for i := int64(0); i < n; i++ {
		b.Field(0).(*array.Int64Builder).Append(i)
	}

	rec := b.NewRecord()
	defer rec.Release()
	return array.NewTableFromRecords(schema, []arrow.Record{rec})
}
```

with imports `"github.com/apache/arrow-go/v18/arrow/array"` and `"github.com/apache/arrow-go/v18/arrow/memory"`.

- [ ] **Step 2: Run the test to verify it fails**

Run: `go test ./internal/core/ -run TestCounting -v`
Expected: FAIL to compile, `undefined: NewCountingSink`.

- [ ] **Step 3: Write the decorator**

Create `internal/core/counting.go`:

```go
package core

import (
	"context"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

// counting counts the rows a sink accepts and the rows a flush delivers.
//
// The two are separate numbers on purpose. Their difference is the sink's
// buffer depth, derived rather than self-reported, which is why this replaces
// the sink_buffered_rows gauge: a sink can misreport its own depth, and a
// count of what the pipeline watched it accept cannot.
type counting struct {
	inner    Sink
	accepted metric.Int64Counter
	written  metric.Int64Counter
	attrs    metric.AddOption

	mu      sync.Mutex
	pending int64
}

// countingBuffered forwards the optional depth report of a sink that has one.
//
// Go has no conditional interface satisfaction, so the choice is made at
// construction. Skipping this is not cosmetic: the retry wrapper declares only
// WriteTable and Flush, which is why sink_buffered_rows never recorded for the
// ClickHouse and Iceberg sinks it wraps -- a wrapper silently removed the
// interface the gauge depended on.
type countingBuffered struct {
	*counting
	reporter BufferedRowReporter
}

func (c *countingBuffered) BufferedRows() int { return c.reporter.BufferedRows() }

// NewCountingSink wraps a sink so its rows are counted.
//
// A nil provider yields instruments that record nothing, so a pipeline started
// without --metrics needs no branch here. A constructor error returns the sink
// unwrapped: losing a counter is not worth failing a pipeline over.
func NewCountingSink(inner Sink, mp metric.MeterProvider, sinkType, role string) Sink {
	if mp == nil {
		mp = noop.NewMeterProvider()
	}
	meter := mp.Meter("sqlflow")

	accepted, err := meter.Int64Counter(
		"sink_rows_accepted",
		metric.WithDescription("Rows the sink buffered, which WriteTable accepted"),
		metric.WithUnit("rows"),
	)
	if err != nil {
		return inner
	}

	written, err := meter.Int64Counter(
		"sink_rows_written",
		metric.WithDescription("Rows the destination acknowledged, which a Flush delivered"),
		metric.WithUnit("rows"),
	)
	if err != nil {
		return inner
	}

	c := &counting{
		inner:    inner,
		accepted: accepted,
		written:  written,
		// Built once: the attribute set is the same for every row this sink
		// counts, and rebuilding it per batch allocates on the hot path.
		attrs: metric.WithAttributes(
			attribute.String("sink", sinkType),
			attribute.String("role", role),
		),
	}

	if reporter, ok := inner.(BufferedRowReporter); ok {
		return &countingBuffered{counting: c, reporter: reporter}
	}
	return c
}

func (c *counting) WriteTable(ctx context.Context, batch arrow.Table) error {
	if err := c.inner.WriteTable(ctx, batch); err != nil {
		return err
	}
	if batch == nil {
		return nil
	}

	n := batch.NumRows()
	c.mu.Lock()
	c.pending += n
	c.mu.Unlock()

	c.accepted.Add(ctx, n, c.attrs)
	return nil
}

// Flush counts on success only.
//
// WriteTable reaches nothing and only Flush does (sink.write.buffers_only), so
// counting at write time would report rows the destination never received --
// the defect #221 fixed. A failed flush keeps its rows buffered
// (sink.flush.keeps_batch), so the pending count carries forward and the retry
// that delivers them counts them once.
func (c *counting) Flush(ctx context.Context) error {
	if err := c.inner.Flush(ctx); err != nil {
		return err
	}

	c.mu.Lock()
	n := c.pending
	c.pending = 0
	c.mu.Unlock()

	if n > 0 {
		c.written.Add(ctx, n, c.attrs)
	}
	return nil
}

// pendingRows reports rows accepted since the last successful flush. For tests.
func (c *counting) pendingRows() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.pending
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test ./internal/core/ -run TestCounting -v`
Expected: PASS, all four tests.

- [ ] **Step 5: Commit**

```bash
git add internal/core/counting.go internal/core/counting_test.go
git commit -m "core: count accepted and delivered rows, on flush success

sink_flush_num_rows is a gauge, so rows written cannot be summed: it reported
244 while a pipeline had written 600,000 rows. Two counters replace it.

The delivered counter increments only when Flush returns nil. WriteTable
reaches nothing -- sink.write.buffers_only -- so counting there reports rows
the destination never received, which is the defect #221 fixed.

The decorator forwards BufferedRows when the inner sink reports it. The retry
wrapper declares only WriteTable and Flush, which is why sink_buffered_rows
never recorded for the ClickHouse and Iceberg sinks it wraps; repeating that
trap here would hide the depth from every sink.

If the pending count is wrong, a failed flush inflates the delivered total and
the metric reports delivery that did not happen."
```

---

### Task 4: Wire the decorator in, and remove sink_buffered_rows

**Files:**
- Modify: `internal/sinks/init.go`
- Modify: `internal/cli/run/root.go:51` and `:293`
- Modify: `internal/cli/run/managers.go:37`
- Modify: `internal/core/turbine.go` (drop `recordBufferedRows` and its two call sites)
- Modify: `internal/core/metrics.go` (drop `SinkBufferedRows`)
- Create: `internal/sinks/counting_wiring_test.go`
- Test: `internal/cli/run/metrics_test.go`

**Interfaces:**
- Consumes: `core.NewCountingSink` from Task 3. `countingBuffered` is unexported in `internal/core`, so this task's tests assert through behaviour rather than through the concrete type.
- Produces: `sinks.WithSinkRole(role string) Option`. Roles are exactly `"pipeline"`, `"dlq"`, `"manager"`.

`core.BufferedRowReporter` and the `sink.buffer.reports_depth` invariant stay. Only the instrument goes.

- [ ] **Step 1: Write the failing test**

Create `internal/sinks/counting_wiring_test.go`:

```go
package sinks

import (
	"context"
	"testing"

	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/core"
	"github.com/zeebo/assert"
)

// TestNewWrapsWithRowCounters pins the decorator's position: outermost, so one
// logical flush is one counted flush whatever the retry ladder does
// underneath. countingBuffered is unexported in internal/core, so this asserts
// the observable consequences instead of the concrete type.
func TestNewWrapsWithRowCounters(t *testing.T) {
	ctx := context.Background()
	s, err := New(ctx, config.Sink{Type: "console"}, nil,
		WithSinkRole("pipeline"))
	assert.NoError(t, err)

	// New never returns the bare sink once the decorator is wired.
	_, bare := s.(*ConsoleSink)
	assert.False(t, bare)

	// And the decorator forwards the console sink's depth report rather than
	// hiding it, which is the trap the retry wrapper fell into.
	_, reports := s.(core.BufferedRowReporter)
	assert.True(t, reports)
}

func TestWithSinkRoleSetsTheRole(t *testing.T) {
	var o options
	WithSinkRole("dlq")(&o)
	assert.Equal(t, "dlq", o.role)

	// Unset is empty here; New substitutes "pipeline".
	var d options
	assert.Equal(t, "", d.role)
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `go test ./internal/sinks/ -run 'TestNewWrapsWithRowCounters|TestWithSinkRole' -v`
Expected: FAIL to compile, `undefined: WithSinkRole`.

- [ ] **Step 3: Add the option and wire the decorator**

In `internal/sinks/init.go`, add to the `options` struct:

```go
type options struct {
	meterProvider metric.MeterProvider
	role          string
}
```

and the option:

```go
// WithSinkRole names what this sink is for: "pipeline", "dlq" or "manager".
//
// It separates the series. Without it, DLQ rows sum into the same counter as
// delivered rows and the end-to-end ratio overstates delivery -- the metric
// would report the pipeline healthier the more records it rejected.
func WithSinkRole(role string) Option {
	return func(o *options) { o.role = role }
}
```

Rewrite the tail of `New` so both return paths are wrapped:

```go
	role := o.role
	if role == "" {
		role = "pipeline"
	}

	policy := RetryPolicyFrom(sink.Retry)
	if !retriesHelp(sink.Type) || !policy.Enabled() {
		return core.NewCountingSink(built, o.meterProvider, sink.Type, role), nil
	}

	r := newRetrying(built, policy)
	r.onRetry = retryCounter(o.meterProvider, sink.Type)

	// Outside the ladder, so one logical flush is one counted flush. The
	// totals come out the same either way -- a failed attempt adds nothing --
	// but the invariant needs the position pinned to mean anything.
	return core.NewCountingSink(r, o.meterProvider, sink.Type, role), nil
```

In `internal/cli/run/root.go`, pass the role at both call sites. At line 51:

```go
			dlqSink, err := sinks.New(ctx, *onError.DLQ, conn,
				sinks.WithMeterProvider(meterProvider),
				sinks.WithSinkRole("dlq"))
```

At line 293:

```go
			sink, err := sinks.New(ctx, conf.Pipeline.Sink, conn,
				sinks.WithMeterProvider(meterProvider),
				sinks.WithSinkRole("pipeline"))
```

If `meterProvider` is not in scope at the DLQ call site, thread it through the enclosing function rather than dropping the option; a DLQ sink with no counters makes the ratio wrong in the direction that hides loss.

In `internal/cli/run/managers.go` at line 37:

```go
		sink, err := sinks.New(ctx, table.Manager.Sink, conn,
			sinks.WithMeterProvider(mp),
			sinks.WithSinkRole("manager"))
```

Thread the meter provider into the function building the managers if it is not already a parameter.

- [ ] **Step 4: Remove the gauge**

In `internal/core/turbine.go`, delete the `recordBufferedRows` method and both calls to it (one on the flush failure path, one after a successful flush). Delete the `SinkBufferedRows` field and its constructor block from `internal/core/metrics.go`.

Leave `BufferedRowReporter` in place. `internal/conformance/conformance.go:284` asserts on it, and the `sink.buffer.reports_depth` invariant is enforced through that assertion rather than through the metric.

- [ ] **Step 5: Update the exported name set**

In `internal/cli/run/metrics_test.go`, remove `"sink_buffered_rows"` from the `want` slice and remove the corresponding `m.SinkBufferedRows.Record(ctx, 1)` line from `exportedNames`.

- [ ] **Step 6: Run the tests to verify they pass**

Run: `go test ./internal/sinks/ ./internal/core/ ./internal/cli/run/`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add internal/sinks internal/core internal/cli/run
git commit -m "sinks: count every writer, and derive the buffer depth

Three call sites write to a sink: the pipeline, the DLQ, and each window
manager. A counter at the flush site in turbine.go would have reported zero
rows written for a windowed pipeline, whose output all comes from the manager.
sinks.New is the one constructor all three pass through.

The role attribute separates them. Without it, DLQ rows sum into the delivered
series and the pipeline looks healthier the more records it rejects.

sink_buffered_rows is removed. accepted minus written derives it, at any
scrape, with a rate the gauge could not give -- and the derived value cannot
misreport itself the way a sink's self-report can. BufferedRowReporter and
sink.buffer.reports_depth stay: that invariant is proven through the
conformance harness, not through the metric.

If the role attribute is missing anywhere, the end-to-end ratio overstates
delivery and hides exactly the loss this change exists to show."
```

---

### Task 5: Prove the invariant in the conformance harness

**Files:**
- Modify: `internal/conformance/pipeline.go`
- Modify: `docs/coverage/invariants.yml`
- Test: `internal/conformance/conformance_test.go`

**Interfaces:**
- Consumes: `core.NewCountingSink` from Task 3. No import problem: `internal/conformance` already imports `internal/core`. This is why Task 3 puts the decorator there — `conformance → sinks` would cycle the `sinks` test binary.

- [ ] **Step 1: Write the failing test**

Add to `internal/conformance/conformance_test.go`:

```go
// TestHarnessSinkCarriesRowCounters guards the reason this invariant is worth
// declaring. Conformance subjects build their sinks directly, and the harness
// wraps them in recordingSink, so nothing in that path passes through
// sinks.New where the counters live. Without this, the invariant asserts
// against an uninstrumented sink and passes while proving nothing.
func TestHarnessSinkCarriesRowCounters(t *testing.T) {
	s := newRecordingSink(&Recorder{}, nil, nil)

	// The counted wrapper is what the pipeline is handed.
	assert.True(t, s.counted != nil)
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `go test ./internal/conformance/ -run TestHarnessSinkCarriesRowCounters -v`
Expected: FAIL to compile, `undefined: newRecordingSink`.

- [ ] **Step 3: Wrap the harness sink**

In `internal/conformance/pipeline.go`, replace the inline construction at line 482 with a constructor, so the wrapping happens in one place:

```go
// newRecordingSink builds the sink the harness hands the pipeline.
//
// The row counters are applied here rather than left to sinks.New, which this
// path never reaches: subjects construct their sinks directly. Without them
// the sink.rows.counted_on_delivery invariant would assert against an
// uninstrumented sink and pass.
func newRecordingSink(rec *Recorder, inner core.Sink, mp metric.MeterProvider) *recordingSink {
	s := &recordingSink{rec: rec, inner: inner}
	s.counted = core.NewCountingSink(s, mp, "conformance", "pipeline")
	return s
}
```

Add `counted core.Sink` to the `recordingSink` struct, and hand `sink.counted` — not `sink` — to `core.NewTurbine` at line 542.

- [ ] **Step 4: Declare the invariant**

Add to `docs/coverage/invariants.yml`, in the resilience family, after `sink.flush.no_hollow_success`:

```yaml
  - id: sink.rows.counted_on_delivery
    family: resilience
    class: safety
    applies_to: sink
    claim: >
      sink_rows_written counts a row once, when a Flush acknowledged it. A
      failed flush counts nothing; the retry that delivers those rows counts
      them.
    verified_by: harness
    requires: []
```

Emit the `COVERS invariant=sink.rows.counted_on_delivery integration=...` marker from the harness alongside the existing markers, matching how `sink.flush.keeps_batch` reports.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `go test ./internal/conformance/ && go test ./internal/coverage/`
Expected: PASS. The coverage registry test holds `invariants.yml` consistent with what the harness emits, so a declared invariant with no marker fails there.

- [ ] **Step 6: Commit**

```bash
git add internal/conformance docs/coverage/invariants.yml
git commit -m "conformance: put the row counters in the harness sink, or the invariant proves nothing

sink.rows.counted_on_delivery claims a row is counted once, when a flush
acknowledged it. Verifying that through the harness requires the harness sink
to carry the counters, and it did not: subjects build sinks directly with
NewIcebergSink and friends, and recordingSink wraps that before handing it to
the pipeline. Nothing in the path reaches sinks.New.

The assertion would have passed against an uninstrumented sink -- coverage
reported, nothing proven, which is the failure mode the invariant matrix
exists to prevent.

If this wrapping is dropped, every subject reports the invariant as covered
while testing a sink that counts nothing."
```

---

### Task 6: Derive the reference tables from the handler SQL

A pure function over the DuckDB AST, tested without a pipeline.

**Files:**
- Create: `internal/core/reftables.go`
- Create: `internal/core/reftables_test.go`

**Interfaces:**
- Produces:
  - `core.RefTable{Catalog, Schema, Name string}` with method `Qualified() string`
  - `core.ReferenceTables(ctx context.Context, conn adbc.Connection, sql string, managed []string) ([]RefTable, error)`

- [ ] **Step 1: Write the failing test**

Create `internal/core/reftables_test.go`:

```go
package core

import (
	"context"
	"testing"

	"github.com/zeebo/assert"
)

// TestReferenceTablesFindsNestedJoin uses the csv.mem.join.yml handler SQL,
// whose join lives inside a subquery. A walk that only looked at the top-level
// FROM would miss the one table that matters.
func TestReferenceTablesFindsNestedJoin(t *testing.T) {
	conn := testConn(t)
	sql := `SELECT properties.city, state_full FROM batch
	        LEFT JOIN (SELECT * FROM locations
	                   WHERE locations.city = properties.city LIMIT 1) AS l
	        ON l.city = properties.city`

	got, err := ReferenceTables(context.Background(), conn, sql, nil)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(got))
	assert.Equal(t, "locations", got[0].Qualified())
}

// TestReferenceTablesExcludesCTEs is the false-positive case. DuckDB emits a
// CTE reference as a BASE_TABLE node, so a naive walk would try to count a
// name that does not exist outside its own query and warn on every start of
// every pipeline that uses WITH.
func TestReferenceTablesExcludesCTEs(t *testing.T) {
	conn := testConn(t)
	sql := `WITH recent AS (SELECT * FROM batch WHERE ts > now())
	        SELECT r.city, l.state FROM recent r
	        JOIN locations l ON l.city = r.city`

	got, err := ReferenceTables(context.Background(), conn, sql, nil)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(got))
	assert.Equal(t, "locations", got[0].Qualified())
}

// TestReferenceTablesExcludesManagedTables keeps a tumbling window's aggregate
// table out of the set. Those legitimately start empty; a dimension does not.
func TestReferenceTablesExcludesManagedTables(t *testing.T) {
	conn := testConn(t)
	sql := `SELECT * FROM batch JOIN agg_city_count a ON a.city = batch.city`

	got, err := ReferenceTables(context.Background(), conn, sql,
		[]string{"agg_city_count"})
	assert.NoError(t, err)
	assert.Equal(t, 0, len(got))
}

// TestReferenceTablesKeepsCatalogQualification separates an attached table
// from a local one, which decides whether it is counted or probed.
func TestReferenceTablesKeepsCatalogQualification(t *testing.T) {
	conn := testConn(t)
	sql := `SELECT * FROM batch LEFT JOIN pgusersdb.public.users u ON u.id = batch.user_id`

	got, err := ReferenceTables(context.Background(), conn, sql, nil)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(got))
	assert.Equal(t, "pgusersdb", got[0].Catalog)
	assert.Equal(t, "pgusersdb.public.users", got[0].Qualified())
}

// TestReferenceTablesUnparseableSQL returns an error rather than an empty set.
// An empty set and a query that could not be read are different facts, and
// silently reporting no reference tables is how a check stops checking.
func TestReferenceTablesUnparseableSQL(t *testing.T) {
	conn := testConn(t)
	_, err := ReferenceTables(context.Background(), conn, "SELECT FROM WHERE", nil)
	assert.Error(t, err)
}
```

Reuse the package's existing DuckDB test-connection helper for `testConn`. If none exists in `internal/core`, add one that opens an in-memory DuckDB through `duckdb.OpenPath(ctx, "")` and returns a connection, registering `t.Cleanup` to close both.

- [ ] **Step 2: Run the test to verify it fails**

Run: `go test ./internal/core/ -run TestReferenceTables -v`
Expected: FAIL to compile, `undefined: ReferenceTables`.

- [ ] **Step 3: Implement the walk**

Create `internal/core/reftables.go`:

```go
package core

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/apache/arrow-adbc/go/adbc"
)

// batchTable is the table the handler materializes for user SQL to select
// from. It is never a reference table.
const batchTable = "batch"

// RefTable is a table the handler SQL reads that the pipeline did not create.
type RefTable struct {
	Catalog string
	Schema  string
	Name    string
}

// Qualified renders the name as SQL can address it.
func (r RefTable) Qualified() string {
	parts := make([]string, 0, 3)
	if r.Catalog != "" {
		parts = append(parts, r.Catalog)
	}
	if r.Schema != "" {
		parts = append(parts, r.Schema)
	}
	return strings.Join(append(parts, r.Name), ".")
}

// Attached reports whether the table lives in an ATTACHed catalog, where a
// full count crosses the wire to somebody else's server.
func (r RefTable) Attached() bool { return r.Catalog != "" }

// ReferenceTables reports the tables the handler SQL joins.
//
// Deriving beats declaring. The operator adds no config, and the set tracks
// the SQL automatically rather than drifting from it when a table is renamed.
//
// managed names the tables the `tables:` block created. Those legitimately
// start empty -- a tumbling window's aggregate table is empty until the first
// window closes -- so they are not reference tables.
func ReferenceTables(
	ctx context.Context,
	conn adbc.Connection,
	sql string,
	managed []string,
) ([]RefTable, error) {
	ast, err := serializeSQL(ctx, conn, sql)
	if err != nil {
		return nil, err
	}

	var root any
	if err := json.Unmarshal([]byte(ast), &root); err != nil {
		return nil, fmt.Errorf("parse serialized sql: %w", err)
	}

	if m, ok := root.(map[string]any); ok {
		if e, ok := m["error"].(bool); ok && e {
			return nil, fmt.Errorf("handler sql did not parse: %v", m["error_message"])
		}
	}

	skip := map[string]bool{batchTable: true}
	for _, name := range managed {
		skip[strings.ToLower(name)] = true
	}
	// DuckDB emits a CTE reference as a BASE_TABLE node, so a CTE name is
	// indistinguishable from a real table until the declarations are
	// subtracted. Counting one fails, and every pipeline using WITH would warn
	// at every start.
	for _, name := range collectCTENames(root) {
		skip[strings.ToLower(name)] = true
	}

	var (
		out  []RefTable
		seen = map[string]bool{}
	)
	for _, tbl := range collectBaseTables(root) {
		if skip[strings.ToLower(tbl.Name)] {
			continue
		}
		q := tbl.Qualified()
		if seen[q] {
			continue
		}
		seen[q] = true
		out = append(out, tbl)
	}
	return out, nil
}

// serializeSQL returns DuckDB's JSON AST for the query, without executing it.
func serializeSQL(ctx context.Context, conn adbc.Connection, sql string) (string, error) {
	stmt, err := conn.NewStatement()
	if err != nil {
		return "", err
	}
	defer stmt.Close()

	// Bound as a literal through DuckDB's own quoting: handler SQL is operator
	// input and may contain any quote sequence.
	if err := stmt.SetSqlQuery(
		"SELECT json_serialize_sql(" + quoteLiteral(sql) + ")",
	); err != nil {
		return "", err
	}

	reader, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		return "", err
	}
	defer reader.Release()

	for reader.Next() {
		rec := reader.Record()
		if rec.NumRows() == 0 {
			continue
		}
		col, ok := rec.Column(0).(interface{ Value(int) string })
		if !ok {
			return "", fmt.Errorf("json_serialize_sql returned %T", rec.Column(0))
		}
		return col.Value(0), nil
	}
	return "", fmt.Errorf("json_serialize_sql returned no rows")
}

// quoteLiteral renders s as a single-quoted SQL string.
func quoteLiteral(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

// collectBaseTables walks every BASE_TABLE node, at any depth. Joins nest
// inside subqueries, so a top-level scan is not enough.
func collectBaseTables(n any) []RefTable {
	var out []RefTable
	switch v := n.(type) {
	case map[string]any:
		if v["type"] == "BASE_TABLE" {
			name, _ := v["table_name"].(string)
			if name != "" {
				catalog, _ := v["catalog_name"].(string)
				schema, _ := v["schema_name"].(string)
				out = append(out, RefTable{
					Catalog: catalog,
					Schema:  schema,
					Name:    name,
				})
			}
		}
		for _, child := range v {
			out = append(out, collectBaseTables(child)...)
		}
	case []any:
		for _, child := range v {
			out = append(out, collectBaseTables(child)...)
		}
	}
	return out
}

// collectCTENames walks every cte_map, at any depth, returning the names a
// WITH clause declares.
func collectCTENames(n any) []string {
	var out []string
	switch v := n.(type) {
	case map[string]any:
		if cm, ok := v["cte_map"].(map[string]any); ok {
			if entries, ok := cm["map"].([]any); ok {
				for _, e := range entries {
					if entry, ok := e.(map[string]any); ok {
						if key, ok := entry["key"].(string); ok && key != "" {
							out = append(out, key)
						}
					}
				}
			}
		}
		for _, child := range v {
			out = append(out, collectCTENames(child)...)
		}
	case []any:
		for _, child := range v {
			out = append(out, collectCTENames(child)...)
		}
	}
	return out
}
```

If the `cte_map` entry shape differs from `{"key": "...", "value": ...}`, print one serialized AST for a `WITH` query and adjust `collectCTENames` to the observed shape. The test in Step 1 is the arbiter.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test ./internal/core/ -run TestReferenceTables -v`
Expected: PASS, all five tests.

- [ ] **Step 5: Commit**

```bash
git add internal/core/reftables.go internal/core/reftables_test.go
git commit -m "core: derive the reference tables a handler query joins

A pipeline whose dimension table never loaded looks exactly like a healthy
one. Knowing which tables to check requires either a new config key the
operator maintains in parallel with their SQL, or reading the SQL. DuckDB's
json_serialize_sql returns the AST without executing anything, so the engine
reads it.

CTE names are excluded. DuckDB emits a CTE reference as a BASE_TABLE node, so
a naive walk counts a name that does not exist outside its own query -- every
pipeline using WITH would log a warning at every start, and a check that cries
wolf is worse than no check.

Managed tables are excluded too: a tumbling window's aggregate table is empty
until the first window closes.

If the walk is wrong, the startup check either misses the dimension it exists
to watch or warns about tables that were never meant to hold rows."
```

---

### Task 7: Count the reference tables at startup

**Files:**
- Modify: `internal/core/reftables.go`
- Modify: `internal/core/metrics.go`
- Modify: `internal/cli/run/root.go`
- Modify: `internal/cli/dev.go`
- Test: `internal/core/reftables_test.go`
- Test: `internal/cli/run/metrics_test.go`

**Interfaces:**
- Consumes: `ReferenceTables` and `RefTable` from Task 6; `core.Metrics` from Task 2.
- Produces: `core.CheckReferenceTables(ctx context.Context, conn adbc.Connection, conf *config.Conf, m *Metrics, l *zap.Logger) error`

- [ ] **Step 1: Write the failing test**

Add to `internal/core/reftables_test.go`:

```go
// TestCheckReferenceTablesWarnsOnEmpty is the catastrophic case: a dimension
// table that loaded nothing, so every joined row takes the unmatched path.
func TestCheckReferenceTablesWarnsOnEmpty(t *testing.T) {
	conn := testConn(t)
	exec(t, conn, "CREATE TABLE dim (id INTEGER)")

	logs, logger := observedLogger(t)
	conf := confWithHandlerSQL("SELECT * FROM batch JOIN dim ON dim.id = batch.id")

	assert.NoError(t, CheckReferenceTables(context.Background(), conn, conf, nil, logger))

	assert.Equal(t, 1, logs.FilterMessageSnippet("0 rows").Len())
}

func TestCheckReferenceTablesLogsCount(t *testing.T) {
	conn := testConn(t)
	exec(t, conn, "CREATE TABLE dim AS SELECT * FROM range(14203) t(id)")

	logs, logger := observedLogger(t)
	conf := confWithHandlerSQL("SELECT * FROM batch JOIN dim ON dim.id = batch.id")

	assert.NoError(t, CheckReferenceTables(context.Background(), conn, conf, nil, logger))

	entries := logs.FilterMessageSnippet("reference table").All()
	assert.Equal(t, 1, len(entries))
	assert.Equal(t, int64(14203), entries[0].ContextMap()["rows"])
}

// TestCheckReferenceTablesSurvivesACountError keeps a startup diagnostic from
// becoming a startup failure. A table that does not exist fails later at
// prepare, which is static validation's job.
func TestCheckReferenceTablesSurvivesACountError(t *testing.T) {
	conn := testConn(t)

	logs, logger := observedLogger(t)
	conf := confWithHandlerSQL("SELECT * FROM batch JOIN nonexistent n ON n.id = batch.id")

	assert.NoError(t, CheckReferenceTables(context.Background(), conn, conf, nil, logger))
	assert.True(t, logs.FilterLevelExact(zapcore.WarnLevel).Len() >= 1)
}
```

Add helpers to the same file: `exec(t, conn, sql)` running a statement and failing the test on error; `observedLogger(t)` returning `(*observer.ObservedLogs, *zap.Logger)` built with `zap.New(core)` from `go.uber.org/zap/zaptest/observer`; and `confWithHandlerSQL(sql string) *config.Conf` returning a `&config.Conf{}` whose `Pipeline.Handler.SQL` is set.

- [ ] **Step 2: Run the test to verify it fails**

Run: `go test ./internal/core/ -run TestCheckReferenceTables -v`
Expected: FAIL to compile, `undefined: CheckReferenceTables`.

- [ ] **Step 3: Add the gauge**

In `internal/core/metrics.go`, add the field beside `StateTableRows`:

```go
	ReferenceTableRows metric.Int64Gauge
```

```go
	// Recorded once, at startup. The value is what the table held when the
	// pipeline started, and the pipeline does not re-count: rescanning a CSV
	// or crossing the wire to Postgres on an interval is a cost the operator
	// never asked for.
	if m.ReferenceTableRows, err = meter.Int64Gauge(
		"reference_table_rows",
		metric.WithDescription("Rows a table joined by the handler SQL held when the pipeline started"),
		metric.WithUnit("rows"),
	); err != nil {
		return nil, fmt.Errorf("reference_table_rows: %w", err)
	}
```

- [ ] **Step 4: Implement the check**

Append to `internal/core/reftables.go`:

```go
// CheckReferenceTables counts the tables the handler SQL joins and reports
// what it found.
//
// This is a diagnostic, not a gate. A count that fails logs a warning and the
// pipeline starts: a table that does not exist fails later at prepare, which
// is static validation's job, and refusing to start on an empty dimension is a
// UX decision nobody has made yet.
//
// m may be nil, for callers with no metrics.
func CheckReferenceTables(
	ctx context.Context,
	conn adbc.Connection,
	conf *config.Conf,
	m *Metrics,
	l *zap.Logger,
) error {
	sql := conf.Pipeline.Handler.SQL
	if strings.TrimSpace(sql) == "" {
		return nil
	}

	var managed []string
	if conf.Tables != nil {
		for _, t := range conf.Tables.SQL {
			managed = append(managed, t.Name)
		}
	}

	tables, err := ReferenceTables(ctx, conn, sql, managed)
	if err != nil {
		// Unreadable SQL is the static validator's finding, not a reason to
		// stop a pipeline that DuckDB may still accept.
		l.Warn("could not derive reference tables from handler sql", zap.Error(err))
		return nil
	}

	for _, tbl := range tables {
		start := time.Now()

		if tbl.Attached() {
			// A full count on an ATTACHed catalog is a sequential scan on
			// somebody else's server. The difference between some rows and
			// none is the fact worth having.
			has, err := probeHasRows(ctx, conn, tbl)
			if err != nil {
				l.Warn("could not probe reference table",
					zap.String("table", tbl.Qualified()), zap.Error(err))
				continue
			}
			if !has {
				l.Warn("reference table is empty, joined by handler SQL",
					zap.String("table", tbl.Qualified()),
					zap.Duration("took", time.Since(start)))
				continue
			}
			l.Info("reference table has rows",
				zap.String("table", tbl.Qualified()),
				zap.Duration("took", time.Since(start)))
			continue
		}

		n, err := countRows(ctx, conn, tbl)
		if err != nil {
			l.Warn("could not count reference table",
				zap.String("table", tbl.Qualified()), zap.Error(err))
			continue
		}

		took := time.Since(start)
		if m != nil {
			m.ReferenceTableRows.Record(ctx, n,
				metric.WithAttributes(attribute.String("table", tbl.Qualified())))
		}

		if n == 0 {
			l.Warn("reference table has 0 rows, joined by handler SQL",
				zap.String("table", tbl.Qualified()),
				zap.Int64("rows", n),
				zap.Duration("took", took))
			continue
		}
		l.Info("reference table loaded",
			zap.String("table", tbl.Qualified()),
			zap.Int64("rows", n),
			zap.Duration("took", took))
	}
	return nil
}
```

Write `countRows` and `probeHasRows` as small helpers in the same file, each opening a statement, running `SELECT COUNT(*) FROM <qualified>` and `SELECT EXISTS(SELECT 1 FROM <qualified> LIMIT 1)` respectively, and reading the single scalar from the returned record. Add the imports `time`, `go.uber.org/zap`, `go.opentelemetry.io/otel/attribute`, `go.opentelemetry.io/otel/metric`, and `github.com/turbolytics/sql-flow/internal/config`.

- [ ] **Step 5: Wire it into both commands**

In `internal/cli/run/root.go`, immediately after the `core.InitTables` call:

```go
			// A diagnostic, on the signal context so a SIGTERM during startup
			// stops it rather than waiting out a count.
			if err := core.CheckReferenceTables(ctx, conn, conf, metrics, l); err != nil {
				return err
			}
```

Place it after `metrics` exists. If the `core.Metrics` value is constructed later in the function, move this call below that construction rather than passing nil, so the gauge is recorded.

In `internal/cli/dev.go`, immediately after the `core.InitTables` call:

```go
	// dev invoke runs it too: the developer iterating on a join against a
	// fixture is the person most likely to have an empty dimension table.
	if err := core.CheckReferenceTables(ctx, conn, conf, nil, logger); err != nil {
		return nil, err
	}
```

Use whichever context and `*zap.Logger` are already in scope in that function.

- [ ] **Step 6: Add the gauge to the exported name set**

In `internal/cli/run/metrics_test.go`, add `m.ReferenceTableRows.Record(ctx, 1)` to `exportedNames` and `"reference_table_rows"` to the `want` slice, sorted.

- [ ] **Step 7: Run the tests to verify they pass**

Run: `go test ./internal/core/ ./internal/cli/... `
Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add internal/core internal/cli
git commit -m "core: report what the joined reference tables held at startup

The catastrophic enrichment failure is a dimension table that did not load.
Nothing made it visible, and no example in this repo materializes one -- every
join example uses CREATE VIEW or ATTACH, so a row count logged when a command
creates a table would have fired on none of them.

The count is chosen by catalog. A local table is counted outright: COUNT(*)
over a 417MB, 20M-row CSV view measured 0.37s, cheap enough to be
unconditional. An ATTACHed table is probed with EXISTS, because a count there
is a sequential scan on another server.

dev invoke runs it too. Wiring it only into run would hide the warning from
the workflow built for finding this.

A failed count warns and startup proceeds. If that became fatal, a permissions
change on an unrelated table would stop a pipeline that DuckDB would have
run."
```

---

### Task 8: Document the whole surface

**Files:**
- Modify: `README.md`
- Test: `internal/cli/run/metrics_test.go`

- [ ] **Step 1: Rewrite the metrics section**

Replace the instrument table in `README.md` (the section beginning "Twelve instruments are exported") with nineteen instruments in six groups, each row giving the instrument name, the exported Prometheus name, the type, the attributes and a one-line description. The groups are: row accounting, sink health, pipeline, source, durable state, and webhook source.

State two facts the current section omits:

1. The webhook pair records under the meter `sqlflow.sources.http`, not `sqlflow`, and appears only when a webhook source is configured.
2. `reference_table_rows` appears whenever the handler SQL joins a table, with or without a state path, so it is not in the state group.

Add a short subsection under the table on reading the ratios:

```markdown
### Reading the row counts

Four counts follow a row through the pipeline:

    message_count_messages_total   messages the source delivered
    handler_rows_read_total        rows the SQL ran over
    sink_rows_accepted_total       rows the sink buffered
    sink_rows_written_total        rows the destination acknowledged

Each adjacent ratio isolates one kind of loss. `handler_rows_read` over
`message_count` is parse and DLQ loss. `sink_rows_accepted` over
`handler_rows_read` is whatever the SQL does — a join that drops, a `WHERE`, a
`GROUP BY` — so its meaning depends on the pipeline. `sink_rows_written` over
`sink_rows_accepted` is delivery loss.

Every ratio is a floor, not an equality. sqlflow is at-least-once: a crash
between the flush and the offset commit replays the batch, and the sink writes
those rows again, so a ratio can exceed 1 after a normal recovery.

Alert on a ratio that is low. Never alert on one that is not exactly 1.

The sink's buffer depth is `sink_rows_accepted_total -
sink_rows_written_total`.
```

- [ ] **Step 2: Verify the README matches the code**

Run: `go test ./internal/cli/run/ -run TestExportedSeriesNames -v`
Expected: PASS with nineteen names.

Then read the `want` slice and confirm every name in it appears in the README table, and that the README lists no name absent from it. Fix whichever is wrong.

- [ ] **Step 3: Run the full build and test suite**

Run: `go build ./... && go test ./internal/...`
Expected: PASS. Report any container-dependent package that is skipped rather than claiming a clean run.

- [ ] **Step 4: Commit**

```bash
git add README.md
git commit -m "docs: document all nineteen instruments, and how to read the ratios

The metrics section claimed twelve instruments where sixteen existed. It
omitted sink_retry_count and both webhook instruments, which record under the
sqlflow.sources.http meter rather than sqlflow, so an operator looking for them
had no reason to think they were there.

Each row now gives the exported Prometheus name beside the instrument name.
The two differ -- the exporter appends the unit and then _total -- and only the
exported name is queryable.

The ratios are documented as floors. At-least-once replay can push them above
1, so an equality alert would page on a healthy restart.

TestExportedSeriesNames asserts the set this table describes, so a future
instrument fails the build until it is documented here."
```

---

## Self-Review

**Spec coverage.** Every section of the spec maps to a task: the four counts (Tasks 2, 3, 4), counting on flush success (Task 3), removing `sink_buffered_rows` (Task 4), `RowsRead` (Task 2), the reference-table check including the CTE exclusion and `dev invoke` (Tasks 6, 7), the instrument table (Tasks 1–7), the invariant and its harness prerequisite (Task 5), and all four surface repairs (Tasks 1, 8).

**Two spec details corrected here.** The spec says `RowsRead` lands on four handlers; `internal/handlers/noop.go` declares `Init() error` and `Invoke() (*arrow.Table, error)`, which do not satisfy `core.Handler`, so it is dead code and Task 2 leaves it alone. The spec does not mention that a decorator hides optional interfaces; Task 3 forwards `BufferedRows` through the two-type pattern, because the retry wrapper's failure to do so is why `sink_buffered_rows` never recorded for ClickHouse and Iceberg.

**One spec assumption was checked and overturned.** The spec puts the counting
decorator in `internal/sinks`. The conformance harness must apply it, and five
test files in **package `sinks`** — not `sinks_test` — import
`internal/conformance`, so a `conformance → sinks` import cycles the `sinks`
test binary. The decorator therefore lives in `internal/core`, which both
packages already import, and `sinks.New` calls it. Verified with `go list
-deps` and the test files' package declarations on 2026-09-09. The design is
unchanged: the decorator is still applied inside `sinks.New`, which is what
gives it the DLQ and the window managers.

**No open risks remain.** Every task's preconditions were checked against the
code rather than assumed.
