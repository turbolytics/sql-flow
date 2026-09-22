# TurboStats Durations, Errors and Wait Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** The bundle says how long the work takes, which phase is failing, and whether the consume loop is waiting for input, all from instruments the engine already keeps.

**Architecture:** `walk` in `internal/turbostats/collect.go` reads only integer sums and gauges today. It learns to read float sums and histograms, and to aggregate the dimensional error counter by phase. `Collect` fills three new groups of fields. The one new instrument is a dimensionless duration histogram for `sqlflow serve`, whose existing one carries attributes.

**Tech Stack:** Go 1.26, OpenTelemetry Go SDK 1.46 (`metricdata.Histogram[float64]`, `Sum[float64]`), `github.com/zeebo/assert`, the repo's `internal/coverage` markers.

**Spec:** `docs/superpowers/specs/2026-09-22-turbostats-v1-signals-design.md`, sections "Durations", "Errors" and "Queue state". This is PR 1 of that spec's four.

## Global Constraints

- A process's field set is fixed from its first report to its last. Nothing keyed by what the process discovers at runtime.
- `count`, `sum_seconds` and every bucket are counters since process start. `min_seconds` and `max_seconds` are since process start and never reset: two readers exist, the reporter and `GET /turbostats/v1`, and a reset on read would hide events from the other.
- The wire's bucket boundaries are `0.001, 0.01, 0.05, 0.25, 1, 5, 30, 60`, in seconds, with a ninth count for everything above 60. They live in the contract, not in the bundle.
- A phase's buckets sum to its `count`, and `min_seconds` is at most `max_seconds`.
- Absent is not zero. A section that measured nothing sends no duration rather than zeros.
- No error message on the wire, ever. The code and the timestamp only.
- The document stays `v1`. Every field is additive.
- Every new test carries `coverage.Covers(t, "observability.turbostats")`, or `"observability.turbostats.serve"` in `internal/serve`.
- Prose follows the repo's CLAUDE.md: SQLFlow in prose, `sqlflow` for the command, why over what in comments.

---

## File Structure

- Modify `turbostats/wire/bundle.go`: the `Duration` type, the two duration groups, the error fields, `recv_wait_seconds`.
- Modify `turbostats/wire/bundle_test.go`.
- Modify `internal/turbostats/collect.go`: `walk` reads float sums and histograms; `dimensional` aggregates errors by phase and DLQ rows; `pipelineSection` and `serveSection` fill the new fields.
- Modify `internal/turbostats/bundle.go`: `PipelineSource.LastError`.
- Modify `internal/turbostats/collect_test.go`.
- Modify `internal/core/metrics.go`: the `recv_wait_seconds` counter.
- Modify `internal/core/turbine.go`: record the wait, and keep the last error's code.
- Modify `internal/core/flat_test.go`.
- Modify `internal/cli/run/root.go`: wire `LastError`.
- Modify `internal/serve/metrics.go`: the dimensionless request duration.
- Modify `internal/serve/turbostats_test.go`.
- Modify `docs/superpowers/specs/2026-09-19-turbostats-contract-amendment-design.md` and `CHANGELOG.md`.

---

### Task 1: The wire types

**Files:**
- Modify: `turbostats/wire/bundle.go`
- Test: `turbostats/wire/bundle_test.go`

**Interfaces:**
- Produces:
  - `wire.DurationBounds = []float64{0.001, 0.01, 0.05, 0.25, 1, 5, 30, 60}`
  - `type wire.Duration struct { Count uint64; SumSeconds, MinSeconds, MaxSeconds float64; Buckets []uint64 }`
  - `type wire.PipelineDurations struct { Batch, SinkFlush *Duration }`
  - `type wire.ServeDurations struct { Request *Duration }`
  - `Pipeline.Duration *PipelineDurations`, `Serve.Duration *ServeDurations`
  - `Pipeline.SourceErrorCount, HandlerErrorCount, SinkErrorCount, StateErrorCount, DLQRows *int64`
  - `Pipeline.LastErrorCode *string`, `Pipeline.LastErrorAt *time.Time`
  - `Pipeline.RecvWaitSeconds *float64`

- [ ] **Step 1: Write the failing test**

Append to `turbostats/wire/bundle_test.go`:

```go
// The wire's buckets are the contract's, not the engine's: nine counts
// against eight boundaries, so a receiver reads a distribution without being
// told the boundaries.
func TestDuration_HasNineCountsAgainstEightBounds(t *testing.T) {
	if len(DurationBounds) != 8 {
		t.Fatalf("DurationBounds has %d entries", len(DurationBounds))
	}
	for i := 1; i < len(DurationBounds); i++ {
		if DurationBounds[i] <= DurationBounds[i-1] {
			t.Fatalf("DurationBounds is not ascending at %d", i)
		}
	}
	d := Duration{Count: 3, SumSeconds: 0.5, MinSeconds: 0.1, MaxSeconds: 0.3,
		Buckets: make([]uint64, len(DurationBounds)+1)}
	raw := mustMarshal(t, d)
	for _, want := range []string{`"count":3`, `"sum_seconds":0.5`,
		`"min_seconds":0.1`, `"max_seconds":0.3`, `"buckets":[0,0,0,0,0,0,0,0,0]`} {
		if !strings.Contains(raw, want) {
			t.Errorf("a duration is missing %s: %s", want, raw)
		}
	}
}

// A section that measured nothing sends no duration at all. Zeros would say
// the work happened and took no time.
func TestPipeline_DurationsAndErrorsAreAbsentUntilMeasured(t *testing.T) {
	raw := mustMarshal(t, Bundle{V: Version, Pipeline: &Pipeline{}})
	for _, absent := range []string{"duration", "source_error_count",
		"handler_error_count", "sink_error_count", "state_error_count",
		"dlq_rows", "last_error_code", "last_error_at", "recv_wait_seconds"} {
		if strings.Contains(raw, absent) {
			t.Errorf("an unmeasured pipeline carries %s: %s", absent, raw)
		}
	}
}

// Zero is a reading: a pipeline that has failed nothing reports zero
// errors, which is different from one whose engine does not count them.
func TestPipeline_ZeroErrorsArePresent(t *testing.T) {
	zero := int64(0)
	raw := mustMarshal(t, Bundle{V: Version, Pipeline: &Pipeline{SinkErrorCount: &zero}})
	if !strings.Contains(raw, `"sink_error_count":0`) {
		t.Errorf("a zero error count is absent: %s", raw)
	}
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test -short ./turbostats/wire/ -run 'TestDuration_|TestPipeline_Durations|TestPipeline_ZeroErrors'`
Expected: FAIL, `undefined: DurationBounds`.

- [ ] **Step 3: Add the types**

In `turbostats/wire/bundle.go`, above the `Pipeline` type:

```go
// DurationBounds are the wire's histogram boundaries, in seconds, with a
// ninth bucket for everything above the last.
//
// They live here rather than in the bundle: nine counts carry a
// distribution, and a receiver knows what they mean without being told.
// They are also fixed across a fleet on purpose, because buckets that
// differ per instance cannot be summed.
var DurationBounds = []float64{0.001, 0.01, 0.05, 0.25, 1, 5, 30, 60}

// Duration is how long one kind of work takes.
//
// Count, SumSeconds and Buckets are counters since the process started, so a
// receiver subtracts two reports for an interval's distribution and sums
// buckets across a fleet. MinSeconds and MaxSeconds are since the process
// started and never reset: the reporter and GET /turbostats/v1 both read
// this, and a value reset on read would hide events from the other.
//
// Min and max earn their place beside the buckets because nobody knows a
// customer's workload. One whose every sample lands above the last boundary
// has a distribution that says nothing, and min, max, count and sum stay
// true regardless.
type Duration struct {
	Count      uint64  `json:"count"`
	SumSeconds float64 `json:"sum_seconds"`
	MinSeconds float64 `json:"min_seconds"`
	MaxSeconds float64 `json:"max_seconds"`
	// Buckets is len(DurationBounds)+1 counts, the last for everything above
	// the final boundary.
	Buckets []uint64 `json:"buckets"`
}

// PipelineDurations is how long the pipeline's work takes. A phase that has
// recorded nothing is absent.
type PipelineDurations struct {
	Batch     *Duration `json:"batch,omitempty"`
	SinkFlush *Duration `json:"sink_flush,omitempty"`
}

// ServeDurations is how long the dataset API's work takes.
type ServeDurations struct {
	Request *Duration `json:"request,omitempty"`
}
```

In the `Pipeline` struct, after `ErrorCount`:

```go
	// The phases errors are attributed to. Absent from engines that predate
	// them; zero is a reading, and means nothing failed in that phase.
	SourceErrorCount  *int64 `json:"source_error_count,omitempty"`
	HandlerErrorCount *int64 `json:"handler_error_count,omitempty"`
	SinkErrorCount    *int64 `json:"sink_error_count,omitempty"`
	StateErrorCount   *int64 `json:"state_error_count,omitempty"`
	// DLQRows is rows diverted to the dead-letter queue rather than dropped.
	DLQRows *int64 `json:"dlq_rows,omitempty"`
	// LastErrorCode is the engine's code, such as system.sink.unreachable.
	// The message never crosses the wire: it carries the row that failed, a
	// connection string, a customer's data. An operator with the code and
	// the time finds the message in their own logs.
	LastErrorCode *string    `json:"last_error_code,omitempty"`
	LastErrorAt   *time.Time `json:"last_error_at,omitempty"`
	// RecvWaitSeconds is how long the consume loop has spent waiting for
	// input. It separates a pipeline waiting on a quiet source from one
	// saturated by its own work.
	RecvWaitSeconds *float64 `json:"recv_wait_seconds,omitempty"`
	// Duration is how long its work takes.
	Duration *PipelineDurations `json:"duration,omitempty"`
```

In the `Serve` struct, after `RequestErrorCount`:

```go
	// Duration is how long a request takes, end to end.
	Duration *ServeDurations `json:"duration,omitempty"`
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test -short ./turbostats/wire/`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add turbostats/wire/
git commit -m "wire: durations, errors by phase, and the consume loop's wait

Nine bucket counts against boundaries the contract fixes, so a receiver
reads a distribution without being told them and can sum a fleet. Min and
max never reset, because two readers share this bundle."
```

---

### Task 2: Read histograms and float sums

**Files:**
- Modify: `internal/turbostats/collect.go` (`walk`, and a new `durations.go`-worthy helper kept in `collect.go` beside it)
- Test: `internal/turbostats/collect_test.go`

**Interfaces:**
- Consumes: `wire.Duration`, `wire.DurationBounds` from Task 1.
- Produces:
  - `walk` returns `(flat map[string]int64, floats map[string]float64, hist map[string]metricdata.HistogramDataPoint[float64], dim *dimensional)`
  - `func durationOf(dp metricdata.HistogramDataPoint[float64]) *Duration`, nil for an empty histogram or one whose boundaries do not cover the wire's
  - `func boundsCover(bounds []float64) bool`

- [ ] **Step 1: Write the failing test**

Append to `internal/turbostats/collect_test.go`:

```go
// The engine's boundaries are finer than the wire's, and every wire
// boundary is one of them, so the coarse counts are sums of fine ones and
// nothing is interpolated. If a boundary is ever dropped from
// latencyBuckets this test fails rather than silently reporting a
// distribution that is wrong.
func TestDurationOf_SumsTheEnginesBucketsIntoTheWires(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	reader, m, _ := provider(t)
	ctx := context.Background()
	for _, seconds := range []float64{0.0005, 0.002, 0.02, 0.2, 0.7, 3, 40, 120} {
		m.BatchProcessingLatency.Record(ctx, seconds)
	}

	b, err := Collect(ctx, runSource(reader, nil))
	assert.NoError(t, err)
	d := b.Pipeline.Duration.Batch
	assert.Equal(t, uint64(8), d.Count)
	assert.Equal(t, 9, len(d.Buckets))
	// Each sample lands in the first wire bucket whose boundary is at or
	// above it. Nothing lands in the 30 s bucket: the 40 s sample is the
	// next one up.
	assert.DeepEqual(t, []uint64{1, 1, 1, 1, 1, 1, 0, 1, 1}, d.Buckets)

	var sum float64
	for _, b := range d.Buckets {
		sum += float64(b)
	}
	assert.Equal(t, float64(d.Count), sum)
	assert.Equal(t, 0.0005, d.MinSeconds)
	assert.Equal(t, float64(120), d.MaxSeconds)
}

// A phase nothing recorded is absent, and so is the group when no phase
// recorded anything. Zeros would say the work happened and took no time.
func TestCollect_DurationsAreAbsentUntilRecorded(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	reader, _, _ := provider(t)
	b, err := Collect(context.Background(), runSource(reader, nil))
	assert.NoError(t, err)
	assert.That(t, b.Pipeline.Duration == nil)
}

func TestCollect_SinkFlushDurationIsItsOwnPhase(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	reader, m, _ := provider(t)
	ctx := context.Background()
	m.SinkFlushLatency.Record(ctx, 0.3)
	m.SinkFlushLatency.Record(ctx, 0.4)

	b, err := Collect(ctx, runSource(reader, nil))
	assert.NoError(t, err)
	assert.That(t, b.Pipeline.Duration.Batch == nil)
	assert.Equal(t, uint64(2), b.Pipeline.Duration.SinkFlush.Count)
	assert.Equal(t, 0.3, b.Pipeline.Duration.SinkFlush.MinSeconds)
	assert.Equal(t, 0.4, b.Pipeline.Duration.SinkFlush.MaxSeconds)
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test -short ./internal/turbostats/ -run 'TestDurationOf_|TestCollect_Durations|TestCollect_SinkFlush'`
Expected: FAIL, `b.Pipeline.Duration` is nil, no `durationOf`.

- [ ] **Step 3: Teach `walk` the other two shapes**

In `internal/turbostats/collect.go`, replace `walk`'s signature and add the two cases:

```go
func walk(rm metricdata.ResourceMetrics) (map[string]int64, map[string]float64,
	map[string]metricdata.HistogramDataPoint[float64], *dimensional) {

	flat := map[string]int64{}
	floats := map[string]float64{}
	hist := map[string]metricdata.HistogramDataPoint[float64]{}
	dim := &dimensional{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			switch data := m.Data.(type) {
			case metricdata.Sum[int64]:
				for _, dp := range data.DataPoints {
					if dp.Attributes.Len() == 0 {
						flat[m.Name] = dp.Value
						continue
					}
					dim.add(m.Name, dp.Attributes, dp.Value)
				}
			case metricdata.Gauge[int64]:
				for _, dp := range data.DataPoints {
					if dp.Attributes.Len() == 0 {
						flat[m.Name] = dp.Value
						continue
					}
					dim.add(m.Name, dp.Attributes, dp.Value)
				}
			case metricdata.Sum[float64]:
				// Seconds, not counts: recv_wait_seconds is the only one, and
				// an attributed float series is a phase histogram's twin that
				// the bundle does not carry.
				for _, dp := range data.DataPoints {
					if dp.Attributes.Len() == 0 {
						floats[m.Name] = dp.Value
					}
				}
			case metricdata.Histogram[float64]:
				// Dimensionless only. serve's attributed request histogram
				// has a flat twin, for the same reason its counters do: a
				// bundle field is one number, and summing an attributed
				// series here would invent one.
				for _, dp := range data.DataPoints {
					if dp.Attributes.Len() == 0 {
						hist[m.Name] = dp
					}
				}
			}
		}
	}
	return flat, floats, hist, dim
}
```

Update the one caller in `Collect`:

```go
	flat, floats, hist, dim := walk(rm)
```

Add the conversion beside it:

```go
// durationOf folds an engine histogram into the wire's nine buckets.
//
// It works only when every wire boundary is also an engine boundary: then
// each engine bucket sits wholly inside one wire bucket and a coarse count
// is a sum of fine ones, with nothing interpolated. A histogram that does
// not satisfy that is refused rather than folded into a distribution that
// is quietly wrong.
//
// An empty histogram is absent, not a zero duration: work that has not
// happened has no duration.
func durationOf(dp metricdata.HistogramDataPoint[float64]) *Duration {
	if dp.Count == 0 || !boundsCover(dp.Bounds) {
		return nil
	}
	d := &Duration{
		Count:      dp.Count,
		SumSeconds: dp.Sum,
		Buckets:    make([]uint64, len(wire.DurationBounds)+1),
	}
	if v, ok := dp.Min.Value(); ok {
		d.MinSeconds = v
	}
	if v, ok := dp.Max.Value(); ok {
		d.MaxSeconds = v
	}
	for i, count := range dp.BucketCounts {
		if count == 0 {
			continue
		}
		// Engine bucket i holds samples at or below Bounds[i]; the last
		// holds everything above the final boundary. Each lands in the
		// first wire bucket whose boundary is at or above that.
		target := len(wire.DurationBounds)
		if i < len(dp.Bounds) {
			for j, w := range wire.DurationBounds {
				if dp.Bounds[i] <= w {
					target = j
					break
				}
			}
		}
		d.Buckets[target] += count
	}
	return d
}

// boundsCover reports whether every wire boundary is one of the engine's.
// Without that, an engine bucket straddles a wire boundary and its samples
// cannot be attributed to either side.
func boundsCover(bounds []float64) bool {
	for _, w := range wire.DurationBounds {
		found := false
		for _, b := range bounds {
			if b == w {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}
```

In `pipelineSection`, before the `return`. A name the map does not hold
yields the zero data point, whose `Count` is zero, which `durationOf`
already reads as absent:

```go
	batch, flush := durationOf(hist["batch_processing_latency"]), durationOf(hist["sink_flush_latency"])
	if batch != nil || flush != nil {
		p.Duration = &PipelineDurations{Batch: batch, SinkFlush: flush}
	}
```

`pipelineSection` needs the map, so change its signature and its one call site in `Collect`:

```go
func pipelineSection(ctx context.Context, flat map[string]int64, floats map[string]float64,
	hist map[string]metricdata.HistogramDataPoint[float64], dim *dimensional,
	sentAt time.Time, src *PipelineSource) (*Pipeline, error) {
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test -short -race ./internal/turbostats/`
Expected: PASS. If `TestDurationOf_SumsTheEnginesBucketsIntoTheWires` fails on a bucket, print `dp.Bounds` and the engine's `latencyBuckets` and check every wire boundary is present in the engine's set.

- [ ] **Step 5: Commit**

```bash
git add internal/turbostats/ turbostats/
git commit -m "turbostats: the bundle carries how long a batch and a flush take

walk read integer sums and gauges only. It now reads float sums and
dimensionless histograms, and the fold into the wire's nine buckets is a
sum of the engine's, because every wire boundary is one of the engine's."
```

---

### Task 3: A dimensionless request duration for serve

**Files:**
- Modify: `internal/serve/metrics.go`
- Modify: `internal/turbostats/collect.go` (`serveSection`)
- Test: `internal/serve/turbostats_test.go`

**Interfaces:**
- Consumes: `wire.ServeDurations`, `durationOf` from Task 2.
- Produces: the instrument `serve_request_duration`, dimensionless, with `wire.DurationBounds`.

- [ ] **Step 1: Write the failing test**

Append to `internal/serve/turbostats_test.go`:

```go
// The bundle's request duration is a flat twin, as its counters are: the
// attributed histogram carries a dataset, a grain and a code, and a bundle
// field is one number.
func TestServeTurbostats_ReportsRequestDuration(t *testing.T) {
	coverage.Covers(t, "observability.turbostats.serve")
	ts := newTestServerWith(t, testServe, WithTurbostats(testStatic, true))
	assert.Equal(t, http.StatusOK, ts.get(t, "/v1/datasets/status").status)

	r := ts.do(t, http.MethodGet, "/turbostats/v1", nil)
	var b wire.Bundle
	assert.NoError(t, json.Unmarshal([]byte(r.raw), &b))
	assert.Equal(t, uint64(1), b.Serve.Duration.Request.Count)
	assert.Equal(t, len(wire.DurationBounds)+1, len(b.Serve.Duration.Request.Buckets))
	assert.That(t, b.Serve.Duration.Request.MaxSeconds > 0)
	var sum uint64
	for _, c := range b.Serve.Duration.Request.Buckets {
		sum += c
	}
	assert.Equal(t, b.Serve.Duration.Request.Count, sum)
}

// A server that has answered nothing sends no duration, the way it sends no
// last_request_at.
func TestServeTurbostats_NoRequestsNoDuration(t *testing.T) {
	coverage.Covers(t, "observability.turbostats.serve")
	ts := newTestServerWith(t, testServe, WithTurbostats(testStatic, true))
	r := ts.do(t, http.MethodGet, "/turbostats/v1", nil)
	var b wire.Bundle
	assert.NoError(t, json.Unmarshal([]byte(r.raw), &b))
	assert.That(t, b.Serve.Duration == nil)
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test -short ./internal/serve/ -run TestServeTurbostats_ReportsRequestDuration`
Expected: FAIL, nil dereference on `b.Serve.Duration`.

- [ ] **Step 3: Add the instrument and fill the section**

In `internal/serve/metrics.go`, add the field to the `metrics` struct beside the other flat twins:

```go
	// flatRequestDuration is the bundle's request duration. Its boundaries
	// are the wire's, so the bundle copies its buckets rather than folding
	// them: this server's own histograms stop at 16 s, and the wire's last
	// boundary is 60.
	flatRequestDuration metric.Float64Histogram
```

In `newMetrics`, add a second view beside the existing one, before the reader is built:

```go
		// The flat twin the bundle reads keeps the wire's boundaries. The
		// view above sets this workload's boundaries for every histogram,
		// and this one puts them back for the one instrument a receiver
		// reads, because buckets that differ per instance cannot be summed
		// across a fleet.
		sdkmetric.WithView(sdkmetric.NewView(
			sdkmetric.Instrument{Name: "serve_request_duration"},
			sdkmetric.Stream{Aggregation: sdkmetric.AggregationExplicitBucketHistogram{
				Boundaries: wire.DurationBounds,
			}},
		)),
```

and create the instrument beside `flatRequests`:

```go
	if mm.flatRequestDuration, err = m.Float64Histogram("serve_request_duration",
		metric.WithDescription("What a caller waited, any dataset, any outcome. The TurboStats bundle's duration.request."),
		metric.WithUnit("s")); err != nil {
		return nil, nil, err
	}
```

In `observeRequest`, beside the flat counters:

```go
	m.flatRequestDuration.Record(ctx, total.Seconds())
```

Add the import `"github.com/turbolytics/sql-flow/turbostats/wire"` to `internal/serve/metrics.go`.

In `internal/turbostats/collect.go`, `serveSection` takes the histogram map and fills the group:

```go
	if d := durationOf(hist["serve_request_duration"]); d != nil {
		sv.Duration = &ServeDurations{Request: d}
	}
```

Change `serveSection`'s signature to take `hist map[string]metricdata.HistogramDataPoint[float64]` and update its call in `Collect`. Add `ServeDurations = wire.ServeDurations` and `PipelineDurations = wire.PipelineDurations` and `Duration = wire.Duration` to the type alias block in `internal/turbostats/bundle.go`, beside the existing aliases.

- [ ] **Step 4: Run the tests**

Run: `go test -short -race ./internal/serve/ ./internal/turbostats/`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/serve/ internal/turbostats/
git commit -m "serve: a dimensionless request duration, on the wire's boundaries

The attributed histogram carries a dataset, a grain and a code, and a
bundle field is one number. The twin keeps the wire's boundaries because
this server's own stop at 16 s and the wire's last is 60."
```

---

### Task 4: Errors by phase, the DLQ, and the last error

**Files:**
- Modify: `internal/turbostats/collect.go` (`dimensional`, `pipelineSection`)
- Modify: `internal/turbostats/bundle.go` (`PipelineSource.LastError`)
- Modify: `internal/core/turbine.go` (keep the last code; expose it)
- Modify: `internal/cli/run/root.go` (wire it)
- Test: `internal/turbostats/collect_test.go`, `internal/core/turbine_test.go`

**Interfaces:**
- Consumes: the wire fields from Task 1.
- Produces:
  - `func (t *Turbine) LastError() (code string, at time.Time, ok bool)`
  - `PipelineSource.LastError func() (code string, at time.Time, ok bool)`

- [ ] **Step 1: Write the failing tests**

Append to `internal/turbostats/collect_test.go`:

```go
// One error_count told an operator that something failed. The phase tells
// them where to look, and the four are the phases the engine already
// attributes errors to.
func TestCollect_ErrorsAreCountedPerPhase(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	reader, m, _ := provider(t)
	ctx := context.Background()
	record := func(phase string, n int) {
		for i := 0; i < n; i++ {
			m.ErrorCount.Add(ctx, 1, metric.WithAttributes(
				attribute.String("class", "system"),
				attribute.String("domain", "sink"),
				attribute.String("code", "system.sink.unreachable"),
				attribute.String("phase", phase),
			))
		}
	}
	record("source.read", 2)
	record("handler.invoke", 3)
	record("sink.flush", 5)
	record("state.commit", 7)

	b, err := Collect(ctx, runSource(reader, nil))
	assert.NoError(t, err)
	assert.Equal(t, int64(2), *b.Pipeline.SourceErrorCount)
	assert.Equal(t, int64(3), *b.Pipeline.HandlerErrorCount)
	assert.Equal(t, int64(5), *b.Pipeline.SinkErrorCount)
	assert.Equal(t, int64(7), *b.Pipeline.StateErrorCount)
}

// A pipeline that has failed nothing reports zeros, not absence: the engine
// counts these, and zero is a reading.
func TestCollect_NoErrorsReportsZeroPerPhase(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	reader, _, _ := provider(t)
	b, err := Collect(context.Background(), runSource(reader, nil))
	assert.NoError(t, err)
	assert.Equal(t, int64(0), *b.Pipeline.SourceErrorCount)
	assert.Equal(t, int64(0), *b.Pipeline.SinkErrorCount)
}

// The DLQ's rows are not the pipeline's rows, and they are not errors
// either: they are what a row's failure cost. They come from the counting
// sink's role attribute.
func TestCollect_DLQRowsComeFromTheDLQsRole(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	reader, _, meter := provider(t)
	ctx := context.Background()
	written, err := meter.Int64Counter("sink_rows_written")
	assert.NoError(t, err)
	written.Add(ctx, 9, metric.WithAttributes(
		attribute.String("sink", "kafka"), attribute.String("role", "dlq")))
	written.Add(ctx, 400, metric.WithAttributes(
		attribute.String("sink", "postgres"), attribute.String("role", "pipeline")))

	b, err := Collect(ctx, runSource(reader, nil))
	assert.NoError(t, err)
	assert.Equal(t, int64(9), *b.Pipeline.DLQRows)
}

// The code and the time, never the message: a message carries the row that
// failed and whatever was in it.
func TestCollect_CarriesTheLastErrorCodeAndTime(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	reader, _, _ := provider(t)
	at := time.Date(2026, 9, 22, 12, 0, 0, 0, time.UTC)
	src := runSource(reader, nil)
	src.Pipeline.LastError = func() (string, time.Time, bool) {
		return "user.sink.encode_failed", at, true
	}

	b, err := Collect(context.Background(), src)
	assert.NoError(t, err)
	assert.Equal(t, "user.sink.encode_failed", *b.Pipeline.LastErrorCode)
	assert.Equal(t, at, b.Pipeline.LastErrorAt.UTC())
}

func TestCollect_NoErrorYetCarriesNoCode(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	reader, _, _ := provider(t)
	src := runSource(reader, nil)
	src.Pipeline.LastError = func() (string, time.Time, bool) { return "", time.Time{}, false }

	b, err := Collect(context.Background(), src)
	assert.NoError(t, err)
	assert.That(t, b.Pipeline.LastErrorCode == nil)
	assert.That(t, b.Pipeline.LastErrorAt == nil)
}
```

Append to `internal/core/turbine_test.go`:

```go
// The bundle reports the last error's code, so the turbine keeps it beside
// the timestamp it already kept.
func TestCoreRecordError_KeepsTheLastCode(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	tb, _ := meteredTurbine(t, &fakeSource{}, &fakeSink{}, 10)
	_, _, ok := tb.LastError()
	assert.That(t, !ok)

	tb.recordError(context.Background(),
		errs.New(errs.CodeSinkUnreachable, "sink", errors.New("connection refused")),
		phaseSinkFlush, "flush failed")

	code, at, ok := tb.LastError()
	assert.That(t, ok)
	assert.Equal(t, string(errs.CodeSinkUnreachable), code)
	assert.That(t, !at.IsZero())
}
```

Before running, check `internal/errs` for the constructor and the code constant this test names: run `grep -n 'func New' internal/errs/*.go` and `grep -rn 'sink.unreachable' internal/errs/*.go`, and use the real names. The test's point is that `LastError` returns what `recordError` recorded.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test -short ./internal/turbostats/ -run TestCollect_Errors; go test -short ./internal/core/ -run TestCoreRecordError_KeepsTheLastCode`
Expected: FAIL, `dim.errors` and `LastError` are undefined.

- [ ] **Step 3: Aggregate the errors and keep the code**

In `internal/turbostats/collect.go`, add to the `dimensional` struct:

```go
	// errorsByPhase is the four phases the bundle reports, summed from the
	// attributed error counter. The keys are fixed: a map that grew with the
	// data would make one instance an unbounded number of series.
	errorsSeen                                  bool
	errSource, errHandler, errSink, errState    int64
	dlqRows                                     int64
```

In `dimensional.add`, beside the existing cases, add:

```go
	case "error_count":
		phase, _ := value(attrs, "phase")
		d.errorsSeen = true
		switch {
		case strings.HasPrefix(phase, "source."):
			d.errSource += v
		case strings.HasPrefix(phase, "handler."):
			d.errHandler += v
		case strings.HasPrefix(phase, "sink."):
			d.errSink += v
		case strings.HasPrefix(phase, "state."):
			d.errState += v
		}
	case "sink_rows_written":
		if role, _ := value(attrs, "role"); role == "dlq" {
			d.dlqRows += v
		}
```

`value(attrs, key)` is how `add` already reads an attribute; if the existing code uses a different helper, use that one. Read `dimensional.add` before writing this step's code.

In `pipelineSection`, after the existing fields:

```go
	// Always sent, zeros included: the engine counts these, and a pipeline
	// that has failed nothing has failed nothing.
	p.SourceErrorCount = &dim.errSource
	p.HandlerErrorCount = &dim.errHandler
	p.SinkErrorCount = &dim.errSink
	p.StateErrorCount = &dim.errState
	p.DLQRows = &dim.dlqRows

	if src.LastError != nil {
		if code, at, ok := src.LastError(); ok {
			p.LastErrorCode = &code
			stamped := at.UTC().Truncate(time.Second)
			p.LastErrorAt = &stamped
		}
	}
```

In `internal/turbostats/bundle.go`, add to `PipelineSource`:

```go
	// LastError is the code and time of the last error the pipeline
	// recorded, and false before the first. The message stays in the
	// process: it carries the row that failed.
	LastError func() (code string, at time.Time, ok bool)
```

In `internal/core/turbine.go`, beside `lastErrorUnixNano`, add a field to the `Turbine` struct:

```go
	// lastErrorCode is the code of the last error recorded, for the bundle.
	// An atomic.Value rather than a lock: recordError runs on the consume
	// loop and the reporter reads from its own goroutine.
	lastErrorCode atomic.Value
```

In `recordError`, after `code := errs.CodeOf(err)`:

```go
	t.lastErrorCode.Store(string(code))
```

and add the reader:

```go
// LastError is the code and time of the last error this pipeline recorded.
// ok is false before the first one.
func (t *Turbine) LastError() (string, time.Time, bool) {
	nanos := t.lastErrorUnixNano.Load()
	code, _ := t.lastErrorCode.Load().(string)
	if nanos == 0 || code == "" {
		return "", time.Time{}, false
	}
	return code, time.Unix(0, nanos), true
}
```

In `internal/cli/run/root.go`, where the `turbostats.PipelineSource` is built, add `LastError: pipeline.LastError` (the variable holding the `*core.Turbine`; read the file to find its name). If the source is built before the turbine exists, pass a closure that reads a captured pointer once it is set.

- [ ] **Step 4: Run the tests**

Run: `go test -short -race ./internal/turbostats/ ./internal/core/`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/turbostats/ internal/core/ internal/cli/
git commit -m "turbostats: errors by phase, the DLQ's rows, and the last code

One error_count said something failed. The phase says where to look. The
code and the time cross the wire and the message never does: it carries
the row that failed and whatever was in it."
```

---

### Task 5: The consume loop's wait

**Files:**
- Modify: `internal/core/metrics.go`
- Modify: `internal/core/turbine.go` (the consume loop's `totalRecvWait`)
- Modify: `internal/turbostats/collect.go` (`pipelineSection`)
- Test: `internal/core/flat_test.go`, `internal/turbostats/collect_test.go`

**Interfaces:**
- Consumes: `Pipeline.RecvWaitSeconds` from Task 1, the `floats` map from Task 2.
- Produces: the instrument `pipeline_recv_wait_seconds`, a float counter.

- [ ] **Step 1: Write the failing tests**

Append to `internal/core/flat_test.go`:

```go
// A quiet source and a saturated engine look identical from outside: both
// report a low rate. The wait separates them, so it is a counter the bundle
// can read rather than a number in a log line.
func TestCoreConsumeLoop_CountsTimeWaitingForInput(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	src := &fakeSource{batches: [][]Message{messages(10)}}
	tb, reader := meteredTurbine(t, src, &fakeSink{}, 10)

	_, err := tb.ConsumeLoop(context.Background(), 0)
	assert.NoError(t, err)

	assert.That(t, flatFloat(t, reader, "pipeline_recv_wait_seconds") >= 0)
}
```

Add the float reader beside the existing `flatValue` helper in that file:

```go
// flatFloat is flatValue for a float counter.
func flatFloat(t *testing.T, r *sdkmetric.ManualReader, name string) float64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	assert.NoError(t, r.Collect(context.Background(), &rm))
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[float64])
			if !ok {
				t.Fatalf("%s is not a float sum", name)
			}
			for _, dp := range sum.DataPoints {
				if dp.Attributes.Len() == 0 {
					return dp.Value
				}
			}
		}
	}
	t.Fatalf("%s was never recorded", name)
	return 0
}
```

Append to `internal/turbostats/collect_test.go`:

```go
func TestCollect_CarriesTheConsumeLoopsWait(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	reader, m, _ := provider(t)
	m.RecvWaitSeconds.Add(context.Background(), 1.5)

	b, err := Collect(context.Background(), runSource(reader, nil))
	assert.NoError(t, err)
	assert.Equal(t, 1.5, *b.Pipeline.RecvWaitSeconds)
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test -short ./internal/core/ -run TestCoreConsumeLoop_CountsTimeWaiting; go test -short ./internal/turbostats/ -run TestCollect_CarriesTheConsumeLoopsWait`
Expected: FAIL, `m.RecvWaitSeconds` is undefined.

- [ ] **Step 3: Add the counter and record it**

In `internal/core/metrics.go`, add the field to `Metrics`:

```go
	// RecvWaitSeconds is how long the consume loop has spent waiting for a
	// batch. A pipeline waiting on a quiet source and one saturated by its
	// own work both report a low rate; this is what tells them apart.
	RecvWaitSeconds metric.Float64Counter
```

and create it in `NewMetrics`, beside the other counters:

```go
	if m.RecvWaitSeconds, err = meter.Float64Counter(
		"pipeline_recv_wait_seconds",
		metric.WithDescription("Time the consume loop spent waiting for input"),
		metric.WithUnit("s"),
	); err != nil {
		return nil, fmt.Errorf("pipeline_recv_wait_seconds: %w", err)
	}
```

In `internal/core/turbine.go`, where `readLatency` is computed and added to `totalRecvWait`:

```go
		readLatency := time.Since(r0)
		totalRecvWait += readLatency
		t.metrics.RecvWaitSeconds.Add(ctx, readLatency.Seconds())
```

In `internal/turbostats/collect.go`, in `pipelineSection`:

```go
	if wait, ok := floats["pipeline_recv_wait_seconds"]; ok {
		p.RecvWaitSeconds = &wait
	}
```

- [ ] **Step 4: Run the tests**

Run: `go test -short -race ./internal/core/ ./internal/turbostats/`
Expected: PASS.

- [ ] **Step 5: Check the cost**

The record is once per batch, beside a histogram record that already runs there. Confirm it allocates nothing:

Run: `go test -short -run XXX -bench BenchmarkConsume -benchmem ./internal/core/ 2>/dev/null | tail -5`

If the repo has no such benchmark, skip this step and say so in the PR: the call is one `Add` on a float counter per batch, beside `SourceReadLatency.Record` which already runs on the same line.

- [ ] **Step 6: Commit**

```bash
git add internal/core/ internal/turbostats/
git commit -m "core: count the time the consume loop waits for input

The loop timed this already and only logged it. A quiet source and a
saturated engine both report a low rate, and the wait is what separates
them."
```

---

### Task 6: The contract and the changelog

**Files:**
- Modify: `docs/superpowers/specs/2026-09-19-turbostats-contract-amendment-design.md`
- Modify: `CHANGELOG.md`

- [ ] **Step 1: Add the fields to the contract**

In the amendment, after the "Durations (added 2026-09-22)" section, add:

```markdown
### Signals (added 2026-09-22)

`pipeline.duration` and `serve.duration` carry how long the work takes. Each
phase has `count`, `sum_seconds`, `min_seconds`, `max_seconds` and `buckets`.
Count, sum and the buckets are counters since the process started; min and
max are since the process started and never reset, because the reporter and
`GET /turbostats/v1` both read this bundle. The boundaries are fixed in this
contract, in seconds: 0.001, 0.01, 0.05, 0.25, 1, 5, 30, 60, with a ninth
bucket above the last. A phase with no samples is absent.

`pipeline` carries errors by phase: `source_error_count`,
`handler_error_count`, `sink_error_count` and `state_error_count`, with
`dlq_rows` for rows diverted rather than dropped, and `last_error_code` and
`last_error_at`. `error_count` remains the total. No error message crosses
the wire.

`pipeline.recv_wait_seconds` is how long the consume loop has spent waiting
for input, as a counter.

Invariants:

- A phase's buckets sum to its `count`, and `min_seconds` is at most
  `max_seconds`.
- `error_count` is at least the sum of the four per-phase counters.
- Every one of these fields is absent from an engine that predates it, and a
  receiver treats absent as absent rather than zero.
```

- [ ] **Step 2: Add the changelog entry**

Under `## Unreleased`, in `### Added`:

```markdown
- The TurboStats bundle carries how long the work takes, which phase failed,
  and how long the consume loop waited. `pipeline.duration.batch`,
  `pipeline.duration.sink_flush` and `serve.duration.request` each carry a
  count, a sum, a min, a max and nine bucket counts against boundaries the
  contract fixes, so a receiver reads any percentile and can sum a fleet.
  `pipeline` also carries `source_error_count`, `handler_error_count`,
  `sink_error_count`, `state_error_count`, `dlq_rows`, `last_error_code`,
  `last_error_at` and `recv_wait_seconds`. No error message crosses the
  wire. All additive; the document stays v1.
```

- [ ] **Step 3: Commit and open the PR**

```bash
git add docs/superpowers/specs/2026-09-19-turbostats-contract-amendment-design.md CHANGELOG.md
git commit -m "docs: the duration, error and wait fields, and their invariants"
```

Run the repo's gate before opening the PR:

```bash
go vet ./... && go test -short -race ./...
uv run --locked pytest tests/tooling -q
make coverage-page && git status --short docs/coverage
```

The PR body names the defect (a bundle that cannot say how long or what failed), the fix, and the evidence. `make soak` is required when the consume loop changes: this adds one float counter add per batch, so either run it or state the per-batch cost and let the reviewer decide. No session links, no attribution lines.
