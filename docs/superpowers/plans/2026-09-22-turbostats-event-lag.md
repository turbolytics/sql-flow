# TurboStats Event Lag Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** The bundle says how far behind the stream a pipeline runs, in time rather than in messages, for any source that has an event time.

**Architecture:** Each message carries the time its event happened, set by the source that read it. The consume loop measures one lag per batch, from the newest event in that batch, and records it with the basis the source declared. The bundle reports the last reading, its age, the worst since start, and the basis.

**Tech Stack:** Go 1.26, OpenTelemetry Go SDK 1.46, `github.com/zeebo/assert`, the repo's `internal/coverage` markers.

**Spec:** `docs/superpowers/specs/2026-09-22-turbostats-v1-signals-design.md`, section "Lag, in time". This is PR 2 of that spec's four.

**Depends on:** PR 1 (`docs/superpowers/plans/2026-09-22-turbostats-durations-and-errors.md`) merged, because both change `walk` and `pipelineSection`. Branch from `main` after it.

## Global Constraints

- One event-time read per batch, not per message. This runs on gateways, where a read and a compare per message is the budget the product is made of.
- Lag is `now − event time`. `event_lag_basis` travels with it, so two lags that mean different things are never compared.
- A source with no event time sends none of the four fields. Absent is not zero.
- `event_lag_observed_at` is what makes a stale reading visible. A consumer cut off from its brokers keeps its last reading.
- `event_lag_max_seconds` is since the process started and never resets: the reporter and `GET /turbostats/v1` both read this bundle.
- A negative lag is reported as zero. A producer's clock ahead of this host's is not a pipeline running ahead of its stream.
- The document stays `v1`. Every field is additive.
- Every new test carries `coverage.Covers(t, "observability.turbostats")`.
- Prose follows the repo's CLAUDE.md.

---

## File Structure

- Modify `internal/core/turbine.go`: `Message.EventAt`, and the per-batch measurement in the consume loop.
- Modify `internal/core/metrics.go`: the two lag instruments.
- Modify `internal/kafka/source.go`: the record timestamp.
- Modify `internal/webhook/source.go` and `internal/websocket/source.go`: the arrival stamp.
- Modify `turbostats/wire/bundle.go`: the four fields.
- Modify `internal/turbostats/collect.go`: read them into the pipeline section.
- Tests in each of those packages.
- Modify `docs/superpowers/specs/2026-09-19-turbostats-contract-amendment-design.md` and `CHANGELOG.md`.

---

### Task 1: A message knows when its event happened

**Files:**
- Modify: `internal/core/turbine.go` (the `Message` struct)
- Modify: `internal/kafka/source.go:259`
- Modify: `internal/webhook/source.go:352`
- Modify: `internal/websocket/source.go:139`
- Test: `internal/kafka/source_test.go`, `internal/webhook/source_test.go`, `internal/websocket/source_test.go`

**Interfaces:**
- Produces:
  - `core.Message.EventAt time.Time`, zero when the source has no event time.
  - `core.EventTimeBasis` constants: `EventBasisKafkaTimestamp = "kafka_timestamp"`, `EventBasisArrival = "arrival"`.
  - `core.Source` gains an optional `EventTimeBasis() string`, which a source implements only if it stamps `EventAt`.

- [ ] **Step 1: Write the failing tests**

In `internal/kafka/source_test.go`, add to whichever test already reads a batch from a fake broker (find it with `grep -n "func TestKafka" internal/kafka/source_test.go`); if none reads records, add:

```go
// The record's own timestamp is the event time: a pipeline behind by an
// hour is handling records the broker stamped an hour ago.
func TestKafkaSource_CarriesTheRecordTimestamp(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	stamped := time.Now().Add(-90 * time.Second).Truncate(time.Millisecond)
	msg := messageFrom(&kgo.Record{Value: []byte(`{"a":1}`), Timestamp: stamped})
	assert.Equal(t, stamped.UTC(), msg.EventAt.UTC())
}
```

`messageFrom` does not exist yet: it is the conversion this task extracts from the loop at `source.go:259` so a test can reach it. If the package's tests already spin a broker, prefer asserting `EventAt` on a consumed message there and delete this unit test.

In `internal/webhook/source_test.go`:

```go
// A webhook carries no event time of its own, so arrival is the event: the
// lag that follows is queueing inside this process, which is what the
// basis says.
func TestWebhookSource_StampsArrival(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	before := time.Now()
	msgs := receiveForTest(t, []byte(`{"a":1}`))
	assert.Equal(t, 1, len(msgs))
	assert.That(t, !msgs[0].EventAt.Before(before))
	assert.Equal(t, core.EventBasisArrival, (&Source{}).EventTimeBasis())
}
```

`receiveForTest` is whatever helper the package's existing tests use to post a body and collect the messages; read `internal/webhook/source_test.go` and use it rather than adding one.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test -short ./internal/kafka/ ./internal/webhook/ -run 'CarriesTheRecordTimestamp|StampsArrival'`
Expected: FAIL, `EventAt` undefined.

- [ ] **Step 3: Add the field and stamp it**

In `internal/core/turbine.go`, in `Message`, after `Value`:

```go
	// EventAt is when the event happened, as the source knows it: a Kafka
	// record's timestamp, or the moment a webhook or websocket message
	// arrived. Zero for a source with no event time, which is why the
	// pipeline's lag fields are absent for one.
	EventAt time.Time
```

and beside the struct:

```go
// The bases an event time can have. It travels with every lag reading,
// because a lag from a broker's timestamp and a lag from arrival measure
// different spans and comparing them is meaningless.
const (
	EventBasisKafkaTimestamp = "kafka_timestamp"
	EventBasisArrival        = "arrival"
)

// EventTimeSource is a source that stamps Message.EventAt. A source that
// does not implement it reports no lag, rather than a lag of zero.
type EventTimeSource interface {
	EventTimeBasis() string
}
```

In `internal/kafka/source.go`, in the loop that builds the batch, add `EventAt: r.Timestamp` to the `core.Message` literal, and add the method:

```go
// EventTimeBasis is the record's own timestamp, which is what the broker
// stamped when the producer wrote it.
func (s *Source) EventTimeBasis() string { return core.EventBasisKafkaTimestamp }
```

In `internal/webhook/source.go` and `internal/websocket/source.go`, stamp arrival where the message is built, for example:

```go
	case s.streamChan <- []core.Message{{Value: body, EventAt: time.Now()}}:
```

and give each the method returning `core.EventBasisArrival`.

- [ ] **Step 4: Run the tests**

Run: `go test -short -race ./internal/kafka/ ./internal/webhook/ ./internal/websocket/`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/core/turbine.go internal/kafka/ internal/webhook/ internal/websocket/
git commit -m "sources: a message carries when its event happened

Kafka has the record's timestamp. A webhook and a websocket have arrival,
which is a different span, so the basis travels with the reading."
```

---

### Task 2: One lag per batch

**Files:**
- Modify: `internal/core/metrics.go`
- Modify: `internal/core/turbine.go` (the consume loop, after the payload-bytes loop)
- Test: `internal/core/flat_test.go`

**Interfaces:**
- Consumes: `Message.EventAt`, `core.EventTimeSource` from Task 1.
- Produces: the instruments `pipeline_event_lag_seconds` (gauge, last reading), `pipeline_event_lag_max_seconds` (gauge, worst since start), `pipeline_event_lag_observed_timestamp` (gauge, unix seconds).

- [ ] **Step 1: Write the failing test**

Append to `internal/core/flat_test.go`:

```go
// The lag is how far behind the stream the pipeline runs: now minus the
// newest event it just handled. One reading per batch, from the newest
// event in it, because the oldest would add the batch's own span and say as
// much about batch_size as about the stream.
func TestCoreConsumeLoop_MeasuresLagPerBatch(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	old := time.Now().Add(-2 * time.Minute)
	batch := messages(3)
	for i := range batch {
		batch[i].EventAt = old.Add(time.Duration(i) * time.Second)
	}
	src := &eventTimeSource{fakeSource: fakeSource{batches: [][]Message{batch}}}
	tb, reader := meteredTurbine(t, src, &fakeSink{}, 10)

	_, err := tb.ConsumeLoop(context.Background(), 0)
	assert.NoError(t, err)

	lag := flatFloat(t, reader, "pipeline_event_lag_seconds")
	// The newest event is two minutes old, less the two seconds it spans.
	assert.That(t, lag > 110 && lag < 130)
	assert.Equal(t, lag, flatFloat(t, reader, "pipeline_event_lag_max_seconds"))
	assert.That(t, flatValue(t, reader, "pipeline_event_lag_observed_timestamp") > 0)
}

// A source with no event time reports no lag at all. Zero would say the
// pipeline is caught up with a stream it cannot measure.
func TestCoreConsumeLoop_NoEventTimeNoLag(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	src := &fakeSource{batches: [][]Message{messages(3)}}
	tb, reader := meteredTurbine(t, src, &fakeSink{}, 10)

	_, err := tb.ConsumeLoop(context.Background(), 0)
	assert.NoError(t, err)

	var rm metricdata.ResourceMetrics
	assert.NoError(t, reader.Collect(context.Background(), &rm))
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == "pipeline_event_lag_seconds" {
				t.Fatal("a source with no event time recorded a lag")
			}
		}
	}
}

// A producer's clock ahead of this host's is not a pipeline running ahead
// of its stream.
func TestCoreConsumeLoop_ANegativeLagIsZero(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	batch := messages(1)
	batch[0].EventAt = time.Now().Add(time.Hour)
	src := &eventTimeSource{fakeSource: fakeSource{batches: [][]Message{batch}}}
	tb, reader := meteredTurbine(t, src, &fakeSink{}, 10)

	_, err := tb.ConsumeLoop(context.Background(), 0)
	assert.NoError(t, err)
	assert.Equal(t, float64(0), flatFloat(t, reader, "pipeline_event_lag_seconds"))
}
```

and the source that stamps event times, beside `fakeSource`:

```go
// eventTimeSource is a fakeSource that declares a basis, which is what
// makes the consume loop measure lag at all.
type eventTimeSource struct{ fakeSource }

func (eventTimeSource) EventTimeBasis() string { return EventBasisArrival }
```

`flatFloat` comes from PR 1's Task 5. If it is missing, that PR has not merged; stop and rebase.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test -short ./internal/core/ -run 'MeasuresLagPerBatch|NoEventTimeNoLag|ANegativeLagIsZero'`
Expected: FAIL, `pipeline_event_lag_seconds` was never recorded.

- [ ] **Step 3: Add the instruments**

In `internal/core/metrics.go`, add to `Metrics`:

```go
	// EventLagSeconds is how far behind the stream the pipeline ran at the
	// last batch: now minus the newest event in it. EventLagMaxSeconds is
	// the worst since the process started, and never resets, because two
	// readers share the bundle this ends up in.
	//
	// EventLagObserved is when the reading was taken, as unix seconds. A
	// consumer cut off from its brokers keeps its last reading, so the age
	// of the reading is what says whether to believe it.
	EventLagSeconds    metric.Float64Gauge
	EventLagMaxSeconds metric.Float64Gauge
	EventLagObserved   metric.Int64Gauge
```

and create them in `NewMetrics` beside the other gauges, with names `pipeline_event_lag_seconds`, `pipeline_event_lag_max_seconds` and `pipeline_event_lag_observed_timestamp`, each with `metric.WithUnit("s")` for the first two.

- [ ] **Step 4: Measure once per batch**

In `internal/core/turbine.go`, add a field to `Turbine`:

```go
	// eventBasis is the source's event-time basis, empty for a source that
	// has none. Read once at construction: a source does not change what it
	// reads mid-run.
	eventBasis string
	// eventLagMax is the worst lag seen, in seconds.
	eventLagMax float64
```

Set it where the turbine is built, from the source:

```go
	if src, ok := source.(EventTimeSource); ok {
		t.eventBasis = src.EventTimeBasis()
	}
```

In the consume loop, inside the block that already walks `msgBatch` for payload bytes, take the newest event time in the same pass:

```go
		var payload int64
		var newest time.Time
		for i := range msgBatch {
			payload += int64(len(msgBatch[i].Value))
			if msgBatch[i].EventAt.After(newest) {
				newest = msgBatch[i].EventAt
			}
		}
```

and after the existing `PipelineLastMessage` record:

```go
		// One reading per batch, from the newest event in it: the oldest
		// would add the batch's own span, which says as much about
		// batch_size as about the stream. The compare runs inside the loop
		// that already counts payload bytes, so it costs no extra pass.
		if t.eventBasis != "" && !newest.IsZero() {
			lag := time.Since(newest).Seconds()
			if lag < 0 {
				// A producer's clock ahead of this host's is not a pipeline
				// running ahead of its stream.
				lag = 0
			}
			t.metrics.EventLagSeconds.Record(ctx, lag)
			if lag > t.eventLagMax {
				t.eventLagMax = lag
			}
			t.metrics.EventLagMaxSeconds.Record(ctx, t.eventLagMax)
			t.metrics.EventLagObserved.Record(ctx, time.Now().Unix())
		}
```

- [ ] **Step 5: Run the tests**

Run: `go test -short -race ./internal/core/`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/core/
git commit -m "core: measure how far behind the stream the pipeline runs

One reading per batch, from the newest event in it, inside the loop that
already counts payload bytes. The oldest event would add the batch's own
span and say as much about batch_size as about the stream."
```

---

### Task 3: The lag on the wire

**Files:**
- Modify: `turbostats/wire/bundle.go`
- Modify: `internal/turbostats/collect.go` (`pipelineSection`) and `internal/turbostats/bundle.go` (`PipelineSource.EventBasis`)
- Modify: `internal/cli/run/root.go`
- Test: `turbostats/wire/bundle_test.go`, `internal/turbostats/collect_test.go`

**Interfaces:**
- Consumes: the instruments from Task 2.
- Produces:
  - `Pipeline.EventLagSeconds *float64`, `EventLagMaxSeconds *float64`, `EventLagObservedAt *time.Time`, `EventLagBasis *string`
  - `PipelineSource.EventBasis string`

- [ ] **Step 1: Write the failing tests**

Append to `internal/turbostats/collect_test.go`:

```go
// The four travel together: a reading, its age, the worst since start, and
// where the event time came from.
func TestCollect_CarriesTheEventLagAndItsBasis(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	reader, m, _ := provider(t)
	ctx := context.Background()
	m.EventLagSeconds.Record(ctx, 12.5)
	m.EventLagMaxSeconds.Record(ctx, 90)
	m.EventLagObserved.Record(ctx, time.Date(2026, 9, 22, 12, 0, 0, 0, time.UTC).Unix())

	src := runSource(reader, nil)
	src.Pipeline.EventBasis = "kafka_timestamp"
	b, err := Collect(ctx, src)
	assert.NoError(t, err)

	assert.Equal(t, 12.5, *b.Pipeline.EventLagSeconds)
	assert.Equal(t, float64(90), *b.Pipeline.EventLagMaxSeconds)
	assert.Equal(t, "kafka_timestamp", *b.Pipeline.EventLagBasis)
	assert.Equal(t, 2026, b.Pipeline.EventLagObservedAt.UTC().Year())
}

// A source with no event time sends none of the four, not zeros: a zero lag
// says the pipeline is caught up.
func TestCollect_NoBasisNoLagFields(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	reader, _, _ := provider(t)
	b, err := Collect(context.Background(), runSource(reader, nil))
	assert.NoError(t, err)
	assert.That(t, b.Pipeline.EventLagSeconds == nil)
	assert.That(t, b.Pipeline.EventLagBasis == nil)
	assert.That(t, b.Pipeline.EventLagObservedAt == nil)
}
```

Append to `turbostats/wire/bundle_test.go`:

```go
// A lag of zero is a reading: a pipeline that has caught up is not a
// pipeline with no event time.
func TestPipeline_AZeroLagIsPresentAndNoBasisIsAbsent(t *testing.T) {
	zero := 0.0
	basis := "arrival"
	with := mustMarshal(t, Bundle{V: Version, Pipeline: &Pipeline{
		EventLagSeconds: &zero, EventLagBasis: &basis}})
	if !strings.Contains(with, `"event_lag_seconds":0`) {
		t.Errorf("a zero lag is absent: %s", with)
	}
	without := mustMarshal(t, Bundle{V: Version, Pipeline: &Pipeline{}})
	if strings.Contains(without, "event_lag") {
		t.Errorf("a pipeline with no event time carries a lag: %s", without)
	}
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test -short ./turbostats/wire/ ./internal/turbostats/ -run 'EventLag|NoBasisNoLag|AZeroLag'`
Expected: FAIL, `EventLagSeconds` undefined.

- [ ] **Step 3: Add the fields and fill them**

In `turbostats/wire/bundle.go`, in `Pipeline`, after the offset-lag fields:

```go
	// EventLagSeconds is how far behind the stream the pipeline ran at its
	// last batch: now minus the newest event in it. EventLagMaxSeconds is
	// the worst since the process started.
	//
	// EventLagObservedAt is when that reading was taken. A consumer cut off
	// from its brokers keeps its last reading, so a receiver judges the
	// reading by its age.
	//
	// EventLagBasis is where the event time came from: kafka_timestamp is
	// broker to processing, arrival is queueing inside the process. It
	// travels with the number because the two measure different spans. All
	// four are absent for a source with no event time.
	EventLagSeconds    *float64   `json:"event_lag_seconds,omitempty"`
	EventLagMaxSeconds *float64   `json:"event_lag_max_seconds,omitempty"`
	EventLagObservedAt *time.Time `json:"event_lag_observed_at,omitempty"`
	EventLagBasis      *string    `json:"event_lag_basis,omitempty"`
```

In `internal/turbostats/bundle.go`, add to `PipelineSource`:

```go
	// EventBasis is the source's event-time basis, empty for a source with
	// none. The command knows it; the instruments do not carry it, because
	// a string is not a metric.
	EventBasis string
```

In `internal/turbostats/collect.go`, in `pipelineSection`:

```go
	// The basis is what makes the reading meaningful, so the four are sent
	// together or not at all.
	if src.EventBasis != "" {
		if lag, ok := floats["pipeline_event_lag_seconds"]; ok {
			basis := src.EventBasis
			p.EventLagSeconds = &lag
			p.EventLagBasis = &basis
			if worst, ok := floats["pipeline_event_lag_max_seconds"]; ok {
				p.EventLagMaxSeconds = &worst
			}
			p.EventLagObservedAt = unixTime(flat["pipeline_event_lag_observed_timestamp"])
		}
	}
```

The gauges record floats, so they arrive in the `floats` map that PR 1 added, and the observed timestamp is an integer gauge, so it arrives in `flat`.

In `internal/cli/run/root.go`, set `EventBasis` on the `PipelineSource` from the source the command built, using the same `EventTimeSource` type assertion the turbine uses:

```go
			if src, ok := source.(core.EventTimeSource); ok {
				pipelineSource.EventBasis = src.EventTimeBasis()
			}
```

Read the file for the real variable names before writing this; the point is that the command knows the source and the bundle needs its basis.

- [ ] **Step 4: Run the tests**

Run: `go vet ./... && go test -short -race ./...`
Expected: PASS.

- [ ] **Step 5: Check it end to end**

```bash
go build -o /tmp/sqlflow ./cmd/sqlflow
```

Run a webhook pipeline with `--turbostats`, post a message, and read the bundle:

```bash
curl -s localhost:8000/turbostats/v1 | python3 -c 'import sys,json; p=json.load(sys.stdin)["pipeline"]; print({k:v for k,v in p.items() if k.startswith("event_lag")})'
```

Expected: `event_lag_basis` is `arrival`, `event_lag_seconds` is small, `event_lag_observed_at` is recent. A Kafka pipeline reports `kafka_timestamp`. Paste both into the PR.

- [ ] **Step 6: Commit**

```bash
git add turbostats/ internal/turbostats/ internal/cli/
git commit -m "turbostats: the bundle says how far behind the stream it runs

Four fields that travel together, because a reading without its basis or
its age is a number nobody can act on."
```

---

### Task 4: The contract and the changelog

**Files:**
- Modify: `docs/superpowers/specs/2026-09-19-turbostats-contract-amendment-design.md`
- Modify: `CHANGELOG.md`

- [ ] **Step 1: Add the fields to the contract**

Append to the "Signals" section PR 1 added:

```markdown
`pipeline.event_lag_seconds` is how far behind the stream the pipeline ran
at its last batch: now minus the newest event in it, measured once per
batch. `event_lag_max_seconds` is the worst since the process started;
`event_lag_observed_at` is when the reading was taken, which is how a
receiver spots a reading that has stopped moving; and `event_lag_basis` is
where the event time came from. All four are absent for a source with no
event time, and a negative lag is reported as zero.

`event_lag_basis` is an open vocabulary, not an enum. v1 defines two values:

- `kafka_timestamp`: the record's timestamp. Broker to processing.
- `arrival`: the source stamped the message as it arrived. Queueing inside
  the process, not transport.

A source whose protocol carries no event time, and whose broker can hold a
message before delivering it, is not honestly described by either. MQTT is
the case in hand: it has no publish timestamp in 3.1.1 or 5.0, and a
retained message or a QoS 1 redelivery after a reconnect arrives now
however old it is, so `arrival` would report a lag near zero exactly when
the pipeline is furthest behind. Such a source gets its own basis when it
lands, or sends none of the four.
```

- [ ] **Step 1b: Say that a receiver tolerates an unknown basis**

In the contract's "Extensibility rules", after rule 4:

```markdown
5. **Open vocabularies.** A field whose value is a name from a list, such as
   `event_lag_basis`, may gain names. A reader that does not recognize one
   keeps the reading and declines to compare it with a reading on a basis it
   does know; it does not treat the bundle as invalid. Two lags on different
   bases were never comparable anyway, which is why the basis is sent.
```

Rules 1 and 2 cover unknown sections and unknown fields, and say nothing
about unknown values. Without this, a receiver written against v1's two
basis names can reject the third the day a source needs one, which is a
break the contract promised not to have.

- [ ] **Step 2: Add the changelog entry**

Under `## Unreleased`, in `### Added`:

```markdown
- The TurboStats bundle carries `event_lag_seconds`, how far behind the
  stream a pipeline runs, in time rather than in messages, with
  `event_lag_max_seconds`, `event_lag_observed_at` and `event_lag_basis`.
  Kafka reports the record's timestamp; the webhook and websocket sources
  report arrival, which is queueing inside the process, and the basis says
  which. A source with no event time reports none of them. The reading is
  taken once per batch, from the newest event in it.
```

- [ ] **Step 3: Commit and open the PR**

```bash
git add docs/superpowers/specs/2026-09-19-turbostats-contract-amendment-design.md CHANGELOG.md
git commit -m "docs: event lag, its basis, and when it is absent"
```

Run the gate:

```bash
go vet ./... && go test -short -race ./...
uv run --locked pytest tests/tooling -q
make coverage-page && git status --short docs/coverage
```

`make soak` is required: this adds a compare per message inside the loop that already counts payload bytes, and three gauge records per batch. Run it and paste the verdict, or state the per-message cost and let the reviewer decide. No session links, no attribution lines.
