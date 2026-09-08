package conformance

// The pipeline half of the harness.
//
// A sink invariant has six subjects, one per sink the engine can build. A
// pipeline invariant looked like it had one, which is why these claims were
// first proven by markers on hand-written tests. That was wrong twice over.
//
// The consume loop has two configurations that change what a commit means,
// durable state or none, and four paths that reach processBatch: the batch
// filled, the flush interval elapsed, the source closed, and a cancel that
// drains. Eight combinations. Two were tested.
//
// So the subject is a configuration, the harness runs every trigger against
// it, and each combination that goes unproven says so in the matrix instead
// of being upheld by inspection.

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/turbolytics/sql-flow/internal/core"
	"github.com/turbolytics/sql-flow/internal/coverage"
)

// Trigger is what makes the consume loop process a batch.
//
// Every one of them ends in the same three steps, and a pipeline that commits
// correctly on one path and not another loses rows on that path alone.
type Trigger string

const (
	// TriggerBatchFull is the common path: the batch reached batchSize.
	TriggerBatchFull Trigger = "batch-full"
	// TriggerInterval is the flush interval elapsing with a partial batch.
	TriggerInterval Trigger = "flush-interval"
	// TriggerSourceClosed is the source's stream closing with rows buffered.
	TriggerSourceClosed Trigger = "source-closed"
	// TriggerDrain is a cancelled context, which a SIGTERM arrives as. The
	// loop drains through a context stripped of cancellation, so this is the
	// one path whose flush must run after the caller gave up.
	TriggerDrain Trigger = "drain"
)

// Triggers is every path in the order the harness runs them.
var Triggers = []Trigger{
	TriggerBatchFull,
	TriggerInterval,
	TriggerSourceClosed,
	TriggerDrain,
}

// PipelineSubject is one configuration of the consume loop.
type PipelineSubject struct {
	// Integration is the integrations.yml id, e.g. "pipeline.stateful".
	Integration string

	// KeepsState says whether this configuration commits durable state and
	// offsets alongside the source position. A configuration that keeps none
	// cannot prove the invariants about committing them together, and the
	// registry must exempt it.
	KeepsState bool

	// Options builds the pipeline's options from the harness's recorder. A
	// stateful configuration returns core.WithStateStore(r.Offsets(), r.Tx()).
	Options func(r *Recorder) []core.TurbineOption

	// NewSink supplies the destination the pipeline writes to. Nil means the
	// harness discards the rows and judges the event order alone.
	//
	// A subject that supplies one, with ReadBack and Break, is judged on what
	// the destination actually holds. That is the difference between asserting
	// the loop emitted three events in an order and asserting it never
	// committed offsets for a row the sink never wrote.
	NewSink func(t *testing.T) core.Sink

	// ReadBack returns every row the destination holds. Required with NewSink.
	ReadBack func(t *testing.T) []Row

	// Break makes the destination stop accepting writes, and Heal reverses it.
	// With a real sink these replace the harness's synthetic flush failure, so
	// the flush fails the way it would in production.
	Break func(t *testing.T)
	Heal  func(t *testing.T)
}

// readsBack reports whether the subject can be judged on delivered rows.
func (s PipelineSubject) readsBack() bool {
	return s.NewSink != nil && s.ReadBack != nil && s.Break != nil && s.Heal != nil
}

// Pipelines proves every pipeline invariant the subject can exercise.
func Pipelines(t *testing.T, s PipelineSubject) {
	t.Helper()
	if s.Integration == "" {
		t.Fatal("conformance: PipelineSubject.Integration is required")
	}
	if s.Options == nil {
		t.Fatal("conformance: PipelineSubject.Options is required")
	}

	// Every subtest is its own entry in `go test -json`, so each one carries
	// both markers. Nothing here is attributed by its name.
	feature, hasFeature, err := coverage.FeatureFor(s.Integration)
	if err != nil {
		t.Fatalf("conformance: %v", err)
	}

	for _, v := range pipelineVerdicts(t, s) {
		t.Run(v.invariant, func(t *testing.T) {
			// Emitted before the outcome, because it says which feature this
			// test touched rather than that it passed. A skipped or failing
			// subtest records as a skip or a failure against the feature,
			// which status() already refuses to count as coverage.
			if hasFeature {
				coverage.Covers(t, feature)
			}
			if v.skipped != "" {
				t.Skip(v.skipped)
			}
			if v.failure != "" {
				t.Fatal(v.failure)
			}
			coverage.Invariant(t, v.invariant, s.Integration)
		})
	}

	if hasFeature {
		coverage.Covers(t, feature)
	}
}

const (
	commitAfterFlush    = "pipeline.commit.after_flush"
	commitNothingOnFail = "pipeline.commit.nothing_on_failure"
	stateWithOffsets    = "pipeline.state.with_offsets"
	drainOnCancel       = "lifecycle.drain.on_cancel"
	flushEventually     = "pipeline.flush.eventually"
	onlyDeliveredRows   = "pipeline.commit.only_delivered_rows"
)

// pipelineVerdicts runs every trigger against the subject and judges each
// invariant across all of them.
//
// An invariant holds only if it holds on every path. A pipeline that commits
// after flushing when the batch fills, and before flushing when the interval
// elapses, loses rows on the second path and the matrix must not call it
// covered.
func pipelineVerdicts(t *testing.T, s PipelineSubject) []verdict {
	t.Helper()

	afterFlush := verdict{invariant: commitAfterFlush}
	onFailure := verdict{invariant: commitNothingOnFail}
	withOffsets := verdict{invariant: stateWithOffsets}
	drain := verdict{invariant: drainOnCancel}
	eventually := verdict{invariant: flushEventually}
	delivered := verdict{invariant: onlyDeliveredRows}

	if !s.readsBack() {
		delivered.skipped = s.Integration + " has no destination to read back " +
			"from, so what it committed cannot be compared with what landed; " +
			"supply NewSink, ReadBack, Break and Heal"
	}

	if !s.KeepsState {
		withOffsets.skipped = s.Integration + " keeps no durable state, so " +
			"there is nothing to commit with the offsets; exempt it in " +
			"integrations.yml"
	}

	for _, trigger := range Triggers {
		// The happy path: every trigger must flush before it commits.
		if afterFlush.failure == "" {
			if err := checkCommitAfterFlush(t, s, trigger); err != nil {
				afterFlush.failure = err.Error()
			}
		}

		// A sink that cannot deliver must leave every position where it was.
		if onFailure.failure == "" {
			if err := checkNothingOnFailure(t, s, trigger); err != nil {
				onFailure.failure = err.Error()
			}
		}

		// State and offsets are one commit, so a failure saving one must roll
		// back the other.
		if s.KeepsState && withOffsets.failure == "" {
			if err := checkStateWithOffsets(t, s, trigger); err != nil {
				withOffsets.failure = err.Error()
			}
		}
	}

	// Drain is one path rather than a property of all of them: the claim is
	// that a cancelled run writes what it buffered.
	if err := checkDrain(t, s); err != nil {
		drain.failure = err.Error()
	}

	// The outcome, not the order. after_flush asserts the sequence the loop
	// happens to use; this asserts the guarantee that sequence exists for, and
	// it needs a destination to count.
	if s.readsBack() {
		for _, trigger := range Triggers {
			if delivered.failure != "" {
				break
			}
			if err := checkOnlyDeliveredRows(t, s, trigger); err != nil {
				delivered.failure = err.Error()
			}
		}
	}

	// The only liveness claim here. Everything above says the pipeline does
	// nothing wrong; this says it does something. A sink that never flushed
	// would satisfy every one of them and fail this one alone.
	if err := checkFlushEventually(t, s); err != nil {
		eventually.failure = err.Error()
	}

	return []verdict{afterFlush, onFailure, withOffsets, drain, eventually,
		delivered}
}

// checkCommitAfterFlush runs one trigger and holds the event order.
func checkCommitAfterFlush(t *testing.T, s PipelineSubject, trigger Trigger) error {
	t.Helper()
	run := runPipeline(t, s, trigger, faults{})
	if run.err != nil {
		return fmt.Errorf("%s: the pipeline failed with no fault injected: %v",
			trigger, run.err)
	}

	want := []string{"flush"}
	if s.KeepsState {
		want = append(want, "save-offsets", "commit")
	}
	if !sameOrder(run.events, want) {
		return fmt.Errorf(
			"%s: the pipeline did %s; want %s. Committing before the flush "+
				"records progress past rows the sink never wrote, so a restart "+
				"skips them and nothing looks wrong",
			trigger, list(run.events), list(want))
	}
	return nil
}

// checkNothingOnFailure fails the flush and holds every position still.
func checkNothingOnFailure(t *testing.T, s PipelineSubject, trigger Trigger) error {
	t.Helper()
	run := runPipeline(t, s, trigger, faults{flush: true})
	if run.err == nil {
		return fmt.Errorf("%s: the flush failed and the pipeline did not", trigger)
	}

	if s.KeepsState {
		if !sameOrder(run.events, []string{"flush-failed", "rollback"}) {
			return fmt.Errorf(
				"%s: after a failed flush the pipeline did %s; want a rollback "+
					"and nothing else",
				trigger, list(run.events))
		}
	}
	if run.sourceCommits != 0 {
		return fmt.Errorf(
			"%s: the flush failed and the pipeline still committed its offsets "+
				"%d time(s). Nothing was written, so a restart has to re-read "+
				"those messages rather than skip them",
			trigger, run.sourceCommits)
	}
	return nil
}

// checkStateWithOffsets fails the offset save and holds that the handler's
// state went back with it.
func checkStateWithOffsets(t *testing.T, s PipelineSubject, trigger Trigger) error {
	t.Helper()
	run := runPipeline(t, s, trigger, faults{offsets: true})
	if run.err == nil {
		return fmt.Errorf("%s: saving offsets failed and the pipeline did not",
			trigger)
	}
	if !sameOrder(run.events, []string{"flush", "save-offsets-failed", "rollback"}) {
		return fmt.Errorf(
			"%s: after a failed offset save the pipeline did %s; want a "+
				"rollback. The window state this batch wrote has to go back "+
				"with the offsets that say where it came from, or a restart "+
				"replays the batch into state that already holds it",
			trigger, list(run.events))
	}
	if run.sourceCommits != 0 {
		return fmt.Errorf(
			"%s: saving the offsets failed and the pipeline still told the "+
				"source it had finished with those messages. A restart would "+
				"skip them", trigger)
	}
	return nil
}

// checkDrain cancels mid-run and holds that the buffered rows were written.
func checkDrain(t *testing.T, s PipelineSubject) error {
	t.Helper()
	run := runPipeline(t, s, TriggerDrain, faults{})
	if run.err != nil {
		return fmt.Errorf("drain: the pipeline failed on a cancel: %v", run.err)
	}
	if run.rows != drainRows {
		return fmt.Errorf(
			"drain: a cancelled run wrote %d of %d buffered rows; returning "+
				"without them drops the tail of every graceful shutdown",
			run.rows, drainRows)
	}
	if run.flushes != 1 {
		return fmt.Errorf("drain: the buffered batch was flushed %d times; want 1",
			run.flushes)
	}
	return nil
}

// checkOnlyDeliveredRows compares what the pipeline committed with what the
// destination holds, on a clean run and on a broken one.
//
// The pipeline commits offsets on what Flush reports. A run that advances the
// position while the destination holds nothing has lost those rows, and the
// consumer group shows no lag while they are gone -- which is #154, found in
// production rather than by a test.
func checkOnlyDeliveredRows(t *testing.T, s PipelineSubject, trigger Trigger) error {
	t.Helper()

	// Deltas, not totals. Every scenario writes to the same destination, so
	// what matters is what this run added.
	start := int64(len(s.ReadBack(t)))

	clean := runPipeline(t, s, trigger, faults{})
	if clean.err != nil {
		return fmt.Errorf("%s: the pipeline failed with no fault injected: %v",
			trigger, clean.err)
	}

	landed := int64(len(s.ReadBack(t))) - start
	if landed != clean.rows {
		return fmt.Errorf(
			"%s: the pipeline gave the sink %d rows and only %d reached the "+
				"table. It commits offsets for all %d, so the missing rows are "+
				"never read again",
			trigger, clean.rows, landed, clean.rows)
	}
	if clean.sourceCommits == 0 {
		return fmt.Errorf(
			"%s: %d rows reached the table and the pipeline never committed its "+
				"offsets. Every restart re-reads and re-writes the same batch",
			trigger, landed)
	}

	// And the other direction: a destination that took nothing must leave the
	// position where it was.
	before := int64(len(s.ReadBack(t)))

	broken := runPipeline(t, s, trigger, faults{flush: true})
	if broken.err == nil {
		return fmt.Errorf("%s: the destination was broken and the pipeline did "+
			"not fail", trigger)
	}
	if added := int64(len(s.ReadBack(t))) - before; added != 0 {
		return fmt.Errorf(
			"%s: the sink was broken and %d rows reached the table anyway",
			trigger, added)
	}
	if broken.sourceCommits != 0 {
		return fmt.Errorf(
			"%s: the sink wrote no rows and the pipeline committed its offsets "+
				"anyway. After a restart the source resumes past those rows, so "+
				"they are gone, and the consumer group reports no lag while they "+
				"are missing",
			trigger)
	}
	return nil
}

// checkFlushEventually holds that a batch too small to fill still lands.
//
// The interval scenario writes fewer messages than batchSize and lets the
// ticker fire. Without that path a low-traffic topic -- and any push source
// between deliveries -- buffers until the process dies.
func checkFlushEventually(t *testing.T, s PipelineSubject) error {
	t.Helper()

	run := runPipeline(t, s, TriggerInterval, faults{})
	if run.stalled {
		return fmt.Errorf(
			"a batch of %d, below batchSize, did not reach the sink within %s "+
				"of a %s flush interval; the run had to be cancelled to end it",
			intervalRows, stallTimeout, 100*time.Millisecond)
	}
	if run.err != nil {
		return fmt.Errorf("interval: the pipeline failed with no fault "+
			"injected: %v", run.err)
	}
	if run.flushes == 0 {
		return errors.New(
			"a batch smaller than batchSize never reached the sink. Only the " +
				"flush interval can move it, so on a quiet topic the rows sit " +
				"in memory for as long as the process runs")
	}
	if run.rows != intervalRows {
		return fmt.Errorf(
			"the flush interval delivered %d of %d buffered rows",
			run.rows, intervalRows)
	}
	return nil
}

func sameOrder(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

func list(events []string) string {
	if len(events) == 0 {
		return "nothing"
	}
	return "[" + strings.Join(events, ", ") + "]"
}

// faults says what the harness breaks on a given run.
type faults struct {
	flush   bool
	offsets bool
}

// outcome is what one run produced.
type outcome struct {
	events        []string
	rows          int64
	flushes       int
	sourceCommits int
	err           error

	// stalled is set when the run had to be cancelled to end it. A liveness
	// check must never wait on the thing it is testing: a pipeline that never
	// flushes would otherwise hang the suite rather than fail one invariant.
	stalled bool
}

// drainRows is the number of messages the drain scenario buffers. Small, and
// larger than one, so a drain that writes a partial batch is visible.
const drainRows = 10

// intervalRows is the partial batch the flush interval has to move. Fewer than
// any batchSize the harness sets, so the batch can never fill.
const intervalRows = 2

// stallTimeout bounds the interval scenario at many times its 100ms tick. A
// pipeline that never flushes is what this check exists to catch, so it must
// report rather than wait.
const stallTimeout = 5 * time.Second

// runPipeline builds the subject's pipeline over the harness's instrumented
// parts and runs it until the trigger fires.
func runPipeline(t *testing.T, s PipelineSubject, trigger Trigger, f faults) outcome {
	t.Helper()

	rec := &Recorder{failOffsets: f.offsets}
	src := &recordingSource{rec: rec}

	sink := &recordingSink{rec: rec}
	if s.NewSink != nil {
		sink.inner = s.NewSink(t)
	}
	// Break the destination where there is one, so the flush fails the way it
	// would in production. Only a subject with nothing to break needs the
	// harness to fake it.
	if f.flush {
		if s.readsBack() {
			s.Break(t)
			defer s.Heal(t)
		} else {
			sink.fail = true
		}
	}
	var flushed chan struct{}

	// Sized so only the intended trigger can fire. A batch size above the
	// message count keeps the batch from filling; an hour-long interval keeps
	// the ticker quiet.
	batchSize, interval := 4, time.Hour
	switch trigger {
	case TriggerBatchFull:
		src.batch = messages(4)
	case TriggerInterval:
		// intervalRows is below every batchSize the harness uses, so only the
		// ticker can move them.
		// The stream stays open so the select blocks on it and the ticker
		// wins. Closing it here instead would end the run through the
		// source-closed path, which is a different branch: an earlier version
		// did exactly that and tested one path twice.
		src.batch = messages(intervalRows)
		src.block = true
		batchSize, interval = 1000, 100*time.Millisecond

		// Close as soon as the interval's flush lands, so the run ends
		// deterministically and no second tick can fire.
		flushed = make(chan struct{})
		var once sync.Once
		rec.onEvent = func(event string) {
			if event == "flush" || event == "flush-failed" {
				once.Do(func() {
					close(flushed)
					go src.Close()
				})
			}
		}
	case TriggerSourceClosed:
		src.batch = messages(2)
		batchSize = 1000
	case TriggerDrain:
		src.batch = messages(drainRows)
		batchSize, src.block = 1000, true
	}

	if trigger != TriggerInterval {
		// Only the interval scenario needs a ticker it can win.
		interval = time.Hour
	}

	tb := core.NewTurbine(src, &passthroughHandler{}, sink, batchSize, interval,
		&sync.Mutex{}, core.PipelineErrorPolicies{}, s.Options(rec)...)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stalled := false
	if flushed != nil {
		// The run ends when the flush lands. If it never does, end it here
		// instead of waiting: that is the failure, not a reason to hang.
		done := make(chan struct{})
		go func() {
			select {
			case <-flushed:
			case <-time.After(stallTimeout):
				stalled = true
				cancel()
				go src.Close()
			case <-done:
			}
		}()
		defer close(done)
	}

	if trigger == TriggerDrain {
		go func() {
			src.wrote.Wait()
			cancel()
		}()
	}

	_, err := tb.ConsumeLoop(ctx, 0)

	return outcome{
		events:        rec.Events(),
		rows:          sink.Rows(),
		flushes:       sink.Flushes(),
		sourceCommits: src.Commits(),
		err:           err,
		stalled:       stalled,
	}
}

func messages(n int) []core.Message {
	out := make([]core.Message, n)
	for i := range out {
		out[i] = core.Message{
			Value:     []byte(`{"id":1}`),
			Topic:     "events",
			Partition: 0,
			Offset:    int64(i),
		}
	}
	return out
}

// Recorder is the shared event log. Every instrumented part appends to it, so
// the order the pipeline did things is what the harness asserts on.
type Recorder struct {
	mu          sync.Mutex
	events      []string
	failOffsets bool

	// onEvent lets a scenario react to what the pipeline did. The flush
	// interval scenario uses it to close the source the moment the interval's
	// flush lands, so the run ends without a sleep and without a second tick.
	onEvent func(event string)
}

func (r *Recorder) record(event string) {
	r.mu.Lock()
	r.events = append(r.events, event)
	notify := r.onEvent
	r.mu.Unlock()

	if notify != nil {
		notify(event)
	}
}

// Events returns what happened, in order.
func (r *Recorder) Events() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.events...)
}

// Offsets returns the offset store to hand core.WithStateStore.
func (r *Recorder) Offsets() *RecordingOffsets { return &RecordingOffsets{rec: r} }

// Tx returns the state transaction to hand core.WithStateStore.
func (r *Recorder) Tx() *RecordingTx { return &RecordingTx{rec: r} }

// RecordingOffsets logs every offset save and can fail one.
type RecordingOffsets struct{ rec *Recorder }

func (o *RecordingOffsets) Save(ctx context.Context, marks *core.Marks) error {
	if o.rec.failOffsets {
		o.rec.record("save-offsets-failed")
		return errors.New("conformance: offset store is down")
	}
	o.rec.record("save-offsets")
	return nil
}

// RecordingTx logs the commit and the rollback.
type RecordingTx struct{ rec *Recorder }

func (x *RecordingTx) Commit(ctx context.Context) error {
	x.rec.record("commit")
	return nil
}

func (x *RecordingTx) Rollback(ctx context.Context) error {
	x.rec.record("rollback")
	return nil
}

// recordingSource yields one batch and counts what it was told to commit.
type recordingSource struct {
	rec   *Recorder
	batch []core.Message
	block bool

	// wrote fires once every message has been handed to the pipeline, so the
	// drain scenario cancels at a point where rows are buffered.
	wrote sync.WaitGroup

	mu      sync.Mutex
	commits int
	ch      chan []core.Message
	once    sync.Once
}

func (s *recordingSource) Start() error {
	s.ch = make(chan []core.Message, 1)
	s.wrote.Add(1)

	go func() {
		s.ch <- s.batch
		s.wrote.Done()
		if !s.block {
			close(s.ch)
			return
		}
		// The drain scenario needs the loop still running when the cancel
		// lands, so the stream stays open.
	}()
	return nil
}

func (s *recordingSource) Stream() <-chan []core.Message { return s.ch }

func (s *recordingSource) Commit() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.commits++
	return nil
}

// CommitMarks is the path a Kafka-shaped source takes, and the one the
// pipeline prefers.
func (s *recordingSource) CommitMarks(marks *core.Marks) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.commits++
	return nil
}

func (s *recordingSource) Commits() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.commits
}

func (s *recordingSource) Close() error {
	s.once.Do(func() {
		if s.block && s.ch != nil {
			close(s.ch)
		}
	})
	return nil
}

// recordingSink logs what the pipeline asked of a sink, and delegates.
//
// It wraps the subject's sink where there is one, so the event order and the
// delivered rows come from the same run. With no inner sink it discards, which
// is what the double-only subjects want.
type recordingSink struct {
	rec   *Recorder
	inner core.Sink
	fail  bool

	mu      sync.Mutex
	rows    int64
	flushes int
}

func (s *recordingSink) WriteTable(ctx context.Context, batch arrow.Table) error {
	s.mu.Lock()
	if batch != nil {
		s.rows += batch.NumRows()
	}
	inner := s.inner
	s.mu.Unlock()

	if inner != nil {
		return inner.WriteTable(ctx, batch)
	}
	return nil
}

func (s *recordingSink) Flush(ctx context.Context) error {
	s.mu.Lock()
	s.flushes++
	inner, fail := s.inner, s.fail
	s.mu.Unlock()

	// The synthetic failure is for subjects with no destination to break. A
	// subject that has one gets a real error from a really broken sink.
	if fail {
		s.rec.record("flush-failed")
		return errors.New("conformance: sink is down")
	}
	if inner != nil {
		if err := inner.Flush(ctx); err != nil {
			s.rec.record("flush-failed")
			return err
		}
	}
	s.rec.record("flush")
	return nil
}

func (s *recordingSink) Batch() (arrow.Table, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.inner != nil {
		return s.inner.Batch()
	}
	return nil, nil
}

func (s *recordingSink) Rows() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.rows
}

func (s *recordingSink) Flushes() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.flushes
}

// passthroughHandler turns each message into a one-row table, which is the
// smallest handler that produces something a sink can be given. What the
// handler does is not what these invariants are about.
type passthroughHandler struct {
	mu   sync.Mutex
	rows int
}

func (h *passthroughHandler) Init(context.Context) error { return nil }

func (h *passthroughHandler) Write([]byte) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.rows++
	return nil
}

func (h *passthroughHandler) Invoke(context.Context) (arrow.Table, error) {
	h.mu.Lock()
	n := h.rows
	h.rows = 0
	h.mu.Unlock()

	if n == 0 {
		return nil, nil
	}
	return idTable(int64(n)), nil
}

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
