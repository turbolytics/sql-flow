package conformance

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/turbolytics/sql-flow/internal/core"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

// The harness is a gate on every integration, so a harness that passes a
// broken sink is worse than no harness. These tests run it against sinks
// whose behaviour is known: one that honours the contract, and several that
// break it in the ways real sinks have.

func TestToolingConformanceSinks_ACorrectSinkPasses(t *testing.T) {
	coverage.Covers(t, "tooling.conformance")
	Sinks(t, subject(newMemSink()))
}

// #221's defect: a failed Flush discards the buffer, so the retry finds
// nothing to send and reports a success that delivered no rows.
func TestToolingConformanceSinks_ASinkThatDropsItsBatchIsCaught(t *testing.T) {
	coverage.Covers(t, "tooling.conformance")
	v := verdicts(t, subject(&dropSink{memSink: newMemSink()}))[keepsBatch]

	assert.True(t, v.failure != "")
	// The pre-warm row still arrived, so the read-back is not empty. What is
	// missing is the row the failed flush was supposed to have kept.
	assert.True(t, strings.Contains(v.failure, "id=2 never reached"))
}

// A sink that delivers on WriteTable rather than on Flush: the Kafka sink's
// shape today. It reaches the destination before the fault, so a read-back
// after the retry finds the row and every downstream check passes for the
// wrong reason. Only a read-back before the first Flush catches it.
func TestToolingConformanceSinks_ASinkThatWritesThroughIsCaught(t *testing.T) {
	coverage.Covers(t, "tooling.conformance")
	vs := verdicts(t, subject(&writeThroughSink{memSink: newMemSink()}))

	assert.True(t, vs[buffersOnly].failure != "")
	assert.True(t, strings.Contains(vs[buffersOnly].failure, "before any Flush"))

	// keeps_batch fails too, and says why: there was nothing left to keep.
	assert.True(t, vs[keepsBatch].failure != "")
	assert.True(t, strings.Contains(vs[keepsBatch].failure, "already been delivered"))
}

// The two invariants are independent: a sink can buffer correctly and still
// lose the batch, and the matrix must show which one broke.
func TestToolingConformanceSinks_ADroppingSinkStillBuffersOnly(t *testing.T) {
	coverage.Covers(t, "tooling.conformance")
	vs := verdicts(t, subject(&dropSink{memSink: newMemSink()}))

	assert.Equal(t, "", vs[buffersOnly].failure)
	assert.True(t, vs[keepsBatch].failure != "")
}

// A sink that never clears its buffer re-delivers everything on every flush.
//
// The duplication itself is legal: delivery is at-least-once, so keeps_batch
// holds. What does not hold is the depth. A buffer that never drains grows
// without bound, and reports_depth is the claim that catches it.
//
// The pre-warm flush is what exposes it here: that buffer should be empty
// afterwards, so the row written next is the only one owed. This sink still
// holds the pre-warm row, and reports two.
func TestToolingConformanceSinks_ASinkThatNeverDrainsIsCaught(t *testing.T) {
	coverage.Covers(t, "tooling.conformance")
	vs := verdicts(t, subject(&neverClearSink{memSink: newMemSink()}))

	assert.Equal(t, "", vs[keepsBatch].failure)
	assert.True(t, strings.Contains(vs[reportsDepth].failure,
		"reports 2 buffered rows after one WriteTable"))
}

// A retry that fails is not the same defect, and the message must not blame
// the buffer for it.
func TestToolingConformanceSinks_ASinkThatCannotRecoverIsCaught(t *testing.T) {
	coverage.Covers(t, "tooling.conformance")
	v := verdicts(t, subject(&staysDownSink{memSink: newMemSink()}))[keepsBatch]

	assert.True(t, strings.Contains(v.failure, "Flush after Heal failed"))
}

func TestToolingConformanceSinks_ASlowProbeIsCaught(t *testing.T) {
	coverage.Covers(t, "tooling.conformance")
	v := verdicts(t, subject(&slowProbeSink{memSink: newMemSink()}))[probeFailsStart]

	assert.True(t, strings.Contains(v.failure, "had not returned"))
}

func TestToolingConformanceSinks_AProbeThatCannotFailIsCaught(t *testing.T) {
	coverage.Covers(t, "tooling.conformance")
	v := verdicts(t, subject(&blindProbeSink{memSink: newMemSink()}))[probeFailsStart]

	assert.True(t, strings.Contains(v.failure, "returned nil"))
}

func TestToolingConformanceSinks_ASinkThatCannotCloseTwiceIsCaught(t *testing.T) {
	coverage.Covers(t, "tooling.conformance")
	v := verdicts(t, subject(&closeOnceSink{memSink: newMemSink()}))[closeIdempotent]

	assert.True(t, strings.Contains(v.failure, "second Close"))
}

// A sink implementing neither is skipped, and the registry must exempt it.
func TestToolingConformanceSinks_ASinkWithNoProbeOrCloseIsSkipped(t *testing.T) {
	coverage.Covers(t, "tooling.conformance")
	vs := verdicts(t, subject(newMemSink()))

	assert.True(t, strings.Contains(vs[probeFailsStart].skipped, "integrations file"))
	assert.True(t, strings.Contains(vs[closeIdempotent].skipped, "integrations file"))
}

// A second flush into the same fault must fail again. Returning nil tells the
// pipeline to commit offsets for rows that never landed.
func TestToolingConformanceSinks_AHollowSuccessIsCaught(t *testing.T) {
	coverage.Covers(t, "tooling.conformance")
	v := verdicts(t, subject(&hollowSink{memSink: newMemSink()}))[noHollowSuccess]

	assert.True(t, strings.Contains(v.failure, "returned nil"))
}

// A sink that delivers its buffer backwards loses nothing, so keeps_batch
// holds and only the ordering claim catches it.
func TestToolingConformanceSinks_AReorderingSinkIsCaught(t *testing.T) {
	coverage.Covers(t, "tooling.conformance")
	vs := verdicts(t, subject(&reorderingSink{memSink: newMemSink()}))

	assert.Equal(t, "", vs[keepsBatch].failure)
	assert.True(t, strings.Contains(vs[preservesOrder].failure, "out of order"))
}

// A subject whose destination cannot report arrival order must skip the
// ordering claim rather than read a sorted list as evidence.
func TestToolingConformanceSinks_AnUnorderedReadBackSkipsOrder(t *testing.T) {
	coverage.Covers(t, "tooling.conformance")
	s := subject(newMemSink())
	s.OrderedReadBack = false

	vs := verdicts(t, s)
	assert.True(t, strings.Contains(vs[preservesOrder].skipped, "integrations file"))
	assert.Equal(t, "", vs[preservesOrder].failure)
}

// A sink that ignores its context cannot be stopped. The pipeline's drain and
// every other caller reach it only through that context.
func TestToolingConformanceSinks_ASinkThatIgnoresItsContextIsCaught(t *testing.T) {
	coverage.Covers(t, "tooling.conformance")
	v := verdicts(t, subject(&deafSink{memSink: newMemSink()}))[honoursContext]

	assert.True(t, strings.Contains(v.failure, "did not return"))
}

// A flush with nothing buffered must reach nothing. The pipeline flushes on an
// interval whether or not a batch arrived, so a sink that writes here writes on
// every idle tick.
func TestToolingConformanceSinks_AnEagerEmptyFlushIsCaught(t *testing.T) {
	coverage.Covers(t, "tooling.conformance")
	v := verdicts(t, subject(&eagerFlushSink{memSink: newMemSink()}))[emptyIsNoop]

	assert.True(t, strings.Contains(v.failure, "nothing was buffered"))
}

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

func TestToolingConformanceSinks_ASinkThatMisreportsItsDepthIsCaught(t *testing.T) {
	coverage.Covers(t, "tooling.conformance")
	vs := verdicts(t, subject(&lyingSink{memSink: newMemSink()}))

	// It keeps its batch, so the other two hold. Only the gauge lies.
	assert.Equal(t, "", vs[buffersOnly].failure)
	assert.Equal(t, "", vs[keepsBatch].failure)
	assert.True(t, strings.Contains(vs[reportsDepth].failure,
		"0 buffered rows after one WriteTable"))
}

// A sink that reports no depth at all is skipped rather than failed, and the
// registry must then exempt it. Two statements that have to agree.
func TestToolingConformanceSinks_ASinkThatReportsNoDepthIsSkipped(t *testing.T) {
	coverage.Covers(t, "tooling.conformance")
	vs := verdicts(t, subject(&noDepthSink{inner: newMemSink()}))

	assert.Equal(t, "", vs[keepsBatch].failure)
	assert.True(t, strings.Contains(vs[reportsDepth].skipped,
		"reports no buffer depth"))
}

func TestToolingConformanceSinks_ASubjectWithNothingToBreakIsSkipped(t *testing.T) {
	coverage.Covers(t, "tooling.conformance")
	s := subject(newMemSink())
	s.Break, s.Heal = nil, nil

	vs := verdicts(t, s)
	for _, id := range []string{buffersOnly, keepsBatch, reportsDepth} {
		assert.True(t, vs[id].skipped != "")
		assert.True(t, strings.Contains(vs[id].skipped, "integrations file"))
		assert.Equal(t, "", vs[id].failure)
	}
}

// A skip is not coverage, so a skipped verdict must not emit a marker. The
// matrix would otherwise read a subject that exercises nothing as covered,
// which is the sink.iceberg failure.
func TestToolingConformanceSinks_ASkippedVerdictEmitsNoMarker(t *testing.T) {
	coverage.Covers(t, "tooling.conformance")
	s := subject(newMemSink())
	s.Break, s.Heal = nil, nil

	for _, v := range sinkVerdicts(t, s) {
		assert.Equal(t, "", v.failure)
		assert.True(t, v.skipped != "")
	}
}

func TestToolingConformanceSinks_ASubjectWithoutAnIntegrationIdIsRejected(t *testing.T) {
	coverage.Covers(t, "tooling.conformance")
	s := subject(newMemSink())
	s.Integration = ""

	assert.True(t, fatals(t, func(t *testing.T) { requireSubject(t, s) }))
}

// Break without Heal would leave the destination broken for whatever runs
// next, which is a fault the next subject cannot explain.
func TestToolingConformanceSinks_ASubjectWithBreakAndNoHealIsRejected(t *testing.T) {
	coverage.Covers(t, "tooling.conformance")
	s := subject(newMemSink())
	s.Heal = nil

	assert.True(t, fatals(t, func(t *testing.T) { requireSubject(t, s) }))
}

func TestToolingConformanceSinks_ASubjectMissingReadBackIsRejected(t *testing.T) {
	coverage.Covers(t, "tooling.conformance")
	s := subject(newMemSink())
	s.ReadBack = nil

	assert.True(t, fatals(t, func(t *testing.T) { requireSubject(t, s) }))
}

// The harness must not report a sink defect when the subject's own fault
// injection did nothing: that sends the reader to the wrong file.
func TestToolingConformanceSinks_ABreakThatDoesNotBreakFailsTheSubjectNotTheSink(t *testing.T) {
	coverage.Covers(t, "tooling.conformance")
	s := subject(newMemSink())
	s.Break = func(*testing.T) {} // does nothing
	s.Heal = func(*testing.T) {}

	assert.True(t, fatals(t, func(t *testing.T) { sinkVerdicts(t, s) }))
}

// A write-through sink also flushes successfully while broken, and it must
// not be reported as a broken fault: buffers_only already showed that the
// rows went out early, so the sink is at fault and the subject is not.
func TestToolingConformanceSinks_AWriteThroughSinkIsNotBlamedOnTheSubject(t *testing.T) {
	coverage.Covers(t, "tooling.conformance")
	vs := verdicts(t, subject(&writeThroughSink{memSink: newMemSink()}))

	assert.True(t, vs[buffersOnly].failure != "")
	assert.True(t, vs[keepsBatch].failure != "")
}

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

func TestToolingConformanceDescribe_NamesTheRowsItFound(t *testing.T) {
	coverage.Covers(t, "tooling.conformance")
	assert.Equal(t, "no rows", describe(nil))
	assert.Equal(t, "rows [id=1]", describe([]Row{{"id": int64(1)}}))
	assert.Equal(t, "rows [id=1, id=2]",
		describe([]Row{{"id": int64(1)}, {"id": int64(2)}}))
}

// fakeIntegration is the id the doubles below run under.
//
// Not a real sink. A marker carries an integration id, so the doubles need
// one, and naming a shipped sink would credit it for what a double did. It
// would also force exemptions onto that sink for reasons unrelated to its
// contract: the first invariant it genuinely satisfied would have to be
// exempted anyway, hiding real coverage.
//
// The registry marks this id test_only, so it gets no cells and a
// double's marker lands nowhere.
const fakeIntegration = "sink.conformance_double"

// --- Test doubles ----------------------------------------------------------

// memSink is the smallest sink that honours the contract: it buffers on
// WriteTable, delivers on Flush, and keeps what it could not deliver.
type memSink struct {
	mu        sync.Mutex
	down      bool
	buffered  []arrow.Table
	delivered []int64
}

func newMemSink() *memSink { return &memSink{} }

func (m *memSink) WriteTable(_ context.Context, t arrow.Table) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	t.Retain()
	m.buffered = append(m.buffered, t)
	return nil
}

func (m *memSink) Flush(context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.down {
		return errors.New("destination unreachable")
	}
	for _, t := range m.buffered {
		m.delivered = append(m.delivered, ids(t)...)
		t.Release()
	}
	m.buffered = nil
	return nil
}

// BufferedRows makes the fakes honest about what they hold, so the harness
// can judge reports_depth against them the way it does against a real sink.
func (m *memSink) BufferedRows() int {
	m.mu.Lock()
	defer m.mu.Unlock()

	var rows int64
	for _, t := range m.buffered {
		rows += t.NumRows()
	}
	return int(rows)
}

func (m *memSink) set(down bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.down = down
}

func (m *memSink) rows() []Row {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]Row, 0, len(m.delivered))
	for _, id := range m.delivered {
		out = append(out, Row{"id": id})
	}
	return out
}

// dropSink discards its buffer when the flush fails.
type dropSink struct{ *memSink }

func (d *dropSink) Flush(ctx context.Context) error {
	d.mu.Lock()
	if d.down {
		for _, t := range d.buffered {
			t.Release()
		}
		d.buffered = nil
	}
	d.mu.Unlock()
	return d.memSink.Flush(ctx)
}

// writeThroughSink delivers on WriteTable, so a fault at flush time can never
// hold anything back.
type writeThroughSink struct{ *memSink }

func (w *writeThroughSink) WriteTable(_ context.Context, t arrow.Table) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.delivered = append(w.delivered, ids(t)...)
	return nil
}

// Flush returns nil even while the destination is broken, because there is
// nothing left to send: the rows went out on WriteTable. That is what the
// real Kafka sink did -- franz-go's Flush has an empty buffer to wait on --
// and an earlier version of this fake returned an error instead, which hid a
// harness bug until a real broker exposed it.
func (w *writeThroughSink) Flush(context.Context) error { return nil }

// neverClearSink keeps its buffer even after a successful flush.
type neverClearSink struct{ *memSink }

func (n *neverClearSink) Flush(context.Context) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.down {
		return errors.New("destination unreachable")
	}
	for _, t := range n.buffered {
		n.delivered = append(n.delivered, ids(t)...)
	}
	// The buffer is not cleared, so the next flush delivers it again. One
	// flush here, one from the harness's retry: two copies.
	n.delivered = append(n.delivered, n.delivered...)
	return nil
}

// lyingSink keeps its batch correctly and misreports the depth. A gauge that
// reads zero while rows pile up is worse than no gauge: an operator watching
// it concludes the sink is draining.
type lyingSink struct{ *memSink }

func (l *lyingSink) BufferedRows() int { return 0 }

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

// slowProbeSink ladders its probe. probe() dials once and does not retry,
// because the supervisor's restart is already the retry, so a ladder here only
// delays the report.
type slowProbeSink struct{ *memSink }

func (s *slowProbeSink) Probe(context.Context) error {
	// Past the grace the harness allows, so it is judged for laddering rather
	// than for using the deadline it was given.
	time.Sleep(3 * probeTimeout)
	return errors.New("destination unreachable")
}

// blindProbeSink cannot fail. A probe that certifies a destination which is not
// there is worse than no probe: the pipeline starts and the operator is told
// the dependency is fine.
type blindProbeSink struct{ *memSink }

func (b *blindProbeSink) Probe(context.Context) error { return nil }

// closeOnceSink fails its second Close. More than one shutdown path reaches
// Close, and the second must not change the exit status.
type closeOnceSink struct {
	*memSink
	closed bool
}

func (c *closeOnceSink) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return errors.New("close of closed sink")
	}
	c.closed = true
	return nil
}

// hollowSink reports success on its second failed flush.
//
// #221's defect reached by another route: the first flush records the failure,
// the second finds an empty error list and returns nil having delivered
// nothing. The pipeline then commits offsets for rows that never landed.
type hollowSink struct {
	*memSink
	failedWhileDown bool
}

func (h *hollowSink) Flush(ctx context.Context) error {
	h.mu.Lock()
	down, already := h.down, h.failedWhileDown
	if down {
		h.failedWhileDown = true
	}
	h.mu.Unlock()

	if down && already {
		return nil
	}
	return h.memSink.Flush(ctx)
}

// deafSink ignores the context it is given: it sleeps past any deadline before
// reporting the failure.
//
// Every caller reaches the sink through that context. A flush interval that
// elapsed, a cancelled run and a SIGTERM have no other way to stop a flush, so
// a sink that ignores it cannot be stopped at all.
type deafSink struct{ *memSink }

func (d *deafSink) Flush(ctx context.Context) error {
	err := d.memSink.Flush(ctx)
	if err != nil {
		time.Sleep(2 * flushTimeout)
	}
	return err
}

// eagerFlushSink writes even when nothing is buffered. The pipeline flushes on
// an interval whether or not a batch arrived, so this sink writes on every idle
// tick.
type eagerFlushSink struct{ *memSink }

func (e *eagerFlushSink) Flush(ctx context.Context) error {
	e.mu.Lock()
	if !e.down && len(e.buffered) == 0 {
		e.delivered = append(e.delivered, 0)
	}
	e.mu.Unlock()
	return e.memSink.Flush(ctx)
}

// staysDownSink never recovers, so the retry fails rather than delivering.
//
// It fails only once Break has been called. The sequence opens with a clean
// delivery, and a double that failed that too would abort the run as a broken
// subject rather than exercising the retry this exists to test.
type staysDownSink struct {
	*memSink
	broken bool
}

func (s *staysDownSink) Flush(ctx context.Context) error {
	s.mu.Lock()
	if s.down {
		s.broken = true
	}
	broken := s.broken
	s.mu.Unlock()

	if broken {
		return errors.New("destination unreachable")
	}
	return s.memSink.Flush(ctx)
}

// --- Helpers ---------------------------------------------------------------

// noDepthSink implements core.Sink and nothing else, so the harness cannot ask
// it for a depth. It forwards rather than embeds: embedding memSink would
// inherit BufferedRows and satisfy the interface after all.
type noDepthSink struct{ inner *memSink }

func (n *noDepthSink) WriteTable(ctx context.Context, t arrow.Table) error {
	return n.inner.WriteTable(ctx, t)
}
func (n *noDepthSink) Flush(ctx context.Context) error { return n.inner.Flush(ctx) }
func (n *noDepthSink) set(down bool)                   { n.inner.set(down) }
func (n *noDepthSink) rows() []Row                     { return n.inner.rows() }

type breakable interface {
	core.Sink
	set(down bool)
	rows() []Row
}

func subject(sink breakable) SinkSubject {
	return SinkSubject{
		// Not a sink with a real destination. These tests run in the unit pass
		// against an in-memory fake, and the marker Sinks emits must not
		// credit a sink the fake never touched. Every invariant the harness
		// judges is exempt for this id, so the marker lands on an exempt cell
		// and changes nothing. A test below holds that true.
		Integration: fakeIntegration,
		New:         func(*testing.T) core.Sink { return sink },
		Break:       func(*testing.T) { sink.set(true) },
		Heal:        func(*testing.T) { sink.set(false) },
		ReadBack:    func(*testing.T) []Row { return sink.rows() },
		Table:       oneRow,
		// The fakes append in delivery order, so the ordering claim is
		// judged rather than skipped for every double.
		OrderedReadBack: true,
	}
}

// verdicts runs the harness and returns its judgements keyed by invariant,
// so a test asserts on the one it means rather than on a position.
func verdicts(t *testing.T, s SinkSubject) map[string]verdict {
	t.Helper()

	out := map[string]verdict{}
	for _, v := range sinkVerdicts(t, s) {
		out[v.invariant] = v
	}
	assert.Equal(t, 10, len(out))
	return out
}

// Every invariant the harness judges must be declared, or its marker reports
// as unknown and the cell never appears.
func TestToolingConformanceSinks_JudgesOnlyDeclaredInvariants(t *testing.T) {
	coverage.Covers(t, "tooling.conformance")
	declared, err := coverage.Invariants()
	assert.NoError(t, err)

	for _, v := range sinkVerdicts(t, subject(newMemSink())) {
		assert.True(t, declared[v.invariant])
	}
}

// fatals reports whether fn called Fatal on its *testing.T. A t.Fatal runs
// runtime.Goexit, so fn runs on its own goroutine and the deferred flag says
// whether it returned normally.
func fatals(t *testing.T, fn func(*testing.T)) bool {
	t.Helper()

	var (
		done      = make(chan struct{})
		completed bool
	)
	go func() {
		defer close(done)
		// A synthetic *testing.T: fn's Fatal marks it failed and exits the
		// goroutine without touching the parent's verdict.
		inner := &testing.T{}
		defer func() { recover() }()
		fn(inner)
		completed = true
	}()
	<-done
	return !completed
}

func ids(t arrow.Table) []int64 {
	var out []int64
	for _, chunk := range t.Column(0).Data().Chunks() {
		arr := chunk.(*array.Int64)
		for i := 0; i < arr.Len(); i++ {
			out = append(out, arr.Value(i))
		}
	}
	return out
}

func oneRow(t *testing.T, id int64) arrow.Table {
	t.Helper()

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	b := array.NewRecordBuilder(memory.NewGoAllocator(), schema)
	defer b.Release()
	b.Field(0).(*array.Int64Builder).Append(id)

	rec := b.NewRecord()
	defer rec.Release()
	return array.NewTableFromRecords(schema, []arrow.Record{rec})
}

// The doubles must never credit a shipped sink.
//
// They emit markers like any subject, and a marker names an integration. This
// held sink.buffer.reports_depth green for sink.noop on the strength of an
// in-memory double that never touched NoopSink. The id the doubles use has to
// be test_only, which is what keeps its markers off every cell.
func TestToolingConformanceSinks_TheDoublesRunUnderATestOnlyIntegration(t *testing.T) {
	coverage.Covers(t, "tooling.conformance")
	testOnly, err := coverage.IsTestOnly(fakeIntegration)
	assert.NoError(t, err)
	assert.True(t, testOnly)
}

// And it must not be a sink the engine can build. A test_only id that the
// constructor switch also names would put a double's marker on a real sink
// after all.
func TestToolingConformanceSinks_TheDoublesIntegrationIsNotAShippedSink(t *testing.T) {
	coverage.Covers(t, "tooling.conformance")
	shipped, err := coverage.Integrations("sink")
	assert.NoError(t, err)

	for _, kind := range shipped {
		assert.That(t, "sink."+kind != fakeIntegration)
	}
}

// --- The pipeline harness --------------------------------------------------

// A pipeline that commits before it flushes moves the position past rows the
// sink never took. The harness must catch that on every path, so this runs a
// subject whose recorder reports the wrong order and holds that the verdict
// names it.
func TestToolingConformancePipelines_CatchesACommitBeforeTheFlush(t *testing.T) {
	coverage.Covers(t, "tooling.conformance")
	vs := pipelineVerdictsFor(t, PipelineSubject{
		Integration: "pipeline.stateful",
		KeepsState:  true,
		Options: func(r *Recorder) []core.TurbineOption {
			// A store that records its save before the sink has flushed is
			// the defect: the events come out in the wrong order.
			r.record("save-offsets")
			return []core.TurbineOption{core.WithStateStore(r.Offsets(), r.Tx())}
		},
	})

	assert.True(t, vs[commitAfterFlush].failure != "")
	assert.True(t, strings.Contains(vs[commitAfterFlush].failure, "want"))
}

// A configuration that keeps no state cannot prove the invariant about
// committing state with the offsets. It must skip rather than fail, and the
// registry must exempt it: two statements that have to agree.
func TestToolingConformancePipelines_AStatelessSubjectSkipsStateInvariants(t *testing.T) {
	coverage.Covers(t, "tooling.conformance")
	vs := pipelineVerdictsFor(t, PipelineSubject{
		Integration: "pipeline.stateless",
		KeepsState:  false,
		Options:     func(*Recorder) []core.TurbineOption { return nil },
	})

	assert.True(t, strings.Contains(vs[stateWithOffsets].skipped, "no durable state"))
	// It reads nothing back either, so the outcome check cannot run.
	assert.True(t, strings.Contains(vs[onlyDeliveredRows].skipped, "read back"))
	assert.Equal(t, "", vs[stateWithOffsets].failure)

	// The other three still apply to a stateless pipeline.
	assert.Equal(t, "", vs[commitAfterFlush].failure)
	assert.Equal(t, "", vs[commitNothingOnFail].failure)
	assert.Equal(t, "", vs[drainOnCancel].failure)
}

// Every invariant the pipeline harness judges must be declared, or its marker
// reports as unknown and the cell never appears.
func TestToolingConformancePipelines_JudgeOnlyDeclaredInvariants(t *testing.T) {
	coverage.Covers(t, "tooling.conformance")
	declared, err := coverage.Invariants()
	assert.NoError(t, err)

	for id := range pipelineVerdictsFor(t, PipelineSubject{
		Integration: "pipeline.stateful",
		KeepsState:  true,
		Options: func(r *Recorder) []core.TurbineOption {
			return []core.TurbineOption{core.WithStateStore(r.Offsets(), r.Tx())}
		},
	}) {
		assert.True(t, declared[id])
	}
}

// The harness must exercise every path that reaches a batch. A trigger that
// stopped firing would leave its path unproven while the matrix stayed green.
func TestToolingConformancePipelines_RunsEveryTrigger(t *testing.T) {
	coverage.Covers(t, "tooling.conformance")
	assert.DeepEqual(t, []Trigger{
		TriggerBatchFull, TriggerInterval, TriggerSourceClosed, TriggerDrain,
	}, Triggers)
}

func pipelineVerdictsFor(t *testing.T, s PipelineSubject) map[string]verdict {
	t.Helper()

	out := map[string]verdict{}
	for _, v := range pipelineVerdicts(t, s) {
		out[v.invariant] = v
	}
	assert.Equal(t, 6, len(out))
	return out
}

// The liveness claim is the only one that says the pipeline does anything.
// Every safety verdict above holds for a pipeline that never flushes, so a
// harness that judged safety alone would call a permanently stalled loop
// conformant.
func TestToolingConformancePipelines_JudgeOneLivenessClaim(t *testing.T) {
	coverage.Covers(t, "tooling.conformance")

	vs := pipelineVerdictsFor(t, PipelineSubject{
		Integration: "pipeline.stateless",
		KeepsState:  false,
		Options:     func(*Recorder) []core.TurbineOption { return nil },
	})

	assert.Equal(t, "", vs[flushEventually].failure)
	assert.Equal(t, "", vs[flushEventually].skipped)
}

// TestHarnessSinkCarriesRowCounters guards why sink.rows.counted_on_delivery
// is worth declaring at all.
//
// Subjects build their sinks directly -- NewIcebergSink and friends -- and the
// harness wraps that in recordingSink before handing it to the pipeline.
// Nothing in that path passes through sinks.New, where the counters live. If
// the harness does not apply them itself, the invariant asserts against an
// uninstrumented sink and passes while proving nothing.
func TestHarnessSinkCarriesRowCounters(t *testing.T) {
	coverage.Covers(t, "tooling.conformance")

	s := newRecordingSink(&Recorder{}, nil, nil)

	// The counted wrapper is what the pipeline is handed, not the bare
	// recordingSink.
	assert.That(t, s.counted != nil)
	assert.That(t, s.counted != core.Sink(s))
}

// The flush interval keeps ticking while a run winds down, and a tick with
// nothing buffered commits state on its own. How many of those land depends on
// how long shutdown takes, so asserting the exact event list tests the speed of
// the machine. It failed main on 1fd39a9 and cost three CI runs.
func TestToolingConformancePipelines_AnIdleIntervalTickIsNotACommitBeforeTheFlush(t *testing.T) {
	coverage.Covers(t, "tooling.conformance")

	// Exactly what CI observed.
	events := []string{"flush", "save-offsets", "commit", "save-offsets", "commit"}

	if err := judgeCommitOrder(TriggerInterval, events, true); err != nil {
		t.Fatalf("an idle tick after the flush is legitimate: %v", err)
	}
}

// Only the interval trigger runs a live ticker; every other trigger sets the
// interval to an hour. A second commit cycle there is the loop committing twice
// for one flush, which is a defect rather than a tick.
func TestToolingConformancePipelines_ASecondCommitWithoutATickerIsCaught(t *testing.T) {
	coverage.Covers(t, "tooling.conformance")

	events := []string{"flush", "save-offsets", "commit", "save-offsets", "commit"}

	if err := judgeCommitOrder(TriggerBatchFull, events, true); err == nil {
		t.Fatal("a second commit cycle with no ticker running must be caught")
	}
}

// The defect the invariant exists to catch, on the path that tolerates idle
// ticks: the tolerance must not extend to committing before anything flushed.
func TestToolingConformancePipelines_ACommitBeforeTheFlushIsCaughtOnTheInterval(t *testing.T) {
	coverage.Covers(t, "tooling.conformance")

	events := []string{"save-offsets", "commit", "flush"}

	if err := judgeCommitOrder(TriggerInterval, events, true); err == nil {
		t.Fatal("committing before the flush must be caught on every trigger")
	}
}

// A run that never flushed has nothing to have committed after.
func TestToolingConformancePipelines_ARunWithNoFlushIsCaught(t *testing.T) {
	coverage.Covers(t, "tooling.conformance")

	if err := judgeCommitOrder(TriggerInterval, []string{"save-offsets", "commit"}, true); err == nil {
		t.Fatal("a run with no flush must be caught")
	}
}

// An idle tick is a whole cycle. A bare commit with no offsets saved beside it
// breaks the atomicity pipeline.state.with_offsets depends on.
func TestToolingConformancePipelines_AnUnpairedCommitIsCaught(t *testing.T) {
	coverage.Covers(t, "tooling.conformance")

	events := []string{"flush", "save-offsets", "commit", "commit"}

	if err := judgeCommitOrder(TriggerInterval, events, true); err == nil {
		t.Fatal("a commit with no save-offsets beside it must be caught")
	}
}

// A stateless subject commits no offsets, so the flush is the whole sequence.
func TestToolingConformancePipelines_AStatelessRunIsJustTheFlush(t *testing.T) {
	coverage.Covers(t, "tooling.conformance")

	if err := judgeCommitOrder(TriggerInterval, []string{"flush"}, false); err != nil {
		t.Fatalf("a stateless run flushes and commits nothing: %v", err)
	}
	if err := judgeCommitOrder(TriggerInterval, []string{"flush", "commit"}, false); err == nil {
		t.Fatal("a stateless run that commits must be caught")
	}
}
