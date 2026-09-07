package conformance

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

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
	Sinks(t, subject(newMemSink()))
}

// #221's defect: a failed Flush discards the buffer, so the retry finds
// nothing to send and reports a success that delivered no rows.
func TestToolingConformanceSinks_ASinkThatDropsItsBatchIsCaught(t *testing.T) {
	v := verdicts(t, subject(&dropSink{memSink: newMemSink()}))[keepsBatch]

	assert.True(t, v.failure != "")
	assert.True(t, strings.Contains(v.failure, "no rows"))
}

// A sink that delivers on WriteTable rather than on Flush: the Kafka sink's
// shape today. It reaches the destination before the fault, so a read-back
// after the retry finds the row and every downstream check passes for the
// wrong reason. Only a read-back before the first Flush catches it.
func TestToolingConformanceSinks_ASinkThatWritesThroughIsCaught(t *testing.T) {
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
	vs := verdicts(t, subject(&dropSink{memSink: newMemSink()}))

	assert.Equal(t, "", vs[buffersOnly].failure)
	assert.True(t, vs[keepsBatch].failure != "")
}

// A sink that keeps the batch but never clears it delivers the row twice.
func TestToolingConformanceSinks_ASinkThatDeliversTwiceIsCaught(t *testing.T) {
	v := verdicts(t, subject(&neverClearSink{memSink: newMemSink()}))[keepsBatch]

	assert.True(t, strings.Contains(v.failure, "id=1, id=1"))
}

// A retry that fails is not the same defect, and the message must not blame
// the buffer for it.
func TestToolingConformanceSinks_ASinkThatCannotRecoverIsCaught(t *testing.T) {
	v := verdicts(t, subject(&staysDownSink{memSink: newMemSink()}))[keepsBatch]

	assert.True(t, strings.Contains(v.failure, "Flush after Heal failed"))
}

func TestToolingConformanceSinks_ASubjectWithNothingToBreakIsSkipped(t *testing.T) {
	s := subject(newMemSink())
	s.Break, s.Heal = nil, nil

	vs := verdicts(t, s)
	for _, id := range []string{buffersOnly, keepsBatch} {
		assert.True(t, vs[id].skipped != "")
		assert.True(t, strings.Contains(vs[id].skipped, "integrations.yml"))
		assert.Equal(t, "", vs[id].failure)
	}
}

// A skip is not coverage, so a skipped verdict must not emit a marker. The
// matrix would otherwise read a subject that exercises nothing as covered,
// which is the sink.iceberg failure.
func TestToolingConformanceSinks_ASkippedVerdictEmitsNoMarker(t *testing.T) {
	s := subject(newMemSink())
	s.Break, s.Heal = nil, nil

	for _, v := range sinkVerdicts(t, s) {
		assert.Equal(t, "", v.failure)
		assert.True(t, v.skipped != "")
	}
}

func TestToolingConformanceSinks_ASubjectWithoutAnIntegrationIdIsRejected(t *testing.T) {
	s := subject(newMemSink())
	s.Integration = ""

	assert.True(t, fatals(t, func(t *testing.T) { requireSubject(t, s) }))
}

// Break without Heal would leave the destination broken for whatever runs
// next, which is a fault the next subject cannot explain.
func TestToolingConformanceSinks_ASubjectWithBreakAndNoHealIsRejected(t *testing.T) {
	s := subject(newMemSink())
	s.Heal = nil

	assert.True(t, fatals(t, func(t *testing.T) { requireSubject(t, s) }))
}

func TestToolingConformanceSinks_ASubjectMissingReadBackIsRejected(t *testing.T) {
	s := subject(newMemSink())
	s.ReadBack = nil

	assert.True(t, fatals(t, func(t *testing.T) { requireSubject(t, s) }))
}

// The harness must not report a sink defect when the subject's own fault
// injection did nothing: that sends the reader to the wrong file.
func TestToolingConformanceSinks_ABreakThatDoesNotBreakFailsTheSubjectNotTheSink(t *testing.T) {
	s := subject(newMemSink())
	s.Break = func(*testing.T) {} // does nothing
	s.Heal = func(*testing.T) {}

	assert.True(t, fatals(t, func(t *testing.T) { sinkVerdicts(t, s) }))
}

// A write-through sink also flushes successfully while broken, and it must
// not be reported as a broken fault: buffers_only already showed that the
// rows went out early, so the sink is at fault and the subject is not.
func TestToolingConformanceSinks_AWriteThroughSinkIsNotBlamedOnTheSubject(t *testing.T) {
	vs := verdicts(t, subject(&writeThroughSink{memSink: newMemSink()}))

	assert.True(t, vs[buffersOnly].failure != "")
	assert.True(t, vs[keepsBatch].failure != "")
}

func TestToolingConformanceDescribe_NamesTheRowsItFound(t *testing.T) {
	assert.Equal(t, "no rows", describe(nil))
	assert.Equal(t, "rows [id=1]", describe([]Row{{"id": int64(1)}}))
	assert.Equal(t, "rows [id=1, id=2]",
		describe([]Row{{"id": int64(1)}, {"id": int64(2)}}))
}

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

func (m *memSink) Batch() (arrow.Table, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.buffered) == 0 {
		return nil, nil
	}
	return m.buffered[0], nil
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

// staysDownSink never recovers, so the retry fails rather than delivering.
type staysDownSink struct{ *memSink }

func (s *staysDownSink) Flush(context.Context) error {
	return errors.New("destination unreachable")
}

// --- Helpers ---------------------------------------------------------------

type breakable interface {
	core.Sink
	set(down bool)
	rows() []Row
}

func subject(sink breakable) SinkSubject {
	return SinkSubject{
		// sink.noop, not a sink with a real destination. These tests run in
		// the unit pass against an in-memory fake, and the marker Sinks emits
		// must not credit a sink this never touched. noop is exempt from both
		// invariants, so the marker lands on an exempt cell and changes
		// nothing -- which is what a fake proving nothing should do.
		Integration: "sink.noop",
		New:         func(*testing.T) core.Sink { return sink },
		Break:       func(*testing.T) { sink.set(true) },
		Heal:        func(*testing.T) { sink.set(false) },
		ReadBack:    func(*testing.T) []Row { return sink.rows() },
		Table:       oneRow,
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
	assert.Equal(t, 2, len(out))
	return out
}

// Every invariant the harness judges must be declared, or its marker reports
// as unknown and the cell never appears.
func TestToolingConformanceSinks_JudgesOnlyDeclaredInvariants(t *testing.T) {
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
