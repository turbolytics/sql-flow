package core

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
	"github.com/zeebo/assert"
)

type fakeSource struct {
	batches [][][]byte
	ch      chan [][]byte
	commits int
}

func (f *fakeSource) Start() error { return nil }

func (f *fakeSource) Stream() <-chan [][]byte {
	f.ch = make(chan [][]byte, len(f.batches))
	for _, b := range f.batches {
		f.ch <- b
	}
	close(f.ch)
	return f.ch
}

func (f *fakeSource) Commit() error { f.commits++; return nil }
func (f *fakeSource) Close() error  { return nil }

// fakeHandler emits a table with one row per buffered message, so the sink
// can be checked against the number of messages the source produced.
type fakeHandler struct {
	buffered int
}

func (h *fakeHandler) Init(ctx context.Context) error { h.buffered = 0; return nil }
func (h *fakeHandler) Write(msg []byte) error         { h.buffered++; return nil }

func (h *fakeHandler) Invoke(ctx context.Context) (arrow.Table, error) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "n", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	}, nil)

	b := array.NewRecordBuilder(memory.NewGoAllocator(), schema)
	defer b.Release()
	for i := 0; i < h.buffered; i++ {
		b.Field(0).(*array.Int64Builder).Append(int64(i))
	}
	rec := b.NewRecord()
	defer rec.Release()

	h.buffered = 0
	return array.NewTableFromRecords(schema, []arrow.Record{rec}), nil
}

type fakeSink struct {
	mu       sync.Mutex
	rows     int64
	flushes  int
	released bool
}

func (s *fakeSink) WriteTable(batch arrow.Table) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rows += batch.NumRows()
	return nil
}

func (s *fakeSink) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.flushes++
	return nil
}

func (s *fakeSink) Batch() (arrow.Table, error) { return nil, nil }

func newTestTurbine(src Source, h Handler, sink Sink, batchSize int) *Turbine {
	return NewTurbine(src, h, sink, batchSize, time.Second, &sync.Mutex{}, PipelineErrorPolicies{})
}

func messages(n int) [][]byte {
	msgs := make([][]byte, n)
	for i := range msgs {
		msgs[i] = []byte(`{"a": 1}`)
	}
	return msgs
}

// Reaching --max-msgs must not cost the batch in flight: every message the
// pipeline reports as consumed has to reach the sink.
func TestConsumeLoop_FlushesFinalBatchWhenMaxMsgsReached(t *testing.T) {
	src := &fakeSource{batches: [][][]byte{messages(1000)}}
	sink := &fakeSink{}

	tb := newTestTurbine(src, &fakeHandler{}, sink, 1000)
	stats, err := tb.ConsumeLoop(context.Background(), 1000)
	assert.NoError(t, err)

	assert.Equal(t, int64(1000), stats.MessagesConsumed())
	assert.Equal(t, int64(1000), sink.rows)
	assert.Equal(t, 1, sink.flushes)
}

// A batch that is still partial when the source ends must also be written,
// otherwise the tail of a finite stream is silently dropped.
func TestConsumeLoop_FlushesPartialBatchWhenStreamEnds(t *testing.T) {
	src := &fakeSource{batches: [][][]byte{messages(250)}}
	sink := &fakeSink{}

	tb := newTestTurbine(src, &fakeHandler{}, sink, 1000)
	stats, err := tb.ConsumeLoop(context.Background(), 0)
	assert.NoError(t, err)

	assert.Equal(t, int64(250), stats.MessagesConsumed())
	assert.Equal(t, int64(250), sink.rows)
	assert.Equal(t, 1, sink.flushes)
}

// failingHandler fails on the nominated phase, mirroring a message that
// cannot be parsed (write) or SQL that cannot bind (invoke).
type failingHandler struct {
	fakeHandler
	failWriteOn  string
	failInvokeOn bool
}

func (h *failingHandler) Write(msg []byte) error {
	if h.failWriteOn != "" && string(msg) == h.failWriteOn {
		return errors.New("invalid json")
	}
	return h.fakeHandler.Write(msg)
}

func (h *failingHandler) Invoke(ctx context.Context) (arrow.Table, error) {
	if h.failInvokeOn {
		return nil, errors.New(`Binder Error: Referenced column "broken" not found`)
	}
	return h.fakeHandler.Invoke(ctx)
}

// recordingSink captures the rows written to it, so DLQ contents can be
// inspected.
type recordingSink struct {
	mu      sync.Mutex
	rows    []map[string]string
	flushes int
}

func (s *recordingSink) WriteTable(batch arrow.Table) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	reader := array.NewTableReader(batch, 0)
	defer reader.Release()
	for reader.Next() {
		rec := reader.Record()
		for i := int64(0); i < rec.NumRows(); i++ {
			row := map[string]string{}
			for c := 0; c < int(rec.NumCols()); c++ {
				row[rec.ColumnName(c)] = rec.Column(c).ValueStr(int(i))
			}
			s.rows = append(s.rows, row)
		}
	}
	return nil
}

func (s *recordingSink) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.flushes++
	return nil
}

func (s *recordingSink) Batch() (arrow.Table, error) { return nil, nil }

func mixedMessages(bad string, good int) [][]byte {
	msgs := [][]byte{[]byte(bad)}
	return append(msgs, messages(good)...)
}

func TestConsumeLoop_RaisePolicyStopsOnWriteError(t *testing.T) {
	src := &fakeSource{batches: [][][]byte{mixedMessages("bad", 4)}}
	h := &failingHandler{failWriteOn: "bad"}

	tb := newTestTurbine(src, h, &fakeSink{}, 5)
	_, err := tb.ConsumeLoop(context.Background(), 0)
	assert.Error(t, err)
}

func TestConsumeLoop_IgnorePolicySkipsBadMessage(t *testing.T) {
	src := &fakeSource{batches: [][][]byte{mixedMessages("bad", 4)}}
	h := &failingHandler{failWriteOn: "bad"}
	sink := &fakeSink{}

	tb := NewTurbine(src, h, sink, 4, time.Second, &sync.Mutex{},
		PipelineErrorPolicies{Policy: PolicyIgnore})

	stats, err := tb.ConsumeLoop(context.Background(), 0)
	assert.NoError(t, err)

	// The 4 good messages are processed; the bad one is skipped but counted
	// as an error.
	assert.Equal(t, int64(4), sink.rows)
	assert.Equal(t, 1, stats.NumErrors)
}

// A message the handler rejects was still consumed from the source. The
// Python engine counts it, and --max-msgs has to account for it too or a
// stream of bad messages never terminates.
func TestConsumeLoop_CountsRejectedMessagesAsConsumed(t *testing.T) {
	src := &fakeSource{batches: [][][]byte{mixedMessages("bad", 4)}}
	h := &failingHandler{failWriteOn: "bad"}

	tb := NewTurbine(src, h, &fakeSink{}, 4, time.Second, &sync.Mutex{},
		PipelineErrorPolicies{Policy: PolicyIgnore})

	stats, err := tb.ConsumeLoop(context.Background(), 0)
	assert.NoError(t, err)

	assert.Equal(t, int64(5), stats.MessagesConsumed())
	assert.Equal(t, 1, stats.NumErrors)
}

func TestConsumeLoop_DLQPolicyRoutesWriteError(t *testing.T) {
	src := &fakeSource{batches: [][][]byte{mixedMessages("bad", 4)}}
	h := &failingHandler{failWriteOn: "bad"}
	sink := &fakeSink{}
	dlq := &recordingSink{}

	tb := NewTurbine(src, h, sink, 4, time.Second, &sync.Mutex{},
		PipelineErrorPolicies{Policy: PolicyDLQ, DLQSink: dlq})

	_, err := tb.ConsumeLoop(context.Background(), 0)
	assert.NoError(t, err)

	assert.Equal(t, int64(4), sink.rows)
	assert.Equal(t, 1, len(dlq.rows))
	assert.Equal(t, "handler.write", dlq.rows[0]["phase"])
	assert.Equal(t, "bad", dlq.rows[0]["message"])
	assert.That(t, dlq.rows[0]["error"] != "")
	assert.That(t, dlq.rows[0]["timestamp"] != "")
	assert.That(t, dlq.flushes > 0)
}

func TestConsumeLoop_DLQPolicyRoutesInvokeError(t *testing.T) {
	src := &fakeSource{batches: [][][]byte{messages(4)}}
	h := &failingHandler{failInvokeOn: true}
	sink := &fakeSink{}
	dlq := &recordingSink{}

	tb := NewTurbine(src, h, sink, 4, time.Second, &sync.Mutex{},
		PipelineErrorPolicies{Policy: PolicyDLQ, DLQSink: dlq})

	_, err := tb.ConsumeLoop(context.Background(), 0)
	assert.NoError(t, err)

	// The batch produced no table, so nothing reaches the main sink.
	assert.Equal(t, int64(0), sink.rows)
	assert.Equal(t, 1, len(dlq.rows))
	assert.Equal(t, "handler.invoke", dlq.rows[0]["phase"])
	assert.Equal(t, "Handler invocation failed", dlq.rows[0]["message"])
	assert.That(t, strings.Contains(dlq.rows[0]["error"], "Binder Error"))
}

func TestConsumeLoop_IgnorePolicyContinuesAfterInvokeError(t *testing.T) {
	src := &fakeSource{batches: [][][]byte{messages(4)}}
	h := &failingHandler{failInvokeOn: true}
	sink := &fakeSink{}

	tb := NewTurbine(src, h, sink, 4, time.Second, &sync.Mutex{},
		PipelineErrorPolicies{Policy: PolicyIgnore})

	stats, err := tb.ConsumeLoop(context.Background(), 0)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), sink.rows)
	assert.Equal(t, 1, stats.NumErrors)
}

func TestConsumeLoop_WritesEveryMessageAcrossManyBatches(t *testing.T) {
	src := &fakeSource{batches: [][][]byte{messages(500), messages(500), messages(250)}}
	sink := &fakeSink{}

	tb := newTestTurbine(src, &fakeHandler{}, sink, 100)
	stats, err := tb.ConsumeLoop(context.Background(), 0)
	assert.NoError(t, err)

	assert.Equal(t, int64(1250), stats.MessagesConsumed())
	assert.Equal(t, int64(1250), sink.rows)
}
