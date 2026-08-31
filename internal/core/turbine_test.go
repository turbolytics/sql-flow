package core

import (
	"context"
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

func TestConsumeLoop_WritesEveryMessageAcrossManyBatches(t *testing.T) {
	src := &fakeSource{batches: [][][]byte{messages(500), messages(500), messages(250)}}
	sink := &fakeSink{}

	tb := newTestTurbine(src, &fakeHandler{}, sink, 100)
	stats, err := tb.ConsumeLoop(context.Background(), 0)
	assert.NoError(t, err)

	assert.Equal(t, int64(1250), stats.MessagesConsumed())
	assert.Equal(t, int64(1250), sink.rows)
}
