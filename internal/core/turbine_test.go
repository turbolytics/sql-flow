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
	batches [][]Message
	ch      chan []Message
	commits int
}

func (f *fakeSource) Start() error { return nil }

func (f *fakeSource) Stream() <-chan []Message {
	f.ch = make(chan []Message, len(f.batches))
	for _, b := range f.batches {
		f.ch <- b
	}
	close(f.ch)
	return f.ch
}

func (f *fakeSource) Commit() error { f.commits++; return nil }
func (f *fakeSource) Close() error  { return nil }

// markingSource is a fakeSource that can commit explicit positions, recording
// the marks it was handed at each commit.
type markingSource struct {
	fakeSource
	marks []*Marks
}

func (m *markingSource) CommitMarks(marks *Marks) error {
	// Copied because the pipeline keeps advancing the same Marks after this
	// returns; without the copy every recorded commit would show the final
	// state.
	copied := NewMarks()
	marks.Each(func(topic string, partition int32, mk Mark) {
		copied.Advance(topic, partition, mk)
	})
	m.marks = append(m.marks, copied)
	return nil
}

// kafkaMessages fabricates n messages from one partition with consecutive
// offsets, the way the Kafka source delivers them.
func kafkaMessages(topic string, partition int32, from, n int) []Message {
	out := make([]Message, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, Message{
			Value: []byte(`{"a":1}`), Topic: topic, Partition: partition,
			Offset: int64(from + i), LeaderEpoch: 7,
		})
	}
	return out
}

// fakeHandler emits a table with one row per buffered message, so the sink
// can be checked against the number of messages the source produced.
type fakeHandler struct {
	buffered int
}

func (h *fakeHandler) Init(ctx context.Context) error { h.buffered = 0; return nil }
func (h *fakeHandler) Write(msg []byte) error         { h.buffered++; return nil }

func (h *fakeHandler) Invoke(ctx context.Context) (arrow.Table, error) {
	// The real handlers return a nil table for an empty batch rather than an
	// error or an empty one; the fake has to agree or it hides that path.
	if h.buffered == 0 {
		return nil, nil
	}

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

func messages(n int) []Message {
	msgs := make([]Message, n)
	for i := range msgs {
		msgs[i] = Message{Value: []byte(`{"a": 1}`)}
	}
	return msgs
}

// Reaching --max-msgs must not cost the batch in flight: every message the
// pipeline reports as consumed has to reach the sink.
func TestConsumeLoop_FlushesFinalBatchWhenMaxMsgsReached(t *testing.T) {
	src := &fakeSource{batches: [][]Message{messages(1000)}}
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
	src := &fakeSource{batches: [][]Message{messages(250)}}
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

func mixedMessages(bad string, good int) []Message {
	msgs := []Message{{Value: []byte(bad)}}
	return append(msgs, messages(good)...)
}

func TestConsumeLoop_RaisePolicyStopsOnWriteError(t *testing.T) {
	src := &fakeSource{batches: [][]Message{mixedMessages("bad", 4)}}
	h := &failingHandler{failWriteOn: "bad"}

	tb := newTestTurbine(src, h, &fakeSink{}, 5)
	_, err := tb.ConsumeLoop(context.Background(), 0)
	assert.Error(t, err)
}

func TestConsumeLoop_IgnorePolicySkipsBadMessage(t *testing.T) {
	src := &fakeSource{batches: [][]Message{mixedMessages("bad", 4)}}
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
	src := &fakeSource{batches: [][]Message{mixedMessages("bad", 4)}}
	h := &failingHandler{failWriteOn: "bad"}

	tb := NewTurbine(src, h, &fakeSink{}, 4, time.Second, &sync.Mutex{},
		PipelineErrorPolicies{Policy: PolicyIgnore})

	stats, err := tb.ConsumeLoop(context.Background(), 0)
	assert.NoError(t, err)

	assert.Equal(t, int64(5), stats.MessagesConsumed())
	assert.Equal(t, 1, stats.NumErrors)
}

func TestConsumeLoop_DLQPolicyRoutesWriteError(t *testing.T) {
	src := &fakeSource{batches: [][]Message{mixedMessages("bad", 4)}}
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
	src := &fakeSource{batches: [][]Message{messages(4)}}
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
	src := &fakeSource{batches: [][]Message{messages(4)}}
	h := &failingHandler{failInvokeOn: true}
	sink := &fakeSink{}

	tb := NewTurbine(src, h, sink, 4, time.Second, &sync.Mutex{},
		PipelineErrorPolicies{Policy: PolicyIgnore})

	stats, err := tb.ConsumeLoop(context.Background(), 0)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), sink.rows)
	assert.Equal(t, 1, stats.NumErrors)
}

// blockingSource delivers a batch and then holds the stream open, standing in
// for a low-traffic topic or a push source between deliveries.
type blockingSource struct {
	batch   []Message
	ch      chan []Message
	release chan struct{}
}

// Same reason as newIdleSource: allocate off the consume-loop goroutine.
func newBlockingSource(batch []Message) *blockingSource {
	return &blockingSource{
		batch:   batch,
		ch:      make(chan []Message),
		release: make(chan struct{}),
	}
}

func (s *blockingSource) Start() error { return nil }

func (s *blockingSource) Stream() <-chan []Message {
	go func() {
		s.ch <- s.batch
		<-s.release
		close(s.ch)
	}()
	return s.ch
}

func (s *blockingSource) Commit() error { return nil }
func (s *blockingSource) Close() error  { return nil }

// A batch that never reaches batch_size must still be flushed once the flush
// interval elapses, otherwise a low-traffic topic stalls forever.
func TestConsumeLoop_FlushesPartialBatchOnFlushInterval(t *testing.T) {
	src := newBlockingSource(messages(10))
	sink := &fakeSink{}

	tb := NewTurbine(src, &fakeHandler{}, sink, 1000, 50*time.Millisecond,
		&sync.Mutex{}, PipelineErrorPolicies{})

	done := make(chan struct{})
	go func() {
		_, _ = tb.ConsumeLoop(context.Background(), 0)
		close(done)
	}()

	deadline := time.After(5 * time.Second)
	for {
		sink.mu.Lock()
		rows := sink.rows
		sink.mu.Unlock()
		if rows == 10 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("partial batch never flushed, sink saw %d rows", rows)
		default:
			time.Sleep(5 * time.Millisecond)
		}
	}

	close(src.release)
	<-done
}

func TestConsumeLoop_WritesEveryMessageAcrossManyBatches(t *testing.T) {
	src := &fakeSource{batches: [][]Message{messages(500), messages(500), messages(250)}}
	sink := &fakeSink{}

	tb := newTestTurbine(src, &fakeHandler{}, sink, 100)
	stats, err := tb.ConsumeLoop(context.Background(), 0)
	assert.NoError(t, err)

	assert.Equal(t, int64(1250), stats.MessagesConsumed())
	assert.Equal(t, int64(1250), sink.rows)
}

// A batch whose messages were all rejected still reaches the handler, because
// a rejected message was consumed and is counted. The handler has nothing
// buffered, which is a batch with no rows -- not a second error on top of the
// per-message ones the policy already handled.
func TestConsumeLoop_BatchOfOnlyBadMessagesIsNotAHandlerError(t *testing.T) {
	src := &fakeSource{batches: [][]Message{{{Value: []byte("bad")}, {Value: []byte("bad")}}}}
	h := &failingHandler{failWriteOn: "bad"}
	sink := &fakeSink{}

	tb := NewTurbine(src, h, sink, 2, time.Second, &sync.Mutex{},
		PipelineErrorPolicies{Policy: PolicyIgnore})

	stats, err := tb.ConsumeLoop(context.Background(), 0)
	assert.NoError(t, err)

	assert.Equal(t, int64(0), sink.rows)
	// Two rejected messages, and no extra error from invoking an empty batch.
	assert.Equal(t, 2, stats.NumErrors)
}

// The Kafka source polls ahead of the pipeline into a buffer, so "commit
// everything fetched" commits messages the pipeline has not processed: after
// one 20,000-message batch it had committed offset 70,086. A crash then loses
// the difference with the consumer group showing no lag. The pipeline must
// instead hand the source the exact position it has finished with.
func TestConsumeLoop_CommitsOnlyProcessedMarks(t *testing.T) {
	src := &markingSource{fakeSource: fakeSource{
		// One fetch delivers 30 messages; batch_size is 20, so the first
		// commit must name offset 19 and the final one 29 -- never 29 twice.
		batches: [][]Message{kafkaMessages("events", 0, 0, 30)},
	}}
	tb := newTestTurbine(src, &fakeHandler{}, &fakeSink{}, 20)

	_, err := tb.ConsumeLoop(context.Background(), 0)
	assert.NoError(t, err)

	assert.Equal(t, 2, len(src.marks))
	first, ok := src.marks[0].Get("events", 0)
	assert.That(t, ok)
	assert.Equal(t, Mark{Offset: 19, LeaderEpoch: 7}, first)
	second, ok := src.marks[1].Get("events", 0)
	assert.That(t, ok)
	assert.Equal(t, Mark{Offset: 29, LeaderEpoch: 7}, second)
	// A source that can take marks must not also get the blanket commit.
	assert.Equal(t, 0, src.commits)
}

func TestConsumeLoop_MarksTrackEachPartition(t *testing.T) {
	p0 := kafkaMessages("events", 0, 100, 3)
	p1 := kafkaMessages("events", 1, 500, 2)
	src := &markingSource{fakeSource: fakeSource{
		batches: [][]Message{{p0[0], p1[0], p0[1], p1[1], p0[2]}},
	}}
	tb := newTestTurbine(src, &fakeHandler{}, &fakeSink{}, 10)

	_, err := tb.ConsumeLoop(context.Background(), 0)
	assert.NoError(t, err)

	assert.Equal(t, 1, len(src.marks))
	mark0, _ := src.marks[0].Get("events", 0)
	mark1, _ := src.marks[0].Get("events", 1)
	assert.Equal(t, int64(102), mark0.Offset)
	assert.Equal(t, int64(501), mark1.Offset)
}

// --- Task 4: transactional batch -------------------------------------------

// orderingSink records the calls made to it, so the sequence that makes state
// and offsets atomic can be asserted rather than assumed.
type orderingSink struct {
	events *[]string
	fail   bool
}

func (s *orderingSink) WriteTable(batch arrow.Table) error { return nil }

func (s *orderingSink) Flush() error {
	if s.fail {
		*s.events = append(*s.events, "flush-failed")
		return errors.New("sink unreachable")
	}
	*s.events = append(*s.events, "flush")
	return nil
}

func (s *orderingSink) Batch() (arrow.Table, error) { return nil, nil }

// fakeOffsetStore stands in for the DuckDB-backed store so the ordering test
// needs no database.
type fakeOffsetStore struct {
	events *[]string
	saved  []*Marks
	fail   bool
}

func (s *fakeOffsetStore) Save(ctx context.Context, marks *Marks) error {
	if s.fail {
		*s.events = append(*s.events, "save-offsets-failed")
		return errors.New("disk full")
	}
	*s.events = append(*s.events, "save-offsets")
	copied := NewMarks()
	marks.Each(func(topic string, partition int32, mk Mark) {
		copied.Advance(topic, partition, mk)
	})
	s.saved = append(s.saved, copied)
	return nil
}

// txConn records the transaction boundary calls.
type txConn struct {
	events *[]string
	fail   bool
}

func (c *txConn) Commit(ctx context.Context) error {
	if c.fail {
		*c.events = append(*c.events, "commit-failed")
		return errors.New("commit rejected")
	}
	*c.events = append(*c.events, "commit")
	return nil
}

func (c *txConn) Rollback(ctx context.Context) error {
	*c.events = append(*c.events, "rollback")
	return nil
}

// The external sink must flush BEFORE the transaction commits. A crash
// between them replays the batch -- a duplicate the sink can absorb -- rather
// than committing offsets for rows the sink never received, which loses them
// with the consumer group reporting no lag.
func TestProcessBatch_FlushesSinkBeforeCommittingState(t *testing.T) {
	var events []string
	src := &markingSource{fakeSource: fakeSource{
		batches: [][]Message{kafkaMessages("events", 0, 0, 4)},
	}}
	store := &fakeOffsetStore{events: &events}
	conn := &txConn{events: &events}

	tb := NewTurbine(src, &fakeHandler{}, &orderingSink{events: &events}, 4,
		time.Second, &sync.Mutex{}, PipelineErrorPolicies{},
		WithStateStore(store, conn))

	_, err := tb.ConsumeLoop(context.Background(), 0)
	assert.NoError(t, err)

	assert.Equal(t, []string{"flush", "save-offsets", "commit"}, events)
}

// A sink failure must roll the transaction back, so the offsets on disk stay
// where they were and the batch is replayed on restart.
func TestProcessBatch_RollsBackWhenSinkFails(t *testing.T) {
	var events []string
	src := &markingSource{fakeSource: fakeSource{
		batches: [][]Message{kafkaMessages("events", 0, 0, 4)},
	}}
	store := &fakeOffsetStore{events: &events}
	conn := &txConn{events: &events}

	tb := NewTurbine(src, &fakeHandler{}, &orderingSink{events: &events, fail: true}, 4,
		time.Second, &sync.Mutex{}, PipelineErrorPolicies{},
		WithStateStore(store, conn))

	_, err := tb.ConsumeLoop(context.Background(), 0)
	assert.Error(t, err)
	assert.Equal(t, []string{"flush-failed", "rollback"}, events)
}

// A failure saving offsets must roll back too: state written by the handler
// in this transaction has to go with the offsets that describe it.
func TestProcessBatch_RollsBackWhenOffsetSaveFails(t *testing.T) {
	var events []string
	src := &markingSource{fakeSource: fakeSource{
		batches: [][]Message{kafkaMessages("events", 0, 0, 4)},
	}}
	store := &fakeOffsetStore{events: &events, fail: true}
	conn := &txConn{events: &events}

	tb := NewTurbine(src, &fakeHandler{}, &orderingSink{events: &events}, 4,
		time.Second, &sync.Mutex{}, PipelineErrorPolicies{},
		WithStateStore(store, conn))

	_, err := tb.ConsumeLoop(context.Background(), 0)
	assert.Error(t, err)
	assert.Equal(t, []string{"flush", "save-offsets-failed", "rollback"}, events)
}

// A commit failure is fatal to the batch and must not be followed by a Kafka
// commit: the durable offsets did not move, so Kafka's must not either.
func TestProcessBatch_CommitFailureDoesNotCommitSource(t *testing.T) {
	var events []string
	src := &markingSource{fakeSource: fakeSource{
		batches: [][]Message{kafkaMessages("events", 0, 0, 4)},
	}}
	store := &fakeOffsetStore{events: &events}
	conn := &txConn{events: &events, fail: true}

	tb := NewTurbine(src, &fakeHandler{}, &orderingSink{events: &events}, 4,
		time.Second, &sync.Mutex{}, PipelineErrorPolicies{},
		WithStateStore(store, conn))

	_, err := tb.ConsumeLoop(context.Background(), 0)
	assert.Error(t, err)
	assert.Equal(t, []string{"flush", "save-offsets", "commit-failed", "rollback"}, events)
	// Kafka must not have been told anything.
	assert.Equal(t, 0, len(src.marks))
}

// The offsets handed to the store are the ones the pipeline processed, not
// whatever the source has fetched ahead to.
func TestProcessBatch_SavesTheProcessedOffsets(t *testing.T) {
	var events []string
	src := &markingSource{fakeSource: fakeSource{
		batches: [][]Message{kafkaMessages("events", 0, 0, 4)},
	}}
	store := &fakeOffsetStore{events: &events}

	tb := NewTurbine(src, &fakeHandler{}, &orderingSink{events: &events}, 4,
		time.Second, &sync.Mutex{}, PipelineErrorPolicies{},
		WithStateStore(store, &txConn{events: &events}))

	_, err := tb.ConsumeLoop(context.Background(), 0)
	assert.NoError(t, err)

	assert.Equal(t, 1, len(store.saved))
	got, ok := store.saved[0].Get("events", 0)
	assert.That(t, ok)
	assert.Equal(t, int64(3), got.Offset)
}

// Without a state store the pipeline behaves exactly as before: no
// transaction calls at all, and the source is still committed.
func TestProcessBatch_NoStateStoreIsUnchanged(t *testing.T) {
	var events []string
	src := &markingSource{fakeSource: fakeSource{
		batches: [][]Message{kafkaMessages("events", 0, 0, 4)},
	}}

	tb := newTestTurbine(src, &fakeHandler{}, &orderingSink{events: &events}, 4)

	_, err := tb.ConsumeLoop(context.Background(), 0)
	assert.NoError(t, err)

	assert.Equal(t, []string{"flush"}, events)
	assert.Equal(t, 1, len(src.marks))
}

// The state gauges are recorded on StatusLoop's existing tick, from the
// dedicated reader connection -- not the pipeline's writer -- so a scrape can
// never stall batch processing.
func TestRecordStateGauges_ReportsSizeAndRows(t *testing.T) {
	tb := newTestTurbine(&fakeSource{}, &fakeHandler{}, &fakeSink{}, 4)

	calls := 0
	tb.stateStats = func() (*StateStats, error) {
		calls++
		return &StateStats{
			Path:      "/state/state.db",
			SizeBytes: 4096,
			Tables:    []TableStat{{Name: "agg", Rows: 24}, {Name: "other", Rows: 7}},
		}, nil
	}

	tb.recordStateGauges(context.Background())
	assert.Equal(t, 1, calls)
}

// A pipeline with no state database records nothing rather than reporting
// zero: an absent series and a genuinely empty state are different facts, and
// a dashboard should be able to tell them apart.
func TestRecordStateGauges_NoProviderRecordsNothing(t *testing.T) {
	tb := newTestTurbine(&fakeSource{}, &fakeHandler{}, &fakeSink{}, 4)
	// stateStats is nil; this must not panic.
	tb.recordStateGauges(context.Background())
}

// A failure collecting stats must not take the status loop down with it --
// the pipeline keeps running and keeps serving its other metrics.
func TestRecordStateGauges_SurvivesCollectionFailure(t *testing.T) {
	tb := newTestTurbine(&fakeSource{}, &fakeHandler{}, &fakeSink{}, 4)
	tb.stateStats = func() (*StateStats, error) {
		return nil, errors.New("state database unreadable")
	}

	tb.recordStateGauges(context.Background())
}

// idleSource never delivers a batch, so the consume loop only ever wakes on
// the flush ticker. It is the shape of a pipeline whose topic has gone quiet.
type idleSource struct {
	ch      chan []Message
	release chan struct{}
}

// Channels are allocated here, not in Stream: Stream runs on the consume-loop
// goroutine while the test closes release from its own.
func newIdleSource() *idleSource {
	return &idleSource{
		ch:      make(chan []Message),
		release: make(chan struct{}),
	}
}

func (s *idleSource) Start() error { return nil }

func (s *idleSource) Stream() <-chan []Message {
	go func() {
		<-s.release
		close(s.ch)
	}()
	return s.ch
}

func (s *idleSource) Commit() error { return nil }
func (s *idleSource) Close() error  { return nil }

// An idle stateful pipeline must still close its transaction on the flush
// tick. DuckDB's now() is the transaction's start time, so a transaction held
// open freezes the clock that every window close predicate is evaluated
// against, and a pipeline that stops receiving messages silently stops
// closing windows.
func TestConsumeLoop_IdleTickCommitsTheStateTransaction(t *testing.T) {
	var events []string
	src := newIdleSource()
	store := &fakeOffsetStore{events: &events}
	conn := &txConn{events: &events}

	tb := NewTurbine(src, &fakeHandler{}, &fakeSink{}, 1000,
		25*time.Millisecond, &sync.Mutex{}, PipelineErrorPolicies{},
		WithStateStore(store, conn))

	done := make(chan struct{})
	go func() {
		_, _ = tb.ConsumeLoop(context.Background(), 0)
		close(done)
	}()

	deadline := time.After(5 * time.Second)
	for {
		tb.lock.Lock()
		n := 0
		for _, e := range events {
			if e == "commit" {
				n++
			}
		}
		tb.lock.Unlock()
		if n >= 2 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("idle pipeline never committed; events=%v", events)
		default:
			time.Sleep(5 * time.Millisecond)
		}
	}

	close(src.release)
	<-done
}

// The same tick on a pipeline with no state database must stay the no-op it
// always was: no commit, no offset save, nothing.
func TestConsumeLoop_IdleTickIsANoopWithoutState(t *testing.T) {
	src := newIdleSource()
	sink := &fakeSink{}

	tb := NewTurbine(src, &fakeHandler{}, sink, 1000,
		10*time.Millisecond, &sync.Mutex{}, PipelineErrorPolicies{})

	done := make(chan struct{})
	go func() {
		_, _ = tb.ConsumeLoop(context.Background(), 0)
		close(done)
	}()

	time.Sleep(150 * time.Millisecond)
	close(src.release)
	<-done

	sink.mu.Lock()
	defer sink.mu.Unlock()
	// An idle pipeline must not flush anything to the sink.
	assert.Equal(t, int64(0), sink.rows)
}

// The lag metric must not allocate per message. metric.WithAttributes builds
// its attribute set before any provider decides whether to keep the
// measurement, so building it inline cost an allocation on every message even
// with metrics switched off.
func BenchmarkMark(b *testing.B) {
	tb := newTestTurbine(&fakeSource{}, &fakeHandler{}, &fakeSink{}, 1000)
	msg := Message{
		Topic: "events", Partition: 0, Offset: 1,
		LeaderEpoch: 1, HighWatermark: 1000,
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		msg.Offset = int64(i)
		tb.mark(msg)
	}
}
