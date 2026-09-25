// Package simulate replays the events that have produced defects against a
// real consume loop: a batch, an idle tick, a restart, partitions moving, and
// the wall clock stepping.
//
// The loop is the engine's own. What is faked is everything around it: a
// source that owns partitions and answers Deliverer, a coordinator holding
// what Kafka guarantees, and a sink that records what it was handed. The
// turbine's clock and its flush tick come from the script, so a sequence is a
// sequence rather than a race.
//
// What it cannot prove: a fake source is not franz-go. Whether a commit from
// a stale generation is refused, and whether a revoke callback really blocks
// the rebalance, are assumptions the coordinator encodes and an integration
// test against a broker has to keep honest. See #183.
package simulate

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/turbolytics/sql-flow/internal/core"
)

const topic = "sim"

// Step is one event in a script.
type Step interface{ apply(*run) }

// Produce delivers rows on a partition the worker owns.
type Produce struct {
	Partition int32
	Rows      int
	// At is the rows' event time: the instant the data says it happened,
	// which is not the instant it arrives. Zero means now, so a script that
	// does not care reads as it did before.
	//
	// This is the whole point of the step. A window decides on event time,
	// and a simulator whose event time is its own clock cannot produce a row
	// that arrives out of order, a row for a bucket that already closed, or
	// a device whose clock is wrong -- which is every question a watermark
	// can get wrong.
	At time.Time
}

// IdleTick fires the flush trigger: a commit with nothing buffered.
type IdleTick struct{}

// Elapse moves the simulated clock forward, as time passing does.
//
// Not a clock step: the turbine reads one injected instant, so a fake cannot
// move its wall reading while holding its monotonic one, and a step would be
// indistinguishable from elapsing. Clock steps are checked in the model,
// which carries both readings (internal/managers/model.go).
type Elapse struct{ By time.Duration }

// Revoke takes a partition away, as a rebalance does.
type Revoke struct{ Partition int32 }

// Assign gives one back.
type Assign struct{ Partition int32 }

// Restart kills the worker and starts a new one, which resumes from the
// committed offsets and replays whatever they did not cover.
type Restart struct{}

// Result is what the run produced and what the sink received.
type Result struct {
	Produced   int
	Published  int
	Duplicated int
	Missing    []int64
}

// coordinator holds what Kafka guarantees and nothing else: who owns what,
// how far each partition is committed, and a generation that refuses a commit
// from a worker whose assignment has moved on.
type coordinator struct {
	mu         sync.Mutex
	log        map[int32][]int64 // partition -> ids, in order
	committed  map[int32]int64   // partition -> next offset to deliver
	generation int
}

func newCoordinator() *coordinator {
	return &coordinator{log: map[int32][]int64{}, committed: map[int32]int64{}}
}

func (c *coordinator) append(p int32, ids []int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.log[p] = append(c.log[p], ids...)
}

func (c *coordinator) commit(p int32, next int64, generation int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if generation != c.generation {
		return // a stale generation commits nothing
	}
	if next > c.committed[p] {
		c.committed[p] = next
	}
}

func (c *coordinator) uncommitted(p int32) (int64, []int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	from := c.committed[p]
	ids := c.log[p]
	if from >= int64(len(ids)) {
		return from, nil
	}
	return from, append([]int64(nil), ids[from:]...)
}

// source is a core.Source that owns partitions and says whether it can
// deliver, the way the Kafka source does through its partition relay.
type source struct {
	mu         sync.Mutex
	ch         chan []core.Message
	owned      map[int32]bool
	assignedAt time.Time
	clock      func() time.Time
	coord      *coordinator
	generation int
	closed     bool
}

func newSource(coord *coordinator, clock func() time.Time, owned ...int32) *source {
	s := &source{
		ch:    make(chan []core.Message),
		owned: map[int32]bool{},
		clock: clock,
		coord: coord,
	}
	for _, p := range owned {
		s.owned[p] = true
	}
	s.assignedAt = clock()
	return s
}

func (s *source) Start() error                                  { return nil }
func (s *source) Stream() <-chan []core.Message                 { return s.ch }
func (s *source) Commit() error                                 { return nil }
func (s *source) OnPartitions(a, r, l func(map[string][]int32)) {}

func (s *source) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.closed {
		s.closed = true
		close(s.ch)
	}
	return nil
}

// Delivering reports the partitions this worker holds, and since when.
func (s *source) Delivering() (time.Duration, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.owned) == 0 {
		return 0, false
	}
	return s.clock().Sub(s.assignedAt), true
}

// CommitMarks hands the positions the pipeline finished with to the group.
func (s *source) CommitMarks(marks *core.Marks) error {
	s.mu.Lock()
	gen := s.generation
	s.mu.Unlock()
	marks.Each(func(_ string, partition int32, mark core.Mark) {
		s.coord.commit(partition, mark.Offset+1, gen)
	})
	return nil
}

func (s *source) revoke(p int32) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.owned, p)
}

func (s *source) assign(p int32) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.owned[p] = true
	s.assignedAt = s.clock()
}

func (s *source) owns(p int32) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.owned[p]
}

// handler collects the ids written to it and hands them back as one column,
// which is the smallest thing the loop will accept.
type handler struct {
	mu  sync.Mutex
	ids []int64
}

func (h *handler) Init(context.Context) error { return nil }

func (h *handler) Write(msg []byte) error {
	var id int64
	if _, err := fmt.Sscanf(string(msg), "%d", &id); err != nil {
		return err
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.ids = append(h.ids, id)
	return nil
}

func (h *handler) Invoke(context.Context) (arrow.Table, error) {
	h.mu.Lock()
	ids := h.ids
	h.ids = nil
	h.mu.Unlock()

	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)
	b := array.NewInt64Builder(memory.DefaultAllocator)
	defer b.Release()
	b.AppendValues(ids, nil)
	col := b.NewArray()
	defer col.Release()
	rec := array.NewRecord(schema, []arrow.Array{col}, int64(len(ids)))
	defer rec.Release()
	return array.NewTableFromRecords(schema, []arrow.Record{rec}), nil
}

func (h *handler) RowsRead() int64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return int64(len(h.ids))
}

// sink records every id it is handed, so a run can say what was published and
// what was published twice.
type sink struct {
	mu    sync.Mutex
	seen  map[int64]int
	total int
}

func newSink() *sink { return &sink{seen: map[int64]int{}} }

func (s *sink) WriteTable(_ context.Context, t arrow.Table) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	col := t.Column(0)
	for _, chunk := range col.Data().Chunks() {
		ids := chunk.(*array.Int64)
		for i := 0; i < ids.Len(); i++ {
			s.seen[ids.Value(i)]++
			s.total++
		}
	}
	return nil
}

func (s *sink) Flush(context.Context) error { return nil }

func (s *sink) counts() (total int, duplicated int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, n := range s.seen {
		if n > 1 {
			duplicated += n - 1
		}
	}
	return s.total, duplicated
}

// run is one scripted sequence.
type run struct {
	t       *testing.T
	coord   *coordinator
	sink    *sink
	src     *source
	tb      *core.Turbine
	trigger chan time.Time
	cancel  context.CancelFunc
	done    chan struct{}

	// window is set when the run pairs the loop with a real manager.
	window *windowRun

	mu       sync.Mutex
	now      time.Time
	produced int
	nextID   int64
	owned    []int32
	// eventAt is every produced row's event time, by id, so a replay after a
	// restart re-delivers it with the time it was produced with.
	eventAt map[int64]time.Time
}

// Run replays a script and reports what the group delivered.
func Run(t *testing.T, owned []int32, script []Step) Result {
	t.Helper()
	r := &run{
		t:       t,
		coord:   newCoordinator(),
		sink:    newSink(),
		now:     time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC),
		owned:   owned,
		eventAt: map[int64]time.Time{},
	}
	r.start()
	for _, s := range script {
		r.elapse()
		s.apply(r)
	}
	r.stop()

	total, duplicated := r.sink.counts()
	return Result{
		Produced:   r.produced,
		Published:  total,
		Duplicated: duplicated,
		Missing:    r.missing(),
	}
}

// elapse moves the simulated clock a second before every step, so a sequence
// spans time without the script saying so and two commits never land on the
// same instant, which the progress write's own throttle would skip.
func (r *run) elapse() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.now = r.now.Add(time.Second)
}

func (r *run) clock() time.Time {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.now
}

// start builds a worker on the current committed offsets and replays what
// they do not cover, which is what a restarted process sees.
func (r *run) start() {
	r.src = newSource(r.coord, r.clock, r.owned...)
	r.trigger = make(chan time.Time)
	r.sinkTurbine()

	ctx, cancel := context.WithCancel(context.Background())
	r.cancel = cancel
	r.done = make(chan struct{})
	go func() {
		_, _ = r.tb.ConsumeLoop(ctx, 0)
		close(r.done)
	}()

	for _, p := range r.owned {
		if _, ids := r.coord.uncommitted(p); len(ids) > 0 {
			r.deliver(p, ids)
		}
	}
}

func (r *run) sinkTurbine() {
	opts := []core.TurbineOption{
		core.WithClock(r.clock), core.WithFlushTrigger(r.trigger),
	}
	var h core.Handler = &handler{}
	if r.window != nil {
		// A windowed pipeline's handler writes the window table and its sink
		// receives nothing; the progress row is what the manager reads.
		h = r.window.handler
		opts = append(opts, core.WithProgressStore(core.NewProgressStore(r.window.db.pipeline)))
	}
	r.tb = core.NewTurbine(r.src, h, r.sink, 1, time.Hour,
		&sync.Mutex{}, core.PipelineErrorPolicies{}, opts...)
}

func (r *run) stop() {
	r.cancel()
	_ = r.src.Close()
	<-r.done
}

// deliver hands a batch to the loop and waits for the sink to hold it, which
// is the synchronisation point that makes a script deterministic.
func (r *run) deliver(p int32, ids []int64) {
	from, _ := r.coord.uncommitted(p)
	batch := make([]core.Message, 0, len(ids))
	for i, id := range ids {
		r.mu.Lock()
		at := r.eventAt[id]
		r.mu.Unlock()
		batch = append(batch, core.Message{
			// id and event time, which is what a real payload carries: the
			// handler's SQL reads the timestamp out of the record.
			Value:     []byte(fmt.Sprintf("%d,%d", id, at.UnixMicro())),
			Topic:     topic,
			Partition: p,
			Offset:    from + int64(i),
		})
	}
	var want int
	if r.window == nil {
		total, _ := r.sink.counts()
		want = total + len(batch)
	} else {
		want = int(r.windowRows()) + len(batch)
	}

	select {
	case r.src.ch <- batch:
	case <-time.After(5 * time.Second):
		r.t.Fatal("the loop never took the batch")
	}
	if r.window == nil {
		r.await(func() bool { total, _ := r.sink.counts(); return total >= want })
		return
	}
	// The window table plus what the manager has already published: a close
	// between the write and this read moves rows from one to the other.
	r.await(func() bool {
		published, _ := r.window.sink.counts()
		return int(r.windowRows()+published) >= want
	})
}

func (r *run) await(cond func() bool) {
	r.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			r.t.Fatal("the run did not reach the state the step waited for")
		}
		time.Sleep(time.Millisecond)
	}
}

func (r *run) missing() []int64 {
	r.sink.mu.Lock()
	defer r.sink.mu.Unlock()
	var out []int64
	r.coord.mu.Lock()
	defer r.coord.mu.Unlock()
	for _, ids := range r.coord.log {
		for _, id := range ids {
			if r.sink.seen[id] == 0 {
				out = append(out, id)
			}
		}
	}
	return out
}

func (p Produce) apply(r *run) {
	if !r.src.owns(p.Partition) {
		return // another worker owns it; nothing arrives here
	}
	at := p.At
	if at.IsZero() {
		at = r.clock()
	}
	ids := make([]int64, 0, p.Rows)
	r.mu.Lock()
	for i := 0; i < p.Rows; i++ {
		r.nextID++
		ids = append(ids, r.nextID)
		// Kept by id rather than in the log, so a replay after a restart
		// delivers each row with the event time it was produced with. A
		// record's event time does not change because it was read twice.
		r.eventAt[r.nextID] = at
	}
	r.produced += p.Rows
	r.mu.Unlock()

	r.coord.append(p.Partition, ids)
	r.deliver(p.Partition, ids)
}

func (IdleTick) apply(r *run) {
	// The commit the tick causes is what a poll after it reads, so the step
	// waits for it: handing off the trigger only means the loop woke up.
	var before int64
	if r.window != nil {
		before = r.lastCommitMicros()
	}
	select {
	case r.trigger <- r.clock():
	case <-time.After(5 * time.Second):
		r.t.Fatal("the loop never took the idle tick")
	}
	if r.window != nil {
		r.await(func() bool { return r.lastCommitMicros() > before })
	}
}

func (s Elapse) apply(r *run) {
	r.mu.Lock()
	r.now = r.now.Add(s.By)
	r.mu.Unlock()
}

func (v Revoke) apply(r *run) { r.src.revoke(v.Partition) }

func (a Assign) apply(r *run) { r.src.assign(a.Partition) }

func (Restart) apply(r *run) {
	r.stop()
	r.coord.mu.Lock()
	r.coord.generation++
	r.coord.mu.Unlock()
	r.start()
}
