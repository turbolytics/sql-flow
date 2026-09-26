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
	"sync/atomic"
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

// Revoke moves a partition to another worker, as a rebalance does. Its
// future rows are that worker's; the engine stops holding the window for
// it.
type Revoke struct{ Partition int32 }

// Lose is the session failing: nobody knows who holds the partition, and it
// may come back here with a backlog. The engine holds the window for it.
type Lose struct{ Partition int32 }

// Assign gives a partition to this worker, or gives a lost one back.
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
	// subs is who wants to hear about the partitions: the engine's
	// watermark tracker, as the Kafka source's relay tells it.
	subs []partitionSubscriber
}

type partitionSubscriber struct {
	assigned, released, lost func(map[string][]int32)
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

func (s *source) Start() error                  { return nil }
func (s *source) Stream() <-chan []core.Message { return s.ch }
func (s *source) Commit() error                 { return nil }

// OnPartitions implements core.PartitionOwner the way the Kafka relay does:
// the subscriber is told what is held now, then every change.
func (s *source) OnPartitions(a, r, l func(map[string][]int32)) {
	s.mu.Lock()
	s.subs = append(s.subs, partitionSubscriber{a, r, l})
	current := make([]int32, 0, len(s.owned))
	for p := range s.owned {
		current = append(current, p)
	}
	s.mu.Unlock()
	if len(current) > 0 && a != nil {
		a(map[string][]int32{topic: current})
	}
}

func (s *source) tell(which func(partitionSubscriber) func(map[string][]int32), p int32) {
	s.mu.Lock()
	subs := append([]partitionSubscriber(nil), s.subs...)
	s.mu.Unlock()
	for _, sub := range subs {
		if f := which(sub); f != nil {
			f(map[string][]int32{topic: {p}})
		}
	}
}

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
	delete(s.owned, p)
	s.mu.Unlock()
	s.tell(func(x partitionSubscriber) func(map[string][]int32) { return x.released }, p)
}

func (s *source) lose(p int32) {
	s.mu.Lock()
	delete(s.owned, p)
	s.mu.Unlock()
	s.tell(func(x partitionSubscriber) func(map[string][]int32) { return x.lost }, p)
}

func (s *source) assign(p int32) {
	s.mu.Lock()
	s.owned[p] = true
	s.assignedAt = s.clock()
	s.mu.Unlock()
	s.tell(func(x partitionSubscriber) func(map[string][]int32) { return x.assigned }, p)
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

	// loopErr is what ConsumeLoop returned, if it has returned. A loop that
	// died makes every await after it time out, and a timeout says nothing
	// about why; await reports this instead when it is set.
	loopErr atomic.Pointer[error]

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
		_, err := r.tb.ConsumeLoop(ctx, 0)
		if err != nil {
			r.loopErr.Store(&err)
		}
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
		// receives nothing. The engine asserts the window's watermark on
		// every commit, from what it has seen per partition, and that is what
		// the manager reads. A restart rebuilds the tracker from the table
		// and the row, as run does once the tables exist.
		h = r.window.handler
		w := r.window.newWatermarks(r.t, r.clock)
		opts = append(opts,
			core.WithProgressStore(core.NewProgressStore(r.window.db.pipeline)),
			core.WithWindows(w, core.NewWatermarkStore(r.window.db.pipeline)),
			// Every commit that moves a watermark writes it. Production paces
			// the write at a second, which a script's steps happen to clear
			// today; pinning it here keeps a scenario with finer steps from
			// silently asserting nothing.
			core.WithWatermarkWriteInterval(0),
			// As run wires a windowing pipeline: a record whose event time
			// the engine cannot place is refused before the handler.
			core.WithEventTimePlacement(true))
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
			Value: []byte(fmt.Sprintf("%d,%d", id, at.UnixMicro())),
			// And on the message itself, where the engine's placement rule
			// reads it: the payload is what the handler buckets on, this is
			// what the engine refuses on, and they are the same clock.
			EventAtNanos: at.UnixNano(),
			Topic:        topic,
			Partition:    p,
			Offset:       from + int64(i),
		})
	}
	// What the loop will accept. A windowing run places event time, and a
	// record the engine cannot place never reaches the handler or the table,
	// so waiting for it would wait forever. The prediction uses the engine's
	// own rule, against the same clock it reads, so it cannot disagree with
	// what the engine does.
	accepted := len(batch)
	if r.window != nil {
		nowNanos := time.Now().UnixNano()
		accepted = 0
		for _, m := range batch {
			if core.CanPlace(m.EventAtNanos, nowNanos) {
				accepted++
			}
		}
	}
	var want int
	if r.window == nil {
		total, _ := r.sink.counts()
		want = total + accepted
	} else {
		want = int(r.windowRows()) + accepted
	}

	select {
	case r.src.ch <- batch:
	case <-time.After(5 * time.Second):
		r.t.Fatal("the loop never took the batch")
	}
	if r.window == nil {
		r.await(fmt.Sprintf("the sink to hold %d rows", want),
			func() bool { total, _ := r.sink.counts(); return total >= want })
		return
	}
	// The window table plus what the manager has already published: a close
	// between the write and this read moves rows from one to the other.
	r.await(fmt.Sprintf("the handler to write %d rows of partition %d (table + published >= %d)",
		len(batch), p, want), func() bool {
		published, _ := r.window.sink.counts()
		return int(r.windowRows()+published) >= want
	})
}

// await blocks until cond holds, and says what it was waiting for when it
// does not. The message matters: two steps wait here for different things,
// and one message for both turns a flake into a guess.
func (r *run) await(what string, cond func() bool) {
	r.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if err := r.loopErr.Load(); err != nil {
			r.t.Fatalf("waiting for %s: the consume loop stopped: %v", what, *err)
		}
		if time.Now().After(deadline) {
			r.t.Fatalf("waited 5s for %s and it never happened", what)
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
	// waits for it: handing off the trigger only means the loop woke up. The
	// tick writes no progress row -- it asserts a watermark only when one
	// moved -- so the wait is on the engine's count of completed commits.
	//
	// Not on the commit's timestamp, which is what this waited on first and
	// which hangs: the loop stamps a commit with the clock as it stands when
	// it runs, the script owns that clock, and a batch's commit landing after
	// the script has moved it carries the same instant the tick's will. Under
	// -race that happened reliably, and a wait for a strictly later instant
	// never returned, because the clock only moves when the script does and
	// the script was waiting.
	before := r.tb.Commits()
	select {
	case r.trigger <- r.clock():
	case <-time.After(5 * time.Second):
		r.t.Fatal("the loop never took the idle tick")
	}
	r.await(fmt.Sprintf("the loop's commit number %d", before+1),
		func() bool { return r.tb.Commits() > before })
}

func (s Elapse) apply(r *run) {
	r.mu.Lock()
	r.now = r.now.Add(s.By)
	r.mu.Unlock()
}

func (v Revoke) apply(r *run) { r.src.revoke(v.Partition) }

func (l Lose) apply(r *run) { r.src.lose(l.Partition) }

func (a Assign) apply(r *run) { r.src.assign(a.Partition) }

func (Restart) apply(r *run) {
	r.stop()
	r.coord.mu.Lock()
	r.coord.generation++
	r.coord.mu.Unlock()
	r.start()
}
