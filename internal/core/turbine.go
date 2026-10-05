package core

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/turbolytics/sql-flow/internal/errs"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
)

// Message is one record from a source, with whatever provenance the source
// knows about it. Only Kafka populates the metadata fields.
type Message struct {
	Value []byte
	// EventAtNanos is when the event happened, as the source knows it, in
	// Unix nanoseconds: a Kafka record's timestamp, or the moment a webhook
	// or websocket message arrived. Zero for a source with no event time,
	// which is why the pipeline's lag fields are absent for one.
	//
	// Nanoseconds rather than a time.Time because this field sits on every
	// message on the hot path while one reading per batch is ever taken
	// from it. A time.Time costs 24 bytes against 8, which measured at +24%
	// B/op and +16% on the consume loop's own overhead.
	//
	// The monotonic reading goes with it. A Kafka record had none to lose,
	// because its time comes from another machine's clock and the
	// subtraction already fell back to wall clock. The arrival sources do
	// stamp and read on one clock, so for those a wall-clock step between
	// the two now skews that batch's reading. What is at stake there is a
	// sub-second number, against 16 bytes on every message, and the step
	// clears on the next batch rather than persisting.
	EventAtNanos int64
	Topic        string
	Partition    int32
	Offset       int64
	// LeaderEpoch is the Kafka leader epoch the record was read under. It is
	// carried through so a commit can name it, which lets the broker detect
	// log truncation. Only meaningful when HasMetadata is true; a source with
	// no positions leaves it zero along with the rest.
	LeaderEpoch int32
	// HighWatermark is the partition's high watermark at fetch time, so lag
	// can be computed against the position last processed. Zero for sources
	// without one.
	HighWatermark int64
}

// The bases an event time can have. It travels with every lag reading,
// because a lag from a broker's timestamp and a lag from arrival measure
// different spans and comparing them is meaningless.
//
// This is an open vocabulary. A source whose protocol carries no event
// time, and whose broker can hold a message before delivering it, is
// honestly described by neither of these and gets its own name rather than
// borrowing arrival.
const (
	// EventBasisKafkaCreateTime is a Kafka record's timestamp as the
	// producer set it. This is Kafka's default, message.timestamp.type =
	// CreateTime, so the reading is only as good as the producer's clock.
	EventBasisKafkaCreateTime = "kafka_create_time"
	// EventBasisKafkaLogAppendTime is a Kafka record's timestamp as the
	// broker set it on append, for a topic configured with
	// message.timestamp.type = LogAppendTime. One clock stamps every
	// record, so a lag from this basis measures the stream rather than the
	// fleet's clocks.
	EventBasisKafkaLogAppendTime = "kafka_log_append_time"
	EventBasisArrival            = "arrival"
)

// eventTimeFloorNanos is the oldest event time a lag reading treats as real.
//
// Nothing this engine reads is genuinely from before 2020. What does arrive
// from the 1970s is a broken clock. Kafka encodes "no timestamp" as -1, and
// a device without a real-time clock boots at the epoch and stamps records
// at 1970 plus its uptime: a millisecond past it, or a year, but never
// exactly at it. A guard against zero alone let every one of those through,
// and each set the run's worst lag to 56 years, which never comes down.
// Fifty years of uptime still lands below this floor.
//
// The cost is a genuine replay of a pre-2020 archive, which reports no lag
// rather than a wrong one. Absent is not zero, and a missing reading is
// better than 56 years on a fleet dashboard.
var eventTimeFloorNanos = EventTimeFloor.UnixNano()

// EventTimeFloor is the oldest event time this engine treats as real; see
// eventTimeFloorNanos for why 2020. Exported because a window applies the
// same rule to the buckets it holds: an event time the lag reading refuses
// is not one a watermark should rest on either.
var EventTimeFloor = time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)

// EventTimeCeiling is how far ahead of this host's clock an event time may be
// and still be believed. Beyond it the producer's clock is wrong; within it
// the two clocks merely disagree, which is the normal condition of two
// correctly run hosts.
//
// It exists because zero tolerance refused real data. The Bluesky demo on
// v2026.09.27 dropped 82% of posts -- 9,739 of 11,873 in ninety seconds --
// because Jetstream's timestamps arrived a few milliseconds ahead of the
// consumer's clock. A stamp is written upstream and read downstream, so it is
// ahead whenever the producer's clock leads the consumer's by more than the
// transit between them, and a few milliseconds of NTP disagreement does that
// for any producer in the same region. Nothing was wrong with the data, the
// clocks, or the pipeline.
//
// A minute is the number this repository already used for the same judgement,
// in the Render template's SQL: `ts <= now() + INTERVAL '60 seconds'`. It is
// far wider than clock disagreement between synchronised hosts and far
// narrower than the failure the floor and this ceiling exist for, a device
// that believes it is 2099 (#358). A window's own grace is unrelated: that
// bounds how far out of order a stream arrives, in the stream's clock, and
// says nothing about whether to believe the stream's clock at all.
const EventTimeCeiling = time.Minute

// EventTimeMissing is the EventAtNanos of a record from a source that assigns
// event time but found none usable on this record: the configured field is
// absent, the wrong type, or unparseable. It is distinct from zero, which is
// a source that assigns nothing at all and whose records are placeable, and
// it sits below the floor, so a windowing pipeline refuses it. Kafka encodes
// "no timestamp" the same way.
const EventTimeMissing int64 = -1

// EventTimeSource is a source that stamps Message.EventAtNanos. A source that
// does not implement it reports no lag, rather than a lag of zero.
type EventTimeSource interface {
	EventTimeBasis() string
}

// Mark is the position of the last message the pipeline has finished with in
// one partition: written to the handler, or dropped by an error policy.
type Mark struct {
	Offset      int64
	LeaderEpoch int32
}

// MarkCommitter is implemented by sources that can commit an explicit
// position. The pipeline prefers it to Commit, because a source that reads
// ahead of the pipeline -- the Kafka source polls into a buffer -- has fetched
// messages the pipeline has not processed, and committing "everything fetched"
// commits those too.
type MarkCommitter interface {
	CommitMarks(marks *Marks) error
}

// Settler is a source that answers its sender once the pipeline is
// finished with what it delivered.
type Settler interface {
	// Settle resolves the oldest n messages the source delivered: err nil
	// when their batch flushed and committed, non-nil when it did not.
	Settle(n int, err error)
}

// ErrStoppedBeforeFlush settles a message the loop took and then stopped
// without flushing.
var ErrStoppedBeforeFlush = errors.New("the pipeline stopped before this message was flushed")

// MetadataCommitter is a MarkCommitter that commits a string with each
// position and reports what the group last committed for a partition when
// this consumer is assigned it.
type MetadataCommitter interface {
	CommitMarksWithMetadata(marks *Marks, metadata string) error
	OnCommittedMetadata(fn func(topic string, partition int32, metadata string))
}

// HasMetadata reports whether the source supplied provenance for this message.
// Topic is the discriminator: a Kafka record always has one, and a source that
// has none leaves it empty.
func (m Message) HasMetadata() bool { return m.Topic != "" }

type Source interface {
	Start() error
	Stream() <-chan []Message
	Commit() error
	Close() error
}

// Deliverer is implemented by a source that knows when it can deliver at
// all: a websocket that is connected, an MQTT client with a session.
//
// For a source that reports no partitions, this is its partition lifecycle
// (see watermarks.go): one partition, held while the source can deliver
// and lost while it cannot. A reconnect longer than idle_close_seconds is
// then not a quiet stream: the window holds through it, and idleness is
// measured again from the resumption. A source that reports its partitions
// (PartitionOwner) is tracked through those instead; a source that
// implements neither is taken to be delivering whenever it is running.
//
// The trade-off: a source that can never deliver holds every window open
// for as long as that lasts, and no bucket closes on idleness meanwhile.
// The engine logs when a source stops delivering and when it resumes, so a
// hold that never ends is visible.
type Deliverer interface {
	Delivering() (deliveringFor time.Duration, ok bool)
}

// MetadataWriter is implemented by handlers that can use a message's source
// metadata. Handlers that only need the payload implement Handler alone and
// the consume loop hands them the value.
type MetadataWriter interface {
	WriteMessage(msg Message) error
}

// Sink is where a batch leaves the pipeline.
//
// Both write paths take a context so a sink that retries can be interrupted.
// Without it, a retry ladder outlives the SIGTERM that asked the pipeline to
// stop, and the graceful drain waits out the full retry deadline before it can
// finish. See #110.
type Sink interface {
	WriteTable(ctx context.Context, batch arrow.Table) error
	Flush(ctx context.Context) error
}

// BufferedRowReporter is implemented by a sink that can say how many rows it
// is holding.
//
// A sink keeps every row a failed flush could not deliver, and nothing bounds
// that buffer. Today a failed flush stops the pipeline, so the buffer dies
// with the process. That bound is a property of the call pattern rather than
// of the design, and it disappears the day a flush failure stops being fatal.
// An operator needs to see the depth before that day, not after.
//
// Optional, like MarkCommitter: a sink that buffers nothing reports nothing.
type BufferedRowReporter interface {
	BufferedRows() int
}

// KeyedSink is implemented by a sink that identifies a row by declared key
// columns, so delivering the same batch twice leaves the destination holding
// it once. Key returns those columns, or nothing for a sink that appends.
//
// Optional, like BufferedRowReporter. The conformance harness reads it to
// decide whether sink.flush.idempotent_on_key applies.
type KeyedSink interface {
	Key() []string
}

type Handler interface {
	Init(ctx context.Context) error
	Write(msg []byte) error
	Invoke(ctx context.Context) (arrow.Table, error)

	// RowsRead reports the rows the last Invoke ingested into the batch table.
	//
	// It is the denominator of the enrichment ratio, so it has to count rows
	// the SQL actually ran over. An Invoke that failed schema inference
	// produced no table at all and reports zero, however many messages were
	// written to it -- counting accepted writes instead would overstate the
	// denominator on exactly the path where rows were lost.
	RowsRead() int64
}

type Stats struct {
	numMessagesConsumed      atomic.Int64
	StartTime                time.Time
	NumErrors                int
	totalThroughputPerSecond atomic.Uint64 // stored as float64 bits
}

func (s *Stats) SetThroughput(throughput float64) {
	s.totalThroughputPerSecond.Store(math.Float64bits(throughput))
}

func (s *Stats) GetThroughput() float64 {
	return math.Float64frombits(s.totalThroughputPerSecond.Load())
}

func (s *Stats) SetNumMessagesConsumed(num int64) {
	s.numMessagesConsumed.Store(num)
}

func (s *Stats) MessagesConsumed() int64 {
	return s.numMessagesConsumed.Load()
}

func (s *Stats) AddMessagesConsumed(n int64) {
	s.numMessagesConsumed.Add(n)
}

type ErrorPolicy int

const (
	PolicyRaise ErrorPolicy = iota
	PolicyIgnore
	PolicyDLQ
)

// ParseErrorPolicy resolves the configured policy name. Matching the Python
// engine, the name is case-insensitive and an empty value means RAISE.
func ParseErrorPolicy(s string) (ErrorPolicy, error) {
	switch strings.ToUpper(strings.TrimSpace(s)) {
	case "", "RAISE":
		return PolicyRaise, nil
	case "IGNORE":
		return PolicyIgnore, nil
	case "DLQ":
		return PolicyDLQ, nil
	default:
		return PolicyRaise, fmt.Errorf("unsupported error policy: %q", s)
	}
}

type PipelineErrorPolicies struct {
	Policy  ErrorPolicy
	DLQSink Sink
}

// Error phases, as reported on DLQ records and as the phase label on
// error_count. Naming every phase is what lets a dashboard say which stage of
// the pipeline is failing, not just that something is.
const (
	phaseHandlerWrite  = "handler.write"
	phaseHandlerInvoke = "handler.invoke"
	phaseHandlerInit   = "handler.init"
	phaseSinkWrite     = "sink.write"
	phaseSinkFlush     = "sink.flush"
	phaseStateCommit   = "state.commit"
)

// phases is every phase above, in the order a batch runs them, so the
// attribute cache is built once at startup rather than grown on the batch
// path.
//
// A phase named in the block above and missing here records a point with no
// phase label, which merges into a series that is true of nothing rather than
// failing. TestObservabilityMetrics_EveryPhaseHasCachedAttributes is what
// catches that.
var phases = []string{
	phaseHandlerWrite,
	phaseHandlerInvoke,
	phaseSinkWrite,
	phaseSinkFlush,
	phaseStateCommit,
	phaseHandlerInit,
}

// offsetSaver writes positions into the pipeline's state database. It is an
// interface so the ordering tests need no DuckDB; OffsetStore implements it.
type offsetSaver interface {
	Save(ctx context.Context, marks *Marks) error
}

// stateTx is the transaction boundary on the state database. ADBC connections
// with autocommit disabled satisfy it.
type stateTx interface {
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
}

type Turbine struct {
	source        Source
	sink          Sink
	handler       Handler
	batchSize     int
	flushInterval time.Duration

	// offsets and stateTx are set together, and only when the pipeline has a
	// state database. Both nil means the historical behaviour: no transaction,
	// and the source's own commit is the only durable record of progress.
	offsets offsetSaver
	stateTx stateTx

	// progress is the liveness record, see progress.go. Optional: a Turbine
	// built without it records nothing and Progress() reports zeros.
	progress ProgressSaver
	// snapshot is the in-memory copy of what progress last recorded, read by
	// /stats and /healthz without touching the database. Guarded by lock.
	snapshot Progress
	// lastArrival is when the newest batch reached the sink, for the
	// progress row's liveness: seeded when the turbine is built and when the
	// loop starts, stamped at the end of every batch. Nothing decides a
	// window on it. Guarded by lock.
	lastArrival time.Time
	// commits counts commits that completed, with or without a state
	// transaction, for a caller that waits on ticks. It counts commits and
	// not instants on purpose: a simulator drives the clock, so two commits
	// can carry the same stamp and a reader waiting for a later one would
	// wait forever. Guarded by lock.
	commits int64
	// progressWrittenAt is when the progress table was last written, and
	// progressEvery is how often it may be. Guarded by lock.
	progressWrittenAt time.Time
	progressEvery     time.Duration
	// windows asserts each window's watermark, written through
	// watermarkSaver; nil for a pipeline with no window. See WithWindows and
	// watermarks.go.
	windows        *Watermarks
	watermarkSaver WatermarkSaver
	// watermarkWrittenAt is when a watermark was last written, and
	// watermarkEvery is how often one may be. Guarded by lock; see
	// assertWatermarks for why the write is paced at all.
	watermarkWrittenAt time.Time
	watermarkEvery     time.Duration
	// observed is what this batch's records said about their partitions,
	// accumulated by the message loop and handed to windows once per batch;
	// see notePlaced. Reused across batches, and the consume loop is its only
	// party, so it takes no lock.
	observed []Observation
	// pendingLate is the buckets this batch's late-but-allowed records landed
	// in, handed to the windows' signals after the commit that made the rows
	// visible. Consume loop only; cleared on a rollback, because the replay
	// classifies the same records again.
	pendingLate []LateBucket
	// lateRefusedLogged is whether a refused late record has been logged
	// this run, so a device stuck in the past writes one line.
	lateRefusedLogged bool
	// lateAttrs caches the attribute set per window and outcome, because
	// WithAttributes allocates and refusals can come per record.
	lateAttrs map[string][]metric.AddOption
	// partitionsRelayed is whether the source reports its partitions to
	// windows itself. Otherwise the loop asks the source whether it can
	// deliver before each commit, and notDelivering is the last answer, for
	// the two log lines.
	partitionsRelayed bool
	notDelivering     bool
	// placesEventTime is whether a message whose event time this engine
	// cannot place is refused; see WithEventTimePlacement. Set for a
	// pipeline that windows, because a window is what an unplaceable event
	// time damages.
	placesEventTime bool
	// unplaceableLogged is whether the last batch refused one, so the
	// condition is logged on its transitions rather than per message.
	unplaceableLogged bool
	// clock is where every instant a decision rests on comes from; nil means
	// time.Now. See WithClock.
	clock func() time.Time
	// flushTrigger replaces the flush ticker when set; see WithFlushTrigger.
	flushTrigger <-chan time.Time

	// stateStats reads a snapshot of durable state for the gauges. It reads a
	// connection dedicated to reading, never the one batches are written on,
	// so a scrape cannot stall the pipeline. Nil when there is no state
	// database.
	stateStats func(context.Context) (*StateStats, error)

	// windowOffsets tracks the lowest offset feeding each retained bucket, so
	// the source commit stays below every row a window still holds;
	// windowOffsetStore persists it in the state transaction. Nil without a
	// window over a source with positions.
	windowOffsets     *WindowOffsets
	windowOffsetStore *WindowOffsetStore
	// dropper deletes a revoked partition's rows from every partition-owned
	// window; drops carries requests from the source's rebalance callback to
	// the consume loop, which owns the connection. loopDone is closed when
	// ConsumeLoop returns, so a callback never waits on a loop that is gone.
	dropper      PartitionDropper
	drops        chan dropRequest
	loopDone     chan struct{}
	loopDoneOnce sync.Once
	// revoked holds partitions dropped since their revocation; records the
	// consumer prefetched from them are skipped. Copy-on-write behind an
	// atomic pointer, so the per-record check takes no lock: after a
	// scale-out the set stays non-empty for the life of the process.
	// revokedMu serializes the writers only.
	revokedMu sync.Mutex
	revoked   atomic.Pointer[map[partitionKey]bool]

	// floorInbox receives replay floors from the source's goroutine;
	// floorsWaiting says it holds any, so the loop checks with one atomic
	// load. replayFloors is the loop's own copy, closed nanos per spec index.
	floorMu       sync.Mutex
	floorInbox    map[partitionKey][]int64
	floorsWaiting atomic.Bool
	replayFloors  map[partitionKey][]int64
	// lowMoved is set when a bucket's offset record expired since the last
	// source commit: the position to commit may have moved with no batch
	// to carry it, and the next idle tick commits it.
	lowMoved bool

	// unsettled is how many messages the loop took from the stream since the
	// last settle; a Settler source hears it after the batch commits.
	unsettled int

	// marks is the last position finished with, per topic and partition; what
	// commitSource hands a MarkCommitter.
	marks *Marks
	// committed is what the last successful commit made durable: the state
	// transaction's offsets, or for a pipeline with no state database the
	// positions the source accepted. A batch that fails resets marks to this,
	// because marks advance when a message reaches the handler, before its
	// batch is flushed, and anything that commits later would otherwise make
	// those positions durable for rows the sink never took.
	committed   *Marks
	lock        *sync.Mutex
	running     bool
	stats       *Stats
	errorPolicy PipelineErrorPolicies

	// drain bounds the final batch after a cancel. Shared with the managers
	// and run's state syncs, so one deadline covers the whole shutdown.
	drain *DrainBudget

	// lastErrorUnixNano and errorCount feed Progress. Atomics rather than
	// fields under lock, because recordError runs on paths that already
	// hold it.
	lastErrorUnixNano atomic.Int64
	errorCount        atomic.Int64
	// eventTimeSource is the source's event-time reporter, nil for a source
	// that stamps none. The basis name is read from it at report time
	// rather than cached, because a Kafka source learns whether its topic
	// stamps CreateTime or LogAppendTime only once it has seen a record.
	eventTimeSource EventTimeSource
	// eventLagMax is the worst lag seen, in seconds. It never resets, so a
	// slow producer clock raises it for the life of the process: under
	// kafka_create_time a device three hours behind sets three hours the
	// first time its records fill a batch alone, which at low volume is
	// routine. The basis travels with the number so a reader can discount
	// it; the alternative, a maximum that decays, would hide the outage it
	// exists to catch. It never resets,
	// because the reporter and GET /turbostats/v1 both read the bundle it
	// ends up in. Written and read on the consume loop only.
	eventLagMax float64

	// lastErrorCode is the code of the last error recorded, for the bundle.
	// An atomic.Value rather than the lock: recordError runs on the consume
	// loop and the reporter reads from its own goroutine.
	lastErrorCode atomic.Value

	// lagPending holds the newest lag seen per topic and partition since the
	// last recordLag. A gauge is a last value, so recording it on every
	// message spends a real instrument call to say what the final message of
	// the fetch says anyway: 18% of throughput on the 10M benchmark once the
	// default provider stopped being a noop. Same goroutine rule as the cache.
	lagPending map[lagKey]int64

	// Built once at startup, because WithAttributes allocates. Typed as
	// AddOption because only counters carry result; the histograms keep
	// measuring every attempt regardless of outcome.
	resultOKAttrs    []metric.AddOption
	resultErrorAttrs []metric.AddOption

	// One cached option slice per phase, for the same reason. Every phase is
	// known at startup, so this is a fixed six entries and never grows.
	phaseAttrs map[string][]metric.RecordOption

	logger  *zap.Logger
	metrics *Metrics
}

// lagKey identifies one topic and partition for the lag attribute cache.
type lagKey struct {
	topic     string
	partition int32
}

func WithTurbineLogger(l *zap.Logger) TurbineOption {
	return func(t *Turbine) {
		t.logger = l
	}
}

// WithStateStore makes each batch transactional: the handler's writes to the
// state database and the offsets that produced them commit together, so a
// crash can never leave one without the other.
func WithStateStore(offsets offsetSaver, tx stateTx) TurbineOption {
	return func(t *Turbine) {
		t.offsets = offsets
		t.stateTx = tx
	}
}

// WithProgressStore records liveness into a store and into an in-memory
// snapshot on every commit and every idle tick.
func WithProgressStore(s ProgressSaver) TurbineOption {
	return func(t *Turbine) { t.progress = s }
}

// progressWrite is what a commit does with the progress table.
type progressWrite int

const (
	// progressOnInterval writes when the interval has come due.
	progressOnInterval progressWrite = iota
	// progressForced writes whether or not it has: the drain.
	progressForced
	// progressSkipped keeps the snapshot and leaves the table alone: an
	// idle tick on a pipeline where nothing reads it.
	progressSkipped
)

// WithWindows makes the engine assert each window's watermark on every
// commit, through the saver, inside the state transaction where there is
// one; see watermarks.go. The tracker learns the source's partitions from
// the source itself: a PartitionOwner reports them, a Deliverer is one
// partition held while it delivers, anything else is one partition always
// held.
//
// A pipeline with no window leaves this unset, and its idle ticks then do
// no accounting at all: no statement, no WAL append, no fsync, which on a
// box that runs its state from an SD card is the difference between a quiet
// pipeline and a busy one. A windowing pipeline's idle tick writes only
// when a watermark moved, which on a quiet stream is once, when the last
// partition goes idle.
type dropRequest struct {
	parts map[string][]int32
	done  chan error
}

// offsetDeleter is an offset store that can forget partitions.
type offsetDeleter interface {
	Delete(ctx context.Context, topic string, partitions []int32) error
}

// dropWait bounds how long a rebalance callback waits for the consume loop
// to drop a partition. Under the group's rebalance timeout, and past a sink
// flush's own bound, so the only thing it cuts short is a loop that will
// never come: a revocation before the loop has started on a run that then
// fails would otherwise hold the client's close forever.
const dropWait = 45 * time.Second

// WithPartitionDropper makes a revocation drop the partition's rows rather
// than let them close.
func WithPartitionDropper(d PartitionDropper) TurbineOption {
	return func(t *Turbine) { t.dropper = d }
}

// WithWindowOffsets commits each partition's low watermark rather than its
// processed position. store is nil without a state path: the tracker still
// holds the commit back, and a restart replays from Kafka.
func WithWindowOffsets(o *WindowOffsets, store *WindowOffsetStore) TurbineOption {
	return func(t *Turbine) {
		t.windowOffsets = o
		t.windowOffsetStore = store
		// A partition this worker replays from the low watermark comes with
		// no watermark of its own; the commit's metadata carries the last
		// owner's.
		if mc, ok := t.source.(MetadataCommitter); ok {
			mc.OnCommittedMetadata(t.noteCommittedMetadata)
		}
	}
}

// noteCommittedMetadata runs on the source's goroutine when this consumer is
// assigned a partition. The loop picks the floor up before its next record.
func (t *Turbine) noteCommittedMetadata(topic string, partition int32, metadata string) {
	closed, ok := DecodeReplayFloor(metadata)
	if !ok || t.windows == nil {
		return
	}
	specs := t.windows.Specs()
	floor := make([]int64, len(specs))
	for i, spec := range specs {
		if c, ok := closed[spec.Name]; ok {
			floor[i] = c.UnixNano()
		}
	}
	t.floorMu.Lock()
	if t.floorInbox == nil {
		t.floorInbox = map[partitionKey][]int64{}
	}
	t.floorInbox[partitionKey{topic, partition}] = floor
	t.floorMu.Unlock()
	t.floorsWaiting.Store(true)
}

// takeFloors moves the floors the source reported into the loop's own map.
// One atomic load when there are none, which is every batch but the first
// after an assignment.
func (t *Turbine) takeFloors() {
	if !t.floorsWaiting.Load() {
		return
	}
	t.floorMu.Lock()
	inbox := t.floorInbox
	t.floorInbox = nil
	t.floorsWaiting.Store(false)
	t.floorMu.Unlock()
	if t.replayFloors == nil {
		t.replayFloors = map[partitionKey][]int64{}
	}
	for k, v := range inbox {
		t.replayFloors[k] = v
	}
}

// belowReplayFloor is true when every window would refuse the record under
// the previous owner's closed watermark: its bucket ended at or before
// closed minus lateness. The same all-windows rule as Classify.
func (t *Turbine) belowReplayFloor(m Message) bool {
	floor, ok := t.replayFloors[partitionKey{m.Topic, m.Partition}]
	if !ok || m.EventAtNanos <= 0 {
		return false
	}
	at := time.Unix(0, m.EventAtNanos).UTC()
	for i, spec := range t.windows.Specs() {
		if floor[i] == 0 || BucketEnd(at, spec.Size).UnixNano()+int64(spec.Lateness) > floor[i] {
			return false
		}
	}
	return true
}

func WithWindows(w *Watermarks, s WatermarkSaver) TurbineOption {
	return func(t *Turbine) {
		t.windows = w
		t.watermarkSaver = s
	}
}

// WithWatermarkWriteInterval bounds how often a window's watermark is
// written. Zero writes on every commit that moves one, which is what a test
// wants when it is checking what gets asserted rather than how often.
// Production leaves it at watermarkWriteInterval; see assertWatermarks.
func WithWatermarkWriteInterval(d time.Duration) TurbineOption {
	return func(t *Turbine) { t.watermarkEvery = d }
}

// WithEventTimePlacement refuses a message whose event time this engine
// cannot place: before EventTimeFloor, or more than EventTimeCeiling ahead of
// the engine's own clock.
//
// On for a pipeline that windows. A window rests on event time, and one
// record stamped in the future otherwise drags the watermark past real time
// and makes every correctly stamped row after it late, which refuses them:
// one device with a fast clock empties a fleet's stream (#358). A pipeline
// with no window has nothing for such a record to damage, so it keeps it.
//
// Refused here rather than in the window manager on purpose. The manager
// decides on event time against the watermark, in one domain, and
// window.close_lag_ignores_the_host_clock says its host's clock cannot move
// a window; putting the comparison here keeps that true. The engine already
// owns a clock -- it is what a partition's idleness is measured on -- and it
// is where the record and its event time are, which is where Flink puts the
// timestamp assigner too.
//
// The exposure this leaves is a host whose own clock is wrong: a gateway
// with no real-time clock boots near 1970 and would refuse every correctly
// stamped record. That is real on exactly the fleets #358 is about, and the
// answer for them is an explicit option rather than a heuristic here that
// guesses when the clock became trustworthy.
func WithEventTimePlacement(on bool) TurbineOption {
	return func(t *Turbine) { t.placesEventTime = on }
}

// WithFlushTrigger replaces the flush ticker. A pipeline given one commits on
// idleness only when the channel fires, which is how a caller owns the order
// of a sequence of events rather than sharing it with a ticker.
func WithFlushTrigger(c <-chan time.Time) TurbineOption {
	return func(t *Turbine) { t.flushTrigger = c }
}

// WithProgressWriteInterval bounds how often the progress table is written.
// Zero writes on every commit, which is what a test wants when it is
// checking what gets recorded rather than how often. Production leaves it at
// progressWriteInterval; see recordProgress for why it is not free.
func WithProgressWriteInterval(d time.Duration) TurbineOption {
	return func(t *Turbine) { t.progressEvery = d }
}

// Progress reports the last recorded liveness facts. Zero values mean
// nothing has been recorded yet.
func (t *Turbine) Progress() Progress {
	t.lock.Lock()
	p := t.snapshot
	t.lock.Unlock()

	if ns := t.lastErrorUnixNano.Load(); ns != 0 {
		p.LastError = time.Unix(0, ns).UTC()
	}
	p.Errors = t.errorCount.Load()
	return p
}

// commitCount is how many commits have completed; tests wait on it.
func (t *Turbine) commitCount() int64 {
	t.lock.Lock()
	defer t.lock.Unlock()
	return t.commits
}

// Commits is how many commits have completed, batches and idle ticks alike.
// A caller that drives the loop's flush trigger waits on this to know the
// commit it asked for has happened: it counts events rather than reporting
// an instant, so a driven clock cannot make two commits indistinguishable.
func (t *Turbine) Commits() int64 { return t.commitCount() }

// progressWriteInterval bounds how often sqlflow_progress is written. The
// snapshot behind /stats and /healthz is updated on every commit regardless;
// this is only the SQL-visible copy, whose one reader in the engine compares
// its two clocks against an idle bound measured in tens of seconds.
const progressWriteInterval = time.Second

// watermarkWriteInterval bounds how often a window's watermark is written.
// One UPDATE measures about 130 microseconds through ADBC
// (BenchmarkCommitStateWindowed), and on a busy pipeline event time advances
// on every batch, so writing per commit is a statement per batch -- the same
// cost the progress write was paced to avoid, for a value with the same
// reader. The window manager reads it on the kick that follows the write, so
// the pace below is the most a close can trail its commit, and a second of
// that on a busy stream costs nothing a reader can see.
const watermarkWriteInterval = time.Second

// watchSource tells the tracker what the source holds. A source that
// reports its partitions is subscribed once, here; the tracker then hears
// every assignment, revocation and loss from the source's own goroutine. A
// source that only knows whether it can deliver is asked before each
// commit, in noteDelivering.
func (t *Turbine) watchSource() {
	if t.windows == nil {
		return
	}
	switch s := t.source.(type) {
	case PartitionOwner:
		t.partitionsRelayed = true
		s.OnPartitions(t.assigned, t.released, t.lost)
	case Deliverer:
		_, ok := s.Delivering()
		t.windows.SetDelivering(ok)
		t.notDelivering = !ok
	default:
		t.windows.SetDelivering(true)
	}
}

// noteDelivering asks a partition-less source whether it can deliver, so
// the tracker holds its one partition as lost through a reconnect. The
// transitions are logged once each, so a source that never resumes shows
// up as a stop with no resumption after it.
func (t *Turbine) assigned(parts map[string][]int32) {
	t.updateRevoked(parts, false)
	t.windows.Assigned(parts)
}

// released and lost drop the partitions' rows before the tracker lets them
// go. Without a dropper they are the tracker's own calls.
func (t *Turbine) released(parts map[string][]int32) {
	t.requestDrop(parts)
	t.windows.Released(parts)
}

func (t *Turbine) lost(parts map[string][]int32) {
	t.requestDrop(parts)
	t.windows.Lost(parts)
}

// updateRevoked adds or removes partitions from the revoked set, copying it,
// so a reader holding the old map never sees it change.
func (t *Turbine) updateRevoked(parts map[string][]int32, revoked bool) {
	if t.dropper == nil {
		return
	}
	t.revokedMu.Lock()
	defer t.revokedMu.Unlock()
	next := map[partitionKey]bool{}
	if cur := t.revoked.Load(); cur != nil {
		for k := range *cur {
			next[k] = true
		}
	}
	for topic, ps := range parts {
		for _, p := range ps {
			if revoked {
				next[partitionKey{topic, p}] = true
			} else {
				delete(next, partitionKey{topic, p})
			}
		}
	}
	t.revoked.Store(&next)
}

// requestDrop runs on the source's rebalance goroutine. It blocks until the
// consume loop has dropped the partitions, which holds the rebalance until
// no process but the new owner can write their rows.
func (t *Turbine) requestDrop(parts map[string][]int32) {
	// A plain windowed Kafka pipeline has no dropper but still commits the
	// low watermark, so it must forget a revoked partition's marks; only a
	// pipeline that neither owns partitions nor tracks window offsets has
	// nothing to do here.
	if t.dropper == nil && t.windowOffsets == nil {
		return
	}
	req := dropRequest{parts: parts, done: make(chan error, 1)}
	timeout := time.NewTimer(dropWait)
	defer timeout.Stop()
	select {
	case t.drops <- req:
	case <-t.loopDone:
		return
	case <-timeout.C:
		t.logger.Error("the consume loop did not take a partition drop; the revoked partitions' rows are still in the window",
			zap.Any("partitions", parts), zap.Duration("waited", dropWait))
		return
	}
	select {
	case err := <-req.done:
		if err != nil {
			t.logger.Error("dropping revoked partitions failed; the pipeline is stopping", zap.Error(err))
		}
	case <-t.loopDone:
	}
}

// dropPartitions handles a revoked partition in two parts. For every windowed
// Kafka pipeline it forgets the partition's marks, committed position, offset
// records and replay floor, so the pipeline stops committing -- at the low
// watermark, which is behind the processed position -- a partition the group
// now gives to another member; without this a plain windowed pipeline rewinds
// that member's offset and it double counts. Only a partition_owned pipeline
// (dropper set) also deletes the window rows and stored offsets and commits
// the deletion, under every owned window's pass lock so no pass publishes
// those rows meanwhile; a plain pipeline leaves its rows for the manager to
// publish and relies on the keyed upsert, as it did before this feature.
func (t *Turbine) dropPartitions(ctx context.Context, parts map[string][]int32) error {
	unlock := t.lockOwnedPasses()
	defer unlock()

	t.lock.Lock()
	defer t.lock.Unlock()
	// The drop runs between batches, so the connection's open transaction,
	// if any, holds only an idle tick's progress write, begun before this
	// lock was taken. Its snapshot can predate a window pass that has since
	// deleted closed buckets' rows on the manager's connection and committed;
	// DuckDB then refuses the drop's delete of those rows as a conflict on
	// tuple deletion, and the pipeline stopped over it (#437). Committed
	// here, so the drop begins a transaction whose snapshot is after every
	// pass this lock excludes. The progress write is rewritten every commit,
	// so committing it early loses nothing.
	if t.dropper != nil && t.stateTx != nil {
		if err := t.stateTx.Commit(ctx); err != nil {
			return errs.Wrap(errs.CodeStateCommitFailed, err, "ending the transaction before a partition drop")
		}
	}
	fail := func(err error) error {
		if t.stateTx != nil {
			if rbErr := t.stateTx.Rollback(context.WithoutCancel(ctx)); rbErr != nil {
				t.logger.Error("rollback after a failed partition drop", zap.Error(rbErr))
			}
		}
		return err
	}
	for topic, ps := range parts {
		if t.dropper != nil {
			if err := t.dropper.DropPartitions(ctx, topic, ps); err != nil {
				return fail(err)
			}
			if del, ok := t.offsets.(offsetDeleter); ok {
				if err := del.Delete(ctx, topic, ps); err != nil {
					return fail(err)
				}
			}
		}
		if t.windowOffsets != nil {
			t.windowOffsets.Drop(topic, ps)
		}
		t.marks.Forget(topic, ps)
		t.committed.Forget(topic, ps)
		for _, p := range ps {
			delete(t.replayFloors, partitionKey{topic, p})
		}
	}
	// Persist the deletion and commit it only when rows were deleted. A plain
	// pipeline deleted nothing here; its forgotten offset records ride the
	// next ordinary state commit, and forgetting marks is in memory.
	if t.dropper == nil {
		return nil
	}
	if t.windowOffsetStore != nil {
		if err := t.windowOffsetStore.Save(ctx, t.windowOffsets.Pending()); err != nil {
			return fail(err)
		}
	}
	if t.stateTx != nil {
		if err := t.stateTx.Commit(ctx); err != nil {
			return fail(errs.Wrap(errs.CodeStateCommitFailed, err, "committing the partition drop"))
		}
	}
	if t.windowOffsets != nil {
		t.windowOffsets.Saved()
	}
	t.updateRevoked(parts, true)
	return nil
}

// dropAllOwned deletes every partition-owned row as the loop ends, before
// the source leaves its group. What this worker held unpublished is the next
// owner's to recount, and a final pass that published it here could land
// after the next owner's whole count.
func (t *Turbine) dropAllOwned(ctx context.Context) {
	if t.dropper == nil || t.windows == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), dropWait)
	defer cancel()
	unlock := t.lockOwnedPasses()
	defer unlock()
	t.lock.Lock()
	defer t.lock.Unlock()
	if err := t.dropper.DropAll(ctx); err != nil {
		t.logger.Error("dropping partition-owned rows on the way out", zap.Error(err))
		if t.stateTx != nil {
			_ = t.stateTx.Rollback(ctx)
		}
		return
	}
	if t.stateTx != nil {
		if err := t.stateTx.Commit(ctx); err != nil {
			t.logger.Error("committing the drop of partition-owned rows on the way out", zap.Error(err))
			_ = t.stateTx.Rollback(ctx)
		}
	}
}

// lockOwnedPasses takes every partition-owned window's pass lock, in spec
// order, and returns the unlock.
func (t *Turbine) lockOwnedPasses() func() {
	var owned []*WindowSignal
	for _, spec := range t.windows.Specs() {
		if spec.PartitionOwned {
			owned = append(owned, t.windows.Signal(spec.Name))
		}
	}
	for _, s := range owned {
		s.LockPass()
	}
	return func() {
		for _, s := range owned {
			s.UnlockPass()
		}
	}
}

func (t *Turbine) noteDelivering() {
	if t.windows == nil || t.partitionsRelayed {
		return
	}
	d, ok := t.source.(Deliverer)
	if !ok {
		return
	}
	_, delivering := d.Delivering()
	t.windows.SetDelivering(delivering)
	switch {
	case !delivering && !t.notDelivering:
		t.notDelivering = true
		t.logger.Warn("source is not delivering; its windows hold until it does")
	case delivering && t.notDelivering:
		t.notDelivering = false
		t.logger.Info("source is delivering again")
	}
}

// assertWatermarks writes every window's watermark that moved, on the
// pipeline's connection. Inside the state transaction it rides the batch:
// the rows and the promise about them commit together or not at all, which
// is what makes #374 unrepresentable rather than fixed. Without one the
// write autocommits after the handler's rows have, which is the late
// direction: a reader can see rows without the watermark that accounts for
// them, and holds, but never a watermark without its rows.
//
// The write is paced, at watermarkWriteInterval, and forced by the drain.
// Skipping one leaves the watermark older than the rows it describes, which
// is the safe direction in exactly the same way: a close that is late leaves
// rows in the table for the next poll, and only an early one splits a bucket
// and publishes it twice. Nothing is lost by the skip, because the value is
// recomputed from the tracker each time rather than accumulated, so the next
// write carries wherever the stream has got to by then. What it buys is one
// statement a second instead of one a batch.
//
// The tracker records the values only once the caller reports the commit, so
// a batch that rolls back -- and a write the pace skipped -- asserts again
// later.
func (t *Turbine) assertWatermarks(ctx context.Context, write progressWrite) (map[string]time.Time, error) {
	if t.windows == nil {
		return nil, nil
	}
	moved := t.windows.Next()
	if len(moved) == 0 {
		return moved, nil
	}
	// Both stamps carry monotonic readings, so a wall clock stepping back
	// cannot make this negative; the guard is for a stamp without one, which
	// a test can inject, and which would otherwise stop the write for as long
	// as the step was.
	now := t.now()
	t.lock.Lock()
	elapsed := now.Sub(t.watermarkWrittenAt)
	due := elapsed >= t.watermarkEvery || elapsed < 0
	t.lock.Unlock()
	if !due && write != progressForced {
		return nil, nil
	}
	// Held through the writes: they run on the pipeline's connection, which
	// the debug API also runs statements on, and DuckDB closes a pending
	// result the moment another statement runs on the same connection (#283).
	// The window managers are not a party -- they poll on connections of
	// their own -- and the state branch of commitState takes this lock after
	// this returns, so nothing here nests.
	t.lock.Lock()
	defer t.lock.Unlock()
	// Advanced on every attempt, success or not, so a store that keeps
	// failing is retried once an interval rather than on every commit.
	t.watermarkWrittenAt = now
	for name, at := range moved {
		if err := t.watermarkSaver.Save(ctx, name, at); err != nil {
			return nil, err
		}
	}
	return moved, nil
}

// recordProgress runs at the top of every commit, before the state guard.
//
// Two audiences, and they are not the same requirement. The snapshot is what
// /stats and /healthz read, so every pipeline needs it whether or not it has
// a state database, and it costs an assignment. The table is what SQL reads
// -- handler SQL, emit_sql, a sqlcommand sink and /debug -- and it costs a
// statement on the commit path. The engine itself reads nothing from it:
// a window's progress is the watermark the engine asserts, not a gap
// between this row's clocks.
//
// The table is written on every pipeline: a table that exists but silently
// stops being maintained is a worse trap than one that costs a little. It
// is written on the interval by batches, and forced by the drain; an idle
// tick keeps the snapshot and leaves the table alone, so a quiet pipeline
// writes nothing. A commit inside the interval of the last write skips it,
// and nothing is lost by the skip: every write carries the arrival as it
// stands, so the next write reports the same arrival at its true time.
//
// The interval used to be overridden by any commit carrying a newer arrival,
// which under load is every commit: a statement per batch, 8 percent of
// throughput at batch 5000 and a quarter at batch 500.
//
// A batch since the last commit moves the arrival clock; an idle tick moves
// the commit clock only.
//
// It returns the store's error when it attempted a write and the write
// failed, and nil otherwise. It does not record that error, because what a
// failure means depends on whether the write rode the state transaction, and
// only commitState knows.
func (t *Turbine) recordProgress(ctx context.Context, write progressWrite) error {
	if t.progress == nil {
		return nil
	}
	now := t.now()
	// Held through the write below, because the debug API runs statements on
	// this same connection and DuckDB closes a pending result the moment
	// another statement runs on it. The window managers are not a party: they
	// poll on connections of their own and never take this lock.
	t.lock.Lock()
	defer t.lock.Unlock()
	p := Progress{
		LastArrival: t.lastArrival,
		LastCommit:  now,
		Messages:    t.stats.MessagesConsumed(),
	}
	t.snapshot.LastArrival = p.LastArrival
	t.snapshot.LastCommit = p.LastCommit
	t.snapshot.Messages = p.Messages

	// Both stamps carry monotonic readings, so a wall clock stepping back
	// cannot make this negative. The guard stays for a stamp without one,
	// which a test can inject: comparing now.Sub(written) >= interval alone
	// would then be false for as long as the step was, and the table would
	// stop being written.
	elapsed := now.Sub(t.progressWrittenAt)
	due := elapsed >= t.progressEvery || elapsed < 0

	// The snapshot above is exact and free. The table is not.
	//
	// One UPDATE through ADBC measures about 150 microseconds on a quiet
	// machine (BenchmarkProgressStoreRecord), and a commit that carries it
	// costs the same again (BenchmarkCommitStateArrivalForced, every_commit
	// against on_the_interval: 156 against 0.4 microseconds in memory, 260
	// against 70 on a state path). A batch of 5000 at a million messages a
	// second commits two hundred times a second, so the statement alone is
	// about 3 percent there. The container benchmark lost 8 to 9 percent end
	// to end at that batch size and a quarter at batch 500: the rest is the
	// write transaction it leaves on the connection, which makes the next
	// batch's truncate and checkpoint in Init dearer. Paying it once a
	// second instead of once a commit is what buys that back.
	if write == progressSkipped || (!due && write != progressForced) {
		return nil
	}

	// The throttle advances on every attempt, success or not, so a store
	// that keeps failing is retried once an interval and not on every
	// commit. A failed write's arrival is not carried specially: the next
	// write reports it, and at shutdown the drain forces one.
	t.progressWrittenAt = now

	return t.progress.Record(ctx, p)
}

// WithStateStats supplies the snapshot function backing the state gauges. It
// must read a connection dedicated to reading; passing the pipeline's writer
// would let a scrape contend with batch processing.
func WithStateStats(fn func(context.Context) (*StateStats, error)) TurbineOption {
	return func(t *Turbine) {
		t.stateStats = fn
	}
}

// WithMetrics records pipeline instruments through the given provider.
func WithMetrics(m *Metrics) TurbineOption {
	return func(t *Turbine) {
		if m != nil {
			t.metrics = m
		}
	}
}

type TurbineOption func(turbine *Turbine)

func NewTurbine(
	source Source,
	handler Handler,
	sink Sink,
	batchSize int,
	flushInterval time.Duration,
	lock *sync.Mutex,
	policy PipelineErrorPolicies,
	opts ...TurbineOption,
) *Turbine {
	t := &Turbine{
		source:         source,
		marks:          NewMarks(),
		committed:      NewMarks(),
		sink:           sink,
		handler:        handler,
		batchSize:      batchSize,
		flushInterval:  flushInterval,
		progressEvery:  progressWriteInterval,
		watermarkEvery: watermarkWriteInterval,
		lock:           lock,
		running:        true,
		stats:          &Stats{},
		errorPolicy:    policy,
		drops:          make(chan dropRequest),
		loopDone:       make(chan struct{}),

		logger: zap.NewNop(),
	}

	// A source that stamps event times says what they mean. One that does
	// not leaves this empty, and the loop then measures no lag at all
	// rather than a lag of zero.
	if s, ok := source.(EventTimeSource); ok {
		t.eventTimeSource = s
	}

	// Built once rather than per batch: metric.WithAttributes allocates on
	// every call whatever the metrics config.
	t.resultOKAttrs = []metric.AddOption{
		metric.WithAttributes(attribute.String("result", resultOK)),
	}
	t.resultErrorAttrs = []metric.AddOption{
		metric.WithAttributes(attribute.String("result", resultError)),
	}

	t.phaseAttrs = make(map[string][]metric.RecordOption, len(phases))
	for _, phase := range phases {
		t.phaseAttrs[phase] = []metric.RecordOption{
			metric.WithAttributes(attribute.String("phase", phase)),
		}
	}

	// Instruments that record nothing until a provider is supplied, so the
	// pipeline never has to nil-check them.
	t.metrics, _ = NewMetrics(nil)

	for _, opt := range opts {
		opt(t)
	}

	// Seeded after the options, so WithClock governs the first instants as
	// well as every later one.
	t.lastArrival = t.now()
	t.stats.StartTime = t.now().UTC()
	t.watchSource()

	if t.drain == nil {
		t.drain = NewDrainBudget(DefaultDrainDeadline)
	}

	return t
}

func (t *Turbine) StatusLoop(ctx context.Context) error {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			t.logThroughput()
			t.recordStateGauges(ctx)
		case <-ctx.Done():
			return nil
		}
	}
}

// recordStateGauges samples the state database for the size and row-count
// gauges. It runs on StatusLoop's existing tick rather than its own ticker,
// and reads the dedicated reader connection, so it neither adds a goroutine
// nor competes with the writer.
//
// A pipeline with no state database records nothing at all rather than
// reporting zero: an absent series and a genuinely empty state are different
// facts, and a dashboard should be able to distinguish them.
func (t *Turbine) recordStateGauges(ctx context.Context) {
	if t.stateStats == nil {
		return
	}

	stats, err := t.stateStats(ctx)
	if err != nil {
		// Never fatal: the pipeline keeps running and keeps serving its
		// other metrics even when state cannot be read.
		t.logger.Error("collecting state stats", zap.Error(err))
		return
	}
	if stats == nil {
		return
	}

	t.metrics.StateSizeBytes.Record(ctx, stats.SizeBytes)
	for _, tbl := range stats.Tables {
		t.metrics.StateTableRows.Record(ctx, tbl.Rows,
			metric.WithAttributes(attribute.String("table", tbl.Name)))
	}
}

func (t *Turbine) ConsumeLoop(ctx context.Context, maxMsgs int) (stats *Stats, err error) {
	t.logger.Info("consumer loop starting")
	defer t.loopDoneOnce.Do(func() { close(t.loopDone) })

	// Whatever made the loop fail, the batch in flight was not delivered, and
	// the positions it advanced must not outlive it. The caller commits again
	// after this returns: run syncs state before and after the table managers'
	// final poll. Those syncs save the positions held here, and the DuckDB
	// driver ignores the context on commit, so nothing about a failed or
	// cancelled run stops them. Before this reset, a batch the sink refused
	// had its offsets made durable on the way out, and a restart resumed past
	// rows nothing had written.
	defer func() {
		if err != nil {
			t.marks.Reset(t.committed)
		}
	}()

	if err := t.source.Start(); err != nil {
		return nil, err
	}
	defer func() {
		// Before the source leaves its group: a revocation the group
		// delivers during Close -- another member joining as this one drains
		// -- reaches requestDrop, and the loop that services drops has
		// already returned. With loopDone still open it would block there
		// until dropWait, past the drain deadline, and the supervisor would
		// kill the process mid-shutdown. dropAllOwned has run, so the rows
		// are already gone; the late request returns at once.
		t.loopDoneOnce.Do(func() { close(t.loopDone) })

		t.logger.Info("closing source from ConsumeLoop",
			zap.Bool("running", t.running),
			zap.String("here", "here"),
			zap.Error(err),
		)

		if err := t.source.Close(); err != nil {
			panic(err)
		}
	}()
	// Before the source closes: a sender still waiting on its flush hears
	// why it will not come.
	defer func() {
		if err != nil {
			t.settle(err)
			return
		}
		t.settle(ErrStoppedBeforeFlush)
	}()
	// And before either: the rows this worker still holds for partitions it
	// is about to give up.
	defer t.dropAllOwned(ctx)

	t.stats.StartTime = t.now().UTC()
	t.stats.SetNumMessagesConsumed(0)
	t.lock.Lock()
	t.lastArrival = t.now()
	t.lock.Unlock()
	// Under the lock: the debug API may already be running statements on
	// this connection.
	if err := t.initHandler(ctx); err != nil {
		return nil, err
	}

	// Every batch runs on batchCtx, not ctx. A SIGTERM arrives as ctx being
	// cancelled, and a busy pipeline is usually inside a flush when it does.
	// On ctx the cancel aborted that flush at once, the loop returned the
	// sink's error, and the drain never ran: the process exited with a
	// retryable code and replayed a batch it could have finished. batchCtx
	// ends when the drain budget does, so the batch in flight gets the same
	// deadline the drain gets, and one clock covers both.
	batchCtx, stopBatches := t.batchContext(ctx)
	defer stopBatches()

	numBatchMessages := 0
	totalConsumed := int64(0)
	hitMax := false

	stream := t.source.Stream()

	var totalRecvWait time.Duration
	defer func() {
		t.logger.Debug("consume loop wait totals", zap.Duration("recv_wait", totalRecvWait))
	}()

	// A batch that never reaches batchSize still has to reach the sink, or a
	// low-traffic topic — and any push source between deliveries — stalls
	// indefinitely.
	var flushC <-chan time.Time
	switch {
	case t.flushTrigger != nil:
		flushC = t.flushTrigger
	case t.flushInterval > 0:
		flushTicker := time.NewTicker(t.flushInterval)
		defer flushTicker.Stop()
		flushC = flushTicker.C
	}

	for t.running && !hitMax {
		// Receive a batch of raw messages from the source
		r0 := time.Now()

		var (
			msgBatch []Message
			ok       bool
		)
		select {
		case msgBatch, ok = <-stream:
		case <-flushC:
			if numBatchMessages > 0 {
				if err := t.processBatch(batchCtx, numBatchMessages); err != nil {
					return nil, t.drainError(err, numBatchMessages)
				}
				numBatchMessages = 0
				continue
			}
			// Nothing buffered. This tick's job is the watermark: a
			// partition that has gone idle since the last commit leaves the
			// minimum here, and when the last one does, every window closes
			// through what it holds. That is the only write a tick makes,
			// and only when a watermark moved, so a quiet pipeline writes
			// nothing. With a state path it also ends the batch transaction
			// left open since the last commit, so nothing stays uncommitted
			// on the connection for as long as the stream is silent. The
			// progress row is left alone: nothing reads it between batches.
			if err := t.commitState(batchCtx, progressSkipped); err != nil {
				t.recordError(ctx, err, phaseStateCommit, "error committing state on idle tick")
				return nil, err
			}
			// Messages consumed with no batch after them, dropped by an
			// error policy or refused by a window, are finished with too.
			t.settle(nil)
			// A bucket the manager closed past its lateness since the last
			// batch releases the rows it held the commit behind. With no
			// batch coming, this tick is what moves the group's offset.
			if t.lowMoved {
				if err := t.commitSource(); err != nil {
					t.logger.Warn("failed to commit the low watermark on an idle tick", zap.Error(err))
				}
			}
			continue
		case req := <-t.drops:
			// The group is taking partitions away, and its rebalance waits
			// on this. Whatever is buffered is processed first, so every
			// position this worker reached is committed with its low
			// watermark before the rows behind it go.
			if numBatchMessages > 0 {
				if err := t.processBatch(batchCtx, numBatchMessages); err != nil {
					req.done <- err
					return nil, t.drainError(err, numBatchMessages)
				}
				numBatchMessages = 0
			}
			if err := t.dropPartitions(batchCtx, req.parts); err != nil {
				req.done <- err
				t.recordError(ctx, err, phaseStateCommit, "error dropping revoked partitions")
				return nil, err
			}
			req.done <- nil
			continue
		case <-ctx.Done():
			t.logger.Info("context done, draining the consumer loop")
			t.running = false
			// The source delivered this batch, but nothing has written it yet.
			// Returning without it drops the tail of every graceful shutdown.
			//
			// batchCtx is still live here: the cancel that ended ctx started
			// the drain budget's clock, and batchCtx ends with that clock. So
			// the drain has the whole deadline, and the same one the batch in
			// flight had.
			if numBatchMessages > 0 {
				if err := t.processBatch(batchCtx, numBatchMessages); err != nil {
					err = t.drainError(err, numBatchMessages)
					t.logger.Error("error draining the final batch", zap.Error(err))
					return nil, err
				}
			}
			t.logThroughput()
			return t.stats, nil
		}

		readLatency := time.Since(r0)
		totalRecvWait += readLatency
		if !ok {
			t.logger.Warn("stream channel closed")
			break
		}
		t.metrics.SourceReadLatency.Record(ctx, readLatency.Seconds())
		// The same reading as the histogram beside it, as a counter: a
		// receiver needs the total to compare against the wall clock, and a
		// histogram's sum is not in the bundle.
		t.metrics.RecvWaitSeconds.Add(ctx, readLatency.Seconds())
		t.metrics.MessageCount.Add(ctx, int64(len(msgBatch)))
		// Over the same messages message_count just counted, received rather
		// than processed, so bytes over count is a true average. One integer
		// add per message and one record per batch.
		var payload int64
		var newestNanos int64
		// One clock read for the whole batch. Two reads per message once
		// took this loop from 22 ns/op to 91, and every comparison below
		// has to be against the same now to be consistent.
		now := time.Now()
		nowNanos := now.UnixNano()
		for i := range msgBatch {
			payload += int64(len(msgBatch[i].Value))
			// The newest event in the batch, taken in the pass that already
			// walks it. A separate pass would double the per-message work
			// this loop exists to keep small.
			//
			// Two kinds of record are skipped, because each reports a lag
			// that is not this pipeline's:
			//
			// Below the floor is a record with no usable timestamp, or a
			// clock that never learned the date; see eventTimeFloorNanos.
			//
			// Past the ceiling is a clock ahead of this host's, not a pipeline
			// ahead of its stream. A Kafka record carries the producer's
			// clock unless the topic sets LogAppendTime, so one fast device
			// in a fleet can win "newest" for the whole batch. Taking it and
			// clamping the negative lag to zero reported "caught up" while
			// the other 499 records in the batch were an hour behind.
			at := msgBatch[i].EventAtNanos
			if at > eventTimeFloorNanos && at <= nowNanos && at > newestNanos {
				newestNanos = at
			}
		}
		t.metrics.MessagePayloadBytes.Add(ctx, payload)
		// Once per batch, not per message: the cost is one gauge record
		// against a whole batch, and a reader asking "is it still doing
		// anything" cannot tell the two apart.
		t.metrics.PipelineLastMessage.Record(ctx, now.Unix())
		t.metrics.Activity.Mark()

		// One reading per batch, from the newest usable event in it: the
		// oldest would add the batch's own span, which says as much about
		// batch_size as about the stream.
		//
		// A batch in which every record was skipped reports nothing at all.
		// Zero would claim the pipeline had caught up, which is the one
		// wrong answer this field exists to avoid, and absent is not zero
		// anywhere else in this contract either.
		//
		// The reading is taken when the batch arrives, before the handler
		// and the sink run. It is how far behind the stream the pipeline
		// reads, not how long the data takes to land; a slow flush shows up
		// as event_lag_observed_at going stale, and in the batch and
		// sink_flush durations beside it.
		if t.eventTimeSource != nil && newestNanos > eventTimeFloorNanos {
			lag := float64(nowNanos-newestNanos) / float64(time.Second)
			t.metrics.EventLagSeconds.Record(ctx, lag)
			if lag > t.eventLagMax {
				t.eventLagMax = lag
			}
			t.metrics.EventLagMaxSeconds.Record(ctx, t.eventLagMax)
			t.metrics.EventLagObserved.Record(ctx, now.Unix())
		}

		// handler.write is timed by bracketing the whole loop and subtracting
		// the batches that ran inside it, rather than by timing each
		// writeMessage call.
		//
		// Two clock reads per message took this loop from 22 ns/op to 91,
		// benchmarked in BenchmarkConsumeLoopWritePath -- 4.1x, the same shape
		// as the regression that put a cached attribute set in front of
		// consumer_lag. Nothing on the per-message path survives that price.
		//
		// The cost of measuring it out here is that mark, the consumed
		// counters and the pending-lag map write are charged to handler.write
		// along with the write itself. That is around 22 ns a message against
		// a real handler that parses JSON and appends to an Arrow builder in
		// microseconds, so the attribution error is under a percent of the
		// phase it lands in.
		loopStart := time.Now()
		var batchTook time.Duration
		// Once per source batch, not per record: a drop is serviced between
		// batches and the floors are taken here, so neither changes while
		// this batch is walked, and an assignment that clears a revocation
		// happens before the partition's records are polled.
		t.takeFloors()
		floors := len(t.replayFloors) > 0
		revoked := t.revoked.Load()
		skipRevoked := revoked != nil && len(*revoked) > 0

		for _, raw := range msgBatch {
			// Fetched before its partition was revoked, arriving after the
			// rows were dropped. It is the new owner's now; neither written
			// nor marked here.
			if skipRevoked && (*revoked)[partitionKey{raw.Topic, raw.Partition}] {
				continue
			}
			// A record whose event time this engine cannot place never
			// reaches the handler, so it can never reach a window table and
			// never move a watermark. One stamped in the future otherwise
			// drags the watermark past real time and makes every correctly
			// stamped record after it late (#358).
			//
			// It is consumed, not failed: marked and counted like a record
			// an error policy dropped, so the position is safe to commit
			// past and the pipeline carries on. Only a windowing pipeline
			// refuses; see WithEventTimePlacement.
			if t.placesEventTime && !t.canPlace(raw.EventAtNanos, nowNanos) {
				t.noteUnplaceable(raw.EventAtNanos, now)
				t.mark(raw)
				totalConsumed++
				t.unsettled++
				t.stats.SetNumMessagesConsumed(totalConsumed)
				if maxMsgs > 0 && totalConsumed >= int64(maxMsgs) {
					t.logger.Info("max messages consumed, stopping consumer loop")
					hitMax = true
					break
				}
				continue
			}
			// Placed, so the window's lateness rule applies: a record whose
			// bucket closed more than allowed_lateness_seconds ago has
			// nowhere to go, and is refused before the handler for the same
			// reason an unplaceable one is -- a row the window's promise
			// excludes must not reach the table. One whose bucket closed
			// within lateness is written, and the bucket is republished
			// whole. Decided here, at arrival, as Flink's window operator
			// decides it; nothing sweeps the table for late rows afterwards.
			if t.windows != nil {
				// A partition replayed from the low watermark: what its last
				// owner had closed past lateness stays closed, whatever this
				// worker's own watermark has reached.
				if floors && t.belowReplayFloor(raw) {
					t.noteRefusedLate(ctx, raw)
					t.mark(raw)
					totalConsumed++
					t.unsettled++
					t.stats.SetNumMessagesConsumed(totalConsumed)
					if maxMsgs > 0 && totalConsumed >= int64(maxMsgs) {
						t.logger.Info("max messages consumed, stopping consumer loop")
						hitMax = true
						break
					}
					continue
				}
				refused, late := t.windows.Classify(raw.EventAtNanos)
				if refused {
					t.noteRefusedLate(ctx, raw)
					t.mark(raw)
					totalConsumed++
					t.unsettled++
					t.stats.SetNumMessagesConsumed(totalConsumed)
					if maxMsgs > 0 && totalConsumed >= int64(maxMsgs) {
						t.logger.Info("max messages consumed, stopping consumer loop")
						hitMax = true
						break
					}
					continue
				}
				if len(late) > 0 {
					t.pendingLate = append(t.pendingLate, late...)
				}
				// And it counts toward the watermark, whether or not the
				// handler takes it: a record an error policy drops still
				// says where the stream has got to, as Flink's assigner does.
				//
				// Accumulated here and handed to the tracker once, when the
				// batch is processed. Calling the tracker per record takes
				// its mutex per record, which measured at 52ns against a loop
				// whose own budget is around 40 -- the same shape as the
				// phase timing this loop already refuses. A partition is one
				// entry, and a fetch spans few, so the scan is a comparison
				// or two.
				t.notePlaced(raw.Topic, raw.Partition, raw.EventAtNanos)
				if t.windowOffsets != nil {
					t.windowOffsets.Note(raw)
				}
			}
			if err := t.writeMessage(raw); err != nil {
				t.recordError(ctx, err, phaseHandlerWrite, "error writing message")

				if err := t.applyErrorPolicy(ctx, err, phaseHandlerWrite, string(raw.Value), 1); err != nil {
					return nil, err
				}
				// The message is dropped from the batch, but it was still
				// consumed from the source: it counts toward the reported
				// total and toward --max-msgs, as in the Python engine -- and
				// its position is finished with, so it is safe to commit past.
				t.mark(raw)
				totalConsumed++
				t.unsettled++
				t.stats.SetNumMessagesConsumed(totalConsumed)
				if maxMsgs > 0 && totalConsumed >= int64(maxMsgs) {
					t.logger.Info("max messages consumed, stopping consumer loop")
					hitMax = true
					break
				}
				continue
			}

			t.mark(raw)
			numBatchMessages++
			totalConsumed++
			t.unsettled++
			t.stats.SetNumMessagesConsumed(totalConsumed)

			if maxMsgs > 0 && totalConsumed >= int64(maxMsgs) {
				t.logger.Info("max messages consumed, stopping consumer loop")
				hitMax = true
				break
			}

			if numBatchMessages == t.batchSize {
				// A cancel that lands as the batch fills drains it, the same as
				// the select's cancel branch. Returning here without it used to
				// report a clean stop while the batch's positions were already
				// advanced, and the shutdown then committed them for rows the
				// sink never received.
				select {
				case <-ctx.Done():
					t.logger.Info("context done at a batch boundary, draining the batch")
					t.running = false
					if err := t.processBatch(batchCtx, numBatchMessages); err != nil {
						err = t.drainError(err, numBatchMessages)
						t.logger.Error("error draining the final batch", zap.Error(err))
						return nil, err
					}
					t.logThroughput()
					return t.stats, nil
				default:
				}

				p0 := time.Now()
				if err := t.processBatch(batchCtx, numBatchMessages); err != nil {
					return nil, t.drainError(err, numBatchMessages)
				}
				batchTook += time.Since(p0)
				numBatchMessages = 0
			}
		}
		// One observation per source batch. Guarded because an empty batch
		// wrote nothing, and recording a zero would drag the histogram toward
		// the floor with time no message spent.
		//
		// Before recordLag, so the per-partition gauge records that fetch owes
		// are not charged to handler.write.
		if len(msgBatch) > 0 {
			t.recordPhase(ctx, phaseHandlerWrite, time.Since(loopStart)-batchTook)
		}

		t.recordLag(ctx)
	}

	// Whatever is buffered when the loop ends — because --max-msgs was
	// reached or the source ended — still has to reach the sink, otherwise
	// messages counted as consumed are silently dropped.
	if numBatchMessages > 0 {
		if err := t.processBatch(batchCtx, numBatchMessages); err != nil {
			return nil, t.drainError(err, numBatchMessages)
		}
	}

	t.logThroughput()
	return t.stats, nil
}

// batchContext derives the context every batch runs on. It is not cancelled
// when run is; it ends when the drain budget's deadline passes after run was
// cancelled, and the budget's clock starts at that cancel. Stop it when the
// loop returns, or the goroutine outlives the run.
func (t *Turbine) batchContext(run context.Context) (context.Context, context.CancelFunc) {
	batchCtx, cancel := context.WithCancel(context.WithoutCancel(run))
	go func() {
		select {
		case <-run.Done():
		case <-batchCtx.Done():
			return
		}
		select {
		case <-t.drain.Context().Done():
			cancel()
		case <-batchCtx.Done():
		}
	}()
	return batchCtx, cancel
}

// drainError classifies a batch failure that happened after the drain
// deadline passed. The batch was not delivered whatever the sink said, and
// the code says why a supervisor sees a retryable exit: the stop ran out of
// time, and the next start replays what was not written.
func (t *Turbine) drainError(err error, buffered int) error {
	if !t.drain.Exceeded() {
		return err
	}
	return errs.Wrap(errs.CodeDrainIncomplete, err,
		"drain deadline %s reached with %d messages buffered", t.drain.Deadline(), buffered)
}

// writeMessage hands the message to the handler, with its source metadata if
// the handler can use it.
func (t *Turbine) writeMessage(msg Message) error {
	if w, ok := t.handler.(MetadataWriter); ok {
		return w.WriteMessage(msg)
	}
	return t.handler.Write(msg.Value)
}

// result labels an operation that either completed or did not. It answers
// "how often does this fail" without the caller knowing a single error code,
// which is the question a dashboard asks first.
const (
	resultOK    = "ok"
	resultError = "error"
)

// resultAttrs returns the cached attribute set for a result.
//
// Cached because metric.WithAttributes allocates on
// every call whatever the metrics config, and passing options one by one
// reallocates the variadic slice too. These sit on the per-batch path.
func (t *Turbine) resultAttrs(result string) []metric.AddOption {
	if result == resultOK {
		return t.resultOKAttrs
	}
	return t.resultErrorAttrs
}

// recordPhase times one stage of a batch.
//
// Called on the success and the failure path both, which is the difference
// between this and sink_flush_latency or state_commit_latency: those record
// after every error return, so a phase that takes thirty seconds to fail
// leaves them flat. Time spent failing is still time the batch spent, and a
// decomposition that drops it points the operator at the wrong phase.
func (t *Turbine) recordPhase(ctx context.Context, phase string, took time.Duration) {
	t.metrics.PhaseDuration.Record(ctx, took.Seconds(), t.phaseAttrs[phase]...)
}

// recordError counts and logs one failure.
//
// Every error path calls it, which is the point: error_count used to be
// incremented in applyErrorPolicy alone, so the eight paths that never reach
// the error policy -- sink writes, sink flushes, state commits, the drain --
// raised stats.NumErrors and left the metric flat. Nobody could alert on a
// failing sink.
//
// The labels all derive from the one code, so a dashboard can group by class
// to ask whose fault it is, by domain to ask which subsystem, or by code for
// the specific failure. Errors are rare, so attribute allocation here does not
// need the caching the per-message paths use.
func (t *Turbine) recordError(ctx context.Context, err error, phase, message string) {
	t.stats.NumErrors++
	t.lastErrorUnixNano.Store(time.Now().UnixNano())
	t.errorCount.Add(1)

	code := errs.CodeOf(err)
	t.lastErrorCode.Store(string(code))
	t.metrics.ErrorCount.Add(ctx, 1, metric.WithAttributes(
		attribute.String("class", string(code.Class())),
		attribute.String("domain", code.Domain()),
		attribute.String("code", string(code)),
		attribute.String("phase", phase),
	))
	t.metrics.PipelineErrors.Add(ctx, 1)

	t.logger.Error(message,
		zap.Error(err),
		zap.String("error.code", string(code)),
		zap.String("error.class", string(code.Class())),
	)
}

// EventBasis is where this pipeline's event times come from, and empty for
// a source that has none.
//
// The turbine holds the source it resolved at construction and asks it each
// time, rather than caching the name: a Kafka topic's timestamp type is not
// known until a record arrives. The source is responsible for making that
// read safe from this goroutine.
func (t *Turbine) EventBasis() string {
	if t.eventTimeSource == nil {
		return ""
	}
	return t.eventTimeSource.EventTimeBasis()
}

// LastError is the code and time of the last error this pipeline recorded.
// ok is false before the first one.
//
// The code only. A message carries the row that failed, a connection string
// or a customer's data, and it is not going on a wire. An operator with the
// code and the time finds the message in their own logs.
func (t *Turbine) LastError() (string, time.Time, bool) {
	nanos := t.lastErrorUnixNano.Load()
	code, _ := t.lastErrorCode.Load().(string)
	if nanos == 0 || code == "" {
		return "", time.Time{}, false
	}
	return code, time.Unix(0, nanos), true
}

// applyErrorPolicy decides what happens to a failed message or batch. It
// returns a non-nil error only when the pipeline should stop. Counting and
// logging belong to recordError, which every caller has already run. rows
// is how many rows the failure covers, which IGNORE discards.
func (t *Turbine) applyErrorPolicy(ctx context.Context, cause error, phase, message string, rows int64) error {

	switch t.errorPolicy.Policy {
	case PolicyIgnore:
		// Counted here and nowhere else: the DLQ diverts its rows and RAISE
		// stops the pipeline, so only IGNORE loses them.
		t.metrics.ErrorRowsDropped.Add(ctx, rows)
		return nil

	case PolicyDLQ:
		if t.errorPolicy.DLQSink == nil {
			return fmt.Errorf("error policy is DLQ but no dlq sink is configured: %w", cause)
		}
		if err := t.writeDLQ(ctx, cause, phase, message); err != nil {
			t.logger.Error("error writing to dlq", zap.Error(err))
			return err
		}
		return nil

	default:
		return cause
	}
}

// writeDLQ records one failed message or batch, in the same shape the Python
// engine produces: error, message, phase and timestamp.
func (t *Turbine) writeDLQ(ctx context.Context, cause error, phase, message string) error {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "error", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "message", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "phase", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "timestamp", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	b := array.NewRecordBuilder(memory.NewGoAllocator(), schema)
	defer b.Release()

	b.Field(0).(*array.StringBuilder).Append(cause.Error())
	b.Field(1).(*array.StringBuilder).Append(message)
	b.Field(2).(*array.StringBuilder).Append(phase)
	b.Field(3).(*array.StringBuilder).Append(time.Now().UTC().Format(time.RFC3339Nano))

	rec := b.NewRecord()
	defer rec.Release()

	table := array.NewTableFromRecords(schema, []arrow.Record{rec})
	defer table.Release()

	if err := t.errorPolicy.DLQSink.WriteTable(ctx, table); err != nil {
		return err
	}
	return t.errorPolicy.DLQSink.Flush(ctx)
}

// mark records that the pipeline has finished with a message -- written to
// the handler or dropped by policy -- so a commit can name that position.
// Messages without source metadata have no position to record.
func (t *Turbine) mark(m Message) {
	if !m.HasMetadata() {
		return
	}
	t.marks.Advance(m.Topic, m.Partition, Mark{Offset: m.Offset, LeaderEpoch: m.LeaderEpoch})

	// -1 because a mark names the last *processed* offset: having processed
	// offset 9 with a watermark of 10 is lag zero.
	if m.HighWatermark > 0 {
		if t.lagPending == nil {
			t.lagPending = make(map[lagKey]int64)
		}
		t.lagPending[lagKey{topic: m.Topic, partition: m.Partition}] = m.HighWatermark - m.Offset - 1
	}
}

// recordLag publishes the lag of every partition marked since the last call,
// once each. The consume loop calls it after each fetch, so the gauge is as
// fresh as the newest message and costs one record per partition per fetch
// rather than one per message.
func (t *Turbine) recordLag(ctx context.Context) {
	if len(t.lagPending) > 0 {
		// Once per fetch that marked anything, beside the readings it dates.
		t.metrics.LagObserved.Record(ctx, time.Now().Unix())
	}
	for key, lag := range t.lagPending {
		t.metrics.Lag.Set(key.topic, key.partition, lag)
		delete(t.lagPending, key)
	}
}

// settle tells a Settler source the loop is finished with what it took:
// flushed and committed when err is nil.
func (t *Turbine) settle(err error) {
	if t.unsettled == 0 {
		return
	}
	if s, ok := t.source.(Settler); ok {
		s.Settle(t.unsettled, err)
	}
	t.unsettled = 0
}

// commitSource commits what the pipeline has processed. A source that can
// take explicit marks gets exactly the positions this pipeline has finished
// with; anything else gets the plain Commit it always did.
//
// The distinction is not academic. The Kafka source reads ahead of the
// pipeline into a buffer, and its plain Commit commits everything it has
// fetched: after one 20,000-message batch it had committed offset 70,086.
// Whatever sat in that buffer when the process died was gone for good, with
// the consumer group showing no lag.
//
// A pipeline that windows commits each partition's low watermark instead:
// the position before the lowest offset still feeding a bucket the window
// retains. The processed position passes rows that exist only in the window's
// table, and a worker that starts without that table -- a new disk, or a
// partition it was just assigned -- would begin after them and never count
// them. Held at the low watermark, the log is the durable copy of the
// window: whoever starts there rebuilds every retained bucket whole.
func (t *Turbine) commitSource() error {
	if mc, ok := t.source.(MarkCommitter); ok {
		if t.marks.Empty() {
			return nil
		}
		t.lowMoved = false
		if t.windowOffsets != nil {
			low := t.windowOffsets.Low(t.marks)
			// The closed watermarks ride the commit, so whoever starts from
			// this position next refuses what this worker had finalized.
			if mc2, ok := t.source.(MetadataCommitter); ok {
				return mc2.CommitMarksWithMetadata(low, EncodeReplayFloor(t.windowOffsets.Closed()))
			}
			return mc.CommitMarks(low)
		}
		return mc.CommitMarks(t.marks)
	}
	return t.source.Commit()
}

// expireWindowOffsets drops the records of buckets the manager has closed
// past their lateness.
func (t *Turbine) expireWindowOffsets() {
	if t.windowOffsets == nil || t.windows == nil {
		return
	}
	for _, spec := range t.windows.Specs() {
		if closed, ok := t.windows.Signal(spec.Name).Closed(); ok {
			if t.windowOffsets.Expire(spec.Name, closed) {
				t.lowMoved = true
			}
		}
	}
}

// initHandler resets the handler under the lock. Init drops or truncates the
// batch table on the shared connection, and the debug API runs statements
// on that connection from its own goroutine. DuckDB closes a pending result
// the moment another statement runs on its connection, so an unlocked reset
// fails whichever query is in flight with "closed pending query result".
// #280 found that with the table managers, which polled this connection
// then; since #281 each manager polls a connection of its own and never
// takes this lock.
func (t *Turbine) initHandler(ctx context.Context) error {
	t.lock.Lock()
	defer t.lock.Unlock()
	if err := t.handler.Init(ctx); err != nil {
		return err
	}
	// Read under the lock, so a panic in Init still releases it and the flag
	// is the one this Init set.
	if r, ok := t.handler.(CheckpointSkipReporter); ok && r.CheckpointSkipped() {
		t.metrics.HandlerCheckpointsSkipped.Add(ctx, 1)
	}
	return nil
}

// CheckpointSkipReporter is a handler that checkpoints as it re-initialises
// and skips the checkpoint when another connection's write refuses it.
//
// Optional, like BufferedRowReporter: a handler that never checkpoints
// reports nothing. The count exists so a window whose I/O holds a write open
// for longer than a batch shows as a run of skipped checkpoints, not as
// memory growth nobody can explain.
type CheckpointSkipReporter interface {
	// CheckpointSkipped reports whether the last Init skipped its checkpoint.
	CheckpointSkipped() bool
}

// rollbackState discards this batch's uncommitted state writes. Used on the
// paths that fail before commitState is reached.
func (t *Turbine) rollbackState(ctx context.Context) {
	if t.stateTx == nil {
		return
	}
	t.lock.Lock()
	defer t.lock.Unlock()
	// Without cancellation. A rollback is local and must happen. The DuckDB
	// driver ignores the context on a rollback today, so this changes nothing
	// there; it is for any transaction that does honour it, where a rollback
	// refused on an expired drain context would leave this batch's writes in
	// the transaction for the next state sync to commit without their
	// offsets. The harness's recording transaction is one.
	if err := t.stateTx.Rollback(context.WithoutCancel(ctx)); err != nil {
		t.logger.Error("rollback failed", zap.Error(err))
	}
	t.dropPendingLate()
}

// commitState writes the processed offsets into the state database and commits
// them together with whatever the handler wrote in this batch. Any failure
// rolls the whole transaction back, leaving the durable offsets where they
// were so the batch is replayed rather than lost.
//
// A pipeline with no state database commits nothing here; its progress write
// autocommits by itself.
//
// The watermark is asserted before the progress row is recorded, and that
// order is load-bearing rather than incidental. The progress snapshot is the
// observable "this commit happened" signal -- /stats and /healthz read it,
// and so does anything waiting on a commit -- so a reader that sees a newer
// commit clock can rely on the watermark that commit asserted being visible
// already. Recorded first, the snapshot moved while the assertion was still
// unwritten, and a poll that ran in between read the older watermark.
func (t *Turbine) commitState(ctx context.Context, write progressWrite) error {
	t.noteDelivering()
	t.expireWindowOffsets()
	moved, watermarkErr := t.assertWatermarks(ctx, write)
	progressErr := t.recordProgress(ctx, write)

	if t.offsets == nil || t.stateTx == nil {
		if progressErr != nil {
			// The write autocommitted by itself, so the batch is untouched
			// and the pipeline carries on. It gets its own code so an alert
			// can tell it from a state commit that failed, which means the
			// pipeline is stopping.
			t.recordError(ctx, errs.Wrap(errs.CodeProgressWriteFailed, progressErr,
				"sqlflow_progress not written"), phaseStateCommit, "sqlflow_progress not written")
		}
		// The watermark autocommitted by itself too, after the handler's
		// rows. A failed write leaves the manager holding, which is the
		// late direction; the next move writes the newer value.
		switch {
		case watermarkErr != nil:
			t.recordError(ctx, errs.Wrap(errs.CodeWatermarkWriteFailed, watermarkErr,
				"sqlflow_watermarks not written"), phaseStateCommit, "sqlflow_watermarks not written")
			t.dropPendingLate()
		case t.windows != nil:
			t.windows.Commit(moved)
			t.signalWindows(ctx, moved)
		}
		// Nothing persists the offset records without a state path.
		if t.windowOffsets != nil {
			t.windowOffsets.Saved()
		}
		t.lock.Lock()
		t.commits++
		t.lock.Unlock()
		return nil
	}

	c0 := time.Now()

	t.lock.Lock()
	defer t.lock.Unlock()

	// With a state path the progress UPDATE ran inside this transaction, and
	// a statement DuckDB refuses may abort it: a constraint or conversion
	// error does, a catalog error does not. Commit on an aborted transaction
	// reports success while keeping none of the batch. A source with offsets
	// would trip over the abort saving them below; a source with none, such
	// as a webhook, has nothing left to write and would lose the batch
	// without a word. Whether or not this one aborted, the batch's
	// transaction is not one to commit, so the refused write fails the commit
	// here the way any other failed commit does, and the rollback leaves the
	// connection ready for the next batch and for the drain rather than stuck
	// in an aborted transaction that fails everything after it.
	if progressErr != nil {
		if rbErr := t.stateTx.Rollback(context.WithoutCancel(ctx)); rbErr != nil {
			t.logger.Error("rollback after failed progress write", zap.Error(rbErr))
		}
		t.dropPendingLate()
		return errs.Wrap(errs.CodeStateCommitFailed, progressErr,
			"the progress write failed inside the state transaction")
	}

	// The watermark rode this transaction, beside the rows it describes, so a
	// write DuckDB refused is this batch's failure like any other.
	if watermarkErr != nil {
		if rbErr := t.stateTx.Rollback(context.WithoutCancel(ctx)); rbErr != nil {
			t.logger.Error("rollback after failed watermark write", zap.Error(rbErr))
		}
		t.dropPendingLate()
		return errs.Wrap(errs.CodeStateCommitFailed, watermarkErr, "asserting the watermark")
	}

	if err := t.offsets.Save(ctx, t.marks); err != nil {
		if rbErr := t.stateTx.Rollback(context.WithoutCancel(ctx)); rbErr != nil {
			t.logger.Error("rollback after failed offset save", zap.Error(rbErr))
		}
		t.dropPendingLate()
		return errs.Wrap(errs.CodeStateCommitFailed, err, "saving offsets")
	}

	// The offset records ride the same transaction as the rows they
	// describe, so a restart on this state file holds both or neither.
	if t.windowOffsetStore != nil {
		if err := t.windowOffsetStore.Save(ctx, t.windowOffsets.Pending()); err != nil {
			if rbErr := t.stateTx.Rollback(context.WithoutCancel(ctx)); rbErr != nil {
				t.logger.Error("rollback after failed offset-record save", zap.Error(rbErr))
			}
			t.dropPendingLate()
			return errs.Wrap(errs.CodeStateCommitFailed, err, "saving offset records")
		}
	}

	if err := t.stateTx.Commit(ctx); err != nil {
		// The commit itself failed, so the transaction is still open and
		// still holds this batch's writes; roll it back explicitly rather
		// than leaving them to leak into the next batch.
		if rbErr := t.stateTx.Rollback(context.WithoutCancel(ctx)); rbErr != nil {
			t.logger.Error("rollback after failed commit", zap.Error(rbErr))
		}
		t.dropPendingLate()
		return errs.Wrap(errs.CodeStateCommitFailed, err, "committing state")
	}
	if t.windows != nil {
		t.windows.Commit(moved)
		t.signalWindows(ctx, moved)
	}
	if t.windowOffsets != nil {
		t.windowOffsets.Saved()
	}

	t.commits++
	t.committed.Reset(t.marks)
	t.metrics.StateCommitLatency.Record(ctx, time.Since(c0).Seconds())
	t.metrics.StateCommitCount.Add(ctx, 1, t.resultAttrs(resultOK)...)
	// Success only. The error path below records the dimensioned series and
	// not this one, because a failed commit is not a commit.
	t.metrics.PipelineCommits.Add(ctx, 1)
	return nil
}

// SyncState closes the open state transaction, making everything written
// since the last commit durable, forces the progress write whether or not
// the interval has come due, and asserts the watermarks as they stand. A
// pipeline with no state database has no transaction to close, but the
// other two still happen.
//
// Shutdown uses it twice: once before the table managers run their final
// poll, so that poll reads the watermark up to the signal -- a partition
// that went idle just before it leaves the minimum here, and the buckets a
// clean shutdown should close are closed -- and once after, so the batch
// transaction is closed with everything the drain wrote. The managers
// commit their own transactions on their own connections; neither call is
// for them.
func (t *Turbine) SyncState(ctx context.Context) error {
	return t.commitState(ctx, progressForced)
}

// processBatch invokes the handler on the buffered messages, writes the
// result to the sink, commits the source, and resets the handler for the
// next batch.
// The buffer depth is no longer published as its own gauge. It is
// sink_rows_accepted minus sink_rows_written, derived at query time, which
// yields a rate the gauge could not and cannot misreport itself the way a
// sink's own count can. BufferedRowReporter stays: the conformance harness
// proves sink.buffer.reports_depth through the interface, not the metric.

func (t *Turbine) processBatch(ctx context.Context, numBatchMessages int) error {
	b0 := time.Now()

	// What this batch's records said about their partitions, handed over
	// before the commit that asserts a watermark from it. Here rather than
	// in the message loop above, so a batch the loop abandoned -- max-msgs
	// reached, a fatal error -- reports only the records it actually reached.
	t.flushObservations()
	if t.windowOffsets != nil {
		t.windowOffsets.Merge()
	}

	t.lock.Lock()
	batch, err := t.handler.Invoke(ctx)
	t.lock.Unlock()

	b1 := time.Now()
	t.recordPhase(ctx, phaseHandlerInvoke, b1.Sub(b0))

	// Recorded before the error branch below. A failed Invoke reports zero,
	// which is the honest number, and skipping the record entirely would
	// freeze the ratio's denominator through every failing batch -- hiding
	// loss in exactly the case where rows were lost.
	t.metrics.HandlerRowsRead.Add(ctx, t.handler.RowsRead())

	if err != nil {
		t.recordError(ctx, err, phaseHandlerInvoke, "error invoking handler")

		if policyErr := t.applyErrorPolicy(ctx, err, phaseHandlerInvoke, "Handler invocation failed", int64(numBatchMessages)); policyErr != nil {
			return policyErr
		}
		// The policy swallowed the failure, so this batch is discarded rather
		// than fatal -- but the handler's writes up to the point it failed are
		// still in the open state transaction. Discard them with the batch
		// they belong to, exactly as the sink-write and flush paths below do.
		// Committing them would leave state that no replay reproduces, with
		// nothing logged and no error returned.
		//
		// The rollback ends the transaction and the next one begins with it,
		// so the commit below still saves this batch's offsets: IGNORE means
		// the batch is dropped, and holding its position would replay it
		// forever.
		t.rollbackState(ctx)

		// The batch yielded no table, so there is nothing to write; the
		// source is still committed so the failed batch is not replayed
		// forever.
		batch = nil
	}

	if batch != nil {
		w0 := time.Now()
		err = t.sink.WriteTable(ctx, batch)
		// Before the branch, so a write that fails slowly is still counted as
		// time the batch spent.
		t.recordPhase(ctx, phaseSinkWrite, time.Since(w0))

		if err != nil {
			t.recordError(ctx, err, phaseSinkWrite, "error writing batch to sink")
			t.metrics.SinkFlushCount.Add(ctx, 1, t.resultAttrs(resultError)...)
			// Same reasoning as the flush path below: this batch's handler
			// writes are still uncommitted, and leaving them open would let
			// the next batch's commit adopt them along with its own offsets.
			t.rollbackState(ctx)
			batch.Release()
			return err
		}
	}

	f0 := time.Now()
	err = t.flush(ctx, batch)
	t.recordPhase(ctx, phaseSinkFlush, time.Since(f0))

	if err != nil {
		t.recordError(ctx, err, phaseSinkFlush, "error flushing sink")
		t.metrics.SinkFlushCount.Add(ctx, 1, t.resultAttrs(resultError)...)
		// A failed flush keeps its rows buffered, and sink_rows_written does
		// not move for them. The gap against sink_rows_accepted is what
		// separates a sink retrying a destination from one that has stopped
		// draining, and the counting sink records it without help from here.
		//
		// The handler's writes are still uncommitted in the state
		// transaction; discard them with the batch they belong to.
		t.rollbackState(ctx)
		if batch != nil {
			batch.Release()
		}
		return err
	}

	b2 := time.Now()

	t.metrics.SinkFlushLatency.Record(ctx, b2.Sub(b1).Seconds())
	t.metrics.SinkFlushCount.Add(ctx, 1, t.resultAttrs(resultOK)...)
	// Success only, for the same reason as PipelineCommits: the two error
	// paths above record the dimensioned series and not this one.
	t.metrics.PipelineFlushes.Add(ctx, 1)
	if batch != nil {
		t.metrics.SinkFlushNumRows.Record(ctx, batch.NumRows())
	}

	// The state transaction closes after the sink has flushed and before the
	// source is committed. That order is the guarantee: a crash between the
	// flush and the commit replays the batch, so an external sink may see a
	// duplicate -- recoverable -- while state and offsets stay consistent.
	// Committing first would move the offsets past rows the sink never
	// received, which loses them silently.
	// The batch has reached the sink: that is the arrival the progress row
	// reports.
	t.lock.Lock()
	t.lastArrival = t.now()
	t.lock.Unlock()

	c0 := time.Now()
	err = t.commitState(ctx, progressOnInterval)
	// Timed at the call site rather than inside commitState, so the phase is
	// reported by a pipeline with no state database too. state_commit_latency
	// is deliberately absent there -- an absent series and an empty state are
	// different facts -- but the commit phase still runs, still writes
	// progress, and still belongs in the batch-time decomposition.
	t.recordPhase(ctx, phaseStateCommit, time.Since(c0))

	if err != nil {
		t.recordError(ctx, err, phaseStateCommit, "error committing state")
		t.metrics.StateCommitCount.Add(ctx, 1, t.resultAttrs(resultError)...)
		if batch != nil {
			batch.Release()
		}
		return err
	}

	// Committed only after the sink has flushed, so a crash replays the
	// batch rather than losing it. With a state database this is advisory:
	// the durable position is the one in the state transaction above, and
	// this keeps the consumer group's lag readable.
	//
	// Which is why a failure here is fatal only without a state database.
	// Once the offsets are durable, killing the pipeline over a rebalance or
	// a commit timeout trades a readable lag figure for an outage, and the
	// restart then has to fight its way back into the group.
	if err := t.commitSource(); err != nil {
		if t.stateTx != nil {
			t.logger.Warn("failed to commit offsets to the source; durable offsets are already committed",
				zap.Error(err))
		} else {
			t.logger.Error("error committing source", zap.Error(err))
			if batch != nil {
				batch.Release()
			}
			return err
		}
	} else if t.stateTx == nil {
		// Without a state database the source's commit is the durable one.
		t.committed.Reset(t.marks)
	}

	// Flushed and committed: a sender waiting on this batch can be told.
	t.settle(nil)

	b3 := time.Now()

	if batch != nil {
		batch.Release()
	}

	i0 := time.Now()
	err = t.initHandler(ctx)
	t.recordPhase(ctx, phaseHandlerInit, time.Since(i0))

	if err != nil {
		t.recordError(ctx, err, phaseHandlerInit, "error reinitializing handler")
		return err
	}

	b4 := time.Now()
	t.metrics.BatchProcessingLatency.Record(ctx, b4.Sub(b0).Seconds())
	t.logger.Debug("batch timing",
		zap.Duration("invoke", b1.Sub(b0)),
		zap.Duration("sink", b2.Sub(b1)),
		zap.Duration("commit", b3.Sub(b2)),
		zap.Duration("init", b4.Sub(b3)),
		zap.Duration("total", b4.Sub(b0)),
	)
	return nil
}

func (t *Turbine) logThroughput() {
	consumed := t.stats.MessagesConsumed()
	if duration := time.Since(t.stats.StartTime).Seconds(); duration > 0 {
		t.stats.SetThroughput(float64(consumed) / duration)
	} else {
		t.stats.SetThroughput(0)
	}

	throughput := t.stats.GetThroughput()
	if throughput > 0 {
		t.logger.Info("throughput",
			zap.Int64("messages_consumed", consumed),
			zap.Float64("total_throughput_per_second", throughput),
		)
	} else {
		t.logger.Info("no messages consumed, throughput is zero")
	}
}

// flush sends the buffered batch. The caller records a failure once; a
// second record here counted every failed flush twice.
func (t *Turbine) flush(ctx context.Context, batch arrow.Table) error {
	return t.sink.Flush(ctx)
}

// notePlaced accumulates one placed record against its partition, for
// flushObservations to hand to the watermark tracker. Runs on the consume
// loop only, so it takes no lock.
func (t *Turbine) notePlaced(topic string, partition int32, atNanos int64) {
	for i := range t.observed {
		if t.observed[i].Partition == partition && t.observed[i].Topic == topic {
			if atNanos > t.observed[i].NewestNanos {
				t.observed[i].NewestNanos = atNanos
			}
			return
		}
	}
	t.observed = append(t.observed, Observation{
		Topic: topic, Partition: partition, NewestNanos: atNanos,
	})
}

// noteRefusedLate counts a record refused as late, once per window, and
// logs the condition once per run: a device stuck in the past writes a
// line rather than a line per record.
func (t *Turbine) noteRefusedLate(ctx context.Context, m Message) {
	for _, spec := range t.windows.Specs() {
		t.metrics.WindowLateRows.Add(ctx, 1, t.lateAttrsFor(spec.Name, "refused")...)
	}
	if t.lateRefusedLogged {
		return
	}
	t.lateRefusedLogged = true
	t.logger.Warn("refusing records late beyond allowed_lateness_seconds",
		zap.Time("event_time", time.Unix(0, m.EventAtNanos).UTC()))
}

// lateAttrsFor is the cached attribute set for one window and outcome.
func (t *Turbine) lateAttrsFor(window, outcome string) []metric.AddOption {
	key := window + "\x00" + outcome
	if opts, ok := t.lateAttrs[key]; ok {
		return opts
	}
	if t.lateAttrs == nil {
		t.lateAttrs = map[string][]metric.AddOption{}
	}
	opts := []metric.AddOption{metric.WithAttributes(
		attribute.String("window", window), attribute.String("outcome", outcome))}
	t.lateAttrs[key] = opts
	return opts
}

// signalWindows tells each window's manager what this commit changed: a kick
// for every window whose watermark moved, and the recompute buckets this
// batch's late rows landed in, followed by a kick. After the commit, never
// before, so a manager woken here reads what the commit wrote. Called only
// when the commit succeeded; a rollback drops pendingLate, and the replay
// classifies the same records again. The recompute counter is recorded here
// for the same reason: a batch that rolls back counts nothing.
func (t *Turbine) signalWindows(ctx context.Context, moved map[string]time.Time) {
	if t.windows == nil {
		return
	}
	kicked := map[string]bool{}
	for name := range moved {
		if sig := t.windows.Signal(name); sig != nil {
			sig.Kick()
			kicked[name] = true
		}
	}
	for _, lb := range t.pendingLate {
		sig := t.windows.Signal(lb.Window)
		if sig == nil {
			continue
		}
		sig.Recompute(lb.Bucket)
		t.metrics.WindowLateRows.Add(ctx, 1, t.lateAttrsFor(lb.Window, "recomputed")...)
		if !kicked[lb.Window] {
			sig.Kick()
			kicked[lb.Window] = true
		}
	}
	t.pendingLate = t.pendingLate[:0]
}

// dropPendingLate forgets this batch's late buckets: the batch rolled back,
// and its replay will find them again.
func (t *Turbine) dropPendingLate() { t.pendingLate = t.pendingLate[:0] }

// flushObservations hands the batch's observations to the tracker and keeps
// the slice for the next batch, so a steady pipeline allocates nothing here.
func (t *Turbine) flushObservations() {
	if t.windows == nil || len(t.observed) == 0 {
		return
	}
	t.windows.ObserveBatch(t.observed)
	t.observed = t.observed[:0]
}

// canPlace reports whether an event time is one this engine can put somewhere
// in event time: at or after EventTimeFloor, and no further ahead of its own
// clock than EventTimeCeiling.
//
// A source that stamps nothing leaves the field zero, which is below the
// floor. Such a pipeline has no event time to window on, so refusing its
// records would refuse all of them; zero is therefore placeable, and the
// window's own time column is what decides where those rows land. A source
// that does assign but found nothing usable on this record stamps
// EventTimeMissing instead, which is not placeable: the operator said where
// the time is, and this record has none there.
//
// nowNanos is the batch's one wall-clock reading, the same one the lag
// reading uses, rather than the injected clock: this is a judgement about
// whether a producer's clock is believable, made against the host's, and a
// simulator's frozen clock is not the host's. A simulated stream is one that
// happened, and its event times sit in the real past.
func (t *Turbine) canPlace(atNanos, nowNanos int64) bool {
	return CanPlace(atNanos, nowNanos)
}

// CanPlace is the placement rule itself, exported so a harness can predict
// what the engine will refuse with the engine's own code rather than a copy
// of it. Zero -- a source that stamps nothing -- is placeable; see canPlace.
//
// The window is [EventTimeFloor, now + EventTimeCeiling]. The ceiling is not
// zero on purpose: a stamp written upstream and read downstream is ahead of
// the reader's clock whenever the writer's leads it, which ordinary clock
// disagreement produces without anything being wrong. See EventTimeCeiling.
func CanPlace(atNanos, nowNanos int64) bool {
	if atNanos == 0 {
		return true
	}
	return atNanos >= eventTimeFloorNanos && atNanos <= nowNanos+int64(EventTimeCeiling)
}

// noteUnplaceable counts a refused record and logs the condition once per
// run, so one device with a wrong clock writes a line rather than a line per
// message for as long as it keeps sending. The counter is what carries the
// ongoing rate.
func (t *Turbine) noteUnplaceable(atNanos int64, now time.Time) {
	t.metrics.MessagesUnplaceable.Add(context.Background(), 1)
	if t.unplaceableLogged {
		return
	}
	t.unplaceableLogged = true
	t.logger.Warn("refusing records whose event time this engine cannot place",
		zap.Time("event_time", time.Unix(0, atNanos).UTC()),
		zap.Time("engine_clock", now.UTC()),
		zap.Time("floor", EventTimeFloor),
		zap.Time("ceiling", now.UTC().Add(EventTimeCeiling)),
		zap.Duration("ahead_by", time.Unix(0, atNanos).Sub(now)))
}
