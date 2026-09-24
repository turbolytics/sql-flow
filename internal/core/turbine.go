package core

import (
	"context"
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
var eventTimeFloorNanos = time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano()

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
// all: a Kafka consumer holding partitions, a websocket that is connected.
//
// The engine counts quiet only while it waits on a source that could have
// delivered. A consumer rejoining its group after a crash waits for the
// session timeout, 45 seconds by default, before it holds anything, and
// with idle_close_seconds below that every bucket saved in the state
// database closed before the backlog arrived; the backlog's rows were then
// late. Delivering reports whether the source can deliver now and for how
// long it has been able to, and the engine's quiet clock never runs
// earlier than that. It is a duration rather than an instant so that no
// implementer can hand back a wall-clock reading with the monotonic clock
// stripped, which would put the clock-step bug back on this path. A source
// that does not implement this is taken to be delivering whenever it is
// running.
//
// The trade-off: a source that can never deliver, a consumer in a group
// with more members than partitions or one whose topic is gone, holds the
// quiet clock at zero for as long as that lasts, and no window closes on
// idleness meanwhile. Before this it closed, too early. The engine logs
// when a source stops delivering and when it resumes, with how long it was
// out, so a hold that never ends is visible.
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
	// quietSince is the last instant this process was doing anything other
	// than waiting on the source: seeded when the turbine is built and again
	// when the loop starts, and stamped at the end of every batch, after the
	// sink write. The progress row confirms quiet from here to the commit,
	// and that is the only quiet this process watched. A restart, a sink
	// held in retries and a wall clock stepping forward are not quiet, and
	// each was read as quiet once: the outage between a previous process's
	// last arrival and this one's first tick, the hold between an arrival
	// stamped before the write and the commit after it, and a step between
	// two wall-clock readings. It keeps its monotonic reading, so the
	// elapsed against it is measured on the monotonic clock; UTC() would
	// strip that. Guarded by lock.
	quietSince time.Time
	// notDeliveringSince is when the source last reported it could not
	// deliver, zero while it can. Only for the two log lines. Guarded by
	// lock.
	notDeliveringSince time.Time
	// commits counts successful state commits, for tests that wait on ticks.
	// Guarded by lock.
	commits int64
	// progressWrittenAt is when the progress table was last written, and
	// progressEvery is how often it may be. Guarded by lock.
	progressWrittenAt time.Time
	progressEvery     time.Duration
	// confirmsQuiet is whether idle ticks write the row; see
	// WithQuietConfirmation.
	confirmsQuiet bool

	// stateStats reads a snapshot of durable state for the gauges. It reads a
	// connection dedicated to reading, never the one batches are written on,
	// so a scrape cannot stall the pipeline. Nil when there is no state
	// database.
	stateStats func(context.Context) (*StateStats, error)

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

// WithQuietConfirmation says whether idle ticks write the progress row.
// The row's job between batches is to confirm a quiet stream to a window
// with idle_close_seconds, and a pipeline with no such window has no
// reader: its idle ticks then do no accounting at all, no statement, no
// WAL append, no fsync, which on a box that runs its state from an SD card
// is the difference between a quiet pipeline and a busy one. Batches still
// write on the interval, and the drain still writes once. On by default.
func WithQuietConfirmation(on bool) TurbineOption {
	return func(t *Turbine) { t.confirmsQuiet = on }
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

// commitCount is how many state commits have succeeded; tests wait on it.
func (t *Turbine) commitCount() int64 {
	t.lock.Lock()
	defer t.lock.Unlock()
	return t.commits
}

// progressWriteInterval bounds how often sqlflow_progress is written. The
// snapshot behind /stats and /healthz is updated on every commit regardless;
// this is only the SQL-visible copy, whose one reader in the engine compares
// its two clocks against an idle bound measured in tens of seconds.
const progressWriteInterval = time.Second

// arrivalAnnounceAfter is how much silence has to be standing before a batch
// announces that it ended it; see announceArrival.
//
// One second because idle_close_seconds is whole seconds, so the smallest
// bound anyone can configure is one: a window can never close on a silence
// shorter than this, and a batch that follows one shorter has nothing to
// correct. Tying it to the write interval instead made the announcement fire
// on every batch wherever that interval was zero, which moved the arrival
// clock twice for one batch and left a reader free to catch either stamp.
const arrivalAnnounceAfter = time.Second

// holdQuietWhileNotDelivering keeps the quiet clock from running over time
// the source could not deliver. A source that is not delivering resets it
// to now; one that resumed since the clock was last set moves it to the
// resumption, so the tick after a rebalance ends confirms quiet from the
// assignment rather than from the loop's last stamp. Called before every
// commit that is not a batch's: the idle tick, and the drain, where a
// forced write during a rebalance would otherwise span it.
//
// The transitions are logged once each, so a source that never resumes
// shows up as a stop with no resumption after it.
func (t *Turbine) holdQuietWhileNotDelivering() {
	d, ok := t.source.(Deliverer)
	if !ok {
		return
	}
	deliveringFor, delivering := d.Delivering()
	now := time.Now()

	// The transition is decided under the lock and logged after it: the
	// lock is the connection's, which the debug API also takes, and a log
	// line is I/O.
	var stopped bool
	var outage time.Duration
	t.lock.Lock()
	switch {
	case !delivering:
		if t.notDeliveringSince.IsZero() {
			t.notDeliveringSince = now
			stopped = true
		}
		t.quietSince = now
	default:
		if !t.notDeliveringSince.IsZero() {
			outage = now.Sub(t.notDeliveringSince)
			t.notDeliveringSince = time.Time{}
		}
		if since := now.Add(-deliveringFor); since.After(t.quietSince) {
			t.quietSince = since
		}
	}
	t.lock.Unlock()

	if stopped {
		t.logger.Warn("source is not delivering; no window closes on idleness until it does")
	}
	if outage > 0 {
		t.logger.Info("source is delivering again", zap.Duration("not_delivering_for", outage))
	}
}

// recordProgress runs at the top of every commit, before the state guard.
//
// Two audiences, and they are not the same requirement. The snapshot is what
// /stats and /healthz read, so every pipeline needs it whether or not it has
// a state database, and it costs an assignment. The table is what SQL reads,
// and it costs a statement on the commit path. The engine reads it in one
// place, the idle-close branch of the watermark predicate; handler SQL,
// emit_sql, a sqlcommand sink and /debug can read it too.
//
// The table is written on every pipeline, including those where the engine
// never reads it: a table that exists but silently stops being maintained is
// a worse trap than one that costs a little. It is written on the interval,
// and forced by the drain. A commit inside the interval of the last write
// skips it, and nothing is lost by the skip: every write carries the arrival
// clock as it stands, so the next write reports the same arrival at its true
// time, and a reader measures the quiet from the right instant.
//
// The interval used to be overridden by any commit carrying a newer arrival,
// which under load is every commit: a statement per batch, 8 percent of
// throughput at batch 5000 and a quarter at batch 500. That existed for a
// reader that compared last_arrival against its own clock, for which a stale
// arrival meant an early close. The reader now compares the row's two clocks
// against each other (managers.nextWatermark), so a late write is a late
// close, never an early one, and the interval is enough.
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
	now := time.Now()
	// Held through the write below, because the debug API runs statements on
	// this same connection and DuckDB closes a pending result the moment
	// another statement runs on it. The window managers are not a party: they
	// poll on connections of their own and never take this lock.
	t.lock.Lock()
	defer t.lock.Unlock()
	// The commit clock is the arrival clock plus the monotonic elapsed, not
	// a second wall-clock reading: the row's difference is then what the
	// monotonic clock measured, and a wall clock stepped forward between the
	// two is not read as quiet. The arrival itself is written as stamped, so
	// it is the same value on every write until the next batch.
	p := Progress{
		LastArrival: t.quietSince,
		LastCommit:  t.quietSince.Add(now.Sub(t.quietSince)),
		Messages:    t.stats.MessagesConsumed(),
	}
	t.snapshot.LastArrival = p.LastArrival
	t.snapshot.LastCommit = p.LastCommit
	t.snapshot.Messages = p.Messages

	// The clock term is load-bearing for windows, not only housekeeping. An
	// idle tick is due on it alone, and that write, a later last_commit
	// against the same last_arrival, is the only thing that confirms to a
	// window manager that the stream is quiet. Without it no window would
	// ever close on idleness.
	//
	// Both stamps carry monotonic readings, so a wall clock stepping back
	// cannot make this negative. The guard stays for a stamp without one,
	// which a test can inject: comparing now.Sub(written) >= interval alone
	// would then be false for as long as the step was, the table would stop
	// being written, and with it every window's idle close.
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
	// write reports it, at shutdown the drain forces one, and until a write
	// succeeds the row confirms no quiet, so no window closes on it.
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
		source:        source,
		marks:         NewMarks(),
		committed:     NewMarks(),
		sink:          sink,
		handler:       handler,
		batchSize:     batchSize,
		flushInterval: flushInterval,
		progressEvery: progressWriteInterval,
		quietSince:    time.Now(),
		confirmsQuiet: true,
		lock:          lock,
		running:       true,
		stats: &Stats{
			StartTime: time.Now().UTC(),
		},
		errorPolicy: policy,

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
		t.logger.Info("closing source from ConsumeLoop",
			zap.Bool("running", t.running),
			zap.String("here", "here"),
			zap.Error(err),
		)

		if err := t.source.Close(); err != nil {
			panic(err)
		}
	}()

	t.stats.StartTime = time.Now().UTC()
	t.stats.SetNumMessagesConsumed(0)
	// The quiet the row may confirm starts here, not at the previous
	// process's last arrival, which the row still carries.
	t.lock.Lock()
	t.quietSince = time.Now()
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
	if t.flushInterval > 0 {
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
			// Nothing buffered. This tick's job is the progress write: a
			// commit with a later last_commit against the same last_arrival
			// is the only thing that confirms to a window manager that the
			// stream is quiet, and without it no window would ever close on
			// idleness. With a state path it also ends the batch transaction
			// left open since the last commit, so nothing stays uncommitted
			// on the connection for as long as the stream is silent.
			//
			// Where no window closes on idleness nothing reads the row
			// between batches, and the tick writes nothing: that is the
			// accounting a pipeline that is not a windowed stream does not
			// pay for.
			write := progressOnInterval
			if t.confirmsQuiet {
				t.holdQuietWhileNotDelivering()
			} else {
				write = progressSkipped
			}
			if err := t.commitState(batchCtx, write); err != nil {
				t.recordError(ctx, err, phaseStateCommit, "error committing state on idle tick")
				return nil, err
			}
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
			// After now is a clock ahead of this host's, not a pipeline
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

		for _, raw := range msgBatch {
			if err := t.writeMessage(raw); err != nil {
				t.recordError(ctx, err, phaseHandlerWrite, "error writing message")

				if err := t.applyErrorPolicy(ctx, err, phaseHandlerWrite, string(raw.Value)); err != nil {
					return nil, err
				}
				// The message is dropped from the batch, but it was still
				// consumed from the source: it counts toward the reported
				// total and toward --max-msgs, as in the Python engine -- and
				// its position is finished with, so it is safe to commit past.
				t.mark(raw)
				totalConsumed++
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
// logging belong to recordError, which every caller has already run.
func (t *Turbine) applyErrorPolicy(ctx context.Context, cause error, phase, message string) error {

	switch t.errorPolicy.Policy {
	case PolicyIgnore:
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

// commitSource commits what the pipeline has processed. A source that can
// take explicit marks gets exactly the positions this pipeline has finished
// with; anything else gets the plain Commit it always did.
//
// The distinction is not academic. The Kafka source reads ahead of the
// pipeline into a buffer, and its plain Commit commits everything it has
// fetched: after one 20,000-message batch it had committed offset 70,086.
// Whatever sat in that buffer when the process died was gone for good, with
// the consumer group showing no lag.
func (t *Turbine) commitSource() error {
	if mc, ok := t.source.(MarkCommitter); ok {
		if t.marks.Empty() {
			return nil
		}
		return mc.CommitMarks(t.marks)
	}
	return t.source.Commit()
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
}

// commitState writes the processed offsets into the state database and commits
// them together with whatever the handler wrote in this batch. Any failure
// rolls the whole transaction back, leaving the durable offsets where they
// were so the batch is replayed rather than lost.
//
// A pipeline with no state database commits nothing here; its progress write
// autocommits by itself.
func (t *Turbine) commitState(ctx context.Context, write progressWrite) error {
	progressErr := t.recordProgress(ctx, write)

	if t.offsets == nil || t.stateTx == nil {
		if progressErr != nil {
			// The write autocommitted by itself, so the batch is untouched
			// and the pipeline carries on. What stops is the idle close: it
			// acts on what this row confirms, so while the write fails no
			// window closes on idleness and a quiet stream's last buckets
			// wait. It gets its own code so an alert can tell it from a state
			// commit that failed, which means the pipeline is stopping.
			t.recordError(ctx, errs.Wrap(errs.CodeProgressWriteFailed, progressErr,
				"sqlflow_progress not written"), phaseStateCommit, "sqlflow_progress not written")
		}
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
		return errs.Wrap(errs.CodeStateCommitFailed, progressErr,
			"the progress write failed inside the state transaction")
	}

	if err := t.offsets.Save(ctx, t.marks); err != nil {
		if rbErr := t.stateTx.Rollback(context.WithoutCancel(ctx)); rbErr != nil {
			t.logger.Error("rollback after failed offset save", zap.Error(rbErr))
		}
		return errs.Wrap(errs.CodeStateCommitFailed, err, "saving offsets")
	}

	if err := t.stateTx.Commit(ctx); err != nil {
		// The commit itself failed, so the transaction is still open and
		// still holds this batch's writes; roll it back explicitly rather
		// than leaving them to leak into the next batch.
		if rbErr := t.stateTx.Rollback(context.WithoutCancel(ctx)); rbErr != nil {
			t.logger.Error("rollback after failed commit", zap.Error(rbErr))
		}
		return errs.Wrap(errs.CodeStateCommitFailed, err, "committing state")
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
// since the last commit durable, and forces the progress write whether or not
// the interval has come due. A pipeline with no state database has no
// transaction to close, but its progress write is still forced, because a
// window's final poll reads it with or without a state path.
//
// Shutdown uses it twice: once before the table managers run their final
// poll, so that poll reads a progress row confirming the quiet up to the
// signal rather than up to the last interval write, and once after, so the
// batch transaction is closed with everything the drain wrote. The managers
// commit their own transactions on their own connections; neither call is
// for them. The second forced write is one statement, and keeping
// SyncState's contract the same both times is worth more than saving it.
func (t *Turbine) SyncState(ctx context.Context) error {
	// The drain forces the progress write. root.go calls this before the
	// managers' final poll, and that poll closes on idleness only for the
	// quiet the row confirms. The last regular write was up to an interval
	// ago, or failed, and either way the quiet since then is unconfirmed:
	// a stream that stopped just under idle_close before the signal would
	// leave the poll with buckets open that a clean shutdown should close.
	//
	// And the same hold the idle tick applies: a signal during a rebalance
	// or a reconnect forced a write whose quiet spanned it, the final poll
	// closed every open bucket, and the backlog after the restart was late.
	t.holdQuietWhileNotDelivering()
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

// announceArrival ends the quiet the progress row confirms, before this
// batch's writes can be seen, so that no reader ever pairs new rows with the
// silence that preceded them.
//
// Only where the handler's writes autocommit on their own. With a state path
// they are invisible until the batch's transaction commits, and the progress
// write rides that same transaction, so a reader sees both or neither and
// there is nothing to announce.
//
// It costs at most one write per waking, because it stamps the quiet clock
// as it goes: a stream whose batches are closer together than
// arrivalAnnounceAfter never reaches the threshold and pays nothing at all,
// and a slower one pays a single UPDATE beside the idle ticks it is already
// paying for over the same second. Understating the quiet is the safe
// direction -- it delays a close, it never brings one forward.
func (t *Turbine) announceArrival(ctx context.Context) {
	// Nobody reads this row for a close, so no reading of it can be early:
	// a pipeline whose idle ticks write nothing pays nothing here either.
	if t.progress == nil || !t.confirmsQuiet || (t.offsets != nil && t.stateTx != nil) {
		return
	}

	now := time.Now()
	t.lock.Lock()
	quiet := now.Sub(t.quietSince)
	announce := quiet >= arrivalAnnounceAfter
	if announce {
		t.quietSince = now
	}
	t.lock.Unlock()

	if !announce {
		return
	}
	if err := t.recordProgress(ctx, progressForced); err != nil {
		// The batch is untouched: the row simply still confirms the silence
		// this batch ended, and a window closing on it closes early. Same
		// failure, same code, as the throttled write reports.
		t.recordError(ctx, errs.Wrap(errs.CodeProgressWriteFailed, err,
			"sqlflow_progress not written"), phaseStateCommit, "sqlflow_progress not written")
	}
}

func (t *Turbine) processBatch(ctx context.Context, numBatchMessages int) error {
	// The waking is written before the rows it woke with can be seen. A
	// windowed pipeline's handler writes this batch into the window table
	// inside Invoke, and with no state path that INSERT autocommits as it
	// runs -- while the arrival clock is stamped only after the sink has
	// flushed, and that write is throttled besides. A window manager polling
	// in between reads a burst's first rows beside the silence they ended,
	// closes the bucket on an idle bound the burst has already broken, and
	// drops every later row of the same burst as late.
	t.announceArrival(ctx)

	b0 := time.Now()

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

		if policyErr := t.applyErrorPolicy(ctx, err, phaseHandlerInvoke, "Handler invocation failed"); policyErr != nil {
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
	// The batch is done with the sink, so from here the loop is back to
	// waiting on the source, and that is where the quiet the row may confirm
	// starts. Stamped before the write, not the handler: the time inside a
	// sink held in retries was not watched, and messages may have waited at
	// the source through all of it.
	t.lock.Lock()
	t.quietSince = time.Now()
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
