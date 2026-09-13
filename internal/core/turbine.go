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
	Value     []byte
	Topic     string
	Partition int32
	Offset    int64
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
	progress progressSaver
	// snapshot is the in-memory copy of what progress last recorded, read by
	// /stats and /healthz without touching the database. Guarded by lock.
	snapshot Progress
	// arrivedAt is when the newest batch reached the handler, set by
	// processBatch. It is the arrival time itself rather than a flag,
	// because a throttled write can happen long after the arrival and
	// stamping it with the commit clock would place the arrival wherever the
	// write landed. Guarded by lock.
	arrivedAt time.Time
	// writtenArrival is the arrival the table already holds, so a skipped
	// write is retried on the next one rather than lost. Guarded by lock.
	writtenArrival time.Time
	// commits counts successful state commits, for tests that wait on ticks.
	// Guarded by lock.
	commits int64
	// progressWrittenAt is when the progress table was last written, and
	// progressEvery is how often it may be. Guarded by lock.
	progressWrittenAt time.Time
	progressEvery     time.Duration

	// stateStats reads a snapshot of durable state for the gauges. It reads a
	// connection dedicated to reading, never the one batches are written on,
	// so a scrape cannot stall the pipeline. Nil when there is no state
	// database.
	stateStats func() (*StateStats, error)

	// marks is the last position finished with, per topic and partition; what
	// commitSource hands a MarkCommitter.
	marks       *Marks
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

	// lagAttrCache keeps one attribute set per topic and partition, so the
	// per-message lag metric costs no allocation. Touched only by mark, on
	// the consume-loop goroutine.
	//
	// The cached value is the variadic slice itself, not just the option:
	// passing options one by one reallocates the slice on every call.
	lagAttrCache map[lagKey][]metric.RecordOption

	// lagPending holds the newest lag seen per topic and partition since the
	// last recordLag. A gauge is a last value, so recording it on every
	// message spends a real instrument call to say what the final message of
	// the fetch says anyway: 18% of throughput on the 10M benchmark once the
	// default provider stopped being a noop. Same goroutine rule as the cache.
	lagPending map[lagKey]int64

	// Built once at startup for the same reason as lagAttrCache. Typed as
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
func WithProgressStore(s progressSaver) TurbineOption {
	return func(t *Turbine) { t.progress = s }
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
// this is only the SQL-visible copy, whose one reader compares it against a
// grace measured in tens of seconds.
const progressWriteInterval = time.Second

// recordProgress runs at the top of every commit, before the state guard.
//
// Two audiences, and they are not the same requirement. The snapshot is what
// /stats and /healthz read, so every pipeline needs it whether or not it has
// a state database, and it costs an assignment. The table is what SQL reads,
// today only the tumbling window predicate, and it costs a statement on the
// commit path.
//
// The table is written on every pipeline even where nothing reads it. That is
// deliberate: a table that exists but silently stops being maintained is a
// worse trap than one that costs a little, and a window can be managed
// without a state path, so "has state" is not the test for whether anyone
// reads it.
//
// A batch since the last commit moves the arrival clock; an idle tick moves
// the commit clock only.
func (t *Turbine) recordProgress(ctx context.Context) {
	if t.progress == nil {
		return
	}
	now := time.Now().UTC()
	t.lock.Lock()
	p := Progress{LastCommit: now, Messages: t.stats.MessagesConsumed()}
	if !t.arrivedAt.IsZero() {
		p.LastArrival = t.arrivedAt
		t.snapshot.LastArrival = t.arrivedAt
	}
	t.snapshot.LastCommit = now
	t.snapshot.Messages = p.Messages

	// Due on the clock, or owing an arrival the table has not got yet.
	// The second half is the liveness guarantee: the newest arrival always
	// reaches the table, at the latest on the next commit after the
	// interval, so a stream that stops cannot strand the arrival that
	// decides when its last window closes.
	//
	// The elapsed test is written to survive a clock that moves backwards.
	// Comparing now.Sub(written) >= interval is false forever after a jump
	// back, which would stop the table being written at all.
	elapsed := now.Sub(t.progressWrittenAt)
	owed := !t.arrivedAt.IsZero() && t.arrivedAt.After(t.writtenArrival)
	due := owed || elapsed >= t.progressEvery || elapsed < 0
	if due {
		t.progressWrittenAt = now
		t.writtenArrival = t.arrivedAt
	}
	t.lock.Unlock()

	// The snapshot above is exact and free. The table is not: one UPDATE
	// through ADBC measures about 112 microseconds, and a batch of 5000 at a
	// million messages a second commits two hundred times a second, so
	// writing it on every commit costs a few percent of throughput.
	// BenchmarkCommitState is where that number comes from.
	//
	// So idle ticks that change nothing but the commit clock are skipped
	// until the interval has passed. An arrival is never skipped: owed
	// above forces the write, because the table's last_arrival is what
	// decides when a window closes, and a value older than the truth closes
	// it early. Early is the dangerous direction, since that is the
	// window-splitting behaviour the stream clock exists to prevent.
	if !due {
		return
	}
	if err := t.progress.Record(ctx, p); err != nil {
		t.logger.Warn("recording progress", zap.Error(err))
	}
}

// WithStateStats supplies the snapshot function backing the state gauges. It
// must read a connection dedicated to reading; passing the pipeline's writer
// would let a scrape contend with batch processing.
func WithStateStats(fn func() (*StateStats, error)) TurbineOption {
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
		sink:          sink,
		handler:       handler,
		batchSize:     batchSize,
		flushInterval: flushInterval,
		progressEvery: progressWriteInterval,
		lock:          lock,
		running:       true,
		stats: &Stats{
			StartTime: time.Now().UTC(),
		},
		errorPolicy: policy,

		logger: zap.NewNop(),
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

	stats, err := t.stateStats()
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
	if err := t.handler.Init(ctx); err != nil {
		return nil, err
	}

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
				if err := t.processBatch(ctx, numBatchMessages); err != nil {
					return nil, err
				}
				numBatchMessages = 0
				continue
			}
			// Nothing buffered, but an idle stateful pipeline still has to
			// close its open transaction. DuckDB's now() returns the
			// transaction's start time, so a transaction left open freezes
			// the clock every window predicate is evaluated against, and a
			// pipeline that stops receiving messages stops closing windows --
			// silently, with the rows still reported as live state. This tick
			// is also what makes a table manager's deletes durable while no
			// messages are arriving.
			if err := t.commitState(ctx); err != nil {
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
			// The drain runs on the budget's context, not the cancelled one.
			// Every step below reaches DuckDB and the sink, and the cancelled
			// ctx would fail the exact work the drain exists to finish. The
			// budget bounds it instead: a supervisor gives a stop a fixed time
			// and then kills the process with nothing recorded.
			if numBatchMessages > 0 {
				drainCtx := t.drain.Context()
				if err := t.processBatch(drainCtx, numBatchMessages); err != nil {
					if drainCtx.Err() != nil {
						err = errs.Wrap(errs.CodeDrainIncomplete, err,
							"drain deadline %s reached with %d messages buffered",
							t.drain.Deadline(), numBatchMessages)
					}
					t.recordError(ctx, err, phaseSinkFlush, "error draining the final batch")
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
		t.metrics.MessageCount.Add(ctx, int64(len(msgBatch)))
		// Once per batch, not per message: the cost is one gauge record
		// against a whole batch, and a reader asking "is it still doing
		// anything" cannot tell the two apart.
		t.metrics.PipelineLastMessage.Record(ctx, time.Now().Unix())

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
				// Check for cancellation at batch boundaries
				select {
				case <-ctx.Done():
					t.logger.Warn("context done, stopping consumer loop")
					t.running = false
					t.logThroughput()
					return t.stats, nil
				default:
				}

				p0 := time.Now()
				if err := t.processBatch(ctx, numBatchMessages); err != nil {
					return nil, err
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
		if err := t.processBatch(ctx, numBatchMessages); err != nil {
			return nil, err
		}
	}

	t.logThroughput()
	return t.stats, nil
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
// Cached for the same reason lagAttrs is: metric.WithAttributes allocates on
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
	for key, lag := range t.lagPending {
		t.metrics.ConsumerLag.Record(ctx, lag, t.lagAttrs(key.topic, key.partition)...)
		delete(t.lagPending, key)
	}
}

// lagAttrs returns the cached attribute set for one topic and partition.
//
// Building it inline costs an allocation per message whatever the metrics
// configuration: metric.WithAttributes allocates before any provider decides
// to discard the measurement. Benchmarked against a noop provider, that took
// mark from 17.5 ns and no allocations to 224 ns and four -- around 19% of a
// core at the throughput this engine advertises, spent on garbage.
//
// A plain map needs no lock: mark runs only on the consume-loop goroutine.
func (t *Turbine) lagAttrs(topic string, partition int32) []metric.RecordOption {
	key := lagKey{topic: topic, partition: partition}
	if opts, ok := t.lagAttrCache[key]; ok {
		return opts
	}
	opts := []metric.RecordOption{metric.WithAttributes(
		attribute.String("topic", topic),
		attribute.Int("partition", int(partition)),
	)}
	if t.lagAttrCache == nil {
		t.lagAttrCache = make(map[lagKey][]metric.RecordOption)
	}
	t.lagAttrCache[key] = opts
	return opts
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

// rollbackState discards this batch's uncommitted state writes. Used on the
// paths that fail before commitState is reached.
func (t *Turbine) rollbackState(ctx context.Context) {
	if t.stateTx == nil {
		return
	}
	t.lock.Lock()
	defer t.lock.Unlock()
	// Without cancellation. A rollback is local and must happen: on a drain
	// that ran out of time the caller's context has already expired, and a
	// rollback refused on it leaves this batch's writes in the transaction
	// for the next state sync to commit without their offsets.
	if err := t.stateTx.Rollback(context.WithoutCancel(ctx)); err != nil {
		t.logger.Error("rollback failed", zap.Error(err))
	}
}

// commitState writes the processed offsets into the state database and commits
// them together with whatever the handler wrote in this batch. Any failure
// rolls the whole transaction back, leaving the durable offsets where they
// were so the batch is replayed rather than lost.
//
// A pipeline with no state database does nothing here.
func (t *Turbine) commitState(ctx context.Context) error {
	t.recordProgress(ctx)

	if t.offsets == nil || t.stateTx == nil {
		return nil
	}

	c0 := time.Now()

	t.lock.Lock()
	defer t.lock.Unlock()

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
	t.metrics.StateCommitLatency.Record(ctx, time.Since(c0).Seconds())
	t.metrics.StateCommitCount.Add(ctx, 1, t.resultAttrs(resultOK)...)
	// Success only. The error path below records the dimensioned series and
	// not this one, because a failed commit is not a commit.
	t.metrics.PipelineCommits.Add(ctx, 1)
	return nil
}

// SyncState closes the open state transaction, making everything written
// since the last commit durable. A pipeline with no state database does
// nothing.
//
// Shutdown uses it twice: once before the table managers run their final
// poll, so that poll sees a current clock rather than one frozen at the last
// batch, and once after, so the rows that poll published are actually deleted
// instead of being rolled back when the connection closes and republished on
// the next start.
func (t *Turbine) SyncState(ctx context.Context) error {
	return t.commitState(ctx)
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

	t.lock.Lock()
	batch, err := t.handler.Invoke(ctx)
	// Messages reached the handler whatever Invoke returns, so this is an
	// arrival. Stamped now rather than at commit time: a throttled write can
	// land much later, and the window predicate needs when the data came,
	// not when the row was updated.
	t.arrivedAt = time.Now().UTC()
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
	c0 := time.Now()
	err = t.commitState(ctx)
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
	}

	b3 := time.Now()

	if batch != nil {
		batch.Release()
	}

	i0 := time.Now()
	err = t.handler.Init(ctx)
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

func (t *Turbine) flush(ctx context.Context, batch arrow.Table) error {
	if err := t.sink.Flush(ctx); err != nil {
		t.recordError(ctx, err, phaseSinkFlush, "flush error")
		return err
	}
	return nil
}
