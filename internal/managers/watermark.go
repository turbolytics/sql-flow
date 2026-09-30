// Package managers holds the loops the engine runs beside a pipeline for the
// lifetime of a run, rather than per batch. Today that is one kind: a
// watermark manager per declared window.
package managers

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/turbolytics/sql-flow/internal/core"
	"github.com/turbolytics/sql-flow/internal/errs"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	"go.uber.org/zap"
)

// Declaration is a window as the config declares it: which table, which
// column holds the bucket start, how long a bucket is, and how long a closed
// bucket is kept.
type Declaration struct {
	Table      string
	TimeColumn string
	Size       time.Duration
	Grace      time.Duration
	// IdleClose is how long a source partition may be silent before it stops
	// holding the window open. Zero means never. The engine reads it; the
	// manager carries it only so one declaration describes the window.
	IdleClose time.Duration
	// Lateness is how long after a bucket closes its rows are kept, and a
	// late row the engine admits republishes it whole. Zero: the rows are
	// deleted in the pass that publishes them, and the engine refuses late
	// rows before they arrive. Flink's allowedLateness.
	Lateness time.Duration
	// EmitSQL shapes the closed rows for the sink. Empty means SELECT * FROM
	// closed.
	EmitSQL string
}

func (d Declaration) validate() error {
	switch {
	case d.Table == "":
		return errs.New(errs.CodeConfigInvalid, "window: table is required")
	case d.TimeColumn == "":
		return errs.New(errs.CodeConfigInvalid, "window %s: time_column is required", d.Table)
	case d.Size <= 0:
		return errs.New(errs.CodeConfigInvalid, "window %s: size_seconds must be positive", d.Table)
	case d.Grace < 0:
		return errs.New(errs.CodeConfigInvalid, "window %s: grace_seconds cannot be negative", d.Table)
	case d.IdleClose < 0:
		return errs.New(errs.CodeConfigInvalid, "window %s: idle_close_seconds cannot be negative", d.Table)
	case d.Lateness < 0:
		return errs.New(errs.CodeConfigInvalid, "window %s: allowed_lateness_seconds cannot be negative", d.Table)
	case core.IsEngineTable(d.Table):
		return errs.New(errs.CodeConfigInvalid, "window %s: an engine table cannot carry a window", d.Table)
	}
	return nil
}

// Watermark closes one window. The engine asserts the window's watermark in
// sqlflow_watermarks, in the commit that makes the rows it describes
// visible (core.Watermarks), and tells the manager through the window's
// signal when it moved and which closed buckets a late row landed in. The
// manager keeps what it has closed up to in sqlflow_windows. A pass moves
// the second to the first and publishes the buckets between them,
// republishes the buckets the signal named, and deletes the buckets past
// their lateness. Neither watermark moves backwards.
//
// The manager has no clock, no ticker and no interval. It runs a pass on
// start, on every kick, and on the drain. Every decision is `bucket end <=
// watermark`, in event time; the engine's clock decides only which
// partitions are in its minimum, and no reading of any clock reaches here.
//
// It runs on a connection of its own, with autocommit off, so it reads
// committed rows only and its deletes commit together with its watermark.
// Nothing here shares the pipeline's lock or transaction.
type Watermark struct {
	conn   adbc.Connection
	tx     transaction
	decl   Declaration
	store  *Store
	sink   core.Sink
	signal *core.WindowSignal

	logger  *zap.Logger
	drain   *core.DrainBudget
	metrics WindowMetrics
	attrs   metric.MeasurementOption

	// republishRetained is set for the start pass: every bucket still
	// retained is republished whole, because a late row that landed before a
	// restart left its recompute in memory only.
	republishRetained bool
}

// transaction is the boundary on the manager's connection. An ADBC
// connection with autocommit off satisfies it.
type transaction interface {
	Commit(context.Context) error
	Rollback(context.Context) error
}

type Option func(*Watermark)

func WithLogger(l *zap.Logger) Option {
	return func(w *Watermark) { w.logger = l.Named("manager.watermark") }
}

// WithDrainBudget bounds the final pass after a cancel.
func WithDrainBudget(b *core.DrainBudget) Option {
	return func(w *Watermark) { w.drain = b }
}

// WithMeterProvider supplies the provider the window instruments record
// through. Without one they record nothing.
func WithMeterProvider(mp metric.MeterProvider) Option {
	return func(w *Watermark) { w.metrics = NewWindowMetrics(mp, w.decl.Table) }
}

// NewWatermark builds the manager for one window on conn, which must be a
// connection of its own with autocommit off: every pass ends with a commit
// or a rollback on it. signal is the window's, from core.Watermarks; nil is
// allowed for a caller that drives Pass itself, and Start then runs its
// start pass and waits for the drain.
func NewWatermark(conn adbc.Connection, d Declaration, sink core.Sink, signal *core.WindowSignal, opts ...Option) (*Watermark, error) {
	if err := d.validate(); err != nil {
		return nil, err
	}
	tx, ok := conn.(transaction)
	if !ok {
		return nil, errs.New(errs.CodeStateInternal, "window %s: the connection does not support transactions", d.Table)
	}

	w := &Watermark{
		conn:    conn,
		tx:      tx,
		decl:    d,
		store:   NewStore(conn),
		sink:    sink,
		signal:  signal,
		logger:  zap.NewNop(),
		metrics: NewWindowMetrics(nil, d.Table),
		attrs:   metric.WithAttributes(attribute.String("window", d.Table)),
	}
	for _, opt := range opts {
		opt(w)
	}
	if w.drain == nil {
		w.drain = core.NewDrainBudget(core.DefaultDrainDeadline)
	}
	return w, nil
}

// Declaration is what the manager was built from.
func (w *Watermark) Declaration() Declaration { return w.decl }

// Start runs one pass, then one on every kick, then one on the drain. It has
// no clock: the engine kicks when the watermark moved or a late row landed,
// and the engine's own flush tick is the only timer in the design. A
// context already cancelled runs the drain pass only, which is the shape a
// shutdown that raced startup has.
//
// The pass on start is what a restart needs and nothing more: buckets the
// watermark had passed are published, and every retained bucket is
// republished whole, since the recompute set a late row landed in before the
// crash was in memory. Bounded by lateness over size buckets per key, and
// idempotent because the value is the whole bucket.
//
// A failed pass returns. The rows are still in the table, because a pass
// deletes only after the sink accepted them, so a restart republishes the
// same bucket. Start does not retry in place: the sink already ran its retry
// ladder before the error reached here, so what arrives is a destination
// that rejected the rows or stayed unreachable past the deadline. The one
// exception is a write conflict with the pipeline, which the next kick
// retries.
func (w *Watermark) Start(ctx context.Context) error {
	w.logger.Info("starting watermark manager", zap.String("table", w.decl.Table))
	if ctx.Err() != nil {
		return w.finalPass()
	}
	if err := w.StartPass(ctx); err != nil && ctx.Err() == nil && !isConflict(err) {
		w.logger.Error("start pass failed, stopping the manager", zap.Error(err))
		return fmt.Errorf("watermark manager %s: %w", w.decl.Table, err)
	}

	var wake <-chan struct{}
	if w.signal != nil {
		wake = w.signal.Wait()
	}
	for {
		select {
		case <-wake:
			err := w.Pass(ctx)
			if err != nil && ctx.Err() == nil {
				if isConflict(err) {
					w.logger.Warn("pass conflicted with the pipeline, retrying on the next kick", zap.Error(err))
					continue
				}
				w.logger.Error("pass failed, stopping the manager", zap.Error(err))
				return fmt.Errorf("watermark manager %s: %w", w.decl.Table, err)
			}
			if ctx.Err() == nil {
				continue
			}
			// The cancel landed during that pass. The final pass below is
			// the one that counts.
		case <-ctx.Done():
		}
		return w.finalPass()
	}
}

// finalPass publishes what closed since the last kick, on the drain budget.
// A pass the deadline ended is reported as the drain running out of time,
// not as the sink's own failure: the rows are still in the table, and the
// next start publishes them.
func (w *Watermark) finalPass() error {
	err := w.Pass(w.drain.Context())
	if err == nil {
		return nil
	}
	if w.drain.Exceeded() {
		err = errs.Wrap(errs.CodeDrainIncomplete, err,
			"drain deadline %s reached before the final pass finished", w.drain.Deadline())
	}
	w.logger.Error("final pass failed", zap.Error(err))
	return fmt.Errorf("watermark manager %s: final pass: %w", w.decl.Table, err)
}

// StartPass is the pass Start runs first: a Pass that also republishes,
// whole, every bucket still retained under the window's lateness. A late
// row admitted before a restart put its bucket in a recompute set that lived
// in memory; the rows are in the table, so the start pass republishes every
// bucket that could hold one. Bounded by lateness over size buckets per key,
// and idempotent because the value is the whole bucket.
func (w *Watermark) StartPass(ctx context.Context) error {
	w.republishRetained = true
	return w.Pass(ctx)
}

// Pass is one unit of the manager's work: publish every bucket the
// assertion closed, republish every bucket a late row landed in, delete
// every bucket past its lateness, and record where the window has closed up
// to. It ends its transaction before returning, committed or rolled back, so
// the next pass reads a fresh snapshot.
//
// The order inside is the guarantee. Publishes come first and deletes last,
// so a flush that fails leaves every row for the next pass. With no lateness
// the purge is exactly the buckets this pass published, which is the close
// this manager has always made; with lateness they stay until the watermark
// passes their end plus it.
func (w *Watermark) Pass(ctx context.Context) (err error) {
	committed := false
	// What the close lag needs, filled in as the pass learns it. Recorded in
	// the defer so a pass that fails after reading the assertion still
	// reports how far behind it is -- that is the case the gauge exists for.
	var (
		candidate      time.Time
		candidateKnown bool
		settled        time.Time
		settledKnown   bool
		recompute      []time.Time
	)
	defer func() {
		if !committed {
			// Every read opened a transaction, and a transaction left open
			// would freeze the next pass's view of the table.
			if rbErr := w.tx.Rollback(context.WithoutCancel(ctx)); rbErr != nil && err == nil {
				err = fmt.Errorf("rolling back: %w", rbErr)
			}
			// The buckets this pass took from the signal were not published;
			// hand them back so the next pass does them.
			if w.signal != nil {
				for _, b := range recompute {
					w.signal.Recompute(b)
				}
			}
		}
		if candidateKnown && settledKnown {
			w.recordCloseLag(context.WithoutCancel(ctx), candidate, settled)
		}
	}()

	closed, hadClosed, err := w.store.Load(ctx, w.decl.Table)
	if err != nil {
		return err
	}
	if hadClosed {
		settled, settledKnown = closed, true
		// Said on every pass, so an engine that started after the last
		// close still learns it.
		if w.signal != nil {
			w.signal.SetClosed(closed)
		}
	}

	// The one fact: what the engine has asserted.
	asserted, hasAsserted, err := core.LoadWatermark(ctx, w.conn, w.decl.Table)
	if err != nil {
		return fmt.Errorf("reading the asserted watermark: %w", err)
	}
	if hasAsserted {
		candidate, candidateKnown = asserted, true
	}

	// Observation, not decision: where the window's data stands, for the
	// gauges. Nothing below reads it.
	newestMicros, hasRows, err := queryInt64(ctx, w.conn, w.decl.newestSQL())
	if err != nil {
		return fmt.Errorf("reading the newest bucket: %w", err)
	}
	if hasRows {
		w.metrics.NewestStart.Record(ctx, time.UnixMicro(newestMicros).Unix(), w.attrs)
	}
	if !hadClosed && hasRows {
		// Never closed: the first close is due once the watermark reaches the
		// oldest bucket's end, so that is what the lag is measured from.
		// Without it a window whose sink was down from the start would report
		// no lag at all while its rows piled up.
		oldestMicros, ok, err := queryInt64(ctx, w.conn, w.decl.oldestSQL())
		if err != nil {
			return fmt.Errorf("reading the oldest bucket: %w", err)
		}
		if ok {
			settled, settledKnown = time.UnixMicro(oldestMicros).UTC().Add(w.decl.Size), true
		}
	}

	// The buckets a late row landed in since the last pass. Taken before the
	// decision, so a kick that carried only recomputes still does them.
	if w.signal != nil {
		recompute = w.signal.TakeRecompute()
	}

	state := StateOf(asserted, hasAsserted, closed, hadClosed)
	rule := watermarkRuleFor(state)
	watermark, moved := rule.Action.Next(asserted, closed)
	if moved {
		w.logger.Debug("close decided",
			zap.String("rule", rule.Name),
			zap.Stringer("state", state),
			zap.Time("watermark", watermark))
	}
	republish := w.republishRetained && hadClosed && w.decl.Lateness > 0
	if !moved && len(recompute) == 0 && !republish {
		w.republishRetained = false
		return nil
	}
	if !moved {
		watermark = closed
	}

	// Publish what is due: the buckets that ended after the closed
	// watermark and at or before the asserted one. A watermark that moved
	// over an empty stretch still has to be saved, or the next pass decides
	// the same move.
	if moved {
		due := w.decl.dueBetween(closed, hadClosed, watermark)
		rows, _, err := queryInt64(ctx, w.conn, w.decl.countSQL(due))
		if err != nil {
			return fmt.Errorf("counting due rows: %w", err)
		}
		if rows > 0 {
			if err := w.publish(ctx, due); err != nil {
				return err
			}
			w.metrics.Closed.Add(ctx, 1, w.attrs)
			w.logger.Debug("closed", zap.Time("watermark", watermark), zap.Int64("rows", rows))
		}
	}

	// Republish, whole, every bucket a late row landed in. The value the
	// sink receives is emit_sql over every row the bucket has, never the late
	// rows alone, so a sink that replaces by key holds the exact count.
	for _, b := range recompute {
		if err := w.publish(ctx, w.decl.bucketIs(b)); err != nil {
			return err
		}
		w.metrics.Recomputed.Add(ctx, 1, w.attrs)
		w.logger.Debug("recomputed", zap.Time("bucket", b))
	}
	// Then the transaction owns it: nothing hands these back on failure past
	// this point, because the publish already happened.
	recompute = nil

	// The start pass: every bucket closed earlier whose lateness has not run
	// out, republished whole. Retained is measured against the watermark this
	// pass settles on, so a bucket the same pass expires is not republished
	// and then deleted.
	if republish {
		retained := w.decl.retainedBetween(closed, watermark)
		rows, _, err := queryInt64(ctx, w.conn, w.decl.countSQL(retained))
		if err != nil {
			return fmt.Errorf("counting retained rows: %w", err)
		}
		if rows > 0 {
			if err := w.publish(ctx, retained); err != nil {
				return err
			}
			w.logger.Debug("republished retained buckets on start", zap.Int64("rows", rows))
		}
	}

	// Purge what is past its lateness: nothing more can arrive for those
	// buckets, because the engine refuses it. With no lateness this is what
	// the pass just published.
	if _, err := execRows(ctx, w.conn, w.decl.deleteSQL(w.decl.expiredBefore(watermark))); err != nil {
		return fmt.Errorf("deleting expired rows: %w", err)
	}

	// closed_at is the wall clock, for an operator reading the table; no
	// decision reads it back.
	if err := w.store.Save(ctx, w.decl.Table, watermark, time.Now().UTC()); err != nil {
		return err
	}
	if err := w.tx.Commit(ctx); err != nil {
		return errs.Wrap(errs.CodeStateCommitFailed, err, "committing the pass")
	}
	committed = true
	// Only now: the engine lets its source commit pass a bucket's rows once
	// it hears the bucket is closed past its lateness, and that must not
	// happen before the publish and the purge are durable.
	if w.signal != nil {
		w.signal.SetClosed(watermark)
	}
	w.republishRetained = false
	settled, settledKnown = watermark, true
	w.metrics.Watermark.Record(ctx, watermark.Unix(), w.attrs)
	return nil
}

// recordCloseLag records how far the window's closes trail its own data, in
// event time.
//
// candidate is where the watermark should be, given what the engine has
// asserted; settled is where it actually is, committed. The difference is the
// close that is overdue. It is zero whenever a pass commits, and grows while
// the engine's assertion moves on and passes fail -- a sink that is down, a
// transaction that keeps conflicting.
//
// No wall clock enters it. Two earlier readings compared event time with
// this host's clock: the watermark's age, which trailed by size and grace by
// design, and wall time past the next close, which grew for any stream that
// went quiet. Both read a sparse stream as stalled, both were wrong on a
// gateway whose clock was never set, and both needed a close since startup
// to report anything.
func (w *Watermark) recordCloseLag(ctx context.Context, candidate, settled time.Time) {
	lag := int64(candidate.Sub(settled) / time.Second)
	if lag < 0 {
		lag = 0
	}
	w.metrics.CloseLag.Record(ctx, lag, w.attrs)
}

// publish runs emit_sql over the rows where selects and hands the result to
// the sink. Flushed before any delete, so a failure leaves the rows in the
// table to be retried rather than dropping them.
//
// The sink runs on a connection of its own, not this one. This connection
// holds the pass's transaction, and a transaction may write to one database
// only: a sink that writes into an attached Postgres, or stages a batch
// table, would fail it.
func (w *Watermark) publish(ctx context.Context, where string) error {
	table, err := w.collect(ctx, where)
	if err != nil {
		return err
	}
	if table == nil {
		return nil
	}
	defer table.Release()
	if table.NumRows() == 0 {
		return nil
	}

	if err := w.sink.WriteTable(ctx, table); err != nil {
		return fmt.Errorf("writing closed windows: %w", err)
	}
	if err := w.sink.Flush(ctx); err != nil {
		return fmt.Errorf("flushing closed windows: %w", err)
	}
	return nil
}

func (w *Watermark) collect(ctx context.Context, where string) (arrow.Table, error) {
	stmt, err := w.conn.NewStatement()
	if err != nil {
		return nil, err
	}
	defer stmt.Close()
	if err := stmt.SetSqlQuery(w.decl.collectSQL(where)); err != nil {
		return nil, err
	}
	reader, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		return nil, fmt.Errorf("emit_sql: %w", err)
	}
	defer reader.Release()

	var records []arrow.Record
	for reader.Next() {
		rec := reader.Record()
		rec.Retain()
		records = append(records, rec)
	}
	if err := reader.Err(); err != nil {
		for _, rec := range records {
			rec.Release()
		}
		return nil, err
	}
	// The table holds one reference and the records are retained by its
	// columns, so releasing them here leaves the table as the sole owner.
	table := array.NewTableFromRecords(reader.Schema(), records)
	for _, rec := range records {
		rec.Release()
	}
	return table, nil
}

// isConflict reports a DuckDB transaction conflict: the pipeline's
// transaction touched a row this pass deleted. The next kick retries. An
// error that carries a code is never one; a sink's failure keeps its code
// and stops the manager.
func isConflict(err error) bool {
	if errs.CodeOf(err) != errs.CodeInternalUnexpected {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "Conflict on") || strings.Contains(msg, "write-write conflict")
}

// WindowMetrics is the instruments a window records. Exported so the
// series-name test drives them through the real constructor, the way it
// drives core.NewMetrics. Late rows are the engine's to count now, in
// core.Metrics.WindowLateRows, because the engine is where they are decided.
type WindowMetrics struct {
	Watermark metric.Int64Gauge
	Closed    metric.Int64Counter
	// Recomputed counts buckets republished whole because a late row arrived
	// within allowed_lateness_seconds.
	Recomputed metric.Int64Counter
	// CloseLag is how far the window's closes trail its own data, in event
	// seconds. See recordCloseLag.
	CloseLag metric.Int64Gauge
	// NewestStart is the start of the newest bucket the window holds, in
	// event time. Ahead of wall time means rows are stamped in the future.
	NewestStart metric.Int64Gauge
}

// NewWindowMetrics builds the instruments from a provider. A nil provider
// yields instruments that record nothing, and so does one that refuses an
// instrument: losing a gauge is not worth failing a window over.
func NewWindowMetrics(mp metric.MeterProvider, table string) WindowMetrics {
	if mp == nil {
		mp = noop.NewMeterProvider()
	}
	meter := mp.Meter("sqlflow")
	var m WindowMetrics
	var err error
	if m.Watermark, err = meter.Int64Gauge("window_watermark_seconds",
		metric.WithDescription("The window's watermark as Unix time; wall time minus this is how far the stream's clock trails"),
		metric.WithUnit("s")); err != nil {
		return NewWindowMetrics(noop.NewMeterProvider(), table)
	}
	// No unit on the counters: the exporter appends a unit it does not know
	// to the series name, and "count" is one it does not know, so
	// window_closed would export as window_closed_count_total.
	if m.Closed, err = meter.Int64Counter("window_closed",
		metric.WithDescription("Closes that published at least one bucket")); err != nil {
		return NewWindowMetrics(noop.NewMeterProvider(), table)
	}
	if m.Recomputed, err = meter.Int64Counter("window_recomputes",
		metric.WithDescription("Buckets republished whole because a late row arrived within allowed_lateness_seconds")); err != nil {
		return NewWindowMetrics(noop.NewMeterProvider(), table)
	}
	if m.CloseLag, err = meter.Int64Gauge("window_close_lag_seconds",
		metric.WithDescription("How far the window's closes trail the data it holds, in event time; zero while closes keep up"),
		metric.WithUnit("s")); err != nil {
		return NewWindowMetrics(noop.NewMeterProvider(), table)
	}
	if m.NewestStart, err = meter.Int64Gauge("window_newest_bucket_start_seconds",
		metric.WithDescription("Start of the newest bucket the window holds, as Unix event time; ahead of wall time means rows are stamped in the future"),
		metric.WithUnit("s")); err != nil {
		return NewWindowMetrics(noop.NewMeterProvider(), table)
	}

	// The window exists from here, and says so. Nothing else is recorded
	// until the first pass commits, and a reader that sees no window series
	// concludes that nothing here drops rows -- on a pipeline configured to
	// refuse them.
	m.Closed.Add(context.Background(), 0,
		metric.WithAttributes(attribute.String("window", table)))
	return m
}
