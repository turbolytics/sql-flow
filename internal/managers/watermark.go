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

// defaultPollInterval is how often a manager looks for closed buckets when
// the declaration does not say.
const defaultPollInterval = 10 * time.Second

// LatePolicy says what happens to a row for a bucket that already closed.
type LatePolicy string

const (
	// LateReemit publishes the bucket again. For a sink that merges.
	LateReemit LatePolicy = "reemit"
	// LateDrop discards the row and counts it. For a sink that appends.
	LateDrop LatePolicy = "drop"
)

// ParseLatePolicy resolves the configured name. Empty means reemit, which is
// what every window did before the policy existed.
func ParseLatePolicy(s string) (LatePolicy, error) {
	switch LatePolicy(s) {
	case "", LateReemit:
		return LateReemit, nil
	case LateDrop:
		return LateDrop, nil
	default:
		return "", errs.New(errs.CodeConfigInvalid, "late_rows must be drop or reemit, not %q", s)
	}
}

// Declaration is a window as the config declares it: which table, which
// column holds the bucket start, how long a bucket is, and how the close is
// decided.
type Declaration struct {
	Table      string
	TimeColumn string
	Size       time.Duration
	Grace      time.Duration
	// IdleClose is how long the stream may be quiet before every open bucket
	// closes. Zero means never.
	IdleClose time.Duration
	Late      LatePolicy
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
	case core.IsEngineTable(d.Table):
		return errs.New(errs.CodeConfigInvalid, "window %s: an engine table cannot carry a window", d.Table)
	}
	if _, err := ParseLatePolicy(string(d.Late)); err != nil {
		return err
	}
	return nil
}

// Watermark closes one window. It keeps an event-time watermark, persisted
// in sqlflow_windows: every bucket ending at or before it has been published
// to the sink and deleted from the table. The watermark never moves
// backwards, so a bucket is closed once, and a row that arrives for it
// afterwards is late.
//
// It runs on a connection of its own, with autocommit off, so it reads
// committed rows only and its delete commits together with its watermark.
// Nothing here shares the pipeline's lock or transaction.
type Watermark struct {
	conn  adbc.Connection
	tx    transaction
	decl  Declaration
	store *Store
	sink  core.Sink
	poll  time.Duration

	// now is the clock the idle rule reads. Injected so a test can make the
	// stream quiet without waiting.
	now func() time.Time

	logger  *zap.Logger
	drain   *core.DrainBudget
	metrics WindowMetrics
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

// WithDrainBudget bounds the final poll after a cancel.
func WithDrainBudget(b *core.DrainBudget) Option {
	return func(w *Watermark) { w.drain = b }
}

// WithMeterProvider supplies the provider the window instruments record
// through. Without one they record nothing.
func WithMeterProvider(mp metric.MeterProvider) Option {
	return func(w *Watermark) { w.metrics = NewWindowMetrics(mp, w.decl.Table) }
}

// WithClock replaces the wall clock the idle rule reads.
func WithClock(now func() time.Time) Option {
	return func(w *Watermark) { w.now = now }
}

// NewWatermark builds the manager for one window on conn, which must be a
// connection of its own with autocommit off: every poll ends with a commit or
// a rollback on it.
func NewWatermark(conn adbc.Connection, d Declaration, poll time.Duration, sink core.Sink, opts ...Option) (*Watermark, error) {
	if err := d.validate(); err != nil {
		return nil, err
	}
	if d.Late == "" {
		d.Late = LateReemit
	}
	tx, ok := conn.(transaction)
	if !ok {
		return nil, errs.New(errs.CodeStateInternal, "window %s: the connection does not support transactions", d.Table)
	}
	if poll <= 0 {
		poll = defaultPollInterval
	}

	w := &Watermark{
		conn:    conn,
		tx:      tx,
		decl:    d,
		store:   NewStore(conn),
		sink:    sink,
		poll:    poll,
		now:     time.Now,
		logger:  zap.NewNop(),
		metrics: NewWindowMetrics(nil, d.Table),
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

// Start polls until the context is cancelled, then polls once more so buckets
// that closed during the final interval are not stranded in the table.
//
// A failed poll returns. The rows are still in the table, because Poll
// deletes only after the sink accepted them, so a restart republishes the
// same bucket. Start does not retry in place: the sink already ran its retry
// ladder before the error reached here, so what arrives is a destination
// that rejected the rows or stayed unreachable past the deadline. The one
// exception is a write conflict with the pipeline, which the next poll
// retries.
func (w *Watermark) Start(ctx context.Context) error {
	w.logger.Info("starting watermark manager",
		zap.String("table", w.decl.Table),
		zap.Duration("poll_interval", w.poll))

	ticker := time.NewTicker(w.poll)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			err := w.Poll(ctx)
			if err != nil && ctx.Err() == nil {
				if isConflict(err) {
					w.logger.Warn("poll conflicted with the pipeline, retrying next poll", zap.Error(err))
					continue
				}
				w.logger.Error("poll failed, stopping the manager", zap.Error(err))
				return fmt.Errorf("watermark manager %s: %w", w.decl.Table, err)
			}
			if ctx.Err() == nil {
				continue
			}
			// The cancel landed during that poll. The final poll below is
			// the one that counts.
		case <-ctx.Done():
		}
		return w.finalPoll()
	}
}

// finalPoll publishes what closed during the last interval, on the drain
// budget. A poll the deadline ended is reported as the drain running out of
// time, not as the sink's own failure: the rows are still in the table, and
// the next start publishes them.
func (w *Watermark) finalPoll() error {
	err := w.Poll(w.drain.Context())
	if err == nil {
		return nil
	}
	if w.drain.Exceeded() {
		err = errs.Wrap(errs.CodeDrainIncomplete, err,
			"drain deadline %s reached before the final poll finished", w.drain.Deadline())
	}
	w.logger.Error("final poll failed", zap.Error(err))
	return fmt.Errorf("watermark manager %s: final poll: %w", w.decl.Table, err)
}

// Poll runs one close. It ends its transaction before returning, committed
// or rolled back, so the next poll reads a fresh snapshot.
func (w *Watermark) Poll(ctx context.Context) (err error) {
	committed := false
	defer func() {
		if committed {
			return
		}
		// Every read opened a transaction, and a transaction left open would
		// freeze the next poll's view of the table.
		if rbErr := w.tx.Rollback(context.WithoutCancel(ctx)); rbErr != nil && err == nil {
			err = fmt.Errorf("rolling back: %w", rbErr)
		}
	}()

	previous, hadPrevious, err := w.store.Load(ctx, w.decl.Table)
	if err != nil {
		return err
	}

	// Late rows belong to buckets that already closed. Under drop they leave
	// now, before the close is computed, so they are never collected.
	var lateToReemit, dropped int64
	if hadPrevious {
		late, _, err := queryInt64(ctx, w.conn, w.decl.countClosedSQL(previous))
		if err != nil {
			return fmt.Errorf("counting late rows: %w", err)
		}
		if late > 0 {
			w.metrics.Late.Add(ctx, late, metric.WithAttributes(
				attribute.String("window", w.decl.Table),
				attribute.String("policy", string(w.decl.Late))))
			if w.decl.Late == LateDrop {
				if _, err := execRows(ctx, w.conn, w.decl.deleteClosedSQL(previous)); err != nil {
					return fmt.Errorf("dropping late rows: %w", err)
				}
				dropped = late
				w.logger.Info("dropped late rows", zap.Int64("rows", late))
			} else {
				lateToReemit = late
			}
		}
	}

	watermark, moved, err := w.nextWatermark(ctx, previous, hadPrevious)
	if err != nil {
		return err
	}
	if !moved && lateToReemit == 0 && dropped == 0 {
		return nil
	}
	if !moved {
		watermark = previous
	}

	// Anything to publish? A watermark that moved over an empty stretch
	// still has to be saved, or the next poll recomputes the same move.
	rows, _, err := queryInt64(ctx, w.conn, w.decl.countClosedSQL(watermark))
	if err != nil {
		return fmt.Errorf("counting closed rows: %w", err)
	}
	if rows > 0 {
		if err := w.publish(ctx, watermark); err != nil {
			return err
		}
		if _, err := execRows(ctx, w.conn, w.decl.deleteClosedSQL(watermark)); err != nil {
			return fmt.Errorf("deleting closed rows: %w", err)
		}
	}

	if err := w.store.Save(ctx, w.decl.Table, watermark, w.now().UTC()); err != nil {
		return err
	}
	if err := w.tx.Commit(ctx); err != nil {
		return errs.Wrap(errs.CodeStateCommitFailed, err, "committing the close")
	}
	committed = true

	w.metrics.Watermark.Record(ctx, watermark.Unix(), metric.WithAttributes(
		attribute.String("window", w.decl.Table)))
	if rows > 0 {
		w.metrics.Closed.Add(ctx, 1, metric.WithAttributes(
			attribute.String("window", w.decl.Table)))
		w.logger.Debug("closed", zap.Time("watermark", watermark), zap.Int64("rows", rows))
	}
	return nil
}

// nextWatermark computes where the watermark stands, and whether it moved.
//
// While data arrives it is the newest bucket start less the grace: a bucket
// closes once the stream has moved past its end by the grace. After the
// idle bound with nothing arriving it is the newest bucket's end: a stream
// that stops closes everything it has. It never moves backwards, so a delete
// that lowers the newest bucket changes nothing.
func (w *Watermark) nextWatermark(ctx context.Context, previous time.Time, hadPrevious bool) (time.Time, bool, error) {
	newestMicros, hasRows, err := queryInt64(ctx, w.conn, w.decl.newestSQL())
	if err != nil {
		return time.Time{}, false, fmt.Errorf("reading the newest bucket: %w", err)
	}
	if !hasRows {
		return previous, false, nil
	}
	newest := time.UnixMicro(newestMicros).UTC()

	candidate := newest.Add(-w.decl.Grace)
	if w.decl.IdleClose > 0 {
		arrivalMicros, arrived, err := queryInt64(ctx, w.conn, lastArrivalSQL())
		if err != nil {
			return time.Time{}, false, fmt.Errorf("reading the last arrival: %w", err)
		}
		if arrived && w.now().Sub(time.UnixMicro(arrivalMicros)) >= w.decl.IdleClose {
			if end := newest.Add(w.decl.Size); end.After(candidate) {
				candidate = end
			}
		}
	}

	if hadPrevious && !candidate.After(previous) {
		return previous, false, nil
	}
	return candidate, true, nil
}

// publish runs emit_sql over the closed rows and hands the result to the
// sink. Flushed before the delete, so a failure leaves the rows in the table
// to be retried rather than dropping them.
func (w *Watermark) publish(ctx context.Context, watermark time.Time) error {
	if _, err := execRows(ctx, w.conn, w.decl.closedViewSQL(watermark)); err != nil {
		return fmt.Errorf("defining %s: %w", closedView, err)
	}
	table, err := w.collect(ctx)
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

func (w *Watermark) collect(ctx context.Context) (arrow.Table, error) {
	stmt, err := w.conn.NewStatement()
	if err != nil {
		return nil, err
	}
	defer stmt.Close()
	if err := stmt.SetSqlQuery(w.decl.emitSQL()); err != nil {
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
// transaction touched a row this poll deleted. The next poll retries. An
// error that carries a code is never one; a sink's failure keeps its code
// and stops the manager.
func isConflict(err error) bool {
	if errs.CodeOf(err) != errs.CodeInternalUnexpected {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "TransactionContext Error") ||
		strings.Contains(msg, "Conflict on")
}

// WindowMetrics is the three instruments a window records. Exported so the
// series-name test drives them through the real constructor, the way it
// drives core.NewMetrics.
type WindowMetrics struct {
	Watermark metric.Int64Gauge
	Closed    metric.Int64Counter
	Late      metric.Int64Counter
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
	if m.Late, err = meter.Int64Counter("window_late_rows",
		metric.WithDescription("Rows that arrived for a bucket that had already closed, by policy")); err != nil {
		return NewWindowMetrics(noop.NewMeterProvider(), table)
	}
	return m
}
