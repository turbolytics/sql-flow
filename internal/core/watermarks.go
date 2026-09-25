package core

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// The engine asserts each window's watermark. A window manager needs one
// thing, how far the stream has got, and nobody used to tell it: it
// inferred the answer from the newest bucket in its table and from the gap
// between two clocks in the progress row, written by someone else in
// another transaction. Every window defect this project has had was one of
// those readings being wrong or two of them disagreeing (#374, #358).
//
// This is Flink's primitive with the coordinator left out. The watermark is
// computed where the facts are, per source partition, from the event time
// of the records this process placed, and written beside those rows in the
// commit that makes them visible. Its meaning is a promise, not a
// measurement: as of this commit, every row this pipeline will ever write
// for this window has time_column at or after the watermark. The manager's
// whole decision is then `bucket end <= watermark`, in one clock.
//
//	candidate(p)  = seen[p] - grace           a partition that has delivered
//	              = -inf                      one that holds and has not
//	W             = min over the partitions in the minimum
//	              = max(seen) + size          when nothing is in the minimum:
//	                                          every partition idle, so the
//	                                          stream is done with what it has
//	stored        = max(stored, W)            monotonic by construction
//
// Which partitions are in the minimum is the partition lifecycle, decided on
// the engine's monotonic clock and nowhere else:
//
//	held, nothing seen             -inf: holds. A partition just assigned may
//	                               still deliver older data than anything here.
//	held, delivering               seen - grace.
//	held, silent for idle_close    leaves the minimum. A quiet partition must
//	                               not pin the window open for the others.
//	lost                           holds at its last position. The session
//	                               failed; the partition may come back with a
//	                               backlog for the buckets this process holds.
//	revoked                        gone. Another member holds it now, and its
//	                               future rows are that member's.
//
// The last two are the one place this departs from Flink, where a split's
// state travels with it and a removed split simply leaves. Here the rows a
// partition contributed stay in this process's table, so a partition that
// may come back is held for and one that will not is not: holding for a
// partition that was moved for good would freeze every window on this
// process, since it is never idle and never delivers again.

// WindowSpec is what the engine needs to assert one window's watermark: the
// name it is stored under, and the three durations the config declares.
type WindowSpec struct {
	Name      string
	Size      time.Duration
	Grace     time.Duration
	IdleClose time.Duration
}

// WatermarksTable holds the engine's assertion per window. Written by the
// engine only, on the pipeline's connection, in the batch's transaction
// where there is one. The manager reads it and never writes it: one row
// with two writers on two connections would make every batch a write-write
// conflict with every poll.
const WatermarksTable = "sqlflow_watermarks"

type partitionKey struct {
	topic     string
	partition int32
}

type partitionState struct {
	// seen is the newest event time this process placed from the partition,
	// in Unix nanoseconds; zero is nothing yet.
	seen int64
	// lastRow is the engine's clock when seen last moved; heldSince is the
	// clock at the assignment. Idleness is measured from the later of the
	// two, so a partition assigned a moment ago is not idle for the silence
	// before it was held.
	lastRow   time.Time
	heldSince time.Time
	// lost is a partition whose session failed and may come back.
	lost bool
}

// Watermarks tracks the partitions this process holds and what it has seen
// from each, and computes every window's watermark from that. One per
// pipeline: a pipeline has one source, so its windows share the partitions
// and differ only in their durations.
//
// Safe for concurrent use. The consume loop observes and advances; a
// source's rebalance callbacks assign and release from their own goroutine.
type Watermarks struct {
	mu    sync.Mutex
	specs []WindowSpec
	now   func() time.Time
	parts map[partitionKey]*partitionState
	// floor is each window's newest bucket start restored from its table at
	// start, in nanoseconds; the all-idle close is measured from it as well
	// as from what this process has seen, so a quiet stream's last buckets
	// close after a restart that saw none of their rows.
	floor map[string]int64
	// stored is each window's asserted watermark, in nanoseconds, zero
	// before the first assertion. It only ever grows.
	stored map[string]int64
}

// NewWatermarks builds the tracker for a pipeline's windows on a clock. The
// clock decides only which partitions are in the minimum; no reading of it
// ever becomes a watermark.
func NewWatermarks(specs []WindowSpec, now func() time.Time) *Watermarks {
	if now == nil {
		now = time.Now
	}
	w := &Watermarks{
		specs:  specs,
		now:    now,
		parts:  map[partitionKey]*partitionState{},
		floor:  map[string]int64{},
		stored: map[string]int64{},
	}
	return w
}

// Restore seeds a window with what its table holds at start: the newest
// bucket start, or a zero time for an empty table. And the watermark last
// asserted, so the first Advance after a restart cannot move it backwards.
func (w *Watermarks) Restore(name string, newestBucketStart, asserted time.Time) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !newestBucketStart.IsZero() {
		w.floor[name] = newestBucketStart.UnixNano()
	}
	if !asserted.IsZero() {
		w.stored[name] = asserted.UnixNano()
	}
}

// Assigned records partitions this process holds now. A partition already
// known keeps what it has seen: a lost partition coming back resumes from
// its last position rather than from -inf, and its idleness restarts from
// the assignment.
func (w *Watermarks) Assigned(parts map[string][]int32) {
	w.mu.Lock()
	defer w.mu.Unlock()
	now := w.now()
	for topic, ps := range parts {
		for _, p := range ps {
			w.hold(partitionKey{topic, p}, now)
		}
	}
}

// Released records partitions another member holds now. They leave the
// minimum: what they contributed to this process's table closes by the
// remaining partitions' progress, and their future rows are not coming here.
func (w *Watermarks) Released(parts map[string][]int32) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for topic, ps := range parts {
		for _, p := range ps {
			delete(w.parts, partitionKey{topic, p})
		}
	}
}

// Lost records partitions whose session failed. They hold the minimum at
// their last position until they are assigned again.
func (w *Watermarks) Lost(parts map[string][]int32) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for topic, ps := range parts {
		for _, p := range ps {
			if s, ok := w.parts[partitionKey{topic, p}]; ok {
				s.lost = true
			} else {
				w.parts[partitionKey{topic, p}] = &partitionState{lost: true}
			}
		}
	}
}

// SetDelivering is the lifecycle for a source with no partitions to
// report: a websocket, a webhook, an MQTT subscription. It is one
// partition, held while the source can deliver and lost while it cannot. A
// reconnect that takes longer than idle_close_seconds is then not a quiet
// stream: the partition holds through it and its idleness restarts when the
// source is back.
func (w *Watermarks) SetDelivering(delivering bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	now := w.now()
	if len(w.parts) == 0 {
		w.parts[partitionKey{}] = &partitionState{heldSince: now, lost: !delivering}
		return
	}
	for _, s := range w.parts {
		switch {
		case !delivering:
			s.lost = true
		case s.lost:
			s.lost = false
			s.heldSince = now
		}
	}
}

// hold marks a partition held from now, keeping what it has seen.
func (w *Watermarks) hold(k partitionKey, now time.Time) {
	if s, ok := w.parts[k]; ok {
		s.lost = false
		s.heldSince = now
		return
	}
	w.parts[k] = &partitionState{heldSince: now}
}

// Observe records a placed record. Its event time moves the partition's
// newest; its arrival, whatever it carries, is activity, so a partition
// delivering records is never idle. Only the engine's placement rule decides
// what reaches here: a record it refused advances nothing, which is what
// keeps one wrong clock from closing every window (#358). A zero is a
// source that stamps nothing: activity, but no event time to move, so such
// a pipeline closes by idleness alone.
//
// A record from a partition not yet held is taken as held from now. A fetch
// buffered before a revocation can deliver one; taking it is the late
// direction, since it can only add a term to the minimum.
func (w *Watermarks) Observe(topic string, partition int32, atNanos int64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	now := w.now()
	k := partitionKey{topic, partition}
	s, ok := w.parts[k]
	if !ok {
		s = &partitionState{heldSince: now}
		w.parts[k] = s
	}
	if atNanos > s.seen {
		s.seen = atNanos
	}
	s.lastRow = now
}

// Next computes every window's watermark and returns the ones that would
// move, with their new value. Nothing is recorded until Commit: the caller
// writes them in the batch's transaction, and a batch that rolls back
// leaves the tracker where it was, so the next commit asserts them again.
func (w *Watermarks) Next() map[string]time.Time {
	w.mu.Lock()
	defer w.mu.Unlock()
	now := w.now()
	moved := map[string]time.Time{}
	for _, spec := range w.specs {
		next, ok := w.compute(spec, now)
		if !ok || next <= w.stored[spec.Name] {
			continue
		}
		moved[spec.Name] = time.Unix(0, next).UTC()
	}
	return moved
}

// Commit records watermarks as asserted, once their write has committed.
// Monotonic here too: a value below what is stored is ignored.
func (w *Watermarks) Commit(moved map[string]time.Time) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for name, at := range moved {
		if n := at.UnixNano(); n > w.stored[name] {
			w.stored[name] = n
		}
	}
}

// Advance is Next then Commit, for a caller with no transaction to ride.
func (w *Watermarks) Advance() map[string]time.Time {
	moved := w.Next()
	w.Commit(moved)
	return moved
}

// Asserted is the watermark last asserted for a window, if any.
func (w *Watermarks) Asserted(name string) (time.Time, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	n, ok := w.stored[name]
	if !ok || n == 0 {
		return time.Time{}, false
	}
	return time.Unix(0, n).UTC(), true
}

// compute is the computation in the package comment for one window.
func (w *Watermarks) compute(spec WindowSpec, now time.Time) (int64, bool) {
	var (
		inMinimum bool
		minimum   int64
		maxSeen   = w.floor[spec.Name]
	)
	for _, s := range w.parts {
		if s.seen > maxSeen {
			maxSeen = s.seen
		}
		if !s.lost && spec.IdleClose > 0 {
			since := s.heldSince
			if s.lastRow.After(since) {
				since = s.lastRow
			}
			if now.Sub(since) >= spec.IdleClose {
				continue // idle: not in the minimum
			}
		}
		if s.seen == 0 {
			return 0, false // -inf: holds everything
		}
		c := s.seen - int64(spec.Grace)
		if !inMinimum || c < minimum {
			inMinimum, minimum = true, c
		}
	}
	if inMinimum {
		return minimum, true
	}
	// Nothing in the minimum. With no idle bound that can only be a source
	// holding nothing yet, which asserts nothing; with one, every partition
	// held is idle, and the stream is done with everything it has delivered:
	// close through the newest row's bucket. maxSeen + size is at or after
	// that bucket's end and before the next bucket's, whatever the bucket's
	// alignment, so no assumption about how time_column truncates is made.
	if spec.IdleClose == 0 || maxSeen == 0 {
		return 0, false
	}
	return maxSeen + int64(spec.Size), true
}

// WatermarkSaver is what the engine writes assertions through; the DuckDB
// store below is the one run wires.
type WatermarkSaver interface {
	Save(ctx context.Context, name string, watermark time.Time) error
}

// WatermarkStore keeps sqlflow_watermarks: one row per window, updated in
// place, never deleted from and carrying no index, because DuckDB never
// frees rows deleted from an indexed table and a row rewritten on every
// commit under an index is that leak on a timer.
//
// Created on the state connection when the pipeline has a state file, so
// its writes ride the batch's transaction beside the rows they describe;
// on the in-memory database otherwise, where a write autocommits after the
// handler's rows have, which is the late direction: a reader can see rows
// without the watermark that accounts for them, and holds, but never a
// watermark without its rows.
type WatermarkStore struct {
	conn adbc.Connection
}

func NewWatermarkStore(conn adbc.Connection) *WatermarkStore {
	return &WatermarkStore{conn: conn}
}

// Init creates the table if it is absent. Run it under autocommit, before
// the pipeline turns autocommit off on its connection.
func (s *WatermarkStore) Init(ctx context.Context) error {
	q := `CREATE TABLE IF NOT EXISTS ` + WatermarksTable + ` (
	    name      VARCHAR NOT NULL,
	    watermark TIMESTAMPTZ NOT NULL
	)`
	if err := s.exec(ctx, q); err != nil {
		return fmt.Errorf("initialising %s: %w", WatermarksTable, err)
	}
	return nil
}

// Load returns the asserted watermark for a window, or ok false when none
// has been asserted.
func (s *WatermarkStore) Load(ctx context.Context, name string) (watermark time.Time, ok bool, err error) {
	return LoadWatermark(ctx, s.conn, name)
}

// LoadWatermark reads a window's asserted watermark on any connection. The
// manager reads through this on its own connection.
func LoadWatermark(ctx context.Context, conn adbc.Connection, name string) (time.Time, bool, error) {
	q := fmt.Sprintf(`SELECT epoch_us(watermark) FROM %s WHERE name = '%s'`,
		WatermarksTable, escapeSQLString(name))
	stmt, err := conn.NewStatement()
	if err != nil {
		return time.Time{}, false, err
	}
	defer stmt.Close()
	if err := stmt.SetSqlQuery(q); err != nil {
		return time.Time{}, false, err
	}
	reader, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		return time.Time{}, false, fmt.Errorf("loading the watermark for %s: %w", name, err)
	}
	defer reader.Release()
	for reader.Next() {
		rec := reader.Record()
		if rec.NumRows() == 0 {
			continue
		}
		col, ok := rec.Column(0).(*array.Int64)
		if !ok {
			return time.Time{}, false, fmt.Errorf("%s: want a BIGINT column, got %T", q, rec.Column(0))
		}
		if col.IsNull(0) {
			return time.Time{}, false, nil
		}
		return time.UnixMicro(col.Value(0)).UTC(), true, nil
	}
	return time.Time{}, false, reader.Err()
}

// Save writes the assertion: an update of the window's row, or an insert
// the first time. It does not commit.
func (s *WatermarkStore) Save(ctx context.Context, name string, watermark time.Time) error {
	esc := escapeSQLString(name)
	update := fmt.Sprintf(`UPDATE %s SET watermark = TIMESTAMPTZ '%s' WHERE name = '%s'`,
		WatermarksTable, utcLiteral(watermark), esc)
	n, err := s.execRows(ctx, update)
	if err != nil {
		return fmt.Errorf("asserting the watermark for %s: %w", name, err)
	}
	if n > 0 {
		return nil
	}
	insert := fmt.Sprintf(`INSERT INTO %s (name, watermark) VALUES ('%s', TIMESTAMPTZ '%s')`,
		WatermarksTable, esc, utcLiteral(watermark))
	if err := s.exec(ctx, insert); err != nil {
		return fmt.Errorf("asserting the watermark for %s: %w", name, err)
	}
	return nil
}

// NewestBucketStart reads the newest value of a window's time column, for
// Restore. A zero time and ok false for an empty table.
func NewestBucketStart(ctx context.Context, conn adbc.Connection, table, column string) (time.Time, bool, error) {
	q := fmt.Sprintf(`SELECT epoch_us(max(%s)) FROM %s`, quoteIdent(column), quoteIdent(table))
	stmt, err := conn.NewStatement()
	if err != nil {
		return time.Time{}, false, err
	}
	defer stmt.Close()
	if err := stmt.SetSqlQuery(q); err != nil {
		return time.Time{}, false, err
	}
	reader, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		return time.Time{}, false, fmt.Errorf("reading the newest bucket of %s: %w", table, err)
	}
	defer reader.Release()
	for reader.Next() {
		rec := reader.Record()
		if rec.NumRows() == 0 {
			continue
		}
		col, ok := rec.Column(0).(*array.Int64)
		if !ok || col.IsNull(0) {
			return time.Time{}, false, nil
		}
		return time.UnixMicro(col.Value(0)).UTC(), true, nil
	}
	return time.Time{}, false, reader.Err()
}

func quoteIdent(name string) string {
	out := []byte{'"'}
	for i := 0; i < len(name); i++ {
		if name[i] == '"' {
			out = append(out, '"')
		}
		out = append(out, name[i])
	}
	return string(append(out, '"'))
}

func (s *WatermarkStore) exec(ctx context.Context, q string) error {
	_, err := s.execRows(ctx, q)
	return err
}

func (s *WatermarkStore) execRows(ctx context.Context, q string) (int64, error) {
	stmt, err := s.conn.NewStatement()
	if err != nil {
		return 0, err
	}
	defer stmt.Close()
	if err := stmt.SetSqlQuery(q); err != nil {
		return 0, err
	}
	return stmt.ExecuteUpdate(ctx)
}
