package rollup

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/errs"
)

// maxChunkLocks bounds the advisory locks one backfill chunk takes: the
// buckets of its table it touches, and the buckets its upserts' triggers
// lock in every coarser grain built from it. The default lock table holds
// 6,400 locks for the whole server, and a chunk shares it with the
// pipeline's writes.
const maxChunkLocks = 1000

// maxChunkRows bounds the rows one chunk re-merges from the table its target
// is built from. A write to any of the chunk's buckets waits for the whole
// chunk, and the chunk's time grows with its rows: a day of 1,000 languages
// by the minute, 1.44 million rows, took 3.5 s on 2026-09-25, and the sink
// gives a write 10 s. A variable, so a test can shorten it.
var maxChunkRows int64 = 100_000

// binOrigin is the instant the generated SQL bins from, so a chunk boundary
// computed here and a bucket computed in Postgres agree.
var binOrigin = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)

// floorTo is the start of t's span-wide bin from binOrigin, as date_bin
// computes it, before the origin as well as after.
func floorTo(t time.Time, span time.Duration) time.Time {
	d := t.Sub(binOrigin)
	q := d / span
	if d < 0 && d%span != 0 {
		q--
	}
	return binOrigin.Add(q * span)
}

// cascadeWidths is e's width, then the width of each grain its upserts
// reach through the triggers: every grain built from e's grain, and every
// grain built from those. The ladder runs narrowest first, so a grain's
// from is decided before the grain.
func cascadeWidths(r config.Rollup, e edge) []time.Duration {
	var out []time.Duration
	for _, g := range cascadeLevels(r, e) {
		out = append(out, g.Width)
	}
	return out
}

// cascadeLevels is e's grain, then each grain cascadeWidths measures.
func cascadeLevels(r config.Rollup, e edge) []config.RollupLevel {
	fed := map[string]bool{e.Grain.Name: true}
	out := []config.RollupLevel{e.Grain}
	for _, g := range r.Ladder() {
		if fed[g.From] && !fed[g.Name] {
			fed[g.Name] = true
			out = append(out, g)
		}
	}
	return out
}

// chunkLocks is the most advisory locks a chunk spanning span takes: at
// each width, the buckets the span covers, plus one it may straddle.
func chunkLocks(span time.Duration, widths []time.Duration) int {
	n := 0
	for _, w := range widths {
		n += int((span+w-1)/w) + 1
	}
	return n
}

// chunkSpan is how much source time one chunk of e's table covers: a UTC
// day, halved until the chunk fits maxChunkLocks. It never drops below the
// table's own width, where a chunk locks about one bucket per grain.
func chunkSpan(r config.Rollup, e edge) time.Duration {
	widths := cascadeWidths(r, e)
	span := 24 * time.Hour
	for span/2 >= e.Grain.Width && chunkLocks(span, widths) > maxChunkLocks {
		span /= 2
	}
	return span
}

// fromWidth is the width of the rows e's table is built from: the source
// grain's, or the finer rollup grain's.
func fromWidth(r config.Rollup, e edge) time.Duration {
	if e.Grain.From == r.Source.Grain {
		w, err := config.ParseServeDuration(r.Source.Grain)
		if err == nil {
			return w
		}
	}
	for _, g := range r.Ladder() {
		if g.Name == e.Grain.From {
			return g.Width
		}
	}
	return e.Grain.Width
}

// findEdge is the declared table named table.
func findEdge(r config.Rollup, table string) (edge, bool) {
	for _, e := range edges(r) {
		if e.Table == table {
			return e, true
		}
	}
	return edge{}, false
}

// Pending is one declared table that still needs filling.
type Pending struct {
	Rollup config.Rollup
	Table  string
	// Done is the start of the oldest chunk filled so far, and nil before
	// the first.
	Done *time.Time
}

// PendingBackfills lists the declared tables that still need filling: rollup
// by rollup in file order, each rollup's tables in the order edges walks
// them, narrowest first, so a table's from table tends to fill first. A
// retained table keeps its pending entry, but nothing declares it, so it is
// skipped until the file declares it again.
func PendingBackfills(ctx context.Context, conn *pgx.Conn, conf *config.RollupsConf) ([]Pending, error) {
	var out []Pending
	for _, r := range conf.Rollups {
		s, err := readState(ctx, conn, r.Name)
		if err != nil {
			return nil, backfillError(err, r.Name, "read state")
		}
		if s == nil {
			continue
		}
		for _, e := range edges(r) {
			if done, ok := s.Backfill[e.Table]; ok {
				out = append(out, Pending{Rollup: r, Table: e.Table, Done: done})
			}
		}
	}
	return out, nil
}

// Step is what one backfill chunk did.
type Step struct {
	Rollup string
	Table  string
	// From and To bound the chunk's source time, To exclusive.
	From, To time.Time
	// Buckets is how many of the table's buckets the chunk re-merged.
	Buckets int64
	// Complete is true when the chunk reached the oldest source row and the
	// table's pending entry is gone.
	Complete bool
	// Remaining is the source history older than the chunk, still to fill.
	Remaining time.Duration
}

// BackfillStep fills one chunk of p's table: the newest source time not yet
// filled, chunkSpan wide or narrower to fit maxChunkRows, so recent history
// is right first.
//
// The chunk reads which of the table's buckets its rows fall in, and takes
// the lock the triggers take on each of them and on every coarser bucket its
// upserts cascade to, before it re-merges exactly those buckets from the
// table they are built from. A chunk and a write on the same bucket
// serialize on that lock, and in READ COMMITTED the later one reads the
// earlier one's commit. A bucket that appears after the read belongs to the
// write that made it, whose trigger holds its lock, so the chunk leaves it
// alone. The coarser triggers the upsert fires take locks the chunk already
// holds, so they return at once.
//
// It only tries the locks. When a write holds one, the attempt rolls back,
// waits busyWait, and tries again, busyTries times, before it returns
// ErrChunkBusy. A chunk that never waits while holding a lock cannot be one
// side of a deadlock. A write waits on a chunk for the chunk's duration at
// most, which maxChunkRows bounds.
//
// It takes the install lock shared, so it never runs beside an install that
// is changing the table's objects, and bounds any other wait with
// lock_timeout. The chunk records its progress in its own transaction, so a
// crash repeats at most one chunk, and a repeat rewrites the same values.
// When the table's entry no longer holds p.Done, the chunk rolls back and
// returns ErrProgressMoved.
func BackfillStep(ctx context.Context, conn *pgx.Conn, p Pending) (Step, error) {
	e, ok := findEdge(p.Rollup, p.Table)
	if !ok {
		return Step{Rollup: p.Rollup.Name, Table: p.Table},
			errs.New(errs.CodeRollupInternal, "rollup backfill: rollup %s declares no table %s", p.Rollup.Name, p.Table)
	}
	for try := 1; ; try++ {
		step, busy, err := backfillAttempt(ctx, conn, p, e)
		if err != nil || !busy {
			return step, err
		}
		if try >= busyTries {
			return step, errs.Wrap(errs.CodeRollupInternal, ErrChunkBusy, "rollup backfill %s", p.Table)
		}
		select {
		case <-ctx.Done():
			return step, backfillError(ctx.Err(), p.Table, "wait for a busy bucket")
		case <-time.After(busyWait):
		}
	}
}

// backfillAttempt is one try at a chunk. busy is true when a write held one
// of its buckets: the attempt rolled back, releasing every lock it took, and
// wrote nothing.
func backfillAttempt(ctx context.Context, conn *pgx.Conn, p Pending, e edge) (step Step, busy bool, err error) {
	step = Step{Rollup: p.Rollup.Name, Table: p.Table}
	tx, err := conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return step, false, backfillError(err, p.Table, "begin")
	}
	// A no-op once Commit has run.
	defer func() { _ = tx.Rollback(ctx) }()

	for _, stmt := range []string{
		"SELECT pg_advisory_xact_lock_shared(hashtextextended('sqlflow_rollup_install', 0))",
		fmt.Sprintf("SET LOCAL lock_timeout = '%dms'", installLockTimeout.Milliseconds()),
	} {
		if _, err := tx.Exec(ctx, stmt); err != nil {
			return step, false, backfillError(err, p.Table, "prepare")
		}
	}

	t := quote(p.Rollup.Source.TimeColumn)
	var oldest, newest *time.Time
	if err := tx.QueryRow(ctx, "SELECT min("+t+"), max("+t+") FROM "+quote(p.Rollup.Source.Table)).Scan(&oldest, &newest); err != nil {
		return step, false, backfillError(err, p.Table, "read the source's range")
	}
	if newest == nil {
		// An empty source has no history to fill.
		step.Complete = true
		return step, false, finishChunk(ctx, tx, p, step)
	}

	// The oldest row the table reads is the source's oldest, binned to the
	// width of the rows the table is built from. A day built from 6-hour
	// rows reads the 6-hour row that holds the source's first minute.
	floor := floorTo(*oldest, fromWidth(p.Rollup, e))
	span := chunkSpan(p.Rollup, e)
	hi := floorTo(*newest, span).Add(span)
	if p.Done != nil {
		hi = *p.Done
	}
	if span, err = fitRows(ctx, tx, p.Rollup, e, hi, span); err != nil {
		return step, false, backfillError(err, p.Table, "count the chunk's rows")
	}
	lo := hi.Add(-span)
	step.From, step.To = lo, hi
	step.Complete = !lo.After(floor)
	if !step.Complete {
		step.Remaining = lo.Sub(floor)
	}

	buckets, err := touchedBuckets(ctx, tx, p.Rollup, e, lo, hi)
	if err != nil {
		return step, false, backfillError(err, p.Table, "read the chunk's buckets")
	}
	step.Buckets = int64(len(buckets))
	if len(buckets) > 0 {
		// Try, never wait. An upsert fires each rollup trigger twice, for
		// its inserted and its updated rows, and the two passes lock buckets
		// in different orders, so a chunk that waited while holding locks
		// could deadlock a writer. bool_and reads every row, so the attempt
		// takes every free lock before it learns one was held.
		var got bool
		if err := tx.QueryRow(ctx,
			"SELECT bool_and(pg_try_advisory_xact_lock(hashtextextended(k, 0))) FROM unnest($1::text[]) AS k",
			chunkKeys(p.Rollup, e, buckets)).Scan(&got); err != nil {
			return step, false, backfillError(err, p.Table, "lock the chunk's buckets")
		}
		if !got {
			return step, true, nil
		}
		var b strings.Builder
		join := fmt.Sprintf("\nJOIN unnest($1::timestamptz[]) AS touched(b)\n  ON f.%s >= touched.b AND f.%s < touched.b + %s",
			t, t, interval(e.Grain.Width))
		writeUpsert(&b, p.Rollup, e.Set, e.Grain, e.From, join)
		if _, err := tx.Exec(ctx, b.String(), buckets); err != nil {
			return step, false, backfillError(err, p.Table, "re-merge the chunk")
		}
	}
	return step, false, finishChunk(ctx, tx, p, step)
}

// fitRows halves span until the rows of the table e is built from, in
// [hi-span, hi), fit maxChunkRows. It stops at the table's own width: one
// bucket re-merges in the time a write's trigger takes to re-merge it.
//
// The query walks back from hi to the first row past the budget, so it
// reads at most maxChunkRows rows however many the span holds. Every row
// newer than that one fits.
func fitRows(ctx context.Context, q querier, r config.Rollup, e edge, hi time.Time, span time.Duration) (time.Duration, error) {
	t := quote(r.Source.TimeColumn)
	var past time.Time
	err := q.QueryRow(ctx, fmt.Sprintf("SELECT %s FROM %s WHERE %s >= $1 AND %s < $2 ORDER BY %s DESC OFFSET $3 LIMIT 1",
		t, quote(e.From), t, t, t), hi.Add(-span), hi, maxChunkRows).Scan(&past)
	if errors.Is(err, pgx.ErrNoRows) {
		return span, nil
	}
	if err != nil {
		return 0, err
	}
	for !hi.Add(-span).After(past) && span/2 >= e.Grain.Width {
		span /= 2
	}
	return span, nil
}

// touchedBuckets is every bucket of e's table that the rows of the table it
// is built from, in [lo, hi), fall in, oldest first: the order the triggers
// lock buckets in.
func touchedBuckets(ctx context.Context, q querier, r config.Rollup, e edge, lo, hi time.Time) ([]time.Time, error) {
	t := quote(r.Source.TimeColumn)
	rows, err := q.Query(ctx, fmt.Sprintf("SELECT DISTINCT %s AS b FROM %s WHERE %s >= $1 AND %s < $2 ORDER BY 1",
		bin(e.Grain.Width, t), quote(e.From), t, t), lo, hi)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[time.Time])
}

// busyTries is how many times a chunk tries to take its buckets' locks
// before it gives up until the daemon's next pass, and busyWait how long it
// waits between tries. Variables, so a test can shorten them.
var (
	busyTries = 20
	busyWait  = 50 * time.Millisecond
)

// ErrChunkBusy is a chunk that found one of its buckets locked by a write on
// every try. It wrote nothing, and the next pass tries again.
var ErrChunkBusy = errors.New("a write held one of the chunk's buckets on every try")

// ErrProgressMoved is a chunk that found its table's progress changed after
// the caller read it: another chunk, or an install, wrote the entry. It
// wrote nothing, and the caller reads the entries again.
var ErrProgressMoved = errors.New("the table's backfill progress changed after it was read")

// bucketKey is the key the generated triggers lock a bucket with: the
// table's name, a colon, and the bucket's epoch in seconds.
func bucketKey(table string, bucket time.Time) string {
	return fmt.Sprintf("%s:%d", table, bucket.Unix())
}

// chunkKeys is every lock key a chunk over buckets of e's table needs: each
// of those buckets, and each coarser bucket its upserts' triggers lock on
// the way up the cascade. The triggers bin a coarser bucket from the finer
// one's start, so floorTo computes the same buckets here.
func chunkKeys(r config.Rollup, e edge, buckets []time.Time) []string {
	var keys []string
	for _, g := range cascadeLevels(r, e) {
		table := Table(e.Set, g.Name)
		seen := map[int64]bool{}
		for _, b := range buckets {
			c := floorTo(b, g.Width)
			if !seen[c.Unix()] {
				seen[c.Unix()] = true
				keys = append(keys, bucketKey(table, c))
			}
		}
	}
	return keys
}

// finishChunk records the chunk's progress in the state row and commits, so
// the progress and the rows it describes land together. It records nothing
// over an entry that changed since the caller read p.Done: an install that
// re-created the table reset it, or another chunk moved it. Progress written
// over a reset entry would skip the history the new table lacks. The UPDATE
// re-reads the row under its lock, so no change slips in between.
func finishChunk(ctx context.Context, tx pgx.Tx, p Pending, step Step) error {
	set := "jsonb_set(backfill, ARRAY[$2::text], to_jsonb($4::timestamptz))"
	args := []any{p.Rollup.Name, p.Table, p.Done, step.From}
	if step.Complete {
		set, args = "backfill - $2::text", args[:3]
	}
	tag, err := tx.Exec(ctx, `UPDATE sqlflow_rollup_state SET backfill = `+set+`
WHERE rollup = $1 AND backfill ? $2::text
  AND (backfill ->> $2::text)::timestamptz IS NOT DISTINCT FROM $3::timestamptz`, args...)
	if err != nil {
		return backfillError(err, p.Table, "record progress")
	}
	if tag.RowsAffected() == 0 {
		return errs.Wrap(errs.CodeRollupInternal, ErrProgressMoved, "rollup backfill %s", p.Table)
	}
	if err := tx.Commit(ctx); err != nil {
		return backfillError(err, p.Table, "commit")
	}
	return nil
}

func backfillError(err error, table, step string) error {
	return errs.Wrap(errs.CodeRollupInternal, err, "rollup backfill %s: %s", table, step)
}
