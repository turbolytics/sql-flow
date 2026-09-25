package rollup

// Backfill against a real Postgres: history written before the triggers
// existed, a grain added later, a chunk beside open and live writers.

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

// history writes one row per minute per language from from to to, straight
// into the source. Before an install no trigger sees them.
func history(t *testing.T, conn *pgx.Conn, from, to string) {
	t.Helper()
	execSQL(t, conn, `INSERT INTO posts_per_minute_by_lang (bucket, lang, posts)
SELECT g, l, (extract(minute FROM g)::int % 7) + 1
FROM generate_series('`+from+`'::timestamptz, '`+to+`'::timestamptz, interval '1 minute') AS g,
     unnest(ARRAY['en', 'ja', 'de']) AS l`)
}

// lockBuckets holds the trigger's lock on each bucket, as a write does.
func lockBuckets(ctx context.Context, tx pgx.Tx, table string, buckets []time.Time) error {
	for _, b := range buckets {
		if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", bucketKey(table, b)); err != nil {
			return err
		}
	}
	return nil
}

// withBusy shortens a chunk's retries for one test.
func withBusy(t *testing.T, tries int, wait time.Duration) {
	t.Helper()
	tr, w := busyTries, busyWait
	busyTries, busyWait = tries, wait
	t.Cleanup(func() { busyTries, busyWait = tr, w })
}

// withRowBudget shortens a chunk's row budget for one test.
func withRowBudget(t *testing.T, rows int64) {
	t.Helper()
	was := maxChunkRows
	maxChunkRows = rows
	t.Cleanup(func() { maxChunkRows = was })
}

// fillAll runs chunks until nothing is pending, and returns them in order.
func fillAll(t *testing.T, conn *pgx.Conn, conf *config.RollupsConf) []Step {
	t.Helper()
	var steps []Step
	for i := 0; i < 10_000; i++ {
		pending, err := PendingBackfills(context.Background(), conn, conf)
		assert.NoError(t, err)
		if len(pending) == 0 {
			return steps
		}
		step, err := BackfillStep(context.Background(), conn, pending[0])
		assert.NoError(t, err)
		steps = append(steps, step)
	}
	t.Fatal("the backfill never finished")
	return nil
}

func TestIntegrationRollupRun_BackfillFillsHistoryNewestDayFirst(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	srv := startRollupPostgres(t)
	history(t, srv.conn, "2026-09-10T00:00:00Z", "2026-09-12T23:59:00Z")
	mustInstall(t, srv.conn, loadExample(t))

	steps := fillAll(t, srv.conn, loadExample(t))
	// Two tables built from the source, three days each.
	assert.Equal(t, 6, len(steps))
	assert.Equal(t, "posts_by_lang_5m", steps[0].Table)
	assert.That(t, steps[0].From.Equal(at("2026-09-12T00:00:00Z")))
	assert.That(t, steps[0].To.Equal(at("2026-09-13T00:00:00Z")))
	assert.Equal(t, int64(288), steps[0].Buckets)
	assert.Equal(t, 48*time.Hour, steps[0].Remaining)
	assert.True(t, steps[2].Complete)
	assert.Equal(t, 0, len(stateOf(t, srv.conn, "posts").Backfill))
	assertGrainsEqualSource(t, srv.conn)
}

func TestIntegrationRollupRun_BackfillResumesWhereItStopped(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	srv := startRollupPostgres(t)
	history(t, srv.conn, "2026-09-10T00:00:00Z", "2026-09-12T23:59:00Z")
	mustInstall(t, srv.conn, loadExample(t))

	pending, err := PendingBackfills(context.Background(), srv.conn, loadExample(t))
	assert.NoError(t, err)
	_, err = BackfillStep(context.Background(), srv.conn, pending[0])
	assert.NoError(t, err)

	// A crash here loses nothing: the chunk and its progress committed
	// together.
	done := stateOf(t, srv.conn, "posts").Backfill["posts_by_lang_5m"]
	assert.That(t, done != nil && done.Equal(at("2026-09-12T00:00:00Z")))
	steps := fillAll(t, srv.conn, loadExample(t))
	assert.Equal(t, 5, len(steps))
	assert.That(t, steps[0].To.Equal(at("2026-09-12T00:00:00Z")))
	assertGrainsEqualSource(t, srv.conn)
}

// A week built from days spans several chunks, and 2026-09-12 starts a new
// week, so the history falls in two weeks. Each chunk re-merges a whole
// week from its days, whatever part of it the chunk covers.
func TestIntegrationRollupRun_BackfillOfAGrainAddedLater(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	srv := startRollupPostgres(t)
	mustInstall(t, srv.conn, loadExample(t))
	// The first install marks the 5m tables; over an empty source each
	// completes in one chunk. History written after it reaches every grain
	// through the triggers.
	fillAll(t, srv.conn, loadExample(t))
	history(t, srv.conn, "2026-09-10T13:00:00Z", "2026-09-12T23:59:00Z")

	withWeek := loadExample(t)
	withWeek.Rollups[0].Grains["7d"] = config.RollupGrain{From: "1d"}
	assert.DeepEqual(t, []string{"posts_by_lang_7d", "posts_total_7d"}, mustInstall(t, srv.conn, withWeek).Rollups[0].Plan.Backfill)

	for _, s := range fillAll(t, srv.conn, withWeek) {
		assert.That(t, s.Table == "posts_by_lang_7d" || s.Table == "posts_total_7d")
	}
	week := "date_bin(INTERVAL '168 hours', bucket, TIMESTAMPTZ '2000-01-01 00:00:00+00')"
	for table, want := range map[string]string{
		"posts_by_lang_7d": "SELECT " + week + " AS bucket, lang, sum(posts)::bigint AS posts FROM posts_per_minute_by_lang GROUP BY 1, 2",
		"posts_total_7d":   "SELECT " + week + " AS bucket, count(DISTINCT bucket) AS minutes, sum(posts)::bigint AS posts FROM posts_per_minute_by_lang GROUP BY 1",
	} {
		got := "SELECT " + tableColumns(table) + " FROM " + table
		assert.Equal(t, int64(0), count(t, srv.conn,
			"SELECT count(*) FROM (("+want+" EXCEPT "+got+") UNION ALL ("+got+" EXCEPT "+want+")) AS d"))
	}
	assert.Equal(t, int64(2), count(t, srv.conn, "SELECT count(*) FROM posts_total_7d"))
}

func TestIntegrationRollupRun_BackfillSkipsARetainedTable(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	srv := startRollupPostgres(t)
	mustInstall(t, srv.conn, loadExample(t))
	withWeek := loadExample(t)
	withWeek.Rollups[0].Grains["7d"] = config.RollupGrain{From: "1d"}
	mustInstall(t, srv.conn, withWeek)
	mustInstall(t, srv.conn, loadExample(t))

	pending, err := PendingBackfills(context.Background(), srv.conn, loadExample(t))
	assert.NoError(t, err)
	for _, p := range pending {
		assert.That(t, p.Table != "posts_by_lang_7d" && p.Table != "posts_total_7d")
	}
	// Still pending, so declaring the week again finds it unfilled.
	_, kept := stateOf(t, srv.conn, "posts").Backfill["posts_by_lang_7d"]
	assert.True(t, kept)
}

// The backfill's bucket lock is the trigger's: while a chunk holds one, a
// write to that bucket waits.
func TestIntegrationRollupRun_BackfillTakesTheTriggersBucketLock(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	srv := startRollupPostgres(t)
	ctx := context.Background()
	mustInstall(t, srv.conn, loadExample(t))

	holder := connectIn(t, srv.dsn, "UTC")
	tx, err := holder.Begin(ctx)
	assert.NoError(t, err)
	assert.NoError(t, lockBuckets(ctx, tx, "posts_by_lang_5m", []time.Time{at("2026-09-15T10:00:00Z")}))

	w := connectIn(t, srv.dsn, "UTC")
	execSQL(t, w, "SET lock_timeout = '200ms'")
	_, err = w.Exec(ctx, "INSERT INTO posts_per_minute_by_lang (bucket, lang, posts) VALUES ('2026-09-15T10:01:00Z', 'en', 5)")
	var pgErr *pgconn.PgError
	assert.That(t, errors.As(err, &pgErr))
	assert.Equal(t, "55P03", pgErr.Code)

	assert.NoError(t, tx.Rollback(ctx))
	execSQL(t, w, "INSERT INTO posts_per_minute_by_lang (bucket, lang, posts) VALUES ('2026-09-15T10:01:00Z', 'en', 5)")
}

// A write left open holds its buckets' locks. A chunk that needs one gives
// up rather than holding a day of bucket locks, which the pipeline's next
// writes would queue behind.
func TestIntegrationRollupRun_ABackfillChunkGivesUpOnAnOpenWrite(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	srv := startRollupPostgres(t)
	ctx := context.Background()
	history(t, srv.conn, "2026-09-12T00:00:00Z", "2026-09-12T23:59:00Z")
	mustInstall(t, srv.conn, loadExample(t))
	withBusy(t, 3, 20*time.Millisecond)

	open := connectIn(t, srv.dsn, "UTC")
	execSQL(t, open, "BEGIN")
	execSQL(t, open, "INSERT INTO posts_per_minute_by_lang (bucket, lang, posts) VALUES ('2026-09-12T10:01:00Z', 'pt', 9)")

	pending, err := PendingBackfills(ctx, srv.conn, loadExample(t))
	assert.NoError(t, err)
	_, err = BackfillStep(ctx, srv.conn, pending[0])
	assert.That(t, errors.Is(err, ErrChunkBusy))

	execSQL(t, open, "COMMIT")
	fillAll(t, srv.conn, loadExample(t))
	assertGrainsEqualSource(t, srv.conn)
}

// A chunk needs the lock of every bucket it re-merges, not only the coarser
// ones its upserts' triggers take. The holder holds one 5-minute bucket's
// lock and nothing else, so only the chunk's own key can make it give up.
func TestIntegrationRollupRun_ABackfillChunkNeedsItsBucketLock(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	srv := startRollupPostgres(t)
	ctx := context.Background()
	history(t, srv.conn, "2026-09-12T00:00:00Z", "2026-09-12T23:59:00Z")
	mustInstall(t, srv.conn, loadExample(t))
	withBusy(t, 3, 20*time.Millisecond)

	holder := connectIn(t, srv.dsn, "UTC")
	tx, err := holder.Begin(ctx)
	assert.NoError(t, err)
	assert.NoError(t, lockBuckets(ctx, tx, "posts_by_lang_5m", []time.Time{at("2026-09-12T10:00:00Z")}))

	pending, err := PendingBackfills(ctx, srv.conn, loadExample(t))
	assert.NoError(t, err)
	assert.Equal(t, "posts_by_lang_5m", pending[0].Table)
	_, err = BackfillStep(ctx, srv.conn, pending[0])
	assert.That(t, errors.Is(err, ErrChunkBusy))

	assert.NoError(t, tx.Rollback(ctx))
	fillAll(t, srv.conn, loadExample(t))
	assertGrainsEqualSource(t, srv.conn)
}

// An upsert fires each rollup trigger twice, for its inserted and its
// updated rows, and the two passes take bucket locks in different orders. A
// chunk that waited while holding locks could form a cycle with that. Here
// the holder plays such a writer: it holds an hour of the chunk's cascade,
// then asks for a 5-minute bucket inside it. A chunk that took its 5-minute
// locks and waited for the hour would hold that bucket, and the holder's
// request would time out.
func TestIntegrationRollupRun_ABackfillChunkNeverWaitsHoldingALock(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	srv := startRollupPostgres(t)
	ctx := context.Background()
	history(t, srv.conn, "2026-09-12T00:00:00Z", "2026-09-12T23:59:00Z")
	mustInstall(t, srv.conn, loadExample(t))
	withBusy(t, 100, 10*time.Millisecond)

	holder := connectIn(t, srv.dsn, "UTC")
	tx, err := holder.Begin(ctx)
	assert.NoError(t, err)
	assert.NoError(t, lockBuckets(ctx, tx, "posts_by_lang_1h", []time.Time{at("2026-09-12T10:00:00Z")}))

	pending, err := PendingBackfills(ctx, srv.conn, loadExample(t))
	assert.NoError(t, err)
	done := make(chan error, 1)
	go func() {
		_, err := BackfillStep(ctx, srv.conn, pending[0])
		done <- err
	}()
	time.Sleep(200 * time.Millisecond)

	_, err = tx.Exec(ctx, "SET LOCAL lock_timeout = '300ms'")
	assert.NoError(t, err)
	assert.NoError(t, lockBuckets(ctx, tx, "posts_by_lang_5m", []time.Time{at("2026-09-12T10:05:00Z")}))
	assert.NoError(t, tx.Commit(ctx))
	assert.NoError(t, <-done)
	fillAll(t, srv.conn, loadExample(t))
	assertGrainsEqualSource(t, srv.conn)
}

// A writer upserts into the days being filled, through the pipeline's own
// sink, while the backfill runs. It never waits past its lock timeout, and
// every grain equals its source after both finish.
//
// One writer, as the pipeline's sink is per process. Two writers contend
// with each other with no backfill running: an upsert fires each rollup
// trigger twice, for its inserted and its updated rows, and the two passes
// take bucket locks in different orders.
func TestIntegrationRollupRun_ChunkedBackfillLosesNoConcurrentWrite(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	srv := startRollupPostgres(t)
	history(t, srv.conn, "2026-09-10T00:00:00Z", "2026-09-12T23:59:00Z")
	mustInstall(t, srv.conn, loadExample(t))

	start := at("2026-09-10T00:00:00Z")
	langs := []string{"en", "ja", "de", "pt"}
	var wg sync.WaitGroup
	errc := make(chan error, 64)
	for w := 0; w < 1; w++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed))
			for i := 0; i < 60; i++ {
				rows := make([]minute, 20)
				for j := range rows {
					rows[j] = minute{
						start.Add(time.Duration(rng.Intn(3*24*60)) * time.Minute),
						langs[rng.Intn(len(langs))], int32(1 + rng.Intn(50)),
					}
				}
				if err := flushMinutes(srv.dsn+"&lock_timeout=1000", rows); err != nil {
					errc <- err
				}
			}
		}(int64(w + 1))
	}
	fillAll(t, srv.conn, loadExample(t))
	wg.Wait()
	close(errc)
	for err := range errc {
		t.Errorf("a writer failed beside the backfill: %v", err)
	}
	assertGrainsEqualSource(t, srv.conn)
}

// A write to a chunk's bucket waits for the whole chunk, so a chunk holds
// no more rows than the budget. A day of three languages by the minute is
// 4,320 rows; three hours is 540, the widest halving under 1,000.
func TestIntegrationRollupRun_ABackfillChunkFitsTheRowBudget(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	srv := startRollupPostgres(t)
	history(t, srv.conn, "2026-09-12T00:00:00Z", "2026-09-12T23:59:00Z")
	mustInstall(t, srv.conn, loadExample(t))
	withRowBudget(t, 1000)

	steps := fillAll(t, srv.conn, loadExample(t))
	assert.That(t, steps[0].From.Equal(at("2026-09-12T21:00:00Z")))
	assert.That(t, steps[0].To.Equal(at("2026-09-13T00:00:00Z")))
	for _, s := range steps {
		assert.That(t, count(t, srv.conn, "SELECT count(*) FROM posts_per_minute_by_lang WHERE bucket >= '"+
			s.From.Format(time.RFC3339)+"' AND bucket < '"+s.To.Format(time.RFC3339)+"'") <= 1000)
	}
	// Eight chunks for each table built from the source.
	assert.Equal(t, 16, len(steps))
	assertGrainsEqualSource(t, srv.conn)
}

// A chunk records its progress only over the entry it started from. Here an
// install re-created the table after the caller read the entry: the table
// is empty and its entry starts over. A chunk that wrote its progress
// anyway would move the entry past the day it emptied, and never fill it.
func TestIntegrationRollupRun_AChunkWritesNoProgressOverAMovedEntry(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	srv := startRollupPostgres(t)
	ctx := context.Background()
	history(t, srv.conn, "2026-09-10T00:00:00Z", "2026-09-12T23:59:00Z")
	mustInstall(t, srv.conn, loadExample(t))

	pending, err := PendingBackfills(ctx, srv.conn, loadExample(t))
	assert.NoError(t, err)
	_, err = BackfillStep(ctx, srv.conn, pending[0])
	assert.NoError(t, err)
	read, err := PendingBackfills(ctx, srv.conn, loadExample(t))
	assert.NoError(t, err)
	assert.That(t, read[0].Done != nil && read[0].Done.Equal(at("2026-09-12T00:00:00Z")))

	execSQL(t, srv.conn, "TRUNCATE posts_by_lang_5m")
	execSQL(t, srv.conn, `UPDATE sqlflow_rollup_state
SET backfill = jsonb_set(backfill, '{posts_by_lang_5m}', 'null') WHERE rollup = 'posts'`)

	_, err = BackfillStep(ctx, srv.conn, read[0])
	assert.That(t, errors.Is(err, ErrProgressMoved))
	_, kept := stateOf(t, srv.conn, "posts").Backfill["posts_by_lang_5m"]
	assert.True(t, kept)
	assert.That(t, stateOf(t, srv.conn, "posts").Backfill["posts_by_lang_5m"] == nil)

	fillAll(t, srv.conn, loadExample(t))
	assertGrainsEqualSource(t, srv.conn)
}
