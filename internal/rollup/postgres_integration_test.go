package rollup

// The generated triggers against a real Postgres: every grain equals its
// source after random writes and republishes, overlapping writers lose
// nothing, the backfill misses no write and survives a long history, and
// buckets land on UTC boundaries whatever the writer's zone.

import (
	"context"
	"fmt"
	"math/rand"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/jackc/pgx/v5"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/turbolytics/sql-flow/internal/sinks"
	"github.com/zeebo/assert"
)

// The demo runs Postgres 18. The DDL needs 15 for NULLS NOT DISTINCT.
const rollupPostgresImage = "postgres:18"

// sourceDDL is sql-flow-bluesky-demo's migration 0001, the table the demo's
// pipeline writes.
const sourceDDL = `CREATE TABLE posts_per_minute_by_lang (
  bucket     TIMESTAMPTZ NOT NULL,
  lang       TEXT        NOT NULL,
  posts      INTEGER     NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (bucket, lang)
)`

type rollupServer struct {
	dsn  string
	conn *pgx.Conn
}

func startRollupPostgres(t *testing.T) *rollupServer {
	t.Helper()
	ctx := context.Background()
	pg, err := tcpostgres.Run(ctx, rollupPostgresImage,
		tcpostgres.WithDatabase("rollup"),
		tcpostgres.WithUsername("rollup"),
		tcpostgres.WithPassword("rollup"),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	t.Cleanup(func() { _ = pg.Terminate(context.Background()) })

	dsn, err := pg.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}
	srv := &rollupServer{dsn: dsn, conn: connectIn(t, dsn, "UTC")}
	execSQL(t, srv.conn, sourceDDL)
	return srv
}

// connectIn opens a session in zone, so a test can write the way a client in
// that zone does.
func connectIn(t *testing.T, dsn, zone string) *pgx.Conn {
	t.Helper()
	conn, err := pgx.Connect(context.Background(), dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	execSQL(t, conn, "SET TIME ZONE '"+zone+"'")
	return conn
}

func execSQL(t *testing.T, conn *pgx.Conn, sql string) {
	t.Helper()
	_, err := conn.Exec(context.Background(), sql)
	assert.NoError(t, err)
}

// applyDDL runs the script in one transaction, as a migration runner does.
func applyDDL(t *testing.T, conn *pgx.Conn, script string) {
	t.Helper()
	execSQL(t, conn, "BEGIN")
	execSQL(t, conn, script)
	execSQL(t, conn, "COMMIT")
}

type minute struct {
	at    time.Time
	lang  string
	posts int32
}

// flushMinutes upserts minutes through the Postgres sink, the pipeline's own
// write path. It returns its error rather than failing the test, so a
// goroutine can call it.
func flushMinutes(dsn string, rows []minute) error {
	s, err := sinks.NewPostgresSink(config.PostgresSink{
		DSN: dsn, Table: "posts_per_minute_by_lang", Mode: sinks.PostgresModeUpsert, Key: []string{"bucket", "lang"},
	})
	if err != nil {
		return err
	}
	defer s.Close()

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "bucket", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}},
		{Name: "lang", Type: arrow.BinaryTypes.String},
		{Name: "posts", Type: arrow.PrimitiveTypes.Int32},
	}, nil)
	b := array.NewRecordBuilder(memory.NewGoAllocator(), schema)
	defer b.Release()
	for _, m := range rows {
		b.Field(0).(*array.TimestampBuilder).Append(arrow.Timestamp(m.at.UnixMicro()))
		b.Field(1).(*array.StringBuilder).Append(m.lang)
		b.Field(2).(*array.Int32Builder).Append(m.posts)
	}
	rec := b.NewRecord()
	defer rec.Release()
	tbl := array.NewTableFromRecords(schema, []arrow.Record{rec})
	defer tbl.Release()

	if err := s.WriteTable(context.Background(), tbl); err != nil {
		return err
	}
	return s.Flush(context.Background())
}

func writeMinutes(t *testing.T, dsn string, rows ...minute) {
	t.Helper()
	assert.NoError(t, flushMinutes(dsn, rows))
}

func at(s string) time.Time {
	v, err := time.Parse(time.RFC3339, s)
	if err != nil {
		panic(err)
	}
	return v
}

// expected is each rollup table of the example computed from scratch, written
// by hand rather than by the generator, so a generator defect cannot hide
// behind itself.
var expected = func() map[string]string {
	widths := map[string]string{"5m": "5 minutes", "15m": "15 minutes", "1h": "1 hour", "6h": "6 hours", "1d": "24 hours"}
	out := map[string]string{}
	for grain, w := range widths {
		b := fmt.Sprintf("date_bin(INTERVAL '%s', bucket, TIMESTAMPTZ '2000-01-01 00:00:00+00')", w)
		out["posts_by_lang_"+grain] = "SELECT " + b + " AS bucket, lang, sum(posts)::bigint AS posts FROM posts_per_minute_by_lang GROUP BY 1, 2"
		out["posts_total_"+grain] = "SELECT " + b + " AS bucket, count(DISTINCT bucket) AS minutes, sum(posts)::bigint AS posts FROM posts_per_minute_by_lang GROUP BY 1"
	}
	return out
}()

func tableColumns(table string) string {
	if strings.HasPrefix(table, "posts_total_") {
		return "bucket, minutes, posts"
	}
	return "bucket, lang, posts"
}

// assertGrainsEqualSource fails when any rollup table differs from its source
// in any row, either way.
func assertGrainsEqualSource(t *testing.T, conn *pgx.Conn) {
	t.Helper()
	for table, want := range expected {
		got := "SELECT " + tableColumns(table) + " FROM " + table
		var diff int64
		q := fmt.Sprintf("SELECT count(*) FROM ((%s EXCEPT %s) UNION ALL (%s EXCEPT %s)) AS d", want, got, got, want)
		assert.NoError(t, conn.QueryRow(context.Background(), q).Scan(&diff))
		if diff != 0 {
			t.Fatalf("%s differs from its source in %d rows", table, diff)
		}
	}
}

func count(t *testing.T, conn *pgx.Conn, q string) int64 {
	t.Helper()
	var n int64
	assert.NoError(t, conn.QueryRow(context.Background(), q).Scan(&n))
	return n
}

// resetRollups drops the generated objects and empties the source, so one
// container serves several cases. Dropping a function drops its triggers.
func resetRollups(t *testing.T, srv *rollupServer) {
	t.Helper()
	for table := range expected {
		execSQL(t, srv.conn, "DROP FUNCTION IF EXISTS "+quote("sqlflow_rollup_"+table)+"() CASCADE")
		execSQL(t, srv.conn, "DROP TABLE IF EXISTS "+quote(table))
	}
	execSQL(t, srv.conn, "TRUNCATE posts_per_minute_by_lang")
}

func exampleDDL(t *testing.T) string {
	t.Helper()
	script, err := PostgresDDL(loadExample(t))
	assert.NoError(t, err)
	return script
}

func TestIntegrationRollup_EveryGrainEqualsItsSource(t *testing.T) {
	coverage.Covers(t, "cli.rollup")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	srv := startRollupPostgres(t)

	rng := rand.New(rand.NewSource(1))
	start := at("2026-09-12T22:00:00Z")
	langs := []string{"en", "ja", "de", "pt", "unknown"}
	batch := func(n int) []minute {
		rows := make([]minute, n)
		for i := range rows {
			rows[i] = minute{
				at:    start.Add(time.Duration(rng.Intn(3*24*60)) * time.Minute),
				lang:  langs[rng.Intn(len(langs))],
				posts: int32(1 + rng.Intn(500)),
			}
		}
		return rows
	}

	// History before the migration, for the backfill to fill.
	writeMinutes(t, srv.dsn, batch(2000)...)
	applyDDL(t, srv.conn, exampleDDL(t))
	assertGrainsEqualSource(t, srv.conn)

	// Then writes through the triggers. Random keys repeat, so many of these
	// republish a minute with a new count.
	for i := 0; i < 20; i++ {
		writeMinutes(t, srv.dsn, batch(200)...)
	}
	assertGrainsEqualSource(t, srv.conn)
	assert.That(t, count(t, srv.conn, "SELECT count(*) FROM posts_by_lang_5m") > 0)
}

func TestIntegrationRollup_ARepublishedMinuteReplaces(t *testing.T) {
	coverage.Covers(t, "cli.rollup")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	srv := startRollupPostgres(t)
	applyDDL(t, srv.conn, exampleDDL(t))

	writeMinutes(t, srv.dsn, minute{at("2026-09-15T10:01:00Z"), "en", 5})
	writeMinutes(t, srv.dsn, minute{at("2026-09-15T10:01:00Z"), "en", 9})

	assert.Equal(t, int64(9), count(t, srv.conn, "SELECT posts FROM posts_by_lang_1d WHERE lang = 'en'"))
	assert.Equal(t, int64(1), count(t, srv.conn, "SELECT minutes FROM posts_total_1d"))
	assertGrainsEqualSource(t, srv.conn)
}

// When every row conflicts, the INSERT trigger still fires, with an empty
// transition table, and must change nothing.
func TestIntegrationRollup_AnUpsertWhereEveryRowConflicts(t *testing.T) {
	coverage.Covers(t, "cli.rollup")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	srv := startRollupPostgres(t)
	applyDDL(t, srv.conn, exampleDDL(t))

	rows := []minute{{at("2026-09-15T10:01:00Z"), "en", 5}, {at("2026-09-15T10:02:00Z"), "ja", 3}}
	writeMinutes(t, srv.dsn, rows...)
	before := count(t, srv.conn, "SELECT count(*) FROM posts_by_lang_5m")
	writeMinutes(t, srv.dsn, rows...)

	assert.Equal(t, before, count(t, srv.conn, "SELECT count(*) FROM posts_by_lang_5m"))
	assertGrainsEqualSource(t, srv.conn)
}

// lockBlock matches every per-bucket lock the generator writes.
var lockBlock = regexp.MustCompile(`(?s)  IF current_setting\('sqlflow\.rollup_backfill', true\) IS DISTINCT FROM 'on' THEN\n.*?  END IF;\n`)

func withoutLocks(script string) string {
	out := lockBlock.ReplaceAllString(script, "")
	if out == script {
		panic("no lock block in the script; the generator's lock SQL changed")
	}
	return out
}

// keyedOnText also drops the rollup's lock, which serializes writers by
// itself, so the case measures the bucket key alone.
func keyedOnText(script string) string {
	out := strings.ReplaceAll(script, "extract(epoch FROM touched.b)::bigint", "touched.b::text")
	if out == script {
		panic("no epoch lock key in the script; the generator's lock SQL changed")
	}
	return withoutRollupLock(out)
}

var rollupLock = regexp.MustCompile(`    PERFORM pg_advisory_xact_lock\(hashtextextended\('sqlflow_rollup:[a-z0-9_]+', 0\)\);\n`)

func withoutRollupLock(script string) string {
	out := rollupLock.ReplaceAllString(script, "")
	if out == script {
		panic("no rollup lock in the script; the generator's lock SQL changed")
	}
	return out
}

// Render runs the old and the new worker side by side during a deploy, so two
// writers can touch one bucket at once. The first holds its transaction open;
// the second's re-merge must wait for it rather than read around it.
func TestIntegrationRollup_OverlappingWritersLoseNothing(t *testing.T) {
	coverage.Covers(t, "cli.rollup")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	srv := startRollupPostgres(t)
	script := exampleDDL(t)

	for _, tt := range []struct {
		name       string
		secondZone string
		edit       func(string) string
		want       int64
	}{
		{"both in UTC", "UTC", nil, 12},
		{"the second in Asia/Kolkata", "Asia/Kolkata", nil, 12},
		// The test must be able to fail. Without the lock, or with a key
		// rendered in the session's zone, the second writer re-merges from a
		// snapshot without the first's minute and overwrites its count.
		{"without the lock", "UTC", withoutLocks, 7},
		{"keyed on the bucket's text", "Asia/Kolkata", keyedOnText, 7},
	} {
		t.Run(tt.name, func(t *testing.T) {
			resetRollups(t, srv)
			s := script
			if tt.edit != nil {
				s = tt.edit(s)
			}
			applyDDL(t, srv.conn, s)

			first := connectIn(t, srv.dsn, "UTC")
			second := connectIn(t, srv.dsn, tt.secondZone)
			execSQL(t, first, "BEGIN")
			execSQL(t, first, "INSERT INTO posts_per_minute_by_lang (bucket, lang, posts) VALUES ('2026-09-15 10:01:00+00', 'en', 5)")

			done := make(chan error, 1)
			go func() {
				_, err := second.Exec(context.Background(),
					"INSERT INTO posts_per_minute_by_lang (bucket, lang, posts) VALUES ('2026-09-15 10:02:00+00', 'en', 7)")
				done <- err
			}()
			// Long enough for the second writer to reach the first's lock or row.
			time.Sleep(500 * time.Millisecond)
			execSQL(t, first, "COMMIT")
			assert.NoError(t, <-done)

			assert.Equal(t, tt.want, count(t, srv.conn,
				"SELECT posts FROM posts_by_lang_5m WHERE bucket = '2026-09-15 10:00:00+00' AND lang = 'en'"))
		})
	}
}

// The lock waits, then reads a new snapshot. Under REPEATABLE READ it would
// read the old one, so the function refuses.
func TestIntegrationRollup_RefusesAnotherIsolationLevel(t *testing.T) {
	coverage.Covers(t, "cli.rollup")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	srv := startRollupPostgres(t)
	applyDDL(t, srv.conn, exampleDDL(t))

	conn := connectIn(t, srv.dsn, "UTC")
	execSQL(t, conn, "BEGIN ISOLATION LEVEL REPEATABLE READ")
	_, err := conn.Exec(context.Background(),
		"INSERT INTO posts_per_minute_by_lang (bucket, lang, posts) VALUES ('2026-09-15 10:01:00+00', 'en', 5)")
	assert.Error(t, err)
	assert.That(t, strings.Contains(err.Error(), "needs READ COMMITTED, not repeatable read"))
	execSQL(t, conn, "ROLLBACK")
}

// A pipeline write that arrives while the migration runs waits for the
// source lock, then lands in every grain.
func TestIntegrationRollup_BackfillMissesNoConcurrentWrite(t *testing.T) {
	coverage.Covers(t, "cli.rollup")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	srv := startRollupPostgres(t)
	writeMinutes(t, srv.dsn, minute{at("2026-09-15T10:01:00Z"), "en", 5}, minute{at("2026-09-15T11:30:00Z"), "ja", 2})

	migrator := connectIn(t, srv.dsn, "UTC")
	execSQL(t, migrator, "BEGIN")
	execSQL(t, migrator, exampleDDL(t))

	done := make(chan error, 1)
	go func() { done <- flushMinutes(srv.dsn, []minute{{at("2026-09-15T12:07:00Z"), "de", 3}}) }()

	time.Sleep(500 * time.Millisecond)
	select {
	case err := <-done:
		t.Fatalf("the write finished while the migration held the source lock: %v", err)
	default:
	}
	execSQL(t, migrator, "COMMIT")
	assert.NoError(t, <-done)

	assert.Equal(t, int64(3), count(t, srv.conn, "SELECT posts FROM posts_by_lang_1d WHERE lang = 'de'"))
	assertGrainsEqualSource(t, srv.conn)
}

// A lock per bucket across a long history exhausts the shared lock table, so
// the backfill skips the per-bucket locks under the source table's lock.
func TestIntegrationRollup_BackfillPastTheLockTable(t *testing.T) {
	coverage.Covers(t, "cli.rollup")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	srv := startRollupPostgres(t)

	slots := count(t, srv.conn,
		"SELECT current_setting('max_locks_per_transaction')::bigint * current_setting('max_connections')::bigint")
	// Enough 5m buckets that the 15m re-merge alone would lock more buckets
	// than the table holds.
	buckets := int(3 * slots)
	start := at("2026-01-01T00:00:00Z")
	rows := make([]minute, buckets)
	for i := range rows {
		rows[i] = minute{start.Add(time.Duration(i) * 5 * time.Minute), "en", 1}
	}
	writeMinutes(t, srv.dsn, rows...)

	script := exampleDDL(t)
	t.Run("taking the per-bucket locks fails", func(t *testing.T) {
		locked := strings.ReplaceAll(script, "IS DISTINCT FROM 'on'", "IS DISTINCT FROM 'never'")
		assert.That(t, locked != script)
		conn := connectIn(t, srv.dsn, "UTC")
		execSQL(t, conn, "BEGIN")
		_, err := conn.Exec(context.Background(), locked)
		assert.Error(t, err)
		assert.That(t, strings.Contains(err.Error(), "out of shared memory"))
		execSQL(t, conn, "ROLLBACK")
	})

	t.Run("the generated backfill succeeds", func(t *testing.T) {
		applyDDL(t, srv.conn, script)
		assert.Equal(t, int64(buckets), count(t, srv.conn, "SELECT count(*) FROM posts_by_lang_5m"))
		assertGrainsEqualSource(t, srv.conn)
	})
}

// Rollups outlive the source: a retention job deleting old minutes must not
// erase hourly and daily history.
func TestIntegrationRollup_DeletesDoNotPropagate(t *testing.T) {
	coverage.Covers(t, "cli.rollup")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	srv := startRollupPostgres(t)
	applyDDL(t, srv.conn, exampleDDL(t))
	writeMinutes(t, srv.dsn, minute{at("2026-09-15T10:01:00Z"), "en", 5}, minute{at("2026-09-15T10:02:00Z"), "en", 7})

	execSQL(t, srv.conn, "DELETE FROM posts_per_minute_by_lang WHERE bucket = '2026-09-15 10:01:00+00'")

	assert.Equal(t, int64(12), count(t, srv.conn, "SELECT posts FROM posts_by_lang_1d WHERE lang = 'en'"))
}

// Writers in any zone produce UTC buckets, and a day is 24 hours in every
// session. Here 1d is built from 1h, so a day's last child starts at 23:00.
// America/New_York springs forward on 2026-03-08: a re-merge range built with
// INTERVAL '1 day' is 23 hours in that session and drops the 23:00 hour, so
// the day undercounts. A 25-hour range on a fall-back day reads an extra
// child, which the GROUP BY puts in its own bucket, so that direction is
// harmless and not the case to test.
func TestIntegrationRollup_BucketsAreUTC(t *testing.T) {
	coverage.Covers(t, "cli.rollup")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	srv := startRollupPostgres(t)
	conf := loadExample(t)
	conf.Rollups[0].Grains["1d"] = config.RollupGrain{From: "1h"}
	script, err := PostgresDDL(conf)
	assert.NoError(t, err)
	applyDDL(t, srv.conn, script)

	// The day's last hour lands first, so the New York writer's re-merge of
	// that day must read it back.
	kolkata := connectIn(t, srv.dsn, "Asia/Kolkata")
	execSQL(t, kolkata, `INSERT INTO posts_per_minute_by_lang (bucket, lang, posts) VALUES
		('2026-03-08 23:30:00+00', 'en', 32), ('2026-10-31 18:29:00+00', 'en', 16), ('2026-11-02 00:00:00+00', 'en', 8)`)
	newYork := connectIn(t, srv.dsn, "America/New_York")
	execSQL(t, newYork, `INSERT INTO posts_per_minute_by_lang (bucket, lang, posts) VALUES
		('2026-03-08 01:00:00+00', 'en', 64),
		('2026-11-01 05:59:00+00', 'en', 1), ('2026-11-01 06:00:00+00', 'en', 2), ('2026-11-01 23:59:00+00', 'en', 4)`)

	read := func(table string) string {
		var s string
		assert.NoError(t, srv.conn.QueryRow(context.Background(),
			"SELECT string_agg(to_char(bucket AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI') || ' ' || posts, ', ' ORDER BY bucket) FROM "+table,
		).Scan(&s))
		return s
	}
	assert.Equal(t, "2026-03-08 00:00 64, 2026-03-08 18:00 32, 2026-10-31 18:00 16, 2026-11-01 00:00 1, 2026-11-01 06:00 2, 2026-11-01 18:00 4, 2026-11-02 00:00 8",
		read("posts_by_lang_6h"))
	assert.Equal(t, "2026-03-08 00:00 96, 2026-10-31 00:00 16, 2026-11-01 00:00 7, 2026-11-02 00:00 8", read("posts_by_lang_1d"))
	assertGrainsEqualSource(t, srv.conn)
}

func TestIntegrationRollup_CountBuckets(t *testing.T) {
	coverage.Covers(t, "cli.rollup")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	srv := startRollupPostgres(t)
	applyDDL(t, srv.conn, exampleDDL(t))

	var rows []minute
	for _, m := range []string{"10:01", "10:02", "17:45"} {
		for _, lang := range []string{"en", "ja"} {
			rows = append(rows, minute{at("2026-09-15T" + m + ":00Z"), lang, 1})
		}
	}
	writeMinutes(t, srv.dsn, rows...)

	assert.Equal(t, int64(3), count(t, srv.conn, "SELECT minutes FROM posts_total_1d"))
	assert.Equal(t, int64(6), count(t, srv.conn, "SELECT posts FROM posts_total_1d"))
	assertGrainsEqualSource(t, srv.conn)
}

// randomMinutes is a flush of 20 minutes at random across three days from
// start, in four languages. Against history in three of them, a flush both
// inserts and updates.
func randomMinutes(rng *rand.Rand, start time.Time) []minute {
	langs := []string{"en", "ja", "de", "pt"}
	rows := make([]minute, 20)
	for j := range rows {
		rows[j] = minute{
			start.Add(time.Duration(rng.Intn(3*24*60)) * time.Minute),
			langs[rng.Intn(len(langs))], int32(1 + rng.Intn(50)),
		}
	}
	return rows
}

// An upsert fires each rollup trigger twice, for its inserted and its
// updated rows, and each pass locks its buckets in order. Writers whose
// flushes span several buckets lock them in opposite orders and deadlock:
// 13 to 24 of 120 statements from two writers on 2026-09-25. Every trigger
// takes its rollup's lock first, so writers take turns instead.
func TestIntegrationRollup_ConcurrentWritersNeverDeadlock(t *testing.T) {
	coverage.Covers(t, "cli.rollup")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	srv := startRollupPostgres(t)
	history(t, srv.conn, "2026-09-10T00:00:00Z", "2026-09-12T23:59:00Z")
	mustInstall(t, srv.conn, loadExample(t))
	fillAll(t, srv.conn, loadExample(t))

	start := at("2026-09-10T00:00:00Z")
	var wg sync.WaitGroup
	errc := make(chan error, 160)
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed))
			for i := 0; i < 40; i++ {
				if err := flushMinutes(srv.dsn, randomMinutes(rng, start)); err != nil {
					errc <- err
				}
			}
		}(int64(w + 1))
	}
	wg.Wait()
	close(errc)
	for err := range errc {
		t.Errorf("a writer failed beside another: %v", err)
	}
	assertGrainsEqualSource(t, srv.conn)
}
