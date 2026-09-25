package rollup

// sqlflow rollup install against a real Postgres: the state row, the checks
// on the source and on existing tables, and the install itself.

import (
	"context"
	"errors"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/turbolytics/sql-flow/internal/errs"
	"github.com/zeebo/assert"
)

func TestIntegrationRollupRun_TheStateRowRoundTrips(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	srv := startRollupPostgres(t)
	ctx := context.Background()
	execSQL(t, srv.conn, stateDDL)

	none, err := readState(ctx, srv.conn, "posts")
	assert.NoError(t, err)
	assert.That(t, none == nil)

	day := at("2026-09-20T00:00:00Z")
	want := State{
		Rollup: "posts", Declaration: AppliedFrom(exampleRollup(t)), Version: "test",
		Backfill: map[string]*time.Time{"posts_by_lang_5m": nil, "posts_total_5m": &day},
		Retained: []string{"posts_by_lang_7d"},
	}
	assert.NoError(t, writeState(ctx, srv.conn, want))

	got, err := readState(ctx, srv.conn, "posts")
	assert.NoError(t, err)
	assert.DeepEqual(t, want.Declaration, got.Declaration)
	assert.DeepEqual(t, want.Retained, got.Retained)
	assert.Equal(t, "test", got.Version)
	pending, ok := got.Backfill["posts_by_lang_5m"]
	assert.True(t, ok)
	assert.That(t, pending == nil)
	assert.That(t, got.Backfill["posts_total_5m"].Equal(day))

	all, err := readAllStates(ctx, srv.conn)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(all))
}

func TestIntegrationRollupRun_SourceChecksNameWhatTheTriggersNeed(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	srv := startRollupPostgres(t)
	check := func(r config.Rollup) []config.Violation {
		t.Helper()
		v, err := checkSource(context.Background(), srv.conn, r, pathInFile)
		assert.NoError(t, err)
		return v
	}

	assert.Equal(t, 0, len(check(exampleRollup(t))))

	gone := exampleRollup(t)
	gone.Source.Table = "posts_nowhere"
	v := check(gone)
	assert.Equal(t, 1, len(v))
	assert.Equal(t, errs.CodeConfigRollup, v[0].Code)
	assert.That(t, strings.Contains(v[0].Message, "does not exist"))

	unread := exampleRollup(t)
	unread.DimensionSets[0].Measures["likes"] = config.RollupMeasure{Type: "sum", Column: "likes"}
	v = check(unread)
	assert.Equal(t, 1, len(v))
	assert.That(t, strings.Contains(v[0].Message, "has no column likes"))

	// A key led by lang serves no range on bucket.
	execSQL(t, srv.conn, "ALTER TABLE posts_per_minute_by_lang DROP CONSTRAINT posts_per_minute_by_lang_pkey")
	execSQL(t, srv.conn, "ALTER TABLE posts_per_minute_by_lang ADD PRIMARY KEY (lang, bucket)")
	v = check(exampleRollup(t))
	assert.Equal(t, 1, len(v))
	assert.Equal(t, "rollups.0.source.time_column", strings.Join(v[0].Path, "."))
	assert.That(t, strings.Contains(v[0].Message, `CREATE INDEX ON "posts_per_minute_by_lang" ("bucket")`))
}

func TestIntegrationRollupRun_CompareTableNamesEachColumnThatDiffers(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	srv := startRollupPostgres(t)
	ctx := context.Background()
	r := exampleRollup(t)
	e := edges(r)[0]
	compare := func() []config.Violation {
		t.Helper()
		tx, err := srv.conn.Begin(ctx)
		assert.NoError(t, err)
		defer func() { _ = tx.Rollback(ctx) }()
		v, err := compareTable(ctx, tx, r, e, quote(e.Table), pathInFile)
		assert.NoError(t, err)
		return v
	}

	execSQL(t, srv.conn, "CREATE TABLE posts_by_lang_5m (bucket timestamptz, lang text, posts text, extra int)")
	v := compare()
	assert.Equal(t, 1, len(v))
	assert.Equal(t, errs.CodeConfigRollupChange, v[0].Code)
	assert.Equal(t, "rollups.0.dimension_sets.0", strings.Join(v[0].Path, "."))
	assert.That(t, strings.Contains(v[0].Message, "posts is text, not bigint"))
	assert.That(t, strings.Contains(v[0].Message, "extra is not declared"))

	execSQL(t, srv.conn, "DROP TABLE posts_by_lang_5m")
	objects, err := PostgresObjects(r)
	assert.NoError(t, err)
	execSQL(t, srv.conn, objects)
	assert.Equal(t, 0, len(compare()))
}

func mustInstall(t *testing.T, conn *pgx.Conn, conf *config.RollupsConf) *InstallReport {
	t.Helper()
	rep, err := Install(context.Background(), conn, conf, "test")
	assert.NoError(t, err)
	return rep
}

func stateOf(t *testing.T, conn *pgx.Conn, rollup string) *State {
	t.Helper()
	s, err := readState(context.Background(), conn, rollup)
	assert.NoError(t, err)
	assert.That(t, s != nil)
	return s
}

func relationExists(t *testing.T, conn *pgx.Conn, relation string) bool {
	t.Helper()
	return count(t, conn, "SELECT count(*) FROM pg_class WHERE oid = to_regclass('"+relation+"')") == 1
}

func TestIntegrationRollupRun_InstallOnAnEmptyDatabase(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	srv := startRollupPostgres(t)

	got := mustInstall(t, srv.conn, loadExample(t)).Rollups[0]
	assert.False(t, got.Adopted)
	assert.Equal(t, 10, len(got.Created))
	assert.DeepEqual(t, []string{"posts_by_lang_5m", "posts_total_5m"}, got.Plan.Backfill)

	// The triggers are live: writes reach every grain.
	writeMinutes(t, srv.dsn, minute{at("2026-09-15T10:01:00Z"), "en", 5}, minute{at("2026-09-15T10:07:00Z"), "ja", 3})
	assertGrainsEqualSource(t, srv.conn)

	s := stateOf(t, srv.conn, "posts")
	assert.Equal(t, "test", s.Version)
	assert.Equal(t, 2, len(s.Backfill))

	// A second install finds nothing to do and keeps the pending backfills.
	again := mustInstall(t, srv.conn, loadExample(t)).Rollups[0]
	assert.Equal(t, 0, len(again.Created))
	assert.DeepEqual(t, Plan{}, again.Plan)
	assert.Equal(t, 2, len(stateOf(t, srv.conn, "posts").Backfill))
}

func TestIntegrationRollupRun_InstallAdoptsTheMigrationsObjects(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	srv := startRollupPostgres(t)
	rng := rand.New(rand.NewSource(2))
	langs := []string{"en", "ja", "de"}
	var rows []minute
	for i := 0; i < 500; i++ {
		rows = append(rows, minute{
			at("2026-09-12T22:00:00Z").Add(time.Duration(rng.Intn(2*24*60)) * time.Minute),
			langs[rng.Intn(len(langs))], int32(1 + rng.Intn(100)),
		})
	}
	writeMinutes(t, srv.dsn, rows...)
	applyDDL(t, srv.conn, exampleDDL(t))
	assertGrainsEqualSource(t, srv.conn)

	got := mustInstall(t, srv.conn, loadExample(t)).Rollups[0]
	assert.True(t, got.Adopted)
	assert.Equal(t, 0, len(got.Created))
	// Nothing says a migration's tables are complete, so run fills them.
	assert.DeepEqual(t, []string{"posts_by_lang_5m", "posts_total_5m"}, got.Plan.Backfill)

	// No stored value changed, and the replaced triggers keep every grain.
	assertGrainsEqualSource(t, srv.conn)
	writeMinutes(t, srv.dsn, minute{at("2026-09-13T01:02:00Z"), "en", 7})
	assertGrainsEqualSource(t, srv.conn)
}

func TestIntegrationRollupRun_InstallRefusesAMismatchedTableAndChangesNothing(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	srv := startRollupPostgres(t)
	execSQL(t, srv.conn, "CREATE TABLE posts_by_lang_5m (bucket timestamptz, lang text, posts text)")

	_, err := Install(context.Background(), srv.conn, loadExample(t), "test")
	assert.Error(t, err)
	assert.Equal(t, errs.CodeConfigRollupChange, errs.CodeOf(err))
	assert.Equal(t, errs.ExitUserError, errs.ExitCode(err))
	assert.That(t, strings.Contains(err.Error(), "posts is text, not bigint"))

	// One transaction: the state table and every other object rolled back.
	assert.False(t, relationExists(t, srv.conn, "posts_by_lang_15m"))
	assert.False(t, relationExists(t, srv.conn, "sqlflow_rollup_state"))
}

// Every entrypoint of a deploy runs install at once.
func TestIntegrationRollupRun_FourInstallsAtOnce(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	srv := startRollupPostgres(t)
	type session struct {
		conn *pgx.Conn
		conf *config.RollupsConf
	}
	sessions := make([]session, 4)
	for i := range sessions {
		sessions[i] = session{connectIn(t, srv.dsn, "UTC"), loadExample(t)}
	}

	errc := make(chan error, len(sessions))
	for _, s := range sessions {
		go func(s session) {
			_, err := Install(context.Background(), s.conn, s.conf, "test")
			errc <- err
		}(s)
	}
	for range sessions {
		assert.NoError(t, <-errc)
	}
	assert.Equal(t, int64(20), count(t, srv.conn, "SELECT count(*) FROM pg_trigger WHERE tgname LIKE 'sqlflow_rollup_%'"))
	assert.Equal(t, int64(1), count(t, srv.conn, "SELECT count(*) FROM sqlflow_rollup_state"))
}

// A rollback to the previous file must not lose a grain's history.
func TestIntegrationRollupRun_InstallKeepsARemovedGrain(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	srv := startRollupPostgres(t)
	mustInstall(t, srv.conn, loadExample(t))

	// The previous file had no 1d grain, so its dataset served none either.
	rollback := loadExample(t)
	delete(rollback.Rollups[0].Grains, "1d")
	for _, ds := range rollback.Rollups[0].Serve.Datasets {
		delete(ds.MaxRange, "1d")
		delete(ds.CacheTTLByGrain, "1d")
	}
	got := mustInstall(t, srv.conn, rollback).Rollups[0]
	retained := []string{"posts_by_lang_1d", "posts_total_1d"}
	assert.DeepEqual(t, retained, got.Plan.Retain)
	assert.DeepEqual(t, retained, stateOf(t, srv.conn, "posts").Retained)

	// The retained tables keep their triggers, so they stay current.
	writeMinutes(t, srv.dsn, minute{at("2026-09-15T10:01:00Z"), "en", 5})
	assertGrainsEqualSource(t, srv.conn)

	// Declared again, they need no backfill.
	back := mustInstall(t, srv.conn, loadExample(t)).Rollups[0]
	assert.DeepEqual(t, retained, back.Plan.Restore)
	assert.Equal(t, 0, len(back.Plan.Backfill))
	assert.Equal(t, 0, len(stateOf(t, srv.conn, "posts").Retained))
}

func TestIntegrationRollupRun_InstallRefusesAChangedMeasureType(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	srv := startRollupPostgres(t)
	mustInstall(t, srv.conn, loadExample(t))

	// posts_total, because the served set folds by its one sum and a max
	// there breaks a rule of the file before install sees the change.
	changed := loadExample(t)
	changed.Rollups[0].DimensionSets[1].Measures["posts"] = config.RollupMeasure{Type: "max", Column: "posts"}
	_, err := Install(context.Background(), srv.conn, changed, "test")
	assert.Error(t, err)
	assert.Equal(t, errs.CodeConfigRollupChange, errs.CodeOf(err))
	assert.That(t, strings.Contains(err.Error(), "rollups.0.dimension_sets.1.measures.posts"))
	assert.Equal(t, "sum", stateOf(t, srv.conn, "posts").Declaration.DimensionSets["posts_total"].Measures["posts"].Type)
}

func TestIntegrationRollupRun_InstallReportsAnUndeclaredRollup(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	srv := startRollupPostgres(t)
	mustInstall(t, srv.conn, loadExample(t))

	renamed := loadExample(t)
	renamed.Rollups[0].Name = "posts_v2"
	rep := mustInstall(t, srv.conn, renamed)
	assert.DeepEqual(t, []string{"posts"}, rep.Undeclared)
	assert.True(t, rep.Rollups[0].Adopted)
}

func TestIntegrationRollupRun_InstallRecreatesADroppedTable(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	srv := startRollupPostgres(t)
	mustInstall(t, srv.conn, loadExample(t))
	execSQL(t, srv.conn, "DROP TABLE posts_by_lang_1h")

	got := mustInstall(t, srv.conn, loadExample(t)).Rollups[0]
	assert.DeepEqual(t, []string{"posts_by_lang_1h"}, got.Created)
	assert.DeepEqual(t, []string{"posts_by_lang_1h"}, got.Plan.Backfill)
	_, pending := stateOf(t, srv.conn, "posts").Backfill["posts_by_lang_1h"]
	assert.True(t, pending)
}

// The triggers name tables unqualified, so they resolve them through the
// writer's search_path. install creates them where that path puts them first.
func TestIntegrationRollupRun_InstallCreatesTablesInTheCurrentSchema(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	srv := startRollupPostgres(t)
	execSQL(t, srv.conn, "CREATE SCHEMA app")
	execSQL(t, srv.conn, "ALTER TABLE posts_per_minute_by_lang SET SCHEMA app")
	conn := connectIn(t, srv.dsn, "UTC")
	execSQL(t, conn, "SET search_path = app")

	mustInstall(t, conn, loadExample(t))
	assert.True(t, relationExists(t, srv.conn, "app.posts_by_lang_5m"))
	assert.True(t, relationExists(t, srv.conn, "app.sqlflow_rollup_state"))
	assert.False(t, relationExists(t, srv.conn, "public.posts_by_lang_5m"))

	execSQL(t, conn, "INSERT INTO posts_per_minute_by_lang (bucket, lang, posts) VALUES ('2026-09-15T10:01:00Z', 'en', 5)")
	assert.Equal(t, int64(5), count(t, conn, "SELECT posts FROM posts_by_lang_1d"))
}

// waitForLockWait returns once the backend pid is waiting on a lock.
func waitForLockWait(t *testing.T, conn *pgx.Conn, pid uint32) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		var waiting bool
		assert.NoError(t, conn.QueryRow(context.Background(),
			"SELECT coalesce(bool_or(NOT granted), false) FROM pg_locks WHERE pid = $1", int(pid)).Scan(&waiting))
		if waiting {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("backend %d never waited on a lock", pid)
}

// A deploy runs install beside a live pipeline. A writer locks the source,
// then its statement's triggers lock the rollup tables; install must take
// them in the same order or the two deadlock.
func TestIntegrationRollupRun_InstallBesideAWriterMidTransaction(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	srv := startRollupPostgres(t)
	ctx := context.Background()
	mustInstall(t, srv.conn, loadExample(t))

	w := connectIn(t, srv.dsn, "UTC")
	execSQL(t, w, "BEGIN")
	execSQL(t, w, "LOCK TABLE posts_per_minute_by_lang IN ROW EXCLUSIVE MODE")

	inst := connectIn(t, srv.dsn, "UTC")
	conf := loadExample(t)
	errc := make(chan error, 1)
	go func() {
		_, err := Install(ctx, inst, conf, "test")
		errc <- err
	}()
	waitForLockWait(t, srv.conn, inst.PgConn().PID())

	_, werr := w.Exec(ctx, "INSERT INTO posts_per_minute_by_lang (bucket, lang, posts) VALUES ('2026-09-15T10:01:00Z', 'en', 5)")
	assert.NoError(t, werr)
	execSQL(t, w, "COMMIT")
	assert.NoError(t, <-errc)
	assertGrainsEqualSource(t, srv.conn)
}

// withLockTimeout shortens install's lock wait for one test.
func withLockTimeout(t *testing.T, d time.Duration) {
	t.Helper()
	was := installLockTimeout
	installLockTimeout = d
	t.Cleanup(func() { installLockTimeout = was })
}

// While install waits for the source, the pipeline's next writes queue
// behind it. A write left open must fail the install, not stall the
// pipeline until someone ends that transaction.
func TestIntegrationRollupRun_InstallGivesUpOnALockRatherThanStallThePipeline(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	srv := startRollupPostgres(t)
	ctx := context.Background()
	mustInstall(t, srv.conn, loadExample(t))
	withLockTimeout(t, 200*time.Millisecond)

	open := connectIn(t, srv.dsn, "UTC")
	execSQL(t, open, "BEGIN")
	execSQL(t, open, "INSERT INTO posts_per_minute_by_lang (bucket, lang, posts) VALUES ('2026-09-15T10:01:00Z', 'en', 5)")

	ictx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	inst := connectIn(t, srv.dsn, "UTC")
	conf := loadExample(t)
	errc := make(chan error, 1)
	go func() {
		_, err := Install(ictx, inst, conf, "test")
		errc <- err
	}()
	waitForLockWait(t, srv.conn, inst.PgConn().PID())

	// The pipeline's next write still lands. It is on the next day, so it
	// shares no bucket, and no bucket lock, with the open write at any grain.
	w := connectIn(t, srv.dsn, "UTC")
	execSQL(t, w, "SET statement_timeout = '3s'")
	execSQL(t, w, "INSERT INTO posts_per_minute_by_lang (bucket, lang, posts) VALUES ('2026-09-16T11:01:00Z', 'ja', 3)")

	err := <-errc
	assert.Error(t, err)
	var pgErr *pgconn.PgError
	assert.That(t, errors.As(err, &pgErr))
	assert.Equal(t, "55P03", pgErr.Code)

	execSQL(t, open, "COMMIT")
	mustInstall(t, srv.conn, loadExample(t))
	assertGrainsEqualSource(t, srv.conn)
}

// A write that holds the source a little longer than the lock timeout costs
// the install an attempt, not the deploy.
func TestIntegrationRollupRun_InstallRetriesALockHeldBriefly(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	srv := startRollupPostgres(t)
	ctx := context.Background()
	mustInstall(t, srv.conn, loadExample(t))
	withLockTimeout(t, 200*time.Millisecond)

	open := connectIn(t, srv.dsn, "UTC")
	execSQL(t, open, "BEGIN")
	execSQL(t, open, "INSERT INTO posts_per_minute_by_lang (bucket, lang, posts) VALUES ('2026-09-15T10:01:00Z', 'en', 5)")
	committed := make(chan error, 1)
	go func() {
		time.Sleep(300 * time.Millisecond)
		_, err := open.Exec(ctx, "COMMIT")
		committed <- err
	}()

	mustInstall(t, connectIn(t, srv.dsn, "UTC"), loadExample(t))
	assert.NoError(t, <-committed)
	assertGrainsEqualSource(t, srv.conn)
}

// The install lock serializes installs only when each statement takes a new
// snapshot, so install must not inherit a database's stricter default.
func TestIntegrationRollupRun_FourInstallsUnderARepeatableReadDefault(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	srv := startRollupPostgres(t)
	execSQL(t, srv.conn, "ALTER DATABASE rollup SET default_transaction_isolation = 'repeatable read'")
	type session struct {
		conn *pgx.Conn
		conf *config.RollupsConf
	}
	sessions := make([]session, 4)
	for i := range sessions {
		sessions[i] = session{connectIn(t, srv.dsn, "UTC"), loadExample(t)}
	}

	errc := make(chan error, len(sessions))
	for _, s := range sessions {
		go func(s session) {
			_, err := Install(context.Background(), s.conn, s.conf, "test")
			errc <- err
		}(s)
	}
	for range sessions {
		assert.NoError(t, <-errc)
	}
	assert.Equal(t, int64(1), count(t, srv.conn, "SELECT count(*) FROM sqlflow_rollup_state"))
}
