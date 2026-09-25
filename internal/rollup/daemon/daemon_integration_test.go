package daemon

// The daemon against a real Postgres: it installs and fills history, one of
// two leads and hands over, it stops mid-backfill and the next resumes, and
// its health follows the database.

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

const examplePath = "../../../dev/config/rollups/bluesky.yml"

func startPostgres(t *testing.T) (string, *pgx.Conn) {
	t.Helper()
	ctx := context.Background()
	pg, err := tcpostgres.Run(ctx, "postgres:18",
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
	assert.NoError(t, err)
	conn, err := pgx.Connect(ctx, dsn)
	assert.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	exec(t, conn, `CREATE TABLE posts_per_minute_by_lang (
  bucket TIMESTAMPTZ NOT NULL, lang TEXT NOT NULL, posts INTEGER NOT NULL,
  PRIMARY KEY (bucket, lang))`)
	return dsn, conn
}

func exec(t *testing.T, conn *pgx.Conn, sql string) {
	t.Helper()
	_, err := conn.Exec(context.Background(), sql)
	assert.NoError(t, err)
}

func history(t *testing.T, conn *pgx.Conn, from, to string) {
	t.Helper()
	exec(t, conn, `INSERT INTO posts_per_minute_by_lang (bucket, lang, posts)
SELECT g, l, (extract(minute FROM g)::int % 7) + 1
FROM generate_series('`+from+`'::timestamptz, '`+to+`'::timestamptz, interval '1 minute') AS g,
     unnest(ARRAY['en', 'ja', 'de']) AS l`)
}

func loadExample(t *testing.T) *config.RollupsConf {
	t.Helper()
	conf, err := config.LoadRollups(examplePath)
	assert.NoError(t, err)
	return conf
}

// assertExact fails when a daily table differs from its source.
func assertExact(t *testing.T, conn *pgx.Conn) {
	t.Helper()
	day := "date_bin(INTERVAL '24 hours', bucket, TIMESTAMPTZ '2000-01-01 00:00:00+00')"
	for table, want := range map[string]string{
		"posts_by_lang_1d": "SELECT " + day + " AS bucket, lang, sum(posts)::bigint AS posts FROM posts_per_minute_by_lang GROUP BY 1, 2",
		"posts_total_1d":   "SELECT " + day + " AS bucket, count(DISTINCT bucket) AS minutes, sum(posts)::bigint AS posts FROM posts_per_minute_by_lang GROUP BY 1",
	} {
		cols := "bucket, lang, posts"
		if strings.HasPrefix(table, "posts_total_") {
			cols = "bucket, minutes, posts"
		}
		got := "SELECT " + cols + " FROM " + table
		var diff int64
		assert.NoError(t, conn.QueryRow(context.Background(),
			fmt.Sprintf("SELECT count(*) FROM ((%s EXCEPT %s) UNION ALL (%s EXCEPT %s)) AS d", want, got, got, want)).Scan(&diff))
		if diff != 0 {
			t.Fatalf("%s differs from its source in %d rows", table, diff)
		}
	}
}

// running starts a daemon and returns a stop function that cancels it and
// returns what Run returned.
func running(t *testing.T, dsn string, opts Options) (*Daemon, func() error) {
	t.Helper()
	if opts.Interval == 0 {
		opts.Interval = 100 * time.Millisecond
	}
	d, err := New(loadExample(t), dsn, opts)
	assert.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- d.Run(ctx) }()
	var once sync.Once
	var result error
	stop := func() error {
		once.Do(func() {
			cancel()
			select {
			case result = <-done:
			case <-time.After(30 * time.Second):
				result = fmt.Errorf("the daemon did not stop within 30s")
			}
		})
		return result
	}
	t.Cleanup(func() { _ = stop() })
	return d, stop
}

func waitFor(t *testing.T, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		if ok() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("waited 60s for %s", what)
}

func status(d *Daemon) string {
	s, _ := d.Health()
	return s
}

func TestIntegrationRollupRun_TheDaemonInstallsAndFillsHistory(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	dsn, conn := startPostgres(t)
	history(t, conn, "2026-09-10T00:00:00Z", "2026-09-12T23:59:00Z")

	d, stop := running(t, dsn, Options{})
	waitFor(t, "healthy", func() bool { return status(d) == "healthy" })
	assertExact(t, conn)
	assert.NoError(t, stop())
}

func TestIntegrationRollupRun_OneDaemonLeadsAndTheOtherTakesOver(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	dsn, conn := startPostgres(t)
	a, _ := running(t, dsn, Options{})
	b, _ := running(t, dsn, Options{})
	waitFor(t, "one leader and one standby", func() bool {
		sa, sb := status(a), status(b)
		return (sa == "healthy" && sb == "standby") || (sa == "standby" && sb == "healthy")
	})
	leader, standby := a, b
	if status(a) == "standby" {
		leader, standby = b, a
	}

	// Kill the leader's session, as a network partition or a crash would.
	exec(t, conn, `SELECT pg_terminate_backend(pid) FROM pg_locks
WHERE locktype = 'advisory' AND granted AND pid <> pg_backend_pid()`)
	waitFor(t, "the standby to lead", func() bool { return status(standby) == "healthy" })
	waitFor(t, "the old leader to stand by", func() bool { return status(leader) == "standby" })
}

func TestIntegrationRollupRun_ADaemonStoppedMidBackfillIsResumed(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	dsn, conn := startPostgres(t)
	history(t, conn, "2026-08-01T00:00:00Z", "2026-09-12T23:59:00Z")

	first, stop := running(t, dsn, Options{})
	waitFor(t, "a backfill in progress", func() bool {
		_, reason := first.Health()
		return strings.Contains(reason, "days of history left")
	})
	stopped := time.Now()
	assert.NoError(t, stop())
	assert.That(t, time.Since(stopped) < 10*time.Second)

	second, _ := running(t, dsn, Options{})
	waitFor(t, "healthy", func() bool { return status(second) == "healthy" })
	assertExact(t, conn)
}

func TestIntegrationRollupRun_AnUnreachableDatabaseFailsHealthAndRecovers(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	dsn, _ := startPostgres(t)
	d, _ := running(t, dsn, Options{})
	waitFor(t, "healthy", func() bool { return status(d) == "healthy" })

	// From another database: Postgres refuses to disallow connections to
	// the database a session is in.
	admin, err := pgx.Connect(context.Background(), strings.Replace(dsn, "/rollup?", "/postgres?", 1))
	assert.NoError(t, err)
	t.Cleanup(func() { _ = admin.Close(context.Background()) })

	// Refuse every new connection, and end the daemon's.
	exec(t, admin, "ALTER DATABASE rollup ALLOW_CONNECTIONS false")
	exec(t, admin, "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = 'rollup'")
	waitFor(t, "failed", func() bool { return status(d) == "failed" })

	exec(t, admin, "ALTER DATABASE rollup ALLOW_CONNECTIONS true")
	waitFor(t, "healthy again, with no restart", func() bool { return status(d) == "healthy" })
}

func TestIntegrationRollupRun_TheDaemonServesHealthzAndMetrics(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	dsn, conn := startPostgres(t)
	history(t, conn, "2026-09-12T00:00:00Z", "2026-09-12T23:59:00Z")
	addrc := make(chan net.Addr, 1)
	d, _ := running(t, dsn, Options{Metrics: "prometheus", Addr: "127.0.0.1:0", OnListen: func(a net.Addr) { addrc <- a }})
	addr := (<-addrc).String()
	waitFor(t, "healthy", func() bool { return status(d) == "healthy" })

	resp, err := http.Get("http://" + addr + "/healthz")
	assert.NoError(t, err)
	var body map[string]string
	assert.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	_ = resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "healthy", body["status"])

	resp, err = http.Get("http://" + addr + "/metrics")
	assert.NoError(t, err)
	text, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	assert.NoError(t, err)
	for _, series := range []string{"rollup_backfill_buckets_total", "rollup_backfill_chunk_duration_seconds", "rollup_leader_acquired_total"} {
		assert.That(t, strings.Contains(string(text), series))
	}
}

// A write left open holds a bucket of posts_by_lang_5m's newest day, so
// every chunk of that table is busy. posts_total_5m fills meanwhile, and
// posts_by_lang_5m fills once the write ends.
func TestIntegrationRollupRun_ABusyTableHoldsUpNoOther(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	ctx := context.Background()
	dsn, conn := startPostgres(t)
	history(t, conn, "2026-09-12T00:00:00Z", "2026-09-12T23:59:00Z")

	holder, err := pgx.Connect(ctx, dsn)
	assert.NoError(t, err)
	t.Cleanup(func() { _ = holder.Close(context.Background()) })
	tx, err := holder.Begin(ctx)
	assert.NoError(t, err)
	_, err = tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
		fmt.Sprintf("posts_by_lang_5m:%d", time.Date(2026, 9, 12, 10, 0, 0, 0, time.UTC).Unix()))
	assert.NoError(t, err)

	addrc := make(chan net.Addr, 1)
	d, _ := running(t, dsn, Options{Metrics: "prometheus", Addr: "127.0.0.1:0", OnListen: func(a net.Addr) { addrc <- a }})
	addr := (<-addrc).String()
	waitFor(t, "posts_total_5m to fill beside the busy table", func() bool {
		var filled bool
		err := conn.QueryRow(ctx, `SELECT NOT backfill ? 'posts_total_5m' AND backfill ? 'posts_by_lang_5m'
FROM sqlflow_rollup_state WHERE rollup = 'posts'`).Scan(&filled)
		return err == nil && filled
	})

	resp, err := http.Get("http://" + addr + "/metrics")
	assert.NoError(t, err)
	text, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	assert.NoError(t, err)
	assert.That(t, strings.Contains(string(text), "rollup_backfill_busy_total"))

	assert.NoError(t, tx.Rollback(ctx))
	waitFor(t, "healthy", func() bool { return status(d) == "healthy" })
	assertExact(t, conn)
}

// Stopping the triggers from 5m to 15m and then writing drifts 15m. The
// next verify pass reports degraded and names the table, the instruments
// record it, and after the trigger returns and the minute is rewritten the
// next pass is healthy again, with no restart.
func TestIntegrationRollupRun_DriftIsReportedAndClears(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	dsn, conn := startPostgres(t)
	history(t, conn, "2026-09-12T00:00:00Z", "2026-09-12T23:59:00Z")
	addrc := make(chan net.Addr, 1)
	d, _ := running(t, dsn, Options{Metrics: "prometheus", Addr: "127.0.0.1:0", OnListen: func(a net.Addr) { addrc <- a }})
	addr := (<-addrc).String()
	waitFor(t, "healthy", func() bool { return status(d) == "healthy" })

	exec(t, conn, `ALTER TABLE posts_by_lang_5m DISABLE TRIGGER sqlflow_rollup_posts_by_lang_15m_ins`)
	exec(t, conn, `ALTER TABLE posts_by_lang_5m DISABLE TRIGGER sqlflow_rollup_posts_by_lang_15m_upd`)
	exec(t, conn, `INSERT INTO posts_per_minute_by_lang (bucket, lang, posts) VALUES ('2026-09-12T23:58:00Z', 'pt', 9)`)
	waitFor(t, "degraded naming posts_by_lang_15m", func() bool {
		s, reason := d.Health()
		return s == "degraded" && strings.Contains(reason, "posts_by_lang_15m")
	})

	resp, err := http.Get("http://" + addr + "/metrics")
	assert.NoError(t, err)
	text, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	assert.NoError(t, err)
	for _, series := range []string{"rollup_verify_buckets_total", "rollup_drift_buckets_total",
		"rollup_verify_duration_seconds", "rollup_newest_bucket_timestamp_seconds"} {
		assert.That(t, strings.Contains(string(text), series))
	}

	exec(t, conn, `ALTER TABLE posts_by_lang_5m ENABLE TRIGGER sqlflow_rollup_posts_by_lang_15m_ins`)
	exec(t, conn, `ALTER TABLE posts_by_lang_5m ENABLE TRIGGER sqlflow_rollup_posts_by_lang_15m_upd`)
	exec(t, conn, `UPDATE posts_per_minute_by_lang SET posts = posts WHERE bucket = '2026-09-12T23:58:00Z'`)
	waitFor(t, "healthy again", func() bool { return status(d) == "healthy" })
}
