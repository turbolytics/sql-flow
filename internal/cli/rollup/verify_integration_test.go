package rollup

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/turbolytics/sql-flow/internal/errs"
	gen "github.com/turbolytics/sql-flow/internal/rollup"
	"github.com/zeebo/assert"
)

// migrated starts a Postgres, writes two days of history, and applies
// `rollup ddl`'s migration, which fills every table and creates no state
// table.
func migrated(t *testing.T) (string, *pgx.Conn) {
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
	conf, err := config.LoadRollups(example)
	assert.NoError(t, err)
	script, err := gen.PostgresDDL(conf)
	assert.NoError(t, err)
	for _, sql := range []string{
		`CREATE TABLE posts_per_minute_by_lang (
  bucket TIMESTAMPTZ NOT NULL, lang TEXT NOT NULL, posts INTEGER NOT NULL,
  PRIMARY KEY (bucket, lang))`,
		`INSERT INTO posts_per_minute_by_lang (bucket, lang, posts)
SELECT g, l, 1 FROM generate_series('2026-09-11T00:00:00Z'::timestamptz, '2026-09-12T23:59:00Z', interval '1 minute') AS g,
       unnest(ARRAY['en', 'ja']) AS l`,
		script,
	} {
		_, err := conn.Exec(ctx, sql)
		assert.NoError(t, err)
	}
	return dsn, conn
}

func sqlExec(t *testing.T, conn *pgx.Conn, sql string) {
	t.Helper()
	_, err := conn.Exec(context.Background(), sql)
	assert.NoError(t, err)
}

// verify reads a database no daemon manages, and takes no lock: an install
// held open elsewhere does not stop it.
func TestIntegrationRollupRun_VerifyCommandPassesACleanDatabase(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	dsn, _ := migrated(t)
	holder, err := pgx.Connect(context.Background(), dsn)
	assert.NoError(t, err)
	t.Cleanup(func() { _ = holder.Close(context.Background()) })
	sqlExec(t, holder, "SELECT pg_advisory_lock(hashtextextended('sqlflow_rollup_install', 0))")

	out, _, err := run(t, "verify", "-c", withStore(t, dsn))
	assert.NoError(t, err)
	assert.Equal(t, 10, strings.Count(out, ", 0 differ"))
	assert.That(t, strings.Contains(out, "posts_by_lang_15m: 192 buckets checked against posts_by_lang_5m, 0 differ"))
}

func TestIntegrationRollupRun_VerifyCommandFailsOnDriftAndNamesTheBucket(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	dsn, conn := migrated(t)
	sqlExec(t, conn, `ALTER TABLE posts_by_lang_5m DISABLE TRIGGER sqlflow_rollup_posts_by_lang_15m_ins`)
	sqlExec(t, conn, `ALTER TABLE posts_by_lang_5m DISABLE TRIGGER sqlflow_rollup_posts_by_lang_15m_upd`)
	sqlExec(t, conn, `UPDATE posts_per_minute_by_lang SET posts = 7 WHERE bucket = '2026-09-11T10:02:00Z' AND lang = 'en'`)

	out, _, err := run(t, "verify", "-c", withStore(t, dsn))
	assert.Error(t, err)
	assert.Equal(t, errs.CodeRollupDrift, errs.CodeOf(err))
	assert.That(t, strings.Contains(out, "posts_by_lang_15m: 192 buckets checked against posts_by_lang_5m, 1 differ"))
	assert.That(t, strings.Contains(out, "  2026-09-11T10:00:00Z differs lang=en: posts stored 15, recomputed 21"))

	// The drift is on 2026-09-11, so a check from the next day passes.
	out, _, err = run(t, "verify", "-c", withStore(t, dsn), "--since", "2026-09-12T00:00:00Z")
	assert.NoError(t, err)
	assert.That(t, strings.Contains(out, "posts_by_lang_15m: 96 buckets checked against posts_by_lang_5m, 0 differ"))
}
