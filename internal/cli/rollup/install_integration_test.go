package rollup

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

func TestIntegrationRollupRun_InstallCommandCreatesTheTables(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
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
	_, err = conn.Exec(ctx, `CREATE TABLE posts_per_minute_by_lang (
  bucket TIMESTAMPTZ NOT NULL, lang TEXT NOT NULL, posts INTEGER NOT NULL,
  PRIMARY KEY (bucket, lang))`)
	assert.NoError(t, err)

	path := withStore(t, dsn)
	out, _, err := run(t, "install", "-c", path)
	assert.NoError(t, err)
	assert.That(t, strings.Contains(out, "rollup posts: created posts_by_lang_5m\n"))
	assert.That(t, strings.Contains(out, "rollup posts: posts_by_lang_5m awaits a backfill\n"))

	out, _, err = run(t, "install", "-c", path)
	assert.NoError(t, err)
	assert.False(t, strings.Contains(out, "created"))
	assert.That(t, strings.Contains(out, "rollup posts: functions and triggers are current\n"))
}
