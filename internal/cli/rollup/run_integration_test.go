package rollup

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

// The command fills history, then stops cleanly on its context, as it does
// on SIGTERM.
func TestIntegrationRollupRun_RunFillsHistoryAndStopsCleanly(t *testing.T) {
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
	for _, sql := range []string{
		`CREATE TABLE posts_per_minute_by_lang (
  bucket TIMESTAMPTZ NOT NULL, lang TEXT NOT NULL, posts INTEGER NOT NULL,
  PRIMARY KEY (bucket, lang))`,
		`INSERT INTO posts_per_minute_by_lang (bucket, lang, posts)
SELECT g, 'en', 1 FROM generate_series('2026-09-12T00:00:00Z'::timestamptz, '2026-09-12T23:59:00Z', interval '1 minute') AS g`,
	} {
		_, err := conn.Exec(ctx, sql)
		assert.NoError(t, err)
	}

	rctx, cancel := context.WithCancel(ctx)
	cmd := NewCommand()
	var out, stderr bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&stderr)
	cmd.SetArgs([]string{"run", "-c", withStore(t, dsn)})
	done := make(chan error, 1)
	go func() { done <- cmd.ExecuteContext(rctx) }()

	deadline := time.Now().Add(60 * time.Second)
	for {
		var total int64
		err := conn.QueryRow(ctx, "SELECT coalesce(sum(posts), 0) FROM posts_total_1d").Scan(&total)
		if err == nil && total == 1440 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("run did not fill the day: total %d, err %v", total, err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	cancel()
	select {
	case err := <-done:
		assert.NoError(t, err)
	case <-time.After(30 * time.Second):
		t.Fatal("run did not stop within 30s of its context ending")
	}
}
