package freshness

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

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
	return dsn, connect(t, dsn)
}

func connect(t *testing.T, dsn string) *pgx.Conn {
	t.Helper()
	conn, err := pgx.Connect(context.Background(), dsn)
	assert.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	return conn
}

func exec(t *testing.T, conn *pgx.Conn, sql string) {
	t.Helper()
	_, err := conn.Exec(context.Background(), sql)
	assert.NoError(t, err)
}

func TestIntegrationRollupRun_ObserveReadsTheNewestBucket(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	_, conn := startPostgres(t)
	ctx := context.Background()
	exec(t, conn, "CREATE TABLE posts_1h (bucket TIMESTAMPTZ PRIMARY KEY, posts BIGINT)")

	o, err := Observe(ctx, conn, "posts_1h", "bucket", time.Hour)
	assert.NoError(t, err)
	assert.Equal(t, "public.posts_1h", o.Table)
	assert.That(t, o.NewestBucketAt == nil)
	_, ok := o.Age()
	assert.False(t, ok)

	exec(t, conn, "INSERT INTO posts_1h VALUES (date_trunc('hour', now()) - interval '2 hours', 1), (date_trunc('hour', now()) - interval '1 hour', 2)")
	o, err = Observe(ctx, conn, "posts_1h", "bucket", time.Hour)
	assert.NoError(t, err)
	age, ok := o.Age()
	assert.True(t, ok)
	assert.That(t, age >= 0 && age < time.Hour)
}

// Two hostnames for one server name one store. Another database on the
// same server is another store.
func TestIntegrationRollupRun_OneStoreIDForTwoHostnames(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	dsn, conn := startPostgres(t)
	ctx := context.Background()
	assert.That(t, strings.Contains(dsn, "localhost"))
	byIP := connect(t, strings.Replace(dsn, "localhost", "127.0.0.1", 1))

	a, err := StoreOf(ctx, conn)
	assert.NoError(t, err)
	b, err := StoreOf(ctx, byIP)
	assert.NoError(t, err)
	assert.Equal(t, KindSystem, a.Kind)
	assert.Equal(t, a, b)

	exec(t, conn, "CREATE DATABASE other")
	other, err := StoreOf(ctx, connect(t, strings.Replace(dsn, "/rollup?", "/other?", 1)))
	assert.NoError(t, err)
	assert.That(t, other.ID != a.ID)
}

// A server that refuses pg_control_system() still gets an id, marked as
// one another reporter may not share.
func TestIntegrationRollupRun_AStoreThatRefusesTheControlFunctionIsNamedByAddress(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	dsn, conn := startPostgres(t)
	exec(t, conn, "REVOKE EXECUTE ON FUNCTION pg_control_system() FROM PUBLIC")
	exec(t, conn, "CREATE ROLE reader LOGIN PASSWORD 'reader'")
	reader := connect(t, strings.Replace(dsn, "rollup:rollup@", "reader:reader@", 1))

	s, err := StoreOf(context.Background(), reader)
	assert.NoError(t, err)
	assert.Equal(t, KindAddress, s.Kind)
	assert.That(t, strings.HasPrefix(s.ID, "pg:") && len(s.ID) == 19)
}
