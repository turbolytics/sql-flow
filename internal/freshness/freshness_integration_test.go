package freshness

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/turbolytics/sql-flow/internal/pgtest"
	"github.com/zeebo/assert"
)

// sharedPostgres is the package's one container.
var sharedPostgres = pgtest.New(pgtest.Options{User: "rollup", Password: "rollup"})

func TestMain(m *testing.M) { os.Exit(sharedPostgres.Run(m)) }

// startPostgres gives the test an empty database of its own on the
// package's container.
func startPostgres(t *testing.T) (string, *pgx.Conn) {
	t.Helper()
	dsn, _ := sharedPostgres.Database(t)
	return dsn, connect(t, dsn)
}

// databaseOf is the database conn is connected to.
func databaseOf(t *testing.T, conn *pgx.Conn) string {
	t.Helper()
	var db string
	assert.NoError(t, conn.QueryRow(context.Background(), "SELECT current_database()").Scan(&db))
	return db
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
	t.Parallel()
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
	t.Parallel()
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

	// Database names are server-wide, and every test's database shares the server.
	db := databaseOf(t, conn)
	otherDB := db + "_other"
	exec(t, conn, "CREATE DATABASE "+otherDB)
	t.Cleanup(func() { _, _ = conn.Exec(context.Background(), "DROP DATABASE "+otherDB+" WITH (FORCE)") })
	other, err := StoreOf(ctx, connect(t, strings.Replace(dsn, "/"+db+"?", "/"+otherDB+"?", 1)))
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
	t.Parallel()
	dsn, conn := startPostgres(t)
	exec(t, conn, "REVOKE EXECUTE ON FUNCTION pg_control_system() FROM PUBLIC")
	// Role names are server-wide, and every test's database shares the server.
	role := databaseOf(t, conn) + "_reader"
	exec(t, conn, "CREATE ROLE "+role+" LOGIN PASSWORD 'reader'")
	reader := connect(t, strings.Replace(dsn, "rollup:rollup@", role+":reader@", 1))

	s, err := StoreOf(context.Background(), reader)
	assert.NoError(t, err)
	assert.Equal(t, KindAddress, s.Kind)
	assert.That(t, strings.HasPrefix(s.ID, "pg:") && len(s.ID) == 19)
}
