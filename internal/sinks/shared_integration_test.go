package sinks

// The integration tests share one Docker network, one Postgres and one
// ClickHouse per test binary. Each container used to start once per test,
// and a start costs more than most tests' own work.
//
// Isolation comes from names, not containers. A Postgres test gets a database
// of its own, and a ClickHouse test names its tables from the clock. A fault
// goes through a proxy each test starts for itself, so breaking one test's
// path leaves every other test's alone.
//
// Each container starts on a test's first request, not in TestMain. The unit
// pass runs this binary with -short, skips every integration test, and so
// starts none.

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/testcontainers/testcontainers-go"
	tcclickhouse "github.com/testcontainers/testcontainers-go/modules/clickhouse"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/zeebo/assert"
)

// sharedMaxConnections leaves room for every parallel test's sessions on the
// one Postgres.
const sharedMaxConnections = "200"

var shared struct {
	networkOnce sync.Once
	network     *testcontainers.DockerNetwork
	networkErr  error

	postgresOnce sync.Once
	postgres     *tcpostgres.PostgresContainer
	postgresErr  error
	databases    atomic.Int64

	clickhouseOnce sync.Once
	clickhouse     *tcclickhouse.ClickHouseContainer
	clickhouseErr  error
}

// TestMain stops the containers a test started. A run killed before it
// returns leaves them to testcontainers' reaper.
func TestMain(m *testing.M) {
	code := m.Run()
	// Teardown must not change the verdict: the tests have already run. The
	// network goes last, because Docker refuses to remove one in use.
	ctx := context.Background()
	if shared.clickhouse != nil {
		_ = shared.clickhouse.Terminate(ctx)
	}
	if shared.postgres != nil {
		_ = shared.postgres.Terminate(ctx)
	}
	if shared.network != nil {
		_ = shared.network.Remove(ctx)
	}
	os.Exit(code)
}

// sharedNetwork is the network the shared containers and every test's fault
// proxy join, so a proxy reaches a server by its alias.
func sharedNetwork(t *testing.T) *testcontainers.DockerNetwork {
	t.Helper()
	shared.networkOnce.Do(func() {
		shared.network, shared.networkErr = network.New(context.Background())
	})
	if shared.networkErr != nil {
		t.Fatalf("docker network: %v", shared.networkErr)
	}
	return shared.network
}

func sharedPostgres(t *testing.T) *tcpostgres.PostgresContainer {
	t.Helper()
	nw := sharedNetwork(t)
	shared.postgresOnce.Do(func() {
		// Run can return a container with its error, and TestMain stops it.
		shared.postgres, shared.postgresErr = tcpostgres.Run(context.Background(), postgresImage,
			network.WithNetwork([]string{"postgres"}, nw),
			tcpostgres.WithDatabase(postgresDatabase),
			tcpostgres.WithUsername(postgresUser),
			tcpostgres.WithPassword(postgresPassword),
			tcpostgres.BasicWaitStrategies(),
			testcontainers.WithCmdArgs("-c", "max_connections="+sharedMaxConnections),
		)
	})
	if shared.postgresErr != nil {
		t.Fatalf("start postgres: %v", shared.postgresErr)
	}
	return shared.postgres
}

// newDatabase creates a database for t on the shared Postgres and returns
// its name. t's cleanup drops it.
func newDatabase(t *testing.T, pg *tcpostgres.PostgresContainer) string {
	t.Helper()
	admin, err := pg.ConnectionString(context.Background(), "sslmode=disable")
	assert.NoError(t, err)
	name := fmt.Sprintf("sinks_%d", shared.databases.Add(1))
	if err := execOnce(admin, "CREATE DATABASE "+name); err != nil {
		t.Fatalf("create database %s: %v", name, err)
	}
	t.Cleanup(func() {
		// FORCE ends the sessions a test left open, as stopping its own
		// container did. Teardown must not change the verdict.
		if err := execOnce(admin, "DROP DATABASE "+name+" WITH (FORCE)"); err != nil {
			t.Logf("drop database %s: %v", name, err)
		}
	})
	return name
}

// execOnce runs one statement on a connection of its own, so parallel tests
// never share one.
func execOnce(dsn, sql string) error {
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(ctx) }()
	_, err = conn.Exec(ctx, sql)
	return err
}

// withDatabase is dsn pointed at another database on the same server.
func withDatabase(t *testing.T, dsn, database string) string {
	t.Helper()
	u, err := url.Parse(dsn)
	assert.NoError(t, err)
	u.Path = "/" + database
	return u.String()
}

func sharedClickhouse(t *testing.T) *tcclickhouse.ClickHouseContainer {
	t.Helper()
	nw := sharedNetwork(t)
	shared.clickhouseOnce.Do(func() {
		// Run can return a container with its error, and TestMain stops it.
		shared.clickhouse, shared.clickhouseErr = tcclickhouse.Run(context.Background(), clickhouseImage,
			network.WithNetwork([]string{"clickhouse"}, nw),
			testcontainers.WithExposedPorts("8123/tcp"),
			tcclickhouse.WithUsername(clickhouseUser),
			tcclickhouse.WithPassword(clickhousePassword),
			tcclickhouse.WithDatabase(clickhouseDatabase),
		)
	})
	if shared.clickhouseErr != nil {
		t.Fatalf("start clickhouse: %v", shared.clickhouseErr)
	}
	return shared.clickhouse
}
