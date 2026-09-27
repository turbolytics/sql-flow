// Package pgtest gives a test package one Postgres container and each of its
// tests a database of its own.
//
// A container takes about a second to start, and most integration tests run
// milliseconds of SQL. One container per test binary takes that second once,
// and a database per test lets the tests run in parallel.
//
// Each test gets a database, not a schema. Advisory locks, pg_locks rows and
// ALTER DATABASE settings belong to a database, and the rollup tests reuse
// lock keys, so tests that shared a database would block each other.
//
// The container starts on first use, not in TestMain. The unit pass runs the
// same test binary with -short, skips every integration test, and must start
// nothing.
package pgtest

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
)

// Image is the Postgres the Bluesky demo and the Render template run. The
// rollup DDL needs 15 or later for NULLS NOT DISTINCT.
const Image = "postgres:18"

// template is the database every test's database copies.
const template = "pgtest_template"

// maxConnections leaves room for every parallel test's sessions. The
// concurrent-writer tests open six each, and Go runs GOMAXPROCS tests at once.
// The rollup lock-table test sizes its history from this setting, so a larger
// value makes that test slower.
const maxConnections = "200"

// Options describe a package's server.
type Options struct {
	// User and Password name the superuser every test connects as.
	User, Password string
	// Template holds the statements every test's database starts with.
	Template []string
}

// Server is one package's Postgres container.
type Server struct {
	opts Options

	once      sync.Once
	err       error
	container *tcpostgres.PostgresContainer
	base      url.URL
	next      atomic.Int64
}

// New describes a server. Nothing starts until a test asks for a database.
func New(opts Options) *Server {
	return &Server{opts: opts}
}

// Run runs the tests, then stops the container if a test started it.
// TestMain calls it as os.Exit(server.Run(m)). A run killed before it
// returns leaves the container to testcontainers' reaper.
func (s *Server) Run(m *testing.M) int {
	code := m.Run()
	if s.container != nil {
		_ = s.container.Terminate(context.Background())
	}
	return code
}

// Database creates a database for t from the template and returns its DSN and
// name. t's cleanup drops it.
func (s *Server) Database(t testing.TB) (string, string) {
	t.Helper()
	s.once.Do(s.start)
	if s.err != nil {
		t.Fatalf("pgtest: %v", s.err)
	}
	name := fmt.Sprintf("test_%d", s.next.Add(1))
	if err := s.exec(context.Background(), "postgres", "CREATE DATABASE "+name+" TEMPLATE "+template); err != nil {
		t.Fatalf("pgtest: create database %s: %v", name, err)
	}
	t.Cleanup(func() {
		// FORCE ends the sessions a test left open, as stopping its own
		// container did.
		if err := s.exec(context.Background(), "postgres", "DROP DATABASE "+name+" WITH (FORCE)"); err != nil {
			t.Errorf("pgtest: drop database %s: %v", name, err)
		}
	})
	return s.dsn(name), name
}

// Logs returns the server's log. Every test's sessions write to it, and each
// line names its database.
func (s *Server) Logs(ctx context.Context) (io.ReadCloser, error) {
	if s.container == nil {
		return nil, errors.New("pgtest: no container started")
	}
	return s.container.Logs(ctx)
}

func (s *Server) start() {
	ctx := context.Background()
	c, err := tcpostgres.Run(ctx, Image,
		tcpostgres.WithDatabase(template),
		tcpostgres.WithUsername(s.opts.User),
		tcpostgres.WithPassword(s.opts.Password),
		tcpostgres.BasicWaitStrategies(),
		testcontainers.WithCmdArgs(
			"-c", "max_connections="+maxConnections,
			"-c", "log_line_prefix=%m [%p] %d ",
		),
	)
	// Run can return a container with an error, and Run's caller stops it.
	s.container = c
	if err != nil {
		s.err = fmt.Errorf("start %s: %w", Image, err)
		return
	}
	raw, err := c.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		s.err = fmt.Errorf("connection string: %w", err)
		return
	}
	u, err := url.Parse(raw)
	if err != nil {
		s.err = fmt.Errorf("connection string: %w", err)
		return
	}
	s.base = *u
	if err := s.exec(ctx, template, s.opts.Template...); err != nil {
		s.err = fmt.Errorf("template: %w", err)
		return
	}
	// CREATE DATABASE refuses a template that any session is connected to.
	if err := s.exec(ctx, "postgres", "ALTER DATABASE "+template+" WITH IS_TEMPLATE true ALLOW_CONNECTIONS false"); err != nil {
		s.err = fmt.Errorf("template: %w", err)
	}
}

func (s *Server) dsn(database string) string {
	u := s.base
	u.Path = "/" + database
	return u.String()
}

// exec runs statements on a connection of its own, so parallel tests never
// share one.
func (s *Server) exec(ctx context.Context, database string, statements ...string) error {
	conn, err := pgx.Connect(ctx, s.dsn(database))
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(context.Background()) }()
	for _, sql := range statements {
		if _, err := conn.Exec(ctx, sql); err != nil {
			return err
		}
	}
	return nil
}
