package rollup

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/turbolytics/sql-flow/internal/errs"
	"github.com/zeebo/assert"
)

// sourceOnly starts a Postgres holding the demo's source table and nothing
// else: the database `sqlflow rollup test` expects, with the team's
// migrations applied and no rollups installed.
func sourceOnly(t *testing.T) (string, *pgx.Conn) {
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
	sqlExec(t, conn, `CREATE TABLE posts_per_minute_by_lang (
  bucket TIMESTAMPTZ NOT NULL, lang TEXT NOT NULL, posts INTEGER NOT NULL,
  PRIMARY KEY (bucket, lang))`)
	return dsn, conn
}

func countOf(t *testing.T, conn *pgx.Conn, sql string) int64 {
	t.Helper()
	var n int64
	assert.NoError(t, conn.QueryRow(context.Background(), sql).Scan(&n))
	return n
}

func sandboxes(t *testing.T, conn *pgx.Conn) int64 {
	t.Helper()
	return countOf(t, conn, "SELECT count(*) FROM pg_namespace WHERE nspname LIKE 'sqlflow_test_%'")
}

// wrongCase is a test file whose one case expects 8 posts where 9 land.
func wrongCase(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "wrong.test.yml")
	assert.NoError(t, os.WriteFile(path, []byte(`tests:
  - name: a wrong count
    rollup: posts
    writes:
      - [{bucket: "2026-09-24T12:07:00Z", lang: en, posts: 9}]
    expect:
      posts_by_lang_5m:
        - {bucket: "2026-09-24T12:05:00Z", lang: en, posts: 8}
`), 0o644))
	return path
}

// The demo passes the workload and its own cases, and the output names the
// seed that reproduces the run.
func TestIntegrationRollupTest_TheCommandPassesTheDemo(t *testing.T) {
	coverage.Covers(t, "cli.rollup_test")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	dsn, conn := sourceOnly(t)
	out, _, err := run(t, "test", "-c", example, "--dsn", dsn, "--tests", exampleTests, "--seed", "42")
	assert.NoError(t, err)
	assert.That(t, strings.HasPrefix(out, "seed 42\n"))
	assert.That(t, regexp.MustCompile(`(?m)^workload posts: 20 batches, \d+ rows: ok$`).MatchString(out))
	assert.That(t, strings.Contains(out, "case a republished minute replaces its count at every grain: ok\n"))
	assert.That(t, strings.Contains(out, "case minutes counts the source minutes present, across languages: ok\n"))
	assert.Equal(t, int64(0), sandboxes(t, conn))
	assert.Equal(t, int64(0), countOf(t, conn, "SELECT count(*) FROM posts_per_minute_by_lang"))
}

// A failing case exits with the test code and prints the table, the key
// and both rows.
func TestIntegrationRollupTest_AFailingCaseExitsWithTheCodeAndTheDiff(t *testing.T) {
	coverage.Covers(t, "cli.rollup_test")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	dsn, _ := sourceOnly(t)
	out, _, err := run(t, "test", "-c", example, "--dsn", dsn, "--tests", wrongCase(t))
	assert.Error(t, err)
	assert.Equal(t, errs.CodeConfigRollupTestFailed, errs.CodeOf(err))
	assert.That(t, strings.Contains(err.Error(), "1 of 2 checks failed"))
	assert.That(t, strings.Contains(out, "case a wrong count: failed\n"+
		"  posts_by_lang_5m differs bucket=2026-09-24T12:05:00Z, lang=en\n"+
		"    expected bucket=2026-09-24T12:05:00Z, lang=en, posts=8\n"+
		"    actual   bucket=2026-09-24T12:05:00Z, lang=en, posts=9\n"))
}

// The schema is dropped after a failing run, and kept with --keep, which
// prints its name.
func TestIntegrationRollupTest_TheCommandDropsItsSchemaUnlessKept(t *testing.T) {
	coverage.Covers(t, "cli.rollup_test")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	dsn, conn := sourceOnly(t)
	_, _, err := run(t, "test", "-c", example, "--dsn", dsn, "--tests", wrongCase(t))
	assert.Error(t, err)
	assert.Equal(t, int64(0), sandboxes(t, conn))

	out, _, err := run(t, "test", "-c", example, "--dsn", dsn, "--keep")
	assert.NoError(t, err)
	assert.Equal(t, int64(1), sandboxes(t, conn))
	kept := regexp.MustCompile(`(?m)^kept schema (sqlflow_test_[0-9a-f]{16})$`).FindStringSubmatch(out)
	assert.Equal(t, 2, len(kept))
	assert.That(t, countOf(t, conn, "SELECT count(*) FROM "+kept[1]+".posts_per_minute_by_lang") > 0)
}

// The command connects only to --dsn: a store naming a port nothing
// listens on does not stop it.
func TestIntegrationRollupTest_TheCommandNeverReadsTheStore(t *testing.T) {
	coverage.Covers(t, "cli.rollup_test")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	dsn, _ := sourceOnly(t)
	_, _, err := run(t, "test", "-c", withStore(t, unreachable), "--dsn", dsn)
	assert.NoError(t, err)
}

// cancelWhen returns a context that a watcher on its own connection cancels
// once ready reports true.
func cancelWhen(t *testing.T, dsn string, ready func(*pgx.Conn) bool) context.Context {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() {
		watch, err := pgx.Connect(context.Background(), dsn)
		if err != nil {
			return
		}
		defer watch.Close(context.Background())
		for ctx.Err() == nil {
			if ready(watch) {
				cancel()
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
	}()
	return ctx
}

// sandboxName is the one sqlflow_test_ schema, or empty.
func sandboxName(watch *pgx.Conn) string {
	var name string
	_ = watch.QueryRow(context.Background(),
		"SELECT nspname FROM pg_namespace WHERE nspname LIKE 'sqlflow_test_%'").Scan(&name)
	return name
}

func runIn(ctx context.Context, args ...string) error {
	cmd := NewCommand()
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs(args)
	return cmd.ExecuteContext(ctx)
}

// A run cancelled while its schema is being built, as Ctrl-C or a CI cancel
// can, still drops the schema. pgx closes a connection whose query was
// cancelled, so the cleanup cannot use it.
func TestIntegrationRollupTest_ARunCancelledDuringSetupDropsItsSchema(t *testing.T) {
	coverage.Covers(t, "cli.rollup_test")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	dsn, conn := sourceOnly(t)
	ctx := cancelWhen(t, dsn, func(watch *pgx.Conn) bool { return sandboxName(watch) != "" })
	assert.Error(t, runIn(ctx, "test", "-c", example, "--dsn", dsn))
	assert.Equal(t, int64(0), sandboxes(t, conn))
}

// A run cancelled once the workload has written rows drops its schema too:
// the command's own cleanup, not the sandbox's, runs then.
func TestIntegrationRollupTest_ARunCancelledMidWorkloadDropsItsSchema(t *testing.T) {
	coverage.Covers(t, "cli.rollup_test")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	dsn, conn := sourceOnly(t)
	ctx := cancelWhen(t, dsn, func(watch *pgx.Conn) bool {
		name := sandboxName(watch)
		if name == "" {
			return false
		}
		var n int64
		err := watch.QueryRow(context.Background(), "SELECT count(*) FROM "+name+".posts_per_minute_by_lang").Scan(&n)
		return err == nil && n > 0
	})
	assert.Error(t, runIn(ctx, "test", "-c", example, "--dsn", dsn))
	assert.Equal(t, int64(0), sandboxes(t, conn))
}
