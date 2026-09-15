package rollup

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/turbolytics/sql-flow/internal/errs"
	"github.com/zeebo/assert"
)

const example = "../../../dev/config/rollups/bluesky.yml"

func run(t *testing.T, args ...string) (string, string, error) {
	t.Helper()
	cmd := NewCommand()
	var out, stderr bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&stderr)
	cmd.SetArgs(args)
	err := cmd.Execute()
	return out.String(), stderr.String(), err
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	assert.NoError(t, err)
	return string(b)
}

func TestCliRollup_DDLPrintsTheMigration(t *testing.T) {
	coverage.Covers(t, "cli.rollup")

	out, _, err := run(t, "ddl", "-c", example)
	assert.NoError(t, err)
	assert.Equal(t, readFile(t, "../../rollup/testdata/bluesky.postgres.sql"), out)

	_, _, err = run(t, "ddl", "-c", example, "--backend", "clickhouse")
	assert.Error(t, err)
	assert.That(t, strings.Contains(err.Error(), "the only backend is postgres"))
}

func TestCliRollup_ServePrintsTheDatasets(t *testing.T) {
	coverage.Covers(t, "cli.rollup")

	out, _, err := run(t, "serve", "-c", example)
	assert.NoError(t, err)
	assert.Equal(t, readFile(t, "../../rollup/testdata/bluesky.serve.yml"), out)

	_, _, err = run(t, "serve", "-c", example, "--dataset", "nope")
	assert.Error(t, err)
}

// writeCommitted writes what a repository adopting the example commits, and
// returns the migration's and the serve file's paths.
func writeCommitted(t *testing.T) (string, string) {
	t.Helper()
	dir := t.TempDir()

	ddl, _, err := run(t, "ddl", "-c", example)
	assert.NoError(t, err)
	migration := filepath.Join(dir, "0005_rollups.sql")
	assert.NoError(t, os.WriteFile(migration, []byte(ddl), 0o644))

	datasets, _, err := run(t, "serve", "-c", example)
	assert.NoError(t, err)
	var serve strings.Builder
	serve.WriteString("serve:\n  auth:\n    tokens:\n      - {name: page, token: \"{{ SQLFLOW_SERVE_TOKEN }}\"}\n  datasets:\n")
	for _, line := range strings.Split(strings.TrimRight(datasets, "\n"), "\n") {
		serve.WriteString("    " + line + "\n")
	}
	servePath := filepath.Join(dir, "serve.yml")
	assert.NoError(t, os.WriteFile(servePath, []byte(serve.String()), 0o644))
	return migration, servePath
}

func TestCliRollup_CheckPassesTheGeneratedFiles(t *testing.T) {
	coverage.Covers(t, "cli.rollup")
	migration, serve := writeCommitted(t)

	out, _, err := run(t, "check", "-c", example, "--migration", migration, "--serve", serve)
	assert.NoError(t, err)
	assert.That(t, strings.Contains(out, "agree"))
}

// CI reads the exit code, and the reader reads which file and key drifted.
func TestCliRollup_CheckFailsOnDriftWithExit10(t *testing.T) {
	coverage.Covers(t, "cli.rollup")
	migration, serve := writeCommitted(t)
	text := readFile(t, migration)
	assert.NoError(t, os.WriteFile(migration, []byte(strings.Replace(text, "NULLS NOT DISTINCT", "", 1)), 0o644))

	_, stderr, err := run(t, "check", "-c", example, "--migration", migration, "--serve", serve)
	assert.Error(t, err)
	assert.Equal(t, errs.CodeConfigRollupDrift, errs.CodeOf(err))
	assert.Equal(t, errs.ExitUserError, errs.ExitCode(err))
	assert.That(t, strings.Contains(stderr, migration+": migration: [user.config.rollup_drift]"))
}

func TestCliRollup_CommandsRequireTheirFiles(t *testing.T) {
	coverage.Covers(t, "cli.rollup")

	_, _, err := run(t, "ddl")
	assert.Error(t, err)
	_, _, err = run(t, "check", "-c", example)
	assert.Error(t, err)
	_, _, err = run(t, "ddl", "-c", "does-not-exist.yml")
	assert.Error(t, err)
}
