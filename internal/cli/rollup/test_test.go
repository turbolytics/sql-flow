package rollup

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/turbolytics/sql-flow/internal/errs"
	"github.com/zeebo/assert"
)

const exampleTests = "../../../dev/config/rollups/bluesky.test.yml"

// unreachable is a DSN naming a port nothing listens on: a test that
// passes with it never connected.
const unreachable = "postgres://rollup@127.0.0.1:1/rollup"

func TestCliRollupTest_TestIsListed(t *testing.T) {
	coverage.Covers(t, "cli.rollup_test")

	out, _, err := run(t, "--help")
	assert.NoError(t, err)
	assert.That(t, strings.Contains(out, "\n  test "))
}

func TestCliRollupTest_TheDSNIsRequired(t *testing.T) {
	coverage.Covers(t, "cli.rollup_test")

	_, _, err := run(t, "test", "-c", example)
	assert.Error(t, err)
	assert.That(t, strings.Contains(err.Error(), `"dsn"`))
}

// A malformed --dsn names the flag, not the store, and never echoes the
// value, which can hold a password.
func TestCliRollupTest_ABadDSNIsAConfigError(t *testing.T) {
	coverage.Covers(t, "cli.rollup_test")

	_, _, err := run(t, "test", "-c", example, "--dsn", "postgres://rollup:s3cret@%zz")
	assert.Error(t, err)
	assert.Equal(t, errs.CodeConfigInvalid, errs.CodeOf(err))
	assert.That(t, strings.Contains(err.Error(), "--dsn is not a Postgres connection string"))
	assert.False(t, strings.Contains(err.Error(), "s3cret"))
}

// A test file that breaks a rule stops the command before it connects.
func TestCliRollupTest_ABadTestFileStopsBeforeConnecting(t *testing.T) {
	coverage.Covers(t, "cli.rollup_test")

	path := filepath.Join(t.TempDir(), "bad.test.yml")
	assert.NoError(t, os.WriteFile(path, []byte("tests:\n  - name: x\n    rollup: nope\n    writes: [[{bucket: \"2026-09-24T12:07:00Z\"}]]\n"), 0o644))
	_, _, err := run(t, "test", "-c", example, "--dsn", unreachable, "--tests", path)
	assert.Error(t, err)
	assert.Equal(t, errs.CodeConfigRollup, errs.CodeOf(err))
	assert.That(t, strings.Contains(err.Error(), "tests.0.rollup"))
}

// A failed test is the user's declaration, so it exits 10 like any other
// config error.
func TestCliRollupTest_AFailedTestIsAUserCode(t *testing.T) {
	coverage.Covers(t, "cli.rollup_test")

	assert.Equal(t, 10, errs.ExitCode(errs.New(errs.CodeConfigRollupTestFailed, "1 of 3 checks failed")))
}

// A connect failure names the flag the user set, not the store the file
// declares, which the command never reads.
func TestCliRollupTest_AnUnreachableDSNNamesTheFlag(t *testing.T) {
	coverage.Covers(t, "cli.rollup_test")

	_, _, err := run(t, "test", "-c", example, "--dsn", unreachable)
	assert.Error(t, err)
	assert.Equal(t, errs.CodeRollupUnreachable, errs.CodeOf(err))
	assert.That(t, strings.Contains(err.Error(), "--dsn 127.0.0.1/rollup"))
	assert.False(t, strings.Contains(err.Error(), "rollup store"))
}
