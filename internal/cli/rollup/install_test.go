package rollup

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/turbolytics/sql-flow/internal/errs"
	"github.com/zeebo/assert"
)

// withStore writes the example with a store block holding dsn, and returns
// its path.
func withStore(t *testing.T, dsn string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "rollups.yml")
	text := fmt.Sprintf("store:\n  type: postgres\n  postgres:\n    dsn: %q\n", dsn) + readFile(t, example)
	assert.NoError(t, os.WriteFile(path, []byte(text), 0o644))
	return path
}

func TestCliRollupRun_InstallIsHiddenUntilRunExists(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")

	install, _, err := NewCommand().Find([]string{"install"})
	assert.NoError(t, err)
	assert.Equal(t, "install", install.Name())
	assert.True(t, install.Hidden)
}

func TestCliRollupRun_InstallRefusesAnEmptyDSN(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")

	_, _, err := run(t, "install", "-c", withStore(t, ""))
	assert.Error(t, err)
	assert.Equal(t, errs.CodeConfigInvalid, errs.CodeOf(err))
	assert.Equal(t, errs.ExitUserError, errs.ExitCode(err))
}

// A deploy log is read by more people than the database's owner.
func TestCliRollupRun_InstallNamesAnUnreachableStoreWithoutItsPassword(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")

	dsn := "postgres://rollup:s3cret-pw@127.0.0.1:1/rollup?sslmode=disable&connect_timeout=2"
	_, stderr, err := run(t, "install", "-c", withStore(t, dsn))
	assert.Error(t, err)
	assert.Equal(t, errs.CodeRollupUnreachable, errs.CodeOf(err))
	assert.Equal(t, errs.ExitInternal, errs.ExitCode(err))
	assert.That(t, strings.Contains(err.Error(), "127.0.0.1"))
	assert.False(t, strings.Contains(err.Error(), "s3cret-pw"))
	assert.False(t, strings.Contains(stderr, "s3cret-pw"))
}
