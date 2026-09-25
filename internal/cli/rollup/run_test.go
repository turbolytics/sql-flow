package rollup

import (
	"strings"
	"testing"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/turbolytics/sql-flow/internal/errs"
	"github.com/zeebo/assert"
)

func TestCliRollupRun_RunRefusesAnEmptyDSN(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")

	_, _, err := run(t, "run", "-c", withStore(t, ""))
	assert.Error(t, err)
	assert.Equal(t, errs.CodeConfigInvalid, errs.CodeOf(err))
}

// A typo in the flag fails before anything connects.
func TestCliRollupRun_RunRefusesAnUnknownExporter(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")

	_, _, err := run(t, "run", "-c", withStore(t, "postgres://rollup@127.0.0.1:1/rollup"), "--metrics", "statsd")
	assert.Error(t, err)
	assert.Equal(t, errs.CodeConfigInvalid, errs.CodeOf(err))
}

func TestCliRollupRun_RunRequiresItsFile(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")

	_, _, err := run(t, "run")
	assert.Error(t, err)
	assert.That(t, strings.Contains(err.Error(), `required flag(s) "config" not set`))
}
