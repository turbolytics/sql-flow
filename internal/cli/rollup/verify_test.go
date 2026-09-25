package rollup

import (
	"strings"
	"testing"
	"time"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/turbolytics/sql-flow/internal/errs"
	"github.com/zeebo/assert"
)

func TestCliRollupRun_VerifyIsListed(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")

	out, _, err := run(t, "--help")
	assert.NoError(t, err)
	assert.That(t, strings.Contains(out, "\n  verify "))
}

func TestCliRollupRun_VerifyRefusesAnEmptyDSN(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")

	_, _, err := run(t, "verify", "-c", withStore(t, ""))
	assert.Error(t, err)
	assert.Equal(t, errs.CodeConfigInvalid, errs.CodeOf(err))
}

// A bad --since fails before anything connects: the DSN names a port
// nothing listens on.
func TestCliRollupRun_VerifyRefusesABadSince(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")

	_, _, err := run(t, "verify", "-c", withStore(t, "postgres://rollup@127.0.0.1:1/rollup"), "--since", "yesterday")
	assert.Error(t, err)
	assert.Equal(t, errs.CodeConfigInvalid, errs.CodeOf(err))
}

func TestCliRollupRun_SinceTakesATimeOrADuration(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")

	now := time.Date(2026, 9, 25, 12, 0, 0, 0, time.UTC)
	got, err := parseSince("", now)
	assert.NoError(t, err)
	assert.That(t, got == nil)
	got, err = parseSince("2026-09-01T00:00:00Z", now)
	assert.NoError(t, err)
	assert.That(t, got.Equal(time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)))
	got, err = parseSince("36h", now)
	assert.NoError(t, err)
	assert.That(t, got.Equal(now.Add(-36*time.Hour)))
	got, err = parseSince("7d", now)
	assert.NoError(t, err)
	assert.That(t, got.Equal(now.Add(-7*24*time.Hour)))
}

// Drift is ours to report, not the user's config: a restart cannot fix
// it, but it is not a user error either.
func TestCliRollupRun_DriftIsASystemCode(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")

	err := errs.New(errs.CodeRollupDrift, "3 buckets differ")
	assert.Equal(t, errs.ExitInternal, errs.ExitCode(err))
	_, ok := errs.Lookup(errs.CodeRollupDrift)
	assert.True(t, ok)
}
