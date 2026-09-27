package rollup

import (
	"strings"
	"testing"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

func TestIntegrationRollupRun_InstallCommandCreatesTheTables(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	t.Parallel()
	dsn, _ := sourceOnly(t)

	path := withStore(t, dsn)
	out, _, err := run(t, "install", "-c", path)
	assert.NoError(t, err)
	assert.That(t, strings.Contains(out, "rollup posts: created posts_by_lang_5m\n"))
	assert.That(t, strings.Contains(out, "rollup posts: posts_by_lang_5m awaits a backfill\n"))

	out, _, err = run(t, "install", "-c", path)
	assert.NoError(t, err)
	assert.False(t, strings.Contains(out, "created"))
	assert.That(t, strings.Contains(out, "rollup posts: functions and triggers are current\n"))
}
