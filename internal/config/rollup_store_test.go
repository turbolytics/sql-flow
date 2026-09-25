package config

import (
	"strings"
	"testing"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/turbolytics/sql-flow/internal/errs"
	"github.com/zeebo/assert"
)

const postgresStore = "store:\n  type: postgres\n  postgres:\n    dsn: postgres://rollup@db:5432/rollup\n"

func TestCliRollupRun_AStoreWithADSNPasses(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")

	conf := parseRollups(t, postgresStore+validRollups)
	assert.Equal(t, 0, len(conf.Check()))
	dsn, err := conf.PostgresDSN()
	assert.NoError(t, err)
	assert.Equal(t, "postgres://rollup@db:5432/rollup", dsn)
}

func TestCliRollupRun_StoreRulesNameTheKey(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")

	for _, c := range []struct{ name, store, path, says string }{
		{"a store this version does not run", "store:\n  type: clickhouse\n", "store.type", "use postgres"},
		{"postgres without its block", "store:\n  type: postgres\n", "store.postgres", "is required"},
	} {
		t.Run(c.name, func(t *testing.T) {
			v := parseRollups(t, c.store+validRollups).Check()
			assert.Equal(t, 1, len(v))
			assert.Equal(t, errs.CodeConfigRollup, v[0].Code)
			assert.Equal(t, c.path, strings.Join(v[0].Path, "."))
			assert.That(t, strings.Contains(v[0].Message, c.says))
		})
	}
}

// validate renders an unset variable as an empty string and must pass the
// file, so an empty DSN is refused where a command connects instead.
func TestCliRollupRun_AnEmptyDSNValidatesAndCannotConnect(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")

	empty := parseRollups(t, "store:\n  type: postgres\n  postgres:\n    dsn: \"\"\n"+validRollups)
	assert.Equal(t, 0, len(empty.Check()))
	_, err := empty.PostgresDSN()
	assert.Error(t, err)
	assert.Equal(t, errs.CodeConfigInvalid, errs.CodeOf(err))
	assert.That(t, strings.Contains(err.Error(), "store.postgres.dsn"))

	// No store block at all is refused the same way.
	_, err = parseRollups(t, validRollups).PostgresDSN()
	assert.Error(t, err)
}

func TestCliRollupRun_TurboStatsRulesApplyAtTheirKey(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")

	v := parseRollups(t, "turbostats:\n  labels: {id: demo}\n"+validRollups).Check()
	assert.Equal(t, 1, len(v))
	assert.Equal(t, "turbostats.labels", strings.Join(v[0].Path, "."))
}
