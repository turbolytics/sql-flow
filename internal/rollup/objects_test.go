package rollup

import (
	"strings"
	"testing"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

func TestCliRollupRun_PostgresObjectsIsTheMigrationWithoutItsLockAndBackfill(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")

	conf := loadExample(t)
	objects, err := PostgresObjects(conf.Rollups[0])
	assert.NoError(t, err)

	assert.False(t, strings.Contains(objects, "LOCK TABLE"))
	assert.False(t, strings.Contains(objects, "\nINSERT INTO"))
	assert.False(t, strings.Contains(objects, "rollup_backfill = on"))
	assert.Equal(t, 10, strings.Count(objects, "CREATE TABLE IF NOT EXISTS"))
	assert.Equal(t, 10, strings.Count(objects, "CREATE OR REPLACE FUNCTION"))
	assert.Equal(t, 20, strings.Count(objects, "CREATE OR REPLACE TRIGGER"))

	// Byte for byte the migration's own statements, so install and a team
	// that applies `rollup ddl` build the same database.
	ddl, err := PostgresDDL(conf)
	assert.NoError(t, err)
	assert.That(t, strings.Contains(ddl, objects))
}

func TestCliRollupRun_PostgresObjectsRefusesANameTooLong(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")

	r := loadExample(t).Rollups[0]
	r.DimensionSets[1].Name = "posts_total_" + strings.Repeat("x", 40)
	_, err := PostgresObjects(r)
	assert.Error(t, err)
	assert.That(t, strings.Contains(err.Error(), "63"))
}

func TestCliRollupRun_EdgesNameEachTableAndWhatItIsBuiltFrom(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")

	es := edges(loadExample(t).Rollups[0])
	assert.Equal(t, 10, len(es))
	assert.Equal(t, "posts_by_lang_5m", es[0].Table)
	assert.Equal(t, "posts_per_minute_by_lang", es[0].From)
	assert.Equal(t, 0, es[0].SetIndex)

	from := map[string]string{}
	for _, e := range es {
		from[e.Table] = e.From
	}
	assert.Equal(t, "posts_by_lang_5m", from["posts_by_lang_15m"])
	assert.Equal(t, "posts_total_6h", from["posts_total_1d"])
	assert.Equal(t, 1, es[9].SetIndex)
}
