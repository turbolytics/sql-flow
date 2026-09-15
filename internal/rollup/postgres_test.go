package rollup

import (
	"strings"
	"testing"
	"time"

	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/turbolytics/sql-flow/internal/errs"
	"github.com/zeebo/assert"
)

func TestCliRollup_PostgresDDLMatchesTheGolden(t *testing.T) {
	coverage.Covers(t, "cli.rollup")

	got, err := PostgresDDL(loadExample(t))
	assert.NoError(t, err)
	golden(t, "testdata/bluesky.postgres.sql", got)
}

// Adding '1 day' to a timestamptz steps a calendar day in the session's zone:
// 23 or 25 hours across a daylight saving change.
func TestCliRollup_IntervalsNeverCountDays(t *testing.T) {
	coverage.Covers(t, "cli.rollup")

	assert.Equal(t, "INTERVAL '24 hours'", interval(24*time.Hour))
	assert.Equal(t, "INTERVAL '6 hours'", interval(6*time.Hour))
	assert.Equal(t, "INTERVAL '15 minutes'", interval(15*time.Minute))
	assert.Equal(t, "INTERVAL '90 seconds'", interval(90*time.Second))

	got, err := PostgresDDL(loadExample(t))
	assert.NoError(t, err)
	assert.False(t, strings.Contains(got, " day"))
}

func TestCliRollup_PostgresDDLHasTheShapeTheSpecNames(t *testing.T) {
	coverage.Covers(t, "cli.rollup")

	got, err := PostgresDDL(loadExample(t))
	assert.NoError(t, err)

	// The source lock comes before anything that could let a write in.
	assert.That(t, strings.Index(got, "LOCK TABLE") < strings.Index(got, "CREATE TABLE"))
	assert.Equal(t, 10, strings.Count(got, "CREATE TABLE IF NOT EXISTS"))
	assert.Equal(t, 10, strings.Count(got, "NULLS NOT DISTINCT"))
	assert.Equal(t, 10, strings.Count(got, "CREATE OR REPLACE FUNCTION"))
	assert.Equal(t, 20, strings.Count(got, "CREATE OR REPLACE TRIGGER"))
	// Only the two 5m tables are built from the source, so only they backfill.
	assert.Equal(t, 2, strings.Count(got, "\nINSERT INTO"))
	assert.That(t, strings.Contains(got, `ON "posts_per_minute_by_lang"`))
	assert.That(t, strings.Contains(got, `hashtextextended('posts_by_lang_1d:' || extract(epoch FROM touched.b)::bigint, 0)`))
	// The migration runner owns the transaction. READ COMMITTED appears in
	// the function bodies, so look for the statements.
	assert.False(t, strings.Contains(got, "BEGIN;"))
	assert.False(t, strings.Contains(got, "COMMIT;"))
}

func TestCliRollup_PostgresDDLRefusesANameTooLong(t *testing.T) {
	coverage.Covers(t, "cli.rollup")

	conf := loadExample(t)
	conf.Rollups[0].DimensionSets[1].Name = "posts_total_" + strings.Repeat("x", 40)
	_, err := PostgresDDL(conf)
	assert.Error(t, err)
	assert.Equal(t, errs.CodeConfigRollup, errs.CodeOf(err))
	assert.That(t, strings.Contains(err.Error(), "63"))
}

func TestCliRollup_PostgresDDLRefusesAFileThatBreaksARule(t *testing.T) {
	coverage.Covers(t, "cli.rollup")

	conf := loadExample(t)
	conf.Rollups[0].Serve.MaxBuckets = 0
	_, err := PostgresDDL(conf)
	assert.Equal(t, errs.CodeConfigRollup, errs.CodeOf(err))

	_, err = PostgresDDL(&config.RollupsConf{})
	assert.Error(t, err)
}
