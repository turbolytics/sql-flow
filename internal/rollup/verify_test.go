package rollup

import (
	"strings"
	"testing"

	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

// The statement recomputes 15m from 5m with the generator's own select
// list, so a change to how triggers merge changes what verify expects.
func TestCliRollupRun_VerifySQLRecomputesFromTheFinerTable(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")

	r := exampleRollup(t)
	e, ok := findEdge(r, "posts_by_lang_15m")
	assert.True(t, ok)
	golden(t, "testdata/verify.posts_by_lang_15m.sql", verifySQL(r, e))
}

// Only a double sum is compared within a tolerance: Postgres sums floats in
// no fixed order. Every other measure is exact.
func TestCliRollupRun_OnlyADoubleSumMatchesWithinATolerance(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")

	r := config.Rollup{
		Name:   "m",
		Source: config.RollupSource{Table: "m_1m", TimeColumn: "minute", Grain: "1m"},
		Grains: map[string]config.RollupGrain{"1h": {From: "1m"}},
		DimensionSets: []config.RollupDimensionSet{{
			Name: "m", Dimensions: []string{},
			Measures: map[string]config.RollupMeasure{
				"a": {Type: "sum", Column: "a", Numeric: "double"},
				"b": {Type: "sum", Column: "b"},
				"c": {Type: "max", Column: "c"},
			},
		}},
	}
	sql := verifySQL(r, edges(r)[0])
	// The double's expression appears four times: in the WHERE, and in the
	// measure, stored and recomputed CASE arms.
	assert.Equal(t, 4, strings.Count(sql, "1e-09"))
	assert.That(t, strings.Contains(sql, `"w:b" IS DISTINCT FROM "g:b"`))
	assert.That(t, strings.Contains(sql, `"w:c" IS DISTINCT FROM "g:c"`))
	assert.False(t, strings.Contains(sql, `"w:a" IS DISTINCT FROM`))
}
