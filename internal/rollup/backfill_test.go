package rollup

import (
	"testing"
	"time"

	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

// secondsRollup is a source written every second: a day of it is far more
// buckets than one chunk may lock.
func secondsRollup() config.Rollup {
	return config.Rollup{
		Name:   "events",
		Source: config.RollupSource{Table: "events_1s", TimeColumn: "second", Grain: "1s"},
		Grains: map[string]config.RollupGrain{"5s": {From: "1s"}, "1m": {From: "5s"}},
		DimensionSets: []config.RollupDimensionSet{{
			Name: "events", Dimensions: []string{},
			Measures: map[string]config.RollupMeasure{"n": {Type: "sum", Column: "n"}},
		}},
	}
}

func TestCliRollupRun_AChunkIsADayWhenADayFitsTheLockBudget(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")

	r := exampleRollup(t)
	e, ok := findEdge(r, "posts_by_lang_5m")
	assert.True(t, ok)
	// 5m, 15m, 1h, 6h and 1d buckets of a day, each plus one at an edge.
	assert.Equal(t, 289+97+25+5+2, chunkLocks(24*time.Hour, cascadeWidths(r, e)))
	assert.Equal(t, 24*time.Hour, chunkSpan(r, e))
}

func TestCliRollupRun_AChunkHalvesUntilItFitsTheLockBudget(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")

	r := secondsRollup()
	e, ok := findEdge(r, "events_5s")
	assert.True(t, ok)
	// 90 minutes locks 1081 + 91; 45 minutes locks 541 + 46.
	assert.Equal(t, 45*time.Minute, chunkSpan(r, e))
	assert.That(t, chunkLocks(chunkSpan(r, e), cascadeWidths(r, e)) <= maxChunkLocks)
}

func TestCliRollupRun_TheCascadeIsEveryGrainBuiltFromTheTable(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")

	r := exampleRollup(t)
	e, _ := findEdge(r, "posts_by_lang_1h")
	assert.DeepEqual(t, []time.Duration{time.Hour, 6 * time.Hour, 24 * time.Hour}, cascadeWidths(r, e))
	e, _ = findEdge(r, "posts_total_1d")
	assert.DeepEqual(t, []time.Duration{24 * time.Hour}, cascadeWidths(r, e))
}

func TestCliRollupRun_FromWidthIsTheWidthOfTheRowsATableReads(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")

	r := exampleRollup(t)
	e, _ := findEdge(r, "posts_by_lang_5m")
	assert.Equal(t, time.Minute, fromWidth(r, e))
	e, _ = findEdge(r, "posts_total_1d")
	assert.Equal(t, 6*time.Hour, fromWidth(r, e))
	_, ok := findEdge(r, "posts_by_lang_7d")
	assert.False(t, ok)
}

// Bins start at the generated SQL's origin, so Go and Postgres agree on
// every boundary, before the origin too.
func TestCliRollupRun_FloorToBinsFromTheSQLOrigin(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")

	at := func(s string) time.Time { v, _ := time.Parse(time.RFC3339, s); return v }
	assert.Equal(t, at("2026-09-12T00:00:00Z"), floorTo(at("2026-09-12T13:07:00Z"), 24*time.Hour).UTC())
	assert.Equal(t, at("2026-09-12T12:00:00Z"), floorTo(at("2026-09-12T13:07:00Z"), 6*time.Hour).UTC())
	assert.Equal(t, at("1999-12-31T00:00:00Z"), floorTo(at("1999-12-31T13:00:00Z"), 24*time.Hour).UTC())
	// A week bins from 2000-01-01, a Saturday.
	assert.Equal(t, at("2026-09-12T00:00:00Z"), floorTo(at("2026-09-14T09:00:00Z"), 7*24*time.Hour).UTC())
}
