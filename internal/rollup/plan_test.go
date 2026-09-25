package rollup

import (
	"encoding/json"
	"slices"
	"strings"
	"testing"

	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/turbolytics/sql-flow/internal/errs"
	"github.com/zeebo/assert"
)

// pathInFile is the example's only rollup.
var pathInFile = []string{"rollups", "0"}

func exampleRollup(t *testing.T) config.Rollup {
	t.Helper()
	return loadExample(t).Rollups[0]
}

func appliedOf(r config.Rollup) *Applied {
	a := AppliedFrom(r)
	return &a
}

func setMeasure(r *config.Rollup, set int, name string, m config.RollupMeasure) {
	r.DimensionSets[set].Measures[name] = m
}

// The state table stores this JSON and every later version reads it, so a
// change to its shape must be deliberate.
func TestCliRollupRun_TheAppliedDeclarationMatchesTheGolden(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")

	b, err := json.MarshalIndent(AppliedFrom(exampleRollup(t)), "", "  ")
	assert.NoError(t, err)
	golden(t, "testdata/bluesky.applied.json", string(b)+"\n")
}

func TestCliRollupRun_AFirstInstallFillsTheTablesBuiltFromTheSource(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")

	plan, v := PlanChange(exampleRollup(t), pathInFile, nil, nil, nil)
	assert.Equal(t, 0, len(v))
	assert.DeepEqual(t, []string{"posts_by_lang_5m", "posts_total_5m"}, plan.Backfill)
	assert.Equal(t, 0, len(plan.Retain))
	assert.Equal(t, 0, len(plan.Restore))
}

// A deploy must not stop on an edit that changes no stored row.
func TestCliRollupRun_AnEditThatChangesNoRowIsNoChange(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")

	metrics, err := config.LoadRollups(metricsPath)
	assert.NoError(t, err)
	r := metrics.Rollups[0]
	prev := appliedOf(r)

	// A sum's default numeric written out, and dimensions in another order.
	setMeasure(&r, 0, "value_count", config.RollupMeasure{Type: "sum", Column: "value_count", Numeric: "integer"})
	slices.Reverse(r.DimensionSets[0].Dimensions)
	slices.Reverse(r.Source.Dimensions)

	plan, v := PlanChange(r, pathInFile, prev, nil, map[string]bool{})
	assert.Equal(t, 0, len(v))
	assert.DeepEqual(t, Plan{}, plan)
}

func TestCliRollupRun_AnAddedGrainFillsFromTheGrainBelow(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")

	r := exampleRollup(t)
	prev := appliedOf(r)
	r.Grains["7d"] = config.RollupGrain{From: "1d"}

	plan, v := PlanChange(r, pathInFile, prev, nil, map[string]bool{"posts_by_lang_7d": true, "posts_total_7d": true})
	assert.Equal(t, 0, len(v))
	assert.DeepEqual(t, []string{"posts_by_lang_7d", "posts_total_7d"}, plan.Backfill)
}

// The second new grain fills when the first does: the first one's upserts
// fire its trigger.
func TestCliRollupRun_OfTwoNewGrainsOnlyTheFirstFills(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")

	r := exampleRollup(t)
	prev := appliedOf(r)
	r.Grains["2d"] = config.RollupGrain{From: "1d"}
	r.Grains["4d"] = config.RollupGrain{From: "2d"}
	missing := map[string]bool{
		"posts_by_lang_2d": true, "posts_total_2d": true,
		"posts_by_lang_4d": true, "posts_total_4d": true,
	}

	plan, v := PlanChange(r, pathInFile, prev, nil, missing)
	assert.Equal(t, 0, len(v))
	assert.DeepEqual(t, []string{"posts_by_lang_2d", "posts_total_2d"}, plan.Backfill)
}

func TestCliRollupRun_ANewSetFillsOnlyItsTableBuiltFromTheSource(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")

	r := exampleRollup(t)
	prev := appliedOf(r)
	r.DimensionSets = append(r.DimensionSets, config.RollupDimensionSet{
		Name: "posts_peak", Dimensions: []string{"lang"},
		Measures: map[string]config.RollupMeasure{"peak": {Type: "max", Column: "posts"}},
	})
	missing := map[string]bool{}
	for _, g := range []string{"5m", "15m", "1h", "6h", "1d"} {
		missing["posts_peak_"+g] = true
	}

	plan, v := PlanChange(r, pathInFile, prev, nil, missing)
	assert.Equal(t, 0, len(v))
	assert.DeepEqual(t, []string{"posts_peak_5m"}, plan.Backfill)
}

func TestCliRollupRun_ARemovedGrainIsRetainedAndRestoredWhenDeclaredAgain(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")

	full := exampleRollup(t)
	without := exampleRollup(t)
	delete(without.Grains, "1d")

	plan, v := PlanChange(without, pathInFile, appliedOf(full), nil, map[string]bool{})
	assert.Equal(t, 0, len(v))
	assert.DeepEqual(t, []string{"posts_by_lang_1d", "posts_total_1d"}, plan.Retain)
	assert.Equal(t, 0, len(plan.Backfill))

	// Its triggers never stopped, so declaring it again fills nothing.
	plan, v = PlanChange(full, pathInFile, appliedOf(without), plan.Retain, map[string]bool{})
	assert.Equal(t, 0, len(v))
	assert.DeepEqual(t, []string{"posts_by_lang_1d", "posts_total_1d"}, plan.Restore)
	assert.Equal(t, 0, len(plan.Backfill))
	assert.Equal(t, 0, len(plan.Retain))
}

// A table dropped by hand is created again holding nothing, so it fills.
func TestCliRollupRun_AMissingTableFills(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")

	r := exampleRollup(t)
	plan, v := PlanChange(r, pathInFile, appliedOf(r), nil, map[string]bool{"posts_by_lang_1h": true})
	assert.Equal(t, 0, len(v))
	assert.DeepEqual(t, []string{"posts_by_lang_1h"}, plan.Backfill)
}

func TestCliRollupRun_ChangesThatCorruptStoredRowsAreRefused(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")

	for _, c := range []struct {
		name string
		edit func(r *config.Rollup)
		path string
	}{
		{"a measure's type", func(r *config.Rollup) {
			setMeasure(r, 0, "posts", config.RollupMeasure{Type: "max", Column: "posts"})
		}, "rollups.0.dimension_sets.0.measures.posts"},
		{"a sum's numeric", func(r *config.Rollup) {
			setMeasure(r, 0, "posts", config.RollupMeasure{Type: "sum", Column: "posts", Numeric: "double"})
		}, "rollups.0.dimension_sets.0.measures.posts"},
		{"a measure's column", func(r *config.Rollup) {
			setMeasure(r, 0, "posts", config.RollupMeasure{Type: "sum", Column: "likes"})
		}, "rollups.0.dimension_sets.0.measures.posts"},
		{"an added measure", func(r *config.Rollup) {
			setMeasure(r, 1, "peak", config.RollupMeasure{Type: "max", Column: "posts"})
		}, "rollups.0.dimension_sets.1.measures.peak"},
		{"a removed measure", func(r *config.Rollup) {
			delete(r.DimensionSets[1].Measures, "minutes")
		}, "rollups.0.dimension_sets.1.measures"},
		{"a set's dimensions", func(r *config.Rollup) {
			r.DimensionSets[0].Dimensions = nil
		}, "rollups.0.dimension_sets.0.dimensions"},
		{"a grain's from", func(r *config.Rollup) {
			r.Grains["1h"] = config.RollupGrain{From: "5m"}
		}, "rollups.0.grains.1h.from"},
		{"the source", func(r *config.Rollup) {
			r.Source.TimeColumn = "minute"
		}, "rollups.0.source"},
	} {
		t.Run(c.name, func(t *testing.T) {
			r := exampleRollup(t)
			prev := appliedOf(r)
			c.edit(&r)

			plan, v := PlanChange(r, pathInFile, prev, nil, map[string]bool{})
			assert.Equal(t, 1, len(v))
			assert.Equal(t, errs.CodeConfigRollupChange, v[0].Code)
			assert.Equal(t, c.path, strings.Join(v[0].Path, "."))
			assert.DeepEqual(t, Plan{}, plan)
		})
	}
}

// No generated table or trigger reads source.dimensions, so adding one to
// group a new set by changes no stored row.
func TestCliRollupRun_AnAddedSourceDimensionIsNoChange(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")

	r := exampleRollup(t)
	prev := appliedOf(r)
	r.Source.Dimensions = append(r.Source.Dimensions, "country")

	plan, v := PlanChange(r, pathInFile, prev, nil, map[string]bool{})
	assert.Equal(t, 0, len(v))
	assert.DeepEqual(t, Plan{}, plan)
}
