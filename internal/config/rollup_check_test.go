package config

import (
	"strings"
	"testing"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/turbolytics/sql-flow/internal/errs"
	"github.com/zeebo/assert"
)

func TestCliRollup_CheckAcceptsTheDemoDeclaration(t *testing.T) {
	coverage.Covers(t, "cli.rollup")
	assert.Equal(t, 0, len(parseRollups(t, validRollups).Check()))
}

// Each case edits validRollups so that exactly one rule breaks, and asserts
// that rule's path and message.
func TestCliRollup_CheckReportsEachRuleAtItsPath(t *testing.T) {
	coverage.Covers(t, "cli.rollup")

	type edit struct{ from, to string }
	for _, tt := range []struct {
		name    string
		edits   []edit
		path    string
		message string
	}{
		{"negative cache ttl", []edit{{"default_range: 24h\n", "default_range: 24h\n          cache_ttl_seconds: -1\n"}},
			"rollups.0.serve.datasets.0.cache_ttl_seconds", "must not be negative"},
		{"bad rollup name", []edit{{"  - name: posts\n    source:", "  - name: Posts\n    source:"}},
			"rollups.0.name", `"Posts"`},
		{"bad source table", []edit{{"table: posts_per_minute_by_lang", "table: Posts"}},
			"rollups.0.source.table", `"Posts"`},
		{"grain name not a duration", []edit{{"      1d: {from: 6h}\n", "      1d: {from: 6h}\n      weekly: {from: 1d}\n"}},
			"rollups.0.grains.weekly", "not a duration"},
		{"grain repeats the source grain", []edit{{"      1d: {from: 6h}\n", "      1d: {from: 6h}\n      1m: {from: 1m}\n"}},
			"rollups.0.grains.1m", "is the source grain"},
		{"grain from nothing declared", []edit{{"      1d: {from: 6h}\n", "      1d: {from: 6h}\n      2d: {from: 12h}\n"}},
			"rollups.0.grains.2d.from", `from "12h" is neither the source grain 1m nor a declared grain`},
		{"grain not a multiple", []edit{{"      1d: {from: 6h}\n", "      1d: {from: 6h}\n      7m: {from: 5m}\n"}},
			"rollups.0.grains.7m.from", "not a whole multiple of from grain 5m"},
		{"duplicate dimension set", []edit{{"      - name: posts_total", "      - name: posts_by_lang"}},
			"rollups.0.dimension_sets.1.name", "declared twice"},
		{"table named like the source", []edit{{"table: posts_per_minute_by_lang", "table: posts_total_1d"}},
			"rollups.0.dimension_sets.1.name", "which is the source table"},
		{"dimension not in the source", []edit{{"        dimensions: []", "        dimensions: [region]"}},
			"rollups.0.dimension_sets.1.dimensions.0", "region is not in source.dimensions"},
		{"reserved measure type", []edit{{"minutes: {type: count_buckets}", "minutes: {type: histogram}"}},
			"rollups.0.dimension_sets.1.measures.minutes.type", "does not generate"},
		{"unknown measure type", []edit{{"minutes: {type: count_buckets}", "minutes: {type: median}"}},
			"rollups.0.dimension_sets.1.measures.minutes.type", "use sum, min, max or count_buckets"},
		{"count_buckets with a column", []edit{{"{type: count_buckets}", "{type: count_buckets, column: bucket}"}},
			"rollups.0.dimension_sets.1.measures.minutes.column", "reads no column"},
		{"sum without a column", []edit{{"posts: {type: sum, column: posts}", "posts: {type: sum}"}},
			"rollups.0.dimension_sets.0.measures.posts.column", "needs column"},
		{"measure named like a key column", []edit{{"posts: {type: sum, column: posts}", "lang: {type: sum, column: posts}"}},
			"rollups.0.dimension_sets.0.measures.lang", "a column the table is keyed on"},
		{"no catalog", []edit{{"catalog: pg", `catalog: ""`}},
			"rollups.0.serve.catalog", "serve.catalog"},
		{"max_buckets zero", []edit{{"max_buckets: 365", "max_buckets: 0"}},
			"rollups.0.serve.max_buckets", "must be positive"},
		{"unknown dimension set", []edit{{"dimension_set: posts_by_lang", "dimension_set: nope"}},
			"rollups.0.serve.datasets.0.dimension_set", `"nope" is not declared`},
		{"two dimensions served", []edit{
			{"      dimensions: [lang]\n    grains", "      dimensions: [lang, region]\n    grains"},
			{"        dimensions: [lang]\n        measures", "        dimensions: [lang, region]\n        measures"}},
			"rollups.0.serve.datasets.0.dimension_set", "at most one"},
		{"dimension without a fold", []edit{{"          fold: {dimension: lang, top: {default: 10, max: 20}}\n", ""}},
			"rollups.0.serve.datasets.0", "needs a fold"},
		{"fold on another dimension", []edit{{"fold: {dimension: lang,", "fold: {dimension: region,"}},
			"rollups.0.serve.datasets.0.fold.dimension", `"region"`},
		{"count_buckets folded", []edit{{"posts: {type: sum, column: posts}\n      - name: posts_total",
			"posts: {type: sum, column: posts}\n          minutes: {type: count_buckets}\n      - name: posts_total"}},
			"rollups.0.serve.datasets.0.dimension_set", "cannot be summed across lang"},
		{"top default above max", []edit{{"top: {default: 10, max: 20}", "top: {default: 30, max: 20}"}},
			"rollups.0.serve.datasets.0.fold.top.default", "within 1 and top.max 20"},
		{"top max zero", []edit{{"top: {default: 10, max: 20}", "top: {default: 0, max: 0}"}},
			"rollups.0.serve.datasets.0.fold.top.max", "at least 1"},
		{"rank_by not a sum", []edit{{"fold: {dimension: lang, top:", "fold: {dimension: lang, rank_by: nope, top:"}},
			"rollups.0.serve.datasets.0.fold.rank_by", `"nope" is not a sum measure`},
		{"rank_by required", []edit{{"posts: {type: sum, column: posts}\n      - name: posts_total",
			"posts: {type: sum, column: posts}\n          replies: {type: sum, column: replies}\n      - name: posts_total"}},
			"rollups.0.serve.datasets.0.fold.rank_by", "required when the dimension set has 2 sum measures"},
		{"filter named top", []edit{{"filters: {lang: lang}", "filters: {top: lang}"}},
			"rollups.0.serve.datasets.0.filters.top", "not since, until, top or grain"},
		{"filter on another dimension", []edit{{"filters: {lang: lang}", "filters: {language: region}"}},
			"rollups.0.serve.datasets.0.filters.language", "not the dimension set's dimension"},
		{"default_range does not parse", []edit{{"default_range: 24h", "default_range: 1 day"}},
			"rollups.0.serve.datasets.0.default_range", "not a duration"},
		{"default_range too wide", []edit{{"default_range: 24h", "default_range: 400d"}},
			"rollups.0.serve.datasets.0.default_range", "wider than the widest max_range 365d"},
		{"max_range names an undeclared grain", []edit{{"15m: 3d", "30m: 3d"}},
			"rollups.0.serve.datasets.0.max_range.30m", "not declared"},
		{"source grain without the source's dimensions", []edit{{"      dimensions: [lang]\n    grains", "      dimensions: [lang, region]\n    grains"}},
			"rollups.0.serve.datasets.0.max_range.1m", "only for a dimension set with the source's dimensions"},
		{"too many buckets", []edit{{"6h: 90d", "6h: 100d"}},
			"rollups.0.serve.datasets.0.max_range.6h", "400 buckets, more than serve.max_buckets 365"},
		{"two grains share a max_range", []edit{{"15m: 3d", "15m: 1d"}},
			"rollups.0.serve.datasets.0.max_range.5m", "grains 15m and 5m both have max_range 1d"},
		{"duplicate dataset", []edit{{"          filters: {lang: lang}\n",
			"          filters: {lang: lang}\n        - name: posts_by_lang\n          dimension_set: posts_total\n          default_range: 24h\n          max_range: {1d: 365d}\n"}},
			"rollups.0.serve.datasets.1.name", "declared twice"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			text := validRollups
			for _, e := range tt.edits {
				assert.That(t, strings.Contains(text, e.from))
				text = strings.Replace(text, e.from, e.to, 1)
			}

			violations := parseRollups(t, text).Check()
			if len(violations) != 1 {
				t.Fatalf("want 1 violation, got %d: %+v", len(violations), violations)
			}
			assert.Equal(t, errs.CodeConfigRollup, violations[0].Code)
			assert.Equal(t, tt.path, strings.Join(violations[0].Path, "."))
			if !strings.Contains(violations[0].Message, tt.message) {
				t.Fatalf("message %q does not contain %q", violations[0].Message, tt.message)
			}
		})
	}

	t.Run("no rollups", func(t *testing.T) {
		violations := parseRollups(t, "rollups: []\n").Check()
		assert.Equal(t, 1, len(violations))
		assert.Equal(t, "rollups", strings.Join(violations[0].Path, "."))
	})

	// A source grain that does not parse also orphans every grain built from
	// it, so it is asserted among the violations rather than alone.
	t.Run("source grain does not parse", func(t *testing.T) {
		violations := parseRollups(t, strings.Replace(validRollups, "grain: 1m\n", "grain: 1 minute\n", 1)).Check()
		found := false
		for _, v := range violations {
			found = found || strings.Join(v.Path, ".") == "rollups.0.source.grain"
		}
		assert.True(t, found)
	})
}

func TestCliRollup_CheckErrorListsEveryViolation(t *testing.T) {
	coverage.Covers(t, "cli.rollup")

	text := strings.Replace(validRollups, "catalog: pg", `catalog: ""`, 1)
	text = strings.Replace(text, "max_buckets: 365", "max_buckets: 0", 1)
	err := parseRollups(t, text).CheckError()
	assert.Error(t, err)
	assert.Equal(t, errs.CodeConfigRollup, errs.CodeOf(err))
	assert.That(t, strings.Contains(err.Error(), "rollups.0.serve.catalog"))
	assert.That(t, strings.Contains(err.Error(), "rollups.0.serve.max_buckets"))
	assert.NoError(t, parseRollups(t, validRollups).CheckError())
}
