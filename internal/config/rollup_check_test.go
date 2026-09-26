package config

import (
	"strings"
	"testing"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/turbolytics/sql-flow/internal/errs"
	"github.com/zeebo/assert"
)

// A rollups file that reports TurboStats sends one freshness entry per
// table, so the file caps its tables. The demo's ten fit; an eleventh and a
// twelfth are refused with the count. A file that reports nothing is not
// capped.
func TestCliRollupRun_AReportingFileDeclaresAtMostTenTables(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")

	reporting := &TurboStats{ID: "rollups-01", ReportTo: "http://127.0.0.1:8080/v1/turbostats",
		Key: "sfc_AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8"}
	capped := func(c *RollupsConf) []Violation {
		var out []Violation
		for _, v := range c.Check() {
			if strings.Contains(v.Message, "at most") {
				out = append(out, v)
			}
		}
		return out
	}

	conf, err := LoadRollups("../../dev/config/rollups/bluesky.yml")
	assert.NoError(t, err)
	assert.Equal(t, 10, conf.TableCount())
	conf.TurboStats = reporting
	assert.Equal(t, 0, len(capped(conf)))

	conf.Rollups[0].Grains["7d"] = RollupGrain{From: "1d"}
	assert.Equal(t, 12, conf.TableCount())
	got := capped(conf)
	assert.Equal(t, 1, len(got))
	assert.DeepEqual(t, []string{"turbostats"}, got[0].Path)
	assert.That(t, strings.Contains(got[0].Message, "declares 12"))

	conf.TurboStats = nil
	assert.Equal(t, 0, len(capped(conf)))
}

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
		{"by-grain ttl without a default", []edit{{"default_range: 24h\n", "default_range: 24h\n          cache_ttl_by_grain: {1d: 600}\n"}},
			"rollups.0.serve.datasets.0.cache_ttl_by_grain", "needs cache_ttl_seconds"},
		{"by-grain ttl for a grain not served", []edit{{"default_range: 24h\n", "default_range: 24h\n          cache_ttl_seconds: 30\n          cache_ttl_by_grain: {2h: 600}\n"}},
			"rollups.0.serve.datasets.0.cache_ttl_by_grain.2h", "not a grain this dataset serves"},
		{"by-grain ttl below one", []edit{{"default_range: 24h\n", "default_range: 24h\n          cache_ttl_seconds: 30\n          cache_ttl_by_grain: {1d: 0}\n"}},
			"rollups.0.serve.datasets.0.cache_ttl_by_grain.1d", "at least 1"},
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
			"rollups.0.dimension_sets.1.measures.minutes.type", "use sum, min, max, last or count_buckets"},
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

// lastRollups is the metrics template's declaration: every source dimension
// kept, a double sum, and a last. It has no serve block, because a served
// dataset takes at most one dimension.
const lastRollups = `
rollups:
  - name: metrics
    source:
      table: metrics_1m
      time_column: bucket
      grain: 1m
      dimensions: [name, type, dimensions_key]
    grains:
      5m: {from: 1m}
      1h: {from: 5m}
    dimension_sets:
      - name: metrics
        dimensions: [name, type, dimensions_key]
        measures:
          value_sum: {type: sum, column: value_sum, numeric: double}
          value_count: {type: sum, column: value_count}
          value_last: {type: last, column: value_last}
`

func TestCliRollup_CheckAcceptsLastAndADoubleSum(t *testing.T) {
	coverage.Covers(t, "cli.rollup")
	assert.Equal(t, 0, len(parseRollups(t, lastRollups).Check()))
}

func TestCliRollup_CheckReportsEachLastAndNumericRule(t *testing.T) {
	coverage.Covers(t, "cli.rollup")

	for _, tt := range []struct {
		name     string
		from, to string
		path     string
		message  string
	}{
		{"last without a column", "{type: last, column: value_last}", "{type: last}",
			"rollups.0.dimension_sets.0.measures.value_last.column", "needs column"},
		{"numeric on a last", "{type: last, column: value_last}", "{type: last, column: value_last, numeric: double}",
			"rollups.0.dimension_sets.0.measures.value_last.numeric", "only a sum takes numeric"},
		{"numeric not a kind", "numeric: double", "numeric: decimal",
			"rollups.0.dimension_sets.0.measures.value_sum.numeric", "use integer or double"},
		// Two series share a bucket once a dimension is dropped, and neither
		// is the later one.
		{"last in a set that drops a dimension", "        dimensions: [name, type, dimensions_key]", "        dimensions: [name, type]",
			"rollups.0.dimension_sets.0.measures.value_last.type", "keeps every source dimension"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			text := strings.Replace(lastRollups, tt.from, tt.to, 1)
			assert.That(t, text != lastRollups)
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
}

// A folded dataset sums the values past the top into "other", and the last
// value of many series is no series' value.
func TestCliRollup_CheckRefusesLastUnderAFold(t *testing.T) {
	coverage.Covers(t, "cli.rollup")

	text := strings.Replace(validRollups,
		"          posts: {type: sum, column: posts}\n      - name: posts_total",
		"          posts: {type: sum, column: posts}\n          latest: {type: last, column: posts}\n      - name: posts_total", 1)
	assert.That(t, text != validRollups)
	violations := parseRollups(t, text).Check()
	if len(violations) != 1 {
		t.Fatalf("want 1 violation, got %d: %+v", len(violations), violations)
	}
	assert.Equal(t, "rollups.0.serve.datasets.0.dimension_set", strings.Join(violations[0].Path, "."))
	assert.That(t, strings.Contains(violations[0].Message, "latest is last"))
}
