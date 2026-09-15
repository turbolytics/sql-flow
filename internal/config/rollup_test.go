package config

import (
	"strings"
	"testing"
	"time"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

// validRollups breaks no rule. It is the demo's declaration. Each rule test
// edits one thing, so a failure names that rule and not an unrelated one.
const validRollups = `
rollups:
  - name: posts
    source:
      table: posts_per_minute_by_lang
      time_column: bucket
      grain: 1m
      dimensions: [lang]
    grains:
      5m: {from: 1m}
      15m: {from: 5m}
      1h: {from: 15m}
      6h: {from: 1h}
      1d: {from: 6h}
    dimension_sets:
      - name: posts_by_lang
        dimensions: [lang]
        measures:
          posts: {type: sum, column: posts}
      - name: posts_total
        dimensions: []
        measures:
          posts: {type: sum, column: posts}
          minutes: {type: count_buckets}
    serve:
      catalog: pg
      max_buckets: 365
      datasets:
        - name: posts_by_lang
          dimension_set: posts_by_lang
          description: Posts per bucket per language.
          default_range: 24h
          max_range: {1m: 6h, 5m: 1d, 15m: 3d, 1h: 14d, 6h: 90d, 1d: 365d}
          fold: {dimension: lang, top: {default: 10, max: 20}}
          filters: {lang: lang}
`

func parseRollups(t *testing.T, text string) *RollupsConf {
	t.Helper()
	conf, err := ParseRollups([]byte(text))
	assert.NoError(t, err)
	return conf
}

// An unknown key is a typo, and dropping it silently would generate
// something the author did not write.
func TestCliRollup_ParseRollupsDecodesStrictly(t *testing.T) {
	coverage.Covers(t, "cli.rollup")

	conf := parseRollups(t, validRollups)
	assert.Equal(t, "posts_per_minute_by_lang", conf.Rollups[0].Source.Table)
	assert.Equal(t, int64(20), conf.Rollups[0].Serve.Datasets[0].Fold.Top.Max)

	_, err := ParseRollups([]byte(strings.Replace(validRollups, "    grains:", "    grain_typo: 1\n    grains:", 1)))
	assert.Error(t, err)
}

func TestCliRollup_IsRollupsReadsTopLevelKeys(t *testing.T) {
	coverage.Covers(t, "cli.rollup")

	assert.True(t, IsRollups([]byte(validRollups)))
	assert.False(t, IsRollups([]byte("serve:\n  datasets: []\n")))
	assert.False(t, IsRollups([]byte("rollups: []\nserve: {}\n")))
	assert.False(t, IsRollups([]byte("rollups: []\npipeline: {}\n")))
	assert.False(t, IsRollups([]byte("not: [valid")))
}

// Width orders the ladder, not the name: sorted by name, 15m would come
// before 5m and 1d before 1h.
func TestCliRollup_LadderOrdersGrainsByWidth(t *testing.T) {
	coverage.Covers(t, "cli.rollup")

	ladder := parseRollups(t, validRollups).Rollups[0].Ladder()
	var names []string
	for _, l := range ladder {
		names = append(names, l.Name)
	}
	assert.DeepEqual(t, []string{"5m", "15m", "1h", "6h", "1d"}, names)
	assert.Equal(t, 15*time.Minute, ladder[1].Width)
	assert.Equal(t, "5m", ladder[1].From)
}

func TestCliRollup_LookupsReturnDeclaredOrder(t *testing.T) {
	coverage.Covers(t, "cli.rollup")

	r := parseRollups(t, validRollups).Rollups[0]
	set, ok := r.DimensionSet("posts_total")
	assert.True(t, ok)
	assert.DeepEqual(t, []string{"minutes", "posts"}, set.MeasureNames())

	_, ok = r.DimensionSet("nope")
	assert.False(t, ok)
}
