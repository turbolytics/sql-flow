package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

func TestCliRollupTest_TheDemosCasesAreValid(t *testing.T) {
	coverage.Covers(t, "cli.rollup_test")
	conf, err := LoadRollups("../../dev/config/rollups/bluesky.yml")
	assert.NoError(t, err)
	tests, err := LoadRollupTests("../../dev/config/rollups/bluesky.test.yml")
	assert.NoError(t, err)
	assert.That(t, len(tests.Tests) >= 2)
	assert.Equal(t, 0, len(tests.Check(conf)))
	assert.NoError(t, tests.CheckError(conf))
}

// Each rule is reported at its YAML path.
func TestCliRollupTest_EachRuleReportsItsPath(t *testing.T) {
	coverage.Covers(t, "cli.rollup_test")
	conf, err := LoadRollups("../../dev/config/rollups/bluesky.yml")
	assert.NoError(t, err)
	path := filepath.Join(t.TempDir(), "bad.test.yml")
	assert.NoError(t, os.WriteFile(path, []byte(`tests:
  - name: ""
    rollup: nope
    writes: []
  - name: wrong table and a short row
    rollup: posts
    writes:
      - [{bucket: "2026-09-24T12:07:00Z", lang: en, posts: 5}]
    expect:
      posts_by_lang_2m:
        - {bucket: "2026-09-24T12:00:00Z"}
      posts_by_lang_5m:
        - {bucket: "2026-09-24T12:05:00Z", lang: en}
`), 0o644))
	tests, err := LoadRollupTests(path)
	assert.NoError(t, err)
	got := map[string]bool{}
	for _, v := range tests.Check(conf) {
		got[strings.Join(v.Path, ".")] = true
	}
	for _, want := range []string{"tests.0.name", "tests.0.rollup", "tests.0.writes",
		"tests.1.expect.posts_by_lang_2m", "tests.1.expect.posts_by_lang_5m.0"} {
		if !got[want] {
			t.Errorf("no violation at %s; got %v", want, got)
		}
	}
	assert.Error(t, tests.CheckError(conf))
}
