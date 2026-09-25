package rollup

import (
	"testing"
	"time"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

func TestCliRollupRun_NextStateKeepsProgressAndMovesRetainedTables(t *testing.T) {
	coverage.Covers(t, "cli.rollup_run")

	day := time.Date(2026, 9, 20, 0, 0, 0, 0, time.UTC)
	prev := &State{
		Rollup:   "posts",
		Backfill: map[string]*time.Time{"posts_by_lang_5m": &day, "posts_by_lang_1d": nil},
		Retained: []RetainedTable{{Table: "posts_by_lang_7d"}, {Table: "posts_total_7d"}},
	}
	p := planned{
		r:    exampleRollup(t),
		prev: prev,
		plan: Plan{
			Backfill: []string{"posts_by_lang_1h"},
			Retain:   []RetainedTable{{Table: "posts_by_lang_1d"}},
			Restore:  []string{"posts_by_lang_7d"},
		},
	}

	s := nextState(p, "v2")
	assert.Equal(t, "v2", s.Version)
	// Progress survives an install; a table created now starts over.
	assert.That(t, s.Backfill["posts_by_lang_5m"].Equal(day))
	_, pending := s.Backfill["posts_by_lang_1h"]
	assert.True(t, pending)
	// A retained table keeps a backfill still pending, so declaring it again
	// finds it unfilled rather than silently complete.
	_, filling := s.Backfill["posts_by_lang_1d"]
	assert.True(t, filling)
	assert.DeepEqual(t, []string{"posts_total_7d", "posts_by_lang_1d"}, tableNames(s.Retained))
}
