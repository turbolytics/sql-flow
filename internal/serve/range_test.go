package serve

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

// Each grain reports its own name and the since and until it was bound, so a
// test reads which grain the server chose and what it bound without data
// that depends on the clock.
const rangedTestServe = `
serve:
  auth:
    tokens: [{name: page, token: page-token}]
  datasets:
    - name: posts
      params:
        - {name: since, type: timestamp}
        - {name: until, type: timestamp}
      range: {since: since, until: until, default: 24h}
      grains:
        1d:
          max_range: 365d
          sql: SELECT '1d' AS picked, $since AS since, $until AS until, count(*) AS n FROM posts WHERE bucket >= $since AND bucket < $until
        5m:
          max_range: 1d
          sql: SELECT '5m' AS picked, $since AS since, $until AS until, count(*) AS n FROM posts WHERE bucket >= $since AND bucket < $until
        1h:
          max_range: 14d
          sql: SELECT '1h' AS picked, $since AS since, $until AS until, count(*) AS n FROM posts WHERE bucket >= $since AND bucket < $until
`

var rangeNow = time.Date(2026, 9, 12, 0, 0, 0, 0, time.UTC)

func newRangedServer(t *testing.T) *testServer {
	t.Helper()
	ts := newTestServer(t, rangedTestServe)
	ts.srv.now = func() time.Time { return rangeNow }
	return ts
}

func rangeRow(t *testing.T, r response) map[string]any {
	t.Helper()
	assert.Equal(t, http.StatusOK, r.status)
	rows := r.body["rows"].([]any)
	assert.Equal(t, 1, len(rows))
	row := rows[0].(map[string]any)
	// What the grain's SQL saw is what the response says the range was.
	win := r.body["range"].(map[string]any)
	assert.Equal(t, win["since"], row["since"])
	assert.Equal(t, win["until"], row["until"])
	assert.Equal(t, r.body["grain"], row["picked"])
	return row
}

// Without a grain the server picks the finest one whose max_range covers the
// range, trying grains by width rather than by name. A range exactly a
// grain's max_range still fits it.
func TestCliServe_ARangeWithoutAGrainPicksTheFinestThatFits(t *testing.T) {
	coverage.Covers(t, "cli.serve")
	ts := newRangedServer(t)

	for _, tt := range []struct {
		name, query, grain, since, until string
	}{
		{"no range: the default 24h ending now", "", "5m", "2026-09-11T00:00:00Z", "2026-09-12T00:00:00Z"},
		{"exactly 1d", "?since=2026-09-10T00:00:00Z&until=2026-09-11T00:00:00Z", "5m", "2026-09-10T00:00:00Z", "2026-09-11T00:00:00Z"},
		{"a second past 1d", "?since=2026-09-09T23:59:59Z&until=2026-09-11T00:00:00Z", "1h", "2026-09-09T23:59:59Z", "2026-09-11T00:00:00Z"},
		{"3d from since to now", "?since=2026-09-09T00:00:00Z", "1h", "2026-09-09T00:00:00Z", "2026-09-12T00:00:00Z"},
		{"until alone takes the default width", "?until=2026-09-10T12:00:00Z", "5m", "2026-09-09T12:00:00Z", "2026-09-10T12:00:00Z"},
		{"90d", "?since=2026-06-14T00:00:00Z", "1d", "2026-06-14T00:00:00Z", "2026-09-12T00:00:00Z"},
		{"an offset resolves to UTC", "?since=2026-09-10T20:00:00-04:00&until=2026-09-11T20:00:00-04:00", "5m", "2026-09-11T00:00:00Z", "2026-09-12T00:00:00Z"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			r := ts.get(t, "/v1/datasets/posts"+tt.query)
			row := rangeRow(t, r)
			assert.Equal(t, tt.grain, r.body["grain"])
			assert.Equal(t, tt.since, row["since"])
			assert.Equal(t, tt.until, row["until"])
		})
	}

	// The window binds as timestamps, so it filters: three of the fixture's
	// four rows fall on 2026-09-10.
	r := ts.get(t, "/v1/datasets/posts?since=2026-09-10T00:00:00Z&until=2026-09-11T00:00:00Z")
	assert.Equal(t, float64(3), rangeRow(t, r)["n"])
}

// A named grain still answers when it serves the range. When it does not,
// the refusal says which grains would.
func TestCliServe_ANamedGrainMustServeTheRange(t *testing.T) {
	coverage.Covers(t, "cli.serve")
	ts := newRangedServer(t)

	r := ts.get(t, "/v1/datasets/posts?grain=1d&since=2026-09-09T00:00:00Z")
	assert.Equal(t, "1d", rangeRow(t, r)["picked"])

	tooWide := ts.get(t, "/v1/datasets/posts?grain=5m&since=2026-09-09T00:00:00Z")
	assert.Equal(t, http.StatusBadRequest, tooWide.status)
	code, message := errorOf(t, tooWide)
	assert.Equal(t, "range_too_wide", code)
	assert.Equal(t, "grain 5m serves at most 1d and the range is 3d; grains that serve it: 1h, 1d", message)

	unknown := ts.get(t, "/v1/datasets/posts?grain=15m")
	code, _ = errorOf(t, unknown)
	assert.Equal(t, "unknown_grain", code)
}

// Nothing is silently cut from a chart: a range wider than every grain is
// refused, not clamped.
func TestCliServe_ARangeWiderThanEveryGrainIsRefused(t *testing.T) {
	coverage.Covers(t, "cli.serve")
	ts := newRangedServer(t)

	r := ts.get(t, "/v1/datasets/posts?since=2025-09-11T00:00:00Z")
	assert.Equal(t, http.StatusBadRequest, r.status)
	code, message := errorOf(t, r)
	assert.Equal(t, "range_too_wide", code)
	assert.Equal(t, "the range is 366d and the widest grain, 1d, serves at most 365d", message)

	ragged := ts.get(t, "/v1/datasets/posts?since=2025-09-10T23:59:58.5Z")
	_, message = errorOf(t, ragged)
	assert.That(t, strings.Contains(message, "the range is 8784h0m2s"))
}

func TestCliServe_ARangeMustStartBeforeItEnds(t *testing.T) {
	coverage.Covers(t, "cli.serve")
	ts := newRangedServer(t)

	for _, query := range []string{
		"?since=2026-09-11T00:00:00Z&until=2026-09-11T00:00:00Z",
		"?since=2026-09-11T00:00:00Z&until=2026-09-10T00:00:00Z",
		"?since=2026-09-13T00:00:00Z",
	} {
		r := ts.get(t, "/v1/datasets/posts"+query)
		assert.Equal(t, http.StatusBadRequest, r.status)
		code, message := errorOf(t, r)
		assert.Equal(t, "invalid_param", code)
		assert.That(t, strings.Contains(message, "since must be before until"))
	}
}

// The listing tells a caller how the server will choose: the range params,
// the default width, and each grain's max_range.
func TestCliServe_ListingShowsTheRangeAndEachGrainsMaxRange(t *testing.T) {
	coverage.Covers(t, "cli.serve")
	ts := newRangedServer(t)

	r := ts.get(t, "/v1/datasets")
	ds := r.body["datasets"].([]any)[0].(map[string]any)
	assert.DeepEqual(t, map[string]any{"since": "since", "until": "until", "default": "24h"}, ds["range"])
	grains := ds["grains"].(map[string]any)
	assert.Equal(t, "1d", grains["5m"].(map[string]any)["max_range"])
	assert.Equal(t, "14d", grains["1h"].(map[string]any)["max_range"])
	assert.Equal(t, "365d", grains["1d"].(map[string]any)["max_range"])

	plain := newTestServer(t, testServe).get(t, "/v1/datasets")
	for _, d := range plain.body["datasets"].([]any) {
		_, hasRange := d.(map[string]any)["range"]
		assert.False(t, hasRange)
	}
}
