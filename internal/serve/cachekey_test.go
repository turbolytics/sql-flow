package serve

import (
	"testing"
	"time"

	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

func ts(s string) time.Time {
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		panic(err)
	}
	return t
}

func TestCliServe_CeilToRoundsUpAndLeavesAnAlignedTimeAlone(t *testing.T) {
	coverage.Covers(t, "cli.serve")

	for _, tt := range []struct {
		name, in, bucket, want string
	}{
		{"inside a 5m bucket", "2026-09-17T12:03:17.5Z", "5m", "2026-09-17T12:05:00Z"},
		{"one microsecond past a boundary", "2026-09-17T12:05:00.000001Z", "5m", "2026-09-17T12:10:00Z"},
		{"on a boundary", "2026-09-17T12:05:00Z", "5m", "2026-09-17T12:05:00Z"},
		{"a day", "2026-09-17T00:00:01Z", "24h", "2026-09-18T00:00:00Z"},
		{"six hours", "2026-09-17T07:00:00Z", "6h", "2026-09-17T12:00:00Z"},
		{"an offset is an instant", "2026-09-17T08:03:00-04:00", "5m", "2026-09-17T12:05:00Z"},
		{"before the epoch", "1969-12-31T23:58:00Z", "5m", "1970-01-01T00:00:00Z"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			bucket, err := time.ParseDuration(tt.bucket)
			assert.NoError(t, err)
			got := ceilTo(ts(tt.in), bucket)
			assert.That(t, got.Equal(ts(tt.want)))
			assert.Equal(t, time.UTC, got.Location())
		})
	}
}

func TestCliServe_AlignRangeRewritesTheBindsAndTheEcho(t *testing.T) {
	coverage.Covers(t, "cli.serve")

	values := map[string]any{"since": ts("2026-09-16T12:03:17Z"), "until": ts("2026-09-17T12:03:17Z"), "lang": "en"}
	win := &window{}
	alignRange("since", "until", 5*time.Minute, values, win)

	assert.That(t, values["since"].(time.Time).Equal(ts("2026-09-16T12:05:00Z")))
	assert.That(t, values["until"].(time.Time).Equal(ts("2026-09-17T12:05:00Z")))
	assert.Equal(t, "2026-09-16T12:05:00Z", win.Since)
	assert.Equal(t, "2026-09-17T12:05:00Z", win.Until)
	assert.Equal(t, "en", values["lang"])
}

var keyParams = []config.ServeParam{
	{Name: "since", Type: "timestamp"},
	{Name: "until", Type: "timestamp"},
	{Name: "lang", Type: "string"},
	{Name: "top", Type: "integer"},
}

func TestCliServe_CacheKeyIsEqualExactlyWhenTheRequestIs(t *testing.T) {
	coverage.Covers(t, "cli.serve")

	base := map[string]any{"since": ts("2026-09-16T12:05:00Z"), "until": ts("2026-09-17T12:05:00Z"), "lang": "en", "top": int64(6)}
	key := func(dataset, grain string, mutate func(map[string]any)) string {
		v := map[string]any{}
		for k, x := range base {
			v[k] = x
		}
		if mutate != nil {
			mutate(v)
		}
		return cacheKey(dataset, grain, keyParams, v)
	}
	same := key("posts", "5m", nil)

	// The same instant spelled with an offset is the same key.
	assert.Equal(t, same, key("posts", "5m", func(v map[string]any) { v["since"] = ts("2026-09-16T08:05:00-04:00") }))

	for name, other := range map[string]string{
		"another dataset":    key("other", "5m", nil),
		"another grain":      key("posts", "1h", nil),
		"another since":      key("posts", "5m", func(v map[string]any) { v["since"] = ts("2026-09-16T12:10:00Z") }),
		"another lang":       key("posts", "5m", func(v map[string]any) { v["lang"] = "ja" }),
		"another top":        key("posts", "5m", func(v map[string]any) { v["top"] = int64(7) }),
		"an absent lang":     key("posts", "5m", func(v map[string]any) { delete(v, "lang") }),
		"an empty lang":      key("posts", "5m", func(v map[string]any) { v["lang"] = "" }),
		"a lang that forges": key("posts", "5m", func(v map[string]any) { v["lang"] = "en\x00i6"; delete(v, "top") }),
	} {
		if other == same {
			t.Fatalf("%s builds the same key", name)
		}
	}
	// Absent and empty are different requests: one binds NULL, one binds ''.
	assert.That(t, key("posts", "5m", func(v map[string]any) { delete(v, "lang") }) !=
		key("posts", "5m", func(v map[string]any) { v["lang"] = "" }))
}
