package config

import (
	"strings"
	"testing"
	"time"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/turbolytics/sql-flow/internal/errs"
	"github.com/zeebo/assert"
)

// cachedServe breaks no rule: one cached dataset without a range and one
// with, every grain carrying its bucket.
const cachedServe = `
serve:
  auth:
    tokens: [{name: page, token: page-token}]
  cache:
    max_mb: 8
  datasets:
    - name: status
      cache: {ttl_seconds: 15}
      sql: SELECT count(*) AS n FROM t
    - name: posts
      cache: {ttl_seconds: 30}
      params:
        - {name: since, type: timestamp}
        - {name: until, type: timestamp}
      range: {since: since, until: until, default: 24h}
      grains:
        5m:
          bucket: 5m
          max_range: 1d
          sql: SELECT * FROM t WHERE bucket >= $since AND bucket < $until
        1d:
          bucket: 1d
          max_range: 365d
          sql: SELECT * FROM t WHERE bucket >= $since AND bucket < $until
`

func TestCliServe_CacheConfigResolves(t *testing.T) {
	coverage.Covers(t, "cli.serve")

	conf := parseServe(t, cachedServe)
	assert.Equal(t, 0, len(conf.Check()))
	assert.Equal(t, int64(8<<20), conf.Serve.CacheMaxBytes())
	assert.That(t, conf.Serve.AnyCached())
	assert.Equal(t, 15*time.Second, conf.Serve.Datasets[0].CacheTTL())
	assert.Equal(t, 30*time.Second, conf.Serve.Datasets[1].CacheTTL())

	// Off unless a dataset says so, and the bound defaults.
	plain := parseServe(t, validServe)
	assert.That(t, !plain.Serve.AnyCached())
	assert.Equal(t, time.Duration(0), plain.Serve.Datasets[0].CacheTTL())
	assert.Equal(t, int64(DefaultServeCacheMaxMB<<20), plain.Serve.CacheMaxBytes())
}

func TestCliServe_CheckReportsEachCacheRuleAtItsPath(t *testing.T) {
	coverage.Covers(t, "cli.serve")

	for _, tt := range []struct {
		name, from, to string
		code           errs.Code
		path, message  string
	}{
		{"negative max_mb", "max_mb: 8", "max_mb: -1",
			errs.CodeConfigInvalid, "serve.cache.max_mb", "must not be negative"},
		{"ttl of zero", "cache: {ttl_seconds: 15}", "cache: {ttl_seconds: 0}",
			errs.CodeConfigServeDataset, "serve.datasets.0.cache.ttl_seconds", "at least 1"},
		{"a cached range without a bucket", "          bucket: 5m\n", "",
			errs.CodeConfigServeDataset, "serve.datasets.1.grains.5m.bucket", "needs bucket on every grain"},
		{"a bucket that is not a duration", "bucket: 5m", "bucket: five",
			errs.CodeConfigServeDataset, "serve.datasets.1.grains.5m.bucket", `"five" is not a duration`},
		{"a bucket that does not divide a day", "bucket: 5m", "bucket: 7m",
			errs.CodeConfigServeDataset, "serve.datasets.1.grains.5m.bucket", "does not divide one day"},
		{"a week", "bucket: 1d", "bucket: 7d",
			errs.CodeConfigServeDataset, "serve.datasets.1.grains.1d.bucket", "does not divide one day"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			assert.That(t, strings.Contains(cachedServe, tt.from))
			conf := parseServe(t, strings.Replace(cachedServe, tt.from, tt.to, 1))

			violations := conf.Check()
			assert.Equal(t, 1, len(violations))
			assert.Equal(t, tt.code, violations[0].Code)
			assert.Equal(t, tt.path, strings.Join(violations[0].Path, "."))
			if !strings.Contains(violations[0].Message, tt.message) {
				t.Fatalf("message %q does not contain %q", violations[0].Message, tt.message)
			}
		})
	}

	// bucket is a fact about a grain of a ranged dataset. Without a range
	// there is nothing for it to align.
	t.Run("bucket without a range", func(t *testing.T) {
		conf := parseServe(t, strings.Replace(validServe, "        5m:\n", "        5m:\n          bucket: 5m\n", 1))
		violations := conf.Check()
		assert.Equal(t, 1, len(violations))
		assert.Equal(t, "serve.datasets.1.grains.5m.bucket", strings.Join(violations[0].Path, "."))
		assert.That(t, strings.Contains(violations[0].Message, "needs a range"))
	})

	// bucket without cache is allowed: the generator always writes it.
	t.Run("bucket without cache", func(t *testing.T) {
		conf := parseServe(t, strings.Replace(cachedServe, "      cache: {ttl_seconds: 30}\n", "", 1))
		assert.Equal(t, 0, len(conf.Check()))
	})
}
