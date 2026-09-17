# Serve Response Cache Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `sqlflow serve` answers repeated and concurrent identical dataset requests from an opt-in, byte-bounded, TTL-bounded in-memory cache, keyed so that ranges selecting the same buckets share one entry.

**Architecture:** One new unit, `internal/serve/cache.go`, holds encoded `result` values by key with an LRU list, an expiry heap and one detached fill per key; it knows nothing of HTTP or DuckDB. `internal/serve/cachekey.go` rounds a ranged request's `since`/`until` up to the grain's `bucket` and builds the key. `queryDataset` calls `cache.do` for a dataset with a `cache` block and takes today's path, untouched, for every other dataset.

**Tech Stack:** Go, `container/heap`, `container/list`, `sync`; `github.com/zeebo/assert` for tests; OpenTelemetry metrics through the existing `metrics` type; `invopop/jsonschema` via `make schema`.

**Spec:** `docs/superpowers/specs/2026-09-17-serve-cache-design.md`. Read it first. This plan argues from it.

## Global Constraints

- Opt-in only. A dataset without `cache:` must produce byte-for-byte the response it produces today, run one query per request, and never touch `cache.go`.
- `serve.cache.max_mb`: 0 or absent means `DefaultServeCacheMaxMB = 16`; negative is refused. It bounds the cache and never enables it.
- `cache.ttl_seconds` is an integer of at least 1.
- A dataset with `cache` and `range` declares `bucket` on every grain. `bucket` is a serve duration (`s|m|h|d`), positive, and divides 24h (`1d` does). `bucket` without `range` is refused.
- Dataset rule violations use `errs.CodeConfigServeDataset`; the `max_mb` rule uses `errs.CodeConfigInvalid`. These are the codes `internal/config/serve.go` already uses for their neighbours.
- Rounding is **up** (`ceil`) to the bucket, aligned to the Unix epoch in UTC, at microsecond precision. The grain is chosen from the width **before** rounding.
- An entry expires `ttl_seconds` after its fill **started**. Errors are never stored. A result larger than a quarter of the bound is returned and not stored.
- Responses stay `Cache-Control: no-store`.
- Every test starts with `coverage.Covers(t, "cli.serve")` (or `"cli.rollup"` in `internal/rollup` and the rollup config tests), the way every neighbouring test does.
- Test names start `TestCliServe_` / `TestCliRollup_`. Comments say why, in the repo's voice; no comment restates the code.
- Run `go test -short -race ./internal/serve/ ./internal/config/ ./internal/rollup/ ./internal/validate/ ./internal/cli/... ./internal/schema/` before every commit. It must be green.
- Commit messages follow `git log`: `serve: <what, as a sentence fragment>`.

## File Structure

| File | Responsibility |
| --- | --- |
| `internal/config/serve.go` (modify) | `ServeCache`, `ServeDatasetCache`, `ServeGrain.Bucket`, accessors, rules |
| `internal/config/serve_cache_test.go` (create) | the rules, each at its path |
| `internal/serve/cache.go` (create) | the store: LRU, expiry heap, byte bound, one detached fill per key |
| `internal/serve/cache_test.go` (create) | the store alone, with an injected clock |
| `internal/serve/cachekey.go` (create) | `ceilTo`, `alignRange`, `cacheKey` |
| `internal/serve/cachekey_test.go` (create) | rounding and key equality |
| `internal/serve/server.go` (modify) | build the cache, carry `cacheTTL` and buckets on `dataset`/`span`, list them |
| `internal/serve/http.go` (modify) | the cached path in `queryDataset`, response and log fields |
| `internal/serve/metrics.go` (modify) | cache counter, gauges, evictions; query histogram only when a query ran |
| `internal/serve/cache_http_test.go` (create) | handler tests, which carry the invariant markers |
| `internal/config/rollup.go`, `rollup_check.go` (modify) | `cache_ttl_seconds` |
| `internal/rollup/serve.go`, `check.go` (modify) | emit and compare `bucket` and `cache` |
| `docs/coverage/*`, `scripts/coverage_matrix/registries.py`, `internal/coverage/registry_test.go` (modify/create) | the `serve` kind, the `cache` family, seven invariants |
| `README.md` (modify) | the `cache` block, `bucket`, the half-open rule, the new response fields and metrics |

---

### Task 1: Config — the `cache` blocks, `bucket`, and their rules

**Files:**
- Modify: `internal/config/serve.go`
- Create: `internal/config/serve_cache_test.go`
- Regenerate: `internal/validate/schemas/serve.json`, `internal/cli/testdata/serve_example.golden` (via `make schema`)

**Interfaces:**
- Produces:
  - `type ServeCache struct { MaxMB int }` on `Serve.Cache *ServeCache` (`yaml:"cache,omitempty"`)
  - `type ServeDatasetCache struct { TTLSeconds int }` on `ServeDataset.Cache *ServeDatasetCache` (`yaml:"cache,omitempty"`)
  - `ServeGrain.Bucket string` (`yaml:"bucket,omitempty"`)
  - `const DefaultServeCacheMaxMB = 16`
  - `func (s Serve) CacheMaxBytes() int64`
  - `func (s Serve) AnyCached() bool`
  - `func (ds ServeDataset) CacheTTL() time.Duration` — 0 when the dataset did not opt in

- [ ] **Step 1: Write the failing tests**

Create `internal/config/serve_cache_test.go`:

```go
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
```

- [ ] **Step 2: Run them and see them fail**

Run: `go test -short ./internal/config/ -run 'TestCliServe_Cache|TestCliServe_CheckReportsEachCacheRule'`
Expected: FAIL to compile — `field cache not found in type config.Serve` (strict decode) or `conf.Serve.CacheMaxBytes undefined`.

- [ ] **Step 3: Add the types and accessors**

In `internal/config/serve.go`, add to the constants block:

```go
	// DefaultServeCacheMaxMB bounds the response cache when the config names
	// no number. Sixteen holds over a hundred of the demo's widest responses
	// and is small beside four sessions' 72 MiB peak on a 256 MB box.
	DefaultServeCacheMaxMB = 16
```

Add to `Serve`, after `Metrics`:

```go
	// Bounds the response cache. It does not enable it: a dataset opts in
	// with its own cache block.
	Cache *ServeCache `yaml:"cache,omitempty"`
```

Add the types beside `ServePool`:

```go
// ServeCache bounds the response cache every cached dataset shares.
type ServeCache struct {
	// The most the cache holds, in MiB. 0 means 16.
	MaxMB int `yaml:"max_mb,omitempty"`
}

// ServeDatasetCache opts one dataset into the response cache. For a dataset
// with a range it also asserts that the SQL reads the range as
// bucket >= $since AND bucket < $until, which is what makes rounding the
// range to the bucket exact.
type ServeDatasetCache struct {
	// How long an answer is served after the query that produced it
	// started, in seconds. At least 1.
	TTLSeconds int `yaml:"ttl_seconds" jsonschema:"minimum=1"`
}
```

Add to `ServeDataset`, after `Limits`:

```go
	// Opts the dataset into the response cache. Absent, every request runs
	// its query.
	Cache *ServeDatasetCache `yaml:"cache,omitempty"`
```

Add to `ServeGrain`, before `MaxRange`:

```go
	// How wide one bucket of this grain is, such as 5m. Required on every
	// grain of a cached dataset with a range, and refused without a range.
	// It must divide one day. Units: s, m, h, d.
	Bucket string `yaml:"bucket,omitempty"`
```

Add the accessors after `MetricsEnabled`:

```go
// CacheMaxBytes is the response cache's bound, defaulted.
func (s Serve) CacheMaxBytes() int64 {
	mb := DefaultServeCacheMaxMB
	if s.Cache != nil && s.Cache.MaxMB > 0 {
		mb = s.Cache.MaxMB
	}
	return int64(mb) << 20
}

// AnyCached reports whether any dataset opted into the cache. A server with
// none builds no cache at all.
func (s Serve) AnyCached() bool {
	for _, ds := range s.Datasets {
		if ds.Cache != nil {
			return true
		}
	}
	return false
}

// CacheTTL is how long a dataset's answers are served, or 0 for a dataset
// that did not opt in.
func (ds ServeDataset) CacheTTL() time.Duration {
	if ds.Cache == nil {
		return 0
	}
	return time.Duration(ds.Cache.TTLSeconds) * time.Second
}
```

- [ ] **Step 4: Add the rules**

In `Check`, after the `s.Pool` block:

```go
	if s.Cache != nil && s.Cache.MaxMB < 0 {
		add(errs.CodeConfigInvalid, []string{"serve", "cache", "max_mb"},
			"cache.max_mb is %d; it must not be negative, and 0 means the default", s.Cache.MaxMB)
	}
```

In `checkDataset`, immediately after the `checkRange(...)` call:

```go
	checkCache(ds, path, add)
```

Add the function after `checkRange`:

```go
// checkCache holds a dataset's cache block and its grains' buckets to the
// rules the cache key depends on. The key rounds a range up to the bucket,
// which is exact only for buckets aligned to the epoch, and a width that
// divides one day is aligned the way time_bucket and date_trunc align it. A
// week is not: date_trunc('week') starts on a Monday and the epoch was a
// Thursday.
func checkCache(ds ServeDataset, path []string, add addFunc) {
	code := errs.CodeConfigServeDataset

	if ds.Cache != nil && ds.Cache.TTLSeconds < 1 {
		add(code, at(path, "cache", "ttl_seconds"),
			"dataset %s: cache.ttl_seconds is %d; it must be at least 1", ds.Name, ds.Cache.TTLSeconds)
	}

	for _, grain := range ds.GrainNames() {
		gpath := at(path, "grains", grain, "bucket")
		raw := ds.Grains[grain].Bucket
		switch {
		case raw == "" && ds.Cache != nil && ds.Range != nil:
			add(code, gpath, "dataset %s grain %s: a cached dataset with a range needs bucket on every grain",
				ds.Name, grain)
		case raw == "":
		case ds.Range == nil:
			add(code, gpath, "dataset %s grain %s: bucket needs a range on the dataset; without one there is nothing to align",
				ds.Name, grain)
		default:
			d, err := ParseServeDuration(raw)
			if err != nil {
				add(code, gpath, "dataset %s grain %s: bucket %v", ds.Name, grain, err)
				continue
			}
			if (24*time.Hour)%d != 0 {
				add(code, gpath, "dataset %s grain %s: bucket %s does not divide one day, so its buckets are not aligned to the epoch",
					ds.Name, grain, raw)
			}
		}
	}
}
```

- [ ] **Step 5: Run the tests and see them pass**

Run: `go test -short ./internal/config/`
Expected: PASS, including every existing `TestCliServe_` test.

- [ ] **Step 6: Regenerate the schema and the example skeleton**

Run: `make schema`
Then: `git diff --stat internal/validate/schemas/serve.json internal/cli/testdata/serve_example.golden`
Expected: both changed; `serve.json` gains `cache` (top level and per dataset) and `bucket`; `config.json` and `rollups.json` unchanged.

Run: `go test -short ./internal/schema/ ./internal/cli/... ./internal/validate/`
Expected: PASS. `internal/validate/serve.go` already reports `conf.Check()`, so `validate` lists the new rules at their lines with no change there.

- [ ] **Step 7: Commit**

```bash
git add internal/config/serve.go internal/config/serve_cache_test.go internal/validate/schemas/serve.json internal/cli/testdata/serve_example.golden
git commit -m "serve: config for an opt-in response cache and each grain's bucket"
```

---

### Task 2: `cache.go` — the store

**Files:**
- Create: `internal/serve/cache.go`
- Create: `internal/serve/cache_test.go`

**Interfaces:**
- Consumes: `result` from `internal/serve/encode.go` (`Columns []column`, `Rows json.RawMessage`, `RowCount int`, `Truncated bool`).
- Produces:
  - `type cacheOutcome string`; `cacheMiss`, `cacheHit`, `cacheShared` = `"miss"`, `"hit"`, `"shared"`
  - `func newCache(maxBytes int64, now func() time.Time) *cache`
  - `func (c *cache) do(ctx context.Context, key string, ttl time.Duration, fill func() (result, error)) (res result, out cacheOutcome, age time.Duration, err error)`
  - `func (c *cache) stats() (bytes int64, entries int)`
  - `c.onEvict func(reason string)` — set once before use; reasons `"size"`, `"expired"`

`fill` takes no context on purpose. The cache never cancels a fill; whoever builds `fill` gives it its own deadline (Task 4). `ctx` bounds the caller's wait and nothing else.

- [ ] **Step 1: Write the failing tests**

Create `internal/serve/cache_test.go`:

```go
package serve

import (
	"context"
	"encoding/json"
	"errors"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

// fakeClock is a clock a test moves. The cache never sleeps, so nothing here
// waits on real time.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{t: time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)}
}
func (f *fakeClock) now() time.Time { f.mu.Lock(); defer f.mu.Unlock(); return f.t }
func (f *fakeClock) advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.t = f.t.Add(d)
}

// sized is a result whose stored size is exactly n bytes: no columns, n
// bytes of rows.
func sized(n int) result {
	return result{Rows: json.RawMessage(make([]byte, n)), RowCount: n}
}

// counted returns a fill that counts its calls and returns res.
func counted(calls *atomic.Int64, res result) func() (result, error) {
	return func() (result, error) { calls.Add(1); return res, nil }
}

func TestCliServe_CacheHitsInsideTheTTLAndFillsAgainAfterIt(t *testing.T) {
	coverage.Covers(t, "cli.serve")
	clk := newFakeClock()
	c := newCache(1<<20, clk.now)
	var calls atomic.Int64
	ctx := context.Background()

	_, out, age, err := c.do(ctx, "k", 30*time.Second, counted(&calls, sized(10)))
	assert.NoError(t, err)
	assert.Equal(t, cacheMiss, out)
	assert.Equal(t, time.Duration(0), age)

	clk.advance(29 * time.Second)
	res, out, age, err := c.do(ctx, "k", 30*time.Second, counted(&calls, sized(10)))
	assert.NoError(t, err)
	assert.Equal(t, cacheHit, out)
	assert.Equal(t, 29*time.Second, age)
	assert.Equal(t, 10, res.RowCount)
	assert.Equal(t, int64(1), calls.Load())

	clk.advance(time.Second)
	_, out, _, err = c.do(ctx, "k", 30*time.Second, counted(&calls, sized(10)))
	assert.NoError(t, err)
	assert.Equal(t, cacheMiss, out)
	assert.Equal(t, int64(2), calls.Load())
}

// The age runs from when the fill started, because that is when the query
// read its data. A fill that takes two seconds leaves TTL minus two.
func TestCliServe_CacheExpiryRunsFromTheStartOfTheFill(t *testing.T) {
	coverage.Covers(t, "cli.serve")
	clk := newFakeClock()
	c := newCache(1<<20, clk.now)
	var calls atomic.Int64
	ctx := context.Background()

	slow := func() (result, error) { calls.Add(1); clk.advance(2 * time.Second); return sized(10), nil }
	_, _, _, err := c.do(ctx, "k", 10*time.Second, slow)
	assert.NoError(t, err)

	clk.advance(7 * time.Second) // 9 s after the start
	_, out, age, _ := c.do(ctx, "k", 10*time.Second, slow)
	assert.Equal(t, cacheHit, out)
	assert.Equal(t, 9*time.Second, age)

	clk.advance(time.Second) // 10 s after the start, 8 s after the end
	_, out, _, _ = c.do(ctx, "k", 10*time.Second, slow)
	assert.Equal(t, cacheMiss, out)
}

func TestCliServe_CacheRunsOneFillForConcurrentCallers(t *testing.T) {
	coverage.Covers(t, "cli.serve")
	c := newCache(1<<20, newFakeClock().now)
	var calls atomic.Int64
	release := make(chan struct{})
	fill := func() (result, error) { calls.Add(1); <-release; return sized(10), nil }

	const n = 16
	outs := make(chan cacheOutcome, n)
	for i := 0; i < n; i++ {
		go func() {
			_, out, _, err := c.do(context.Background(), "k", time.Minute, fill)
			if err != nil {
				out = cacheOutcome("error: " + err.Error())
			}
			outs <- out
		}()
	}
	// Every caller is inside do before the fill is let go.
	for c.waiting("k") < n {
		time.Sleep(time.Millisecond)
	}
	close(release)

	got := map[cacheOutcome]int{}
	for i := 0; i < n; i++ {
		got[<-outs]++
	}
	assert.Equal(t, int64(1), calls.Load())
	assert.Equal(t, 1, got[cacheMiss])
	assert.Equal(t, n-1, got[cacheShared])
}

// The first caller leaving fails nobody, and every caller leaving still
// stores the result: DuckDB would have finished the query anyway.
func TestCliServe_CacheFillOutlivesItsCallers(t *testing.T) {
	coverage.Covers(t, "cli.serve")
	c := newCache(1<<20, newFakeClock().now)
	var calls atomic.Int64
	release := make(chan struct{})
	fill := func() (result, error) { calls.Add(1); <-release; return sized(10), nil }

	ctx, cancel := context.WithCancel(context.Background())
	left := make(chan error, 1)
	go func() { _, _, _, err := c.do(ctx, "k", time.Minute, fill); left <- err }()
	for c.waiting("k") < 1 {
		time.Sleep(time.Millisecond)
	}

	stayed := make(chan cacheOutcome, 1)
	go func() { _, out, _, _ := c.do(context.Background(), "k", time.Minute, fill); stayed <- out }()
	for c.waiting("k") < 2 {
		time.Sleep(time.Millisecond)
	}

	cancel()
	assert.That(t, errors.Is(<-left, context.Canceled))
	close(release)
	assert.Equal(t, cacheShared, <-stayed)

	_, out, _, _ := c.do(context.Background(), "k", time.Minute, fill)
	assert.Equal(t, cacheHit, out)
	assert.Equal(t, int64(1), calls.Load())
}

func TestCliServe_CacheStoresNoError(t *testing.T) {
	coverage.Covers(t, "cli.serve")
	c := newCache(1<<20, newFakeClock().now)
	boom := errors.New("postgres went away")
	var calls atomic.Int64

	_, _, _, err := c.do(context.Background(), "k", time.Minute,
		func() (result, error) { calls.Add(1); return result{}, boom })
	assert.That(t, errors.Is(err, boom))

	_, out, _, err := c.do(context.Background(), "k", time.Minute, counted(&calls, sized(10)))
	assert.NoError(t, err)
	assert.Equal(t, cacheMiss, out)
	assert.Equal(t, int64(2), calls.Load())
	_, entries := c.stats()
	assert.Equal(t, 1, entries)
}

func TestCliServe_CacheEvictsTheColdestAndAHitWarms(t *testing.T) {
	coverage.Covers(t, "cli.serve")
	c := newCache(100, newFakeClock().now) // a quarter is 25
	var evicted []string
	c.onEvict = func(reason string) { evicted = append(evicted, reason) }
	var calls atomic.Int64
	ctx := context.Background()
	put := func(k string) { _, _, _, _ = c.do(ctx, k, time.Minute, counted(&calls, sized(25))) }

	put("a")
	put("b")
	put("c")
	put("d") // 100 bytes: full
	put("a") // a hit: a is now the warmest
	put("e") // evicts b, the coldest

	_, out, _, _ := c.do(ctx, "a", time.Minute, counted(&calls, sized(25)))
	assert.Equal(t, cacheHit, out)
	_, out, _, _ = c.do(ctx, "b", time.Minute, counted(&calls, sized(25)))
	assert.Equal(t, cacheMiss, out)
	assert.Equal(t, "size", evicted[0])
	bytes, _ := c.stats()
	assert.That(t, bytes <= 100)
}

// Recency order is not expiry order. Without the heap the bound would evict
// an answer still good while an expired one kept its bytes.
func TestCliServe_CacheReclaimsExpiredEntriesBeforeLiveOnes(t *testing.T) {
	coverage.Covers(t, "cli.serve")
	clk := newFakeClock()
	c := newCache(100, clk.now)
	var reasons []string
	c.onEvict = func(reason string) { reasons = append(reasons, reason) }
	var calls atomic.Int64
	ctx := context.Background()

	_, _, _, _ = c.do(ctx, "live", time.Hour, counted(&calls, sized(25))) // the coldest
	for _, k := range []string{"x", "y", "z"} {
		_, _, _, _ = c.do(ctx, k, time.Second, counted(&calls, sized(25)))
	}
	clk.advance(2 * time.Second) // x, y and z are expired; live is not

	_, _, _, _ = c.do(ctx, "new", time.Hour, counted(&calls, sized(25)))
	_, out, _, _ := c.do(ctx, "live", time.Hour, counted(&calls, sized(25)))
	assert.Equal(t, cacheHit, out)
	assert.Equal(t, []string{"expired", "expired", "expired"}, reasons)
}

func TestCliServe_CacheDoesNotStoreAResultOverAQuarterOfTheBound(t *testing.T) {
	coverage.Covers(t, "cli.serve")
	c := newCache(100, newFakeClock().now)
	var calls atomic.Int64
	ctx := context.Background()

	res, out, _, err := c.do(ctx, "big", time.Minute, counted(&calls, sized(26)))
	assert.NoError(t, err)
	assert.Equal(t, cacheMiss, out)
	assert.Equal(t, 26, res.RowCount)

	_, out, _, _ = c.do(ctx, "big", time.Minute, counted(&calls, sized(26)))
	assert.Equal(t, cacheMiss, out)
	_, entries := c.stats()
	assert.Equal(t, 0, entries)
}

// A seeded run of every operation, with the structure checked after each
// step: the byte total is the sum of the entries and never over the bound,
// and the map, the list and the heap hold the same entries.
func TestCliServe_CacheInvariantsHoldAcrossARandomRun(t *testing.T) {
	coverage.Covers(t, "cli.serve")
	clk := newFakeClock()
	c := newCache(400, clk.now)
	rng := rand.New(rand.NewSource(1))
	var calls atomic.Int64
	ctx := context.Background()

	for step := 0; step < 5000; step++ {
		key := "k" + strconv.Itoa(rng.Intn(40))
		switch rng.Intn(10) {
		case 0:
			clk.advance(time.Duration(rng.Intn(5)) * time.Second)
		case 1:
			_, _, _, _ = c.do(ctx, key, time.Second, func() (result, error) { return result{}, errors.New("x") })
		default:
			ttl := time.Duration(1+rng.Intn(10)) * time.Second
			_, _, _, _ = c.do(ctx, key, ttl, counted(&calls, sized(1+rng.Intn(120))))
		}
		if problem := c.check(); problem != "" {
			t.Fatalf("step %d: %s", step, problem)
		}
	}
}
```

`c.waiting(key)` and `c.check()` are test hooks defined in `cache.go` (Step 3), unexported, the way `setOnWait` is an unexported seam on the pool.

- [ ] **Step 2: Run them and see them fail**

Run: `go test -short ./internal/serve/ -run TestCliServe_Cache`
Expected: FAIL to compile — `undefined: newCache`.

- [ ] **Step 3: Write `cache.go`**

```go
package serve

import (
	"container/heap"
	"container/list"
	"context"
	"fmt"
	"sync"
	"time"
)

// cacheOutcome says how a cached dataset's request was answered. It is what
// the response, the log line and the counter all name.
type cacheOutcome string

const (
	// cacheMiss ran the query.
	cacheMiss cacheOutcome = "miss"
	// cacheHit was answered from a stored result.
	cacheHit cacheOutcome = "hit"
	// cacheShared waited on a query another request had already started.
	cacheShared cacheOutcome = "shared"
)

// cache holds encoded results by key, bounded by bytes, each for its TTL,
// and runs one fill for concurrent requests of one key.
//
// Nothing is invalidated, because serve is never told that the backend
// changed. Every entry is bounded instead: it expires its TTL after its fill
// started, and nothing gives an entry a longer life than that.
//
// It knows nothing of HTTP, datasets or DuckDB.
type cache struct {
	mu  sync.Mutex
	max int64
	now func() time.Time

	bytes   int64
	entries map[string]*cacheEntry
	// lru is recency order, warmest at the front.
	lru *list.List
	// byExpiry is expiry order, soonest at the top. Recency order is not
	// expiry order, so the bound alone would evict an answer still good
	// while an expired one kept its bytes.
	byExpiry expiryHeap
	flights  map[string]*flight

	// onEvict is told why an entry left: "size" or "expired". Set before the
	// cache is used; nil records nothing.
	onEvict func(reason string)
}

type cacheEntry struct {
	key     string
	res     result
	size    int64
	started time.Time
	expires time.Time
	elem    *list.Element
	index   int
}

// flight is one fill in progress and everyone waiting on it.
type flight struct {
	done    chan struct{}
	started time.Time
	waiters int
	res     result
	err     error
}

func newCache(maxBytes int64, now func() time.Time) *cache {
	return &cache{
		max:     maxBytes,
		now:     now,
		entries: map[string]*cacheEntry{},
		lru:     list.New(),
		flights: map[string]*flight{},
	}
}

// do returns the result for key: a stored one that has not expired, the
// result of a fill already running, or the result of fill, which it starts.
//
// ctx bounds this caller's wait and nothing else. The fill runs to its own
// end whoever leaves: the engine finishes an abandoned query anyway, and a
// result that reaches the cache is the next caller's hit rather than work
// thrown away. fill therefore carries its own deadline.
//
// age is how long ago the result's fill started, and 0 for the caller that
// started it.
func (c *cache) do(ctx context.Context, key string, ttl time.Duration, fill func() (result, error)) (result, cacheOutcome, time.Duration, error) {
	c.mu.Lock()
	now := c.now()
	if e, ok := c.entries[key]; ok {
		if now.Before(e.expires) {
			c.lru.MoveToFront(e.elem)
			res, age := e.res, now.Sub(e.started)
			c.mu.Unlock()
			return res, cacheHit, age, nil
		}
		c.remove(e, "expired")
	}

	f, running := c.flights[key]
	outcome := cacheShared
	if !running {
		outcome = cacheMiss
		f = &flight{done: make(chan struct{}), started: now}
		c.flights[key] = f
		go c.run(key, ttl, f, fill)
	}
	f.waiters++
	c.mu.Unlock()

	select {
	case <-f.done:
		if f.err != nil {
			return result{}, outcome, 0, f.err
		}
		age := time.Duration(0)
		if outcome == cacheShared {
			age = c.now().Sub(f.started)
		}
		return f.res, outcome, age, nil
	case <-ctx.Done():
		return result{}, outcome, 0, ctx.Err()
	}
}

// run is the fill's goroutine. The mutex is never held across fill.
func (c *cache) run(key string, ttl time.Duration, f *flight, fill func() (result, error)) {
	res, err := fill()

	c.mu.Lock()
	delete(c.flights, key)
	f.res, f.err = res, err
	if err == nil {
		c.store(key, res, f.started, ttl)
	}
	c.mu.Unlock()
	close(f.done)
}

// store keeps res, making room in two steps and in this order: everything
// expired, then the coldest. The caller holds the mutex.
func (c *cache) store(key string, res result, started time.Time, ttl time.Duration) {
	size := int64(len(res.Rows))
	for _, col := range res.Columns {
		size += int64(len(col.Name) + len(col.Type))
	}
	// One response must not empty the cache.
	if size > c.max/4 {
		return
	}
	now := c.now()
	expires := started.Add(ttl)
	// A fill that outran its own TTL produced an answer already too old.
	if !now.Before(expires) {
		return
	}

	for len(c.byExpiry) > 0 && !now.Before(c.byExpiry[0].expires) {
		c.remove(c.byExpiry[0], "expired")
	}
	if old, ok := c.entries[key]; ok {
		c.remove(old, "expired")
	}
	for c.bytes+size > c.max {
		c.remove(c.lru.Back().Value.(*cacheEntry), "size")
	}

	e := &cacheEntry{key: key, res: res, size: size, started: started, expires: expires}
	e.elem = c.lru.PushFront(e)
	heap.Push(&c.byExpiry, e)
	c.entries[key] = e
	c.bytes += size
}

// remove drops e from all three structures. The caller holds the mutex.
func (c *cache) remove(e *cacheEntry, reason string) {
	delete(c.entries, e.key)
	c.lru.Remove(e.elem)
	heap.Remove(&c.byExpiry, e.index)
	c.bytes -= e.size
	if c.onEvict != nil {
		c.onEvict(reason)
	}
}

// stats is what the gauges publish: the bytes and entries held. After a
// quiet spell that includes expired entries, until the next store.
func (c *cache) stats() (int64, int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.bytes, len(c.entries)
}

// waiting is how many callers are waiting on key's fill. A test uses it to
// know every caller has arrived before it lets the fill go.
func (c *cache) waiting(key string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	if f, ok := c.flights[key]; ok {
		return f.waiters
	}
	return 0
}

// check names the first broken structural invariant, or returns "". A test
// calls it after every step of a random run.
func (c *cache) check() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.entries) != c.lru.Len() || len(c.entries) != len(c.byExpiry) {
		return fmt.Sprintf("map %d, list %d, heap %d entries", len(c.entries), c.lru.Len(), len(c.byExpiry))
	}
	var sum int64
	for i, e := range c.byExpiry {
		if e.index != i {
			return fmt.Sprintf("entry %s says index %d at heap slot %d", e.key, e.index, i)
		}
		if c.entries[e.key] != e {
			return "heap holds an entry the map does not: " + e.key
		}
		sum += e.size
	}
	if sum != c.bytes {
		return fmt.Sprintf("byte total %d, entries sum to %d", c.bytes, sum)
	}
	if c.bytes > c.max {
		return fmt.Sprintf("%d bytes held over a bound of %d", c.bytes, c.max)
	}
	return ""
}

// expiryHeap is a min-heap of entries by expiry. Each entry tracks its own
// index so remove can take it from the middle.
type expiryHeap []*cacheEntry

func (h expiryHeap) Len() int           { return len(h) }
func (h expiryHeap) Less(i, j int) bool { return h[i].expires.Before(h[j].expires) }
func (h expiryHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index, h[j].index = i, j
}
func (h *expiryHeap) Push(x any) {
	e := x.(*cacheEntry)
	e.index = len(*h)
	*h = append(*h, e)
}
func (h *expiryHeap) Pop() any {
	old := *h
	e := old[len(old)-1]
	old[len(old)-1] = nil
	*h = old[:len(old)-1]
	return e
}
```

- [ ] **Step 4: Run the tests and see them pass**

Run: `go test -short -race ./internal/serve/ -run TestCliServe_Cache -count=3`
Expected: PASS, no race reported, three times over.

- [ ] **Step 5: Prove two tests can fail**

Temporarily change `expires := started.Add(ttl)` to `expires := now.Add(ttl)`.
Run: `go test -short ./internal/serve/ -run TestCliServe_CacheExpiryRunsFromTheStartOfTheFill`
Expected: FAIL (`hit` where `miss` is expected). Revert.

Temporarily delete the `for len(c.byExpiry) > 0 ...` loop in `store`.
Run: `go test -short ./internal/serve/ -run TestCliServe_CacheReclaimsExpiredEntriesBeforeLiveOnes`
Expected: FAIL (`live` is a miss, and a reason is `size`). Revert.

Run `git diff internal/serve/cache.go` and confirm it shows only the new file's intended content.

- [ ] **Step 6: Commit**

```bash
git add internal/serve/cache.go internal/serve/cache_test.go
git commit -m "serve: a byte-bounded cache that expires from the fill's start and runs one fill per key"
```

---

### Task 3: `cachekey.go` — rounding to the bucket, and the key

**Files:**
- Create: `internal/serve/cachekey.go`
- Create: `internal/serve/cachekey_test.go`

**Interfaces:**
- Consumes: `window` (`internal/serve/params.go`: `Since`, `Until string`), `config.ServeParam`.
- Produces:
  - `func ceilTo(t time.Time, bucket time.Duration) time.Time`
  - `func alignRange(sinceName, untilName string, bucket time.Duration, values map[string]any, win *window)`
  - `func cacheKey(dataset, grain string, params []config.ServeParam, values map[string]any) string`

- [ ] **Step 1: Write the failing tests**

Create `internal/serve/cachekey_test.go`:

```go
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
		"another dataset":   key("other", "5m", nil),
		"another grain":     key("posts", "1h", nil),
		"another since":     key("posts", "5m", func(v map[string]any) { v["since"] = ts("2026-09-16T12:10:00Z") }),
		"another lang":      key("posts", "5m", func(v map[string]any) { v["lang"] = "ja" }),
		"another top":       key("posts", "5m", func(v map[string]any) { v["top"] = int64(7) }),
		"an absent lang":    key("posts", "5m", func(v map[string]any) { delete(v, "lang") }),
		"an empty lang":     key("posts", "5m", func(v map[string]any) { v["lang"] = "" }),
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
```

- [ ] **Step 2: Run them and see them fail**

Run: `go test -short ./internal/serve/ -run 'TestCliServe_CeilTo|TestCliServe_AlignRange|TestCliServe_CacheKey'`
Expected: FAIL to compile — `undefined: ceilTo`.

- [ ] **Step 3: Write `cachekey.go`**

```go
package serve

import (
	"strconv"
	"strings"
	"time"

	"github.com/turbolytics/sql-flow/internal/config"
)

// ceilTo rounds t up to the next multiple of bucket since the Unix epoch, in
// UTC, and leaves a t already on one alone. Microseconds, because that is
// what a timestamp param binds.
func ceilTo(t time.Time, bucket time.Duration) time.Time {
	us, b := t.UnixMicro(), bucket.Microseconds()
	r := us % b
	if r < 0 {
		r += b
	}
	if r != 0 {
		us += b - r
	}
	return time.UnixMicro(us).UTC()
}

// alignRange rounds a cached request's since and until up to the grain's
// bucket, in the values the statement binds and in the range the response
// echoes.
//
// Rounding up is exact for the half-open range a cached dataset asserts.
// Buckets are aligned, so for an aligned b, b >= since holds exactly when
// b >= ceil(since), and b < until exactly when b < ceil(until). Every request
// in one bucket-wide window therefore selects the same buckets, and they can
// share an answer. It is not exact for b <= until, which is why caching is
// the dataset author's claim and not the server's guess.
//
// The grain was chosen from the width as sent. Rounding can widen a range by
// less than a bucket, and a request that fit max_range must not be refused
// for it.
func alignRange(sinceName, untilName string, bucket time.Duration, values map[string]any, win *window) {
	since := ceilTo(values[sinceName].(time.Time), bucket)
	until := ceilTo(values[untilName].(time.Time), bucket)
	values[sinceName], values[untilName] = since, until
	win.Since, win.Until = since.Format(time.RFC3339Nano), until.Format(time.RFC3339Nano)
}

// cacheKey spells one request: the dataset, the grain, and each declared
// param's value in declared order. Query-string order and the spelling of a
// timestamp's offset never reach it.
//
// Each value is tagged and a string is length-prefixed, so no value can
// spell another's encoding, and an absent param differs from an empty one:
// one binds NULL and the other ''.
func cacheKey(dataset, grain string, params []config.ServeParam, values map[string]any) string {
	var b strings.Builder
	b.WriteString(dataset)
	b.WriteByte(0)
	b.WriteString(grain)
	for _, p := range params {
		b.WriteByte(0)
		switch v := values[p.Name].(type) {
		case time.Time:
			b.WriteByte('t')
			b.WriteString(strconv.FormatInt(v.UnixMicro(), 10))
		case int64:
			b.WriteByte('i')
			b.WriteString(strconv.FormatInt(v, 10))
		case string:
			b.WriteByte('s')
			b.WriteString(strconv.Itoa(len(v)))
			b.WriteByte(':')
			b.WriteString(v)
		default:
			b.WriteByte('n')
		}
	}
	return b.String()
}
```

- [ ] **Step 4: Run the tests and see them pass**

Run: `go test -short ./internal/serve/ -run 'TestCliServe_CeilTo|TestCliServe_AlignRange|TestCliServe_CacheKey'`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/serve/cachekey.go internal/serve/cachekey_test.go
git commit -m "serve: round a cached range up to its bucket, and key a request by what it binds"
```

---

### Task 4: The handler — a cached dataset goes through the cache

**Files:**
- Modify: `internal/serve/server.go` (the `Server` and `dataset` structs, `spanGrain`, `New`, `newSpan`, `datasetDoc`, `grainDoc`)
- Modify: `internal/serve/http.go` (`rowsResponse`, `queryDataset`, `logEntry`, `logRequests`)
- Modify: `internal/serve/metrics.go` (`observeRequest` gains `ran bool`)
- Create: `internal/serve/cache_http_test.go`

**Interfaces:**
- Consumes: `newCache`, `cache.do`, `cacheOutcome` (Task 2); `alignRange`, `cacheKey` (Task 3); `config.Serve.CacheMaxBytes`, `AnyCached`, `ServeDataset.CacheTTL`, `ServeGrain.Bucket` (Task 1).
- Produces:
  - `Server.cache *cache` — nil when no dataset opted in
  - `dataset.cacheTTL time.Duration`; `spanGrain.bucket time.Duration`; `func (sp *span) bucketOf(grain string) time.Duration`
  - `logEntry.cache string`, `logEntry.ran bool`
  - `func (m *metrics) observeRequest(dataset, grain, code string, query time.Duration, ran bool, total time.Duration)`
  - response fields `cache` and `age_ms`, present only for a cached dataset

- [ ] **Step 1: Write the failing tests**

Create `internal/serve/cache_http_test.go`:

```go
package serve

import (
	"context"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

// cachedTestServe has the same SQL twice, cached and not, so one is the
// control for the other. The ranged pair report what they were bound, the
// way rangedTestServe does.
const cachedTestServe = `
serve:
  auth:
    tokens: [{name: page, token: page-token}]
  datasets:
    - name: status
      cache: {ttl_seconds: 30}
      sql: SELECT count(*) AS n FROM posts
    - name: status_uncached
      sql: SELECT count(*) AS n FROM posts
    - name: broken
      cache: {ttl_seconds: 30}
      params: [{name: v, type: string}]
      sql: SELECT CAST($v AS INTEGER) AS n
    - name: posts
      cache: {ttl_seconds: 30}
      params:
        - {name: since, type: timestamp}
        - {name: until, type: timestamp}
      range: {since: since, until: until, default: 24h}
      grains:
        1h:
          bucket: 1h
          max_range: 14d
          sql: SELECT $since AS since, $until AS until, count(*) AS n, coalesce(sum(posts), 0)::BIGINT AS posts FROM posts WHERE bucket >= $since AND bucket < $until
    - name: posts_uncached
      params:
        - {name: since, type: timestamp}
        - {name: until, type: timestamp}
      range: {since: since, until: until, default: 24h}
      grains:
        1h:
          max_range: 14d
          sql: SELECT $since AS since, $until AS until, count(*) AS n, coalesce(sum(posts), 0)::BIGINT AS posts FROM posts WHERE bucket >= $since AND bucket < $until
`

// countingExecutor counts the statements that run, which is the one thing a
// cache test has to know and a response cannot say.
type countingExecutor struct {
	Executor
	runs atomic.Int64
}

func (c *countingExecutor) Acquire(ctx context.Context) (Session, error) {
	s, err := c.Executor.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	return &countingSession{Session: s, runs: &c.runs}, nil
}

type countingSession struct {
	Session
	runs *atomic.Int64
}

func (c *countingSession) Run(ctx context.Context, st Statement, values map[string]any) (array.RecordReader, error) {
	c.runs.Add(1)
	return c.Session.Run(ctx, st, values)
}

const serveIntegration = "serve.duckdb"

type cachedServer struct {
	*testServer
	ex  *countingExecutor
	clk *fakeClock
}

func newCachedServer(t *testing.T) *cachedServer {
	t.Helper()
	inner, _ := newExec(t, 1, "SET TimeZone='UTC'", createPostsTable)
	ex := &countingExecutor{Executor: inner}
	clk := newFakeClock()
	ts := newTestServerOn(t, cachedTestServe, ex)
	ts.srv.now = clk.now
	ts.srv.cache.now = clk.now
	return &cachedServer{testServer: ts, ex: ex, clk: clk}
}

// A dataset without a cache block is served as it always was.
func TestCliServe_AnUncachedDatasetRunsEveryRequest(t *testing.T) {
	coverage.Covers(t, "cli.serve")
	coverage.Invariant(t, "serve.cache.opt_in", serveIntegration)
	cs := newCachedServer(t)

	for i := 0; i < 3; i++ {
		r := cs.get(t, "/v1/datasets/status_uncached")
		assert.Equal(t, http.StatusOK, r.status)
		_, hasCache := r.body["cache"]
		_, hasAge := r.body["age_ms"]
		assert.That(t, !hasCache && !hasAge)
	}
	assert.Equal(t, int64(3), cs.ex.runs.Load())
}

func TestCliServe_ACachedDatasetAnswersTheSecondRequestFromMemory(t *testing.T) {
	coverage.Covers(t, "cli.serve")
	coverage.Invariant(t, "serve.cache.answers_from_memory", serveIntegration)
	cs := newCachedServer(t)

	first := cs.get(t, "/v1/datasets/status")
	assert.Equal(t, "miss", first.body["cache"])
	assert.Equal(t, float64(0), first.body["age_ms"])

	cs.clk.advance(12 * time.Second)
	second := cs.get(t, "/v1/datasets/status")
	assert.Equal(t, http.StatusOK, second.status)
	assert.Equal(t, "hit", second.body["cache"])
	assert.Equal(t, float64(12000), second.body["age_ms"])
	assert.Equal(t, float64(0), second.body["elapsed_ms"])
	assert.Equal(t, float64(0), second.body["queued_ms"])
	assert.Equal(t, first.body["rows"], second.body["rows"])
	assert.Equal(t, "no-store", second.header.Get("Cache-Control"))
	assert.Equal(t, int64(1), cs.ex.runs.Load())

	// A hit borrows no session: with the only session held, a warm key
	// still answers.
	held, err := cs.ex.Acquire(context.Background())
	assert.NoError(t, err)
	defer held.Release()
	third := cs.get(t, "/v1/datasets/status")
	assert.Equal(t, "hit", third.body["cache"])
}

// The staleness bound, end to end. Removing the expiry check in cache.do
// fails this test, which is what makes it a test.
func TestCliServe_ACachedAnswerIsNeverOlderThanItsTTL(t *testing.T) {
	coverage.Covers(t, "cli.serve")
	coverage.Invariant(t, "serve.cache.bounded_staleness", serveIntegration)
	cs := newCachedServer(t)

	n := func(r response) float64 { return r.body["rows"].([]any)[0].(map[string]any)["n"].(float64) }
	assert.Equal(t, float64(4), n(cs.get(t, "/v1/datasets/status")))

	execSQL(t, cs.ex, "INSERT INTO posts VALUES (TIMESTAMPTZ '2026-09-11 01:00:00+00', 'de', 2)")

	cs.clk.advance(29 * time.Second)
	inside := cs.get(t, "/v1/datasets/status")
	assert.Equal(t, "hit", inside.body["cache"])
	assert.Equal(t, float64(4), n(inside))

	cs.clk.advance(time.Second)
	past := cs.get(t, "/v1/datasets/status")
	assert.Equal(t, "miss", past.body["cache"])
	assert.Equal(t, float64(5), n(past))
}

// Rounding to the bucket changes the key and never the rows. Every request is
// compared with an uncached dataset running the same SQL on the values as
// sent. Rounding down instead of up fails this, and so does writing the SQL
// as bucket <= $until, which is the case the cache block asks an author to
// rule out.
func TestCliServe_RoundingToTheBucketNeverChangesTheRows(t *testing.T) {
	coverage.Covers(t, "cli.serve")
	coverage.Invariant(t, "serve.cache.bucket_exact", serveIntegration)
	cs := newCachedServer(t)

	row := func(r response) map[string]any {
		assert.Equal(t, http.StatusOK, r.status)
		return r.body["rows"].([]any)[0].(map[string]any)
	}
	// The fixture has rows at 2026-09-10 00:00 and 01:00. Each range below
	// puts an edge inside, on, or either side of one of those buckets.
	for _, q := range []string{
		"?since=2026-09-09T23:30:00Z&until=2026-09-10T00:30:00Z",
		"?since=2026-09-10T00:00:00Z&until=2026-09-10T01:00:00Z",
		"?since=2026-09-10T00:00:00.000001Z&until=2026-09-10T01:00:00.000001Z",
		"?since=2026-09-10T00:59:59Z&until=2026-09-10T01:00:01Z",
		"?since=2026-09-10T00:10:00Z&until=2026-09-10T00:50:00Z",
		"?since=2026-09-09T00:00:00Z&until=2026-09-12T00:00:00Z",
	} {
		t.Run(q, func(t *testing.T) {
			cached, plain := row(cs.get(t, "/v1/datasets/posts"+q)), row(cs.get(t, "/v1/datasets/posts_uncached"+q))
			assert.Equal(t, plain["n"], cached["n"])
			assert.Equal(t, plain["posts"], cached["posts"])
		})
	}

	// Two ranges in one bucket window are one key, and the echoed range is
	// the rounded one on the miss and on the hit alike.
	a := cs.get(t, "/v1/datasets/posts?since=2026-09-10T00:10:00Z&until=2026-09-10T02:10:00Z")
	b := cs.get(t, "/v1/datasets/posts?since=2026-09-10T00:40:00Z&until=2026-09-10T02:55:00Z")
	assert.Equal(t, "miss", a.body["cache"])
	assert.Equal(t, "hit", b.body["cache"])
	for _, r := range []response{a, b} {
		win := r.body["range"].(map[string]any)
		assert.Equal(t, "2026-09-10T01:00:00Z", win["since"])
		assert.Equal(t, "2026-09-10T03:00:00Z", win["until"])
		assert.Equal(t, win["since"], row(r)["since"])
	}

	// Either side of a boundary is two keys.
	c := cs.get(t, "/v1/datasets/posts?since=2026-09-10T01:00:01Z&until=2026-09-10T02:10:00Z")
	assert.Equal(t, "miss", c.body["cache"])
}

// The grain is chosen before rounding: a range that fits max_range as sent is
// served though rounding widens it past max_range.
func TestCliServe_RoundingNeverRefusesARangeThatFit(t *testing.T) {
	coverage.Covers(t, "cli.serve")
	cs := newCachedServer(t)

	// Exactly 14d, neither edge aligned: rounded, it is 14d still, and one
	// that started a second later would round to 14d1h.
	r := cs.get(t, "/v1/datasets/posts?since=2026-08-27T00:00:01Z&until=2026-09-10T00:00:01Z")
	assert.Equal(t, http.StatusOK, r.status)
	r = cs.get(t, "/v1/datasets/posts?since=2026-08-27T00:59:59Z&until=2026-09-10T00:00:01Z")
	assert.Equal(t, http.StatusOK, r.status)
}

func TestCliServe_AFailedQueryIsNotCached(t *testing.T) {
	coverage.Covers(t, "cli.serve")
	coverage.Invariant(t, "serve.cache.errors_not_stored", serveIntegration)
	cs := newCachedServer(t)

	for i := int64(1); i <= 2; i++ {
		r := cs.get(t, "/v1/datasets/broken?v=nope")
		assert.Equal(t, http.StatusInternalServerError, r.status)
		code, _ := errorOf(t, r)
		assert.Equal(t, "query_failed", code)
		assert.Equal(t, i, cs.ex.runs.Load())
	}
}

func TestCliServe_ConcurrentRequestsForOneKeyRunOneQuery(t *testing.T) {
	coverage.Covers(t, "cli.serve")
	coverage.Invariant(t, "serve.cache.one_fill", serveIntegration)
	cs := newCachedServer(t)

	// Hold the only session, so every request is waiting on the one fill
	// before it can run.
	held, err := cs.ex.Executor.Acquire(context.Background())
	assert.NoError(t, err)

	const n = 8
	done := make(chan response, n)
	for i := 0; i < n; i++ {
		go func() { done <- cs.get(t, "/v1/datasets/status") }()
	}
	key := cacheKey("status", "", nil, nil)
	for cs.srv.cache.waiting(key) < n {
		time.Sleep(time.Millisecond)
	}
	held.Release()

	got := map[string]int{}
	for i := 0; i < n; i++ {
		r := <-done
		assert.Equal(t, http.StatusOK, r.status)
		got[r.body["cache"].(string)]++
	}
	assert.Equal(t, 1, got["miss"])
	assert.Equal(t, n-1, got["shared"])
	assert.Equal(t, int64(1), cs.ex.runs.Load())
}

func TestCliServe_AWarmKeyStillNeedsAToken(t *testing.T) {
	coverage.Covers(t, "cli.serve")
	cs := newCachedServer(t)

	assert.Equal(t, http.StatusOK, cs.get(t, "/v1/datasets/status").status)
	r := cs.do(t, http.MethodGet, "/v1/datasets/status", nil)
	assert.Equal(t, http.StatusUnauthorized, r.status)
}

func TestCliServe_TheLogLineAndTheListingNameTheCache(t *testing.T) {
	coverage.Covers(t, "cli.serve")
	cs := newCachedServer(t)

	cs.get(t, "/v1/datasets/status")
	cs.get(t, "/v1/datasets/status")
	cs.get(t, "/v1/datasets/status_uncached")
	var seen []string
	for _, e := range cs.logs.FilterMessage("request").All() {
		seen = append(seen, e.ContextMap()["cache"].(string))
	}
	assert.Equal(t, []string{"miss", "hit", ""}, seen)

	listing := cs.get(t, "/v1/datasets")
	assert.That(t, strings.Contains(listing.raw, `"cache":{"ttl_seconds":30}`))
	assert.That(t, strings.Contains(listing.raw, `"bucket":"1h"`))
}
```

Two helpers this file uses do not exist yet. Add them to `internal/serve/http_test.go`:

```go
// newTestServerOn builds the server over an executor the test already has,
// for a test that wraps one.
func newTestServerOn(t *testing.T, text string, ex Executor) *testServer {
	t.Helper()
	conf, err := config.ParseServe([]byte(text))
	assert.NoError(t, err)

	core, logs := observer.New(zap.InfoLevel)
	srv, err := New(context.Background(), conf, ex, WithLogger(zap.New(core)))
	assert.NoError(t, err)
	t.Cleanup(srv.Close)
	return &testServer{srv: srv, handler: srv.Handler(), logs: logs}
}

// execSQL changes the data under a running server, through a statement of
// its own, the way a pipeline's write would.
func execSQL(t *testing.T, ex Executor, sql string) {
	t.Helper()
	st, err := ex.Prepare(context.Background(), StatementSpec{Dataset: "test", SQL: sql})
	assert.NoError(t, err)
	sess, err := ex.Acquire(context.Background())
	assert.NoError(t, err)
	defer sess.Release()
	rdr, err := sess.Run(context.Background(), st, nil)
	assert.NoError(t, err)
	for rdr.Next() {
	}
	rdr.Release()
}
```

Before relying on `execSQL`, read `internal/serve/duckdb.go`'s `Prepare`: if it refuses a statement that returns no columns or is not a `SELECT`, change `execSQL` to take the `adbc.Connection` that `newExec` returns as its second value and execute on that instead (`internal/rollup/serve_test.go`'s `execDuck` shows the ADBC calls), and have `newCachedServer` keep that connection. Either way the test's assertions do not change.

- [ ] **Step 2: Run them and see them fail**

Run: `go test -short ./internal/serve/ -run 'TestCliServe_AnUncached|TestCliServe_ACached|TestCliServe_Rounding|TestCliServe_AFailedQuery|TestCliServe_Concurrent|TestCliServe_AWarmKey|TestCliServe_TheLogLine'`
Expected: FAIL to compile — `ts.srv.cache undefined`.

- [ ] **Step 3: Carry the cache config into the server**

In `internal/serve/server.go`:

Add to `Server`, after `exec`:

```go
	// cache is nil unless some dataset opted in. Only a cached dataset's
	// requests reach it.
	cache *cache
```

Add to `dataset`, after `timeout`:

```go
	// cacheTTL is 0 for a dataset that did not opt into the cache.
	cacheTTL time.Duration
```

Replace `spanGrain` and add `bucketOf`:

```go
type spanGrain struct {
	name string
	max  time.Duration
	// bucket is 0 for a grain that declares none.
	bucket time.Duration
}

// bucketOf is the width of one of grain's buckets, or 0.
func (sp *span) bucketOf(grain string) time.Duration {
	for _, g := range sp.grains {
		if g.name == grain {
			return g.bucket
		}
	}
	return 0
}
```

In `New`, directly after the options loop and **before** the metrics block, because Task 5's gauges read the cache:

```go
	if conf.Serve.AnyCached() {
		s.cache = newCache(conf.Serve.CacheMaxBytes(), time.Now)
	}
```

In `New`'s dataset loop, set the TTL where `ds` is built (`cacheTTL: dc.CacheTTL(),`), and after the `dc.Range` block add:

```go
		if dc.Cache != nil {
			doc.Cache = &cacheDoc{TTLSeconds: dc.Cache.TTLSeconds}
		}
```

Inside the existing `for _, g := range sp.grains` loop in `New`, also set the bucket on the grain's doc:

```go
			if g.bucket > 0 {
				gd.Bucket = config.FormatServeDuration(g.bucket)
			}
```

Add one field to `datasetDoc`, after `Range`, one to `grainDoc`, as its first field, and the new type:

```go
	// in datasetDoc
	Cache *cacheDoc `json:"cache,omitempty"`

	// in grainDoc, first
	Bucket string `json:"bucket,omitempty"`

// cacheDoc tells a caller how stale a dataset's answer may be.
type cacheDoc struct {
	TTLSeconds int `json:"ttl_seconds"`
}
```

In `newSpan`, inside the grain loop, parse the bucket before the `append`:

```go
		var bucket time.Duration
		if raw := dc.Grains[name].Bucket; raw != "" {
			if bucket, err = config.ParseServeDuration(raw); err != nil {
				return nil, fmt.Errorf("dataset %s grain %s: bucket: %w", dc.Name, name, err)
			}
		}
		sp.grains = append(sp.grains, spanGrain{name: name, max: max, bucket: bucket})
```

(and delete the old `append` line it replaces).

- [ ] **Step 4: Route a cached dataset through the cache**

In `internal/serve/http.go`, add to `rowsResponse` after `ElapsedMS`:

```go
	// Cache and AgeMS are present only for a dataset that opted into the
	// cache. On a hit or a shared fill, queued_ms and elapsed_ms are this
	// request's own wait and work, never the filling request's: a reader
	// must not be sent looking for a slow query this request did not run.
	Cache string `json:"cache,omitempty"`
	AgeMS *int64 `json:"age_ms,omitempty"`
```

Add to `logEntry`: `cache string` beside `code`, and `ran bool` beside `measured`, with this comment on `ran`:

```go
	// ran is whether this request ran a query. A hit did not, and must not
	// drag the query histogram toward zero.
	ran bool
```

In `queryDataset`, replace everything from `start := time.Now()` through the line `entry.queryDur, entry.measured = time.Since(start)-queued, true` with:

```go
	start := time.Now()
	var (
		res     result
		queued  time.Duration
		work    time.Duration
		outcome cacheOutcome
		age     time.Duration
	)
	// err is the function's own, declared where the query string is parsed.
	if ds.cacheTTL == 0 {
		res, queued, err = query(ctx, s.exec, st.stmt, values, ds.maxRows)
		work = time.Since(start) - queued
		entry.ran = true
	} else {
		// Check guarantees a bucket on every grain of a cached range. The
		// guard is for a Server built from a config nobody checked: it must
		// serve unrounded rather than divide by zero.
		if ds.span != nil {
			if bucket := ds.span.bucketOf(st.grain); bucket > 0 {
				alignRange(ds.span.since, ds.span.until, bucket, values, win)
			}
		}
		// The fill answers to its own deadline, not this caller's. It is the
		// dataset's timeout either way, so a fill nobody waits for is bounded
		// as a request is.
		var fillQueued, fillWork time.Duration
		res, outcome, age, err = s.cache.do(ctx, cacheKey(name, st.grain, ds.conf.Params, values), ds.cacheTTL,
			func() (result, error) {
				fctx, done := context.WithTimeout(context.Background(), ds.timeout)
				defer done()
				began := time.Now()
				r, q, e := query(fctx, s.exec, st.stmt, values, ds.maxRows)
				fillQueued, fillWork = q, time.Since(began)-q
				return r, e
			})
		switch {
		case outcome == cacheMiss && err == nil:
			// Read only after the fill is done, which do waited for.
			queued, work = fillQueued, fillWork
			entry.ran = true
		case outcome == cacheShared:
			queued = time.Since(start)
		}
		entry.cache = string(outcome)
		s.metrics.observeCache(name, outcome)
	}
	// Measured whatever the outcome: the error mix is what the counter is
	// for, so a timeout and a failure count too.
	entry.queryDur, entry.measured = work, true
```

`alignRange` must run before `cacheKey`, because the key is built from the rounded values. `values` is not written again after this point, so the fill's goroutine reading it is safe.

Then replace the `json.Marshal(rowsResponse{...})` call's last two fields and add the cache fields:

```go
	resp := rowsResponse{
		Dataset:   name,
		Grain:     st.grain,
		Range:     win,
		Columns:   res.Columns,
		Rows:      res.Rows,
		RowCount:  res.RowCount,
		Truncated: res.Truncated,
		QueuedMS:  queued.Milliseconds(),
		ElapsedMS: work.Milliseconds(),
	}
	if ds.cacheTTL > 0 {
		ms := age.Milliseconds()
		resp.Cache, resp.AgeMS = string(outcome), &ms
	}
	body, err := json.Marshal(resp)
```

The `switch` on `err` between those two blocks stays exactly as it is: a fill's own timeout is a `context.DeadlineExceeded` and maps to `query_timeout`, and this caller's deadline arrives the same way.

In `logRequests`, add `zap.String("cache", entry.cache),` after the `grain` field, and change the metrics call to:

```go
			s.metrics.observeRequest(entry.dataset, entry.grain, code, entry.queryDur, entry.ran, time.Since(start))
```

- [ ] **Step 5: Observe a query only when one ran**

In `internal/serve/metrics.go`, change `observeRequest`:

```go
// observeRequest records one finished request. code is the error code, or
// "ok". ran is whether the request ran a query: a cache hit did not, and
// recording its zero would flatten the query histogram.
func (m *metrics) observeRequest(dataset, grain, code string, query time.Duration, ran bool, total time.Duration) {
	if m == nil {
		return
	}
	attrs := metric.WithAttributes(
		attribute.String("dataset", dataset),
		attribute.String("grain", grain),
		attribute.String("code", code),
	)
	ctx := context.Background()
	m.requests.Add(ctx, 1, attrs)
	m.requestDuration.Record(ctx, total.Seconds(), attrs)
	if ran {
		m.queryDuration.Record(ctx, query.Seconds(), attrs)
	}
}
```

and add a stub that Task 5 fills in, so this task compiles on its own:

```go
// observeCache counts one cached dataset's request by how it was answered.
func (m *metrics) observeCache(dataset string, outcome cacheOutcome) {
	if m == nil || m.cacheRequests == nil {
		return
	}
	m.cacheRequests.Add(context.Background(), 1, metric.WithAttributes(
		attribute.String("dataset", dataset),
		attribute.String("outcome", string(outcome)),
	))
}
```

with the field `cacheRequests metric.Int64Counter` added to the `metrics` struct. It stays nil until Task 5 builds it.

An uncached request that fails before a query runs (a timeout waiting for a session) sets `entry.ran = true` above, as it did before this change when `measured` was the only flag. That is deliberate: the uncached path's metrics must not change.

- [ ] **Step 6: Run everything**

Run: `go test -short -race ./internal/serve/ -count=2`
Expected: PASS, every old test included. `TestCliServe_LogsOneLinePerRequest` and `TestCliServe_DatasetResponseCarriesEveryField` must pass **unedited**: if either needs a change, the uncached path changed, which the spec forbids. Find what changed and undo it.

- [ ] **Step 7: Prove the two invariant tests can fail**

Temporarily change `ceilTo` so it rounds down (`us -= r` in place of `us += b - r`).
Run: `go test -short ./internal/serve/ -run TestCliServe_RoundingToTheBucketNeverChangesTheRows`
Expected: FAIL. Revert.

Temporarily change both `posts` grains' SQL in `cachedTestServe` (cached and uncached) to `bucket <= $until`.
Run the same test. Expected: FAIL on a range whose `until` is inside a bucket that holds a row. Revert.

Temporarily change `if now.Before(e.expires)` in `cache.do` to `if true`.
Run: `go test -short ./internal/serve/ -run TestCliServe_ACachedAnswerIsNeverOlderThanItsTTL`
Expected: FAIL. Revert. Confirm with `git diff` that none of the three edits remain.

- [ ] **Step 8: Commit**

```bash
git add internal/serve/server.go internal/serve/http.go internal/serve/metrics.go internal/serve/http_test.go internal/serve/cache_http_test.go
git commit -m "serve: a dataset that opts in is answered from the cache, and every other as before"
```

---

### Task 5: Metrics

**Files:**
- Modify: `internal/serve/metrics.go`, `internal/serve/server.go`
- Modify: `internal/serve/metrics_test.go`

**Interfaces:**
- Consumes: `cache.stats()`, `cache.onEvict` (Task 2); `metrics.observeCache` stub (Task 4).
- Produces: `func newMetrics(reg *prom.Registry, stats func() Stats, cacheStats func() (int64, int)) (*metrics, error)` — `cacheStats` is nil when there is no cache; `func (m *metrics) observeEviction(reason string)`.

- [ ] **Step 1: Write the failing test**

Append to `internal/serve/metrics_test.go`:

```go
// The cache's four instruments, and the rule that a hit leaves the query
// histogram alone.
func TestCliServe_CacheMetrics(t *testing.T) {
	coverage.Covers(t, "cli.serve")

	text := strings.Replace(cachedTestServe, "serve:\n", "serve:\n  metrics: {enabled: true}\n", 1)
	reg := prom.NewRegistry()
	ts := newTestServerWith(t, text, WithMetrics(reg))

	ts.get(t, "/v1/datasets/status")
	ts.get(t, "/v1/datasets/status")
	ts.get(t, "/v1/datasets/status")

	body := ts.do(t, http.MethodGet, "/metrics", nil).raw
	assert.Equal(t, "1", sampleValue(t, body, `sqlflow_serve_cache_requests_total{dataset="status",otel_scope_name="sqlflow.serve",otel_scope_schema_url="",otel_scope_version="",outcome="miss"}`))
	assert.Equal(t, "2", sampleValue(t, body, `sqlflow_serve_cache_requests_total{dataset="status",otel_scope_name="sqlflow.serve",otel_scope_schema_url="",otel_scope_version="",outcome="hit"}`))
	assert.Equal(t, "1", sampleValue(t, body, `sqlflow_serve_cache_entries{otel_scope_name="sqlflow.serve",otel_scope_schema_url="",otel_scope_version=""}`))
	assert.That(t, strings.Contains(body, "sqlflow_serve_cache_bytes"))

	// Three requests, one query.
	assert.Equal(t, "3", sampleValue(t, body, `sqlflow_serve_request_duration_seconds_count{code="ok",dataset="status",grain="",otel_scope_name="sqlflow.serve",otel_scope_schema_url="",otel_scope_version=""}`))
	assert.Equal(t, "1", sampleValue(t, body, `sqlflow_serve_query_duration_seconds_count{code="ok",dataset="status",grain="",otel_scope_name="sqlflow.serve",otel_scope_schema_url="",otel_scope_version=""}`))
}
```

Read `sampleValue` and `TestCliServe_MetricsCarryEveryInstrument` first, and spell each sample name the way that test already does; the label order above is the exporter's alphabetical order, as it appears in a live `/metrics`. Add whatever imports `metrics_test.go` lacks (`net/http`, `strings`).

- [ ] **Step 2: Run it and see it fail**

Run: `go test -short ./internal/serve/ -run TestCliServe_CacheMetrics`
Expected: FAIL — no `sqlflow_serve_cache_requests_total` sample.

- [ ] **Step 3: Build the instruments**

In `internal/serve/metrics.go`, add to the struct `cacheEvictions metric.Int64Counter`, change the signature to `newMetrics(reg *prom.Registry, stats func() Stats, cacheStats func() (int64, int))`, and before `return &mm, nil` add:

```go
	// A server where no dataset opted in has no cache, and publishes nothing
	// about one.
	if cacheStats != nil {
		if mm.cacheRequests, err = m.Int64Counter("sqlflow_serve_cache_requests_total",
			metric.WithDescription("Requests to cached datasets, by dataset and by whether the answer was a hit, a miss, or shared with a query already running.")); err != nil {
			return nil, err
		}
		if mm.cacheEvictions, err = m.Int64Counter("sqlflow_serve_cache_evictions_total",
			metric.WithDescription("Entries dropped, by reason: size, when the bound needed the room, or expired.")); err != nil {
			return nil, err
		}
		cacheBytes, err := m.Int64ObservableGauge("sqlflow_serve_cache_bytes",
			metric.WithDescription("Bytes of encoded results held. After a quiet spell this includes expired entries, until the next store reclaims them."),
			metric.WithUnit("By"))
		if err != nil {
			return nil, err
		}
		cacheEntries, err := m.Int64ObservableGauge("sqlflow_serve_cache_entries",
			metric.WithDescription("Results held, counted the way the bytes are."))
		if err != nil {
			return nil, err
		}
		if _, err := m.RegisterCallback(func(_ context.Context, o metric.Observer) error {
			bytes, entries := cacheStats()
			o.ObserveInt64(cacheBytes, bytes)
			o.ObserveInt64(cacheEntries, int64(entries))
			return nil
		}, cacheBytes, cacheEntries); err != nil {
			return nil, err
		}
	}
```

and:

```go
// observeEviction counts one entry leaving the cache.
func (m *metrics) observeEviction(reason string) {
	if m == nil || m.cacheEvictions == nil {
		return
	}
	m.cacheEvictions.Add(context.Background(), 1, metric.WithAttributes(attribute.String("reason", reason)))
}
```

In `internal/serve/server.go`, the cache is already built above the metrics block (Task 4). Change the metrics block's first lines to:

```go
		var cacheStats func() (int64, int)
		if s.cache != nil {
			cacheStats = s.cache.stats
		}
		m, err := newMetrics(s.registry, ex.Stats, cacheStats)
		if err != nil {
			return nil, err
		}
		s.metrics = m
		if s.cache != nil {
			s.cache.onEvict = m.observeEviction
		}
```

`onEvict` runs under the cache's mutex; `observeEviction` takes no lock of the cache's, so there is no cycle.

Update the comment on the `metrics` type: "the six instruments the pool needs" becomes "the pool's six instruments, and the cache's four when a dataset opted in".

- [ ] **Step 4: Run the tests**

Run: `go test -short -race ./internal/serve/`
Expected: PASS, including `TestCliServe_MetricsCarryEveryInstrument` unedited (its config has no cached dataset, so it must see no cache metric).

- [ ] **Step 5: Commit**

```bash
git add internal/serve/metrics.go internal/serve/metrics_test.go internal/serve/server.go
git commit -m "serve: four cache metrics, and a query histogram that only counts queries"
```

---

### Task 6: The rollup generator writes `bucket`, and `cache` when asked

**Files:**
- Modify: `internal/config/rollup.go`, `internal/config/rollup_check.go`, `internal/config/rollup_check_test.go`
- Modify: `internal/rollup/serve.go`, `internal/rollup/check.go`, `internal/rollup/check_test.go`
- Modify: `dev/config/rollups/bluesky.yml`
- Regenerate: `internal/rollup/testdata/bluesky.serve.yml`, `internal/validate/schemas/rollups.json`

**Interfaces:**
- Consumes: `config.ServeDatasetCache`, `config.ServeGrain.Bucket` (Task 1).
- Produces: `RollupDataset.CacheTTLSeconds int` (`yaml:"cache_ttl_seconds,omitempty"`).

- [ ] **Step 1: Write the failing tests**

Append to `internal/rollup/serve_test.go`:

```go
// Every generated grain carries its bucket, which is its name: a rollup
// grain is named for its width. The cache block appears only when the
// declaration asks.
func TestCliRollup_ServeDatasetsCarryBucketsAndTheCacheOptIn(t *testing.T) {
	coverage.Covers(t, "cli.rollup")

	conf := loadExample(t)
	for i := range conf.Rollups[0].Serve.Datasets {
		conf.Rollups[0].Serve.Datasets[i].CacheTTLSeconds = 0
	}
	conf.Rollups[0].Serve.Datasets[0].CacheTTLSeconds = 30

	datasets, err := ServeDatasets(conf)
	assert.NoError(t, err)
	for _, ds := range datasets {
		for name, g := range ds.Grains {
			assert.Equal(t, name, g.Bucket)
		}
	}
	assert.Equal(t, 30, datasets[0].Cache.TTLSeconds)
	for _, ds := range datasets[1:] {
		assert.That(t, ds.Cache == nil)
	}
}
```

Append a case to the table in `internal/config/rollup_check_test.go`'s `TestCliRollup_CheckReportsEachRuleAtItsPath`. Read the table's struct first and match its fields; the mutation is "add `cache_ttl_seconds: -1` to the first serve dataset", the path is `rollups.0.serve.datasets.0.cache_ttl_seconds`, and the message contains `must not be negative`.

Append to `internal/rollup/check_test.go`, modelled on the drift test already there (read it, and reuse its way of building the generated serve config and mutating one field):

```go
// A hand-edited bucket or cache block is drift, like a hand-edited max_range.
```

with two subtests: one changes a generated grain's `Bucket` to `"7m"` and expects a violation whose path ends `grains.<grain>.bucket`; one sets a generated dataset's `Cache` to `&config.ServeDatasetCache{TTLSeconds: 5}` when the declaration says 30 and expects a violation whose path ends `cache`.

- [ ] **Step 2: Run them and see them fail**

Run: `go test -short ./internal/rollup/ ./internal/config/ -run 'TestCliRollup_'`
Expected: FAIL to compile — `CacheTTLSeconds undefined`.

- [ ] **Step 3: Implement**

`internal/config/rollup.go`, add to `RollupDataset` after `Filters`:

```go
	// Opts the generated dataset into serve's response cache, for this many
	// seconds. 0 leaves it out. The generated SQL reads the range half-open,
	// which is what the cache's key needs.
	CacheTTLSeconds int `yaml:"cache_ttl_seconds,omitempty" jsonschema:"minimum=0"`
```

`internal/config/rollup_check.go`, in `checkRollupDataset` after the name checks:

```go
	if ds.CacheTTLSeconds < 0 {
		add(at(path, "cache_ttl_seconds"), "rollup %s dataset %s: cache_ttl_seconds is %d; it must not be negative, and 0 means no cache",
			r.Name, ds.Name, ds.CacheTTLSeconds)
	}
```

`internal/rollup/serve.go`, in `serveDataset`: set the bucket in the grain loop and the cache on the dataset:

```go
	for g, maxRange := range ds.MaxRange {
		// A rollup grain is named for its width, so the name is the bucket.
		grains[g] = config.ServeGrain{Bucket: g, MaxRange: maxRange, SQL: grainSQL(r, set, ds, g)}
	}

	var cache *config.ServeDatasetCache
	if ds.CacheTTLSeconds > 0 {
		cache = &config.ServeDatasetCache{TTLSeconds: ds.CacheTTLSeconds}
	}
```

and add `Cache: cache,` to the returned `config.ServeDataset`.

`internal/rollup/check.go`, in `compareDataset`, after the `Range` comparison:

```go
	if !reflect.DeepEqual(want.Cache, got.Cache) {
		drift(child("cache"), "dataset %s: the cache block differs from what the declaration's cache_ttl_seconds generates", want.Name)
	}
```

and inside the per-grain loop, before the `max_range` comparison:

```go
		if want.Grains[g].Bucket != got.Grains[g].Bucket {
			drift(child("grains", g, "bucket"), "dataset %s grain %s: bucket is %q; the declaration generates %s",
				want.Name, g, got.Grains[g].Bucket, want.Grains[g].Bucket)
		}
```

`dev/config/rollups/bluesky.yml`: add `cache_ttl_seconds: 30` to the `posts_by_lang` serve dataset, directly under its `default_range`, so the golden shows both a cached and an uncached dataset.

- [ ] **Step 4: Regenerate the goldens and read the diff**

Run: `UPDATE_GOLDEN=1 go test -short ./internal/rollup/ -run TestCliRollup_ServeYAMLMatchesTheGolden && make schema`
Run: `git diff internal/rollup/testdata/bluesky.serve.yml`
Expected: every grain gains one `bucket:` line equal to its name, `posts_by_lang` gains `cache:\n  ttl_seconds: 30`, and **no SQL line changes**. If any SQL changed, stop: the generator's SQL is not this task's business.

- [ ] **Step 5: Run the tests**

Run: `go test -short ./internal/rollup/ ./internal/config/ ./internal/schema/ ./internal/cli/... ./internal/validate/`
Expected: PASS. `TestCliRollup_ServeDatasetsPassServesRules` now also proves the generated `bucket`s pass Task 1's rules.

- [ ] **Step 6: Commit**

```bash
git add internal/config/rollup.go internal/config/rollup_check.go internal/config/rollup_check_test.go internal/rollup/ dev/config/rollups/bluesky.yml internal/validate/schemas/rollups.json
git commit -m "rollup: every generated grain names its bucket, and cache_ttl_seconds opts a dataset into the cache"
```

---

### Task 7: The invariants registry

**Files:**
- Modify: `scripts/coverage_matrix/registries.py` (`FAMILIES`, `KINDS`, `INTEGRATION_KINDS`)
- Modify: `docs/coverage/invariants.yml`
- Create: `docs/coverage/integrations/serve.duckdb.yml`
- Modify: `docs/coverage/integrations/README.md` (the `kind` list)
- Modify: `internal/coverage/registry_test.go`
- Create: `internal/serve/cache_bound_test.go`
- Generated: `docs/coverage/status/serve.duckdb.yml` and the matrix pages, by `make coverage-matrix`

All seven rows share one family, `cache`: the matrix draws a table per family with a column per integration the family touches, so a serve row filed under `resilience` or `errors` puts an empty `serve.duckdb` column on the sinks' tables.

**Interfaces:**
- Consumes: the `coverage.Invariant(t, id, "serve.duckdb")` markers Task 4's tests already emit for six of the seven ids.

- [ ] **Step 1: Write the seventh invariant's test**

`serve.cache.bounded_bytes` is a claim about the running server, so its marker belongs on a handler test. Create `internal/serve/cache_bound_test.go`:

```go
package serve

import (
	"net/http"
	"strconv"
	"strings"
	"testing"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

// Whatever is asked for, the cache holds no more than its bound. A caller can
// vary a param to mint keys nobody else asks for; the bound makes that churn,
// not growth.
func TestCliServe_TheCacheNeverHoldsMoreThanItsBound(t *testing.T) {
	coverage.Covers(t, "cli.serve")
	coverage.Invariant(t, "serve.cache.bounded_bytes", serveIntegration)

	text := strings.Replace(cachedTestServe, "serve:\n", "serve:\n  cache: {max_mb: 1}\n", 1)
	text = strings.Replace(text, "    - name: status\n", `    - name: echo
      cache: {ttl_seconds: 300}
      params: [{name: v, type: string}]
      sql: SELECT repeat($v, 2000) AS s
    - name: status
`, 1)
	ex, _ := newExec(t, 1, "SET TimeZone='UTC'", createPostsTable)
	ts := newTestServerOn(t, text, ex)

	// Each response is about 20 KB, under a quarter of 1 MiB, so each is
	// stored; two hundred of them are four times the bound.
	for i := 0; i < 200; i++ {
		r := ts.get(t, "/v1/datasets/echo?v=k"+strconv.Itoa(1000000+i)+"xx")
		assert.Equal(t, http.StatusOK, r.status)
		bytes, _ := ts.srv.cache.stats()
		assert.That(t, bytes <= 1<<20)
	}
	bytes, entries := ts.srv.cache.stats()
	assert.That(t, bytes > 1<<19) // it is a cache, not an empty one
	assert.That(t, entries > 1)
	assert.Equal(t, "", ts.srv.cache.check())
}
```

Run: `go test -short ./internal/serve/ -run TestCliServe_TheCacheNeverHoldsMoreThanItsBound`
Expected: PASS (the code exists since Task 2; this test is the evidence the registry needs). If `bytes > 1<<19` fails, the responses are smaller than the comment assumes: print one response's length and raise `repeat`'s count until 200 of them exceed 1 MiB.

- [ ] **Step 2: Register the kind, the family and the integration**

`scripts/coverage_matrix/registries.py`:

```python
FAMILIES = ("resilience", "checkpoint", "types", "lifecycle", "errors", "cache")
```

```python
KINDS = ("sink", "source", "handler", "pipeline", "manager", "serve")
```

```python
# A pipeline configuration is an integration of the harness, though no
# constructor switch builds one. `constructed: false` says so, and the Kinds()
# agreement tests skip those entries. A manager is the same: buildManagedTables
# builds the one kind there is, and no switch lists it. So is serve's executor:
# cli/serve builds the DuckDB one directly.
INTEGRATION_KINDS = ("sink", "source", "handler", "pipeline", "manager", "serve")
```

Create `docs/coverage/integrations/serve.duckdb.yml`:

```yaml
# serve over the DuckDB executor. The integration is the executor because
# that is the part of serve built to be swapped (serve/executor.go says why),
# and everything serve promises a caller must hold whatever runs the SQL
# underneath. A second executor proves these again or is exempt with a reason.
#
# `constructed: false` because no constructor switch lists executors:
# cli/serve builds the one there is.
id: serve.duckdb
kind: serve
constructed: false
implements: [Executor]
feature: cli.serve
exempt: []
```

In `docs/coverage/integrations/README.md`, change the `kind` sentence to list `sink`, `source`, `handler`, `pipeline`, `manager`, `serve`.

In `internal/coverage/registry_test.go`, beside `assert.That(t, kinds["manager"] >= 1)`, add `assert.That(t, kinds["serve"] >= 1)`.

- [ ] **Step 3: Declare the seven invariants**

Append to `docs/coverage/invariants.yml`:

```yaml
  # --- Cache: serve's response cache ----------------------------------------
  # Serve is never told that its backend changed, so the cache invalidates
  # nothing and bounds everything. These say what the bounds are. Each is
  # proven through the HTTP handler over a counting executor, never against
  # the cache alone: the claim is about what a caller is sent.
  - id: serve.cache.bounded_staleness
    family: cache
    class: safety
    applies_to: serve
    claim: >
      A response is never built from a result whose fill started more than
      the dataset's ttl_seconds before the request arrived.
    verified_by: harness
    requires: []

  - id: serve.cache.bucket_exact
    family: cache
    class: safety
    applies_to: serve
    claim: >
      For a ranged dataset, a response from the cache carries the rows a
      query bound to the request's own since and until would have returned
      against the same data. Rounding to the bucket changes the key and never
      the rows.
    verified_by: harness
    requires: []

  - id: serve.cache.opt_in
    family: cache
    class: safety
    applies_to: serve
    claim: >
      A dataset without a cache block runs one query per request, and its
      response carries no cache or age_ms.
    verified_by: harness
    requires: []

  - id: serve.cache.bounded_bytes
    family: cache
    class: safety
    applies_to: serve
    claim: The bytes the cache holds never exceed max_mb, whatever is requested.
    verified_by: harness
    requires: []

  - id: serve.cache.errors_not_stored
    family: cache
    class: safety
    applies_to: serve
    claim: >
      A request that fails stores nothing: the next request for its key runs
      a query.
    verified_by: harness
    requires: []

  - id: serve.cache.one_fill
    family: cache
    class: safety
    applies_to: serve
    claim: >
      Concurrent requests for one key run one query, and a caller leaving
      fails no other caller.
    verified_by: harness
    requires: []

  # Every safety row above holds for a cache that never stores anything.
  - id: serve.cache.answers_from_memory
    family: cache
    class: liveness
    applies_to: serve
    claim: >
      A second request for a key inside its TTL is answered without a query
      and without a session.
    verified_by: harness
    requires: []
```

- [ ] **Step 4: Check the registry, then regenerate the matrix**

Run: `go test -short ./internal/coverage/ ./internal/serve/`
Expected: PASS. `coverage.Invariant` only logs a marker, so Task 4's tests passed before these ids existed; it is `make coverage-check`, next, that holds each marker to the registry. Between Task 4 and this step `make coverage-check` is expected to complain of unknown ids, and is not part of the per-commit gate for that reason.

Run: `make coverage-check`
Expected: passes, or fails only by naming generated files that are stale. If it names a registry error (`applies_to 'serve' is not one of`), Step 2 was missed somewhere; `grep -rn '"manager"' scripts/coverage_matrix internal/coverage` and give `serve` the same treatment at every hit.

Run: `make coverage-matrix` (this builds the image and runs every suite; allow it the time).
Expected: `docs/coverage/status/serve.duckdb.yml` is created with seven lines, each `unit: covered`, and the generated pages gain a serve section. Read `git status` and `git diff --stat docs/coverage` and confirm nothing unrelated to serve changed; if other rows changed, a suite failed locally — do not commit those rows, run `git checkout` on them and say so in the PR.

- [ ] **Step 5: Commit**

```bash
git add scripts/coverage_matrix/registries.py docs/coverage internal/coverage/registry_test.go internal/serve/cache_bound_test.go
git commit -m "coverage: a serve kind, a cache family, and seven invariants the cache is held to"
```

---

### Task 8: README

**Files:**
- Modify: `README.md` (the `### sqlflow serve` section, lines ~203–380, and the `sqlflow rollup` section near line 417)

No test cycle of its own; `internal/cli/serve/examples_test.go` already loads every `dev/config/serve/*.yml`, so the example added here is checked.

- [ ] **Step 1: Add an example config that caches**

Create `dev/config/serve/local.cached.yml` by copying `dev/config/serve/local.table.yml` and adding `cache: {ttl_seconds: 15}` to its first dataset, with a header comment in that file's style saying what it shows.

Run: `go test -short ./internal/cli/serve/`
Expected: PASS.

- [ ] **Step 2: Write the section**

After the README's paragraph on ranges and grain selection, add a `#### Caching` subsection that says, in the README's existing voice and in this order:

1. Caching is off. A dataset opts in with `cache: {ttl_seconds: N}`; `serve.cache.max_mb` (default 16) bounds what all of them share and enables nothing. Show the YAML from the spec's Config section.
2. What a cached answer is: the encoded result, served for at most `ttl_seconds` after the query that produced it **started**. Nothing is invalidated, because serve is never told the backend changed; after a backfill, wait one TTL or restart.
3. Ranges: each grain declares `bucket`, its bucket's width; `since` and `until` are rounded **up** to it, the statement binds the rounded values, and `range` echoes them. State the rule in one sentence and the warning in the next: this is exact for `bucket >= $since AND bucket < $until`, and **not** for `bucket <= $until`, which would admit one bucket too many — a dataset written that way must not opt in. `bucket` must divide one day, so a week is refused.
4. Concurrent identical requests run one query, and that query finishes even if every caller leaves, so its result is the next caller's hit.
5. The response fields `cache` (`miss`, `hit`, `shared`) and `age_ms`, and that `queued_ms`/`elapsed_ms` are this request's own.
6. Responses stay `Cache-Control: no-store`.
7. Several instances each hold their own cache and may differ by one TTL.

Add the four metrics to the README's metrics table or list, with the note that the byte and entry gauges include expired entries until the next store. Add `cache` to the documented log fields if the README lists them.

In the `sqlflow rollup` section, document `cache_ttl_seconds` on a serve dataset and that `rollup serve` writes `bucket` on every grain.

- [ ] **Step 3: Check nothing in the README contradicts the spec**

Read the new subsection beside the spec's Config, The key, and Freshness sections. Every number and every rule must match: 16, "at least 1", "divides one day", "from when the fill started", "rounded up".

- [ ] **Step 4: Commit**

```bash
git add README.md dev/config/serve/local.cached.yml
git commit -m "docs: the serve response cache, and the one rule a cached range must keep"
```

---

### Task 9: Whole-change verification

- [ ] **Step 1: Everything, with the race detector**

Run: `go test -short -race ./...`
Expected: PASS.

- [ ] **Step 2: The generated files are current**

Run: `make schema && git status --porcelain`
Expected: no output. Anything listed is a generated file that a task forgot to regenerate; regenerate, amend that task's commit.

- [ ] **Step 3: See it work**

```bash
go build -o /tmp/sqlflow ./cmd/sqlflow
SQLFLOW_SERVE_PORT=18080 /tmp/sqlflow serve -c dev/config/serve/local.cached.yml &
sleep 1
TOKEN=$(grep -m1 'token:' dev/config/serve/local.cached.yml | sed 's/.*token: *//; s/[}"]//g')
NAME=$(grep -m1 -- '- name:' dev/config/serve/local.cached.yml | tail -1 | sed 's/.*name: *//')
for i in 1 2 3; do curl -s -H "Authorization: Bearer $TOKEN" localhost:18080/v1/datasets/$NAME | grep -o '"cache":"[a-z]*","age_ms":[0-9]*'; done
kill %1
```

Adjust the port variable, the token line and the binary path to what `local.table.yml` and `cmd/` actually use (read them first). Expected: `"cache":"miss","age_ms":0`, then two lines of `"cache":"hit"` with a growing `age_ms`.

- [ ] **Step 4: Load, with the owner's say-so**

Do **not** send load at the demo's production API. Ask the repo owner which machine to measure from and against what. Then re-measure the three rows of the spec's opening table at 16 concurrent with the cache on, and put the before and after in the PR description. If no target is approved, say in the PR that the load rows are unmeasured.

- [ ] **Step 5: Mark PR #325 ready**

Push, update the PR title to `serve: an opt-in response cache, keyed at the grain` and its body to list what landed against the spec, with the verification output from Steps 1–4.
