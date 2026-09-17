package serve

import (
	"context"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
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
	// writer changes the data from outside the pool, the way a pipeline's
	// write changes it under a running server.
	writer adbc.Connection
}

func newCachedServer(t *testing.T) *cachedServer {
	t.Helper()
	inner, db := newExec(t, 1, "SET TimeZone='UTC'", createPostsTable)
	writer, err := db.Connect(context.Background())
	assert.NoError(t, err)
	t.Cleanup(func() { _ = writer.Close() })
	ex := &countingExecutor{Executor: inner}
	clk := newFakeClock()
	ts := newTestServerOn(t, cachedTestServe, ex)
	ts.srv.now = clk.now
	ts.srv.cache.now = clk.now
	return &cachedServer{testServer: ts, ex: ex, clk: clk, writer: writer}
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

	execSQL(t, cs.writer, "INSERT INTO posts VALUES (TIMESTAMPTZ '2026-09-11 01:00:00+00', 'de', 2)")

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

// The page sends no fixed range: until is now, and now moves. While the clock
// stays inside one bucket every request is one key; the moment it crosses a
// boundary the rounded until moves with it, the old key is never asked for
// again, and the first request of the new window runs a query. At every step
// the rows are the rows an uncached dataset returns for the same instant.
func TestCliServe_TheKeyRollsOverWithTheClock(t *testing.T) {
	coverage.Covers(t, "cli.serve")
	coverage.Invariant(t, "serve.cache.bucket_exact", serveIntegration)
	cs := newCachedServer(t)

	at := func(s string) {
		target, err := time.Parse(time.RFC3339, s)
		assert.NoError(t, err)
		cs.clk.advance(target.Sub(cs.clk.now()))
	}
	step := func(now, outcome, until string, runs int64) {
		t.Helper()
		at(now)
		before := cs.ex.runs.Load()
		cached := cs.get(t, "/v1/datasets/posts")
		assert.Equal(t, http.StatusOK, cached.status)
		assert.Equal(t, outcome, cached.body["cache"])
		assert.Equal(t, until, cached.body["range"].(map[string]any)["until"])
		assert.Equal(t, runs, cs.ex.runs.Load()-before)

		plain := cs.get(t, "/v1/datasets/posts_uncached")
		c, p := cached.body["rows"].([]any)[0].(map[string]any), plain.body["rows"].([]any)[0].(map[string]any)
		assert.Equal(t, p["n"], c["n"])
		assert.Equal(t, p["posts"], c["posts"])
	}

	// The fixture's last row is at 2026-09-11 00:00, so a 24-hour range ending
	// here holds rows, and loses the 2026-09-10 00:00 bucket's two rows as it
	// slides past them.
	step("2026-09-10T23:59:50Z", "miss", "2026-09-11T00:00:00Z", 1)
	step("2026-09-10T23:59:59Z", "hit", "2026-09-11T00:00:00Z", 0)
	step("2026-09-11T00:00:00Z", "hit", "2026-09-11T00:00:00Z", 0) // on the boundary is still this window
	step("2026-09-11T00:00:01Z", "miss", "2026-09-11T01:00:00Z", 1)
	step("2026-09-11T00:00:20Z", "hit", "2026-09-11T01:00:00Z", 0)
	// Same window, past the TTL: the key is the same and the answer is refilled.
	step("2026-09-11T00:00:31Z", "miss", "2026-09-11T01:00:00Z", 1)
}
