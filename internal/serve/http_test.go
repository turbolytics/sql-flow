package serve

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/turbolytics/sql-flow/internal/errs"
	"github.com/zeebo/assert"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

const testServe = `
serve:
  http:
    cors:
      allowed_origins: [https://turbolytics.io]
  clients:
    - {name: page, id: page-id}
    - {name: ops, id: ops-id}
  limits:
    max_rows: 3
  datasets:
    - name: status
      description: One row.
      sql: SELECT count(*) AS n FROM posts
    - name: every_post
      sql: SELECT lang, posts FROM posts ORDER BY bucket, lang
    - name: posts_by_lang
      description: Posts per bucket per language.
      params:
        - {name: since, type: timestamp}
        - {name: lang, type: string}
        - {name: min_posts, type: integer}
      limits:
        max_rows: 100
      grains:
        1h:
          sql: |
            SELECT bucket, lang, posts FROM posts
            WHERE bucket >= coalesce($since, TIMESTAMPTZ '2000-01-01 00:00:00+00')
              AND lang = coalesce($lang, lang)
              AND posts >= coalesce($min_posts, 0)
            ORDER BY bucket, lang
        1d:
          sql: |
            SELECT date_trunc('day', bucket) AS bucket, lang, sum(posts)::BIGINT AS posts FROM posts
            WHERE bucket >= coalesce($since, TIMESTAMPTZ '2000-01-01 00:00:00+00')
              AND lang = coalesce($lang, lang)
              AND posts >= coalesce($min_posts, 0)
            GROUP BY ALL
            ORDER BY bucket, lang
    - name: cast
      params:
        - {name: v, type: string}
      sql: SELECT CAST($v AS INTEGER) AS n
    - name: slow
      sql: ` + slowSQL + `
`

const createPostsTable = `CREATE TABLE posts AS SELECT * FROM (VALUES
	(TIMESTAMPTZ '2026-09-10 00:00:00+00', 'en', 10::BIGINT),
	(TIMESTAMPTZ '2026-09-10 00:00:00+00', 'ja', 4::BIGINT),
	(TIMESTAMPTZ '2026-09-10 01:00:00+00', 'en', 7::BIGINT),
	(TIMESTAMPTZ '2026-09-11 00:00:00+00', 'en', 1::BIGINT)
) AS t(bucket, lang, posts)`

type testServer struct {
	srv     *Server
	handler http.Handler
	logs    *observer.ObservedLogs
}

func newTestServer(t *testing.T, text string) *testServer {
	t.Helper()
	return newTestServerWith(t, text)
}

// newTestServerWith takes extra options, for a test that needs the metrics
// registry.
func newTestServerWith(t *testing.T, text string, extra ...Option) *testServer {
	t.Helper()
	// One session: every assertion below about ordering and timing is the
	// old single-connection behaviour.
	ex, _ := newExec(t, 1, "SET TimeZone='UTC'", createPostsTable)

	conf, err := config.ParseServe([]byte(text))
	assert.NoError(t, err)

	core, logs := observer.New(zap.InfoLevel)
	srv, err := New(context.Background(), conf, ex, append([]Option{WithLogger(zap.New(core))}, extra...)...)
	assert.NoError(t, err)
	// Registered after newExec's cleanups, so it runs first: a slow query
	// still holding a session finishes before the database closes.
	t.Cleanup(srv.Close)

	return &testServer{srv: srv, handler: srv.Handler(), logs: logs}
}

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

type response struct {
	status int
	header http.Header
	body   map[string]any
	raw    string
}

func (ts *testServer) do(t *testing.T, method, target string, header map[string]string) response {
	t.Helper()
	req := httptest.NewRequest(method, target, nil)
	for k, v := range header {
		req.Header.Set(k, v)
	}
	w := httptest.NewRecorder()
	ts.handler.ServeHTTP(w, req)

	resp := response{status: w.Code, header: w.Header(), raw: w.Body.String()}
	// Only the JSON routes decode. /metrics answers Prometheus text, and a
	// HEAD answers nothing.
	if w.Body.Len() > 0 && strings.HasPrefix(w.Header().Get("Content-Type"), "application/json") {
		assert.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp.body))
	}
	return resp
}

// as adds a client's id to a target, the way a caller sends it.
func as(id, target string) string {
	sep := "?"
	if strings.Contains(target, "?") {
		sep = "&"
	}
	return target + sep + "client_id=" + id
}

func (ts *testServer) get(t *testing.T, target string) response {
	t.Helper()
	return ts.do(t, http.MethodGet, as("page-id", target), nil)
}

func errorOf(t *testing.T, r response) (string, string) {
	t.Helper()
	e, ok := r.body["error"].(map[string]any)
	if !ok {
		t.Fatalf("no error object in %s", r.raw)
	}
	return e["code"].(string), e["message"].(string)
}

func TestCliServe_HealthzNeedsNoClientID(t *testing.T) {
	coverage.Covers(t, "cli.serve")
	ts := newTestServer(t, testServe)

	r := ts.do(t, http.MethodGet, "/healthz", nil)
	assert.Equal(t, http.StatusOK, r.status)
	assert.Equal(t, "ok", r.body["status"])
	assert.Equal(t, "application/json", r.header.Get("Content-Type"))
}

// No id, an empty id, an unknown id, a prefix of one and an id given twice are
// all 401. An unknown client_id is refused even beside a header that would
// pass: a request answers to one id.
func TestCliServe_RequiresAConfiguredClientID(t *testing.T) {
	coverage.Covers(t, "cli.serve")
	ts := newTestServer(t, testServe)

	for name, tt := range map[string]struct {
		target string
		header map[string]string
	}{
		"no id":                   {target: "/v1/datasets/status"},
		"empty id":                {target: "/v1/datasets/status?client_id="},
		"unknown id":              {target: "/v1/datasets/status?client_id=nope"},
		"prefix only":             {target: "/v1/datasets/status?client_id=page"},
		"given twice":             {target: "/v1/datasets/status?client_id=page-id&client_id=ops-id"},
		"unknown id, good header": {target: "/v1/datasets/status?client_id=nope", header: map[string]string{"Authorization": "Bearer page-id"}},
	} {
		t.Run(name, func(t *testing.T) {
			r := ts.do(t, http.MethodGet, tt.target, tt.header)
			assert.Equal(t, http.StatusUnauthorized, r.status)
			code, message := errorOf(t, r)
			assert.Equal(t, "unauthorized", code)
			assert.True(t, strings.Contains(message, "?client_id=<id>"))
		})
	}

	r := ts.do(t, http.MethodGet, "/v1/datasets/status?client_id=ops-id", nil)
	assert.Equal(t, http.StatusOK, r.status)

	// client_id is the caller's, not the dataset's: a dataset with params
	// does not refuse it as one it never declared.
	r = ts.do(t, http.MethodGet, "/v1/datasets/posts_by_lang?grain=1h&lang=en&client_id=ops-id", nil)
	assert.Equal(t, http.StatusOK, r.status)
}

// The deprecated form answers for one release, so a page and its API need not
// deploy in the same instant. The scheme is case-insensitive, as RFC 7235
// says. The warning is one line however many requests send the header.
func TestCliServe_StillAcceptsTheDeprecatedBearerHeader(t *testing.T) {
	coverage.Covers(t, "cli.serve")
	ts := newTestServer(t, testServe)

	for name, header := range map[string]map[string]string{
		"basic scheme": {"Authorization": "Basic page-id"},
		"empty id":     {"Authorization": "Bearer "},
		"unknown id":   {"Authorization": "Bearer nope"},
	} {
		t.Run(name, func(t *testing.T) {
			r := ts.do(t, http.MethodGet, "/v1/datasets/status", header)
			assert.Equal(t, http.StatusUnauthorized, r.status)
			assert.Equal(t, "Bearer", r.header.Get("WWW-Authenticate"))
		})
	}
	assert.Equal(t, 0, ts.logs.FilterLevelExact(zap.WarnLevel).Len())

	for range 3 {
		r := ts.do(t, http.MethodGet, "/v1/datasets/status", map[string]string{"Authorization": "bearer ops-id"})
		assert.Equal(t, http.StatusOK, r.status)
	}
	warnings := ts.logs.FilterLevelExact(zap.WarnLevel).All()
	assert.Equal(t, 1, len(warnings))
	assert.Equal(t, "ops", warnings[0].ContextMap()["client"])
}

// Every field of a data response, against rows the test inserted.
func TestCliServe_DatasetResponseCarriesEveryField(t *testing.T) {
	coverage.Covers(t, "cli.serve")
	ts := newTestServer(t, testServe)

	r := ts.get(t, "/v1/datasets/posts_by_lang?grain=1d&lang=en")
	assert.Equal(t, http.StatusOK, r.status)
	assert.Equal(t, "application/json", r.header.Get("Content-Type"))
	assert.Equal(t, "no-store", r.header.Get("Cache-Control"))

	assert.Equal(t, "posts_by_lang", r.body["dataset"])
	assert.Equal(t, "1d", r.body["grain"])
	assert.DeepEqual(t, []any{
		map[string]any{"name": "bucket", "type": "TIMESTAMP WITH TIME ZONE"},
		map[string]any{"name": "lang", "type": "VARCHAR"},
		map[string]any{"name": "posts", "type": "BIGINT"},
	}, r.body["columns"])
	assert.DeepEqual(t, []any{
		map[string]any{"bucket": "2026-09-10T00:00:00Z", "lang": "en", "posts": float64(17)},
		map[string]any{"bucket": "2026-09-11T00:00:00Z", "lang": "en", "posts": float64(1)},
	}, r.body["rows"])
	assert.Equal(t, float64(2), r.body["row_count"])
	assert.Equal(t, false, r.body["truncated"])
	_, hasElapsed := r.body["elapsed_ms"]
	assert.That(t, hasElapsed)

	plain := ts.get(t, "/v1/datasets/status")
	assert.Equal(t, http.StatusOK, plain.status)
	_, hasGrain := plain.body["grain"]
	assert.False(t, hasGrain)
	assert.DeepEqual(t, []any{map[string]any{"n": float64(4)}}, plain.body["rows"])
}

// Each param type filters, and an absent param leaves its coalesce default.
func TestCliServe_ParamsBindByDeclaredType(t *testing.T) {
	coverage.Covers(t, "cli.serve")
	ts := newTestServer(t, testServe)

	count := func(target string) float64 {
		t.Helper()
		r := ts.get(t, target)
		assert.Equal(t, http.StatusOK, r.status)
		return r.body["row_count"].(float64)
	}

	assert.Equal(t, float64(4), count("/v1/datasets/posts_by_lang?grain=1h"))
	assert.Equal(t, float64(3), count("/v1/datasets/posts_by_lang?grain=1h&lang=en"))
	assert.Equal(t, float64(2), count("/v1/datasets/posts_by_lang?grain=1h&min_posts=5"))
	assert.Equal(t, float64(1), count("/v1/datasets/posts_by_lang?grain=1h&since=2026-09-10T20:00:00-04:00"))
	assert.Equal(t, float64(1), count("/v1/datasets/posts_by_lang?grain=1h&since=2026-09-11T00:00:00%2B00:00"))
}

const boundedServe = `
serve:
  clients:
    - {name: page, id: page-id}
  datasets:
    - name: bounded
      params:
        - {name: top, type: integer, min: 1, max: 20}
        - {name: floor, type: integer, min: 0}
        - {name: ceiling, type: integer, max: 100}
      sql: |
        SELECT count(*) AS n FROM posts
        WHERE coalesce($top, 1) >= 1 AND coalesce($floor, 0) >= 0 AND coalesce($ceiling, 0) <= 100
`

// An integer outside its bounds is refused, not clamped, and the message
// names the bounds. The listing publishes them so a page can respect them.
func TestCliServe_AnIntegerOutsideItsBoundsIsRefused(t *testing.T) {
	coverage.Covers(t, "cli.serve")
	ts := newTestServer(t, boundedServe)

	for _, tt := range []struct {
		query   string
		status  int
		message string
	}{
		{"top=1", 200, ""},
		{"top=20", 200, ""},
		{"top=0", 400, "param top must be between 1 and 20; got 0"},
		{"top=21", 400, "param top must be between 1 and 20; got 21"},
		{"floor=-1", 400, "param floor must be at least 0; got -1"},
		{"ceiling=101", 400, "param ceiling must be at most 100; got 101"},
		{"floor=0&ceiling=100", 200, ""},
	} {
		t.Run(tt.query, func(t *testing.T) {
			r := ts.get(t, "/v1/datasets/bounded?"+tt.query)
			assert.Equal(t, tt.status, r.status)
			if tt.status == http.StatusOK {
				return
			}
			code, message := errorOf(t, r)
			assert.Equal(t, "invalid_param", code)
			assert.Equal(t, tt.message, message)
		})
	}

	r := ts.get(t, "/v1/datasets")
	assert.Equal(t, http.StatusOK, r.status)
	params := r.body["datasets"].([]any)[0].(map[string]any)["params"].([]any)
	top := params[0].(map[string]any)
	assert.Equal(t, float64(1), top["min"])
	assert.Equal(t, float64(20), top["max"])
	_, floorHasMax := params[1].(map[string]any)["max"]
	assert.False(t, floorHasMax)
}

// Every error code the contract lists, with a message that names the thing.
func TestCliServe_ErrorsCarryTheirCodeAndNameTheCause(t *testing.T) {
	coverage.Covers(t, "cli.serve")
	ts := newTestServer(t, testServe)

	for _, tt := range []struct {
		name, method, target string
		status               int
		code, message        string
	}{
		{"unknown dataset", "GET", "/v1/datasets/nope", 404, "unknown_dataset", "nope"},
		{"not a route", "GET", "/v2/anything", 404, "not_found", "/v2/anything"},
		{"a nested path", "GET", "/v1/datasets/status/extra", 404, "not_found", "/v1/datasets/status/extra"},
		{"not GET", "POST", "/v1/datasets/status", 405, "method_not_allowed", "POST"},
		{"unknown param", "GET", "/v1/datasets/posts_by_lang?grain=1h&language=en", 400, "unknown_param", "language; params: since, lang, min_posts"},
		{"param on a dataset with none", "GET", "/v1/datasets/status?lang=en", 400, "unknown_param", "lang"},
		{"bad integer", "GET", "/v1/datasets/posts_by_lang?grain=1h&min_posts=five", 400, "invalid_param", "min_posts is a base-10 integer"},
		{"timestamp without offset", "GET", "/v1/datasets/posts_by_lang?grain=1h&since=2026-09-10T00:00:00", 400, "invalid_param", "since is an RFC 3339 timestamp"},
		{"repeated param", "GET", "/v1/datasets/posts_by_lang?grain=1h&lang=en&lang=ja", 400, "invalid_param", "lang is given 2 times"},
		{"missing grain", "GET", "/v1/datasets/posts_by_lang", 400, "missing_grain", "grains: 1d, 1h"},
		{"unknown grain", "GET", "/v1/datasets/posts_by_lang?grain=15m", 400, "unknown_grain", "no grain 15m; grains: 1d, 1h"},
		{"grain on a dataset without grains", "GET", "/v1/datasets/status?grain=1h", 400, "unknown_grain", "status has no grains"},
		{"query failed", "GET", "/v1/datasets/cast?v=abc", 500, "query_failed", "dataset cast failed"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			r := ts.do(t, tt.method, as("page-id", tt.target), nil)
			assert.Equal(t, tt.status, r.status)
			code, message := errorOf(t, r)
			assert.Equal(t, tt.code, code)
			if !strings.Contains(message, tt.message) {
				t.Fatalf("message %q does not contain %q", message, tt.message)
			}
		})
	}
}

// max_rows cuts the result and says so with a 200. A dataset's own limit
// beats the top level's, and the next request still answers.
func TestCliServe_MaxRowsTruncatesAndTheNextRequestAnswers(t *testing.T) {
	coverage.Covers(t, "cli.serve")
	ts := newTestServer(t, testServe)

	cut := ts.get(t, "/v1/datasets/every_post")
	assert.Equal(t, http.StatusOK, cut.status)
	assert.Equal(t, float64(3), cut.body["row_count"])
	assert.Equal(t, true, cut.body["truncated"])

	override := ts.get(t, "/v1/datasets/posts_by_lang?grain=1h")
	assert.Equal(t, float64(4), override.body["row_count"])
	assert.Equal(t, false, override.body["truncated"])

	again := ts.get(t, "/v1/datasets/every_post")
	assert.Equal(t, http.StatusOK, again.status)
	assert.Equal(t, float64(3), again.body["row_count"])
}

// A query past its deadline is a 504, and the health check, waiting for the
// pool's only session, goes red until the query finishes.
func TestCliServe_ASlowQueryIsA504AndTurnsHealthRed(t *testing.T) {
	coverage.Covers(t, "cli.serve")
	ts := newTestServer(t, testServe)
	ts.srv.datasets["slow"].timeout = 100 * time.Millisecond
	ts.srv.healthTimeout = 100 * time.Millisecond

	r := ts.get(t, "/v1/datasets/slow")
	assert.Equal(t, http.StatusGatewayTimeout, r.status)
	code, message := errorOf(t, r)
	assert.Equal(t, "query_timeout", code)
	assert.That(t, strings.Contains(message, "dataset slow"))

	health := ts.do(t, http.MethodGet, "/healthz", nil)
	assert.Equal(t, http.StatusServiceUnavailable, health.status)
}

func TestCliServe_CORSAnswersAllowedOriginsOnly(t *testing.T) {
	coverage.Covers(t, "cli.serve")
	ts := newTestServer(t, testServe)

	preflight := ts.do(t, http.MethodOptions, "/v1/datasets/status", map[string]string{
		"Origin":                        "https://turbolytics.io",
		"Access-Control-Request-Method": "GET",
	})
	assert.Equal(t, http.StatusNoContent, preflight.status)
	assert.Equal(t, "https://turbolytics.io", preflight.header.Get("Access-Control-Allow-Origin"))
	assert.Equal(t, "GET", preflight.header.Get("Access-Control-Allow-Methods"))
	assert.Equal(t, "Authorization", preflight.header.Get("Access-Control-Allow-Headers"))
	assert.Equal(t, "600", preflight.header.Get("Access-Control-Max-Age"))
	assert.Equal(t, "Origin", preflight.header.Get("Vary"))

	stranger := ts.do(t, http.MethodOptions, "/v1/datasets/status", map[string]string{
		"Origin": "https://evil.example",
	})
	assert.Equal(t, http.StatusNoContent, stranger.status)
	assert.Equal(t, "", stranger.header.Get("Access-Control-Allow-Origin"))
	assert.Equal(t, "", stranger.header.Get("Access-Control-Allow-Methods"))

	get := ts.do(t, http.MethodGet, as("page-id", "/v1/datasets/status"), map[string]string{
		"Origin": "https://turbolytics.io",
	})
	assert.Equal(t, http.StatusOK, get.status)
	assert.Equal(t, "https://turbolytics.io", get.header.Get("Access-Control-Allow-Origin"))
	assert.Equal(t, "", get.header.Get("Access-Control-Allow-Credentials"))

	noCORS := newTestServer(t, strings.Replace(testServe,
		"  http:\n    cors:\n      allowed_origins: [https://turbolytics.io]\n", "", 1))
	options := noCORS.do(t, http.MethodOptions, "/v1/datasets/status", map[string]string{
		"Origin": "https://turbolytics.io",
	})
	assert.Equal(t, http.StatusMethodNotAllowed, options.status)
	assert.Equal(t, "", options.header.Get("Access-Control-Allow-Origin"))
	assert.Equal(t, "", options.header.Get("Vary"))
}

// The listing shows the SQL as the config wrote it. $since is what the author
// wrote; $1 is the server's business.
func TestCliServe_ListingReturnsTheSQLAsWritten(t *testing.T) {
	coverage.Covers(t, "cli.serve")
	ts := newTestServer(t, testServe)

	r := ts.get(t, "/v1/datasets")
	assert.Equal(t, http.StatusOK, r.status)
	assert.That(t, strings.Contains(r.raw, "$since"))
	assert.False(t, strings.Contains(r.raw, "$1"))

	datasets := r.body["datasets"].([]any)
	assert.Equal(t, 5, len(datasets))

	status := datasets[0].(map[string]any)
	assert.Equal(t, "status", status["name"])
	assert.Equal(t, "One row.", status["description"])
	assert.DeepEqual(t, []any{}, status["params"])
	assert.Equal(t, "SELECT count(*) AS n FROM posts", status["sql"])

	posts := datasets[2].(map[string]any)
	grains := posts["grains"].(map[string]any)
	assert.Equal(t, 2, len(grains))
	_, hasSQL := posts["sql"]
	assert.False(t, hasSQL)
	assert.DeepEqual(t, map[string]any{"name": "since", "type": "timestamp"}, posts["params"].([]any)[0])

	unauthorized := ts.do(t, http.MethodGet, "/v1/datasets", nil)
	assert.Equal(t, http.StatusUnauthorized, unauthorized.status)
}

// A config not yet moved to clients keeps answering its callers, by either
// form, and says at startup that it must move.
func TestCliServe_DeprecatedAuthTokensAreClients(t *testing.T) {
	coverage.Covers(t, "cli.serve")
	ts := newTestServer(t, strings.Replace(testServe,
		"  clients:\n    - {name: page, id: page-id}\n    - {name: ops, id: ops-id}\n",
		"  clients:\n    - {name: page, id: page-id}\n  auth:\n    tokens:\n      - {name: ops, token: ops-id}\n", 1))

	startup := ts.logs.FilterLevelExact(zap.WarnLevel).All()
	assert.Equal(t, 1, len(startup))
	assert.True(t, strings.Contains(startup[0].Message, "serve.auth.tokens is deprecated"))

	for _, target := range []string{"/v1/datasets/status?client_id=page-id", "/v1/datasets/status?client_id=ops-id"} {
		assert.Equal(t, http.StatusOK, ts.do(t, http.MethodGet, target, nil).status)
	}
	r := ts.do(t, http.MethodGet, "/v1/datasets/status", map[string]string{"Authorization": "Bearer ops-id"})
	assert.Equal(t, http.StatusOK, r.status)
}

// One line per request, naming the client and never its id.
func TestCliServe_LogsOneLinePerRequest(t *testing.T) {
	coverage.Covers(t, "cli.serve")
	ts := newTestServer(t, testServe)

	ts.get(t, "/v1/datasets/posts_by_lang?grain=1h&lang=en")
	ts.do(t, http.MethodGet, as("ops-id", "/v1/datasets/posts_by_lang?grain=15m"), nil)

	lines := ts.logs.FilterMessage("request").All()
	assert.Equal(t, 2, len(lines))

	ok := lines[0].ContextMap()
	assert.Equal(t, "page", ok["client"])
	assert.Equal(t, "posts_by_lang", ok["dataset"])
	assert.Equal(t, "1h", ok["grain"])
	assert.Equal(t, int64(200), ok["status"])
	assert.Equal(t, int64(3), ok["rows"])
	_, hasCode := ok["code"]
	assert.False(t, hasCode)

	bad := lines[1].ContextMap()
	assert.Equal(t, "ops", bad["client"])
	assert.Equal(t, int64(400), bad["status"])
	assert.Equal(t, "unknown_grain", bad["code"])

	for _, entry := range ts.logs.All() {
		for _, v := range entry.ContextMap() {
			if s, isString := v.(string); isString && strings.Contains(s, "-id") {
				t.Fatalf("a log line carries a client id: %v", entry.ContextMap())
			}
		}
	}
}

// Shutdown lets an in-flight request finish, then Serve returns.
func TestCliServe_ServeDrainsAnInFlightRequest(t *testing.T) {
	coverage.Covers(t, "cli.serve")
	ts := newTestServer(t, testServe)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	assert.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	served := make(chan error, 1)
	go func() { served <- ts.srv.Serve(ctx, ln) }()

	status := make(chan int, 1)
	go func() {
		req, _ := http.NewRequest(http.MethodGet, "http://"+ln.Addr().String()+"/v1/datasets/slow?client_id=page-id", nil)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			status <- 0
			return
		}
		resp.Body.Close()
		status <- resp.StatusCode
	}()

	time.Sleep(200 * time.Millisecond)
	cancel()

	assert.Equal(t, http.StatusOK, <-status)
	select {
	case err := <-served:
		assert.NoError(t, err)
	case <-time.After(shutdownTimeout + time.Second):
		t.Fatal("Serve did not return after its context ended")
	}
}

// A config that breaks a rule, or a statement that does not prepare, stops
// New. Neither is found on the first request.
func TestCliServe_NewRefusesWhatCannotAnswer(t *testing.T) {
	coverage.Covers(t, "cli.serve")
	ex, _ := newExec(t, 1)

	emptyID, err := config.ParseServe([]byte(strings.Replace(testServe, "id: page-id", `id: ""`, 1)))
	assert.NoError(t, err)
	_, err = New(context.Background(), emptyID, ex)
	assert.Equal(t, errs.CodeConfigInvalid, errs.CodeOf(err))

	noTable, err := config.ParseServe([]byte(testServe))
	assert.NoError(t, err)
	_, err = New(context.Background(), noTable, ex)
	assert.Equal(t, errs.CodeSQLInvalid, errs.CodeOf(err))
	assert.That(t, strings.Contains(err.Error(), "dataset status"))
}

// A client id is public, so a 500 carries nothing from the database. The probe
// that found this stopped a Postgres mid-run and got back
// "Unable to connect to Postgres at \"postgresql://postgres:postgres@...\"".
// A cast error echoes its input, which stands in for that message here
// without a Postgres.
func TestCliServe_AQueryFailureKeepsTheDatabaseErrorOutOfTheResponse(t *testing.T) {
	coverage.Covers(t, "cli.serve")
	ts := newTestServer(t, testServe)

	uri := "postgresql://demo_user:s3cret-pw@db.internal:5432/bluesky"
	r := ts.get(t, "/v1/datasets/cast?v="+url.QueryEscape(uri))
	assert.Equal(t, http.StatusInternalServerError, r.status)
	assert.False(t, strings.Contains(r.raw, "s3cret-pw"))
	assert.False(t, strings.Contains(r.raw, "demo_user"))

	failures := ts.logs.FilterMessage("query failed").All()
	assert.Equal(t, 1, len(failures))
	logged := failures[0].ContextMap()["error"].(string)
	assert.That(t, strings.Contains(logged, "postgresql://demo_user:***@db.internal:5432/bluesky"))
	assert.False(t, strings.Contains(logged, "s3cret-pw"))
}

func TestCliServe_RedactRemovesPasswords(t *testing.T) {
	coverage.Covers(t, "cli.serve")

	for in, want := range map[string]string{
		`Unable to connect to Postgres at "postgresql://u:p@h:5432/db": refused`: `Unable to connect to Postgres at "postgresql://u:***@h:5432/db": refused`,
		`postgres://u:p%40ss@h/db?sslmode=require`:                               `postgres://u:***@h/db?sslmode=require`,
		`host=h user=u password=hunter2 dbname=db`:                               `host=h user=u password=*** dbname=db`,
		`host=h password='two words' dbname=db`:                                  `host=h password=*** dbname=db`,
		`postgresql://u@h/db has no password`:                                    `postgresql://u@h/db has no password`,
		`Conversion Error: Could not convert string 'abc' to INT32`:              `Conversion Error: Could not convert string 'abc' to INT32`,
	} {
		assert.Equal(t, want, Redact(in))
	}
}

// A password is not the only credential a config attaches with. MotherDuck
// takes a token in the URL or as an ATTACH option, and DuckDB's own secrets
// carry keys the same way, so an ATTACH that fails prints them into the log
// exactly as a Postgres one prints its password.
func TestCliServe_RedactRemovesTokensAndKeys(t *testing.T) {
	coverage.Covers(t, "cli.serve")

	for in, want := range map[string]string{
		// MotherDuck, both forms.
		`ATTACH 'md:db?motherduck_token=ey.J9.sig' AS md`: `ATTACH 'md:db?motherduck_token=***' AS md`,
		`ATTACH 'md:db' AS md (TOKEN 'ey.J9.sig')`:        `ATTACH 'md:db' AS md (TOKEN '***')`,
		`failed: motherduck_token = 'ey.J9.sig'`:          `failed: motherduck_token = ***`,

		// DuckDB secrets: the key id identifies, the secret authenticates.
		`CREATE SECRET (TYPE S3, KEY_ID 'AKIA', SECRET 'wJalr')`: `CREATE SECRET (TYPE S3, KEY_ID 'AKIA', SECRET '***')`,
		`s3_secret_access_key=wJalr&s3_region=us-east-1`:         `s3_secret_access_key=***&s3_region=us-east-1`,
		`api_key: abc123`: `api_key: ***`,

		// Untouched: no credential, and an error a reader needs whole.
		`ATTACH 'md:db' AS md`:                                 `ATTACH 'md:db' AS md`,
		`Catalog Error: no database named 'sample_data' found`: `Catalog Error: no database named 'sample_data' found`,
		`ATTACH '...' AS pg (TYPE POSTGRES, READ_ONLY)`:        `ATTACH '...' AS pg (TYPE POSTGRES, READ_ONLY)`,
		`Binder Error: column "token" does not exist`:          `Binder Error: column "token" does not exist`,
	} {
		assert.Equal(t, want, Redact(in))
	}
}

// elapsed_ms used to include the wait for a connection, so a 13 ms query on a
// loaded server reported a second and sent its reader looking for a slow query
// that did not exist. The two are separate fields now.
func TestCliServe_QueuedMsSeparatesWaitFromWork(t *testing.T) {
	coverage.Covers(t, "cli.serve")
	ts := newTestServer(t, testServe)

	idle := ts.get(t, "/v1/datasets/status")
	assert.Equal(t, http.StatusOK, idle.status)
	assert.Equal(t, float64(0), idle.body["queued_ms"])

	// Hold the only session, then time a request that has to wait for it.
	held, err := ts.srv.exec.Acquire(context.Background())
	assert.NoError(t, err)

	done := make(chan response, 1)
	go func() { done <- ts.get(t, "/v1/datasets/status") }()
	time.Sleep(300 * time.Millisecond)
	held.Release()

	queued := <-done
	assert.Equal(t, http.StatusOK, queued.status)
	assert.That(t, queued.body["queued_ms"].(float64) >= 250)
	// The query itself is unchanged by the wait, which is the whole point.
	assert.That(t, queued.body["elapsed_ms"].(float64) < 250)
}

// A full pool is not a dead server. A supervisor that cannot tell them apart
// restarts one that is merely loaded.
//
// The config gives health one second rather than the default ten, because the
// handler waits its whole timeout for a session before it can say busy.
const busyServe = `
serve:
  clients: [{name: page, id: page-id}]
  limits:
    timeout_seconds: 1
  datasets:
    - name: status
      sql: SELECT count(*) AS n FROM posts
`

func TestCliServe_HealthzIsBusyNotDownWhenThePoolIsFull(t *testing.T) {
	coverage.Covers(t, "cli.serve")
	ts := newTestServer(t, busyServe)

	held, err := ts.srv.exec.Acquire(context.Background())
	assert.NoError(t, err)
	defer held.Release()

	r := ts.do(t, http.MethodGet, "/healthz", nil)
	assert.Equal(t, http.StatusServiceUnavailable, r.status)
	assert.Equal(t, "busy", r.body["status"])
}

// Monitors send HEAD, and the status line is the whole answer. A dataset
// still refuses it: running the query and discarding the rows would spend a
// session on nothing.
//
// The status is all this asserts. httptest.NewRecorder hands the handler's
// body straight back, where a real http.Server suppresses it for HEAD, so an
// empty-body assertion here would be testing the recorder.
func TestCliServe_HealthzAnswersHead(t *testing.T) {
	coverage.Covers(t, "cli.serve")
	ts := newTestServer(t, busyServe)

	assert.Equal(t, http.StatusOK, ts.do(t, http.MethodHead, "/healthz", nil).status)

	held, err := ts.srv.exec.Acquire(context.Background())
	assert.NoError(t, err)
	busy := ts.do(t, http.MethodHead, "/healthz", nil)
	held.Release()
	assert.Equal(t, http.StatusServiceUnavailable, busy.status)

	ds := ts.do(t, http.MethodHead, as("page-id", "/v1/datasets/status"), nil)
	assert.Equal(t, http.StatusMethodNotAllowed, ds.status)
	assert.Equal(t, http.MethodGet, ds.header.Get("Allow"))
}
