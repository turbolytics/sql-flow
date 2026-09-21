package serve

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"runtime"
	"runtime/debug"
	"testing"
	"time"

	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/turbolytics/sql-flow/internal/turbostats"
	"github.com/turbolytics/sql-flow/turbostats/wire"
	"github.com/zeebo/assert"
)

var testStatic = turbostats.Static{
	Name:       "test-serve",
	Version:    "v0.0.0",
	Commit:     "abc1234",
	ConfigHash: "sha256:00",
	StartedAt:  time.Date(2026, 9, 1, 8, 0, 0, 0, time.UTC),
}

// A process does not answer on a route unless told to.
func TestServeTurbostats_IsOffUnlessAsked(t *testing.T) {
	coverage.Covers(t, "observability.turbostats.serve")
	ts := newTestServerWith(t, testServe)
	assert.Equal(t, http.StatusNotFound, ts.do(t, http.MethodGet, "/turbostats/v1", nil).status)
}

// End to end through the real server: a request is answered, the flat series
// moves, and the bundle the route serves says so. This is the test that
// catches the two packages disagreeing about an instrument name.
func TestServeTurbostats_ServesAServeSection(t *testing.T) {
	coverage.Covers(t, "observability.turbostats.serve")
	ts := newTestServerWith(t, testServe, WithTurbostats(testStatic, true))

	assert.Equal(t, http.StatusOK, ts.get(t, "/v1/datasets/status").status)

	r := ts.do(t, http.MethodGet, "/turbostats/v1", nil)
	assert.Equal(t, http.StatusOK, r.status)

	var b wire.Bundle
	assert.NoError(t, json.Unmarshal([]byte(r.raw), &b))
	assert.Equal(t, 1, b.V)
	assert.Equal(t, "test-serve", b.Instance.Name)
	assert.That(t, b.Pipeline == nil)
	assert.That(t, b.Serve != nil)
	assert.Equal(t, int64(1), b.Serve.RequestCount)
	assert.Equal(t, int64(0), b.Serve.RequestErrorCount)
	// newTestServerWith builds a pool of one.
	assert.Equal(t, 1, b.Serve.SessionsTotal)
	assert.That(t, b.Serve.LastRequestAt != nil)
	assert.That(t, b.LastActivityAt != nil)
	// testServe opts no dataset into the cache.
	assert.That(t, b.Serve.Cache == nil)
}

// TestServeTurbostats_DoesNotLeakNativeMemory drives dataset requests and
// bundle collections through the handler and asserts the process does not
// grow.
//
// Two things here are new and live for the life of the process. The
// instruments now record on every request whether or not /metrics is on, and
// the manual reader holds their aggregation. Each bundle calls the reader's
// Collect, which allocates a ResourceMetrics, and reads the pool and the
// cache. A path that retained per request or per bundle would grow with
// either count, so both are driven: ten bundles per request, the ratio a
// one-second poll makes against a lightly used server.
//
// The memory soak cannot stand in for this. Its harness starts `sqlflow run`
// and reads duckdb_memory() through the debug endpoint, and `serve` has
// neither. The threshold and the warmup are the dataset leak test's, for the
// reasons given there.
func TestServeTurbostats_DoesNotLeakNativeMemory(t *testing.T) {
	coverage.Covers(t, "observability.turbostats.serve")
	ex, _ := newExec(t, 1, `CREATE TABLE posts AS
		SELECT TIMESTAMPTZ '2026-09-10 00:00:00+00' + INTERVAL (range) MINUTE AS bucket,
		       'lang_' || (range % 33) AS lang,
		       range::BIGINT AS posts
		FROM range(1000)`)

	conf, err := config.ParseServe([]byte(`
serve:
  clients: [{name: leak, id: leak-id}]
  datasets:
    - name: posts
      params: [{name: lang, type: string}]
      sql: SELECT bucket, lang, posts FROM posts WHERE lang = coalesce($lang, lang)
`))
	assert.NoError(t, err)
	srv, err := New(context.Background(), conf, ex, WithTurbostats(testStatic, true))
	assert.NoError(t, err)
	t.Cleanup(srv.Close)
	handler := srv.Handler()

	get := func(path string) {
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
		if w.Code != http.StatusOK {
			t.Fatalf("%s: %d %s", path, w.Code, w.Body.String())
		}
	}
	const bundlesPerRequest = 10
	run := func(n int) {
		for i := 0; i < n; i++ {
			get("/v1/datasets/posts?client_id=leak-id")
			for j := 0; j < bundlesPerRequest; j++ {
				get("/turbostats/v1")
			}
		}
	}

	settle := func() {
		runtime.GC()
		debug.FreeOSMemory()
	}
	resident := func() int64 {
		n, err := turbostats.ResidentAnonBytes()
		assert.NoError(t, err)
		return n
	}

	const warmup, iters = 500, 500
	run(warmup)
	settle()
	before := resident()

	run(iters)
	settle()
	after := resident()

	const limit = 8 << 20
	growth := after - before
	t.Logf("resident anon memory: before %d MiB, after %d MiB, growth %d MiB over %d requests and %d bundles",
		before>>20, after>>20, growth>>20, iters, iters*bundlesPerRequest)
	if growth > limit {
		t.Fatalf("process grew %d MiB over %d requests and %d bundles; serve is retaining per request or per bundle",
			growth>>20, iters, iters*bundlesPerRequest)
	}

	// The run above is only evidence if the bundles counted it.
	b, err := srv.CollectBundle(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, int64(warmup+iters), b.Serve.RequestCount)
}
