package serve

import (
	"context"
	"net/http"
	"net/http/httptest"
	"runtime"
	"runtime/debug"
	"testing"

	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/turbolytics/sql-flow/internal/turbostats"
	"github.com/zeebo/assert"
)

// TestCliServe_DoesNotLeakNativeMemory drives requests through the handler
// and asserts the process does not grow.
//
// The encoder copies values out of Arrow buffers DuckDB owns, which is the
// boundary the handler-table leaks in #243 and #247 crossed: a reader or
// record retained once more than it is released keeps every buffer it points
// at. Half a million rows go through here, as in the handler leak test,
// against the same 8 MiB threshold. Retaining each record batch in readRows
// fails it; growth without that is about 1 MiB.
func TestCliServe_DoesNotLeakNativeMemory(t *testing.T) {
	coverage.Covers(t, "cli.serve")
	ex, _ := newExec(t, 1, `CREATE TABLE posts AS
		SELECT TIMESTAMPTZ '2026-09-10 00:00:00+00' + INTERVAL (range) MINUTE AS bucket,
		       'lang_' || (range % 33) AS lang,
		       range::BIGINT AS posts
		FROM range(1000)`)

	conf, err := config.ParseServe([]byte(`
serve:
  auth:
    tokens: [{name: leak, token: leak-token}]
  datasets:
    - name: posts
      params: [{name: lang, type: string}]
      sql: SELECT bucket, lang, posts FROM posts WHERE lang = coalesce($lang, lang)
`))
	assert.NoError(t, err)
	srv, err := New(context.Background(), conf, ex)
	assert.NoError(t, err)
	t.Cleanup(srv.Close)
	handler := srv.Handler()

	run := func(n int) {
		for i := 0; i < n; i++ {
			req := httptest.NewRequest(http.MethodGet, "/v1/datasets/posts", nil)
			req.Header.Set("Authorization", "Bearer leak-token")
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, req)
			if w.Code != http.StatusOK {
				t.Fatalf("request %d: %d %s", i, w.Code, w.Body.String())
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

	// The warmup outlasts the allocator's own settling, which is what the
	// baseline has to be taken after. A pool reaches steady state later than
	// one connection did: measured over four consecutive runs of iters, the
	// process grew 10 MiB, then 3, then 1, then 0 -- a plateau, not a leak.
	// Fifty requests sampled the middle of that curve and read the climb as
	// growth.
	const warmup, iters, rowsPerRequest = 500, 500, 1000
	run(warmup)
	settle()
	before := resident()

	run(iters)
	settle()
	after := resident()

	const limit = 8 << 20
	growth := after - before
	t.Logf("resident anon memory: before %d MiB, after %d MiB, growth %d MiB over %d rows",
		before>>20, after>>20, growth>>20, iters*rowsPerRequest)
	if growth > limit {
		t.Fatalf("process grew %d MiB over %d rows; serve is retaining native buffers", growth>>20, iters*rowsPerRequest)
	}
}
