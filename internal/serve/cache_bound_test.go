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
