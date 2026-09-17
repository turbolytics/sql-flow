package serve

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/turbolytics/sql-flow/internal/errs"
	"github.com/zeebo/assert"
	"go.uber.org/zap"
)

const cliServe = `
commands:
  - name: create
    sql: CREATE TABLE t AS SELECT range AS n FROM range(3);
serve:
  http:
    addr: 127.0.0.1:0
  clients:
    - {name: test, id: test-id}
  datasets:
    - name: numbers
      sql: SELECT n FROM t ORDER BY n
`

func writeConfig(t *testing.T, text string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "serve.yml")
	assert.NoError(t, os.WriteFile(path, []byte(text), 0o644))
	return path
}

// The command end to end: load, attach, prepare, listen, answer, and return
// nil when its context ends.
func TestCliServe_ServesAConfigUntilItsContextEnds(t *testing.T) {
	coverage.Covers(t, "cli.serve")
	path := writeConfig(t, cliServe)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	addrs := make(chan net.Addr, 1)
	done := make(chan error, 1)
	go func() {
		done <- serveConfig(ctx, path, zap.NewNop(), func(a net.Addr) { addrs <- a })
	}()

	var base string
	select {
	case a := <-addrs:
		base = "http://" + a.String()
	case err := <-done:
		t.Fatalf("serve returned before listening: %v", err)
	case <-time.After(30 * time.Second):
		t.Fatal("serve did not listen")
	}

	resp, err := http.Get(base + "/healthz")
	assert.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	req, _ := http.NewRequest(http.MethodGet, base+"/v1/datasets/numbers?client_id=test-id", nil)
	resp, err = http.DefaultClient.Do(req)
	assert.NoError(t, err)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var got struct {
		RowCount int `json:"row_count"`
	}
	assert.NoError(t, json.Unmarshal(body, &got))
	assert.Equal(t, 3, got.RowCount)

	cancel()
	select {
	case err := <-done:
		assert.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("serve did not return after its context ended")
	}
}

// rate_limit is parsed and refused. Accepted and ignored, it would let an
// author ship a config believing it enforced. Exit 10 tells a supervisor not
// to restart into the same refusal.
func TestCliServe_RefusesARateLimitWithExit10(t *testing.T) {
	coverage.Covers(t, "cli.serve")
	path := writeConfig(t, cliServe+"  limits:\n    rate_limit:\n      requests_per_second: 5\n")

	err := serveConfig(context.Background(), path, zap.NewNop(), func(net.Addr) {
		t.Fatal("serve listened on a config it must refuse")
	})
	assert.Error(t, err)
	assert.Equal(t, errs.CodeConfigServeReserved, errs.CodeOf(err))
	assert.Equal(t, errs.ExitUserError, errs.ExitCode(err))
	assert.That(t, strings.Contains(err.Error(), "rate_limit"))
}

// validate warns on a client id rendered from an unset variable, so CI passes.
// serve must not start with it: every request would be refused.
func TestCliServe_RefusesAnEmptyClientIDAtStart(t *testing.T) {
	coverage.Covers(t, "cli.serve")
	path := writeConfig(t, strings.Replace(cliServe,
		"id: test-id", `id: "{{ SQLFLOW_SERVE_CLIENT_ID_UNSET_IN_THIS_TEST }}"`, 1))

	err := serveConfig(context.Background(), path, zap.NewNop(), nil)
	assert.Equal(t, errs.CodeConfigInvalid, errs.CodeOf(err))
	assert.Equal(t, errs.ExitUserError, errs.ExitCode(err))
}

func TestCliServe_ResolvesTheConfigFromEitherForm(t *testing.T) {
	coverage.Covers(t, "cli.serve", "cli.invocation")

	path, err := resolveConfigPath("a.yml", nil)
	assert.NoError(t, err)
	assert.Equal(t, "a.yml", path)

	path, err = resolveConfigPath("", []string{"b.yml"})
	assert.NoError(t, err)
	assert.Equal(t, "b.yml", path)

	_, err = resolveConfigPath("", nil)
	assert.Error(t, err)

	_, err = resolveConfigPath("a.yml", []string{"b.yml"})
	assert.Error(t, err)
}
