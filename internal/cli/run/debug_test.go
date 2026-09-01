package run

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/turbolytics/turbine/internal/duckdb"
	"github.com/zeebo/assert"
)

func newTestConn(t *testing.T) adbc.Connection {
	t.Helper()

	conn, err := duckdb.Open(context.Background())
	assert.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	return conn
}

func execSQL(t *testing.T, conn adbc.Connection, sql string) {
	t.Helper()

	stmt, err := conn.NewStatement()
	assert.NoError(t, err)
	defer stmt.Close()

	assert.NoError(t, stmt.SetSqlQuery(sql))
	_, err = stmt.ExecuteUpdate(context.Background())
	assert.NoError(t, err)
}

func debugGet(t *testing.T, h http.Handler, sql string) *httptest.ResponseRecorder {
	t.Helper()

	target := "/debug"
	if sql != "" {
		target += "?sql=" + url.QueryEscape(sql)
	}

	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, target, nil))
	return w
}

// The Python endpoint jsonifies duckdb's fetchall(), a list of row tuples, so
// rows arrive as arrays rather than objects.
func TestDebugHandler_ReturnsRowsAsJSON(t *testing.T) {
	conn := newTestConn(t)
	execSQL(t, conn, "CREATE TABLE t (id BIGINT, name VARCHAR)")
	execSQL(t, conn, "INSERT INTO t VALUES (1, 'a'), (2, 'b')")

	h := newDebugHandler(conn, &sync.Mutex{})
	w := debugGet(t, h, "SELECT id, name FROM t ORDER BY id")

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "application/json", w.Header().Get("Content-Type"))
	assert.Equal(t, `[[1,"a"],[2,"b"]]`, strings.TrimSpace(w.Body.String()))
}

func TestDebugHandler_ReturnsEmptyArrayForNoRows(t *testing.T) {
	conn := newTestConn(t)
	execSQL(t, conn, "CREATE TABLE empty (id BIGINT)")

	h := newDebugHandler(conn, &sync.Mutex{})
	w := debugGet(t, h, "SELECT * FROM empty")

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, `[]`, strings.TrimSpace(w.Body.String()))
}

func TestDebugHandler_RejectsMissingQuery(t *testing.T) {
	h := newDebugHandler(newTestConn(t), &sync.Mutex{})
	w := debugGet(t, h, "")

	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Equal(t, `{"error":"No SQL query provided"}`, strings.TrimSpace(w.Body.String()))
}

func TestDebugHandler_ReportsQueryErrors(t *testing.T) {
	h := newDebugHandler(newTestConn(t), &sync.Mutex{})
	w := debugGet(t, h, "SELECT * FROM does_not_exist")

	assert.Equal(t, http.StatusInternalServerError, w.Code)
	body := strings.TrimSpace(w.Body.String())
	assert.That(t, strings.HasPrefix(body, `{"error":"`))
	assert.That(t, strings.Contains(body, "does_not_exist"))
}

// The endpoint shares the pipeline's connection, so it must not touch it while
// a handler holds the lock.
func TestDebugHandler_WaitsForThePipelineLock(t *testing.T) {
	conn := newTestConn(t)
	execSQL(t, conn, "CREATE TABLE t (id BIGINT)")

	lock := &sync.Mutex{}
	h := newDebugHandler(conn, lock)

	lock.Lock()
	done := make(chan int, 1)
	go func() {
		done <- debugGet(t, h, "SELECT * FROM t").Code
	}()

	select {
	case <-done:
		t.Fatal("query ran while the pipeline held the lock")
	case <-time.After(250 * time.Millisecond):
	}

	lock.Unlock()
	select {
	case code := <-done:
		assert.Equal(t, http.StatusOK, code)
	case <-time.After(5 * time.Second):
		t.Fatal("query stayed blocked after the lock was released")
	}
}

// Python serves the endpoint from Flask's default 127.0.0.1:5000.
func TestDebugServer_ServesOnPythonPort(t *testing.T) {
	assert.Equal(t, "127.0.0.1:5000", debugAddr)
}

func TestDebugServer_ServesTheDebugRoute(t *testing.T) {
	conn := newTestConn(t)
	execSQL(t, conn, "CREATE TABLE t (id BIGINT)")
	execSQL(t, conn, "INSERT INTO t VALUES (7)")

	srv := httptest.NewServer(newDebugHandler(conn, &sync.Mutex{}))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/debug?sql=" + url.QueryEscape("SELECT id FROM t"))
	assert.NoError(t, err)
	defer resp.Body.Close()

	b, err := io.ReadAll(resp.Body)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, `[[7]]`, strings.TrimSpace(string(b)))
}
