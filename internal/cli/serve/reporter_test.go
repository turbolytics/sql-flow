package serve

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/turbolytics/sql-flow/turbostats/wire"
	"github.com/zeebo/assert"
	"go.uber.org/zap"
)

// testCredential is the vectors' key. The control plane issues one of these;
// a test only needs one that parses.
const testCredential = "sfc_AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8"

// bundleSink records the signed bundles that arrive.
type bundleSink struct {
	mu      sync.Mutex
	bundles []wire.Bundle
	headers []http.Header
}

func (s *bundleSink) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	var b wire.Bundle
	if err := json.Unmarshal(body, &b); err == nil {
		s.mu.Lock()
		s.bundles = append(s.bundles, b)
		s.headers = append(s.headers, r.Header.Clone())
		s.mu.Unlock()
	}
	w.Header().Set("Content-Type", wire.MediaType)
	_, _ = w.Write([]byte(`{"v":1,"commands":[]}`))
}

func (s *bundleSink) all() []wire.Bundle {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]wire.Bundle(nil), s.bundles...)
}

// A serve instance reports itself, and its last bundle says it stopped.
//
// The route is not enabled: a fleet instance reports outbound and has no
// reason to listen, which is the case this wiring exists for.
func TestCliServe_ReportsItselfAndSaysWhenItStops(t *testing.T) {
	coverage.Covers(t, "observability.turbostats.reporter")
	sink := &bundleSink{}
	receiver := httptest.NewServer(sink)
	defer receiver.Close()

	dir := t.TempDir()
	path := filepath.Join(dir, "serve.yml")
	assert.NoError(t, os.WriteFile(path, []byte(`
serve:
  name: reporting_serve
  # An ephemeral port, as every other serve test uses. Without it this bound
  # the default 8080 and failed against anything already listening there,
  # including another test and a developer's own instance.
  http:
    addr: 127.0.0.1:0
  clients: [{name: local, id: local-dev}]
  turbostats:
    id: serve-01
    report_to: `+receiver.URL+`
    key: `+testCredential+`
    interval_seconds: 1
  datasets:
    - name: ping
      sql: SELECT 1 AS n
`), 0o600))

	ctx, cancel := context.WithCancel(context.Background())
	addrs := make(chan net.Addr, 1)
	done := make(chan error, 1)
	go func() {
		done <- serveConfig(ctx, path, zap.NewNop(), func(a net.Addr) { addrs <- a }, false)
	}()
	<-addrs

	waitFor(t, func() bool { return len(sink.all()) >= 1 })

	first := sink.all()[0]
	assert.Equal(t, wire.Version, first.V)
	assert.Equal(t, "serve-01", first.Instance.ID)
	assert.Equal(t, "reporting_serve", first.Instance.Name)
	// A serve process reports a serve section and no pipeline one.
	assert.That(t, first.Serve != nil)
	assert.That(t, first.Pipeline == nil)
	// The receiver needs the interval to tell a late heartbeat from a normal
	// one, and only the reporter knows it.
	assert.Equal(t, 1, first.IntervalSeconds)

	// Signed, with the key id the receiver looks the public half up by.
	priv, err := wire.ParseCredential(testCredential)
	assert.NoError(t, err)
	sink.mu.Lock()
	header := sink.headers[0]
	sink.mu.Unlock()
	keyID, _, _, err := wire.ParseHeaders(header)
	assert.NoError(t, err)
	assert.Equal(t, wire.KeyID(priv.Public().(ed25519.PublicKey)), keyID)

	cancel()
	select {
	case err := <-done:
		assert.NoError(t, err)
	case <-time.After(20 * time.Second):
		t.Fatal("serve did not stop")
	}

	// The last bundle carries an exit. A process that crashed sends none,
	// which is how a receiver tells the two apart.
	all := sink.all()
	last := all[len(all)-1]
	assert.That(t, last.Exit != nil)
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition never held")
}
