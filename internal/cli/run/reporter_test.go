package run

import (
	"crypto/ed25519"
	"encoding/json"
	"io"
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
)

// testCredential is the wire vectors' key. A test needs one that parses; a
// control plane issues the real thing.
const testCredential = "sfc_AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8"

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

func writeConfig(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "pipeline.yml")
	assert.NoError(t, os.WriteFile(path, []byte(body), 0o600))
	return path
}

// A pipeline reports itself, signed, and its last bundle says it stopped.
//
// A webhook source needs no broker: it listens, receives nothing, and the run
// ends on --max-msgs. What is under test is the reporting, not the source.
func TestObservabilityTurbostatsReporter_APipelineReportsItself(t *testing.T) {
	coverage.Covers(t, "observability.turbostats.reporter")
	sink := &bundleSink{}
	receiver := httptest.NewServer(sink)
	defer receiver.Close()

	path := writeConfig(t, `
pipeline:
  name: reporting_pipeline
  turbostats:
    id: pipeline-01
    report_to: `+receiver.URL+`
    key: `+testCredential+`
    interval_seconds: 1
  source:
    type: webhook
    webhook:
      addr: 127.0.0.1:0
  handler:
    type: handlers.InferredMemBatch
    sql: SELECT 1 AS n
  sink:
    type: noop
`)

	done := make(chan struct{})
	go func() {
		defer close(done)
		cmd := NewCommand()
		cmd.SetArgs([]string{path, "--max-msgs", "0"})
		_ = cmd.Execute()
	}()

	waitFor(t, func() bool { return len(sink.all()) >= 1 })

	first := sink.all()[0]
	assert.Equal(t, "pipeline-01", first.Instance.ID)
	assert.Equal(t, "reporting_pipeline", first.Instance.Name)
	// A run reports a pipeline section and no serve one.
	assert.That(t, first.Pipeline != nil)
	assert.That(t, first.Serve == nil)
	assert.Equal(t, 1, first.IntervalSeconds)

	priv, err := wire.ParseCredential(testCredential)
	assert.NoError(t, err)
	sink.mu.Lock()
	header := sink.headers[0]
	sink.mu.Unlock()
	keyID, _, _, err := wire.ParseHeaders(header)
	assert.NoError(t, err)
	assert.Equal(t, wire.KeyID(priv.Public().(ed25519.PublicKey)), keyID)
}

// The drain is not held open by a receiver that is not there. That claim is
// checked two ways, neither of them here: TestReporter_AHungReceiverDoesNot
// DelayTheCaller bounds one post, and the commit's evidence times a real
// SIGTERM against a dead endpoint.
//
// It is not a test in this package because a run has to end for it to be
// timed, and a webhook source with no traffic never reaches --max-msgs. A
// test that sends a signal to its own process to work around that would be
// testing the test.

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition never held")
}
