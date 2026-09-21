package run

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
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

	// Stopped by cancelling, not by --max-msgs. The flag was set to 0, which
	// means unlimited, so this pipeline never ended: it outlived the test and
	// kept posting to a closed receiver for the rest of the package run,
	// holding a goroutine, a port and a DuckDB connection.
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		cmd := NewCommand()
		cmd.SetArgs([]string{path})
		_ = cmd.ExecuteContext(ctx)
	}()
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(30 * time.Second):
			t.Fatal("the pipeline did not stop when its context was cancelled")
		}
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

	// The last bundle says how it ended, which is what the receiver reads to
	// tell a clean stop from a crash. The doc comment claimed this and
	// nothing checked it.
	cancel()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("the pipeline did not stop when its context was cancelled")
	}

	all := sink.all()
	last := all[len(all)-1]
	assert.That(t, last.Exit != nil)
	// Cancelled from outside, so a signal rather than a run that finished
	// what it was asked to do.
	assert.Equal(t, "signal", last.Exit.Reason)
	assert.Equal(t, 0, last.Exit.Code)
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

// run refuses a turbostats block that validate rejects.
//
// serve refused one at startup and run did not, so a plaintext report_to to a
// public address started anyway: the bundle and the signature headers that
// authenticate it crossed the network in the clear. A signature proves who
// sent a document, it does not hide it.
//
// The path in the message is checked too. It used to read "turbostats.
// report_to" from both commands, which is in neither file: the block lives at
// pipeline.turbostats under run and serve.turbostats under serve, and an
// operator cannot act on a path they cannot find.
func TestObservabilityTurbostatsReporter_RunRefusesAPlaintextDestination(t *testing.T) {
	coverage.Covers(t, "observability.turbostats.reporter")
	path := writeConfig(t, `
pipeline:
  name: reporting_pipeline
  turbostats:
    id: pipeline-01
    report_to: http://control.example.com/v1/turbostats
    key: `+testCredential+`
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
	cmd := NewCommand()
	cmd.SetArgs([]string{path})
	cmd.SilenceUsage, cmd.SilenceErrors = true, true
	err := cmd.ExecuteContext(context.Background())

	assert.That(t, err != nil)
	assert.That(t, strings.Contains(err.Error(), "plaintext"))
	assert.That(t, strings.Contains(err.Error(), "pipeline.turbostats.report_to"))
}
