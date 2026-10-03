package webhook

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/turbolytics/sql-flow/internal/core"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

type countingHandler struct{ n int }

func (h *countingHandler) Init(context.Context) error { h.n = 0; return nil }
func (h *countingHandler) Write([]byte) error         { h.n++; return nil }
func (h *countingHandler) Invoke(context.Context) (arrow.Table, error) {
	return nil, nil
}
func (h *countingHandler) RowsRead() int64 { return int64(h.n) }

// jsonHandler refuses a message that is not JSON, as the inferred handlers
// do in Write. Under the default RAISE policy that refusal ends the run.
type jsonHandler struct{ countingHandler }

func (h *jsonHandler) Write(b []byte) error {
	if !json.Valid(b) {
		return errors.New("invalid json")
	}
	return h.countingHandler.Write(b)
}

type failingSink struct{}

func (failingSink) WriteTable(context.Context, arrow.Table) error { return nil }
func (failingSink) Flush(context.Context) error                   { return errors.New("broker down") }

type okSink struct{}

func (okSink) WriteTable(context.Context, arrow.Table) error { return nil }
func (okSink) Flush(context.Context) error                   { return nil }

// Scenario 9: a real webhook source and turbine. A flush that fails answers
// 503; one that succeeds answers 200.
func TestSourceWebhook_AfterFlushThroughTheTurbine(t *testing.T) {
	coverage.Covers(t, "source.webhook")
	for _, tc := range []struct {
		name string
		sink core.Sink
		want int
	}{{"flush fails", failingSink{}, http.StatusServiceUnavailable}, {"flush succeeds", okSink{}, http.StatusOK}} {
		t.Run(tc.name, func(t *testing.T) {
			// ConsumeLoop calls Start, which binds; port 0 keeps that bind
			// harmless beside the test server.
			s, err := NewSource(WithAckAfterFlush(), WithAddr("127.0.0.1:0"))
			assert.NoError(t, err)
			srv := httptest.NewServer(s.Handler())
			defer srv.Close()
			tb := core.NewTurbine(s, &countingHandler{}, tc.sink, 1, time.Second, &sync.Mutex{}, core.PipelineErrorPolicies{})
			go func() { _, _ = tb.ConsumeLoop(context.Background(), 0) }()
			resp := post(t, srv.URL+"/events", []byte(`{"a":1}`), "", "")
			resp.Body.Close()
			assert.Equal(t, tc.want, resp.StatusCode)
			_ = s.Close()
		})
	}
}

// One sender's malformed body must not stop the pipeline for everyone else
// (#425). It is answered 400, the run keeps going, and the next good body is
// flushed and answered 200.
func TestSourceWebhook_MalformedBodyDoesNotStopTheRun(t *testing.T) {
	coverage.Covers(t, "source.webhook")
	s, err := NewSource(WithAckAfterFlush(), WithAddr("127.0.0.1:0"))
	assert.NoError(t, err)
	srv := httptest.NewServer(s.Handler())
	defer srv.Close()
	defer s.Close()

	tb := core.NewTurbine(s, &jsonHandler{}, okSink{}, 1, time.Second, &sync.Mutex{}, core.PipelineErrorPolicies{})
	stopped := make(chan error, 1)
	go func() {
		_, err := tb.ConsumeLoop(context.Background(), 0)
		stopped <- err
	}()

	client := &http.Client{Timeout: 5 * time.Second}
	send := func(body string) int {
		resp, err := client.Post(srv.URL+"/events", "application/json", strings.NewReader(body))
		if err != nil {
			return 0 // no answer at all
		}
		resp.Body.Close()
		return resp.StatusCode
	}

	assert.Equal(t, http.StatusBadRequest, send("x"))
	assert.Equal(t, http.StatusOK, send(`{"a":1}`))
	select {
	case err := <-stopped:
		t.Fatalf("the run stopped: %v", err)
	default:
	}
}
