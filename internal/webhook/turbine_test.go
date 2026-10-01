package webhook

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
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
