package webhook

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/zeebo/assert"
)

func sign(secret string, body []byte) string {
	m := hmac.New(sha256.New, []byte(secret))
	m.Write(body)
	return "sha256=" + hex.EncodeToString(m.Sum(nil))
}

func post(t *testing.T, url string, body []byte, header, signature string) *http.Response {
	t.Helper()

	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	assert.NoError(t, err)
	if header != "" {
		req.Header.Set(header, signature)
	}

	resp, err := http.DefaultClient.Do(req)
	assert.NoError(t, err)
	return resp
}

func readBody(t *testing.T, resp *http.Response) string {
	t.Helper()

	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	assert.NoError(t, err)
	return strings.TrimSpace(string(b))
}

// Status codes and response bodies are the Python engine's, see
// tests/sources/test_webhook.py.
func TestSource_EventsHMACValidation(t *testing.T) {
	body := []byte(`{"key": "value"}`)
	conf := &HMAC{Header: "X-HMAC-Signature", SigKey: "sha256", Secret: "test_secret"}

	tests := []struct {
		name       string
		hmac       *HMAC
		header     string
		signature  string
		wantStatus int
		wantBody   string
	}{
		{
			name:       "no hmac configured",
			wantStatus: http.StatusOK,
			wantBody:   `{"status":"received"}`,
		},
		{
			name:       "missing signature header",
			hmac:       conf,
			wantStatus: http.StatusBadRequest,
			wantBody:   `{"detail":"Missing HMAC signature"}`,
		},
		{
			name:       "invalid signature",
			hmac:       conf,
			header:     conf.Header,
			signature:  "invalid_signature",
			wantStatus: http.StatusForbidden,
			wantBody:   `{"detail":"Invalid HMAC signature"}`,
		},
		{
			name:       "signature computed with the wrong secret",
			hmac:       conf,
			header:     conf.Header,
			signature:  sign("not_the_secret", body),
			wantStatus: http.StatusForbidden,
			wantBody:   `{"detail":"Invalid HMAC signature"}`,
		},
		{
			name:       "digest without the sha256= prefix",
			hmac:       conf,
			header:     conf.Header,
			signature:  strings.TrimPrefix(sign(conf.Secret, body), "sha256="),
			wantStatus: http.StatusForbidden,
			wantBody:   `{"detail":"Invalid HMAC signature"}`,
		},
		{
			name:       "valid signature",
			hmac:       conf,
			header:     conf.Header,
			signature:  sign(conf.Secret, body),
			wantStatus: http.StatusOK,
			wantBody:   `{"status":"received"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, err := NewSource(WithHMAC(tt.hmac))
			assert.NoError(t, err)

			srv := httptest.NewServer(s.Handler())
			defer srv.Close()

			// Accepted requests park a message on a queue of one; drain it so
			// the request cannot block on the pipeline.
			stream := s.Stream()
			go func() {
				for range stream {
				}
			}()
			defer s.Close()

			resp := post(t, srv.URL+"/events", body, tt.header, tt.signature)
			assert.Equal(t, tt.wantStatus, resp.StatusCode)
			assert.Equal(t, tt.wantBody, readBody(t, resp))
		})
	}
}

func TestSource_DeliversBodyToStream(t *testing.T) {
	s, err := NewSource()
	assert.NoError(t, err)
	defer s.Close()

	srv := httptest.NewServer(s.Handler())
	defer srv.Close()

	body := []byte(`{"action":"opened"}`)
	resp := post(t, srv.URL+"/events", body, "", "")
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	resp.Body.Close()

	select {
	case batch := <-s.Stream():
		assert.Equal(t, 1, len(batch))
		assert.Equal(t, string(body), string(batch[0].Value))
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the message")
	}
}

// The Python source queues at most one message, so a second delivery waits
// for the pipeline to consume the first.
func TestSource_BackpressureHoldsSecondRequest(t *testing.T) {
	s, err := NewSource()
	assert.NoError(t, err)
	defer s.Close()

	srv := httptest.NewServer(s.Handler())
	defer srv.Close()

	resp := post(t, srv.URL+"/events", []byte("first"), "", "")
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	resp.Body.Close()

	second := make(chan int, 1)
	go func() {
		resp := post(t, srv.URL+"/events", []byte("second"), "", "")
		resp.Body.Close()
		second <- resp.StatusCode
	}()

	select {
	case <-second:
		t.Fatal("second request completed while the queue was full")
	case <-time.After(250 * time.Millisecond):
	}

	batch := <-s.Stream()
	assert.Equal(t, "first", string(batch[0].Value))

	select {
	case code := <-second:
		assert.Equal(t, http.StatusOK, code)
	case <-time.After(2 * time.Second):
		t.Fatal("second request stayed blocked after the queue drained")
	}

	batch = <-s.Stream()
	assert.Equal(t, "second", string(batch[0].Value))
}

func TestSource_CloseReleasesBlockedRequest(t *testing.T) {
	s, err := NewSource()
	assert.NoError(t, err)

	srv := httptest.NewServer(s.Handler())
	defer srv.Close()

	resp := post(t, srv.URL+"/events", []byte("first"), "", "")
	resp.Body.Close()

	blocked := make(chan int, 1)
	go func() {
		resp := post(t, srv.URL+"/events", []byte("second"), "", "")
		resp.Body.Close()
		blocked <- resp.StatusCode
	}()

	// Give the second request time to reach the full queue.
	time.Sleep(100 * time.Millisecond)
	assert.NoError(t, s.Close())

	select {
	case code := <-blocked:
		assert.Equal(t, http.StatusServiceUnavailable, code)
	case <-time.After(2 * time.Second):
		t.Fatal("close did not release the blocked request")
	}
}

func TestSource_RoutesOnlyPostEvents(t *testing.T) {
	s, err := NewSource()
	assert.NoError(t, err)
	defer s.Close()

	srv := httptest.NewServer(s.Handler())
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/events")
	assert.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusMethodNotAllowed, resp.StatusCode)

	resp = post(t, srv.URL+"/unknown", []byte("{}"), "", "")
	resp.Body.Close()
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestSource_StartServesOnItsOwnListener(t *testing.T) {
	s, err := NewSource(WithAddr("127.0.0.1:0"))
	assert.NoError(t, err)
	assert.NoError(t, s.Start())
	defer s.Close()

	body := []byte(`{"hello":"world"}`)
	resp := post(t, "http://"+s.Addr()+"/events", body, "", "")
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	resp.Body.Close()

	select {
	case batch := <-s.Stream():
		assert.Equal(t, string(body), string(batch[0].Value))
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the message")
	}
}

func TestSource_StartReportsBindFailure(t *testing.T) {
	first, err := NewSource(WithAddr("127.0.0.1:0"))
	assert.NoError(t, err)
	assert.NoError(t, first.Start())
	defer first.Close()

	second, err := NewSource(WithAddr(first.Addr()))
	assert.NoError(t, err)
	assert.Error(t, second.Start())
}

func TestSource_DefaultsToPythonHostAndPort(t *testing.T) {
	s, err := NewSource()
	assert.NoError(t, err)
	assert.Equal(t, "0.0.0.0:8001", s.addr)
}

func TestSource_CloseIsIdempotent(t *testing.T) {
	s, err := NewSource(WithAddr("127.0.0.1:0"))
	assert.NoError(t, err)
	assert.NoError(t, s.Start())

	assert.NoError(t, s.Close())
	assert.NoError(t, s.Close())
	assert.NoError(t, s.Commit())
}
