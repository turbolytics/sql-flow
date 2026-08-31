package websocket

import (
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	ws "github.com/coder/websocket"
	"github.com/zeebo/assert"
)

// newServer starts a real websocket server that hands every connection the
// same messages. serve decides what happens once they are written.
func newServer(t *testing.T, msgs []string, hold bool) (*httptest.Server, *atomic.Int64) {
	t.Helper()

	var conns atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conns.Add(1)
		c, err := ws.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer c.CloseNow()

		for _, m := range msgs {
			if err := c.Write(r.Context(), ws.MessageText, []byte(m)); err != nil {
				return
			}
		}

		if hold {
			// Reads service the client's control frames, so a close from the
			// source completes its handshake instead of timing out.
			for {
				if _, _, err := c.Read(r.Context()); err != nil {
					return
				}
			}
		}
	}))
	t.Cleanup(srv.Close)

	return srv, &conns
}

func wsURL(srv *httptest.Server) string {
	return "ws" + strings.TrimPrefix(srv.URL, "http")
}

func recv(t *testing.T, stream <-chan [][]byte) []byte {
	t.Helper()

	select {
	case batch, ok := <-stream:
		if !ok {
			t.Fatal("stream closed before a message arrived")
		}
		assert.Equal(t, 1, len(batch))
		return batch[0]
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for a message")
		return nil
	}
}

func TestSource_StreamsMessages(t *testing.T) {
	srv, _ := newServer(t, []string{"one", "two", "three"}, true)

	s, err := NewSource(wsURL(srv))
	assert.NoError(t, err)
	assert.NoError(t, s.Start())
	defer s.Close()

	stream := s.Stream()
	for _, want := range []string{"one", "two", "three"} {
		assert.Equal(t, want, string(recv(t, stream)))
	}
}

func TestSource_StartReportsDialFailure(t *testing.T) {
	// A port that nothing listens on: bind one, then release it.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	assert.NoError(t, err)
	addr := ln.Addr().String()
	assert.NoError(t, ln.Close())

	s, err := NewSource("ws://" + addr)
	assert.NoError(t, err)
	assert.Error(t, s.Start())
}

// The server drops every connection after one message, so a second message
// can only arrive over a reconnect.
func TestSource_ReconnectsAfterDrop(t *testing.T) {
	srv, conns := newServer(t, []string{"msg"}, false)

	s, err := NewSource(wsURL(srv), WithReconnectDelay(10*time.Millisecond))
	assert.NoError(t, err)
	assert.NoError(t, s.Start())
	defer s.Close()

	stream := s.Stream()
	assert.Equal(t, "msg", string(recv(t, stream)))
	assert.Equal(t, "msg", string(recv(t, stream)))
	assert.That(t, conns.Load() >= 2)
}

func TestSource_CloseEndsStream(t *testing.T) {
	srv, _ := newServer(t, []string{"one"}, true)

	s, err := NewSource(wsURL(srv))
	assert.NoError(t, err)
	assert.NoError(t, s.Start())

	stream := s.Stream()
	assert.Equal(t, "one", string(recv(t, stream)))
	assert.NoError(t, s.Close())

	select {
	case _, ok := <-stream:
		assert.That(t, !ok)
	case <-time.After(5 * time.Second):
		t.Fatal("stream was not closed")
	}

	assert.NoError(t, s.Close())
	assert.NoError(t, s.Commit())
}

// A message larger than the library's 32KiB default read limit must not kill
// the connection.
func TestSource_ReadsLargeMessages(t *testing.T) {
	large := strings.Repeat("a", 128*1024)
	srv, _ := newServer(t, []string{large}, true)

	s, err := NewSource(wsURL(srv))
	assert.NoError(t, err)
	assert.NoError(t, s.Start())
	defer s.Close()

	assert.Equal(t, len(large), len(recv(t, s.Stream())))
}
