package websocket

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	ws "github.com/coder/websocket"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/turbolytics/sql-flow/internal/leakloop"
)

// The websocket leak loops. The Bluesky demo reads Jetstream through this
// source on a 512 MB worker, so it is one of the components its memory could
// be growing in. Each loop runs the source against a local server and reports
// growth per event; see internal/leakloop for what the numbers mean.

// serveLoop starts a server that writes posts in a cycle, perConn frames per
// connection, then drops the connection without a close handshake, the way a
// failing network does. perConn 0 never drops.
func serveLoop(t *testing.T, posts [][]byte, perConn int) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := ws.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer c.CloseNow()
		for i := 0; perConn == 0 || i < perConn; i++ {
			if err := c.Write(r.Context(), ws.MessageText, posts[i%len(posts)]); err != nil {
				return
			}
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

// TestSourceWebsocket__SteadyStreamPerMessage reads frames from one
// connection that never drops.
func TestSourceWebsocket__SteadyStreamPerMessage(t *testing.T) {
	coverage.Covers(t, "source.websocket")
	total := leakloop.Count(t, 50_000)
	posts := leakloop.Posts(t, 20_000)
	srv := serveLoop(t, posts, 0)

	s, err := NewSource(wsURL(srv))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	loop := leakloop.New(t, "websocket source, one connection", "message", nil)
	step := total / 20
	n, next := 0, step
	loop.Sample(0)
	for batch := range s.Stream() {
		n += len(batch)
		if n >= next {
			loop.Sample(int64(n))
			next += step
		}
		if n >= total {
			break
		}
	}
	loop.Report()
}

// TestSourceWebsocket__ReconnectPerReconnect drops the connection every 50
// frames, so the source tears down and redials thousands of times.
func TestSourceWebsocket__ReconnectPerReconnect(t *testing.T) {
	coverage.Covers(t, "source.websocket")
	const perConn = 50
	reconnects := leakloop.Count(t, 400)
	posts := leakloop.Posts(t, 1_000)
	srv := serveLoop(t, posts, perConn)

	s, err := NewSource(wsURL(srv),
		WithReconnectDelay(time.Millisecond),
		WithMaxReconnectDelay(time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	loop := leakloop.New(t, "websocket source, dropped every 50 frames", "reconnect", nil)
	step := reconnects / 20
	if step == 0 {
		step = 1
	}
	// Counted from frames, not from the server: a frame the drop cuts off
	// only delays the next sample.
	n, next := 0, step
	loop.Sample(0)
	for batch := range s.Stream() {
		n += len(batch)
		done := n / perConn
		if done >= next {
			loop.Sample(int64(done))
			next += step
		}
		if done >= reconnects {
			break
		}
	}
	loop.Report()
}
