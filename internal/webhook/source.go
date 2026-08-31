package webhook

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net"
	"net/http"
	"sync"
	"time"

	"go.uber.org/zap"
)

// The Python engine serves on 0.0.0.0:8001; configs and reverse proxies point
// at that port, so it is not configurable from YAML.
const defaultAddr = "0.0.0.0:8001"

const shutdownTimeout = 5 * time.Second

// HMAC configures signature validation of incoming request bodies.
type HMAC struct {
	Header string
	SigKey string
	Secret string
}

type Source struct {
	addr       string
	hmac       *HMAC
	server     *http.Server
	listener   net.Listener
	streamChan chan [][]byte
	done       chan struct{}
	closeOnce  sync.Once
	// mu guards closed against in-flight handlers: a request holds it for
	// read across its send, so the stream is only closed once no handler can
	// still write to it.
	mu     sync.RWMutex
	closed bool

	logger *zap.Logger
}

type Option func(*Source)

func WithLogger(logger *zap.Logger) Option {
	return func(s *Source) {
		s.logger = logger.Named("source.webhook")
	}
}

func WithHMAC(h *HMAC) Option {
	return func(s *Source) {
		s.hmac = h
	}
}

// WithAddr overrides the listen address. Tests use it to bind port 0.
func WithAddr(addr string) Option {
	return func(s *Source) {
		s.addr = addr
	}
}

func NewSource(opts ...Option) (*Source, error) {
	s := &Source{
		addr: defaultAddr,
		// A queue of one, as in the Python source: a delivery is accepted
		// while the pipeline works on the previous one, and the next sender
		// waits rather than having its event dropped.
		streamChan: make(chan [][]byte, 1),
		done:       make(chan struct{}),

		logger: zap.NewNop(),
	}

	for _, opt := range opts {
		opt(s)
	}

	return s, nil
}

// HMACConfig reports the signature validation in effect, nil when bodies are
// accepted unvalidated.
func (s *Source) HMACConfig() *HMAC {
	return s.hmac
}

// Handler is the webhook endpoint, exposed so it can be served on a listener
// the caller owns.
func (s *Source) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /events", s.receiveEvents)
	return mux
}

// Addr reports the bound address, which only differs from the configured one
// when the port was left to the kernel.
func (s *Source) Addr() string {
	if s.listener == nil {
		return s.addr
	}
	return s.listener.Addr().String()
}

func (s *Source) Start() error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	s.listener = ln
	s.server = &http.Server{Handler: s.Handler()}

	s.logger.Info("starting webhook server", zap.String("addr", ln.Addr().String()))

	go func() {
		if err := s.server.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.logger.Error("webhook server stopped", zap.Error(err))
		}
	}()

	return nil
}

func (s *Source) Stream() <-chan [][]byte {
	return s.streamChan
}

func (s *Source) Commit() error {
	return nil
}

func (s *Source) Close() error {
	var err error
	s.closeOnce.Do(func() {
		s.logger.Info("closing webhook source")
		// Released first: a handler blocked on a full queue would otherwise
		// keep Shutdown waiting for a request that can never complete.
		close(s.done)

		if s.server != nil {
			ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
			defer cancel()
			err = s.server.Shutdown(ctx)
		}

		s.mu.Lock()
		s.closed = true
		close(s.streamChan)
		s.mu.Unlock()
	})
	return err
}

func (s *Source) receiveEvents(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, `{"detail":"Unable to read request body"}`)
		return
	}

	if s.hmac != nil {
		signature := r.Header.Get(s.hmac.Header)
		if signature == "" {
			writeJSON(w, http.StatusBadRequest, `{"detail":"Missing HMAC signature"}`)
			return
		}
		if !validSignature(s.hmac.Secret, signature, body) {
			writeJSON(w, http.StatusForbidden, `{"detail":"Invalid HMAC signature"}`)
			return
		}
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		writeJSON(w, http.StatusServiceUnavailable, `{"detail":"Source is closed"}`)
		return
	}

	select {
	case s.streamChan <- [][]byte{body}:
		writeJSON(w, http.StatusOK, `{"status":"received"}`)
	case <-s.done:
		writeJSON(w, http.StatusServiceUnavailable, `{"detail":"Source is closed"}`)
	case <-r.Context().Done():
		// The client hung up while waiting on the pipeline; the event is
		// dropped and there is nobody left to answer.
	}
}

// validSignature compares against the "sha256=<hexdigest>" form the Python
// source builds. sig_key is carried in config for parity but, as in Python,
// the digest is always SHA-256.
func validSignature(secret, signature string, body []byte) bool {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	expected := "sha256=" + hex.EncodeToString(mac.Sum(nil))
	return hmac.Equal([]byte(signature), []byte(expected))
}

func writeJSON(w http.ResponseWriter, status int, body string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(body))
}
