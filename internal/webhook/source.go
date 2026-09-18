package webhook

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"github.com/turbolytics/sql-flow/internal/core"
	"io"
	"net"
	"net/http"
	"os"
	"sync"
	"time"

	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
	"golang.org/x/net/netutil"
)

// The Python engine served on 0.0.0.0:8001. It is the address a Source built
// without WithAddr binds; the config's default is config.DefaultWebhookAddr,
// the same value.
const defaultAddr = "0.0.0.0:8001"

const shutdownTimeout = 5 * time.Second

// DefaultMaxBodyBytes bounds one delivery. GitHub caps a payload at 25 MB,
// the largest of the senders the examples point at, so the default admits
// every delivery those senders make and nothing larger.
const DefaultMaxBodyBytes int64 = 25 << 20

// DefaultMaxConnections bounds open connections. The pipeline takes one
// delivery at a time, so beyond a handful the extra connections only hold
// bodies; 64 leaves room for a proxy's pool and a burst of senders while
// keeping the worst case, every slot holding a full body, at 1.6 GiB.
const DefaultMaxConnections = 64

const (
	// readHeaderTimeout closes a connection that sends no request, the value
	// the serve package uses.
	readHeaderTimeout = 10 * time.Second
	// bodyReadTimeout bounds the body read alone, set from the handler and
	// cleared once the body is in. It is not the server's ReadTimeout: that
	// deadline runs through the handler, and when it expires Go cancels the
	// request, which would drop a delivery waiting on the pipeline. 60s
	// carries the default body bound at 0.5 MB/s.
	bodyReadTimeout = 60 * time.Second
	// idleTimeout releases a keep-alive connection that has gone quiet.
	idleTimeout = 60 * time.Second
)

// HMAC configures signature validation of incoming request bodies.
type HMAC struct {
	Header string
	SigKey string
	Secret string
}

type Source struct {
	addr              string
	hmac              *HMAC
	maxBodyBytes      int64
	maxConnections    int
	readHeaderTimeout time.Duration
	bodyReadTimeout   time.Duration
	server            *http.Server
	listener          net.Listener
	streamChan        chan []core.Message
	done              chan struct{}
	closeOnce         sync.Once
	// mu guards closed against in-flight handlers: a request holds it for
	// read across its send, so the stream is only closed once no handler can
	// still write to it.
	mu     sync.RWMutex
	closed bool

	logger        *zap.Logger
	meterProvider metric.MeterProvider
	metrics       *Metrics
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

// WithAddr sets the listen address. The source registry passes the config's
// addr; tests pass port 0.
func WithAddr(addr string) Option {
	return func(s *Source) {
		s.addr = addr
	}
}

// WithMaxBodyBytes bounds a request body. A body past it is refused with 413
// before it is read, ahead of signature validation, so the bound holds
// against a sender that cannot sign.
func WithMaxBodyBytes(n int64) Option {
	return func(s *Source) {
		s.maxBodyBytes = n
	}
}

// WithMaxConnections bounds open connections. Past it, a new connection waits
// in the kernel backlog until one closes.
func WithMaxConnections(n int) Option {
	return func(s *Source) {
		s.maxConnections = n
	}
}

// WithReadHeaderTimeout overrides the header timeout. Tests use it.
func WithReadHeaderTimeout(d time.Duration) Option {
	return func(s *Source) {
		s.readHeaderTimeout = d
	}
}

// WithBodyReadTimeout overrides the body timeout. Tests use it.
func WithBodyReadTimeout(d time.Duration) Option {
	return func(s *Source) {
		s.bodyReadTimeout = d
	}
}

// WithMeterProvider records request metrics against the given provider. A nil
// provider leaves the source recording nothing.
func WithMeterProvider(mp metric.MeterProvider) Option {
	return func(s *Source) {
		s.meterProvider = mp
	}
}

func NewSource(opts ...Option) (*Source, error) {
	s := &Source{
		addr:              defaultAddr,
		maxBodyBytes:      DefaultMaxBodyBytes,
		maxConnections:    DefaultMaxConnections,
		readHeaderTimeout: readHeaderTimeout,
		bodyReadTimeout:   bodyReadTimeout,
		// A queue of one, as in the Python source: a delivery is accepted
		// while the pipeline works on the previous one, and the next sender
		// waits rather than having its event dropped.
		streamChan: make(chan []core.Message, 1),
		done:       make(chan struct{}),

		logger: zap.NewNop(),
	}

	for _, opt := range opts {
		opt(s)
	}

	m, err := NewMetrics(s.meterProvider)
	if err != nil {
		return nil, err
	}
	s.metrics = m

	return s, nil
}

// HMACConfig reports the signature validation in effect, nil when bodies are
// accepted unvalidated.
func (s *Source) HMACConfig() *HMAC {
	return s.hmac
}

// MaxBodyBytes reports the body bound in effect.
func (s *Source) MaxBodyBytes() int64 {
	return s.maxBodyBytes
}

// MaxConnections reports the connection bound in effect.
func (s *Source) MaxConnections() int {
	return s.maxConnections
}

// Handler is the webhook endpoint, exposed so it can be served on a listener
// the caller owns.
func (s *Source) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /events", s.receiveEvents)

	root := http.NewServeMux()
	// Outside the metrics middleware. webhook_requests_total is how an
	// operator counts deliveries, and a platform checks health every few
	// seconds: counted, the checks would bury them under 200s that delivered
	// nothing. Registered for every method, because beside the catch-all
	// below a GET-only pattern would hand POST /healthz to the delivery mux,
	// which answers 404 for a path that exists.
	root.HandleFunc("/healthz", s.healthz)
	// Wrapped rather than applied per route, so unrouted requests are counted
	// as they are by the Python middleware.
	root.Handle("/", s.metrics.middleware(mux))
	return root
}

// healthz says whether a delivery sent now would be admitted. It is on the
// webhook's own listener because a platform routes one port to a service and
// checks health on that port: the pipeline's /healthz is on the metrics
// listener, which a platform that exposes only this one cannot reach.
//
// It reads no body and checks no signature, since a health check has neither,
// and it admits nothing to the pipeline. It does not wait on the queue: a full
// queue is backpressure, and a platform that reads busy as dead restarts an
// instance while it holds a sender's event. A closing source answers 503, as
// it does to a delivery, so the platform stops routing to it.
func (s *Source) healthz(w http.ResponseWriter, r *http.Request) {
	// Monitors send HEAD, and the status line is the whole answer. net/http
	// drops the body of a HEAD response.
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		writeJSON(w, http.StatusMethodNotAllowed, `{"detail":"Method Not Allowed"}`)
		return
	}
	select {
	case <-s.done:
		writeJSON(w, http.StatusServiceUnavailable, `{"detail":"Source is closed"}`)
	default:
		writeJSON(w, http.StatusOK, `{"status":"ok"}`)
	}
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
	s.listener = netutil.LimitListener(ln, s.maxConnections)
	s.server = &http.Server{
		Handler:           s.Handler(),
		ReadHeaderTimeout: s.readHeaderTimeout,
		IdleTimeout:       idleTimeout,
	}

	s.logger.Info("starting webhook server", zap.String("addr", ln.Addr().String()))

	go func() {
		if err := s.server.Serve(s.listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.logger.Error("webhook server stopped", zap.Error(err))
		}
	}()

	return nil
}

func (s *Source) Stream() <-chan []core.Message {
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
	// The body read runs under its own deadline, cleared once the body is
	// in, so the wait on the pipeline below is under none. Set before the
	// length check so the server's drain of a refused body is covered too.
	rc := http.NewResponseController(w)
	if err := rc.SetReadDeadline(time.Now().Add(s.bodyReadTimeout)); err != nil {
		s.logger.Debug("body read deadline not supported", zap.Error(err))
	}

	// The bound comes before the read and before the signature check: the
	// body is the allocation, and a signature only proves who sent it after
	// it is held in full. A declared length past the bound is refused
	// without reading a byte; a chunked body is cut at the bound.
	if r.ContentLength > s.maxBodyBytes {
		writeJSON(w, http.StatusRequestEntityTooLarge, `{"detail":"Request body too large"}`)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, s.maxBodyBytes)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeJSON(w, http.StatusRequestEntityTooLarge, `{"detail":"Request body too large"}`)
			return
		}
		if errors.Is(err, os.ErrDeadlineExceeded) {
			writeJSON(w, http.StatusRequestTimeout, `{"detail":"Request body timed out"}`)
			return
		}
		writeJSON(w, http.StatusBadRequest, `{"detail":"Unable to read request body"}`)
		return
	}
	_ = rc.SetReadDeadline(time.Time{})

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
	case s.streamChan <- []core.Message{{Value: body}}:
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
