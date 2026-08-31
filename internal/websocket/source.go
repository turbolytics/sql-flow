package websocket

import (
	"context"
	"fmt"
	"github.com/turbolytics/turbine/internal/core"
	"sync"
	"time"

	ws "github.com/coder/websocket"
	"go.uber.org/zap"
)

const (
	defaultChannelBuffer = 100
	defaultDialTimeout   = 30 * time.Second
	// Raised from the library's 32KiB default: a firehose frame over the
	// limit fails the read and drops the connection.
	defaultReadLimit = 32 << 20
	// Reconnect backoff, doubling from min to max.
	defaultReconnectDelay    = time.Second
	defaultMaxReconnectDelay = 30 * time.Second
)

type Source struct {
	uri               string
	channelBuffer     int
	dialTimeout       time.Duration
	readLimit         int64
	reconnectDelay    time.Duration
	maxReconnectDelay time.Duration

	streamChan chan []core.Message
	done       chan struct{}
	closeOnce  sync.Once

	// mu guards conn, which the read loop replaces on every reconnect while
	// Close may be tearing it down.
	mu   sync.Mutex
	conn *ws.Conn

	logger *zap.Logger
}

type Option func(*Source)

func WithLogger(logger *zap.Logger) Option {
	return func(s *Source) {
		s.logger = logger.Named("source.websocket")
	}
}

func WithChannelBuffer(size int) Option {
	return func(s *Source) {
		s.channelBuffer = size
	}
}

func WithDialTimeout(d time.Duration) Option {
	return func(s *Source) {
		s.dialTimeout = d
	}
}

func WithReadLimit(limit int64) Option {
	return func(s *Source) {
		s.readLimit = limit
	}
}

// WithReconnectDelay sets the first backoff after a dropped connection.
func WithReconnectDelay(d time.Duration) Option {
	return func(s *Source) {
		s.reconnectDelay = d
	}
}

func WithMaxReconnectDelay(d time.Duration) Option {
	return func(s *Source) {
		s.maxReconnectDelay = d
	}
}

func NewSource(uri string, opts ...Option) (*Source, error) {
	if uri == "" {
		return nil, fmt.Errorf("websocket source: uri is required")
	}

	s := &Source{
		uri:               uri,
		channelBuffer:     defaultChannelBuffer,
		dialTimeout:       defaultDialTimeout,
		readLimit:         defaultReadLimit,
		reconnectDelay:    defaultReconnectDelay,
		maxReconnectDelay: defaultMaxReconnectDelay,

		done:   make(chan struct{}),
		logger: zap.NewNop(),
	}

	for _, opt := range opts {
		opt(s)
	}

	s.streamChan = make(chan []core.Message, s.channelBuffer)

	return s, nil
}

// Start dials the endpoint. Unlike the Python source, which connects lazily
// inside stream(), a bad uri or an unreachable server is reported here rather
// than after the pipeline is running.
func (s *Source) Start() error {
	s.logger.Info("connecting to websocket", zap.String("uri", s.uri))
	return s.dial()
}

func (s *Source) Stream() <-chan []core.Message {
	go func() {
		defer close(s.streamChan)

		for {
			data, err := s.read()
			if err != nil {
				select {
				case <-s.done:
					return
				default:
				}

				s.logger.Warn("websocket read failed, reconnecting", zap.Error(err))
				if !s.reconnect() {
					return
				}
				continue
			}

			select {
			case s.streamChan <- []core.Message{{Value: data}}:
			case <-s.done:
				return
			}
		}
	}()

	return s.streamChan
}

func (s *Source) Commit() error {
	return nil
}

func (s *Source) Close() error {
	s.closeOnce.Do(func() {
		s.logger.Info("closing websocket source")
		close(s.done)
		s.closeConn(true)
	})
	return nil
}

func (s *Source) dial() error {
	ctx, cancel := context.WithTimeout(context.Background(), s.dialTimeout)
	defer cancel()

	c, _, err := ws.Dial(ctx, s.uri, nil)
	if err != nil {
		return fmt.Errorf("websocket dial %q: %w", s.uri, err)
	}
	c.SetReadLimit(s.readLimit)

	s.mu.Lock()
	s.conn = c
	s.mu.Unlock()

	return nil
}

// read blocks on the current connection. Reads are not given a deadline: the
// stream is idle whenever the publisher is, and Close is what interrupts it.
func (s *Source) read() ([]byte, error) {
	s.mu.Lock()
	c := s.conn
	s.mu.Unlock()

	if c == nil {
		return nil, fmt.Errorf("websocket source: not connected")
	}

	_, data, err := c.Read(context.Background())
	return data, err
}

// reconnect retries with an exponential backoff until it connects, reporting
// false once the source is closed.
func (s *Source) reconnect() bool {
	s.closeConn(false)

	delay := s.reconnectDelay
	for {
		select {
		case <-s.done:
			return false
		case <-time.After(delay):
		}

		if err := s.dial(); err != nil {
			s.logger.Warn("websocket reconnect failed",
				zap.Duration("retry_in", delay),
				zap.Error(err),
			)
			if delay = delay * 2; delay > s.maxReconnectDelay {
				delay = s.maxReconnectDelay
			}
			continue
		}

		s.logger.Info("websocket reconnected", zap.String("uri", s.uri))
		return true
	}
}

// closeConn tears down the current connection. Only a deliberate Close
// attempts the closing handshake: after a read failure the connection is
// already broken, and waiting on a close frame that will never arrive costs
// the handshake timeout on every reconnect.
func (s *Source) closeConn(graceful bool) {
	s.mu.Lock()
	c := s.conn
	s.conn = nil
	s.mu.Unlock()

	if c == nil {
		return
	}

	if graceful {
		if err := c.Close(ws.StatusNormalClosure, ""); err == nil {
			return
		}
	}
	c.CloseNow()
}
