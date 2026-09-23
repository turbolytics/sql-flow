package mqtt

import (
	"context"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
	"github.com/turbolytics/sql-flow/internal/core"
	"github.com/turbolytics/sql-flow/internal/errs"
	"go.uber.org/zap"
)

const (
	defaultChannelBuffer  = 100
	defaultConnectTimeout = 30 * time.Second
	// Reconnect backoff, doubling from min to max, as in the websocket
	// source.
	defaultReconnectDelay    = time.Second
	defaultMaxReconnectDelay = 30 * time.Second
	keepAliveSeconds         = 30
	disconnectTimeout        = 5 * time.Second

	// ackFlushInterval overrides paho's 50ms default for sending queued
	// PUBACKs. paho.Client.Ack only marks a publish acked in memory; a
	// background ticker at this interval is what actually writes the
	// PUBACK to the broker. Shortening it bounds ackDrainWait.
	ackFlushInterval = 10 * time.Millisecond
	// ackDrainWait is how long Close waits before disconnecting.
	// paho.Client.Disconnect does not flush pending manual acks: shutdown
	// only resets the tracker (paho.golang issue #160 territory). A
	// CommitMarks call immediately followed by Close can otherwise race
	// the flush ticker, so the broker never sees the PUBACK and
	// redelivers a publish the pipeline already committed.
	ackDrainWait = 10 * ackFlushInterval
)

// Config is an mqtt block after config.MqttSource.Resolved.
type Config struct {
	Broker         *url.URL
	ClientID       string
	Topics         []string
	SessionExpiry  uint32
	ReceiveMaximum uint16
}

// Source reads MQTT 5 publishes at QoS 1 on a persistent session and
// acknowledges them only when the pipeline commits. The broker keeps whatever
// SQLFlow has not acknowledged and redelivers it after a crash or a
// reconnect: at-least-once, like the Kafka source.
type Source struct {
	cfg            Config
	channelBuffer  int
	connectTimeout time.Duration

	ledger     *ledger
	streamChan chan []core.Message
	done       chan struct{}
	closeOnce  sync.Once

	cm     *autopaho.ConnectionManager
	cancel context.CancelFunc

	// firstSubscribed is set once Start's synchronous subscribe has run.
	// Until then, onConnectionUp's async resubscribe stays out of the way:
	// otherwise the first connection could subscribe twice, and a rejected
	// filter caught by the async path would only be logged, not returned
	// from Start.
	firstSubscribed atomic.Bool

	// mu guards connected and connectedAt, which the connection callbacks
	// write while the engine reads Delivering.
	mu          sync.Mutex
	connected   bool
	connectedAt time.Time

	// sendMu lets Close close the stream while a late callback may still
	// send. autopaho's Done does not promise that paho has stopped, and a
	// send on a closed channel panics.
	sendMu sync.RWMutex
	closed bool

	logger *zap.Logger
}

type Option func(*Source)

func WithLogger(l *zap.Logger) Option {
	return func(s *Source) { s.logger = l.Named("source.mqtt") }
}

func WithChannelBuffer(n int) Option {
	return func(s *Source) { s.channelBuffer = n }
}

func WithConnectTimeout(d time.Duration) Option {
	return func(s *Source) { s.connectTimeout = d }
}

func NewSource(cfg Config, opts ...Option) (*Source, error) {
	if cfg.Broker == nil || cfg.ClientID == "" || len(cfg.Topics) == 0 {
		return nil, errs.New(errs.CodeSourceInvalid, "mqtt source: broker, client_id and topics are required")
	}
	s := &Source{
		cfg:            cfg,
		channelBuffer:  defaultChannelBuffer,
		connectTimeout: defaultConnectTimeout,
		ledger:         newLedger(),
		done:           make(chan struct{}),
		logger:         zap.NewNop(),
	}
	for _, opt := range opts {
		opt(s)
	}
	s.streamChan = make(chan []core.Message, s.channelBuffer)
	return s, nil
}

// Start connects and waits for the first connection, so an unreachable
// broker is reported here rather than after the pipeline is running.
func (s *Source) Start() error {
	ctx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel
	recvMax := s.cfg.ReceiveMaximum

	cm, err := autopaho.NewConnection(ctx, autopaho.ClientConfig{
		ServerUrls: []*url.URL{s.cfg.Broker},
		KeepAlive:  keepAliveSeconds,
		// Never clean: the session is where the broker keeps what SQLFlow
		// has not acknowledged.
		CleanStartOnInitialConnection: false,
		SessionExpiryInterval:         s.cfg.SessionExpiry,
		ConnectTimeout:                s.connectTimeout,
		ReconnectBackoff:              backoff,
		OnConnectionUp:                s.onConnectionUp,
		OnConnectionDown:              s.onConnectionDown,
		OnConnectError: func(err error) {
			s.logger.Warn("mqtt connect failed", zap.Error(err))
		},
		ConnectPacketBuilder: func(c *paho.Connect, _ *url.URL) (*paho.Connect, error) {
			if c.Properties == nil {
				c.Properties = &paho.ConnectProperties{}
			}
			c.Properties.ReceiveMaximum = &recvMax
			return c, nil
		},
		ClientConfig: paho.ClientConfig{
			ClientID:                   s.cfg.ClientID,
			EnableManualAcknowledgment: true,
			SendAcksInterval:           ackFlushInterval,
			OnPublishReceived:          []func(paho.PublishReceived) (bool, error){s.onPublish},
		},
	})
	if err != nil {
		cancel()
		return errs.Wrap(errs.CodeSourceInternal, err, "mqtt connection")
	}
	s.cm = cm

	s.logger.Info("connecting to mqtt broker",
		zap.String("broker", s.cfg.Broker.Redacted()),
		zap.String("client_id", s.cfg.ClientID),
		zap.Strings("topics", s.cfg.Topics),
	)
	actx, acancel := context.WithTimeout(ctx, s.connectTimeout)
	defer acancel()
	if err := cm.AwaitConnection(actx); err != nil {
		return errs.Wrap(errs.CodeSourceUnreachable, err, "mqtt broker %s", s.cfg.Broker.Redacted())
	}
	// AwaitConnection can return before OnConnectionUp runs, and the engine
	// reads Delivering as soon as Start returns.
	s.markConnected()

	// Subscribe here, synchronously, rather than leaving the first
	// connection to onConnectionUp's async path. A broker that refuses a
	// filter -- an ACL denial, for instance -- must fail Start, not leave
	// the source reporting Delivering() with nothing ever arriving.
	err = s.subscribeNow(ctx, cm)
	s.firstSubscribed.Store(true)
	if err != nil {
		return err
	}
	return nil
}

func (s *Source) markConnected() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.connected {
		s.connected = true
		s.connectedAt = time.Now()
	}
}

func backoff(attempt int) time.Duration {
	d := defaultReconnectDelay
	for i := 0; i < attempt && d < defaultMaxReconnectDelay; i++ {
		d *= 2
	}
	if d > defaultMaxReconnectDelay {
		d = defaultMaxReconnectDelay
	}
	return d
}

func (s *Source) onConnectionUp(cm *autopaho.ConnectionManager, ca *paho.Connack) {
	s.markConnected()
	s.logger.Info("mqtt connected", zap.Bool("session_present", ca.SessionPresent))

	if !s.firstSubscribed.Load() {
		// This is the first connection. Start's synchronous subscribeNow
		// handles it and reports a refusal as a failed Start; subscribing
		// again here would race that call and could double-subscribe.
		return
	}

	// A reconnect: autopaho forbids blocking here. A resumed session keeps
	// its subscriptions, and subscribing again costs nothing and covers a
	// broker that lost them.
	go s.subscribeAsync(cm)
}

// subscribeOptions is one subscription per configured topic filter.
func (s *Source) subscribeOptions() []paho.SubscribeOptions {
	subs := make([]paho.SubscribeOptions, 0, len(s.cfg.Topics))
	for _, t := range s.cfg.Topics {
		// Retain handling 2: a retained reading is the last value a device
		// sent, not a new event, and would enter the stream again on every
		// subscribe.
		subs = append(subs, paho.SubscribeOptions{Topic: t, QoS: 1, RetainHandling: 2})
	}
	return subs
}

// subscribeNow subscribes and waits for the SUBACK, so Start can fail when
// the broker refuses a filter rather than leaving the source connected with
// nothing ever arriving.
func (s *Source) subscribeNow(ctx context.Context, cm *autopaho.ConnectionManager) error {
	sctx, cancel := context.WithTimeout(ctx, s.connectTimeout)
	defer cancel()
	subs := s.subscribeOptions()
	sa, err := cm.Subscribe(sctx, &paho.Subscribe{Subscriptions: subs})
	if sa == nil {
		// No Suback at all: the transport failed, not a specific filter.
		return errs.Wrap(errs.CodeSourceUnreachable, err, "mqtt subscribe")
	}
	if topic, reason, refused := refusedSubscription(subs, sa.Reasons); refused {
		return errs.New(errs.CodeSourceInvalid, "mqtt subscribe: broker refused %s (reason 0x%02x)", topic, reason)
	}
	return nil
}

// refusedSubscription reports the first subscription the broker refused,
// paired with its topic filter. MQTT 5 section 3.9.3 marks any reason code
// 0x80 or above as a failure; sa.Reasons is parallel to subs, one code per
// requested filter.
func refusedSubscription(subs []paho.SubscribeOptions, reasons []byte) (topic string, reason byte, refused bool) {
	for i, code := range reasons {
		if code < 0x80 {
			continue
		}
		topic = "?"
		if i < len(subs) {
			topic = subs[i].Topic
		}
		return topic, code, true
	}
	return "", 0, false
}

// subscribeAsync resubscribes after a reconnect. autopaho forbids blocking
// in OnConnectionUp, so this runs on its own goroutine and can only log a
// failure -- Start already returned, so nothing is left to fail it.
func (s *Source) subscribeAsync(cm *autopaho.ConnectionManager) {
	ctx, cancel := context.WithTimeout(context.Background(), s.connectTimeout)
	defer cancel()
	if _, err := cm.Subscribe(ctx, &paho.Subscribe{Subscriptions: s.subscribeOptions()}); err != nil {
		select {
		case <-s.done:
			// Close is already tearing the connection down; losing this
			// race is expected, not a broker problem worth an Error log.
			s.logger.Warn("mqtt resubscribe failed during shutdown", zap.Error(err))
		default:
			s.logger.Error("mqtt resubscribe failed", zap.Error(err))
		}
	}
}

func (s *Source) onConnectionDown() bool {
	s.mu.Lock()
	s.connected = false
	s.mu.Unlock()
	s.ledger.disconnected()
	s.logger.Warn("mqtt connection down; the broker will redeliver what was not acknowledged")
	return true
}

// onPublish runs on paho's router goroutine. Blocking it on a full stream is
// the backpressure: the broker stops at receive_maximum unacknowledged
// publishes.
func (s *Source) onPublish(pr paho.PublishReceived) (bool, error) {
	seq := s.ledger.receive(pr.Packet, pr.Client)
	msg := core.Message{
		Value: pr.Packet.Payload,
		// No event time in an MQTT publish, so arrival is the event. The
		// reading's own ts is in the payload for SQL to use.
		EventAtNanos: time.Now().UnixNano(),
		Topic:        filterFor(s.cfg.Topics, pr.Packet.Topic),
		Partition:    0,
		Offset:       seq,
	}
	s.sendMu.RLock()
	defer s.sendMu.RUnlock()
	if s.closed {
		return true, nil
	}
	select {
	case s.streamChan <- []core.Message{msg}:
	case <-s.done:
	}
	return true, nil
}

func (s *Source) Stream() <-chan []core.Message { return s.streamChan }

// Commit does nothing. The engine prefers CommitMarks, and acknowledging
// everything received would acknowledge what the pipeline has not processed.
func (s *Source) Commit() error { return nil }

// CommitMarks acknowledges every held publish through the highest committed
// sequence. The pipeline processes in stream order, so the highest mark
// covers every publish before it.
func (s *Source) CommitMarks(marks *core.Marks) error {
	high := int64(-1)
	marks.Each(func(_ string, _ int32, m core.Mark) {
		if m.Offset > high {
			high = m.Offset
		}
	})
	if high < 0 {
		return nil
	}
	_, err := s.ledger.ackThrough(high)
	return err
}

// SeekTo does nothing. Stored offsets are an earlier process's sequence
// numbers, and the broker's session already holds the position.
func (s *Source) SeekTo(*core.Marks) error { return nil }

func (s *Source) EventTimeBasis() string { return core.EventBasisArrival }

// Delivering implements core.Deliverer: connected, and since when.
func (s *Source) Delivering() (time.Duration, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.connected {
		return 0, false
	}
	return time.Since(s.connectedAt), true
}

// Close disconnects without ending the session, so the broker keeps what
// SQLFlow has not acknowledged for the next process.
func (s *Source) Close() error {
	s.closeOnce.Do(func() {
		s.logger.Info("closing mqtt source")
		close(s.done)
		if s.cm != nil {
			// Give a CommitMarks call that just ran a chance to reach the
			// broker before the connection goes away. See ackDrainWait.
			time.Sleep(ackDrainWait)

			ctx, cancel := context.WithTimeout(context.Background(), disconnectTimeout)
			if err := s.cm.Disconnect(ctx); err != nil {
				s.logger.Warn("mqtt disconnect", zap.Error(err))
			}
			cancel()
			s.cancel()
			<-s.cm.Done()
		}
		s.mu.Lock()
		s.connected = false
		s.mu.Unlock()
		// A callback blocked on the stream returned when done closed, so
		// this lock does not wait on one. Callbacks after it see closed.
		s.sendMu.Lock()
		s.closed = true
		close(s.streamChan)
		s.sendMu.Unlock()
	})
	return nil
}
