package core

import (
	"context"
	"github.com/apache/arrow-go/v18/arrow"
	"go.uber.org/zap"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/metric"
)

type Source interface {
	Start() error
	Stream() <-chan [][]byte
	Commit() error
	Close() error
}

type Sink interface {
	WriteTable(batch arrow.Table) error
	Flush() error
	Batch() (arrow.Table, error)
}

type Handler interface {
	Init(ctx context.Context) error
	Write(msg []byte) error
	Invoke(ctx context.Context) (arrow.Table, error)
}

type Stats struct {
	numMessagesConsumed      atomic.Int64
	StartTime                time.Time
	NumErrors                int
	totalThroughputPerSecond atomic.Uint64 // stored as float64 bits
}

func (s *Stats) SetThroughput(throughput float64) {
	s.totalThroughputPerSecond.Store(math.Float64bits(throughput))
}

func (s *Stats) GetThroughput() float64 {
	return math.Float64frombits(s.totalThroughputPerSecond.Load())
}

func (s *Stats) SetNumMessagesConsumed(num int64) {
	s.numMessagesConsumed.Store(num)
}

func (s *Stats) MessagesConsumed() int64 {
	return s.numMessagesConsumed.Load()
}

func (s *Stats) AddMessagesConsumed(n int64) {
	s.numMessagesConsumed.Add(n)
}

type ErrorPolicy int

const (
	PolicyRaise ErrorPolicy = iota
	PolicyIgnore
)

type PipelineErrorPolicies struct {
	Source ErrorPolicy
}

type Turbine struct {
	source        Source
	sink          Sink
	handler       Handler
	batchSize     int
	flushInterval time.Duration
	lock          *sync.Mutex
	running       bool
	stats         *Stats
	errorPolicy   PipelineErrorPolicies

	logger *zap.Logger

	// Metrics
	messageCounter         metric.Int64Counter
	errorCounter           metric.Int64Counter
	sourceReadLatency      metric.Float64Histogram
	sinkFlushLatency       metric.Float64Histogram
	sinkFlushNumRows       metric.Float64ObservableGauge
	sinkFlushCount         metric.Int64Counter
	batchProcessingLatency metric.Float64Histogram
}

func WithTurbineLogger(l *zap.Logger) TurbineOption {
	return func(t *Turbine) {
		t.logger = l
	}
}

type TurbineOption func(turbine *Turbine)

func NewTurbine(
	source Source,
	handler Handler,
	sink Sink,
	batchSize int,
	flushInterval time.Duration,
	lock *sync.Mutex,
	policy PipelineErrorPolicies,
	opts ...TurbineOption,
) *Turbine {
	t := &Turbine{
		source:        source,
		sink:          sink,
		handler:       handler,
		batchSize:     batchSize,
		flushInterval: flushInterval,
		lock:          lock,
		running:       true,
		stats: &Stats{
			StartTime: time.Now().UTC(),
		},
		errorPolicy: policy,

		logger: zap.NewNop(),
	}

	for _, opt := range opts {
		opt(t)
	}

	return t
}

func (t *Turbine) StatusLoop(ctx context.Context) error {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			t.logThroughput()
		case <-ctx.Done():
			return nil
		}
	}
}

func (t *Turbine) ConsumeLoop(ctx context.Context, maxMsgs int) (stats *Stats, err error) {
	t.logger.Info("consumer loop starting")

	if err := t.source.Start(); err != nil {
		return nil, err
	}
	defer func() {
		t.logger.Info("closing source from ConsumeLoop",
			zap.Bool("running", t.running),
			zap.String("here", "here"),
			zap.Error(err),
		)

		if err := t.source.Close(); err != nil {
			panic(err)
		}
	}()

	t.stats.StartTime = time.Now().UTC()
	t.stats.SetNumMessagesConsumed(0)
	if err := t.handler.Init(ctx); err != nil {
		return nil, err
	}

	numBatchMessages := 0
	totalConsumed := int64(0)
	hitMax := false

	stream := t.source.Stream()

	for t.running && !hitMax {
		// Receive a batch of raw messages from the source
		msgBatch, ok := <-stream
		if !ok {
			t.logger.Warn("stream channel closed")
			break
		}

		for _, raw := range msgBatch {
			if err := t.handler.Write(raw); err != nil {
				t.stats.NumErrors++
				t.logger.Error("error writing message", zap.Error(err))
				return nil, err
			}

			numBatchMessages++
			totalConsumed++

			if maxMsgs > 0 && totalConsumed >= int64(maxMsgs) {
				t.stats.SetNumMessagesConsumed(totalConsumed)
				t.logger.Info("max messages consumed, stopping consumer loop")
				hitMax = true
				break
			}

			if numBatchMessages == t.batchSize {
				t.stats.AddMessagesConsumed(int64(numBatchMessages))

				// Check for cancellation at batch boundaries
				select {
				case <-ctx.Done():
					t.logger.Warn("context done, stopping consumer loop")
					t.running = false
					t.logThroughput()
					return t.stats, nil
				default:
				}

				t.lock.Lock()
				batch, err := t.handler.Invoke(ctx)
				t.lock.Unlock()

				if err != nil {
					t.stats.NumErrors++
					t.logger.Error("error invoking handler", zap.Error(err))
					return nil, err
				}

				if err := t.sink.WriteTable(batch); err != nil {
					t.stats.NumErrors++
					t.logger.Error("error writing batch to sink", zap.Error(err))
					return nil, err
				}

				if err := t.flush(batch); err != nil {
					t.stats.NumErrors++
					t.logger.Error("error flushing sink", zap.Error(err))
					return nil, err
				}

				if err := t.source.Commit(); err != nil {
					t.logger.Error("error committing source", zap.Error(err))
					return nil, err
				}

				batch.Release()

				if err := t.handler.Init(ctx); err != nil {
					t.stats.NumErrors++
					t.logger.Error("error reinitializing handler", zap.Error(err))
					return nil, err
				}

				numBatchMessages = 0
			}
		}
	}

	t.logThroughput()
	return t.stats, nil
}

func (t *Turbine) logThroughput() {
	consumed := t.stats.MessagesConsumed()
	if duration := time.Since(t.stats.StartTime).Seconds(); duration > 0 {
		t.stats.SetThroughput(float64(consumed) / duration)
	} else {
		t.stats.SetThroughput(0)
	}

	throughput := t.stats.GetThroughput()
	if throughput > 0 {
		t.logger.Info("throughput",
			zap.Int64("messages_consumed", consumed),
			zap.Float64("total_throughput_per_second", throughput),
		)
	} else {
		t.logger.Info("no messages consumed, throughput is zero")
	}
}

func (t *Turbine) flush(batch arrow.Table) error {
	if err := t.sink.Flush(); err != nil {
		t.stats.NumErrors++
		t.logger.Error("flush error", zap.Error(err))
		return err
	}
	return nil
}
