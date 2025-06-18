package core

import (
	"context"
	"github.com/apache/arrow-go/v18/arrow"
	"go.uber.org/zap"
	"log"
	"sync"
	"time"

	"go.opentelemetry.io/otel/metric"
)

type Source interface {
	Start() error
	Stream() <-chan Message
	Commit() error
	Close() error
}

type Sink interface {
	WriteTable(batch arrow.Table) error
	Flush() error
	Batch() (arrow.Table, error)
}

type Message interface {
	Value() []byte
}

type Handler interface {
	Init(ctx context.Context) error
	Write(msg []byte) error
	Invoke(ctx context.Context) (arrow.Table, error)
}

type Stats struct {
	mu sync.Mutex

	numMessagesConsumed      int
	StartTime                time.Time
	NumErrors                int
	TotalThroughputPerSecond float64
}

func (s *Stats) SetThroughput(throughput float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.TotalThroughputPerSecond = throughput
}

func (s *Stats) GetThroughput() float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.TotalThroughputPerSecond
}

func (s *Stats) SetNumMessagesConsumed(num int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.numMessagesConsumed = num
}

func (s *Stats) MessagesConsumed() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.numMessagesConsumed
}

func (s *Stats) IncrementMessagesConsumed() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.numMessagesConsumed++
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
	log.Println("consumer loop starting")

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

	stream := t.source.Stream()

	for t.running {
		// t.logger.Debug("top of loop", zap.Bool("running", t.running))
		select {
		case <-ctx.Done():
			t.logger.Warn("context done, stopping consumer loop")
			t.running = false
			break
		case msg, ok := <-stream:
			if !ok {
				t.logger.Warn("stream channel closed")
				t.running = false
				break
			}

			if msg == nil {
				t.logger.Debug("received nil message")
				continue
			}

			t.stats.IncrementMessagesConsumed()

			if err := t.handler.Write(msg.Value()); err != nil {
				t.stats.NumErrors++
				t.logger.Error("error writing message", zap.Error(err))
				return nil, err
			}

			numBatchMessages++
			if maxMsgs > 0 && t.stats.MessagesConsumed() >= maxMsgs {
				t.logger.Info("max messages consumed, stopping consumer loop")
				t.running = false
				break
			}

			if numBatchMessages == t.batchSize {
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
	if duration := time.Since(t.stats.StartTime).Seconds(); duration > 0 {
		t.stats.SetThroughput(float64(t.stats.MessagesConsumed()) / duration)
	} else {
		t.stats.SetThroughput(0)
	}

	if t.stats.TotalThroughputPerSecond > 0 {
		t.logger.Info("throughput",
			zap.Int("messages_consumed", t.stats.MessagesConsumed()),
			zap.Float64("total_throughput_per_second", t.stats.GetThroughput()),
		)
	} else {
		t.logger.Info("no messages consumed, throughput is zero")
	}
}

func (t *Turbine) flush(batch arrow.Table) error {
	start := time.Now()
	if err := t.sink.Flush(); err != nil {
		t.stats.NumErrors++
		rows, _ := t.sink.Batch()
		log.Printf("flush error: %v, rows: %+v", err, rows)
		return err
	}
	log.Printf("flushed sink with %d rows in %v", batch.NumRows(), time.Since(start))
	return nil
}
