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
	WriteTable(data any) error
	Flush() error
	Batch() any
}

type Message interface {
	Value() []byte
}

type Handler interface {
	Init() error
	Write(msg []byte) error
	Invoke() (*arrow.Table, error)
}

type Stats struct {
	StartTime                time.Time
	NumMessagesConsumed      int
	NumErrors                int
	TotalThroughputPerSecond float64
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
	stats         Stats
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
		stats: Stats{
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

func (t *Turbine) ConsumeLoop(ctx context.Context, maxMsgs int) (*Stats, error) {
	log.Println("consumer loop starting")

	if err := t.source.Start(); err != nil {
		return nil, err
	}
	defer func() {
		if err := t.source.Close(); err != nil {
			panic(err)
		}
	}()

	t.stats.StartTime = time.Now().UTC()
	t.stats.NumMessagesConsumed = 0
	if err := t.handler.Init(); err != nil {
		return nil, err
	}

	numBatchMessages := 0

	for t.running {
		select {
		case <-ctx.Done():
			t.running = false
			break
		case msg := <-t.source.Stream():
			if msg == nil {
				continue
			}

			t.stats.NumMessagesConsumed++
			if numBatchMessages == 0 {
			}

			/*
				var msgObj string
				if err := json.Unmarshal(msg.Value(), &msgObj); err != nil {
					t.stats.NumErrors++
					log.Printf("error decoding message: %v", err)
					if t.errorPolicy.Source == PolicyRaise {
						return nil, err
					}
					continue
				}
			*/
			if err := t.handler.Write(msg.Value()); err != nil {
				t.stats.NumErrors++
				log.Printf("error writing message: %v", err)
				return nil, err
			}

			numBatchMessages++
			if maxMsgs > 0 && t.stats.NumMessagesConsumed >= maxMsgs {
				t.running = false
				break
			}

			if numBatchMessages == t.batchSize {
				t.lock.Lock()
				batch, _ := t.handler.Invoke()
				t.lock.Unlock()

				if err := t.sink.WriteTable(batch); err != nil {
					t.stats.NumErrors++
					return nil, err
				}
				t.flush()
				if err := t.source.Commit(); err != nil {
					return nil, err
				}
				t.handler.Init()
				numBatchMessages = 0
			}
		}
	}

	duration := time.Since(t.stats.StartTime).Seconds()
	if duration > 0 {
		t.stats.TotalThroughputPerSecond = float64(t.stats.NumMessagesConsumed) / duration
	}

	log.Printf("consumer loop ending: total messages/sec = %f", t.stats.TotalThroughputPerSecond)
	return &t.stats, nil
}

func (t *Turbine) flush() {
	start := time.Now()
	if err := t.sink.Flush(); err != nil {
		t.stats.NumErrors++
		rows := t.sink.Batch()
		log.Printf("flush error: %v, rows: %+v", err, rows)
		return
	}
	log.Printf("flushed sink in %v", time.Since(start))
}
