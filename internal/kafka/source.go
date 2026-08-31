package kafka

import (
	"context"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
	"sync"
	"time"
)

type Source struct {
	client        *kgo.Client
	readTimeout   time.Duration
	channelBuffer int
	streamChan    chan [][]byte
	done          chan struct{}
	closeOnce     sync.Once

	logger *zap.Logger
}

type Option func(*Source)

func WithReadTimeout(timeout time.Duration) Option {
	return func(s *Source) {
		s.readTimeout = timeout
	}
}

func WithLogger(logger *zap.Logger) Option {
	return func(s *Source) {
		l := logger.Named("source.kafka")
		s.logger = l
	}
}

func WithChannelBuffer(size int) Option {
	return func(s *Source) {
		s.channelBuffer = size
	}
}

func NewSource(client *kgo.Client, opts ...Option) (*Source, error) {
	s := &Source{
		client:        client,
		readTimeout:   5 * time.Second,
		channelBuffer: 100,

		logger: zap.NewNop(),
	}

	for _, opt := range opts {
		opt(s)
	}

	s.streamChan = make(chan [][]byte, s.channelBuffer)
	s.done = make(chan struct{})

	return s, nil
}

func (k *Source) Start() error {
	k.logger.Info("starting franz-go consumer")
	return nil
}

func (k *Source) Close() error {
	k.logger.Info("closing franz-go consumer")
	k.closeOnce.Do(func() {
		close(k.done)
	})
	k.client.Close()
	return nil
}

func (k *Source) Commit() error {
	if err := k.client.CommitUncommittedOffsets(context.Background()); err != nil {
		k.logger.Error("failed to commit offsets", zap.Error(err))
		return err
	}
	return nil
}

func (k *Source) Stream() <-chan [][]byte {
	k.logger.Info("starting stream",
		zap.Int("channel_buffer", k.channelBuffer),
	)

	go func() {
		defer close(k.streamChan)

		var pollCount int
		var totalPoll, totalSend time.Duration
		defer func() {
			k.logger.Debug("poll loop totals",
				zap.Int("polls", pollCount),
				zap.Duration("total_poll_wait", totalPoll),
				zap.Duration("total_send_wait", totalSend),
			)
		}()

		for {
			p0 := time.Now()
			fetches := k.client.PollFetches(context.Background())
			pollDur := time.Since(p0)
			totalPoll += pollDur
			pollCount++
			if fetches.IsClientClosed() {
				return
			}
			k.logger.Debug("poll fetch",
				zap.Duration("poll", pollDur),
				zap.Int("records", fetches.NumRecords()),
			)

			if errs := fetches.Errors(); len(errs) > 0 {
				for _, e := range errs {
					k.logger.Error("fetch error",
						zap.String("topic", e.Topic),
						zap.Int32("partition", e.Partition),
						zap.Error(e.Err),
					)
				}
			}

			batch := make([][]byte, 0, fetches.NumRecords())
			fetches.EachRecord(func(r *kgo.Record) {
				batch = append(batch, r.Value)
			})

			if len(batch) == 0 {
				continue
			}

			s0 := time.Now()
			select {
			case k.streamChan <- batch:
				totalSend += time.Since(s0)
			case <-k.done:
				return
			}
		}
	}()
	return k.streamChan
}
