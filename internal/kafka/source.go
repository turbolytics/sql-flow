package kafka

import (
	"context"
	"fmt"
	"github.com/turbolytics/sql-flow/internal/core"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"go.uber.org/zap"
	"sync"
	"time"
)

type Source struct {
	client        *kgo.Client
	readTimeout   time.Duration
	channelBuffer int
	streamChan    chan []core.Message
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

	s.streamChan = make(chan []core.Message, s.channelBuffer)
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

// CommitMarks commits exactly the positions the pipeline has finished with.
//
// Commit above commits everything this source has fetched, and the poll
// goroutine fetches well ahead of the pipeline: after one 20,000-message
// batch it had committed offset 70,086. A crash then lost the difference
// with the consumer group showing no lag. Kafka commits the next offset to
// read, so a mark at offset N commits N+1.
func (k *Source) CommitMarks(marks *core.Marks) error {
	if marks == nil || marks.Empty() {
		return nil
	}
	offsets := make(map[string]map[int32]kgo.EpochOffset, marks.Len())
	marks.Each(func(topic string, partition int32, m core.Mark) {
		if offsets[topic] == nil {
			offsets[topic] = map[int32]kgo.EpochOffset{}
		}
		offsets[topic][partition] = kgo.EpochOffset{Epoch: m.LeaderEpoch, Offset: m.Offset + 1}
	})

	ctx, cancel := context.WithTimeout(context.Background(), commitTimeout)
	defer cancel()

	var commitErr error
	k.client.CommitOffsetsSync(ctx, offsets, func(_ *kgo.Client, _ *kmsg.OffsetCommitRequest, resp *kmsg.OffsetCommitResponse, err error) {
		if err != nil {
			commitErr = err
			return
		}
		// The request can succeed while a partition inside it is refused.
		for _, t := range resp.Topics {
			for _, p := range t.Partitions {
				if err := kerr.ErrorForCode(p.ErrorCode); err != nil {
					commitErr = fmt.Errorf("commit %s[%d]: %w", t.Topic, p.Partition, err)
					return
				}
			}
		}
	})
	if commitErr != nil {
		k.logger.Error("failed to commit offsets", zap.Error(commitErr))
	}
	return commitErr
}

// commitTimeout bounds a synchronous commit; the pipeline blocks on it.
const commitTimeout = 30 * time.Second

func (k *Source) Stream() <-chan []core.Message {
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

			batch := make([]core.Message, 0, fetches.NumRecords())
			fetches.EachPartition(func(p kgo.FetchTopicPartition) {
				for _, r := range p.Records {
					batch = append(batch, core.Message{
						Value:         r.Value,
						Topic:         r.Topic,
						Partition:     r.Partition,
						Offset:        r.Offset,
						LeaderEpoch:   r.LeaderEpoch,
						HighWatermark: p.HighWatermark,
					})
				}
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
