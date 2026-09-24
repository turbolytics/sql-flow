package kafka

import (
	"context"
	"fmt"
	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/core"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"go.uber.org/zap"
	"sync"
	"sync/atomic"
	"time"
)

type Source struct {
	client        *kgo.Client
	readTimeout   time.Duration
	channelBuffer int
	streamChan    chan []core.Message
	done          chan struct{}
	closeOnce     sync.Once
	seeker        *OffsetSeeker
	partitions    *PartitionEvents
	// timestampType is the record timestamp type last observed, as
	// kgo.RecordAttrs.TimestampType reports it: 0 the producer's clock, 1
	// the broker's, -1 a pre-0.10.0 record carrying none. It decides which
	// basis a lag reading travels with, and the bundle reads it from
	// another goroutine while the poll loop writes it.
	timestampType atomic.Int32

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

// WithChannelBuffer sets how many fetches the source may hold ahead of the
// pipeline. It is the read-ahead bound; see Stream.
func WithChannelBuffer(size int) Option {
	return func(s *Source) {
		s.channelBuffer = size
	}
}

// WithPartitionEvents gives the source the relay registered on its client,
// so the pipeline can learn which partitions it holds.
func WithPartitionEvents(e *PartitionEvents) Option {
	return func(s *Source) {
		s.partitions = e
	}
}

// WithSeeker gives the source the seeker registered on its client, which is
// what SeekTo writes durable positions into. The seeker has to be built before
// the client, because it is a client option.
func WithSeeker(seeker *OffsetSeeker) Option {
	return func(s *Source) {
		s.seeker = seeker
	}
}

func NewSource(client *kgo.Client, opts ...Option) (*Source, error) {
	s := &Source{
		client:        client,
		readTimeout:   5 * time.Second,
		channelBuffer: config.DefaultKafkaFetchPrefetch,

		logger: zap.NewNop(),
	}

	for _, opt := range opts {
		opt(s)
	}

	s.streamChan = make(chan []core.Message, s.channelBuffer)
	s.done = make(chan struct{})

	return s, nil
}

// ChannelBuffer reports the read-ahead bound in fetches. The startup log
// prints it, and the source builder's test reads it to prove the configured
// value arrived here.
func (k *Source) ChannelBuffer() int {
	return k.channelBuffer
}

// OnPartitions reports the partitions this consumer holds, now and after
// every rebalance. It implements core.PartitionOwner; a source built without
// the relay reports nothing, and the pipeline then reports every partition
// it reads, as it did before ownership was tracked.
func (k *Source) OnPartitions(assigned, released, lost func(map[string][]int32)) {
	if k.partitions == nil {
		return
	}
	k.partitions.Subscribe(assigned, released, lost)
}

// Delivering implements core.Deliverer through the partition relay. A
// source built without the relay cannot tell, and reports delivering, which
// is what the engine assumed before ownership was tracked.
func (k *Source) Delivering() (time.Duration, bool) {
	if k.partitions == nil {
		return 0, true
	}
	return k.partitions.Delivering()
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
	// Before the client closes, because closing leaves the group and
	// leaving revokes every partition.
	if k.partitions != nil {
		k.partitions.Closing()
	}
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

// SeekTo resumes consumption from positions recorded in the pipeline's state
// database, so a restart picks up where the durable state left off rather
// than wherever the consumer group happens to sit.
//
// The positions are handed to the seeker, which applies them during the
// group's join. Committing them here instead does not work: that commit
// carries an empty member ID and Kafka refuses it unless the group is Empty,
// which it is not after a crash. See OffsetSeeker.
//
// Empty marks are a no-op rather than a seek to zero. "Nothing recorded" and
// "recorded position zero" are different facts: the first must leave
// auto_offset_reset in charge, and seeking to zero would silently replay an
// entire topic on a pipeline's first run against a fresh state file.
func (k *Source) SeekTo(marks *core.Marks) error {
	if marks == nil || marks.Empty() {
		return nil
	}

	if k.seeker == nil {
		return fmt.Errorf("seeking to stored offsets: source was built without an offset seeker")
	}
	k.seeker.SetMarks(marks)

	k.logger.Info("resuming from stored offsets", zap.Int("partitions", marks.Len()))
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

// Stream hands each fetch to the pipeline through a channel channelBuffer
// deep. That depth is the read-ahead bound: a full channel blocks the poll
// goroutine, and franz-go stops fetching from a broker whose last fetch is
// unpolled, so back-pressure reaches the wire. The source holds at most
// channelBuffer fetches here, one in hand, and one per broker inside the
// client. Before this was a setting the depth was 100, which with 10 MiB
// fetches held a 10M message backlog in memory in full.
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
					batch = append(batch, messageFrom(r, p.HighWatermark))
				}
				// Which clock stamped these records. message.timestamp.type
				// is topic-level config, so the last record of a fetch
				// speaks for the rest, and a lag reading can say whether it
				// came from a producer's clock or the broker's.
				//
				// This assumes one timestamp type across the consumer. A
				// topics: list mixing a CreateTime topic with a
				// LogAppendTime one reports whichever was fetched last, and
				// readings from the two are not comparable. Naming one basis
				// for a consumer that has two is a contract problem rather
				// than a code one.
				//
				// A pre-0.10.0 topic reports -1 and never overwrites a real
				// type. Its records carry no usable time and the floor skips
				// them anyway, so letting it clear the basis would drop the
				// valid readings of every topic beside it.
				if n := len(p.Records); n > 0 {
					k.observeTimestampType(p.Records[n-1].Attrs.TimestampType())
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

// messageFrom is the record-to-message conversion, lifted out of the fetch
// loop so a test can reach it without a broker.
//
// EventAtNanos is the record's own timestamp: a pipeline behind by an hour
// is handling records stamped an hour ago. Which clock set it depends on the
// topic, which is what EventTimeBasis reports.
func messageFrom(r *kgo.Record, highWatermark int64) core.Message {
	return core.Message{
		Value:         r.Value,
		EventAtNanos:  r.Timestamp.UnixNano(),
		Topic:         r.Topic,
		Partition:     r.Partition,
		Offset:        r.Offset,
		LeaderEpoch:   r.LeaderEpoch,
		HighWatermark: highWatermark,
	}
}

// observeTimestampType records which clock stamped a fetch's records, and
// is lifted out of the fetch loop so a test can reach it without a broker.
// A -1 never overwrites a real type; see the call site in Stream.
func (k *Source) observeTimestampType(ts int8) {
	if ts >= 0 {
		k.timestampType.Store(int32(ts))
	}
}

// EventTimeBasis names the clock behind Message.EventAtNanos.
//
// A Kafka record's timestamp is the producer's own clock unless the topic
// sets message.timestamp.type to LogAppendTime, and CreateTime is the
// default. The two measure different things -- one is only as good as the
// fleet's clocks, the other is one broker's -- so a lag reading has to say
// which it came from rather than claiming "the broker stamped it".
//
// Before the first fetch this reports the Kafka default. A consumer reading
// only pre-0.10.0 topics reports it too, and reports no lag at all: those
// records carry no usable time, so no reading is ever taken from them.
func (s *Source) EventTimeBasis() string {
	if s.timestampType.Load() == 1 {
		return core.EventBasisKafkaLogAppendTime
	}
	return core.EventBasisKafkaCreateTime
}
