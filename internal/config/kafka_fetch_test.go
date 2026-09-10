package config

import (
	"strings"
	"testing"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

const kafkaFetchConfig = `
pipeline:
  batch_size: 10
  source:
    type: kafka
    kafka:
      brokers: [localhost:9092]
      group_id: g
      auto_offset_reset: earliest
      topics: [t]
      fetch:
        max_bytes: 52428800
        max_partition_bytes: 1048576
        prefetch: 4
  handler:
    type: handlers.InferredMemBatch
    sql: SELECT 1
  sink:
    type: noop
`

// The block is optional. Every field it carries is read as written.
func TestConfigTemplating_Load_KafkaFetchBlock(t *testing.T) {
	coverage.Covers(t, "config.templating")
	conf := loadString(t, kafkaFetchConfig)

	f := conf.Pipeline.Source.Kafka.Fetch
	assert.NotNil(t, f)
	assert.Equal(t, 52428800, f.MaxBytes)
	assert.Equal(t, 1048576, f.MaxPartitionBytes)
	assert.Equal(t, 4, f.Prefetch)

	resolved, err := f.Resolved()
	assert.NoError(t, err)
	assert.Equal(t, *f, resolved)
}

// An absent block, or an absent field, is the default. The two byte defaults
// are the values sources/init.go set before the block existed; only prefetch
// is new, and it is what bounds a backlog replay.
func TestConfigTemplating_Load_KafkaFetchAbsentIsTheDefault(t *testing.T) {
	coverage.Covers(t, "config.templating")

	var absent *KafkaFetch
	resolved, err := absent.Resolved()
	assert.NoError(t, err)
	assert.Equal(t, KafkaFetch{
		MaxBytes:          100 << 20,
		MaxPartitionBytes: 10 << 20,
		Prefetch:          DefaultKafkaFetchPrefetch,
	}, resolved)

	partial := &KafkaFetch{Prefetch: 1}
	resolved, err = partial.Resolved()
	assert.NoError(t, err)
	assert.Equal(t, 100<<20, resolved.MaxBytes)
	assert.Equal(t, 10<<20, resolved.MaxPartitionBytes)
	assert.Equal(t, 1, resolved.Prefetch)
}

// A bound that is zero, negative, inverted, or past int32 is a config error
// that names its field, not a fetch that franz-go silently clamps. A field
// left at zero is absent and takes its default, so the negative cases are
// what exercise "must be positive".
func TestConfigTemplating_Load_KafkaFetchRejectsBadBounds(t *testing.T) {
	coverage.Covers(t, "config.templating")
	for _, tt := range []struct {
		name string
		in   KafkaFetch
		want string
	}{
		{"negative prefetch", KafkaFetch{Prefetch: -1}, "prefetch"},
		{"negative max_bytes", KafkaFetch{MaxBytes: -5}, "max_bytes"},
		{"negative partition bytes", KafkaFetch{MaxPartitionBytes: -1}, "max_partition_bytes"},
		{"partition above broker", KafkaFetch{MaxBytes: 1 << 20, MaxPartitionBytes: 2 << 20}, "max_partition_bytes"},
		{"max_bytes past int32", KafkaFetch{MaxBytes: 1 << 31}, "max_bytes"},
		{"partition bytes past int32", KafkaFetch{MaxBytes: 1 << 31, MaxPartitionBytes: 1 << 31}, "max_bytes"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := tt.in.Resolved()
			assert.Error(t, err)
			assert.That(t, strings.Contains(err.Error(), tt.want))
		})
	}
}
