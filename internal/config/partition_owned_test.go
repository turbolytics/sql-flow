package config

import (
	"strings"
	"testing"

	"github.com/zeebo/assert"
)

// The start wipe clears the pipeline's stored positions, so a window that is
// not partition_owned beside one that is would be fed its rows twice.
func TestConfig_PartitionOwnedMustCoverEveryWindow(t *testing.T) {
	owned := Window{PartitionOwned: true, Sink: Sink{Type: "postgres",
		Postgres: &PostgresSink{Mode: "upsert", Key: []string{"bucket", "kafka_partition"}}}}
	c := &Conf{Tables: &Tables{SQL: []TableSQL{{Name: "a", Window: &owned}}}}
	c.Pipeline.Source = Source{Type: "kafka", Kafka: &KafkaSource{Topics: []string{"t"}}}
	assert.NoError(t, c.CheckPartitionOwned())

	c.Tables.SQL = append(c.Tables.SQL, TableSQL{Name: "b", Window: &Window{}})
	err := c.CheckPartitionOwned()
	assert.Error(t, err)
	assert.That(t, strings.Contains(err.Error(), "every window"))

	assert.Equal(t, []string{"a"}, (&Conf{Tables: &Tables{SQL: []TableSQL{{Name: "a", Window: &owned}}}}).PartitionOwnedTables())
	assert.Equal(t, 0, len((&Conf{}).PartitionOwnedTables()))
}

// The upsert key must include the window's time_column. Without it the
// Postgres PK (same columns) collapses every bucket for a (customer, meter,
// partition) into one row -- only the latest minute survives, a silent
// undercount. validate exists to catch exactly that.
func TestConfig_PartitionOwnedKeyMustIncludeTheBucketColumn(t *testing.T) {
	mk := func(key []string) *Conf {
		c := &Conf{Tables: &Tables{SQL: []TableSQL{{Name: "a", Window: &Window{
			TimeColumn:     "minute",
			PartitionOwned: true,
			Sink:           Sink{Type: "postgres", Postgres: &PostgresSink{Mode: "upsert", Key: key}},
		}}}}}
		c.Pipeline.Source = Source{Type: "kafka", Kafka: &KafkaSource{Topics: []string{"t"}}}
		return c
	}
	// Missing the time_column: refused.
	err := mk([]string{"customer", "meter", "kafka_partition"}).CheckPartitionOwned()
	assert.Error(t, err)
	assert.That(t, strings.Contains(err.Error(), "minute"))
	// Present: accepted.
	assert.NoError(t, mk([]string{"minute", "customer", "meter", "kafka_partition"}).CheckPartitionOwned())
}

// partition_owned promises exact, reproducible counts, which a continue-on-
// error policy breaks: a batch whose handler fails is dropped non-
// deterministically, and its offset records outlive the rolled-back rows, so
// a fresh owner replaying them counts records the original dropped. Exact
// counting requires RAISE -- stop and fix, do not silently drop a bill.
func TestConfig_PartitionOwnedRefusesAContinueOnErrorPolicy(t *testing.T) {
	mk := func(policy string) *Conf {
		c := &Conf{Tables: &Tables{SQL: []TableSQL{{Name: "a", Window: &Window{
			TimeColumn:     "minute",
			PartitionOwned: true,
			Sink: Sink{Type: "postgres", Postgres: &PostgresSink{Mode: "upsert",
				Key: []string{"minute", "kafka_partition"}}},
		}}}}}
		c.Pipeline.Source = Source{Type: "kafka", Kafka: &KafkaSource{Topics: []string{"t"}}}
		if policy != "" {
			c.Pipeline.OnError = &OnError{Policy: policy}
		}
		return c
	}
	assert.NoError(t, mk("").CheckPartitionOwned())      // default (RAISE)
	assert.NoError(t, mk("RAISE").CheckPartitionOwned()) // explicit RAISE
	for _, p := range []string{"IGNORE", "DLQ"} {
		err := mk(p).CheckPartitionOwned()
		assert.Error(t, err)
		assert.That(t, strings.Contains(err.Error(), "RAISE"))
	}
}
