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
