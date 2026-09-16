package config

import (
	"fmt"
	"testing"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

// avroConfigYAML is the spec's example: a registry, an Avro source, and an
// Avro sink writing against a registered version. %s is the version.
const avroConfigYAML = `pipeline:
  schema_registry:
    url: http://localhost:8081
    auth:
      username: u
      password: p
    ssl:
      ca_location: /etc/certs/ca.pem
  source:
    type: kafka
    kafka:
      brokers: ["localhost:9092"]
      group_id: g
      auto_offset_reset: earliest
      topics: ["orders"]
      value:
        format: avro
  handler:
    type: handlers.InferredMemBatch
    sql: SELECT * FROM batch
  sink:
    type: kafka
    kafka:
      brokers: ["localhost:9092"]
      topic: orders-enriched
      value:
        format: avro
        subject: orders-enriched-value
        schema:
          version: %s
`

func decodeAvroConfig(t *testing.T, version string) Conf {
	t.Helper()
	var conf Conf
	assert.NoError(t, decodeStrict([]byte(fmt.Sprintf(avroConfigYAML, version)), &conf))
	return conf
}

// A config declaring format: avro parses into the new blocks, and the
// version is a string whether the YAML wrote latest or a number.
func TestConfigSchemaRegistry_ValueBlocksParse(t *testing.T) {
	coverage.Covers(t, "config.validation")
	conf := decodeAvroConfig(t, "latest")

	reg := conf.Pipeline.SchemaRegistry
	assert.NotNil(t, reg)
	assert.Equal(t, "http://localhost:8081", reg.URL)
	assert.Equal(t, "u", reg.Auth.Username)
	assert.Equal(t, "p", reg.Auth.Password)
	assert.Equal(t, "", reg.Auth.BearerToken)
	assert.Equal(t, "/etc/certs/ca.pem", reg.SSL.CALocation)

	assert.Equal(t, "avro", conf.Pipeline.Source.Kafka.Value.Format)
	assert.Equal(t, "", conf.Pipeline.Source.Kafka.Value.Subject)
	assert.Nil(t, conf.Pipeline.Source.Kafka.Value.Schema)

	sink := conf.Pipeline.Sink.Kafka.Value
	assert.Equal(t, "avro", sink.Format)
	assert.Equal(t, "orders-enriched-value", sink.Subject)
	assert.Equal(t, "latest", sink.Schema.Version)

	conf = decodeAvroConfig(t, "3")
	assert.Equal(t, "3", conf.Pipeline.Sink.Kafka.Value.Schema.Version)
}

// The defaults, each from a nil block: json, not registry-backed, and a
// subject of <topic>-value.
func TestConfigSchemaRegistry_Defaults(t *testing.T) {
	coverage.Covers(t, "config.validation")
	var v *KafkaValue
	assert.Equal(t, FormatJSON, v.ResolvedFormat())
	assert.False(t, v.RegistryBacked())

	v = &KafkaValue{}
	assert.Equal(t, FormatJSON, v.ResolvedFormat())
	assert.False(t, v.RegistryBacked())

	v = &KafkaValue{Format: FormatJSONSchema}
	assert.Equal(t, FormatJSONSchema, v.ResolvedFormat())
	assert.True(t, v.RegistryBacked())

	v = &KafkaValue{Format: FormatAvro}
	assert.True(t, v.RegistryBacked())

	var s *KafkaSink
	assert.Equal(t, "", s.ResolvedSubject())
	s = &KafkaSink{Topic: "orders-enriched"}
	assert.Equal(t, "orders-enriched-value", s.ResolvedSubject())
	s.Value = &KafkaValue{Subject: "orders"}
	assert.Equal(t, "orders", s.ResolvedSubject())
}
