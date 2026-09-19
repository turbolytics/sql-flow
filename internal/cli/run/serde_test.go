package run

import (
	"strings"
	"testing"

	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/turbolytics/sql-flow/internal/errs"
	"github.com/zeebo/assert"
)

func avroConf() *config.Conf {
	return &config.Conf{Pipeline: config.Pipeline{
		SchemaRegistry: &config.SchemaRegistry{URL: "http://localhost:8081"},
		Source: config.Source{Type: "kafka", Kafka: &config.KafkaSource{
			Topics: []string{"orders"},
			Value:  &config.KafkaValue{Format: config.FormatAvro},
		}},
		Handler: config.Handler{Type: "handlers.InferredMemBatch", SQL: "SELECT * FROM batch"},
		Sink: config.Sink{Type: "kafka", Kafka: &config.KafkaSink{
			Topic: "orders-enriched",
			Value: &config.KafkaValue{Format: config.FormatAvro},
		}},
	}}
}

// A config that skipped validate is refused at start with the exit code a
// config error gets, and the message names the key.
func TestSchemaRegistryRuleIsRefusedAtStartup(t *testing.T) {
	coverage.Covers(t, "cli.invocation")
	conf := avroConf()
	conf.Pipeline.SchemaRegistry = nil

	err := checkSchemaRegistry(conf)
	assert.Error(t, err)
	assert.Equal(t, errs.CodeConfigInvalid, errs.CodeOf(err))
	assert.Equal(t, errs.ExitUserError, errs.ExitCode(err))
	assert.That(t, strings.Contains(err.Error(), "pipeline.source.kafka.value.format"))
	assert.That(t, strings.Contains(err.Error(), "needs pipeline.schema_registry"))
}

// Until #300 and #304 land, a registry format is refused rather than read
// or written as JSON. The message names the issue that lifts the refusal.
func TestUnshippedFormatIsRefusedAtStartup(t *testing.T) {
	coverage.Covers(t, "cli.invocation")
	conf := avroConf()
	err := checkSchemaRegistry(conf)
	assert.Error(t, err)
	assert.Equal(t, errs.CodeConfigInvalid, errs.CodeOf(err))
	assert.That(t, strings.Contains(err.Error(), "pipeline.source.kafka.value.format"))
	assert.That(t, strings.Contains(err.Error(), "#300"))

	conf.Pipeline.Source.Kafka.Value = nil
	err = checkSchemaRegistry(conf)
	assert.Error(t, err)
	assert.That(t, strings.Contains(err.Error(), "pipeline.sink.kafka.value.format"))
	assert.That(t, strings.Contains(err.Error(), "#304"))

	conf.Pipeline.Sink.Kafka.Value = &config.KafkaValue{Format: config.FormatJSON}
	assert.NoError(t, checkSchemaRegistry(conf))
}

// The gate covers the DLQ and a window's sink, which are sinks too.
func TestUnshippedFormatOnEverySinkIsRefused(t *testing.T) {
	coverage.Covers(t, "cli.invocation")
	conf := avroConf()
	conf.Pipeline.Source.Kafka.Value = nil
	conf.Pipeline.Sink = config.Sink{Type: "console"}
	conf.Pipeline.OnError = &config.OnError{Policy: "DLQ", DLQ: &config.Sink{Type: "kafka", Kafka: &config.KafkaSink{
		Topic: "dead", Value: &config.KafkaValue{Format: config.FormatJSONSchema},
	}}}
	err := checkSchemaRegistry(conf)
	assert.Error(t, err)
	assert.That(t, strings.Contains(err.Error(), "pipeline.on_error.dlq.kafka.value.format"))

	conf.Pipeline.OnError = nil
	conf.Tables = &config.Tables{SQL: []config.TableSQL{{Name: "agg", Window: &config.Window{
		Sink: config.Sink{Type: "kafka", Kafka: &config.KafkaSink{
			Topic: "agg", Value: &config.KafkaValue{Format: config.FormatAvro},
		}},
	}}}}
	err = checkSchemaRegistry(conf)
	assert.Error(t, err)
	assert.That(t, strings.Contains(err.Error(), "tables.sql.0.window.sink.kafka.value.format"))
}
