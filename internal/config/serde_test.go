package config

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/turbolytics/sql-flow/internal/errs"
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

// avroConf is the spec's example as a struct: a registry, an Avro source on
// one topic under InferredMemBatch, and an Avro sink. Every rule test
// starts from it and breaks one thing.
func avroConf() Conf {
	return Conf{Pipeline: Pipeline{
		SchemaRegistry: &SchemaRegistry{URL: "http://localhost:8081"},
		Source: Source{Type: "kafka", Kafka: &KafkaSource{
			Topics: []string{"orders"},
			Value:  &KafkaValue{Format: FormatAvro},
		}},
		Handler: Handler{Type: "handlers.InferredMemBatch", SQL: "SELECT * FROM batch"},
		Sink: Sink{Type: "kafka", Kafka: &KafkaSink{
			Topic: "orders-enriched",
			Value: &KafkaValue{Format: FormatAvro, Schema: &KafkaValueSchema{Version: "latest"}},
		}},
	}}
}

func keys(vs []Violation) []string {
	out := make([]string, 0, len(vs))
	for _, v := range vs {
		out = append(out, v.Key())
	}
	return out
}

// The spec's example passes, and so does a config that names nothing.
func TestConfigSchemaRegistry_ValidConfigsHaveNoViolations(t *testing.T) {
	coverage.Covers(t, "config.validation")
	conf := avroConf()
	assert.Equal(t, 0, len(conf.CheckSchemaRegistry()))

	conf.Pipeline.Source.Kafka.Value.Format = FormatJSONSchema
	conf.Pipeline.Sink.Kafka.Value.Schema.Version = "3"
	assert.Equal(t, 0, len(conf.CheckSchemaRegistry()))

	conf.Pipeline.SchemaRegistry.Auth = &SchemaRegistryAuth{BearerToken: "t"}
	assert.Equal(t, 0, len(conf.CheckSchemaRegistry()))

	plain := Conf{Pipeline: Pipeline{
		Source:  Source{Type: "kafka", Kafka: &KafkaSource{Topics: []string{"a", "b"}}},
		Handler: Handler{Type: "handlers.InferredDiskBatch"},
		Sink:    Sink{Type: "console"},
	}}
	assert.Equal(t, 0, len(plain.CheckSchemaRegistry()))
}

// Each rule fails with the key it is about, and only that key.
func TestConfigSchemaRegistry_EachRuleNamesItsKey(t *testing.T) {
	coverage.Covers(t, "config.validation")
	cases := []struct {
		name   string
		mutate func(c *Conf)
		want   []string
		says   string
	}{
		{
			name:   "a registry format needs the registry block",
			mutate: func(c *Conf) { c.Pipeline.SchemaRegistry = nil },
			want:   []string{"pipeline.source.kafka.value.format", "pipeline.sink.kafka.value.format"},
			says:   "needs pipeline.schema_registry",
		},
		{
			name:   "a registry-backed source reads one topic",
			mutate: func(c *Conf) { c.Pipeline.Source.Kafka.Topics = []string{"a", "b"} },
			want:   []string{"pipeline.source.kafka.topics"},
			says:   "exactly one topic",
		},
		{
			name:   "a registry-backed source needs InferredMemBatch",
			mutate: func(c *Conf) { c.Pipeline.Handler.Type = "handlers.InferredDiskBatch" },
			want:   []string{"pipeline.handler.type"},
			says:   "handlers.InferredMemBatch",
		},
		{
			name:   "subject is a sink key",
			mutate: func(c *Conf) { c.Pipeline.Source.Kafka.Value.Subject = "orders-value" },
			want:   []string{"pipeline.source.kafka.value.subject"},
			says:   "sink",
		},
		{
			name:   "schema is a sink key",
			mutate: func(c *Conf) { c.Pipeline.Source.Kafka.Value.Schema = &KafkaValueSchema{Version: "latest"} },
			want:   []string{"pipeline.source.kafka.value.schema"},
			says:   "sink",
		},
		{
			name:   "subject needs a registry format",
			mutate: func(c *Conf) { c.Pipeline.Sink.Kafka.Value = &KafkaValue{Subject: "x"} },
			want:   []string{"pipeline.sink.kafka.value.subject"},
			says:   "json",
		},
		{
			name: "schema needs a registry format",
			mutate: func(c *Conf) {
				c.Pipeline.Sink.Kafka.Value = &KafkaValue{Format: FormatJSON, Schema: &KafkaValueSchema{Version: "1"}}
			},
			want: []string{"pipeline.sink.kafka.value.schema"},
			says: "json",
		},
		{
			name:   "version zero",
			mutate: func(c *Conf) { c.Pipeline.Sink.Kafka.Value.Schema.Version = "0" },
			want:   []string{"pipeline.sink.kafka.value.schema.version"},
			says:   "latest or a version number of at least 1",
		},
		{
			name:   "version that is a word",
			mutate: func(c *Conf) { c.Pipeline.Sink.Kafka.Value.Schema.Version = "newest" },
			want:   []string{"pipeline.sink.kafka.value.schema.version"},
			says:   "latest or a version number of at least 1",
		},
		{
			name:   "an unknown format",
			mutate: func(c *Conf) { c.Pipeline.Source.Kafka.Value.Format = "protobuf" },
			want:   []string{"pipeline.source.kafka.value.format"},
			says:   "json, json_schema or avro",
		},
		{
			name: "an unknown format on a sink",
			mutate: func(c *Conf) {
				c.Pipeline.Sink.Kafka.Value = &KafkaValue{Format: "protobuf", Subject: "x"}
			},
			want: []string{"pipeline.sink.kafka.value.format", "pipeline.sink.kafka.value.subject"},
			says: "json, json_schema or avro",
		},
		{
			name: "auth with both credentials",
			mutate: func(c *Conf) {
				c.Pipeline.SchemaRegistry.Auth = &SchemaRegistryAuth{Username: "u", Password: "p", BearerToken: "t"}
			},
			want: []string{"pipeline.schema_registry.auth"},
			says: "not both",
		},
		{
			name:   "auth with a username and no password",
			mutate: func(c *Conf) { c.Pipeline.SchemaRegistry.Auth = &SchemaRegistryAuth{Username: "u"} },
			want:   []string{"pipeline.schema_registry.auth"},
			says:   "both username and password",
		},
		{
			name:   "auth with nothing in it",
			mutate: func(c *Conf) { c.Pipeline.SchemaRegistry.Auth = &SchemaRegistryAuth{} },
			want:   []string{"pipeline.schema_registry.auth"},
			says:   "empty",
		},
		{
			name:   "a registry with no url",
			mutate: func(c *Conf) { c.Pipeline.SchemaRegistry.URL = "" },
			want:   []string{"pipeline.schema_registry.url"},
			says:   "url",
		},
		{
			name: "a dlq value block without the registry",
			mutate: func(c *Conf) {
				c.Pipeline.SchemaRegistry = nil
				c.Pipeline.Source.Kafka.Value = nil
				c.Pipeline.Sink = Sink{Type: "console"}
				c.Pipeline.OnError = &OnError{Policy: "DLQ", DLQ: &Sink{Type: "kafka", Kafka: &KafkaSink{
					Topic: "dead", Value: &KafkaValue{Format: FormatAvro},
				}}}
			},
			want: []string{"pipeline.on_error.dlq.kafka.value.format"},
			says: "needs pipeline.schema_registry",
		},
		{
			name: "a window sink value block without the registry",
			mutate: func(c *Conf) {
				c.Pipeline.SchemaRegistry = nil
				c.Pipeline.Source.Kafka.Value = nil
				c.Pipeline.Sink = Sink{Type: "console"}
				c.Tables = &Tables{SQL: []TableSQL{
					{Name: "plain"},
					{Name: "agg", Window: &Window{Sink: Sink{Type: "kafka", Kafka: &KafkaSink{
						Topic: "agg", Value: &KafkaValue{Format: FormatJSONSchema},
					}}}},
				}}
			},
			want: []string{"tables.sql.1.window.sink.kafka.value.format"},
			says: "needs pipeline.schema_registry",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			conf := avroConf()
			tc.mutate(&conf)
			got := conf.CheckSchemaRegistry()
			assert.Equal(t, tc.want, keys(got))
			assert.That(t, strings.Contains(got[0].Message, tc.says))
			for _, v := range got {
				assert.Equal(t, errs.CodeConfigInvalid, v.Code)
			}
		})
	}
}

// A DLQ or window sink builds its violation paths from a base of three or
// more elements. A naive append from that base can leave spare capacity,
// so two violations built from the same base share a backing array and
// the second write clobbers the first. Both keys, and both violations,
// must survive.
func TestConfigSchemaRegistry_TwoViolationsOnOneSinkKeepTheirKeys(t *testing.T) {
	coverage.Covers(t, "config.validation")

	dlq := avroConf()
	dlq.Pipeline.Source.Kafka.Value = nil
	dlq.Pipeline.Sink = Sink{Type: "console"}
	dlq.Pipeline.OnError = &OnError{Policy: "DLQ", DLQ: &Sink{Type: "kafka", Kafka: &KafkaSink{
		Topic: "dead", Value: &KafkaValue{Subject: "x", Schema: &KafkaValueSchema{Version: "1"}},
	}}}
	assert.Equal(t, []string{
		"pipeline.on_error.dlq.kafka.value.subject",
		"pipeline.on_error.dlq.kafka.value.schema",
	}, keys(dlq.CheckSchemaRegistry()))

	win := avroConf()
	win.Pipeline.Source.Kafka.Value = nil
	win.Pipeline.Sink = Sink{Type: "console"}
	win.Tables = &Tables{SQL: []TableSQL{
		{Name: "agg", Window: &Window{Sink: Sink{Type: "kafka", Kafka: &KafkaSink{
			Topic: "agg", Value: &KafkaValue{Subject: "x", Schema: &KafkaValueSchema{Version: "1"}},
		}}}},
	}}
	assert.Equal(t, []string{
		"tables.sql.0.window.sink.kafka.value.subject",
		"tables.sql.0.window.sink.kafka.value.schema",
	}, keys(win.CheckSchemaRegistry()))
}

// The handler adds kafka_topic, kafka_partition and kafka_offset itself. A
// reader schema that defines one would put two columns of that name in
// batch. #300 calls this where it fetches the schema.
func TestConfigSchemaRegistry_ReservedSourceColumns(t *testing.T) {
	coverage.Covers(t, "config.validation")
	assert.Equal(t, []string{"kafka_topic", "kafka_partition", "kafka_offset"}, ReservedSourceColumns)
	for _, name := range ReservedSourceColumns {
		assert.True(t, IsReservedSourceColumn(name))
	}
	assert.False(t, IsReservedSourceColumn("topic"))
	assert.False(t, IsReservedSourceColumn("Kafka_Topic"))
}

// The table and the ordered list name the same formats, and a rule reads
// the table: json has no registry behind it, the other two do, and a name
// the engine does not know is not registry-backed.
func TestConfigSchemaRegistry_FormatTable(t *testing.T) {
	coverage.Covers(t, "config.validation")
	assert.Equal(t, len(Formats), len(formatSpecs))
	for _, name := range Formats {
		_, ok := LookupFormat(name)
		assert.True(t, ok)
	}
	for name, want := range map[string]bool{FormatJSON: false, FormatJSONSchema: true, FormatAvro: true} {
		spec, _ := LookupFormat(name)
		assert.Equal(t, want, spec.Registry)
		assert.Equal(t, want, (&KafkaValue{Format: name}).RegistryBacked())
	}
	_, ok := LookupFormat("protobuf")
	assert.False(t, ok)
	assert.False(t, (&KafkaValue{Format: "protobuf"}).RegistryBacked())
}

// EachSink reaches the pipeline's sink, the DLQ and every window's sink, in
// that order, names each by its path, and stops when the caller does.
func TestConfigSchemaRegistry_EachSinkWalksEverySink(t *testing.T) {
	coverage.Covers(t, "config.validation")
	conf := Conf{
		Pipeline: Pipeline{
			Sink:    Sink{Type: "console"},
			OnError: &OnError{Policy: "DLQ", DLQ: &Sink{Type: "kafka"}},
		},
		Tables: &Tables{SQL: []TableSQL{
			{Name: "plain"},
			{Name: "agg", Window: &Window{Sink: Sink{Type: "postgres"}}},
			{Name: "agg2", Window: &Window{Sink: Sink{Type: "noop"}}},
		}},
	}
	var got []string
	for path, s := range conf.EachSink() {
		got = append(got, strings.Join(path, ".")+"="+s.Type)
	}
	assert.Equal(t, []string{
		"pipeline.sink=console",
		"pipeline.on_error.dlq=kafka",
		"tables.sql.1.window.sink=postgres",
		"tables.sql.2.window.sink=noop",
	}, got)

	n := 0
	for range conf.EachSink() {
		n++
		break
	}
	assert.Equal(t, 1, n)

	n = 0
	for range (&Conf{}).EachSink() {
		n++
	}
	assert.Equal(t, 1, n)
}

// run and validate accept the same spellings of a version: latest, or
// digits with no sign and no leading zero. Atoi would take +3 and 03.
func TestConfigSchemaRegistry_VersionSpellings(t *testing.T) {
	coverage.Covers(t, "config.validation")
	for _, ok := range []string{"latest", "1", "3", "10", "120"} {
		assert.True(t, validVersion(ok))
	}
	for _, bad := range []string{"", "0", "-1", "+3", "03", " 3", "3 ", "3.0", "Latest", "newest"} {
		if validVersion(bad) {
			t.Fatalf("%q is accepted", bad)
		}
	}
}

// A struct tag cannot name a constant, so the schema's pattern and
// versionPattern are written twice. This holds them equal.
func TestConfigSchemaRegistry_VersionTagCarriesThePattern(t *testing.T) {
	coverage.Covers(t, "config.validation")
	f, ok := reflect.TypeOf(KafkaValueSchema{}).FieldByName("Version")
	assert.True(t, ok)
	assert.That(t, strings.Contains(f.Tag.Get("jsonschema_extras"), "pattern="+versionPattern+","))
}

// The three rules the JSON Schema states too are marked, and no other rule
// is: validate may drop a marked finding the schema already printed, and
// must never drop an unmarked one.
func TestConfigSchemaRegistry_OnlySchemaRulesAreMarked(t *testing.T) {
	coverage.Covers(t, "config.validation")
	conf := avroConf()
	conf.Pipeline.SchemaRegistry.URL = ""
	conf.Pipeline.SchemaRegistry.Auth = &SchemaRegistryAuth{Username: "u"}
	conf.Pipeline.Source.Kafka.Value = &KafkaValue{Format: "protobuf", Subject: "x"}
	conf.Pipeline.Sink.Kafka.Value.Schema.Version = "03"

	marked := map[string]bool{}
	for _, v := range conf.CheckSchemaRegistry() {
		marked[v.Key()] = v.InSchema
	}
	assert.Equal(t, map[string]bool{
		"pipeline.schema_registry.url":             true,
		"pipeline.schema_registry.auth":            false,
		"pipeline.source.kafka.value.format":       true,
		"pipeline.source.kafka.value.subject":      false,
		"pipeline.sink.kafka.value.schema.version": true,
	}, marked)
}
