package config

// The formats a value block may name. json is the default and needs no
// registry. The other two are framed by a Confluent-compatible schema
// registry: a magic byte, a four-byte schema ID, then the payload.
const (
	FormatJSON       = "json"
	FormatJSONSchema = "json_schema"
	FormatAvro       = "avro"
)

// Formats lists every format, for a rule that names them.
var Formats = []string{FormatJSON, FormatJSONSchema, FormatAvro}

// SchemaRegistry is the registry a source reads schemas from and a sink
// registers them with. One block on the pipeline: a source and a sink almost
// always share a registry, and a shared block is one URL and one credential.
type SchemaRegistry struct {
	// The registry's base URL.
	URL string `yaml:"url"`
	// Credentials, when the registry asks for them.
	Auth *SchemaRegistryAuth `yaml:"auth,omitempty"`
	// TLS material, the same shape as kafka.ssl.
	SSL *KafkaSSL `yaml:"ssl,omitempty"`
}

// SchemaRegistryAuth carries a username and password, or a bearer token, not
// both.
type SchemaRegistryAuth struct {
	Username    string `yaml:"username,omitempty"`
	Password    string `yaml:"password,omitempty"`
	BearerToken string `yaml:"bearer_token,omitempty"`
}

// KafkaValue says how a record's value is encoded. On a source it names the
// format the records arrive in. On a sink it names the format to write, the
// subject to register under, and optionally the registered version to write
// against.
type KafkaValue struct {
	// json, json_schema or avro. Absent means json. Anything but json needs
	// pipeline.schema_registry.
	Format string `yaml:"format,omitempty" jsonschema:"enum=json,enum=json_schema,enum=avro"`
	// Sink only. The registry subject. Absent means <topic>-value.
	Subject string `yaml:"subject,omitempty"`
	// Sink only. A registered version to write against. Absent means the
	// sink generates a schema from its output and registers it.
	Schema *KafkaValueSchema `yaml:"schema,omitempty"`
}

// KafkaValueSchema names the registered version a sink writes against.
type KafkaValueSchema struct {
	// pattern and minimum ride the jsonschema_extras tag. oneof_type clears
	// the schema's type, and the reflector then dispatches pattern and
	// minimum on that type and drops both. pattern constrains only a
	// string and minimum only a number, so together they read as "latest,
	// or an integer of at least 1".

	// latest, or a version number of at least 1. Written as the word
	// latest or as a number.
	Version string `yaml:"version" jsonschema:"oneof_type=string;integer" jsonschema_extras:"pattern=^latest$,minimum=1"`
}

// ResolvedFormat is the format the block names, json for an absent block or
// an absent field.
func (v *KafkaValue) ResolvedFormat() string {
	if v == nil || v.Format == "" {
		return FormatJSON
	}
	return v.Format
}

// RegistryBacked reports whether the records are framed by a schema registry.
func (v *KafkaValue) RegistryBacked() bool {
	return v.ResolvedFormat() != FormatJSON
}

// ResolvedSubject is the subject the sink registers under: the one the value
// block names, or <topic>-value.
func (s *KafkaSink) ResolvedSubject() string {
	if s == nil {
		return ""
	}
	if s.Value != nil && s.Value.Subject != "" {
		return s.Value.Subject
	}
	return s.Topic + "-value"
}
