package config

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/turbolytics/sql-flow/internal/errs"
)

// The formats a value block may name. json is the default and needs no
// registry. The other two are framed by a Confluent-compatible schema
// registry: a magic byte, a four-byte schema ID, then the payload.
const (
	FormatJSON       = "json"
	FormatJSONSchema = "json_schema"
	FormatAvro       = "avro"
)

// Formats lists every format, in the order a message names them.
var Formats = []string{FormatJSON, FormatJSONSchema, FormatAvro}

// FormatSpec is what the engine knows about one format. A rule asks the
// table, never the name: a format that is not json is not thereby framed by
// a registry, and the next format added may be neither.
type FormatSpec struct {
	// Registry reports that a schema registry frames the records. Such a
	// format needs pipeline.schema_registry.
	Registry bool
}

var formatSpecs = map[string]FormatSpec{
	FormatJSON:       {},
	FormatJSONSchema: {Registry: true},
	FormatAvro:       {Registry: true},
}

// LookupFormat returns the spec for a format name. ok is false for a name
// the engine does not know.
func LookupFormat(name string) (FormatSpec, bool) {
	spec, ok := formatSpecs[name]
	return spec, ok
}

// SchemaRegistry is the registry a source reads schemas from and a sink
// registers them with. One block on the pipeline: a source and a sink almost
// always share a registry, and a shared block is one URL and one credential.
type SchemaRegistry struct {
	// The registry's base URL.
	URL string `yaml:"url" jsonschema:"minLength=1"`
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

// versionPattern is latest, or a version number of at least 1 with no sign
// and no leading zero. The schema carries the same pattern, so validate and
// run accept the same spellings, quoted or not.
const versionPattern = `^(latest|[1-9][0-9]*)$`

var versionRE = regexp.MustCompile(versionPattern)

// KafkaValueSchema names the registered version a sink writes against.
type KafkaValueSchema struct {
	// pattern and minimum ride the jsonschema_extras tag. oneof_type clears
	// the schema's type, and the reflector then dispatches pattern and
	// minimum on that type and drops both. pattern constrains only a
	// string and minimum only a number, so together they read as "latest,
	// or an integer of at least 1", quoted or not. The pattern here is
	// versionPattern; a struct tag cannot name a constant, and
	// TestConfigSchemaRegistry_VersionTagCarriesThePattern holds them equal.

	// latest, or a version number of at least 1. Written as the word
	// latest or as a number.
	Version string `yaml:"version" jsonschema:"oneof_type=string;integer" jsonschema_extras:"pattern=^(latest|[1-9][0-9]*)$,minimum=1"`
}

// ResolvedFormat is the format the block names, json for an absent block or
// an absent field.
func (v *KafkaValue) ResolvedFormat() string {
	if v == nil || v.Format == "" {
		return FormatJSON
	}
	return v.Format
}

// RegistryBacked reports whether a schema registry frames the records. A
// format the engine does not know is not: the unknown name is its own
// violation, and no registry rule can be held against it.
func (v *KafkaValue) RegistryBacked() bool {
	spec, ok := LookupFormat(v.ResolvedFormat())
	return ok && spec.Registry
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

// ReservedSourceColumns are the metadata columns the inferred handler adds
// to every batch. A reader schema that defines one of them would give batch
// two columns of that name, so #300 refuses it at start.
var ReservedSourceColumns = []string{"kafka_topic", "kafka_partition", "kafka_offset"}

// IsReservedSourceColumn reports whether name is one of ReservedSourceColumns.
func IsReservedSourceColumn(name string) bool {
	for _, r := range ReservedSourceColumns {
		if name == r {
			return true
		}
	}
	return false
}

// Key is the path as a reader writes it: pipeline.sink.kafka.value.format.
// Violation itself is declared in serve.go, shared with ServeConf.Check and
// RollupsConf.Check; CheckSchemaRegistry below fills Code the same way they
// do.
func (v Violation) Key() string { return strings.Join(v.Path, ".") }

// inferredMemBatch is the one handler a registry-backed source may run
// under. The disk handler stages JSON files and the structured handler
// derives its schema from a table; a typed path for either is a follow-up.
const inferredMemBatch = "handlers.InferredMemBatch"

// keyPath is base plus keys, in a slice of its own. A violation keeps its
// path, so two violations built from one base must not share an array.
func keyPath(base []string, keys ...string) []string {
	out := make([]string, 0, len(base)+len(keys))
	out = append(out, base...)
	return append(out, keys...)
}

// CheckSchemaRegistry holds a config to the rules the JSON Schema cannot
// state, plus the three it can, because run has no schema pass. Those three
// are marked InSchema, so validate can leave them to the schema check. It
// returns every violation rather than the first: validate lists them all,
// and run prints the first. Nothing here reads the network.
func (c *Conf) CheckSchemaRegistry() []Violation {
	var out []Violation
	add := func(msg string, path ...string) {
		out = append(out, Violation{Code: errs.CodeConfigInvalid, Path: path, Message: msg})
	}
	// addInSchema is add for a rule the JSON Schema states too: a missing or
	// empty url, an unknown format, a malformed version.
	addInSchema := func(msg string, path ...string) {
		out = append(out, Violation{Code: errs.CodeConfigInvalid, Path: path, Message: msg, InSchema: true})
	}
	unknownFormat := func(format string, path []string) {
		addInSchema(fmt.Sprintf("format %q is not one of %s", format, formatList()),
			keyPath(path, "format")...)
	}

	p := &c.Pipeline
	registry := p.SchemaRegistry

	if registry != nil {
		if registry.URL == "" {
			addInSchema("url is needed: the registry's base URL", "pipeline", "schema_registry", "url")
		}
		if a := registry.Auth; a != nil {
			basic := a.Username != "" || a.Password != ""
			switch {
			case basic && a.BearerToken != "":
				add("auth carries a username and password, or a bearer_token, not both",
					"pipeline", "schema_registry", "auth")
			case basic && (a.Username == "" || a.Password == ""):
				add("auth needs both username and password",
					"pipeline", "schema_registry", "auth")
			case !basic && a.BearerToken == "":
				add("auth is empty: give it a username and password, or a bearer_token, or remove the block",
					"pipeline", "schema_registry", "auth")
			}
		}
	}

	if src := p.Source.Kafka; src != nil && src.Value != nil {
		v := src.Value
		path := []string{"pipeline", "source", "kafka", "value"}
		format := v.ResolvedFormat()
		if _, known := LookupFormat(format); !known {
			unknownFormat(format, path)
		}
		if v.Subject != "" {
			add("subject is a sink key: a source reads the subject its records carry",
				keyPath(path, "subject")...)
		}
		if v.Schema != nil {
			add("schema is a sink key: a source reads the schema its records carry",
				keyPath(path, "schema")...)
		}
		if v.RegistryBacked() {
			if registry == nil {
				add(fmt.Sprintf("format %s needs pipeline.schema_registry", format),
					keyPath(path, "format")...)
			}
			if len(src.Topics) != 1 {
				add(fmt.Sprintf("format %s reads exactly one topic, one reader schema per pipeline; got %d",
					format, len(src.Topics)), "pipeline", "source", "kafka", "topics")
			}
			if p.Handler.Type != inferredMemBatch {
				add(fmt.Sprintf("format %s needs handler type %s; got %s",
					format, inferredMemBatch, p.Handler.Type), "pipeline", "handler", "type")
			}
		}
	}

	for base, s := range c.EachSink() {
		if s.Kafka == nil || s.Kafka.Value == nil {
			continue
		}
		v := s.Kafka.Value
		path := keyPath(base, "kafka", "value")
		format := v.ResolvedFormat()
		if _, known := LookupFormat(format); !known {
			unknownFormat(format, path)
		}
		// A format with no registry behind it, json or a name the engine
		// does not know, registers nothing and writes against no version.
		backed := v.RegistryBacked()
		if backed && registry == nil {
			add(fmt.Sprintf("format %s needs pipeline.schema_registry", format),
				keyPath(path, "format")...)
		}
		if v.Subject != "" && !backed {
			add("subject needs a format a registry frames; a json sink registers nothing",
				keyPath(path, "subject")...)
		}
		if v.Schema != nil {
			if !backed {
				add("schema needs a format a registry frames; a json sink writes against no registered version",
					keyPath(path, "schema")...)
			} else if !validVersion(v.Schema.Version) {
				addInSchema(fmt.Sprintf("version is latest or a version number of at least 1; got %q", v.Schema.Version),
					keyPath(path, "schema", "version")...)
			}
		}
	}

	return out
}

// formatList is "json, json_schema or avro", from Formats.
func formatList() string {
	n := len(Formats)
	return strings.Join(Formats[:n-1], ", ") + " or " + Formats[n-1]
}

// validVersion accepts latest, or a version number of at least 1 with no
// sign and no leading zero. yaml.v3 hands every spelling over as a string,
// so this is the schema's pattern, not strconv: Atoi takes +3 and 03.
func validVersion(v string) bool {
	return versionRE.MatchString(v)
}
