# Schema Registry Config Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** The config surface for the schema registry milestone: the `pipeline.schema_registry` block, the `value` block on the Kafka source and the three Kafka sinks, the rules that hold them together, and the five error codes the later issues fail with. Issue #296.

**Architecture:** Four new structs in `internal/config`, reflected into the JSON Schema by `make schema`. One rule function, `Conf.CheckSchemaRegistry`, returns every violation with the path of the key it names. `validate` turns each path into a line. `run` and `dev invoke` return the first as `user.config.invalid`. A temporary gate in `run` refuses a registry format outright until #300 and #304 ship the engine behind it, so `main` stays releasable at every merge.

**Tech Stack:** Go 1.25, `gopkg.in/yaml.v3`, `github.com/invopop/jsonschema` v0.14.0 for reflection, `github.com/santhosh-tekuri/jsonschema/v6` for validation.

**Spec:** `docs/superpowers/specs/2026-09-12-schema-registry-mvp-design.md`, sections Config and New error codes. Read those two sections first. Every task below argues from them.

## Global Constraints

- The branch is `feat/schema-registry-config`, from `origin/main` at 9bae2f5. Work in the worktree `.claude/worktrees/schema-registry-config`. Never touch the user's main checkout.
- Every Go test carries `coverage.Covers(t, "<feature>")` as its first statement. This issue adds no feature ids; the serde features arrive in #298. Use `config.validation` for the config rules, `validate.schema` for the validate check, `cli.dev_invoke` for dev invoke, `error.taxonomy` for the codes, and `cli.invocation` for the run gate.
- Error messages from constructors and rules must not contain the substrings `not supported` or `requires a`: `internal/cli/examples_test.go` reads those as a parity failure. Write `needs`.
- Prose in comments, commits and YAML follows Google Technical Writing One. No em dashes. No attribution lines in commits.
- `gofmt -l` empty and `go vet ./...` clean after every task. `go test -short -race ./...` green at every commit.
- Commit after every task with the message given. Do not push until Task 7 says so.
- Do not run `make release-image` and do not tag. Never hand-edit `docs/coverage/status/`. After the first CI run on the branch, download that run's reports (`gh run download <run> --pattern 'report-*'`, flattened into `.coverage/`), run `make coverage-check`, and commit whatever changed under `docs/coverage/status/` and `matrix.md`.
- No CHANGELOG entry. Nothing in this issue changes what a pipeline does. The milestone gets one entry when #305 ships the docs.
- The rules never read the network. Fetching a schema to check it is #297 and #300.

## Deviations from the spec and the issue, decided here

1. **The reserved-column rule is a function here and a call site in #300.** The spec refuses a source schema field named `kafka_topic`, `kafka_partition` or `kafka_offset` at start. That needs the reader schema, which the registry client fetches, and the client is #297. This plan exports the list and the check as `config.ReservedSourceColumns` and `config.IsReservedSourceColumn`, moves the handler's private copy of the list onto them, and tests them as pure functions. #300 calls the check where it fetches the schema.
2. **A temporary gate in `run` refuses any registry format.** After this PR, `format: avro` parses and validates, but nothing decodes it until #300, and nothing encodes it until #304. Without a gate the source would read framed bytes as JSON and fail every record as `user.data.malformed`, and the sink would write JSON under a format the config promised was Avro. `refuseUnshippedFormats` in `internal/cli/run/serde.go` fails the build with `user.config.invalid` and names the issue that lifts it. #300 deletes the source half; #304 deletes the sink half.
3. **`validate` does not repeat what the schema found.** The JSON Schema already refuses an unknown `format` and a `version` that is neither `latest` nor an integer of at least 1. `run` has no schema pass, so `CheckSchemaRegistry` checks both too. In `validate` a rule finding at a path the schema already reported is dropped, the way `checkSinks` leaves a missing `mode` to the schema.
4. **Two more shapes are refused.** The spec's `auth` rule says username and password, or a bearer token, not both. A username without a password, or an empty `auth` block, is refused with the same key. `url` is required by the schema, and `CheckSchemaRegistry` refuses an empty one too, for `run`.
5. **`pattern` and `minimum` ride the `jsonschema_extras` tag, not `jsonschema`.** The spec writes one tag: `oneof_type=string;integer,pattern=^latest$,minimum=1`. In `invopop/jsonschema` v0.14.0, `oneof_type` clears the schema's `type`, and the reflector then dispatches `pattern` and `minimum` on that type, so both are dropped without a word. Read `reflect.go` in the module cache: `genericKeywords` sets `t.Type = ""`, and the `switch t.Type` after it runs neither `stringKeywords` nor `numericalKeywords`. `jsonschema_extras` writes any keyword through, and its `minimum` is parsed as an integer. Task 1 checks the generated JSON for all three keywords.

## File structure

| File | Responsibility |
| --- | --- |
| `internal/config/config.go` | `Pipeline.SchemaRegistry`, `KafkaSource.Value`, `KafkaSink.Value` |
| `internal/config/serde.go` | The four structs, the format constants, `ResolvedFormat`, `RegistryBacked`, `ResolvedSubject`, the reserved columns, `Violation`, `Conf.CheckSchemaRegistry` |
| `internal/config/serde_test.go` | Parsing, the helpers, and one test per rule naming its key |
| `internal/validate/schemas/config.json` | Regenerated by `make schema` |
| `internal/validate/serde.go` | `checkSchemaRegistry`: violations to positioned diagnostics under check id `config.schema_registry` |
| `internal/validate/serde_test.go` | The check, its positions, the schema's own findings, and the no-repeat rule |
| `internal/cli/run/serde.go` | `checkSchemaRegistry` for run, and `refuseUnshippedFormats` |
| `internal/cli/run/serde_test.go` | Both |
| `internal/cli/run/root.go` | One call after `LoadRendered` |
| `internal/cli/dev.go` | The rules, then the registry-backed source refusal |
| `internal/cli/dev_test.go` | Both refusals |
| `internal/handlers/inferred.go` | `isMetadataField` reads `config.IsReservedSourceColumn` |
| `internal/errs/registry.go` | Five codes, and the `security_invalid` text |
| `internal/errs/testdata/codes.golden` | Regenerated, five lines longer |
| `internal/errs/errs_test.go` | The five codes exit 10 and are not retried |

---

### Task 1: The config structs and the regenerated schema

**Files:**
- Create: `internal/config/serde.go`
- Create: `internal/config/serde_test.go`
- Modify: `internal/config/config.go:53-61` (`KafkaSink`), `:230-244` (`KafkaSource`), `:362-386` (`Pipeline`)
- Regenerate: `internal/validate/schemas/config.json`

**Interfaces:**
- Produces: `config.SchemaRegistry{URL string; Auth *SchemaRegistryAuth; SSL *KafkaSSL}`, `config.SchemaRegistryAuth{Username, Password, BearerToken string}`, `config.KafkaValue{Format, Subject string; Schema *KafkaValueSchema}`, `config.KafkaValueSchema{Version string}`, constants `config.FormatJSON`, `config.FormatJSONSchema`, `config.FormatAvro`, `config.Formats []string`, methods `(*KafkaValue).ResolvedFormat() string`, `(*KafkaValue).RegistryBacked() bool`, `(*KafkaSink).ResolvedSubject() string`. All three methods accept a nil receiver.

- [ ] **Step 1: Write the failing tests**

`internal/config/serde_test.go`:

```go
package config

import (
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
```

Add `"fmt"` to the imports.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test ./internal/config/ -run 'TestConfigSchemaRegistry_' -v`
Expected: build failure, `undefined: FormatJSON`, `conf.Pipeline.SchemaRegistry undefined`.

- [ ] **Step 3: Write the structs**

`internal/config/serde.go`:

```go
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
	// latest, or a version number of at least 1. A string in Go because
	// yaml.v3 reads both spellings into one field.
	//
	// pattern and minimum sit in jsonschema_extras: oneof_type clears the
	// schema's type, and the reflector drops the two keywords for a field
	// with no type. pattern constrains only a string and minimum only a
	// number, so the pair reads as "latest, or an integer of at least 1".
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
```

In `internal/config/config.go`, add the fields:

```go
type KafkaSink struct {
	// List of Kafka brokers.
	Brokers []string `yaml:"brokers"`
	// Target Kafka topic.
	Topic            string     `yaml:"topic"`
	SecurityProtocol string     `yaml:"security_protocol,omitempty"`
	SSL              *KafkaSSL  `yaml:"ssl,omitempty"`
	SASL             *KafkaSASL `yaml:"sasl,omitempty"`
	// How the record's value is encoded. Absent means json.
	Value *KafkaValue `yaml:"value,omitempty"`
}
```

In `KafkaSource`, after `Fetch`:

```go
	// How the record's value is encoded. Absent means json.
	Value *KafkaValue `yaml:"value,omitempty"`
```

In `Pipeline`, after `Description`:

```go
	// The schema registry a source reads schemas from and a sink registers
	// them with. Needed when any value block names a format other than json.
	SchemaRegistry *SchemaRegistry `yaml:"schema_registry,omitempty"`
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test ./internal/config/ -run 'TestConfigSchemaRegistry_' -v`
Expected: PASS, both tests.

- [ ] **Step 5: Regenerate the schema and check the version shape**

Run: `make schema`
Expected: `regenerated internal/validate/schemas/config.json, serve.json and rollups.json`. `git diff --stat` shows only `config.json` changed.

Run: `go test ./internal/schema/ ./internal/validate/`
Expected: PASS. `TestConfigSchema_CommittedFileMatchesTheTypes` and `TestConfigSchema_AcceptsEveryShippedExample` still pass.

Open the diff and confirm `version` came out as the spec describes:

```json
"version": {
  "oneOf": [
    { "type": "string" },
    { "type": "integer" }
  ],
  "pattern": "^latest$",
  "minimum": 1,
  ...
}
```

All three keywords must be there. If `oneOf` is missing, the `jsonschema` tag did not apply: check its spelling against `reflect.go` in the module cache (`oneof_type`, values split on `;`). If `pattern` or `minimum` is missing, it was put in the `jsonschema` tag instead of `jsonschema_extras`, where the reflector drops it (see deviation 5). Fix and rerun `make schema`.

- [ ] **Step 6: Commit**

```bash
git add internal/config/config.go internal/config/serde.go internal/config/serde_test.go internal/validate/schemas/config.json
git commit -m "config: the schema_registry block and the value blocks, parsed and reflected into the schema

A source or sink may now name json, json_schema or avro, and a sink a
subject and a registered version. Nothing reads them yet: #300 wires the
source and #304 the sink. The version field is a string because yaml.v3
reads latest and 3 into one field, and the schema accepts both spellings.

If the schema is wrong, validate refuses a config run accepts, or the other
way round, which is what generating it from the types exists to prevent."
```

---

### Task 2: The rules

**Files:**
- Modify: `internal/config/serde.go`
- Modify: `internal/config/serde_test.go`
- Modify: `internal/handlers/inferred.go:515-517`

**Interfaces:**
- Consumes: the structs from Task 1.
- Produces: `config.Violation{Path []string; Message string}` with `(Violation).Key() string` joining the path with dots, `(*Conf).CheckSchemaRegistry() []Violation`, `config.ReservedSourceColumns []string`, `config.IsReservedSourceColumn(name string) bool`. A `Path` is the key from the document root; a sequence index is its number as a string, so `validate` can hand it to `lineOf`.

- [ ] **Step 1: Write the failing tests**

Append to `internal/config/serde_test.go`:

```go
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
			name:   "schema needs a registry format",
			mutate: func(c *Conf) { c.Pipeline.Sink.Kafka.Value = &KafkaValue{Format: FormatJSON, Schema: &KafkaValueSchema{Version: "1"}} },
			want:   []string{"pipeline.sink.kafka.value.schema"},
			says:   "json",
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
			name:   "auth with both credentials",
			mutate: func(c *Conf) { c.Pipeline.SchemaRegistry.Auth = &SchemaRegistryAuth{Username: "u", Password: "p", BearerToken: "t"} },
			want:   []string{"pipeline.schema_registry.auth"},
			says:   "not both",
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
		})
	}
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
```

Add `"strings"` to the imports.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test ./internal/config/ -run 'TestConfigSchemaRegistry_' -v`
Expected: build failure, `undefined: Violation`, `ReservedSourceColumns`.

- [ ] **Step 3: Write the rules**

Append to `internal/config/serde.go`:

```go
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

// Violation is one rule a config breaks. Path is the offending key from the
// document root, with a sequence index as its number, so validate can turn
// it into a line and run can print it.
type Violation struct {
	Path    []string
	Message string
}

// Key is the path as a reader writes it: pipeline.sink.kafka.value.format.
func (v Violation) Key() string { return strings.Join(v.Path, ".") }

// inferredMemBatch is the one handler a registry-backed source may run
// under. The disk handler stages JSON files and the structured handler
// derives its schema from a table; a typed path for either is a follow-up.
const inferredMemBatch = "handlers.InferredMemBatch"

// CheckSchemaRegistry holds a config to the rules the JSON Schema cannot
// state, plus the two it can, because run has no schema pass. It returns
// every violation rather than the first: validate lists them all, and run
// prints the first. Nothing here reads the network.
func (c *Conf) CheckSchemaRegistry() []Violation {
	var out []Violation
	add := func(msg string, path ...string) {
		out = append(out, Violation{Path: path, Message: msg})
	}

	p := &c.Pipeline
	registry := p.SchemaRegistry

	if registry != nil {
		if registry.URL == "" {
			add("url is needed: the registry's base URL", "pipeline", "schema_registry", "url")
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
		if !knownFormat(format) {
			add(fmt.Sprintf("format %q is not one of json, json_schema or avro", format),
				append(path, "format")...)
		}
		if v.Subject != "" {
			add("subject is a sink key: a source reads the subject its records carry",
				append(path, "subject")...)
		}
		if v.Schema != nil {
			add("schema is a sink key: a source reads the schema its records carry",
				append(path, "schema")...)
		}
		if knownFormat(format) && v.RegistryBacked() {
			if registry == nil {
				add(fmt.Sprintf("format %s needs pipeline.schema_registry", format),
					append(path, "format")...)
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

	checkSink := func(s Sink, path ...string) {
		if s.Kafka == nil || s.Kafka.Value == nil {
			return
		}
		v := s.Kafka.Value
		path = append(path, "kafka", "value")
		format := v.ResolvedFormat()
		if !knownFormat(format) {
			add(fmt.Sprintf("format %q is not one of json, json_schema or avro", format),
				append(path, "format")...)
			return
		}
		if v.RegistryBacked() && registry == nil {
			add(fmt.Sprintf("format %s needs pipeline.schema_registry", format),
				append(path, "format")...)
		}
		if v.Subject != "" && !v.RegistryBacked() {
			add("subject needs a format other than json; a json sink registers nothing",
				append(path, "subject")...)
		}
		if v.Schema != nil {
			if !v.RegistryBacked() {
				add("schema needs a format other than json; a json sink writes against no registered version",
					append(path, "schema")...)
			} else if !validVersion(v.Schema.Version) {
				add(fmt.Sprintf("version is latest or a version number of at least 1; got %q", v.Schema.Version),
					append(path, "schema", "version")...)
			}
		}
	}

	checkSink(p.Sink, "pipeline", "sink")
	if p.OnError != nil && p.OnError.DLQ != nil {
		checkSink(*p.OnError.DLQ, "pipeline", "on_error", "dlq")
	}
	if c.Tables != nil {
		for i, table := range c.Tables.SQL {
			if table.Window != nil {
				checkSink(table.Window.Sink, "tables", "sql", strconv.Itoa(i), "window", "sink")
			}
		}
	}

	return out
}

func knownFormat(format string) bool {
	for _, f := range Formats {
		if format == f {
			return true
		}
	}
	return false
}

// validVersion accepts latest or a positive integer. yaml.v3 hands both
// spellings over as a string.
func validVersion(v string) bool {
	if v == "latest" {
		return true
	}
	n, err := strconv.Atoi(v)
	return err == nil && n >= 1
}
```

Add `"fmt"`, `"strconv"` and `"strings"` to the file's imports.

One ordering detail the test pins: for "a registry format needs the registry block", the source violation comes before the sink's, because the source is checked first. Keep that order.

In `internal/handlers/inferred.go`, replace the private list:

```go
func isMetadataField(name string) bool {
	return config.IsReservedSourceColumn(name)
}
```

`inferred.go` does not import `config` today. Add `"github.com/turbolytics/sql-flow/internal/config"` to its imports. `handlers/init.go` already imports it, so there is no cycle.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test ./internal/config/ ./internal/handlers/ -run 'TestConfigSchemaRegistry_|TestHandler' -v 2>&1 | tail -40`
Expected: PASS. Every subtest of `EachRuleNamesItsKey` passes. If a subtest's `want` order differs from `got`, the rule order in `CheckSchemaRegistry` is what is wrong, not the test.

- [ ] **Step 5: Commit**

```bash
git add internal/config/serde.go internal/config/serde_test.go internal/handlers/inferred.go
git commit -m "config: the schema registry rules, each naming its key

CheckSchemaRegistry returns every violation with the path of the key it
is about. A registry format needs the registry block; a registry-backed
source reads one topic under InferredMemBatch; subject and schema are sink
keys and need a registry format; a version is latest or at least 1; auth
is one credential, not both. The reserved metadata columns move to config,
where #300 will check a fetched schema against them.

If a rule is missing, run builds a pipeline it cannot serve and fails on
the first record instead of at start."
```

---

### Task 3: The validate check

**Files:**
- Create: `internal/validate/serde.go`
- Create: `internal/validate/serde_test.go`
- Modify: `internal/validate/validate.go:52-56`

**Interfaces:**
- Consumes: `conf.CheckSchemaRegistry()`, `Violation.Path`, `Violation.Key()` from Task 2; `lineOf` and `diagnostic` in this package.
- Produces: check id `config.schema_registry`.

- [ ] **Step 1: Write the failing tests**

`internal/validate/serde_test.go`:

```go
package validate

import (
	"context"
	"strings"
	"testing"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

// registryConfig has three holes: the schema_registry block (or nothing),
// the source value block at six spaces, and the sink value block at six.
const registryConfig = `pipeline:
%s
  source:
    type: kafka
    kafka:
      brokers: ["localhost:9092"]
      group_id: g
      auto_offset_reset: earliest
      topics: ["orders"]
%s
  handler:
    type: handlers.InferredMemBatch
    sql: SELECT * FROM batch
  sink:
    type: kafka
    kafka:
      brokers: ["localhost:9092"]
      topic: orders-enriched
%s
`

const (
	registryBlock = `  schema_registry:
    url: http://localhost:8081`
	avroValue = `      value:
        format: avro`
	avroPinned = `      value:
        format: avro
        schema:
          version: %s`
)

func validateRegistry(t *testing.T, registry, sourceValue, sinkValue string) (Report, string) {
	t.Helper()
	cfg := registryConfig
	for _, v := range []string{registry, sourceValue, sinkValue} {
		cfg = strings.Replace(cfg, "%s", v, 1)
	}
	rep, err := Validate(context.Background(), Request{Path: "p.yml", Config: cfg})
	assert.NoError(t, err)
	return rep, cfg
}

// lineOfText is the 1-based line the needle first appears on.
func lineOfText(t *testing.T, text, needle string) int {
	t.Helper()
	for i, l := range strings.Split(text, "\n") {
		if strings.Contains(l, needle) {
			return i + 1
		}
	}
	t.Fatalf("%q is not in the config", needle)
	return 0
}

func registryDiagnostics(rep Report) []Diagnostic {
	var out []Diagnostic
	for _, d := range rep.Diagnostics {
		if strings.Contains(d.Context, "/value/") || strings.Contains(d.Context, "/schema_registry") ||
			strings.Contains(d.Context, "/topics") || strings.Contains(d.Context, "/handler/type") {
			out = append(out, d)
		}
	}
	return out
}

// The spec's example passes the check.
func TestValidateSchema_SchemaRegistryExamplePasses(t *testing.T) {
	coverage.Covers(t, "validate.schema")
	rep, _ := validateRegistry(t, registryBlock, avroValue, avroValue)
	assert.That(t, rep.OK)
	assert.Equal(t, StatusPass, checkStatus(t, rep, "config.schema_registry"))
	assert.Equal(t, 0, len(registryDiagnostics(rep)))
}

// A config with no value block anywhere passes too, and reports the check
// as run, not skipped.
func TestValidateSchema_SchemaRegistryAbsentPasses(t *testing.T) {
	coverage.Covers(t, "validate.schema")
	rep, _ := validateRegistry(t, "", "", "")
	assert.That(t, rep.OK)
	assert.Equal(t, StatusPass, checkStatus(t, rep, "config.schema_registry"))
}

// A rule finding is an error at the line of the key it names, with the key
// in the message and the path in the context.
func TestValidateSchema_SchemaRegistryFindingNamesTheLine(t *testing.T) {
	coverage.Covers(t, "validate.schema")
	rep, cfg := validateRegistry(t, "", avroValue, "")
	assert.That(t, !rep.OK)
	assert.Equal(t, StatusFail, checkStatus(t, rep, "config.schema_registry"))
	diags := registryDiagnostics(rep)
	assert.Equal(t, 1, len(diags))
	d := diags[0]
	assert.Equal(t, SeverityError, d.Severity)
	assert.Equal(t, "user.config.invalid", d.Code)
	assert.That(t, strings.HasPrefix(d.Message, "pipeline.source.kafka.value.format: "))
	assert.That(t, strings.Contains(d.Message, "needs pipeline.schema_registry"))
	assert.Equal(t, "/pipeline/source/kafka/value/format", d.Context)
	assert.NotNil(t, d.Position)
	assert.Equal(t, lineOfText(t, cfg, "format: avro"), d.Position.Line)
}

// Every violation is listed, not the first.
func TestValidateSchema_SchemaRegistryListsEveryFinding(t *testing.T) {
	coverage.Covers(t, "validate.schema")
	rep, _ := validateRegistry(t, "", avroValue, avroValue)
	diags := registryDiagnostics(rep)
	assert.Equal(t, 2, len(diags))
	assert.Equal(t, "/pipeline/source/kafka/value/format", diags[0].Context)
	assert.Equal(t, "/pipeline/sink/kafka/value/format", diags[1].Context)
}

// The schema accepts version as latest or an integer of at least 1, and
// refuses the rest. A shape the schema refuses is reported once: the schema
// finds it, and the rule does not repeat it.
func TestValidateSchema_SchemaRegistryVersionShapes(t *testing.T) {
	coverage.Covers(t, "validate.schema")
	for _, ok := range []string{"latest", "3"} {
		rep, _ := validateRegistry(t, registryBlock, avroValue, fmt.Sprintf(avroPinned, ok))
		if !rep.OK {
			t.Fatalf("version %s: %v", ok, rep.Diagnostics)
		}
	}
	for _, bad := range []string{"0", "newest", "-1"} {
		rep, _ := validateRegistry(t, registryBlock, avroValue, fmt.Sprintf(avroPinned, bad))
		assert.That(t, !rep.OK)
		assert.Equal(t, StatusFail, checkStatus(t, rep, "config.schema"))
		at := 0
		for _, d := range rep.Diagnostics {
			if d.Context == "/pipeline/sink/kafka/value/schema/version" {
				at++
			}
		}
		if at != 1 {
			t.Fatalf("version %s: %d diagnostics at the version, want 1: %v", bad, at, rep.Diagnostics)
		}
	}
}

// An unknown format is the schema's finding, reported once.
func TestValidateSchema_SchemaRegistryUnknownFormatReportedOnce(t *testing.T) {
	coverage.Covers(t, "validate.schema")
	rep, _ := validateRegistry(t, registryBlock, "      value:\n        format: protobuf", "")
	assert.That(t, !rep.OK)
	at := 0
	for _, d := range rep.Diagnostics {
		if d.Context == "/pipeline/source/kafka/value/format" {
			at++
		}
	}
	assert.Equal(t, 1, at)
}
```

Add `"fmt"` to the imports.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test ./internal/validate/ -run 'TestValidateSchema_SchemaRegistry' -v 2>&1 | tail -30`
Expected: FAIL. `checkStatus` fatals with `no check "config.schema_registry"`.

- [ ] **Step 3: Write the check**

`internal/validate/serde.go`:

```go
package validate

import (
	"strings"

	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/errs"
	"gopkg.in/yaml.v3"
)

const schemaRegistryCheck = "config.schema_registry"

// checkSchemaRegistry holds the schema_registry block and every value block
// to config.CheckSchemaRegistry, and anchors each finding to the line of the
// key it names. run applies the same rules and stops at the first; here the
// reader wants the whole list.
//
// A finding at a path the schema check already reported is dropped. The
// schema refuses an unknown format and a malformed version; the rule checks
// them too because run has no schema pass, and saying it twice helps nobody.
func checkSchemaRegistry(rendered []byte, rep *Report) {
	var root yaml.Node
	if err := yaml.Unmarshal(rendered, &root); err != nil {
		rep.SetCheck(schemaRegistryCheck, StatusSkipped, "the config did not parse, so there was no schema registry block to check")
		return
	}
	var conf config.Conf
	if err := root.Decode(&conf); err != nil {
		rep.SetCheck(schemaRegistryCheck, StatusSkipped, "the config did not decode, so there was no schema registry block to check")
		return
	}

	reported := map[string]bool{}
	for _, d := range rep.Diagnostics {
		if d.Context != "" {
			reported[d.Context] = true
		}
	}

	status := StatusPass
	for _, v := range conf.CheckSchemaRegistry() {
		context := "/" + strings.Join(v.Path, "/")
		if reported[context] {
			continue
		}
		status = StatusFail
		var pos *Position
		if line, col, ok := lineOf(&root, v.Path); ok {
			pos = &Position{Source: "config", Line: line, Column: col}
		}
		d := diagnostic(errs.CodeConfigInvalid, SeverityError, v.Key()+": "+v.Message, pos)
		d.Context = context
		rep.Add(d)
	}
	rep.SetCheck(schemaRegistryCheck, status, "")
}
```

In `internal/validate/validate.go`, the pipeline branch:

```go
	default:
		checkDrainDeadline(rendered, &rep)
		checkWindows(rendered, &rep)
		checkSinks(rendered, &rep)
		checkSchemaRegistry(rendered, &rep)
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test ./internal/validate/ -v 2>&1 | tail -40`
Expected: PASS, the six new tests and every existing one. If `TestValidateSchema_SchemaRegistryVersionShapes` counts 2 at the version for `0`, the schema diagnostic's `Context` and the rule's differ in spelling. Print both and align the rule's path, not the schema's. No test enumerates the check ids, so nothing else needs the new one.

- [ ] **Step 5: Commit**

```bash
git add internal/validate/serde.go internal/validate/serde_test.go internal/validate/validate.go
git commit -m "validate: the schema registry rules, each finding at the line of its key

The check config.schema_registry lists every violation CheckSchemaRegistry
returns, anchored to the key it names, and leaves an unknown format or a
malformed version to the schema check that already reported it.

If validate accepted what run refuses, the offline check would pass a
config that fails at start."
```

---

### Task 4: The run gate

**Files:**
- Create: `internal/cli/run/serde.go`
- Create: `internal/cli/run/serde_test.go`
- Modify: `internal/cli/run/root.go:161-166`

**Interfaces:**
- Consumes: `conf.CheckSchemaRegistry()`, `(*KafkaValue).RegistryBacked()`, `(*KafkaValue).ResolvedFormat()`.
- Produces: `checkSchemaRegistry(conf *config.Conf) error`, `refuseUnshippedFormats(conf *config.Conf) error`.

- [ ] **Step 1: Write the failing tests**

`internal/cli/run/serde_test.go`:

```go
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
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test ./internal/cli/run/ -run 'SchemaRegistry|UnshippedFormat' -v 2>&1 | tail -20`
Expected: build failure, `undefined: checkSchemaRegistry`.

- [ ] **Step 3: Write the gate**

`internal/cli/run/serde.go`:

```go
package run

import (
	"strconv"

	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/errs"
)

// checkSchemaRegistry refuses, before anything is dialed, a config validate
// would refuse, for a config that never went through validate. The first
// violation is the error; validate lists them all.
func checkSchemaRegistry(conf *config.Conf) error {
	if vs := conf.CheckSchemaRegistry(); len(vs) > 0 {
		return errs.New(errs.CodeConfigInvalid, "%s: %s", vs[0].Key(), vs[0].Message)
	}
	return refuseUnshippedFormats(conf)
}

// refuseUnshippedFormats is the gate between this config surface and the
// engine behind it. Delete the source half when #300 wires the typed
// handler, and the sink half when #304 wires the encoders. Until then a
// registry format has to fail here: the source would read framed bytes as
// JSON and fail every record as user.data.malformed, and the sink would
// write JSON under a format the config promised was Avro.
func refuseUnshippedFormats(conf *config.Conf) error {
	p := &conf.Pipeline
	if src := p.Source.Kafka; src != nil && src.Value.RegistryBacked() {
		return errs.New(errs.CodeConfigInvalid,
			"pipeline.source.kafka.value.format: this build reads json only; %s arrives with #300",
			src.Value.ResolvedFormat())
	}
	sink := func(s config.Sink, key string) error {
		if s.Kafka == nil || !s.Kafka.Value.RegistryBacked() {
			return nil
		}
		return errs.New(errs.CodeConfigInvalid,
			"%s.kafka.value.format: this build writes json only; %s arrives with #304",
			key, s.Kafka.Value.ResolvedFormat())
	}
	if err := sink(p.Sink, "pipeline.sink"); err != nil {
		return err
	}
	if p.OnError != nil && p.OnError.DLQ != nil {
		if err := sink(*p.OnError.DLQ, "pipeline.on_error.dlq"); err != nil {
			return err
		}
	}
	if conf.Tables != nil {
		for i, table := range conf.Tables.SQL {
			if table.Window == nil {
				continue
			}
			if err := sink(table.Window.Sink, "tables.sql."+strconv.Itoa(i)+".window.sink"); err != nil {
				return err
			}
		}
	}
	return nil
}
```

In `internal/cli/run/root.go`, after the `LoadRendered` block:

```go
			conf, rendered, err := config.LoadRendered(configPath, map[string]string{})
			if err != nil {
				// Returned as-is: the error already names the file, the stage and the
				// code, so another prefix adds a word and no information.
				return err
			}

			// Before the database opens: a config error needs nothing
			// closed. The code is already user.config.invalid, so it is
			// returned as-is.
			if err := checkSchemaRegistry(conf); err != nil {
				return err
			}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test ./internal/cli/run/ -run 'SchemaRegistry|UnshippedFormat' -v 2>&1 | tail -20`
Expected: PASS, three tests.

Run: `go test -short ./internal/cli/... 2>&1 | tail -5`
Expected: PASS. `examples_test.go` still builds every shipped example; none names a value block, so the gate is silent.

- [ ] **Step 5: Commit**

```bash
git add internal/cli/run/serde.go internal/cli/run/serde_test.go internal/cli/run/root.go
git commit -m "run: refuse a schema registry rule at start, and refuse a registry format until the engine reads it

run applies CheckSchemaRegistry before the database opens, the way it
applies ReemitOverwrites, for a config that skipped validate. Then a gate
refuses any registry format: #300 ships the source, #304 the sinks, and
each deletes its half. Without it the source reads framed bytes as JSON and
fails every record, and the sink writes JSON under a format the config
called Avro."
```

---

### Task 5: dev invoke

**Files:**
- Modify: `internal/cli/dev.go:82-85`
- Modify: `internal/cli/dev_test.go`

**Interfaces:**
- Consumes: `conf.CheckSchemaRegistry()`, `(*KafkaValue).RegistryBacked()`, `(*KafkaValue).ResolvedFormat()`.

- [ ] **Step 1: Write the failing tests**

Append to `internal/cli/dev_test.go`:

```go
// writeDevConfig writes a pipeline config into a temp dir and returns its
// path. sourceValue is the source's value block at six spaces, or empty.
func writeDevConfig(t *testing.T, registry, sourceValue string) string {
	t.Helper()
	cfg := "pipeline:\n" + registry + `
  source:
    type: kafka
    kafka:
      brokers: ["localhost:9092"]
      group_id: g
      auto_offset_reset: earliest
      topics: ["orders"]
` + sourceValue + `
  handler:
    type: handlers.InferredMemBatch
    sql: SELECT city, count(*) AS city_count FROM batch GROUP BY city
  sink:
    type: console
`
	path := filepath.Join(t.TempDir(), "p.yml")
	if err := os.WriteFile(path, []byte(cfg), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

// dev invoke reads a JSONL fixture. A registry-backed source reads framed
// records, and inferring types from JSONL would give the SQL different
// types than the run gives it. Refused, with the config code.
func TestCliDevInvoke_RefusesARegistryBackedSource(t *testing.T) {
	coverage.Covers(t, "cli.dev_invoke")
	conn, cleanup := newTestADBCConn(t)
	defer cleanup()

	path := writeDevConfig(t,
		"  schema_registry:\n    url: http://localhost:8081",
		"      value:\n        format: avro")
	var out bytes.Buffer
	_, err := devInvoke(context.Background(), conn, path, "../../dev/fixtures/basic.agg.jsonl", &out)
	assert.Error(t, err)
	assert.Equal(t, errs.CodeConfigInvalid, errs.CodeOf(err))
	assert.That(t, strings.Contains(err.Error(), "pipeline.source.kafka.value.format"))
	assert.That(t, strings.Contains(err.Error(), "JSONL"))
	assert.Equal(t, "", out.String())
}

// The rules run here too: a config validate refuses, dev invoke refuses.
func TestCliDevInvoke_RefusesASchemaRegistryRule(t *testing.T) {
	coverage.Covers(t, "cli.dev_invoke")
	conn, cleanup := newTestADBCConn(t)
	defer cleanup()

	path := writeDevConfig(t, "", "      value:\n        format: avro")
	var out bytes.Buffer
	_, err := devInvoke(context.Background(), conn, path, "../../dev/fixtures/basic.agg.jsonl", &out)
	assert.Error(t, err)
	assert.Equal(t, errs.CodeConfigInvalid, errs.CodeOf(err))
	assert.That(t, strings.Contains(err.Error(), "needs pipeline.schema_registry"))
}

// A json source with a value block spelled out is the default, and runs.
func TestCliDevInvoke_JsonValueBlockRuns(t *testing.T) {
	coverage.Covers(t, "cli.dev_invoke")
	conn, cleanup := newTestADBCConn(t)
	defer cleanup()

	path := writeDevConfig(t, "", "      value:\n        format: json")
	var out bytes.Buffer
	table, err := devInvoke(context.Background(), conn, path, "../../dev/fixtures/basic.agg.jsonl", &out)
	assert.NoError(t, err)
	defer table.Release()
	assert.Equal(t, int64(2), table.NumRows())
}
```

Add `"github.com/turbolytics/sql-flow/internal/errs"` to the imports. `os`, `filepath`, `strings`, `bytes` and `context` are already imported.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test ./internal/cli/ -run 'TestCliDevInvoke_Refuses|TestCliDevInvoke_JsonValueBlockRuns' -v 2>&1 | tail -20`
Expected: `RefusesARegistryBackedSource` and `RefusesASchemaRegistryRule` FAIL: `devInvoke` returned no error, or an error without the code. `JsonValueBlockRuns` passes already.

- [ ] **Step 3: Write the refusals**

In `internal/cli/dev.go`, after `config.Load`:

```go
	conf, err := config.Load(configPath, map[string]string{})
	if err != nil {
		return nil, fmt.Errorf("failed to load config: %w", err)
	}

	// The same rules run and validate apply, then one of dev invoke's own:
	// the fixture is JSONL, and a registry-backed source reads framed
	// records. Inferring types from the file would give the SQL different
	// types than the run gives it. #321 is the typed fixture mode.
	if vs := conf.CheckSchemaRegistry(); len(vs) > 0 {
		return nil, errs.New(errs.CodeConfigInvalid, "%s: %s", vs[0].Key(), vs[0].Message)
	}
	if src := conf.Pipeline.Source.Kafka; src != nil && src.Value.RegistryBacked() {
		return nil, errs.New(errs.CodeConfigInvalid,
			"pipeline.source.kafka.value.format: dev invoke reads a JSONL fixture, and a %s source reads framed records. Run it with sqlflow run against a broker, or set the format to json for the fixture",
			src.Value.ResolvedFormat())
	}
```

Add `"github.com/turbolytics/sql-flow/internal/errs"` to the imports.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test ./internal/cli/ -run 'TestCliDevInvoke_' -v 2>&1 | tail -20`
Expected: PASS, every dev invoke test.

- [ ] **Step 5: Commit**

```bash
git add internal/cli/dev.go internal/cli/dev_test.go
git commit -m "dev invoke: refuse a registry-backed source, whose records a JSONL fixture cannot stand in for

The fixture is JSONL and a registry-backed source reads framed records, so
inferring types from the file would give the SQL different types than the
run gives it. The config rules run first, as they do in run and validate.

If dev invoke accepted it, a pipeline would pass its fixture and fail on
its first real batch."
```

---

### Task 6: The five error codes

**Files:**
- Modify: `internal/errs/registry.go:59-68` and the definitions map
- Modify: `internal/errs/errs_test.go`
- Regenerate: `internal/errs/testdata/codes.golden`

**Interfaces:**
- Produces: `errs.CodeDataSchemaUnknown`, `errs.CodeSinkSchemaIncompatible`, `errs.CodeSinkSchemaUnregistered`, `errs.CodeSinkSchemaMismatch`, `errs.CodeSinkNameInvalid`. Later issues raise them; nothing raises them here.

- [ ] **Step 1: Write the failing test**

Append to `internal/errs/errs_test.go`:

```go
// The schema registry codes are user errors: a record nobody registered, a
// schema the registry refused, a version that is not there, a shape the SQL
// does not match, a name the schema language cannot spell. Each exits 10 and
// is never retried; a restart fails the same way.
func TestErrorTaxonomy_SchemaRegistryCodesAreUserErrors(t *testing.T) {
	coverage.Covers(t, "error.taxonomy")
	codes := []Code{
		CodeDataSchemaUnknown,
		CodeSinkSchemaIncompatible,
		CodeSinkSchemaUnregistered,
		CodeSinkSchemaMismatch,
		CodeSinkNameInvalid,
	}
	assert.Equal(t, Code("user.data.schema_unknown"), codes[0])
	assert.Equal(t, Code("user.sink.schema_incompatible"), codes[1])
	assert.Equal(t, Code("user.sink.schema_unregistered"), codes[2])
	assert.Equal(t, Code("user.sink.schema_mismatch"), codes[3])
	assert.Equal(t, Code("user.sink.name_invalid"), codes[4])
	for _, c := range codes {
		d, ok := Lookup(c)
		assert.True(t, ok)
		assert.That(t, d.Summary != "")
		assert.That(t, d.Action != "")
		exit := ExitCode(New(c, "x"))
		assert.Equal(t, ExitUserError, exit)
		assert.False(t, Retryable(exit))
	}

	// security_invalid keeps its code and now names the registry too.
	d, ok := Lookup(CodeSourceSecurityInvalid)
	assert.True(t, ok)
	assert.That(t, strings.Contains(d.Summary, "schema registry"))
	assert.That(t, strings.Contains(d.Action, "schema_registry"))
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `go test ./internal/errs/ -run TestErrorTaxonomy_SchemaRegistryCodesAreUserErrors -v`
Expected: build failure, `undefined: CodeDataSchemaUnknown`.

- [ ] **Step 3: Add the codes**

In the `const` block of `internal/errs/registry.go`, after `CodeDataInvalid`:

```go
	// A record's schema ID is not in the registry the pipeline reads. The
	// producer registered against another registry, or none.
	CodeDataSchemaUnknown Code = "user.data.schema_unknown"
```

After `CodeSinkEncodeFailed`:

```go
	// The registry and the sink's output schema disagree. incompatible: the
	// registry refused the generated schema under the subject's
	// compatibility rule. unregistered: the subject or version the config
	// names is not there. mismatch: the handler SQL's columns do not match
	// the specified schema's fields. name_invalid: a result column's name
	// cannot be written in the sink's schema language.
	CodeSinkSchemaIncompatible Code = "user.sink.schema_incompatible"
	CodeSinkSchemaUnregistered Code = "user.sink.schema_unregistered"
	CodeSinkSchemaMismatch     Code = "user.sink.schema_mismatch"
	CodeSinkNameInvalid        Code = "user.sink.name_invalid"
```

In the `registry` map, after the `CodeDataInvalid` entry:

```go
	CodeDataSchemaUnknown: {
		CodeDataSchemaUnknown,
		"A record's schema ID is not in the registry.",
		"Check that the producer registers against the same registry the pipeline reads.",
	},
```

After the `CodeSinkEncodeFailed` entry:

```go
	CodeSinkSchemaIncompatible: {
		CodeSinkSchemaIncompatible,
		"The registry refused the output schema under the subject's compatibility rule.",
		"Change the handler SQL to keep the output shape, or change the subject's compatibility level.",
	},
	CodeSinkSchemaUnregistered: {
		CodeSinkSchemaUnregistered,
		"The subject or version that schema names is not in the registry.",
		"Register the schema, or name a version the subject holds.",
	},
	CodeSinkSchemaMismatch: {
		CodeSinkSchemaMismatch,
		"The handler SQL's output columns do not match the specified schema's fields.",
		"Rename, add or remove columns in the handler SQL to match the schema, or name a version that matches.",
	},
	CodeSinkNameInvalid: {
		CodeSinkNameInvalid,
		"A result column's name cannot be written in the sink's schema language.",
		"Alias the column in the handler SQL, as in `SELECT count(*) AS event_count`.",
	},
```

Replace the `CodeSourceSecurityInvalid` entry:

```go
	CodeSourceSecurityInvalid: {
		CodeSourceSecurityInvalid,
		"The source's TLS or SASL configuration, or the schema registry's credentials, is wrong, or a certificate file cannot be read.",
		"Check security_protocol, the sasl block, pipeline.schema_registry.auth and ssl, and that every ssl path exists and is readable by the pipeline's user.",
	},
```

- [ ] **Step 4: Regenerate the golden and run the package**

Run: `UPDATE_GOLDEN=1 go test ./internal/errs/ -run TestErrorTaxonomy_RegistryIsAppendOnly`
Expected: PASS with `golden updated`.

Run: `git diff --stat internal/errs/testdata/codes.golden`
Expected: `1 file changed, 5 insertions(+)`. No deletions. If there is a deletion, a published code went missing; restore it.

Run: `go test ./internal/errs/ -v 2>&1 | tail -30`
Expected: PASS, every test including `RegistryIsAppendOnly`, `ExitCodeMapsEveryCode`, `EveryCodeIsWellFormed` and the new one.

- [ ] **Step 5: Commit**

```bash
git add internal/errs/registry.go internal/errs/errs_test.go internal/errs/testdata/codes.golden
git commit -m "errs: five schema registry codes, and security_invalid names the registry's credentials

user.data.schema_unknown, user.sink.schema_incompatible,
user.sink.schema_unregistered, user.sink.schema_mismatch and
user.sink.name_invalid, each a user error that exits 10 and is not
retried. Nothing raises them yet; #297, #300 and #304 do. The golden
gains five lines and loses none.

If a code were missing when its raise site lands, that site would reach
for a catch-all and the runbook would have no entry."
```

---

### Task 7: The whole suite, the push, and the PR

**Files:** none new.

- [ ] **Step 1: Format and vet**

Run: `gofmt -l ./internal ./cmd; go vet ./...`
Expected: no files listed, no vet output.

- [ ] **Step 2: The unit suite**

Run: `go test -short -race ./... 2>&1 | tail -40`
Expected: every package `ok`. Look for `FAIL` and fix it before going on.

- [ ] **Step 3: The schema and the golden are current**

Run: `make schema && git status --short`
Expected: `git status` shows nothing changed. If `config.json` changed, a struct or tag moved after Task 1; commit the regeneration with `git commit -am "config: regenerate the schema"`.

- [ ] **Step 4: Push and open the PR**

```bash
git push -u origin feat/schema-registry-config
gh pr create --title "Config: the schema_registry block, the value blocks, and five error codes" --body-file - <<'EOF'
Closes #296. Part of the Schema registry v1 milestone; the first of twelve PRs, in the spec's build order.

## What

- `pipeline.schema_registry`: `url`, an optional `auth` with a username and password or a bearer token, and an optional `ssl` with the shape of `kafka.ssl`.
- A `value` block on the Kafka source and on the three Kafka sinks: `format`, one of `json`, `json_schema` or `avro`. A sink's block also takes `subject` and `schema.version`.
- `config.CheckSchemaRegistry` holds a config to the spec's rules and names the key each one is about. `validate` lists every finding at its line under `config.schema_registry`. `run` and `dev invoke` return the first as `user.config.invalid`.
- `dev invoke` refuses a registry-backed source: its fixture is JSONL.
- Five codes: `user.data.schema_unknown`, `user.sink.schema_incompatible`, `user.sink.schema_unregistered`, `user.sink.schema_mismatch`, `user.sink.name_invalid`. `user.source.security_invalid` now names the registry's credentials.

## What a user sees

Nothing changes for a config with no `value` block. A config that names `avro` or `json_schema` parses and validates, then `run` refuses it: this build reads and writes JSON only. #300 lifts the source half of that gate and #304 the sink half. The gate is `refuseUnshippedFormats` in `internal/cli/run/serde.go`.

## Deviations from the issue

- The reserved-column rule (`kafka_topic`, `kafka_partition`, `kafka_offset`) needs the fetched reader schema. This PR exports the list and the check; #300 calls it.
- `validate` leaves an unknown `format` or a malformed `version` to the schema check, which already reports them, and does not repeat the finding.

## Verification

- `go test -short -race ./...` green.
- `make schema` regenerates nothing further.
- `codes.golden` gains five lines and loses none.
- No CHANGELOG entry: nothing in this PR changes what a pipeline does. The milestone gets one with #305.
EOF
```

- [ ] **Step 5: After the first CI run, the coverage status**

Wait for the `Coverage` job. If it fails on `docs/coverage/status/`, download that run's reports, flatten them into `.coverage/`, run `make coverage-check`, and commit what changed:

```bash
gh run list --branch feat/schema-registry-config --limit 1
gh run download <run-id> --pattern 'report-*' --dir .coverage-dl
mkdir -p .coverage && find .coverage-dl -type f -exec mv {} .coverage/ \;
make coverage-check
git add docs/coverage/status docs/coverage/matrix.md
git commit -m "coverage: the status after the schema registry config tests"
git push
```

Do not edit anything under `docs/coverage/status/` by hand.

---

## Self-review

**Spec coverage.** Config section: the four structs and their tags, Task 1. The six rules, Task 2; `validate` and build time, Tasks 3 and 4. `config.Sink` reaching the DLQ and a window's sink, Tasks 2 and 4. `dev invoke`, Task 5. `make schema` and `version` as string or integer, Task 1. New error codes section: five codes, the golden, `security_invalid`'s text, Task 6. The reserved-column rule is the one deviation, recorded above with its call site in #300.

**Placeholders.** None. Every step carries its code and its expected output.

**Type consistency.** `Violation.Path []string` and `Key()` are produced in Task 2 and consumed in Tasks 3, 4 and 5 by those names. `ResolvedFormat`, `RegistryBacked` and `ResolvedSubject` are produced in Task 1 and consumed in Tasks 2, 4 and 5. The five code constants are named identically in Task 6's const block, map and test. The check id `config.schema_registry` is the same string in Task 3's code and tests.
