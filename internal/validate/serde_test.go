package validate

import (
	"context"
	"fmt"
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

func diagnosticsAt(rep Report, context string) int {
	n := 0
	for _, d := range rep.Diagnostics {
		if d.Context == context {
			n++
		}
	}
	return n
}

// The spec's example passes the check.
func TestValidateSchema_SchemaRegistryExamplePasses(t *testing.T) {
	coverage.Covers(t, "validate.schema")
	rep, _ := validateRegistry(t, registryBlock, avroValue, avroValue)
	if !rep.OK {
		t.Fatalf("not ok: %v", rep.Diagnostics)
	}
	assert.Equal(t, StatusPass, checkStatus(t, rep, schemaRegistryCheck))
	assert.Equal(t, 0, len(registryDiagnostics(rep)))
}

// A config with no value block anywhere passes too, and reports the check
// as run, not skipped.
func TestValidateSchema_SchemaRegistryAbsentPasses(t *testing.T) {
	coverage.Covers(t, "validate.schema")
	rep, _ := validateRegistry(t, "", "", "")
	assert.That(t, rep.OK)
	assert.Equal(t, StatusPass, checkStatus(t, rep, schemaRegistryCheck))
}

// A rule finding is an error at the line of the key it names, with the key
// in the message and the path in the context.
func TestValidateSchema_SchemaRegistryFindingNamesTheLine(t *testing.T) {
	coverage.Covers(t, "validate.schema")
	rep, cfg := validateRegistry(t, "", avroValue, "")
	assert.That(t, !rep.OK)
	assert.Equal(t, StatusFail, checkStatus(t, rep, schemaRegistryCheck))
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
		if at := diagnosticsAt(rep, "/pipeline/sink/kafka/value/schema/version"); at != 1 {
			t.Fatalf("version %s: %d diagnostics at the version, want 1: %v", bad, at, rep.Diagnostics)
		}
	}
}

// An unknown format is the schema's finding, reported once.
func TestValidateSchema_SchemaRegistryUnknownFormatReportedOnce(t *testing.T) {
	coverage.Covers(t, "validate.schema")
	rep, _ := validateRegistry(t, registryBlock, "      value:\n        format: protobuf", "")
	assert.That(t, !rep.OK)
	assert.Equal(t, 1, diagnosticsAt(rep, "/pipeline/source/kafka/value/format"))
}

// A schema error at a path never hides a rule the schema cannot state. A
// typo under auth is the schema's finding, and "auth needs both username
// and password" at the same path is the rule's: the reader gets both in one
// run, not the second after fixing the first.
func TestValidateSchema_SchemaRegistrySchemaErrorDoesNotHideARule(t *testing.T) {
	coverage.Covers(t, "validate.schema")
	registry := registryBlock + "\n    auth:\n      user: u\n      password: p"
	rep, _ := validateRegistry(t, registry, "", "")
	assert.That(t, !rep.OK)
	assert.Equal(t, StatusFail, checkStatus(t, rep, "config.schema"))
	assert.Equal(t, StatusFail, checkStatus(t, rep, schemaRegistryCheck))
	assert.Equal(t, 2, diagnosticsAt(rep, "/pipeline/schema_registry/auth"))
	found := false
	for _, d := range rep.Diagnostics {
		if strings.Contains(d.Message, "auth needs both username and password") {
			found = true
		}
	}
	assert.That(t, found)
}

// A missing url is reported once. The schema reports a missing required key
// at the block, and the rule names the key, so the two paths differ.
func TestValidateSchema_SchemaRegistryMissingURLReportedOnce(t *testing.T) {
	coverage.Covers(t, "validate.schema")
	rep, _ := validateRegistry(t, "  schema_registry:\n    ssl:\n      ca_location: /ca.pem", "", "")
	assert.That(t, !rep.OK)
	assert.Equal(t, StatusFail, checkStatus(t, rep, schemaRegistryCheck))
	about := 0
	for _, d := range rep.Diagnostics {
		if strings.Contains(d.Message, "url") {
			about++
		}
	}
	assert.Equal(t, 1, about)

	// An empty url is the schema's too, at the key itself.
	rep, _ = validateRegistry(t, "  schema_registry:\n    url: \"\"", "", "")
	assert.That(t, !rep.OK)
	assert.Equal(t, 1, diagnosticsAt(rep, "/pipeline/schema_registry/url"))
}

// The check fails whenever a rule is broken, even when the schema printed
// the only finding. A per-check table must not show it green.
func TestValidateSchema_SchemaRegistryFailsEvenWhenTheSchemaPrintedIt(t *testing.T) {
	coverage.Covers(t, "validate.schema")
	rep, _ := validateRegistry(t, registryBlock, "      value:\n        format: protobuf", "")
	assert.Equal(t, StatusFail, checkStatus(t, rep, schemaRegistryCheck))
	assert.Equal(t, 1, diagnosticsAt(rep, "/pipeline/source/kafka/value/format"))
}

// validate and run agree on a version's spelling. A quoted number is a
// number; a sign or a leading zero is refused, once.
func TestValidateSchema_SchemaRegistryVersionSpellingsMatchRun(t *testing.T) {
	coverage.Covers(t, "validate.schema")
	rep, _ := validateRegistry(t, registryBlock, avroValue, fmt.Sprintf(avroPinned, `"3"`))
	if !rep.OK {
		t.Fatalf(`version "3": %v`, rep.Diagnostics)
	}
	for _, bad := range []string{"+3", "03", `"03"`, `"+3"`} {
		rep, _ := validateRegistry(t, registryBlock, avroValue, fmt.Sprintf(avroPinned, bad))
		assert.That(t, !rep.OK)
		assert.Equal(t, StatusFail, checkStatus(t, rep, schemaRegistryCheck))
		if at := diagnosticsAt(rep, "/pipeline/sink/kafka/value/schema/version"); at != 1 {
			t.Fatalf("version %s: %d diagnostics at the version, want 1: %v", bad, at, rep.Diagnostics)
		}
	}
}
