package cli

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

// The config schema is generated from internal/config, so the Go types are the
// config format and the schema is an artifact of them. Edit the structs, their
// yaml tags or their doc comments, then run `make schema`.
//
// It has been three things in turn. First a copy of the Python engine's
// sqlflow/static/schemas/config.json, with a test comparing the two byte for
// byte. Then, once that engine was dropped, the spec itself, hand-edited and
// answerable to nothing. Neither survived contact with drift: a hand-written
// schema states the format a second time, and the second statement is never
// the one the engine reads. `type: webhook` was rejected by `config validate`
// while `run` accepted it, for exactly that reason.
//
// internal/schema/schema_test.go holds the committed file equal to the types,
// and holds the type enums equal to the registries the engine builds from.

func TestConfigValidation_ExampleMatchesPythonOutput(t *testing.T) {
	coverage.Covers(t, "config.validation")
	out, err := configExample()
	assert.NoError(t, err)

	// The example is the second artifact of internal/config, beside the JSON
	// schema, so `make schema` regenerates both. Adding a config field used
	// to leave this one stale with nothing but a hand edit to fix it.
	if os.Getenv("UPDATE_GOLDEN") == "1" {
		assert.NoError(t, os.WriteFile("testdata/config_example.golden", []byte(out), 0o644))
		t.Log("example updated")
		return
	}

	golden, err := os.ReadFile("testdata/config_example.golden")
	assert.NoError(t, err)
	assert.Equal(t, string(golden), out)
}

// TestConfigValidation_Examples validates the shipped example configs, mirroring
// the Python suite, which asserts every example satisfies the schema.
// Every shipped example must satisfy the schema. Checking a hand-picked few
// lets the schema drift away from the configs it is supposed to describe --
// that is how `type: webhook` came to be rejected by `config validate` while
// `run` accepted it.
func TestConfigValidation_Examples(t *testing.T) {
	coverage.Covers(t, "config.validation")
	for _, path := range exampleConfigs(t) {
		t.Run(filepath.Base(path), func(t *testing.T) {
			assert.NoError(t, validateConfig(path))
		})
	}
}

func TestConfigValidation_RendersTemplateBeforeValidating(t *testing.T) {
	coverage.Covers(t, "config.validation")
	// batch_size arrives only through the Jinja2 default filter; validating
	// the raw file would see a template expression where an integer is required.
	path := writeTempConfig(t, `
pipeline:
  batch_size: {{ SQLFLOW_BATCH_SIZE|default(100) }}
  source:
    type: kafka
    kafka:
      brokers: [localhost:9092]
      group_id: test
      auto_offset_reset: earliest
      topics: [input]
  handler:
    type: 'handlers.InferredMemBatch'
    sql: SELECT 1
  sink:
    type: console
`)
	assert.NoError(t, validateConfig(path))
}

func TestConfigValidation_RejectsMissingPipeline(t *testing.T) {
	coverage.Covers(t, "config.validation")
	path := writeTempConfig(t, "commands: []\n")

	err := validateConfig(path)
	assert.Error(t, err)
	assert.That(t, strings.Contains(err.Error(), "pipeline"))
}

func TestConfigValidation_RejectsBadEnum(t *testing.T) {
	coverage.Covers(t, "config.validation")
	path := writeTempConfig(t, `
pipeline:
  batch_size: 1
  source:
    type: carrier-pigeon
  handler:
    type: 'handlers.InferredMemBatch'
    sql: SELECT 1
  sink:
    type: console
`)

	err := validateConfig(path)
	assert.Error(t, err)
	assert.That(t, strings.Contains(err.Error(), "type"))
}

func TestConfigValidation_RejectsMissingRequiredHandlerSQL(t *testing.T) {
	coverage.Covers(t, "config.validation")
	path := writeTempConfig(t, `
pipeline:
  batch_size: 1
  source:
    type: kafka
    kafka:
      brokers: [localhost:9092]
      group_id: test
      auto_offset_reset: earliest
      topics: [input]
  handler:
    type: 'handlers.InferredMemBatch'
  sink:
    type: console
`)

	err := validateConfig(path)
	assert.Error(t, err)
	assert.That(t, strings.Contains(err.Error(), "sql"))
}

func TestConfigValidation_ReportsMissingFile(t *testing.T) {
	coverage.Covers(t, "config.validation")
	err := validateConfig(filepath.Join(t.TempDir(), "nope.yml"))
	assert.Error(t, err)
}

func TestConfigValidation_AcceptsStatePath(t *testing.T) {
	coverage.Covers(t, "config.validation")
	path := writeTempConfig(t, `
pipeline:
  batch_size: 10
  state:
    path: /var/lib/sqlflow/state.db
  source:
    type: kafka
    kafka:
      brokers: [localhost:9092]
      group_id: g
      auto_offset_reset: earliest
      topics: [t]
  handler:
    type: handlers.InferredMemBatch
    sql: SELECT 1
  sink:
    type: noop
`)

	assert.NoError(t, validateConfig(path))
}

func writeTempConfig(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yml")
	assert.NoError(t, os.WriteFile(path, []byte(body), 0o644))
	return path
}
