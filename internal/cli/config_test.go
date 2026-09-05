package cli

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/zeebo/assert"
)

// schemas/config.json is now the config spec itself, not a copy of one. It was
// mirrored from the Python engine's sqlflow/static/schemas/config.json, and a
// test compared the two byte for byte; with that engine gone there is nothing
// left to drift from, and editing the schema here no longer needs a second
// edit somewhere else.

func TestConfigValidation_ExampleMatchesPythonOutput(t *testing.T) {
	golden, err := os.ReadFile("testdata/config_example.golden")
	assert.NoError(t, err)

	out, err := configExample()
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
	for _, path := range exampleConfigs(t) {
		t.Run(filepath.Base(path), func(t *testing.T) {
			assert.NoError(t, validateConfig(path))
		})
	}
}

func TestConfigValidation_RendersTemplateBeforeValidating(t *testing.T) {
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
	path := writeTempConfig(t, "commands: []\n")

	err := validateConfig(path)
	assert.Error(t, err)
	assert.That(t, strings.Contains(err.Error(), "pipeline"))
}

func TestConfigValidation_RejectsBadEnum(t *testing.T) {
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
	err := validateConfig(filepath.Join(t.TempDir(), "nope.yml"))
	assert.Error(t, err)
}

func TestConfigValidation_AcceptsStatePath(t *testing.T) {
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
