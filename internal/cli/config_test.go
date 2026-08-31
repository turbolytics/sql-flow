package cli

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/zeebo/assert"
)

// TestEmbeddedSchemaMatchesPython guards the copy of the config schema that
// go:embed requires against drifting from the Python engine's original, which
// lives outside the package and so cannot be embedded directly.
func TestEmbeddedSchemaMatchesPython(t *testing.T) {
	original, err := os.ReadFile("../../sqlflow/static/schemas/config.json")
	assert.NoError(t, err)
	assert.Equal(t, string(original), string(configSchemaJSON))
}

func TestConfigExample_MatchesPythonOutput(t *testing.T) {
	golden, err := os.ReadFile("testdata/config_example.golden")
	assert.NoError(t, err)

	out, err := configExample()
	assert.NoError(t, err)
	assert.Equal(t, string(golden), out)
}

// TestConfigValidate_Examples validates the shipped example configs, mirroring
// the Python suite, which asserts every example satisfies the schema.
func TestConfigValidate_Examples(t *testing.T) {
	for _, name := range []string{
		"basic.agg.mem.yml",
		"basic.agg.yml",
		"kafka.structured.mem.yml",
		"tumbling.window.yml",
	} {
		t.Run(name, func(t *testing.T) {
			err := validateConfig(filepath.Join("../../dev/config/examples", name))
			assert.NoError(t, err)
		})
	}
}

func TestConfigValidate_RendersTemplateBeforeValidating(t *testing.T) {
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

func TestConfigValidate_RejectsMissingPipeline(t *testing.T) {
	path := writeTempConfig(t, "commands: []\n")

	err := validateConfig(path)
	assert.Error(t, err)
	assert.That(t, strings.Contains(err.Error(), "pipeline"))
}

func TestConfigValidate_RejectsBadEnum(t *testing.T) {
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

func TestConfigValidate_RejectsMissingRequiredHandlerSQL(t *testing.T) {
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

func TestConfigValidate_ReportsMissingFile(t *testing.T) {
	err := validateConfig(filepath.Join(t.TempDir(), "nope.yml"))
	assert.Error(t, err)
}

func writeTempConfig(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yml")
	assert.NoError(t, os.WriteFile(path, []byte(body), 0o644))
	return path
}
