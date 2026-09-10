package cli

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/turbolytics/sql-flow/internal/validate"
	"github.com/zeebo/assert"
)

func writeConfig(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "pipeline.yml")
	assert.NoError(t, os.WriteFile(path, []byte(body), 0o644))
	return path
}

const runnableConfig = `pipeline:
  name: p
  batch_size: 50
  source:
    type: kafka
    kafka:
      brokers: ["localhost:9092"]
      group_id: g
      auto_offset_reset: earliest
      topics: ["t"]
  handler:
    type: handlers.InferredMemBatch
    sql: SELECT * FROM batch
  sink:
    type: console
`

// JSON is the contract an agent reads, so it has to parse back into the same
// type the package publishes.
func TestValidateCommand_JSONIsTheContract(t *testing.T) {
	t.Setenv("SQLFLOW_TOPIC", "events")
	path := writeConfig(t, "pipeline: {{ SQLFLOW_TOPICC }}\n")

	cmd := newValidateCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{path, "--json"})

	err := cmd.Execute()
	assert.Error(t, err)

	var rep validate.Report
	assert.NoError(t, json.Unmarshal(out.Bytes(), &rep))
	assert.That(t, !rep.OK)
	assert.That(t, len(rep.Diagnostics) > 0)
	assert.That(t, len(rep.Checks) > 0)
}

func TestValidateCommand_ValidConfigSucceeds(t *testing.T) {
	path := writeConfig(t, runnableConfig)

	cmd := newValidateCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetArgs([]string{path})

	assert.NoError(t, cmd.Execute())
	assert.That(t, bytes.Contains(out.Bytes(), []byte("valid")))
}

// A skipped check must be visible to someone reading the terminal, or the
// word "valid" claims more than the tool checked.
func TestValidateCommand_TextSaysWhatItSkipped(t *testing.T) {
	path := writeConfig(t, "x: {{ unclosed\n")

	cmd := newValidateCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{path})

	assert.Error(t, cmd.Execute())
	assert.That(t, bytes.Contains(out.Bytes(), []byte("skipped config.schema")))
}

func TestValidateCommand_MissingFileFails(t *testing.T) {
	cmd := newValidateCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{filepath.Join(t.TempDir(), "nope.yml")})

	assert.Error(t, cmd.Execute())
}
