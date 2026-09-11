package validate

import (
	"context"
	"github.com/turbolytics/sql-flow/internal/errs"
	"os"
	"testing"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

// The regression this package exists for. The reporter in #120 provided
// SQLFLOW_AZURE_STORAGE_CONNECTION_STRING and read
// SQLFLOW_AZURE_CONNECTION_STRING. Nothing said so, and the failure surfaced
// as an authentication error days later.
func TestValidateNoSideEffects_Issue120TypoIsNamed(t *testing.T) {
	coverage.Covers(t, "validate.template", "validate.schema")

	src, err := os.ReadFile("testdata/issue-120.yml")
	assert.NoError(t, err)

	t.Setenv("SQLFLOW_AZURE_STORAGE_CONNECTION_STRING", "DefaultEndpointsProtocol=https;")

	rep, err := Validate(context.Background(), Request{
		Path:   "testdata/issue-120.yml",
		Config: string(src),
	})
	assert.NoError(t, err)
	assert.That(t, !rep.OK)

	var found bool
	for _, d := range rep.Diagnostics {
		if d.Code != "user.config.template_undefined" {
			continue
		}
		found = true
		assert.That(t, contains(d.DidYouMean, "SQLFLOW_AZURE_STORAGE_CONNECTION_STRING"))
		assert.Equal(t, 8, d.Position.Line)
	}
	assert.That(t, found)

	// The unread half is what turns the missing half into a misspelling.
	assert.That(t, contains(rep.Variables.Unused, "SQLFLOW_AZURE_STORAGE_CONNECTION_STRING"))
}

// Every diagnostic in one pass: a config that is wrong twice reports twice.
func TestValidateReportsEveryDiagnostic_TwoFaultsOneRun(t *testing.T) {
	src := `commands:
  name: not a list
pipeline:
  batch_size: 1
  source:
    type: kafka
    kafka:
      brokers: ["localhost:9092"]
      group_id: g
      auto_offset_reset: earliest
      topics: ["t"]
  handler:
    type: handlers.InferredMemBatch
    sql: SELECT {{ MISSING_VAR }}
  sink:
    type: console
`
	rep, err := Validate(context.Background(), Request{Config: src})
	assert.NoError(t, err)
	assert.That(t, !rep.OK)

	var template, schema bool
	for _, d := range rep.Diagnostics {
		switch d.Code {
		case "user.config.template_undefined":
			template = true
		case "user.config.invalid", "user.config.parse_failed":
			schema = true
		}
	}
	assert.That(t, template)
	assert.That(t, schema)
}

// A template that will not parse cannot be rendered, so the schema check has
// nothing to inspect. It must report skipped, never pass.
func TestValidateSkipIsExplicit_UnparseableTemplateSkipsSchema(t *testing.T) {
	rep, err := Validate(context.Background(), Request{Config: "x: {{ unclosed\n"})
	assert.NoError(t, err)

	assert.That(t, !rep.OK)
	assert.Equal(t, StatusSkipped, checkStatus(t, rep, "config.schema"))
}

func TestValidateNoSideEffects_CleanConfigIsOK(t *testing.T) {
	rep, err := Validate(context.Background(), Request{Config: validSchemaConfig})
	assert.NoError(t, err)
	assert.That(t, rep.OK)
}

// A variable with no default is a declared required input, so an empty
// substitution is not the config being wrong. checkTemplate already says so
// and warns; the schema check used to fail on the consequence, and only the
// error reached the reader because the CLI prints errors alone.
//
// The effect was that `config validate` could not run on a clean shell,
// which is what #142 asked for, and a connection string had to carry a fake
// default to pass. Nobody should put a bunk DSN in a production pipeline to
// satisfy a linter.
func TestValidateSchema_UnsuppliedVariableIsAWarningNotAnError(t *testing.T) {
	coverage.Covers(t, "validate.schema", "validate.template")
	rep, err := Validate(context.Background(), Request{
		Path: "dsn.yml",
		Config: `
pipeline:
  batch_size: 10
  source:
    type: kafka
    kafka:
      brokers: [localhost:9092]
      group_id: g
      auto_offset_reset: earliest
      topics: [t]
  handler:
    type: 'handlers.InferredMemBatch'
    sql: SELECT 1
  sink:
    type: clickhouse
    clickhouse:
      dsn: {{ SQLFLOW_CLICKHOUSE_DSN }}
      table: events
`,
	})
	assert.NoError(t, err)
	assert.That(t, rep.OK)

	var warnedAboutTheVariable, anyError bool
	for _, d := range rep.Diagnostics {
		if d.Severity == SeverityError {
			anyError = true
		}
		if d.Code == string(errs.CodeConfigTemplateUndefined) {
			warnedAboutTheVariable = true
		}
	}
	// The reader still learns which variable to set: the warning survives,
	// and the demoted schema diagnostic now carries the action.
	assert.That(t, warnedAboutTheVariable)
	assert.That(t, !anyError)
}

// The demotion is attributed by line, so a null the author wrote themselves,
// on a line with no template reference, still fails.
func TestValidateSchema_ANullTheAuthorWroteStillFails(t *testing.T) {
	coverage.Covers(t, "validate.schema")
	rep, err := Validate(context.Background(), Request{
		Path: "blank.yml",
		Config: `
pipeline:
  batch_size: 10
  source:
    type: kafka
    kafka:
      brokers: [localhost:9092]
      group_id: g
      auto_offset_reset: earliest
      topics: [t]
  handler:
    type: 'handlers.InferredMemBatch'
    sql: SELECT 1
  sink:
    type: clickhouse
    clickhouse:
      dsn:
      table: events
`,
	})
	assert.NoError(t, err)
	assert.That(t, !rep.OK)
}
