package validate

import (
	"context"
	"strings"
	"testing"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/turbolytics/sql-flow/internal/errs"
	"github.com/zeebo/assert"
)

const validServeConfig = `serve:
  auth:
    tokens:
      - name: page
        token: page-token
  datasets:
    - name: posts
      params:
        - {name: lang, type: string}
      sql: SELECT * FROM t WHERE lang = coalesce($lang, lang)
`

func errorsOf(rep Report) []Diagnostic {
	var out []Diagnostic
	for _, d := range rep.Diagnostics {
		if d.Severity == SeverityError {
			out = append(out, d)
		}
	}
	return out
}

func TestValidateServe_AValidServeFilePasses(t *testing.T) {
	coverage.Covers(t, "cli.serve", "validate.schema")

	rep, err := Validate(context.Background(), Request{Config: validServeConfig})
	assert.NoError(t, err)
	assert.That(t, rep.OK)
	assert.Equal(t, StatusPass, checkStatus(t, rep, "config.schema"))
	assert.Equal(t, StatusPass, checkStatus(t, rep, "serve.rules"))

	// A serve file has no pipeline, so no pipeline check reports on it. A
	// pass for a check that could not apply reads as proof.
	for _, c := range rep.Checks {
		if strings.HasPrefix(c.ID, "pipeline.") {
			t.Fatalf("a serve file reports pipeline check %s: %s", c.ID, c.Status)
		}
	}
}

// A serve file is held to the serve schema, not the pipeline one: a missing
// pipeline key is not an error, and an unknown key under serve is.
func TestValidateServe_AServeFileIsCheckedAgainstTheServeSchema(t *testing.T) {
	coverage.Covers(t, "cli.serve", "validate.schema")

	rep, err := Validate(context.Background(), Request{
		Config: strings.Replace(validServeConfig, "  datasets:", "  dataset_typo: 1\n  datasets:", 1),
	})
	assert.NoError(t, err)
	assert.That(t, !rep.OK)

	errors := errorsOf(rep)
	assert.Equal(t, 1, len(errors))
	assert.Equal(t, "/serve", errors[0].Context)
	assert.That(t, strings.Contains(errors[0].Message, "dataset_typo"))
	// Strict decoding fails too, so the rules report skipped, never pass.
	assert.Equal(t, StatusSkipped, checkStatus(t, rep, "serve.rules"))
}

// A file with both keys is a pipeline, checked exactly as before serve
// existed, so serve is an unknown property.
func TestValidateServe_AFileWithBothKeysIsAPipeline(t *testing.T) {
	coverage.Covers(t, "cli.serve", "validate.schema")

	rep, err := Validate(context.Background(), Request{
		Config: validSchemaConfig + "serve: {}\n",
	})
	assert.NoError(t, err)
	assert.That(t, !rep.OK)

	var named bool
	for _, d := range errorsOf(rep) {
		if strings.Contains(d.Message, "serve") {
			named = true
		}
	}
	assert.That(t, named)
	for _, c := range rep.Checks {
		assert.That(t, c.ID != "serve.rules")
	}
}

// A rule the schema cannot express still reports its code and its line.
func TestValidateServe_ARuleViolationNamesItsLine(t *testing.T) {
	coverage.Covers(t, "cli.serve", "validate.schema")

	rep, err := Validate(context.Background(), Request{
		Config: validServeConfig + "  limits:\n    rate_limit:\n      burst: 5\n",
	})
	assert.NoError(t, err)
	assert.That(t, !rep.OK)
	assert.Equal(t, StatusFail, checkStatus(t, rep, "serve.rules"))

	errors := errorsOf(rep)
	assert.Equal(t, 1, len(errors))
	assert.Equal(t, string(errs.CodeConfigServeReserved), errors[0].Code)
	assert.Equal(t, "/serve/limits/rate_limit", errors[0].Context)
	assert.That(t, errors[0].Position != nil)
	// validServeConfig is ten lines; rate_limit is the twelfth.
	assert.Equal(t, 12, errors[0].Position.Line)
}

// A param type outside the enum breaks the schema and a rule at the same key.
// The reader gets one line for one fault.
func TestValidateServe_ASchemaFaultIsNotRepeatedByTheRules(t *testing.T) {
	coverage.Covers(t, "cli.serve", "validate.schema")

	rep, err := Validate(context.Background(), Request{
		Config: strings.Replace(validServeConfig, "type: string", "type: text", 1),
	})
	assert.NoError(t, err)

	errors := errorsOf(rep)
	assert.Equal(t, 1, len(errors))
	assert.Equal(t, "/serve/datasets/0/params/0/type", errors[0].Context)
	assert.Equal(t, StatusFail, checkStatus(t, rep, "serve.rules"))
}

// CI validates without secrets. A token rendered from an unset variable is
// empty, and that is the environment being incomplete, not the config being
// wrong, so validate warns. serve refuses to start on the same file.
func TestValidateServe_AnUnsetTokenVariableIsAWarning(t *testing.T) {
	coverage.Covers(t, "cli.serve", "validate.template")

	rep, err := Validate(context.Background(), Request{
		Config: strings.Replace(validServeConfig,
			"token: page-token", `token: "{{ SQLFLOW_SERVE_TOKEN_UNSET_IN_THIS_TEST }}"`, 1),
	})
	assert.NoError(t, err)
	assert.That(t, rep.OK)

	var demoted bool
	for _, d := range rep.Diagnostics {
		if d.Code == string(errs.CodeConfigInvalid) && strings.Contains(d.Message, "empty value") {
			assert.Equal(t, SeverityWarning, d.Severity)
			demoted = true
		}
	}
	assert.That(t, demoted)
}
