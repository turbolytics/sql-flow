package validate

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

// labelsConfig is a pipeline whose turbostats block the test fills. The
// reporting lines are their own slot so the same labels can be checked with
// reporting on and off.
const labelsConfig = `pipeline:
  name: labels
  turbostats:
%s    labels:
      %s
  source:
    type: kafka
    kafka:
      brokers: ["localhost:9092"]
      group_id: g
      auto_offset_reset: earliest
      topics: ["t"]
  handler:
    type: handlers.InferredMemBatch
    sql: SELECT 1
  sink:
    type: noop
`

const reporting = `    id: gw-1
    report_to: https://control.turbolytics.io/v1/turbostats
    key: sfc_AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8
`

func validateLabels(t *testing.T, reportingLines, label string) Report {
	t.Helper()
	rep, err := Validate(context.Background(), Request{
		Path:   "labels.yml",
		Config: fmt.Sprintf(labelsConfig, reportingLines, label),
	})
	assert.NoError(t, err)
	return rep
}

func labelDiagnostics(rep Report) []Diagnostic {
	var out []Diagnostic
	for _, d := range rep.Diagnostics {
		if strings.Contains(d.Message, "turbostats.labels") {
			out = append(out, d)
		}
	}
	return out
}

// A label the bundle cannot carry fails validation before the pipeline runs.
func TestValidateSchema_RefusesALabelTheBundleCannotCarry(t *testing.T) {
	coverage.Covers(t, "validate.schema")
	found := labelDiagnostics(validateLabels(t, reporting, "Region: eu-west"))
	assert.Equal(t, 1, len(found))
	assert.Equal(t, SeverityError, found[0].Severity)
}

// The label rules hold with reporting off. A config that would be refused
// the moment someone sets report_to has the defect now, and finding it then
// means finding it in production.
func TestValidateSchema_ChecksLabelsWithReportingOff(t *testing.T) {
	coverage.Covers(t, "validate.schema")
	found := labelDiagnostics(validateLabels(t, "", "Region: eu-west"))
	assert.Equal(t, 1, len(found))
}

// A diagnostic points at the key that is wrong. An editor jumps there, and
// an operator reading a failure is told which line to fix.
func TestValidateSchema_ALabelViolationPointsAtTheLabelsLine(t *testing.T) {
	coverage.Covers(t, "validate.schema")
	found := labelDiagnostics(validateLabels(t, reporting, "Region: eu-west"))
	assert.Equal(t, 1, len(found))
	pos := found[0].Position
	assert.That(t, pos != nil)
	// `labels:` is the seventh line of the rendered config.
	assert.Equal(t, 7, pos.Line)
	assert.Equal(t, "/pipeline/turbostats/labels", found[0].Context)
}

// A bounded set passes, with reporting on or off.
func TestValidateSchema_AcceptsABoundedLabelSet(t *testing.T) {
	coverage.Covers(t, "validate.schema")
	assert.Equal(t, 0, len(labelDiagnostics(validateLabels(t, reporting, "region: eu_west"))))
	assert.Equal(t, 0, len(labelDiagnostics(validateLabels(t, "", "region: eu_west"))))
}
