package validate

import (
	"encoding/json"
	"testing"

	"github.com/zeebo/assert"
)

// The control plane is pull-based: a validate job crosses a queue, so both
// ends of the contract must survive JSON with nothing lost (#178).
func TestValidateReport_SurvivesJSONRoundTrip(t *testing.T) {
	req := Request{
		Path:   "pipeline.yml",
		Config: "pipeline:\n  name: x\n",
		Vars:   map[string]string{"SQLFLOW_TOPIC": "events"},
	}

	var gotReq Request
	encoded, err := json.Marshal(req)
	assert.NoError(t, err)
	assert.NoError(t, json.Unmarshal(encoded, &gotReq))
	assert.DeepEqual(t, req, gotReq)

	rep := Report{
		Config: "pipeline.yml",
		Checks: []Check{
			{ID: "config.template", Status: StatusFail},
			{ID: "sql.bind", Status: StatusSkipped, Reason: "no schema without a sample"},
		},
		Diagnostics: []Diagnostic{{
			Code:       "user.config.template_undefined",
			Class:      "user",
			Severity:   SeverityError,
			Message:    "template variable SQLFLOW_TOPI is not defined",
			Position:   &Position{Source: "config", Line: 6, Column: 15},
			DidYouMean: []string{"SQLFLOW_TOPIC"},
			Action:     "Define the variable, or correct the name.",
		}},
		Variables: &Variables{
			Referenced: []string{"SQLFLOW_TOPI"},
			Provided:   []string{"SQLFLOW_TOPIC"},
			Missing:    []string{"SQLFLOW_TOPI"},
			Unused:     []string{"SQLFLOW_TOPIC"},
		},
	}
	rep.Finish()
	assert.That(t, !rep.OK)

	var gotRep Report
	encoded, err = json.Marshal(rep)
	assert.NoError(t, err)
	assert.NoError(t, json.Unmarshal(encoded, &gotRep))
	assert.DeepEqual(t, rep, gotRep)
}

// A warning is advice. It must not fail the run, or every convention hint
// becomes a CI gate nobody can turn off.
func TestValidateReport_WarningKeepsOK(t *testing.T) {
	rep := Report{Diagnostics: []Diagnostic{{
		Code:     "user.config.invalid",
		Severity: SeverityWarning,
		Message:  "sqlcommand sink never reads sqlflow_sink_batch",
	}}}
	rep.Finish()
	assert.That(t, rep.OK)
}
