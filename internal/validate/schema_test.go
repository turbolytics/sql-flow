package validate

import (
	"testing"

	"github.com/zeebo/assert"
)

// validSchemaConfig satisfies the config schema in full. Shared so a test
// about one fault does not trip over an unrelated missing property.
const validSchemaConfig = `pipeline:
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
    sql: SELECT 1
  sink:
    type: console
`

// Issue #120's first fault: commands written as a mapping, not a sequence.
func TestValidateSchema_CommandsMustBeASequence(t *testing.T) {
	rendered := []byte(`commands:
  name: load extensions
  sql: INSTALL azure;
pipeline:
  source:
    type: kafka
  handler:
    type: handlers.InferredMemBatch
    sql: SELECT 1
  sink:
    type: console
`)

	var rep Report
	checkSchema(rendered, &rep)
	rep.Finish()

	assert.That(t, !rep.OK)
	assert.That(t, len(rep.Diagnostics) > 0)
	assert.Equal(t, StatusFail, checkStatus(t, rep, "config.schema"))

	// The position must name the offending key, not the top of the file.
	d := rep.Diagnostics[0]
	assert.Equal(t, "/commands", d.Context)
	assert.That(t, d.Position != nil)
	assert.Equal(t, 1, d.Position.Line)
}

// jsonschema builds an error's causes from a map, so the same config yields
// the same faults in a different order on every run. A report a machine reads
// has to be stable, or it cannot be diffed, cached, or asserted on.
func TestValidateReport_DiagnosticOrderIsStable(t *testing.T) {
	rendered := []byte(`commands:
  name: load extensions
  sql: INSTALL azure;
pipeline:
  source:
    type: kafka
  handler:
    type: handlers.InferredMemBatch
  sink:
    type: console
`)

	var first []string
	for i := 0; i < 20; i++ {
		var rep Report
		checkSchema(rendered, &rep)
		rep.Finish()

		got := make([]string, 0, len(rep.Diagnostics))
		for _, d := range rep.Diagnostics {
			got = append(got, d.Context)
		}
		if i == 0 {
			first = got
			assert.That(t, len(first) > 1)
			continue
		}
		assert.DeepEqual(t, first, got)
	}
}

// batch_size and auto_offset_reset are required by the schema, though a
// pipeline runs without them. That gap is #231's subject, not this test's.
func TestValidateSchema_ValidConfigPasses(t *testing.T) {
	rendered := []byte(validSchemaConfig)

	var rep Report
	checkSchema(rendered, &rep)
	rep.Finish()

	assert.That(t, rep.OK)
	assert.Equal(t, StatusPass, checkStatus(t, rep, "config.schema"))
}

func TestValidateSchema_MalformedYAMLIsOneDiagnostic(t *testing.T) {
	var rep Report
	checkSchema([]byte("pipeline:\n  - : :\n"), &rep)
	rep.Finish()

	assert.That(t, !rep.OK)
	assert.Equal(t, 1, len(rep.Diagnostics))
}
