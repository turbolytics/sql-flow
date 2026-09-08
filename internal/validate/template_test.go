package validate

import (
	"testing"

	"github.com/zeebo/assert"
)

func TestValidateTemplate_CollectsReferencesWithPositions(t *testing.T) {
	src := "brokers: [{{ SQLFLOW_KAFKA_BROKERS|default('localhost:9092') }}]\n" +
		"topic: \"{{ SQLFLOW_TOPIC }}\"\n"

	refs, complete, err := scanTemplate(src)
	assert.NoError(t, err)
	assert.That(t, complete)
	assert.Equal(t, 2, len(refs))

	assert.Equal(t, "SQLFLOW_KAFKA_BROKERS", refs[0].name)
	assert.Equal(t, 1, refs[0].line)
	assert.Equal(t, "SQLFLOW_TOPIC", refs[1].name)
	assert.Equal(t, 2, refs[1].line)
}

// A filter's arguments can themselves be variables, and missing one is the
// same fault as missing a bare reference.
func TestValidateTemplate_CollectsFilterArguments(t *testing.T) {
	refs, complete, err := scanTemplate("x: {{ A|default(B) }}\n")
	assert.NoError(t, err)
	assert.That(t, complete)

	names := refNames(refs)
	assert.That(t, contains(names, "A"))
	assert.That(t, contains(names, "B"))
}

// A control structure binds its own names and hides its body from this walk.
// Reporting incomplete is what stops the unused set from lying.
func TestValidateTemplate_ControlStructureMarksIncomplete(t *testing.T) {
	src := "{% for t in SQLFLOW_TOPICS %}\n- {{ t }}\n{% endfor %}\n"

	_, complete, err := scanTemplate(src)
	assert.NoError(t, err)
	assert.That(t, !complete)
}

func TestValidateTemplate_ParseErrorIsReturned(t *testing.T) {
	_, _, err := scanTemplate("x: {{ unclosed \n")
	assert.Error(t, err)
}

// Issue #120: the reporter provided SQLFLOW_AZURE_STORAGE_CONNECTION_STRING
// and the config read SQLFLOW_AZURE_CONNECTION_STRING. gonja rendered the
// missing name as an empty string with a nil error, and the resulting
// authentication failure named nothing.
func TestValidateTemplate_MissingVariableNamesItsNeighbour(t *testing.T) {
	src := "sql: SET conn = '{{ SQLFLOW_AZURE_CONNECTION_STRING }}';\n"
	provided := map[string]string{
		"SQLFLOW_AZURE_STORAGE_CONNECTION_STRING": "real",
	}

	var rep Report
	checkTemplate(src, provided, &rep)
	rep.Finish()

	assert.That(t, !rep.OK)
	assert.Equal(t, 1, len(rep.Diagnostics))

	d := rep.Diagnostics[0]
	assert.Equal(t, "user.config.template_undefined", d.Code)
	assert.Equal(t, SeverityError, d.Severity)
	assert.Equal(t, 1, d.Position.Line)
	assert.DeepEqual(t, []string{"SQLFLOW_AZURE_STORAGE_CONNECTION_STRING"}, d.DidYouMean)
	assert.That(t, d.Action != "")

	assert.DeepEqual(t, []string{"SQLFLOW_AZURE_CONNECTION_STRING"}, rep.Variables.Missing)
	assert.DeepEqual(t, []string{"SQLFLOW_AZURE_STORAGE_CONNECTION_STRING"}, rep.Variables.Unused)
}

func TestValidateTemplate_AllVariablesDefinedPasses(t *testing.T) {
	var rep Report
	checkTemplate("topic: {{ SQLFLOW_TOPIC }}\n",
		map[string]string{"SQLFLOW_TOPIC": "events"}, &rep)
	rep.Finish()

	assert.That(t, rep.OK)
	assert.Equal(t, StatusPass, checkStatus(t, rep, "config.template"))
	assert.Equal(t, 0, len(rep.Variables.Unused))
}

// An incomplete walk cannot prove a variable is unread, so it must not claim
// one is. The missing set stays trustworthy either way: a name the walk found
// really is referenced.
func TestValidateTemplate_IncompleteWalkSuppressesUnused(t *testing.T) {
	src := "{% for t in SQLFLOW_TOPICS %}\n- {{ t }}\n{% endfor %}\n"

	var rep Report
	checkTemplate(src, map[string]string{"SQLFLOW_UNREAD": "x"}, &rep)

	assert.Equal(t, 0, len(rep.Variables.Unused))
	assert.Equal(t, StatusSkipped, checkStatus(t, rep, "config.template.unused"))
}

// Every diagnostic in one pass. Two missing variables must not cost two runs.
func TestValidateTemplate_ReportsEveryMissingVariable(t *testing.T) {
	var rep Report
	checkTemplate("a: {{ ONE }}\nb: {{ TWO }}\n", nil, &rep)

	assert.Equal(t, 2, len(rep.Diagnostics))
}

func checkStatus(t *testing.T, rep Report, id string) Status {
	t.Helper()
	for _, c := range rep.Checks {
		if c.ID == id {
			return c.Status
		}
	}
	t.Fatalf("no check %q in %v", id, rep.Checks)
	return ""
}

func refNames(refs []ref) []string {
	out := make([]string, 0, len(refs))
	for _, r := range refs {
		out = append(out, r.name)
	}
	return out
}

func contains(haystack []string, needle string) bool {
	for _, s := range haystack {
		if s == needle {
			return true
		}
	}
	return false
}
