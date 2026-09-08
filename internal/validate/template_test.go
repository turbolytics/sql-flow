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
