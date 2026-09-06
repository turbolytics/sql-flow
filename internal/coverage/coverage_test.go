package coverage

import (
	"fmt"
	"strings"
	"testing"

	"github.com/zeebo/assert"
)

// The generator parses these lines with a fixed regex. A format drift is a
// silent loss of every cell the marker feeds, so the exact bytes are pinned
// on this side too.
func TestCoverageInvariant_EmitsTheStructuredMarker(t *testing.T) {
	rec := &recorder{}
	Invariant(rec, "sink.flush.keeps_batch", "sink.clickhouse")

	assert.DeepEqual(t,
		[]string{"COVERS invariant=sink.flush.keeps_batch integration=sink.clickhouse"},
		rec.lines)
}

func TestCoverageCovers_EmitsOneLinePerFeature(t *testing.T) {
	rec := &recorder{}
	Covers(rec, "sink.clickhouse", "source.kafka")

	assert.DeepEqual(t, []string{
		"COVERS sink.clickhouse",
		"COVERS source.kafka",
	}, rec.lines)
}

// A structured marker must not also read as a plain one. The generator drops
// the plain match when a structured one is present, and this is the other
// half of that contract: the two lines cannot be confused for each other.
func TestCoverageInvariant_DoesNotLookLikeAFeatureMarker(t *testing.T) {
	rec := &recorder{}
	Invariant(rec, "sink.flush.keeps_batch", "sink.clickhouse")

	assert.True(t, strings.HasPrefix(rec.lines[0], "COVERS invariant="))
}

// recorder stands in for *testing.T so a test can read what was logged.
// testing.TB has an unexported method, so it cannot be implemented outside
// the testing package; embedding it satisfies the compiler, and the two
// methods these functions call are overridden below.
type recorder struct {
	testing.TB
	lines []string
}

func (r *recorder) Helper() {}

func (r *recorder) Logf(format string, args ...any) {
	r.lines = append(r.lines, fmt.Sprintf(format, args...))
}
