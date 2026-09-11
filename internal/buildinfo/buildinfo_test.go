package buildinfo

import (
	"testing"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

// An unstamped build must still identify itself, because a bundle from a
// developer's binary should say "dev" rather than an empty string.
func TestBuildInfo_UnstampedDefaults(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	assert.Equal(t, "dev", Version)
	assert.Equal(t, "unknown", Commit)
}
