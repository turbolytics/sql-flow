package turbostats

import (
	"testing"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

func TestHashConfig_IsSHA256OfTheRenderedText(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	got := HashConfig([]byte("pipeline:\n  name: x\n"))
	// sha256 of that exact text, computed once with
	// printf 'pipeline:\n  name: x\n' | shasum -a 256
	assert.Equal(t, "sha256:2d1b7510db148fa0dfa4e13e50f1bd30c4ccceeae5dc5f5f20cce0c8034c6fcd", got)
}

func TestHashConfig_DiffersWhenTheTextDoes(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	assert.That(t, HashConfig([]byte("a")) != HashConfig([]byte("b")))
	assert.Equal(t, 7+64, len(HashConfig([]byte("a"))))
}
