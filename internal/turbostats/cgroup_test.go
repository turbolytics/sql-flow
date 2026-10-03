package turbostats

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

func writeCgroup(t *testing.T, rel, content string) string {
	t.Helper()
	root := t.TempDir()
	path := filepath.Join(root, rel)
	assert.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
	assert.NoError(t, os.WriteFile(path, []byte(content), 0o644))
	return root
}

// A container's limit predicts the kernel killing it, and no limit is not a
// limit of zero.
func TestMemoryLimit_ReadsCgroupV2AndV1(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	for name, tt := range map[string]struct {
		rel, content string
		want         int64
		ok           bool
	}{
		"v2 limit":     {"memory.max", "536870912\n", 536870912, true},
		"v2 unlimited": {"memory.max", "max\n", 0, false},
		"v1 limit":     {"memory/memory.limit_in_bytes", "536870912\n", 536870912, true},
		// cgroup v1 spells "unlimited" as the largest page-aligned int64.
		"v1 unlimited": {"memory/memory.limit_in_bytes", "9223372036854771712\n", 0, false},
		"garbage":      {"memory.max", "lots\n", 0, false},
	} {
		t.Run(name, func(t *testing.T) {
			got, ok := MemoryLimit(writeCgroup(t, tt.rel, tt.content))
			assert.Equal(t, tt.ok, ok)
			assert.Equal(t, tt.want, got)
		})
	}
}

// A host with no cgroup filesystem, such as a laptop, has no limit to read.
func TestMemoryLimit_NoCgroupIsNoLimit(t *testing.T) {
	_, ok := MemoryLimit(t.TempDir())
	assert.That(t, !ok)
}
