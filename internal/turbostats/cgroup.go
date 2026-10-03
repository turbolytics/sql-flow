package turbostats

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// cgroupRoot is where Linux mounts the cgroup filesystem. A container with
// its own cgroup namespace sees its own cgroup here.
const cgroupRoot = "/sys/fs/cgroup"

// MemoryLimit is the memory limit of the cgroup under root, and false when
// there is none or it cannot be read: no cgroup filesystem, an unlimited
// cgroup, or a file this does not understand.
//
// It reads a file per call rather than once at startup, because an
// orchestrator can resize a running container.
func MemoryLimit(root string) (int64, bool) {
	// cgroup v2: one file, "max" when unlimited.
	if raw, err := os.ReadFile(filepath.Join(root, "memory.max")); err == nil {
		return parseLimit(raw)
	}
	// cgroup v1.
	if raw, err := os.ReadFile(filepath.Join(root, "memory", "memory.limit_in_bytes")); err == nil {
		return parseLimit(raw)
	}
	return 0, false
}

// parseLimit reads a limit file. cgroup v1 reports "unlimited" as the
// largest page-aligned int64, so anything at or above 2^62 is no limit.
func parseLimit(raw []byte) (int64, bool) {
	s := strings.TrimSpace(string(raw))
	if s == "max" {
		return 0, false
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || n <= 0 || n >= 1<<62 {
		return 0, false
	}
	return n, true
}
