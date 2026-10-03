package turbostats

import (
	"os"
	"sync"
)

// hostname is read once: it does not change while a process runs, and
// Collect runs on every report.
var hostname = sync.OnceValue(func() string {
	h, err := os.Hostname()
	if err != nil {
		return ""
	}
	return h
})
