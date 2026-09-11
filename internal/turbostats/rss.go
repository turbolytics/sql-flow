package turbostats

import (
	"bufio"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"syscall"
)

// ResidentAnonBytes is the process's anonymous resident memory.
//
// That is the figure a native leak moves: Go's heap profiler cannot see a
// buffer DuckDB allocated across the ADBC boundary, and duckdb_memory() does
// not track it either, so the only honest instrument is the process itself.
//
// Linux reads RssAnon, which excludes file-backed pages and is exact. Other
// platforms fall back to peak resident size from getrusage, which only ever
// rises, so it is a weaker signal there; a leak still shows as growth between
// two readings, since a steady process has a steady peak.
func ResidentAnonBytes() (int64, error) {
	if runtime.GOOS == "linux" {
		f, err := os.Open("/proc/self/status")
		if err != nil {
			return 0, err
		}
		defer f.Close()
		sc := bufio.NewScanner(f)
		for sc.Scan() {
			line := sc.Text()
			if !strings.HasPrefix(line, "RssAnon:") {
				continue
			}
			fields := strings.Fields(line)
			kb, err := strconv.ParseInt(fields[1], 10, 64)
			if err != nil {
				return 0, err
			}
			return kb << 10, nil
		}
		return 0, fmt.Errorf("RssAnon not found in /proc/self/status")
	}
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0, err
	}
	maxrss := int64(ru.Maxrss)
	if runtime.GOOS != "darwin" {
		maxrss <<= 10 // kilobytes everywhere but darwin, which reports bytes
	}
	return maxrss, nil
}
