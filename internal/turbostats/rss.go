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

// ResidentAnonBytes is the process's resident memory now.
//
// That is the figure a native leak moves: Go's heap profiler cannot see a
// buffer DuckDB allocated across the ADBC boundary, and duckdb_memory() does
// not track it either, so the only honest instrument is the process itself.
//
// Every platform reports a current reading, never a peak. Linux reads
// RssAnon, which excludes file-backed pages and is exact. Darwin asks mach
// for the task's resident size.
//
// It used to fall back to getrusage's ru_maxrss off Linux, which is a
// high-water mark: it only ever rises, so a process that spiked once reported
// that spike forever. That was tolerable when the only reader was a soak test
// watching for growth. It is not tolerable now that a control plane charts
// it, because a monotonic line is exactly the shape of a leak, and a fleet
// page that draws one where none exists costs an operator an afternoon.
func ResidentAnonBytes() (int64, error) {
	if runtime.GOOS == "darwin" {
		return residentBytesDarwin()
	}
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
	// Everything else: the peak, which is a weaker signal, and said to be so
	// where it is read. No third platform ships today.
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0, err
	}
	return int64(ru.Maxrss) << 10, nil
}
