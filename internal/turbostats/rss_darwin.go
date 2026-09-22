package turbostats

import (
	"encoding/binary"
	"fmt"
	"os"
	"syscall"
	"unsafe"
)

// Darwin reports a process's memory three ways, and two of them are traps.
//
// getrusage's ru_maxrss is the peak since the process started. It never
// falls, so charting it draws a staircase whatever the process does -- which
// is the shape of a leak, and the reason this file exists.
//
// resident_size is a current reading that still does not fall. Darwin's
// madvise marks freed pages reusable rather than unmapping them, and they
// stay resident until the kernel wants them back. Measured: after returning
// 256 MB to the OS, resident_size went from 260.4 MB to 260.8 MB. This is
// what ps, top's RSIZE, and gopsutil's MemoryInfo all report, so reaching
// for a library here would have shipped the bug with more steps.
//
// ri_phys_footprint subtracts those reusable pages. It is what Activity
// Monitor calls Memory and what the kernel enforces a per-process limit
// against. Same measurement: 2.4 MB, then 259.4 MB, then back to 6.3 MB.
const (
	// proc_info(2). proc_pid_rusage(3) is a libSystem wrapper over it, so
	// calling the syscall directly keeps this file free of cgo.
	sysProcInfo       = 336
	procInfoPidRusage = 9
	rusageInfoV4      = 4

	// rusage_info_v0 opens with ri_uuid[16] and continues in uint64s:
	// user_time, system_time, pkg_idle_wkups, interrupt_wkups, pageins,
	// wired_size, resident_size, phys_footprint. Later flavors only append,
	// so this offset holds for every one of them.
	offPhysFootprint = 16 + 7*8
)

// residentBytesDarwin is the task's physical footprint.
//
// This counts file-backed pages that Linux's RssAnon excludes, so the two
// platforms are close rather than identical. What matters for a chart is that
// both rise and fall with what the process holds; the older darwin path only
// rose.
func residentBytesDarwin() (int64, error) {
	// Oversized on purpose: the kernel writes the flavor's struct, and a
	// future flavor that grows cannot then run past the end.
	var buf [512]byte
	_, _, errno := syscall.Syscall6(sysProcInfo, procInfoPidRusage,
		uintptr(os.Getpid()), rusageInfoV4, 0,
		uintptr(unsafe.Pointer(&buf[0])), 0)
	if errno != 0 {
		return 0, fmt.Errorf("proc_info pidrusage: %w", errno)
	}
	return int64(binary.LittleEndian.Uint64(buf[offPhysFootprint:])), nil
}
