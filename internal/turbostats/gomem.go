package turbostats

import (
	"math"
	"runtime/debug"
	"runtime/metrics"
)

// goMemorySamples are the runtime metrics GoMemory reads. runtime/metrics
// rather than runtime.ReadMemStats, because ReadMemStats stops the world, and
// Collect runs on every report and every GET /turbostats/v1.
var goMemorySamples = []string{
	"/memory/classes/total:bytes",
	"/memory/classes/heap/released:bytes",
	"/gc/heap/live:bytes",
	"/gc/cycles/total:gc-cycles",
}

// GoMem is what the Go runtime says about its memory. A field is zero when
// the runtime does not report it, which the bundle sends as absent.
type GoMem struct {
	// Retained is what the runtime holds from the operating system.
	Retained int64
	// HeapLive is the heap the last collection found live.
	HeapLive int64
	// GCCycles is collections completed since the process started.
	GCCycles int64
	// HeapLimit is GOMEMLIMIT, and zero when none is set.
	HeapLimit int64
}

// GoMemory reads the Go runtime's memory.
func GoMemory() GoMem {
	s := make([]metrics.Sample, len(goMemorySamples))
	for i, name := range goMemorySamples {
		s[i].Name = name
	}
	metrics.Read(s)

	total, released := uint64Of(s[0]), uint64Of(s[1])
	m := GoMem{
		HeapLive:  int64(uint64Of(s[2])),
		GCCycles:  int64(uint64Of(s[3])),
		HeapLimit: heapLimit(),
	}
	if total > released {
		m.Retained = int64(total - released)
	}
	return m
}

// heapLimit is GOMEMLIMIT. A negative argument reads the limit without
// changing it, and math.MaxInt64 is the runtime's spelling of no limit.
func heapLimit() int64 {
	if l := debug.SetMemoryLimit(-1); l != math.MaxInt64 {
		return l
	}
	return 0
}

// uint64Of is a sample's value, or zero when this runtime does not know the
// metric. A Go release that renamed one would otherwise panic the collector.
func uint64Of(s metrics.Sample) uint64 {
	if s.Value.Kind() != metrics.KindUint64 {
		return 0
	}
	return s.Value.Uint64()
}
