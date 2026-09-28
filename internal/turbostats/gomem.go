package turbostats

import "runtime/metrics"

// goMemorySamples are the runtime metrics GoMemory reads. runtime/metrics
// rather than runtime.ReadMemStats, because ReadMemStats stops the world, and
// Collect runs on every report and every GET /turbostats/v1.
var goMemorySamples = []string{
	"/memory/classes/total:bytes",
	"/memory/classes/heap/released:bytes",
	"/gc/heap/live:bytes",
}

// GoMemory is what the Go runtime holds from the operating system, and the
// heap the last garbage collection found live. Either is zero when the
// runtime does not report it, which the bundle sends as absent.
func GoMemory() (retained, heapLive int64) {
	s := make([]metrics.Sample, len(goMemorySamples))
	for i, name := range goMemorySamples {
		s[i].Name = name
	}
	metrics.Read(s)

	total, released, live := uint64Of(s[0]), uint64Of(s[1]), uint64Of(s[2])
	if total > released {
		retained = int64(total - released)
	}
	return retained, int64(live)
}

// uint64Of is a sample's value, or zero when this runtime does not know the
// metric. A Go release that renamed one would otherwise panic the collector.
func uint64Of(s metrics.Sample) uint64 {
	if s.Value.Kind() != metrics.KindUint64 {
		return 0
	}
	return s.Value.Uint64()
}
