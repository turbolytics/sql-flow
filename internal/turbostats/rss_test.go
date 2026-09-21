package turbostats

import (
	"runtime"
	"runtime/debug"
	"testing"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

// The reading falls when memory is returned.
//
// This is the whole difference between a current reading and a peak. The
// darwin path used to report getrusage's ru_maxrss, a high-water mark that
// only ever rises, so a process that spiked once charted a staircase forever
// -- which is exactly the shape of a leak. A soak test comparing two readings
// tolerated that. A control plane drawing a line does not.
func TestResidentAnonBytes_FallsWhenMemoryIsReturned(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")

	before, err := ResidentAnonBytes()
	assert.NoError(t, err)

	// Something large enough to dwarf the noise, touched so the pages are
	// real rather than reserved.
	const size = 256 << 20
	hog := make([]byte, size)
	for i := 0; i < len(hog); i += 4096 {
		hog[i] = 1
	}
	peak, err := ResidentAnonBytes()
	assert.NoError(t, err)
	assert.That(t, peak > before+(200<<20))

	runtime.KeepAlive(hog)
	hog = nil
	runtime.GC()
	debug.FreeOSMemory()

	after, err := ResidentAnonBytes()
	assert.NoError(t, err)
	// A peak would still report the spike. A current reading gives it back.
	assert.That(t, after < peak-(100<<20))
}

func TestResidentAnonBytes_IsPlausible(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	got, err := ResidentAnonBytes()
	assert.NoError(t, err)
	// A Go test binary holds more than a megabyte and less than a terabyte.
	assert.That(t, got > 1<<20)
	assert.That(t, got < 1<<40)
}
