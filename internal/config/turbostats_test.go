package config

import (
	"testing"

	"github.com/zeebo/assert"
)

func valid() *TurboStats {
	return &TurboStats{
		ID:       "bluesky-01",
		ReportTo: "https://control.turbolytics.io/v1/turbostats",
		Key:      "sfc_AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8",
	}
}

func TestTurboStats_AcceptsAWholeBlock(t *testing.T) {
	assert.Equal(t, 0, len(valid().Check()))
	assert.That(t, valid().Enabled())
	// Sixty seconds unless asked otherwise: a fleet page derives rates from
	// consecutive bundles, and a minute is dense enough to see a rate change.
	assert.Equal(t, 60, int(valid().Interval().Seconds()))
}

// Reporting is off until report_to names somewhere. An instance with no
// control plane is the ordinary case, not a misconfiguration.
func TestTurboStats_IsOffWithoutAReportTo(t *testing.T) {
	var off TurboStats
	assert.That(t, !off.Enabled())
	assert.Equal(t, 0, len(off.Check()))
}

// The control plane files a bundle under instance.id. Without one it cannot
// name what reported, so a config that enables reporting without it fails
// before the pipeline starts rather than on the first post.
func TestTurboStats_RequiresAnIDWhenReporting(t *testing.T) {
	c := valid()
	c.ID = ""
	assert.Equal(t, 1, len(c.Check()))
}

// TLS is not optional on a public network, and a device is on one. A
// plaintext report_to would put a signed bundle and its headers on the wire
// in the clear.
func TestTurboStats_RefusesPlaintextToTheInternet(t *testing.T) {
	c := valid()
	c.ReportTo = "http://control.turbolytics.io/v1/turbostats"
	assert.Equal(t, 1, len(c.Check()))

	// Loopback is how someone tests against a control plane on their own
	// machine, and there is no network to eavesdrop.
	for _, url := range []string{
		"http://127.0.0.1:8090/v1/turbostats",
		"http://localhost:8090/v1/turbostats",
		"http://[::1]:8090/v1/turbostats",
	} {
		c.ReportTo = url
		assert.Equal(t, 0, len(c.Check()))
	}
}

// A key that cannot sign is a fleet that never reports, and the operator
// finds out from a silent page rather than a failed start.
func TestTurboStats_RefusesAKeyThatCannotSign(t *testing.T) {
	for _, key := range []string{"", "not-a-credential", "sfc_short"} {
		c := valid()
		c.Key = key
		assert.That(t, len(c.Check()) > 0)
	}
}

// v1 issues one scope. A config naming another describes a permission this
// build does not implement, and accepting it silently would be worse than
// refusing it.
func TestTurboStats_AcceptsOnlyTheReadScope(t *testing.T) {
	c := valid()
	c.Allow = []string{"read"}
	assert.Equal(t, 0, len(c.Check()))

	c.Allow = []string{"read", "execute"}
	assert.Equal(t, 1, len(c.Check()))
}

// An interval under a second is a mistake that would hammer a control plane,
// and a negative one is not a duration.
func TestTurboStats_RefusesAnImpossibleInterval(t *testing.T) {
	for _, s := range []int{-1, 0} {
		c := valid()
		c.IntervalSeconds = s
		// Zero is absent, which is the default.
		if s == 0 {
			assert.Equal(t, 0, len(c.Check()))
			continue
		}
		assert.Equal(t, 1, len(c.Check()))
	}
}
