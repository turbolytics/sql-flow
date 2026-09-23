package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/turbolytics/sql-flow/internal/coverage"
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
	assert.Equal(t, 0, len(valid().Check([]string{"pipeline", "turbostats"})))
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
	assert.Equal(t, 0, len(off.Check([]string{"pipeline", "turbostats"})))
}

// The control plane files a bundle under instance.id. Without one it cannot
// name what reported, so a config that enables reporting without it fails
// before the pipeline starts rather than on the first post.
func TestTurboStats_RequiresAnIDWhenReporting(t *testing.T) {
	c := valid()
	c.ID = ""
	assert.Equal(t, 1, len(c.Check([]string{"pipeline", "turbostats"})))
}

// TLS is not optional on a public network, and a device is on one. A
// plaintext report_to would put a signed bundle and its headers on the wire
// in the clear.
func TestTurboStats_RefusesPlaintextToTheInternet(t *testing.T) {
	c := valid()
	c.ReportTo = "http://control.turbolytics.io/v1/turbostats"
	assert.Equal(t, 1, len(c.Check([]string{"pipeline", "turbostats"})))

	// Loopback is how someone tests against a control plane on their own
	// machine, and there is no network to eavesdrop.
	for _, url := range []string{
		"http://127.0.0.1:8090/v1/turbostats",
		"http://localhost:8090/v1/turbostats",
		"http://[::1]:8090/v1/turbostats",
	} {
		c.ReportTo = url
		assert.Equal(t, 0, len(c.Check([]string{"pipeline", "turbostats"})))
	}
}

// A key that cannot sign is a fleet that never reports, and the operator
// finds out from a silent page rather than a failed start.
func TestTurboStats_RefusesAKeyThatCannotSign(t *testing.T) {
	for _, key := range []string{"", "not-a-credential", "sfc_short"} {
		c := valid()
		c.Key = key
		assert.That(t, len(c.Check([]string{"pipeline", "turbostats"})) > 0)
	}
}

// An interval under a second is a mistake that would hammer a control plane,
// and a negative one is not a duration.
func TestTurboStats_RefusesAnImpossibleInterval(t *testing.T) {
	for _, s := range []int{-1, 0} {
		c := valid()
		c.IntervalSeconds = s
		// Zero is absent, which is the default.
		if s == 0 {
			assert.Equal(t, 0, len(c.Check([]string{"pipeline", "turbostats"})))
			continue
		}
		assert.Equal(t, 1, len(c.Check([]string{"pipeline", "turbostats"})))
	}
}

// A scope ceiling is refused, not ignored.
//
// turbostats.allow was parsed, validated against the one scope v1 issues, and
// then read by nothing: not the bundle, not a header, not the reporter. An
// operator who wrote `allow: [read]` got no protection and every sign of
// having some. There are no commands yet for a ceiling to constrain, so the
// key is gone, and the loader's strict decoding makes a leftover one an error
// at startup rather than a comfort. It comes back in the change that makes it
// do something.
func TestTurboStats_AScopeCeilingIsRefusedUntilItIsEnforced(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pipeline.yml")
	assert.NoError(t, os.WriteFile(path, []byte(`
pipeline:
  name: p
  turbostats:
    id: p-01
    report_to: https://control.example.com/v1/turbostats
    key: sfc_AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8
    allow: [read]
  source:
    type: webhook
    webhook:
      addr: 127.0.0.1:0
  handler:
    type: handlers.InferredMemBatch
    sql: SELECT 1 AS n
  sink:
    type: noop
`), 0o600))

	_, _, err := LoadRendered(path, map[string]string{})
	assert.Error(t, err)
	assert.That(t, strings.Contains(err.Error(), "allow"))
}

// The rules exist so a report keeps a fixed shape and a bounded size, and
// so a label never shadows a field the contract defines.
func TestTurboStatsLabels_RefusesWhatBreaksTheShape(t *testing.T) {
	coverage.Covers(t, "config.validation")
	tooMany := map[string]string{}
	for i := 0; i < 11; i++ {
		tooMany[fmt.Sprintf("k%d", i)] = "v"
	}
	cases := map[string]map[string]string{
		"too many":      tooMany,
		"reserved name": {"version": "v1"},
		"upper case":    {"Region": "eu"},
		"leading digit": {"1region": "eu"},
		"long key":      {strings.Repeat("k", 33): "eu"},
		"long value":    {"region": strings.Repeat("v", 65)},
	}
	for name, labels := range cases {
		t.Run(name, func(t *testing.T) {
			ts := valid()
			ts.Labels = labels
			assert.That(t, len(ts.Check([]string{"pipeline", "turbostats"})) > 0)
		})
	}
}

// The ordinary case passes, and so does no labels at all.
func TestTurboStatsLabels_AcceptsABoundedSet(t *testing.T) {
	coverage.Covers(t, "config.validation")
	ts := valid()
	ts.Labels = map[string]string{"region": "eu_west", "tenant": "acme", "env": "prod"}
	assert.Equal(t, 0, len(ts.Check([]string{"pipeline", "turbostats"})))

	ts.Labels = nil
	assert.Equal(t, 0, len(ts.Check([]string{"pipeline", "turbostats"})))
}

// Labels are validated even with reporting off: a config that would be
// refused the moment someone sets report_to is a config with a defect in
// it now.
func TestTurboStatsLabels_AreCheckedWithReportingOff(t *testing.T) {
	coverage.Covers(t, "config.validation")
	ts := &TurboStats{Labels: map[string]string{"Region": "eu"}}
	assert.That(t, len(ts.Check([]string{"pipeline", "turbostats"})) > 0)
}

// A config with no turbostats block checks clean rather than panicking.
// The label rules read the block before asking whether reporting is on, so
// the nil case stopped being absorbed by Enabled().
func TestTurboStats_ChecksANilBlock(t *testing.T) {
	coverage.Covers(t, "config.validation")
	var ts *TurboStats
	assert.Equal(t, 0, len(ts.Check([]string{"serve", "turbostats"})))
}
