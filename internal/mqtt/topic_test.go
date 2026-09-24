package mqtt

import (
	"testing"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

func TestSourceMqtt_MatchesTopicFilters(t *testing.T) {
	coverage.Covers(t, "source.mqtt")
	cases := []struct {
		filter, topic string
		want          bool
	}{
		{"sensors/#", "sensors/pi-1/temperature", true},
		{"sensors/#", "sensors", true}, // # also matches the parent level (§4.7.1.2)
		{"sensors/+/temperature", "sensors/pi-1/temperature", true},
		{"sensors/+/temperature", "sensors/pi-1/humidity", false},
		{"sensors/+", "sensors/pi-1/temperature", false},
		{"sensors/pi-1/temperature", "sensors/pi-1/temperature", true},
		{"#", "$SYS/broker/uptime", false}, // wildcards never match $ topics (§4.7.2)
		{"+/broker/uptime", "$SYS/broker/uptime", false},
		{"$SYS/#", "$SYS/broker/uptime", true},
	}
	for _, c := range cases {
		assert.Equal(t, c.want, matches(c.filter, c.topic))
	}
}

func TestSourceMqtt_FilterForNamesTheFirstMatch(t *testing.T) {
	coverage.Covers(t, "source.mqtt")
	filters := []string{"alerts/#", "sensors/#"}
	assert.Equal(t, "sensors/#", filterFor(filters, "sensors/pi-1/co2"))
	// A publish no filter matches still needs a key for its mark. The first
	// filter serves: the source acknowledges by sequence, not by key.
	assert.Equal(t, "alerts/#", filterFor(filters, "other/x"))
}
