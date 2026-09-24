package mqtt

import "strings"

// matches reports whether a topic filter matches a topic name, per MQTT 5
// section 4.7.
func matches(filter, topic string) bool {
	f := strings.Split(filter, "/")
	t := strings.Split(topic, "/")
	// A leading wildcard never matches a $ topic, so a "#" subscription does
	// not receive the broker's $SYS statistics as readings.
	if strings.HasPrefix(t[0], "$") && (f[0] == "#" || f[0] == "+") {
		return false
	}
	for i, level := range f {
		if level == "#" {
			return true
		}
		if i >= len(t) {
			return false
		}
		if level != "+" && level != t[i] {
			return false
		}
	}
	return len(f) == len(t)
}

// filterFor names the subscription a publish arrived on. Marks are keyed by
// it rather than by the topic, because one topic per device and metric would
// make the marks, and the offsets table, grow without bound.
func filterFor(filters []string, topic string) string {
	for _, f := range filters {
		if matches(f, topic) {
			return f
		}
	}
	return filters[0]
}
