package eventtime

import (
	"fmt"
	"testing"
	"time"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

// One instant, in every encoding the extractor reads, from the shapes real
// producers send: a bare number, a number in a string, a nested object.
func TestEventTime_EveryFormatReadsTheSameInstant(t *testing.T) {
	coverage.Covers(t, "source.websocket")
	want := time.Date(2026, 9, 25, 12, 34, 56, 789000000, time.UTC)

	// The numeric fixtures are derived from the instant rather than typed,
	// after a hand-typed epoch turned out to be a day off: the point is that
	// every encoding names the same instant, not that anyone can do the
	// arithmetic in their head.
	cases := []struct {
		format  Format
		payload string
	}{
		{UnixSeconds, fmt.Sprintf(`{"t": %d}`, want.Unix())},
		{UnixMilliseconds, fmt.Sprintf(`{"t": %d}`, want.UnixMilli())},
		{UnixMicroseconds, fmt.Sprintf(`{"t": %d}`, want.UnixMicro())},
		{UnixNanoseconds, fmt.Sprintf(`{"t": %d}`, want.UnixNano())},
		{UnixMicroseconds, fmt.Sprintf(`{"t": "%d"}`, want.UnixMicro())},
		{RFC3339, `{"t": "2026-09-25T12:34:56.789Z"}`},
		{RFC3339, `{"t": "2026-09-25T14:34:56.789+02:00"}`},
	}
	for _, c := range cases {
		ex, err := New("t", c.format)
		assert.NoError(t, err)
		got, err := ex.Extract([]byte(c.payload))
		assert.NoError(t, err)
		if c.format == UnixSeconds {
			// Seconds carry no sub-second part; the instant is truncated.
			assert.Equal(t, want.Truncate(time.Second).UnixNano(), got)
			continue
		}
		assert.Equal(t, want.UnixNano(), got)
	}
}

// The Bluesky Jetstream shape: time_us at the top level, in microseconds.
func TestEventTime_ReadsJetstreamsTimeUs(t *testing.T) {
	coverage.Covers(t, "source.websocket")
	ex, err := New("time_us", UnixMicroseconds)
	assert.NoError(t, err)
	got, err := ex.Extract([]byte(`{"did":"did:plc:x","time_us":1790253296789000,"kind":"commit"}`))
	assert.NoError(t, err)
	assert.Equal(t, int64(1790253296789000)*int64(time.Microsecond), got)
	assert.Equal(t, "time_us", ex.Basis())
}

// A dotted path walks into nested objects.
func TestEventTime_DottedPathWalksNestedObjects(t *testing.T) {
	coverage.Covers(t, "source.websocket")
	ex, err := New("commit.record.createdAt", RFC3339)
	assert.NoError(t, err)
	got, err := ex.Extract([]byte(`{"commit":{"record":{"createdAt":"2026-09-25T12:00:00Z"}}}`))
	assert.NoError(t, err)
	assert.Equal(t, time.Date(2026, 9, 25, 12, 0, 0, 0, time.UTC).UnixNano(), got)
	assert.Equal(t, "commit.record.createdAt", ex.Basis())
}

// A record with no usable time is an error, never a guess: absent, the wrong
// type, or unparseable in the configured format. The caller decides what
// that means; the extractor does not substitute arrival.
func TestEventTime_ARecordWithNoUsableTimeIsAnError(t *testing.T) {
	coverage.Covers(t, "source.websocket")
	num, _ := New("t", UnixMicroseconds)
	rfc, _ := New("t", RFC3339)

	for _, c := range []struct {
		ex      *Extractor
		payload string
	}{
		{num, `{"other": 1}`},
		{num, `{"t": null}`},
		{num, `{"t": "yesterday"}`},
		{num, `{"t": 12.5}`},
		{num, `{"t": {"nested": 1}}`},
		{rfc, `{"t": 1790253296}`},
		{rfc, `{"t": "2026-13-45"}`},
		{num, `not json at all`},
	} {
		_, err := c.ex.Extract([]byte(c.payload))
		assert.Error(t, err)
	}
}

// The extractor does not judge whether an instant is believable. A time in
// 2099 parses; refusing it is the engine's placement rule, applied after,
// so that a producer with a wrong clock is refused by one rule whatever its
// protocol.
func TestEventTime_DoesNotJudgeTheInstant(t *testing.T) {
	coverage.Covers(t, "source.websocket")
	ex, _ := New("t", RFC3339)
	got, err := ex.Extract([]byte(`{"t": "2099-01-01T00:00:00Z"}`))
	assert.NoError(t, err)
	assert.Equal(t, time.Date(2099, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano(), got)
}

// A bad configuration is refused when the extractor is built, so a typo in
// the format or an empty path fails at start rather than on the first frame.
func TestEventTime_ABadConfigurationIsRefusedAtBuild(t *testing.T) {
	coverage.Covers(t, "source.websocket")
	for _, c := range []struct{ path, format string }{
		{"", "unix_us"},
		{"   ", "unix_us"},
		{"a..b", "unix_us"},
		{"t", "micros"},
		{"t", ""},
	} {
		_, err := New(c.path, Format(c.format))
		assert.Error(t, err)
	}
}
