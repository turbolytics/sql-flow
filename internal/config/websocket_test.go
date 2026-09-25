package config

import (
	"testing"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/turbolytics/sql-flow/internal/errs"
	"github.com/zeebo/assert"
)

// A websocket block with no event_time is arrival, as before; one with it is
// checked at start: a path, and a format the extractor understands.
func TestSourceWebsocket_EventTimeIsValidatedAtStart(t *testing.T) {
	coverage.Covers(t, "config.validation")

	_, err := (*WebsocketSource)(nil).Resolved()
	assert.Equal(t, errs.CodeSourceInvalid, errs.CodeOf(err))

	_, err = (&WebsocketSource{}).Resolved()
	assert.Equal(t, errs.CodeSourceInvalid, errs.CodeOf(err))

	r, err := (&WebsocketSource{URI: "wss://x"}).Resolved()
	assert.NoError(t, err)
	assert.That(t, r.EventTime == nil)

	r, err = (&WebsocketSource{URI: "wss://x", EventTime: &EventTimeField{Path: "time_us", Format: "unix_us"}}).Resolved()
	assert.NoError(t, err)
	assert.That(t, r.EventTime != nil)
	assert.Equal(t, "time_us", r.EventTime.Basis())

	for _, bad := range []*EventTimeField{
		{Path: "", Format: "unix_us"},
		{Path: "time_us", Format: "micros"},
		{Path: "time_us", Format: ""},
		{Path: "a..b", Format: "unix_us"},
	} {
		_, err := (&WebsocketSource{URI: "wss://x", EventTime: bad}).Resolved()
		assert.Equal(t, errs.CodeSourceInvalid, errs.CodeOf(err))
	}
}
