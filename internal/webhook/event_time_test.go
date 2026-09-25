package webhook

import (
	"testing"
	"time"

	"github.com/turbolytics/sql-flow/internal/core"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/turbolytics/sql-flow/internal/eventtime"
	"github.com/zeebo/assert"
	"go.uber.org/zap"
)

// A body's event time is arrival unless the source was told where it is;
// then it is the body's, or missing where the body says nothing usable.
// stamp runs without a listener, so this needs no port.
func TestSourceWebhook_EventTimeComesFromTheBodyWhenTold(t *testing.T) {
	coverage.Covers(t, "source.webhook")
	plain := &Source{logger: zap.NewNop()}
	before := time.Now().UnixNano()
	at := plain.stamp([]byte(`{"ts": 1790253296789}`))
	assert.That(t, at >= before && at <= time.Now().UnixNano())
	assert.Equal(t, core.EventBasisArrival, plain.EventTimeBasis())

	ex, err := eventtime.New("ts", eventtime.UnixMilliseconds)
	assert.NoError(t, err)
	told := &Source{logger: zap.NewNop(), eventTime: ex}
	assert.Equal(t, int64(1790253296789)*int64(time.Millisecond), told.stamp([]byte(`{"ts": 1790253296789}`)))
	assert.Equal(t, core.EventTimeMissing, told.stamp([]byte(`{"other": 1}`)))
	assert.Equal(t, "ts", told.EventTimeBasis())
}
