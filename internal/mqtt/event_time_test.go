package mqtt

import (
	"testing"
	"time"

	"github.com/turbolytics/sql-flow/internal/core"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/turbolytics/sql-flow/internal/eventtime"
	"github.com/zeebo/assert"
	"go.uber.org/zap"
)

// A publish's event time is arrival unless the source was told where it is;
// then it is the payload's, or missing where the payload says nothing
// usable. stamp runs without a broker.
func TestSourceMqtt_EventTimeComesFromThePayloadWhenTold(t *testing.T) {
	coverage.Covers(t, "source.mqtt")
	plain := &Source{logger: zap.NewNop()}
	before := time.Now().UnixNano()
	at := plain.stamp([]byte(`{"ts": 1790253296}`))
	assert.That(t, at >= before && at <= time.Now().UnixNano())
	assert.Equal(t, core.EventBasisArrival, plain.EventTimeBasis())

	ex, err := eventtime.New("ts", eventtime.UnixSeconds)
	assert.NoError(t, err)
	told := &Source{logger: zap.NewNop(), cfg: Config{EventTime: ex}}
	assert.Equal(t, int64(1790253296)*int64(time.Second), told.stamp([]byte(`{"ts": 1790253296}`)))
	assert.Equal(t, core.EventTimeMissing, told.stamp([]byte(`{"other": 1}`)))
	assert.Equal(t, "ts", told.EventTimeBasis())
}
