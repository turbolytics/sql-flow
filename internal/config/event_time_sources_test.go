package config

import (
	"testing"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/turbolytics/sql-flow/internal/errs"
	"github.com/zeebo/assert"
)

// Every source accepts the same event_time block, checked the same way at
// start: absent is nil, a good block is an extractor whose basis is the
// path, and a bad path or format is a source error naming the source.
func TestConfigValidation_EventTimeIsTheSameBlockOnEverySource(t *testing.T) {
	coverage.Covers(t, "config.validation")
	good := &EventTimeField{Path: "ts", Format: "unix_ms"}
	bad := &EventTimeField{Path: "ts", Format: "millis"}

	// Kafka: nil receiver and absent block are both "the record's timestamp".
	ex, err := (*KafkaSource)(nil).ResolvedEventTime()
	assert.NoError(t, err)
	assert.That(t, ex == nil)
	ex, err = (&KafkaSource{}).ResolvedEventTime()
	assert.NoError(t, err)
	assert.That(t, ex == nil)
	ex, err = (&KafkaSource{EventTime: good}).ResolvedEventTime()
	assert.NoError(t, err)
	assert.Equal(t, "ts", ex.Basis())
	_, err = (&KafkaSource{EventTime: bad}).ResolvedEventTime()
	assert.Equal(t, errs.CodeSourceInvalid, errs.CodeOf(err))

	// Webhook: the same.
	ex, err = (&WebhookSource{}).ResolvedEventTime()
	assert.NoError(t, err)
	assert.That(t, ex == nil)
	ex, err = (&WebhookSource{EventTime: good}).ResolvedEventTime()
	assert.NoError(t, err)
	assert.Equal(t, "ts", ex.Basis())
	_, err = (&WebhookSource{EventTime: bad}).ResolvedEventTime()
	assert.Equal(t, errs.CodeSourceInvalid, errs.CodeOf(err))

	// MQTT: rides on the block's own Resolved.
	base := MqttSource{Broker: "tcp://b:1883", ClientID: "c", Topics: []string{"t"}}
	r, err := base.Resolved()
	assert.NoError(t, err)
	assert.That(t, r.EventTime == nil)
	with := base
	with.EventTime = good
	r, err = with.Resolved()
	assert.NoError(t, err)
	assert.Equal(t, "ts", r.EventTime.Basis())
	broken := base
	broken.EventTime = bad
	_, err = broken.Resolved()
	assert.Equal(t, errs.CodeSourceInvalid, errs.CodeOf(err))
}
