package config

import (
	"strings"
	"testing"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/turbolytics/sql-flow/internal/errs"
	"github.com/zeebo/assert"
)

func validMqtt() *MqttSource {
	return &MqttSource{
		Broker:   "tcp://mosquitto:1883",
		ClientID: "sqlflow-edge-1",
		Topics:   []string{"sensors/#"},
	}
}

func TestSourceMqtt_ResolvedFillsDefaults(t *testing.T) {
	coverage.Covers(t, "source.mqtt")
	r, err := validMqtt().Resolved()
	assert.NoError(t, err)
	assert.Equal(t, "tcp", r.Broker.Scheme)
	assert.Equal(t, "mosquitto:1883", r.Broker.Host)
	assert.Equal(t, uint32(3600), r.SessionExpiry)
	assert.Equal(t, uint16(65535), r.ReceiveMaximum)
	assert.DeepEqual(t, []string{"sensors/#"}, r.Topics)
}

func TestSourceMqtt_ResolvedRefusesWhatLosesTheSession(t *testing.T) {
	coverage.Covers(t, "source.mqtt")
	cases := map[string]struct {
		edit func(*MqttSource)
		want string
	}{
		"no client id":     {func(m *MqttSource) { m.ClientID = "" }, "client_id is required"},
		"no topics":        {func(m *MqttSource) { m.Topics = nil }, "topics is required"},
		"no broker":        {func(m *MqttSource) { m.Broker = "" }, "broker is required"},
		"tls scheme":       {func(m *MqttSource) { m.Broker = "ssl://b:8883" }, "scheme"},
		"negative expiry":  {func(m *MqttSource) { m.SessionExpirySeconds = -1 }, "session_expiry_seconds"},
		"expiry above uint32": {func(m *MqttSource) { m.SessionExpirySeconds = 1 << 32 }, "session_expiry_seconds"},
		"receive max high": {func(m *MqttSource) { m.ReceiveMaximum = 70000 }, "receive_maximum"},
		"receive max neg":  {func(m *MqttSource) { m.ReceiveMaximum = -5 }, "receive_maximum"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			m := validMqtt()
			c.edit(m)
			_, err := m.Resolved()
			assert.Error(t, err)
			assert.Equal(t, errs.CodeSourceInvalid, errs.CodeOf(err))
			assert.That(t, strings.Contains(err.Error(), c.want))
		})
	}
}

func TestSourceMqtt_ResolvedRefusesAMissingBlock(t *testing.T) {
	coverage.Covers(t, "source.mqtt")
	var m *MqttSource
	_, err := m.Resolved()
	assert.Error(t, err)
	assert.Equal(t, errs.CodeSourceInvalid, errs.CodeOf(err))
}

// The broker stops sending at receive_maximum unacknowledged publishes, and
// the source acknowledges only on commit. A batch larger than that never
// fills, so every flush waits out flush_interval_seconds.
func TestSourceMqtt_CheckRefusesABatchLargerThanReceiveMaximum(t *testing.T) {
	coverage.Covers(t, "source.mqtt")
	m := validMqtt()
	m.ReceiveMaximum = 100
	p := &Pipeline{BatchSize: 500, Source: Source{Type: "mqtt", Mqtt: m}}
	err := p.CheckMQTT()
	assert.Error(t, err)
	assert.Equal(t, errs.CodeConfigInvalid, errs.CodeOf(err))
	assert.That(t, strings.Contains(err.Error(), "receive_maximum 100 is below pipeline.batch_size 500"))

	p.BatchSize = 100
	assert.NoError(t, p.CheckMQTT())
}

func TestSourceMqtt_CheckIgnoresOtherSources(t *testing.T) {
	coverage.Covers(t, "source.mqtt")
	p := &Pipeline{BatchSize: 500, Source: Source{Type: "kafka"}}
	assert.NoError(t, p.CheckMQTT())
}
