package validate

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

const mqttPipeline = `pipeline:
  batch_size: %d
  source:
    type: mqtt
    mqtt:
      broker: tcp://mosquitto:1883
      client_id: %s
      topics: ["sensors/#"]
      receive_maximum: 1000
  handler:
    type: handlers.InferredMemBatch
    sql: SELECT * FROM batch
  sink:
    type: noop
`

func validateMqtt(t *testing.T, batch int, clientID string) Report {
	t.Helper()
	rep, err := Validate(context.Background(), Request{
		Path:   "mqtt.yml",
		Config: fmt.Sprintf(mqttPipeline, batch, clientID),
	})
	assert.NoError(t, err)
	return rep
}

func TestValidateSchema_MqttBatchAboveReceiveMaximumFails(t *testing.T) {
	coverage.Covers(t, "validate.schema")
	rep := validateMqtt(t, 5000, "edge-1")
	assert.That(t, !rep.OK)
	assert.Equal(t, StatusFail, checkStatus(t, rep, "source.mqtt"))
	found := false
	for _, d := range rep.Diagnostics {
		if d.Severity == SeverityError && strings.Contains(d.Message, "receive_maximum 1000 is below pipeline.batch_size 5000") {
			found = true
		}
	}
	assert.That(t, found)
}

func TestValidateSchema_MqttEmptyClientIDFails(t *testing.T) {
	coverage.Covers(t, "validate.schema")
	rep := validateMqtt(t, 500, `""`)
	assert.That(t, !rep.OK)
	assert.Equal(t, StatusFail, checkStatus(t, rep, "source.mqtt"))
}

func TestValidateSchema_MqttWithinReceiveMaximumPasses(t *testing.T) {
	coverage.Covers(t, "validate.schema")
	rep := validateMqtt(t, 500, "edge-1")
	assert.Equal(t, StatusPass, checkStatus(t, rep, "source.mqtt"))
}
