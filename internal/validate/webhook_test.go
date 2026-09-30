package validate

import (
	"context"
	"strings"
	"testing"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

func TestValidateSchema_AfterFlushOnAWindowingPipelineFails(t *testing.T) {
	coverage.Covers(t, "validate.schema")
	cfg := strings.Replace(windowedConfig, "%s", "bucket TIMESTAMPTZ", 1)
	cfg = strings.Replace(cfg, "%s", "", 1)
	cfg = strings.Replace(cfg, `    type: kafka
    kafka:
      brokers: ["localhost:9092"]
      group_id: g
      auto_offset_reset: earliest
      topics: ["t"]
      event_time:
        path: ts
        format: rfc3339`, `    type: webhook
    webhook:
      ack: after_flush
      event_time:
        path: ts
        format: rfc3339`, 1)
	rep, err := Validate(context.Background(), Request{Path: "w.yml", Config: cfg})
	assert.NoError(t, err)
	assert.That(t, !rep.OK)
	assert.Equal(t, StatusFail, checkStatus(t, rep, "source.webhook.ack"))
}

func TestValidateSchema_AfterFlushWithoutAWindowPasses(t *testing.T) {
	coverage.Covers(t, "validate.schema")
	rep, err := Validate(context.Background(), Request{Path: "i.yml", Config: `
pipeline:
  batch_size: 50
  source:
    type: webhook
    webhook:
      ack: after_flush
  handler:
    type: handlers.InferredMemBatch
    sql: SELECT * FROM batch
  sink:
    type: noop
`})
	assert.NoError(t, err)
	assert.That(t, rep.OK)
	assert.Equal(t, StatusPass, checkStatus(t, rep, "source.webhook.ack"))
}
