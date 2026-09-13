package validate

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

// drainConfig is a pipeline the test fills in three ways: the drain deadline
// line, the sink type, and the sink's retry block. It carries both a
// clickhouse and a kafka block so either type validates.
const drainConfig = `pipeline:
  batch_size: 1
  %s
  source:
    type: kafka
    kafka:
      brokers: ["localhost:9092"]
      group_id: g
      auto_offset_reset: earliest
      topics: ["t"]
  handler:
    type: handlers.InferredMemBatch
    sql: SELECT 1
  sink:
    type: %s
    clickhouse:
      dsn: clickhouse://localhost:9000
      table: t
    kafka:
      brokers: ["localhost:9092"]
      topic: out
%s
`

const twentySecondLadder = `    retry:
      deadline_seconds: 20`

func validateDrain(t *testing.T, drainLine, sinkType, retry string) Report {
	t.Helper()
	rep, err := Validate(context.Background(), Request{
		Path:   "drain.yml",
		Config: fmt.Sprintf(drainConfig, drainLine, sinkType, retry),
	})
	assert.NoError(t, err)
	return rep
}

// drainWarnings returns the diagnostics the drain check raised.
func drainWarnings(rep Report) []Diagnostic {
	var out []Diagnostic
	for _, d := range rep.Diagnostics {
		if strings.Contains(d.Message, "drain_deadline_seconds") {
			out = append(out, d)
		}
	}
	return out
}

// A drain deadline shorter than a sink's retry deadline is a choice, not a
// fault: the operator wants the process out. They are told what it costs, and
// the config stays valid.
func TestValidateSchema_ShortDrainDeadlineWarns(t *testing.T) {
	coverage.Covers(t, "validate.schema")
	rep := validateDrain(t, "drain_deadline_seconds: 5", "clickhouse", twentySecondLadder)

	assert.That(t, rep.OK)
	assert.Equal(t, StatusPass, checkStatus(t, rep, "pipeline.drain_deadline"))

	warnings := drainWarnings(rep)
	assert.Equal(t, 1, len(warnings))
	assert.Equal(t, SeverityWarning, warnings[0].Severity)
	assert.That(t, strings.Contains(warnings[0].Message, "retry.deadline_seconds"))
	assert.That(t, strings.Contains(warnings[0].Message, "pipeline.sink"))
	assert.That(t, strings.Contains(warnings[0].Message, "exits 15"))
}

func TestValidateSchema_LongDrainDeadlineIsQuiet(t *testing.T) {
	coverage.Covers(t, "validate.schema")
	rep := validateDrain(t, "drain_deadline_seconds: 60", "clickhouse", twentySecondLadder)

	assert.That(t, rep.OK)
	assert.Equal(t, StatusPass, checkStatus(t, rep, "pipeline.drain_deadline"))
	assert.Equal(t, 0, len(drainWarnings(rep)))
}

// The defaults agree with each other: a thirty-second drain outlasts a
// ten-second ladder, so a config that sets neither says nothing.
func TestValidateSchema_DefaultDrainOutlastsTheDefaultLadder(t *testing.T) {
	coverage.Covers(t, "validate.schema")
	rep := validateDrain(t, "", "clickhouse", "")
	assert.That(t, rep.OK)
	assert.Equal(t, 0, len(drainWarnings(rep)))
}

// A sink with no ladder has nothing to outlive the drain. The Kafka sink
// hands retries to its client, so a short drain there is not warned about
// even with a retry block present.
func TestValidateSchema_ShortDrainOnASinkWithNoLadderIsQuiet(t *testing.T) {
	coverage.Covers(t, "validate.schema")
	rep := validateDrain(t, "drain_deadline_seconds: 5", "kafka", twentySecondLadder)
	assert.That(t, rep.OK)
	assert.Equal(t, 0, len(drainWarnings(rep)))
}

// max_attempts: 1 turns the ladder off, so its deadline never runs.
func TestValidateSchema_ShortDrainWithRetryingOffIsQuiet(t *testing.T) {
	coverage.Covers(t, "validate.schema")
	rep := validateDrain(t, "drain_deadline_seconds: 5", "clickhouse",
		"    retry:\n      max_attempts: 1\n      deadline_seconds: 20")
	assert.That(t, rep.OK)
	assert.Equal(t, 0, len(drainWarnings(rep)))
}

// The schema rejects a drain deadline of zero. Absent means the default; a
// zero written down is a mistake.
func TestValidateSchema_ZeroDrainDeadlineFails(t *testing.T) {
	coverage.Covers(t, "validate.schema")
	rep := validateDrain(t, "drain_deadline_seconds: 0", "clickhouse", "")
	assert.That(t, !rep.OK)
}
