package validate

import (
	"context"
	"strings"
	"testing"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

const windowedConfig = `tables:
  sql:
    - name: agg
      sql: |
        CREATE TABLE agg (%s, city VARCHAR, count INT)
      window:
        time_column: bucket
        size_seconds: 60
        late_rows: drop
        %s
        sink:
          type: console
pipeline:
  batch_size: 1
  source:
    type: kafka
    kafka:
      brokers: ["localhost:9092"]
      group_id: g
      auto_offset_reset: earliest
      topics: ["t"]
  handler:
    type: handlers.InferredMemBatch
    sql: SELECT time_bucket(INTERVAL '1 minute', event_time) AS bucket, city, count(*) FROM batch GROUP BY ALL
  sink:
    type: noop
`

func windowDiagnostics(rep Report) []Diagnostic {
	var out []Diagnostic
	for _, d := range rep.Diagnostics {
		if strings.Contains(d.Message, "window") {
			out = append(out, d)
		}
	}
	return out
}

func validateWindowed(t *testing.T, column, extra string) Report {
	t.Helper()
	rep, err := Validate(context.Background(), Request{
		Path: "w.yml", Config: strings.Replace(strings.Replace(windowedConfig, "%s", column, 1), "%s", extra, 1)})
	assert.NoError(t, err)
	return rep
}

// A well-formed declaration passes with no window diagnostic.
func TestValidateSchema_WindowDeclarationPasses(t *testing.T) {
	coverage.Covers(t, "validate.schema")
	rep := validateWindowed(t, "bucket TIMESTAMPTZ", "emit_sql: SELECT city, sum(count) FROM closed GROUP BY ALL")
	assert.That(t, rep.OK)
	assert.Equal(t, StatusPass, checkStatus(t, rep, "tables.window"))
	assert.Equal(t, 0, len(windowDiagnostics(rep)))
}

// TIMESTAMP WITH TIME ZONE is the same type spelled out.
func TestValidateSchema_WindowAcceptsTheLongTypeName(t *testing.T) {
	coverage.Covers(t, "validate.schema")
	rep := validateWindowed(t, "bucket TIMESTAMP WITH TIME ZONE", "")
	assert.That(t, rep.OK)
	assert.Equal(t, 0, len(windowDiagnostics(rep)))
}

// A TIMESTAMP bucket is read in the session's zone, and the watermark
// compares instants.
func TestValidateSchema_WindowTimeColumnMustBeTimestamptz(t *testing.T) {
	coverage.Covers(t, "validate.schema")
	rep := validateWindowed(t, "bucket TIMESTAMP", "")
	assert.That(t, !rep.OK)
	assert.Equal(t, StatusFail, checkStatus(t, rep, "tables.window"))
	diags := windowDiagnostics(rep)
	assert.Equal(t, 1, len(diags))
	assert.That(t, strings.Contains(diags[0].Message, `time_column "bucket" is not declared as TIMESTAMPTZ`))
	assert.That(t, diags[0].Position != nil)
}

// An emit_sql that never reads closed reads nothing the close supplies.
func TestValidateSchema_WindowEmitSQLMustReadClosed(t *testing.T) {
	coverage.Covers(t, "validate.schema")
	rep := validateWindowed(t, "bucket TIMESTAMPTZ", "emit_sql: SELECT * FROM agg")
	assert.That(t, !rep.OK)
	diags := windowDiagnostics(rep)
	assert.Equal(t, 1, len(diags))
	assert.That(t, strings.Contains(diags[0].Message, "emit_sql does not read closed"))
}

// An indexed window table with no state path leaks every deleted bucket.
// A warning, with the two ways out named.
func TestValidateSchema_IndexedWindowTableWithoutStateWarns(t *testing.T) {
	coverage.Covers(t, "validate.schema")
	for _, ddl := range []string{
		"CREATE TABLE agg (bucket TIMESTAMPTZ, city VARCHAR, count INT); CREATE UNIQUE INDEX agg_idx ON agg (bucket, city);",
		"CREATE TABLE agg (bucket TIMESTAMPTZ, city VARCHAR, count INT, PRIMARY KEY (bucket, city))",
	} {
		rep, err := Validate(context.Background(), Request{Path: "w.yml", Config: strings.Replace(
			strings.Replace(strings.Replace(windowedConfig, "%s", "bucket TIMESTAMPTZ", 1), "%s", "", 1),
			"CREATE TABLE agg (bucket TIMESTAMPTZ, city VARCHAR, count INT)", ddl, 1)})
		assert.NoError(t, err)
		assert.That(t, rep.OK)
		diags := windowDiagnostics(rep)
		assert.Equal(t, 1, len(diags))
		assert.Equal(t, SeverityWarning, diags[0].Severity)
		assert.That(t, strings.Contains(diags[0].Message, "no state.path"))
	}

	// With a state path the checkpoint reclaims, and nothing is said.
	stateful := strings.Replace(windowedConfig, "pipeline:\n  batch_size: 1\n",
		"pipeline:\n  batch_size: 1\n  state:\n    path: /tmp/s.db\n", 1)
	rep, err := Validate(context.Background(), Request{Path: "w.yml", Config: strings.Replace(
		strings.Replace(strings.Replace(stateful, "%s", "bucket TIMESTAMPTZ", 1), "%s", "", 1),
		"CREATE TABLE agg (bucket TIMESTAMPTZ, city VARCHAR, count INT)",
		"CREATE TABLE agg (bucket TIMESTAMPTZ, city VARCHAR, count INT, PRIMARY KEY (bucket, city))", 1)})
	assert.NoError(t, err)
	assert.Equal(t, 0, len(windowDiagnostics(rep)))
}

// reemit against a sink that appends is a warning: the config runs, and the
// operator is told the downstream will see corrections.
func TestValidateSchema_ReemitOnAnAppendOnlySinkWarns(t *testing.T) {
	coverage.Covers(t, "validate.schema")
	rep, err := Validate(context.Background(), Request{Path: "w.yml", Config: `tables:
  sql:
    - name: agg
      sql: CREATE TABLE agg (bucket TIMESTAMPTZ, count INT)
      window:
        time_column: bucket
        size_seconds: 60
        late_rows: reemit
        sink:
          type: kafka
          kafka:
            brokers: ["localhost:9092"]
            topic: out
pipeline:
  batch_size: 1
  source:
    type: kafka
    kafka:
      brokers: ["localhost:9092"]
      group_id: g
      auto_offset_reset: earliest
      topics: ["t"]
  handler:
    type: handlers.InferredMemBatch
    sql: SELECT time_bucket(INTERVAL '1 minute', event_time) AS bucket, city, count(*) FROM batch GROUP BY ALL
  sink:
    type: noop
`})
	assert.NoError(t, err)
	assert.That(t, rep.OK)
	diags := windowDiagnostics(rep)
	assert.Equal(t, 1, len(diags))
	assert.Equal(t, SeverityWarning, diags[0].Severity)
	assert.That(t, strings.Contains(diags[0].Message, "the kafka sink appends"))
	assert.Equal(t, 8, diags[0].Position.Line)
}

// A window with no late_rows does not pass the schema: the two policies are
// different promises to the sink, and a config has to say which it makes.
func TestValidateSchema_LateRowsIsRequired(t *testing.T) {
	coverage.Covers(t, "validate.schema")
	rep, err := Validate(context.Background(), Request{Path: "w.yml", Config: strings.Replace(
		strings.Replace(strings.Replace(windowedConfig, "%s", "bucket TIMESTAMPTZ", 1), "%s", "", 1),
		"        late_rows: drop\n", "", 1)})
	assert.NoError(t, err)
	assert.That(t, !rep.OK)
	var found bool
	for _, d := range rep.Diagnostics {
		if strings.Contains(d.Message, "late_rows") {
			found = true
		}
	}
	assert.That(t, found)
}

// The old block is refused with its replacement named, on the line it sits
// on, rather than as an unknown key.
func TestValidateSchema_ManagerBlockIsRefusedWithTheReplacement(t *testing.T) {
	coverage.Covers(t, "validate.schema")
	rep, err := Validate(context.Background(), Request{Path: "m.yml", Config: `tables:
  sql:
    - name: agg
      sql: CREATE TABLE agg (bucket TIMESTAMPTZ, count INT)
      manager:
        tumbling_window:
          collect_closed_windows_sql: SELECT * FROM agg
          delete_closed_windows_sql: DELETE FROM agg
        sink:
          type: console
pipeline:
  batch_size: 1
  source:
    type: kafka
    kafka:
      brokers: ["localhost:9092"]
      group_id: g
      auto_offset_reset: earliest
      topics: ["t"]
  handler:
    type: handlers.InferredMemBatch
    sql: SELECT time_bucket(INTERVAL '1 minute', event_time) AS bucket, city, count(*) FROM batch GROUP BY ALL
  sink:
    type: noop
`})
	assert.NoError(t, err)
	assert.That(t, !rep.OK)

	var found bool
	for _, d := range rep.Diagnostics {
		if strings.Contains(d.Message, "manager is gone") {
			found = true
			assert.That(t, strings.Contains(d.Message, "time_column"))
			assert.That(t, strings.Contains(d.Message, "late_rows"))
			assert.Equal(t, 5, d.Position.Line)
		}
	}
	assert.That(t, found)
}

// An idle close shorter than the flush interval is honoured late: the close
// waits for a commit made past the bound, and a quiet stream commits once a
// flush interval. validate says so, and says nothing once the interval is at
// or below the bound.
func TestValidateSchema_IdleCloseShorterThanTheFlushIntervalWarns(t *testing.T) {
	coverage.Covers(t, "validate.schema")
	// The default flush interval is thirty seconds, and ten is below it.
	rep := validateWindowed(t, "bucket TIMESTAMPTZ", "idle_close_seconds: 10")
	assert.That(t, rep.OK)
	diags := windowDiagnostics(rep)
	assert.Equal(t, 1, len(diags))
	assert.Equal(t, SeverityWarning, diags[0].Severity)
	assert.That(t, strings.Contains(diags[0].Message, "close 10 to 40 seconds after the last arrival"))

	// The interval brought down to the bound: nothing to say.
	paced := strings.Replace(windowedConfig, "pipeline:\n  batch_size: 1\n",
		"pipeline:\n  batch_size: 1\n  flush_interval_seconds: 10\n", 1)
	rep, err := Validate(context.Background(), Request{Path: "w.yml", Config: strings.Replace(
		strings.Replace(paced, "%s", "bucket TIMESTAMPTZ", 1), "%s", "idle_close_seconds: 10", 1)})
	assert.NoError(t, err)
	assert.Equal(t, 0, len(windowDiagnostics(rep)))

	// No idle close: nothing to trail.
	assert.Equal(t, 0, len(windowDiagnostics(validateWindowed(t, "bucket TIMESTAMPTZ", ""))))
}

// A pipeline with no window, for the rule that only a windowing pipeline is
// asked to read event_time.
const plainConfig = `pipeline:
  batch_size: 1
  source:
    type: kafka
    kafka:
      brokers: ["localhost:9092"]
      group_id: g
      auto_offset_reset: earliest
      topics: ["t"]
  handler:
    type: handlers.InferredMemBatch
    sql: SELECT time_bucket(INTERVAL '1 minute', to_timestamp(time_us / 1000000)) AS bucket FROM batch
  sink:
    type: noop
`

// A windowing pipeline's handler must derive the window's time from the
// event_time column -- the time the source assigned -- or the buckets and
// the watermark are on different clocks. A pipeline with no window is free
// to cut time from any field it likes.
func TestValidateSchema_AWindowingHandlerMustReadEventTime(t *testing.T) {
	coverage.Covers(t, "validate.schema")
	cfg := strings.Replace(strings.Replace(windowedConfig, "%s", "bucket TIMESTAMPTZ", 1), "%s", "", 1)
	cfg = strings.Replace(cfg, "time_bucket(INTERVAL '1 minute', event_time)",
		"time_bucket(INTERVAL '1 minute', to_timestamp(time_us / 1000000))", 1)
	rep, err := Validate(context.Background(), Request{Path: "w.yml", Config: cfg})
	assert.NoError(t, err)
	assert.That(t, !rep.OK)
	diags := windowDiagnostics(rep)
	assert.Equal(t, 1, len(diags))
	assert.That(t, strings.Contains(diags[0].Message, "must derive the window's time from the event_time column"))
	assert.That(t, diags[0].Position != nil)

	rep, err = Validate(context.Background(), Request{Path: "p.yml", Config: plainConfig})
	assert.NoError(t, err)
	assert.Equal(t, 0, len(windowDiagnostics(rep)))
}

// A structured handler's batch table is the user's, and the engine fills its
// event_time column from the record; a windowing pipeline on one must
// declare that column, as TIMESTAMPTZ, or there is nothing to cut on.
func TestValidateSchema_AStructuredWindowingHandlerMustDeclareEventTime(t *testing.T) {
	coverage.Covers(t, "validate.schema")
	structured := func(postsDDL string) string {
		cfg := strings.Replace(strings.Replace(windowedConfig, "%s", "bucket TIMESTAMPTZ", 1), "%s", "", 1)
		cfg = strings.Replace(cfg, "tables:\n  sql:\n",
			"tables:\n  sql:\n    - name: posts\n      sql: |\n        "+postsDDL+"\n", 1)
		cfg = strings.Replace(cfg, "type: handlers.InferredMemBatch\n",
			"type: handlers.StructuredBatch\n    table: posts\n", 1)
		return cfg
	}

	rep, err := Validate(context.Background(), Request{Path: "s.yml",
		Config: structured("CREATE TABLE posts (text TEXT, event_time TIMESTAMPTZ)")})
	assert.NoError(t, err)
	assert.Equal(t, 0, len(windowDiagnostics(rep)))

	rep, err = Validate(context.Background(), Request{Path: "s.yml",
		Config: structured("CREATE TABLE posts (text TEXT, time_us BIGINT)")})
	assert.NoError(t, err)
	assert.That(t, !rep.OK)
	diags := windowDiagnostics(rep)
	assert.Equal(t, 1, len(diags))
	assert.That(t, strings.Contains(diags[0].Message, `table "posts" must declare event_time TIMESTAMPTZ`))
}
