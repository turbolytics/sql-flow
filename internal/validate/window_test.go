package validate

import (
	"context"
	"fmt"
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
      event_time:
        path: ts
        format: rfc3339
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

// Lateness above zero republishes a bucket as a whole value, which an
// append-only sink holds twice. Refused, not warned: Flink's contract is the
// same. A sink validate cannot classify -- sqlcommand, clickhouse -- is
// warned, because whether it replaces is the SQL's or the table engine's
// call. A sink that replaces by key passes with nothing said.
func TestValidateSchema_LatenessNeedsAReplacingSink(t *testing.T) {
	coverage.Covers(t, "validate.schema")
	withSink := func(sink string) string {
		cfg := strings.Replace(strings.Replace(windowedConfig, "%s", "bucket TIMESTAMPTZ", 1),
			"%s", "allowed_lateness_seconds: 300", 1)
		return strings.Replace(cfg, "        sink:\n          type: console\n", sink, 1)
	}
	upsert := `        sink:
          type: postgres
          postgres:
            dsn: postgres://u:p@localhost:5432/db
            table: agg
            mode: upsert
            key: [bucket]
`
	append := `        sink:
          type: kafka
          kafka:
            brokers: ["localhost:9092"]
            topic: out
`
	sqlcommand := `        sink:
          type: sqlcommand
          sqlcommand:
            sql: INSERT OR REPLACE INTO out SELECT * FROM sqlflow_sink_batch
`

	rep, err := Validate(context.Background(), Request{Path: "w.yml", Config: withSink(upsert)})
	assert.NoError(t, err)
	assert.That(t, rep.OK)
	assert.Equal(t, 0, len(windowDiagnostics(rep)))

	rep, err = Validate(context.Background(), Request{Path: "w.yml", Config: withSink(append)})
	assert.NoError(t, err)
	assert.That(t, !rep.OK)
	diags := windowDiagnostics(rep)
	assert.Equal(t, 1, len(diags))
	assert.Equal(t, SeverityError, diags[0].Severity)
	assert.That(t, strings.Contains(diags[0].Message, "allowed_lateness_seconds is 300 and the kafka sink appends"))
	// On the key that made the pairing wrong, not on the sink.
	assert.Equal(t, 9, diags[0].Position.Line)

	rep, err = Validate(context.Background(), Request{Path: "w.yml", Config: withSink(sqlcommand)})
	assert.NoError(t, err)
	assert.That(t, rep.OK)
	diags = windowDiagnostics(rep)
	assert.Equal(t, 1, len(diags))
	assert.Equal(t, SeverityWarning, diags[0].Severity)
	assert.That(t, strings.Contains(diags[0].Message, "must replace the row"))
}

// A window with no allowed_lateness_seconds refuses late rows: the key is
// optional and its absence is 0. Nothing is said about it.
func TestValidateSchema_LatenessDefaultsToRefusing(t *testing.T) {
	coverage.Covers(t, "validate.schema")
	rep := validateWindowed(t, "bucket TIMESTAMPTZ", "")
	assert.That(t, rep.OK)
	assert.Equal(t, 0, len(windowDiagnostics(rep)))
	rep = validateWindowed(t, "bucket TIMESTAMPTZ", "allowed_lateness_seconds: 0")
	assert.That(t, rep.OK)
	assert.Equal(t, 0, len(windowDiagnostics(rep)))
}

// The removed keys fail with a message naming the replacement, on the line
// they sit on, so a config written against the old schema learns what
// changed rather than "unknown key".
func TestValidateSchema_RemovedWindowKeysNameTheirReplacement(t *testing.T) {
	coverage.Covers(t, "validate.schema")
	for key, want := range map[string]string{
		"late_rows: drop":           "Set allowed_lateness_seconds instead",
		"late_rows: reemit":         "Set allowed_lateness_seconds instead",
		"poll_interval_seconds: 10": "nothing polls",
	} {
		t.Run(key, func(t *testing.T) {
			coverage.Covers(t, "validate.schema")
			rep := validateWindowed(t, "bucket TIMESTAMPTZ", key)
			assert.That(t, !rep.OK)
			var found *Diagnostic
			for i, d := range rep.Diagnostics {
				if d.Severity == SeverityError && strings.Contains(d.Message, want) {
					found = &rep.Diagnostics[i]
				}
			}
			if found == nil {
				t.Fatalf("no error names %q: %v", want, rep.Diagnostics)
			}
			assert.Equal(t, 9, found.Position.Line)
		})
	}
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
      event_time:
        path: ts
        format: rfc3339
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
			assert.That(t, strings.Contains(d.Message, "allowed_lateness_seconds"))
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
      event_time:
        path: ts
        format: rfc3339
  handler:
    type: handlers.InferredMemBatch
    sql: SELECT time_bucket(INTERVAL '1 minute', to_timestamp(time_us / 1000000)) AS bucket FROM batch
  sink:
    type: noop
`

// A windowing pipeline's time_column must be time_bucket over event_time
// with the window's own size, because the engine decides a record's lateness
// from that bucket and a different expression would put the row somewhere
// else. This was a warning while the manager only swept the table; the
// engine refuses records against that bucket now, so it is an error. A
// pipeline with no window is free to cut time from any field it likes.
func TestValidateSchema_AWindowingHandlerMustBucketEventTime(t *testing.T) {
	coverage.Covers(t, "validate.schema")
	base := strings.Replace(strings.Replace(windowedConfig, "%s", "bucket TIMESTAMPTZ", 1), "%s", "", 1)
	const good = "time_bucket(INTERVAL '1 minute', event_time)"

	// The same size spelled in seconds, and the column quoted, both pass.
	for _, ok := range []string{
		"time_bucket(INTERVAL '60 seconds', event_time)",
		"TIME_BUCKET(interval '1 MINUTE', event_time)",
	} {
		rep, err := Validate(context.Background(), Request{Path: "w.yml", Config: strings.Replace(base, good, ok, 1)})
		assert.NoError(t, err)
		assert.Equal(t, 0, len(windowDiagnostics(rep)))
	}

	for name, bad := range map[string]string{
		"payload field": "time_bucket(INTERVAL '1 minute', to_timestamp(time_us / 1000000))",
		"wrong size":    "time_bucket(INTERVAL '5 minutes', event_time)",
		"date_trunc":    "date_trunc('minute', event_time)",
		"no derivation": "now()",
	} {
		t.Run(name, func(t *testing.T) {
			coverage.Covers(t, "validate.schema")
			rep, err := Validate(context.Background(), Request{Path: "w.yml", Config: strings.Replace(base, good, bad, 1)})
			assert.NoError(t, err)
			assert.That(t, !rep.OK)
			diags := windowDiagnostics(rep)
			assert.Equal(t, 1, len(diags))
			assert.Equal(t, SeverityError, diags[0].Severity)
			assert.That(t, strings.Contains(diags[0].Message, "time_bucket(INTERVAL '60 seconds', event_time)"))
			assert.That(t, diags[0].Position != nil)
		})
	}

	rep, err := Validate(context.Background(), Request{Path: "p.yml", Config: plainConfig})
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
	assert.Equal(t, SeverityError, diags[0].Severity)

	// The table declared in a command, as the bluesky examples do, is read
	// the same way.
	inCommand := strings.Replace(strings.Replace(windowedConfig, "%s", "bucket TIMESTAMPTZ", 1), "%s", "", 1)
	inCommand = "commands:\n  - name: posts\n    sql: |\n      CREATE TABLE IF NOT EXISTS posts (text TEXT, event_time TIMESTAMPTZ);\n" + inCommand
	inCommand = strings.Replace(inCommand, "type: handlers.InferredMemBatch\n",
		"type: handlers.StructuredBatch\n    table: posts\n", 1)
	rep, err = Validate(context.Background(), Request{Path: "s.yml", Config: inCommand})
	assert.NoError(t, err)
	assert.Equal(t, 0, len(windowDiagnostics(rep)))
	assert.That(t, strings.Contains(diags[0].Message, `table "posts" does not declare event_time TIMESTAMPTZ`))
	assert.That(t, diags[0].Position != nil)
}

// A window is cut on event_time, and a source that declares none still has
// one: Kafka's record timestamp, or arrival. Both belong to the transport
// rather than to the event, so the pipeline runs and buckets on the wrong
// clock. dev/config/examples/logs.rollup.clickhouse.yml did exactly that after
// the window rewrite, and a two-month replay landed in a single bucket.
func TestValidateSchema_WindowWarnsWhenTheSourceDeclaresNoEventTime(t *testing.T) {
	coverage.Covers(t, "validate.schema")
	noEventTime := strings.Replace(windowedConfig, `
      event_time:
        path: ts
        format: rfc3339`, "", 1)
	rep, err := Validate(context.Background(), Request{
		Path:   "w.yml",
		Config: strings.Replace(strings.Replace(noEventTime, "%s", "bucket TIMESTAMPTZ", 1), "%s", "", 1)})
	assert.NoError(t, err)

	var found *Diagnostic
	for i, d := range rep.Diagnostics {
		if strings.Contains(d.Message, "declares no event_time") {
			found = &rep.Diagnostics[i]
		}
	}
	if found == nil {
		t.Fatalf("no diagnostic about an undeclared event_time; got %v", rep.Diagnostics)
	}
	// A warning, not a failure: a producer that stamps a record at the moment
	// of the event makes the Kafka timestamp the event's own time, and there
	// is no way to declare that intent, so refusing it would be unsatisfiable.
	assert.Equal(t, SeverityWarning, found.Severity)
	assert.That(t, strings.Contains(found.Message, "Kafka record timestamp"))
	assert.That(t, strings.Contains(found.Message, "event_time: {path, format}"))
}

// Declaring one silences it. This is the shape every shipped windowed example
// uses.
func TestValidateSchema_WindowAcceptsADeclaredEventTime(t *testing.T) {
	coverage.Covers(t, "validate.schema")
	rep := validateWindowed(t, "bucket TIMESTAMPTZ", "")
	for _, d := range rep.Diagnostics {
		if strings.Contains(d.Message, "declares no event_time") {
			t.Fatalf("warned about a source that declares one: %s", d.Message)
		}
	}
}

// Only a windowing pipeline is warned. Without a window nothing is cut on
// event time, and arrival is a perfectly ordinary thing to carry.
func TestValidateSchema_NoWindowNoEventTimeWarning(t *testing.T) {
	coverage.Covers(t, "validate.schema")
	const unwindowed = `pipeline:
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
    sql: SELECT city FROM batch
  sink:
    type: noop
`
	rep, err := Validate(context.Background(), Request{Path: "u.yml", Config: unwindowed})
	assert.NoError(t, err)
	for _, d := range rep.Diagnostics {
		if strings.Contains(d.Message, "declares no event_time") {
			t.Fatalf("warned about a pipeline with no window: %s", d.Message)
		}
	}
}

// ownedConfig is a partition_owned window that passes. Its holes, in order:
// the source block, the window table's partition column, the handler's
// partition expression, and the sink's key.
const ownedConfig = `tables:
  sql:
    - name: agg
      sql: |
        CREATE TABLE agg (bucket TIMESTAMPTZ, city VARCHAR, %[2]s n BIGINT)
      window:
        time_column: bucket
        size_seconds: 60
        allowed_lateness_seconds: 60
        partition_owned: true
        sink:
          type: postgres
          postgres:
            dsn: "postgresql://u:p@localhost:5432/db"
            table: agg
            mode: upsert
            key: [%[4]s]
pipeline:
  batch_size: 1
  source:
%[1]s
  handler:
    type: handlers.InferredMemBatch
    sql: INSERT INTO agg SELECT time_bucket(INTERVAL '60 seconds', event_time) AS bucket, city, %[3]s 1 FROM batch
  sink:
    type: noop
`

const ownedKafkaSource = `    type: kafka
    kafka:
      brokers: ["localhost:9092"]
      group_id: g
      auto_offset_reset: earliest
      topics: [%s]
      event_time:
        path: ts
        format: rfc3339`

func validateOwned(t *testing.T, source, column, expr, key string) Report {
	t.Helper()
	rep, err := Validate(context.Background(), Request{Path: "o.yml",
		Config: fmt.Sprintf(ownedConfig, source, column, expr, key)})
	assert.NoError(t, err)
	return rep
}

func TestValidateSchema_PartitionOwnedPasses(t *testing.T) {
	coverage.Covers(t, "validate.schema")
	rep := validateOwned(t, fmt.Sprintf(ownedKafkaSource, `"t"`), "kafka_partition INTEGER,", "kafka_partition,",
		"bucket, city, kafka_partition")
	assert.That(t, rep.OK)
	assert.Equal(t, StatusPass, checkStatus(t, rep, "tables.window"))
}

// Each requirement, broken alone, fails the window check and says which.
func TestValidateSchema_PartitionOwnedRefusesWhatItCannotHonor(t *testing.T) {
	coverage.Covers(t, "validate.schema")
	kafka := fmt.Sprintf(ownedKafkaSource, `"t"`)
	for _, tc := range []struct {
		name, source, column, expr, key, want string
	}{
		{"a webhook source", "    type: webhook\n    webhook:\n      event_time:\n        path: ts\n        format: rfc3339",
			"kafka_partition INTEGER,", "kafka_partition,", "bucket, city, kafka_partition", "kafka source with exactly one topic"},
		{"two topics", fmt.Sprintf(ownedKafkaSource, `"t", "u"`),
			"kafka_partition INTEGER,", "kafka_partition,", "bucket, city, kafka_partition", "kafka source with exactly one topic"},
		{"a sink key without the partition", kafka,
			"kafka_partition INTEGER,", "kafka_partition,", "bucket, city", "kafka_partition in its key"},
		{"a table without the column", kafka,
			"", "kafka_partition,", "bucket, city, kafka_partition", "column in the table's CREATE"},
		{"a handler that does not write it", kafka,
			"kafka_partition INTEGER,", "", "bucket, city, kafka_partition", "handler's SQL to write kafka_partition"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rep := validateOwned(t, tc.source, tc.column, tc.expr, tc.key)
			assert.That(t, !rep.OK)
			assert.Equal(t, StatusFail, checkStatus(t, rep, "tables.window"))
			found := false
			for _, d := range rep.Diagnostics {
				if strings.Contains(d.Message, tc.want) {
					found = true
				}
			}
			if !found {
				t.Fatalf("no diagnostic containing %q in %+v", tc.want, rep.Diagnostics)
			}
		})
	}
}
