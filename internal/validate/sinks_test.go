package validate

import (
	"context"
	"strings"
	"testing"

	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

// sinksConfig has four holes: the attach command's TYPE, the window's
// late_rows, the window's sink block at ten spaces, and the pipeline's sink
// block at four.
const sinksConfig = `commands:
  - name: attach
    sql: |
      ATTACH 'postgresql://u:p@localhost:5432/db' AS pg (TYPE %s);
tables:
  sql:
    - name: agg
      sql: CREATE TABLE agg (bucket TIMESTAMPTZ, count INT)
      window:
        time_column: bucket
        size_seconds: 60
        late_rows: %s
        sink:
%s
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
    sql: SELECT 1
  sink:
%s
`

// indent prefixes every line of block with n spaces.
func indent(block string, n int) string {
	pad := strings.Repeat(" ", n)
	lines := strings.Split(block, "\n")
	for i, l := range lines {
		lines[i] = pad + l
	}
	return strings.Join(lines, "\n")
}

const (
	upsertBlock = `type: postgres
postgres:
  dsn: postgres://u:p@localhost:5432/db
  table: agg
  mode: upsert
  key: [bucket]`
	appendBlock = `type: postgres
postgres:
  dsn: postgres://u:p@localhost:5432/db
  table: agg
  mode: append`
	conflictBlock = `type: sqlcommand
sqlcommand:
  sql: INSERT INTO pg.agg SELECT * FROM sqlflow_sink_batch ON CONFLICT (bucket) DO UPDATE SET count = EXCLUDED.count`
	consoleBlock = `type: console`
	noopBlock    = `type: noop`
)

func validateSinks(t *testing.T, attachType, lateRows, windowSink, pipelineSink string) Report {
	t.Helper()
	cfg := sinksConfig
	for _, v := range []string{attachType, lateRows, indent(windowSink, 10), indent(pipelineSink, 4)} {
		cfg = strings.Replace(cfg, "%s", v, 1)
	}
	rep, err := Validate(context.Background(), Request{Path: "p.yml", Config: cfg})
	assert.NoError(t, err)
	return rep
}

// sinkDiagnostics are the diagnostics about a sink.
func sinkDiagnostics(rep Report) []Diagnostic {
	var out []Diagnostic
	for _, d := range rep.Diagnostics {
		if strings.Contains(d.Message, "sink") {
			out = append(out, d)
		}
	}
	return out
}

// upsert with reemit is refused: the sink replaces a bucket's row with what
// it is handed, and a reemit hands it emit_sql over the late rows alone.
func TestValidateSchema_PostgresUpsertRefusesReemit(t *testing.T) {
	coverage.Covers(t, "validate.schema")
	rep := validateSinks(t, "POSTGRES", "reemit", upsertBlock, noopBlock)
	assert.That(t, !rep.OK)
	diags := sinkDiagnostics(rep)
	assert.Equal(t, 1, len(diags))
	assert.Equal(t, SeverityError, diags[0].Severity)
	assert.That(t, strings.Contains(diags[0].Message, "late_rows is reemit and the postgres sink upserts"))
	assert.Equal(t, StatusFail, checkStatus(t, rep, "sinks.postgres"))

	rep = validateSinks(t, "POSTGRES", "drop", upsertBlock, noopBlock)
	assert.That(t, rep.OK)
	assert.Equal(t, 0, len(sinkDiagnostics(rep)))
	assert.Equal(t, StatusPass, checkStatus(t, rep, "sinks.postgres"))
}

// append with reemit warns, as kafka and iceberg do, and only once.
func TestValidateSchema_PostgresAppendWarnsOnReemit(t *testing.T) {
	coverage.Covers(t, "validate.schema")
	rep := validateSinks(t, "POSTGRES", "reemit", appendBlock, noopBlock)
	assert.That(t, rep.OK)
	diags := sinkDiagnostics(rep)
	assert.Equal(t, 1, len(diags))
	assert.Equal(t, SeverityWarning, diags[0].Severity)
	assert.That(t, strings.Contains(diags[0].Message, "the postgres sink appends"))
}

// A sqlcommand upsert into an attached Postgres reads the whole target table
// per flush. The warning names the cost and the keyed sink, on the window's
// sink and on the pipeline's alike.
func TestValidateSchema_SqlcommandUpsertIntoPostgresWarns(t *testing.T) {
	coverage.Covers(t, "validate.schema")
	for name, c := range map[string]struct{ window, pipeline string }{
		"window sink":   {conflictBlock, noopBlock},
		"pipeline sink": {consoleBlock, conflictBlock},
	} {
		rep := validateSinks(t, "POSTGRES", "drop", c.window, c.pipeline)
		assert.That(t, rep.OK)
		diags := sinkDiagnostics(rep)
		if len(diags) != 1 {
			t.Fatalf("%s: %d sink diagnostics, want 1: %v", name, len(diags), diags)
		}
		assert.Equal(t, SeverityWarning, diags[0].Severity)
		assert.That(t, strings.Contains(diags[0].Message, "whole target table"))
		assert.That(t, strings.Contains(diags[0].Message, "type: postgres"))
	}

	// Without an attached Postgres, ON CONFLICT is DuckDB's own and the
	// extension's cost does not apply.
	rep := validateSinks(t, "DUCKDB", "drop", conflictBlock, noopBlock)
	assert.Equal(t, 0, len(sinkDiagnostics(rep)))
}

// The block's own shape: mode is required, which the schema reports; upsert
// needs a key and append refuses one, which only checkSinks can.
func TestValidateSchema_PostgresBlockShape(t *testing.T) {
	coverage.Covers(t, "validate.schema")
	for want, block := range map[string]string{
		// The schema's own finding; checkSinks does not repeat it.
		"missing property 'mode'": "type: postgres\npostgres:\n  dsn: x\n  table: t",
		"needs key":               "type: postgres\npostgres:\n  dsn: x\n  table: t\n  mode: upsert",
		"takes no key":            "type: postgres\npostgres:\n  dsn: x\n  table: t\n  mode: append\n  key: [a]",
	} {
		rep := validateSinks(t, "POSTGRES", "drop", block, noopBlock)
		assert.That(t, !rep.OK)
		found := false
		for _, d := range sinkDiagnostics(rep) {
			if strings.Contains(d.Message, want) {
				found = true
			}
		}
		if !found {
			t.Fatalf("no sink diagnostic says %q: %v", want, rep.Diagnostics)
		}
	}
}
