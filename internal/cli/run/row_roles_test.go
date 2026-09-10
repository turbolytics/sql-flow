package run

import (
	"context"
	"os"
	"sync"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/turbolytics/sql-flow/internal/duckdb"
	"github.com/zeebo/assert"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.uber.org/zap"
)

// writtenRowsByRole reads sink_rows_written out of a manual reader, keyed by
// the role attribute.
//
// The role is the whole point of the assertion. Rows summed across roles count
// a rejected record as a delivered one, so the end-to-end ratio would report a
// pipeline healthier the more records it threw away.
func writtenRowsByRole(t *testing.T, r *sdkmetric.ManualReader) map[string]int64 {
	t.Helper()

	var rm metricdata.ResourceMetrics
	assert.NoError(t, r.Collect(context.Background(), &rm))

	out := map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "sink_rows_written" {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				continue
			}
			for _, dp := range sum.DataPoints {
				role, _ := dp.Attributes.Value("role")
				out[role.AsString()] += dp.Value
			}
		}
	}
	return out
}

func rowsTestConn(t *testing.T) adbc.Connection {
	t.Helper()
	if os.Getenv("SQLFLOW_DUCKDB_LIB") == "" {
		os.Setenv("SQLFLOW_DUCKDB_LIB", "/opt/homebrew/lib/libduckdb.dylib")
	}
	db, err := duckdb.OpenPath(context.Background(), "")
	assert.NoError(t, err)
	conn, err := db.Connect(context.Background())
	assert.NoError(t, err)
	t.Cleanup(func() { conn.Close(); db.Close() })
	return conn
}

func rowsTestExec(t *testing.T, conn adbc.Connection, sql string) {
	t.Helper()
	stmt, err := conn.NewStatement()
	assert.NoError(t, err)
	defer stmt.Close()
	assert.NoError(t, stmt.SetSqlQuery(sql))
	_, err = stmt.ExecuteUpdate(context.Background())
	assert.NoError(t, err)
}

func oneRowTable() arrow.Table {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)
	b := array.NewRecordBuilder(memory.NewGoAllocator(), schema)
	defer b.Release()
	b.Field(0).(*array.Int64Builder).Append(1)
	rec := b.NewRecord()
	defer rec.Release()
	return array.NewTableFromRecords(schema, []arrow.Record{rec})
}

// TestDLQRowsCarryTheDLQRole holds the wiring at the DLQ call site, not just
// the option that expresses it.
//
// Without the role, a record the pipeline rejected counts in the same series
// as one the destination acknowledged, and the end-to-end ratio reports a
// pipeline healthier the more records it threw away. Deleting the option from
// newErrorPolicies must fail a test, or the attribute is decoration.
func TestDLQRowsCarryTheDLQRole(t *testing.T) {
	coverage.Covers(t, "observability.metrics")

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	conf := &config.Conf{}
	conf.Pipeline.OnError = &config.OnError{
		Policy: "dlq",
		DLQ:    &config.Sink{Type: "console"},
	}

	policies, err := newErrorPolicies(context.Background(), conf, nil, mp)
	assert.NoError(t, err)
	assert.That(t, policies.DLQSink != nil)

	tbl := oneRowTable()
	defer tbl.Release()
	assert.NoError(t, policies.DLQSink.WriteTable(context.Background(), tbl))
	assert.NoError(t, policies.DLQSink.Flush(context.Background()))

	byRole := writtenRowsByRole(t, reader)
	assert.Equal(t, int64(1), byRole["dlq"])
	assert.Equal(t, int64(0), byRole["pipeline"])
}

// TestWindowManagerRowsAreCounted is the regression the decorator's placement
// exists to prevent.
//
// A windowed pipeline's entire output leaves through the manager's own sink,
// not the pipeline's. A counter attached at turbine.go's flush site instead of
// inside sinks.New reports zero rows written for such a pipeline while it is
// delivering every row correctly -- success reported, delivery invisible.
func TestWindowManagerRowsAreCounted(t *testing.T) {
	coverage.Covers(t, "observability.metrics")

	conn := rowsTestConn(t)
	rowsTestExec(t, conn, "CREATE TABLE agg (id BIGINT)")
	rowsTestExec(t, conn, "INSERT INTO agg VALUES (1), (2), (3)")

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	conf := &config.Conf{
		Tables: &config.Tables{
			SQL: []config.TableSQL{{
				Name: "agg",
				Manager: &config.TableManager{
					TumblingWindow: &config.TumblingWindow{
						CollectSQL:       "SELECT id FROM agg",
						DeleteSQL:        "DELETE FROM agg",
						PollIntervalSecs: 3600,
					},
					Sink: config.Sink{Type: "console"},
				},
			}},
		},
	}

	built, err := buildManagedTables(
		context.Background(), conf, conn, &sync.Mutex{}, zap.NewNop(), mp)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(built))

	// One poll closes the window, writes the rows to the manager's sink and
	// flushes them.
	assert.NoError(t, built[0].Poll(context.Background()))

	byRole := writtenRowsByRole(t, reader)
	assert.Equal(t, int64(3), byRole["manager"])
}
