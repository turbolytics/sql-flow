package run

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sort"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	prom "github.com/prometheus/client_golang/prometheus"
	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/core"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/turbolytics/sql-flow/internal/sinks"
	"github.com/turbolytics/sql-flow/internal/webhook"
	"github.com/zeebo/assert"
	"go.opentelemetry.io/otel/exporters/prometheus"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

// DuckDB takes an exclusive lock on the state file, so a second process
// cannot read it while the pipeline holds it -- not even read-only. A running
// pipeline therefore has to serve its own stats.
func TestObservabilityMetrics_StatsHandler_ReportsState(t *testing.T) {
	coverage.Covers(t, "observability.metrics")
	want := &core.StateStats{
		Path:      "/state/state.db",
		SizeBytes: 4096,
		Tables:    []core.TableStat{{Name: "agg", Rows: 24}},
		Offsets:   []core.OffsetStat{{Topic: "events", Partition: 0, Offset: 999, LeaderEpoch: 7}},
	}

	mux := newHTTPMux(nil, func() (*core.StateStats, error) { return want, nil })

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/stats", nil))
	assert.Equal(t, http.StatusOK, rec.Code)

	var got map[string]any
	assert.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))

	state, ok := got["state"].(map[string]any)
	assert.That(t, ok)
	assert.Equal(t, "/state/state.db", state["path"])
	assert.Equal(t, float64(4096), state["size_bytes"])

	tables, ok := state["tables"].([]any)
	assert.That(t, ok)
	assert.Equal(t, float64(24), tables[0].(map[string]any)["rows"])

	offsets, ok := state["offsets"].([]any)
	assert.That(t, ok)
	assert.Equal(t, float64(999), offsets[0].(map[string]any)["offset"])
}

// A pipeline with no state path still answers, with a null state block. The
// endpoint stays useful for the counters even when nothing is durable.
func TestObservabilityMetrics_StatsHandler_NullStateWithoutAStateDatabase(t *testing.T) {
	coverage.Covers(t, "observability.metrics")
	mux := newHTTPMux(nil, func() (*core.StateStats, error) { return nil, nil })

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/stats", nil))
	assert.Equal(t, http.StatusOK, rec.Code)

	var got map[string]any
	assert.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
	assert.That(t, got["state"] == nil)
}

// A failure reading state is reported as a server error rather than an empty
// success, so a monitoring system sees the problem instead of a healthy-looking
// blank.
func TestObservabilityMetrics_StatsHandler_ReportsCollectionFailure(t *testing.T) {
	coverage.Covers(t, "observability.metrics")
	mux := newHTTPMux(nil, func() (*core.StateStats, error) {
		return nil, errors.New("state database unreadable")
	})

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/stats", nil))
	assert.Equal(t, http.StatusInternalServerError, rec.Code)
}

// With no stats provider at all -- metrics enabled but state unwired -- the
// endpoint must not be registered as a half-working route.
func TestObservabilityMetrics_StatsHandler_AbsentWithoutAProvider(t *testing.T) {
	coverage.Covers(t, "observability.metrics")
	mux := newHTTPMux(nil, nil)

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/stats", nil))
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// exportedNames registers every instrument the engine declares against a real
// Prometheus exporter and returns the series names it produces.
//
// The instrument name and the exported name differ: the exporter appends the
// unit, then _total for counters, and skips the unit when the name already
// contains it. Only the exported name is queryable, so only the exported name
// is worth asserting.
func exportedNames(t *testing.T) []string {
	t.Helper()

	reg := prom.NewRegistry()
	exp, err := prometheus.New(prometheus.WithRegisterer(reg))
	assert.NoError(t, err)
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(exp))

	m, err := core.NewMetrics(mp)
	assert.NoError(t, err)

	ctx := context.Background()
	m.MessageCount.Add(ctx, 1)
	m.HandlerRowsRead.Add(ctx, 1)
	m.ErrorCount.Add(ctx, 1)
	m.SourceReadLatency.Record(ctx, 1)
	m.SinkFlushLatency.Record(ctx, 1)
	m.SinkFlushNumRows.Record(ctx, 1)
	m.SinkFlushCount.Add(ctx, 1)
	m.BatchProcessingLatency.Record(ctx, 1)
	m.ConsumerLag.Record(ctx, 1)
	m.StateCommitLatency.Record(ctx, 1)
	m.StateCommitCount.Add(ctx, 1)
	m.StateSizeBytes.Record(ctx, 1)
	m.StateTableRows.Record(ctx, 1)
	m.ReferenceTableRows.Record(ctx, 1)

	wm, err := webhook.NewMetrics(mp)
	assert.NoError(t, err)
	wm.RequestCount.Add(ctx, 1)
	wm.RequestDuration.Record(ctx, 1)

	// The row counters are declared in internal/sinks, not core.NewMetrics, so
	// a core-only sweep would miss them and let the README document series no
	// test knows about. Driven through the real constructor rather than
	// re-declared here, so a rename over there fails this.
	sink, err := sinks.New(ctx, config.Sink{Type: "console"}, nil,
		sinks.WithMeterProvider(mp), sinks.WithSinkRole("pipeline"))
	assert.NoError(t, err)

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)
	b := array.NewRecordBuilder(memory.NewGoAllocator(), schema)
	b.Field(0).(*array.Int64Builder).Append(1)
	rec := b.NewRecord()
	tbl := array.NewTableFromRecords(schema, []arrow.Record{rec})
	rec.Release()
	b.Release()

	assert.NoError(t, sink.WriteTable(ctx, tbl))
	assert.NoError(t, sink.Flush(ctx))
	tbl.Release()

	families, err := reg.Gather()
	assert.NoError(t, err)

	var names []string
	for _, f := range families {
		if f.GetName() == "target_info" {
			continue
		}
		names = append(names, f.GetName())
	}
	sort.Strings(names)
	return names
}

// TestExportedSeriesNames holds the metrics endpoint to what the README
// documents. Nothing asserted the instrument-to-series mapping before, so an
// OTel upgrade that changed unit suffixing would rename every series in this
// repo with no test failing.
func TestExportedSeriesNames(t *testing.T) {
	coverage.Covers(t, "observability.metrics")
	want := []string{
		"batch_processing_latency_seconds",
		"consumer_lag_messages",
		"error_count_total",
		"handler_rows_read_total",
		"message_count_messages_total",
		"reference_table_rows",
		"sink_flush_count_flushes_total",
		"sink_flush_latency_seconds",
		"sink_flush_num_rows",
		"sink_rows_accepted_total",
		"sink_rows_written_total",
		"source_read_latency_seconds",
		"state_commit_count_commits_total",
		"state_commit_latency_seconds",
		"state_db_size_bytes",
		"state_table_rows",
		"webhook_request_duration_seconds",
		"webhook_requests_total",
	}
	assert.DeepEqual(t, want, exportedNames(t))
}

// TestStateCommitLatencyUnitIsNameNeutral guards the unit alignment.
// state_commit_latency declared its unit as "s" where every other latency
// declares "seconds". The exporter normalizes UCUM "s" before appending it, so
// the series name is the same either way -- but a unit edit that did rename a
// series would break dashboards with no test failing.
func TestStateCommitLatencyUnitIsNameNeutral(t *testing.T) {
	coverage.Covers(t, "observability.metrics")
	found := false
	for _, n := range exportedNames(t) {
		if n == "state_commit_latency_seconds" {
			found = true
		}
	}
	assert.That(t, found)
}
