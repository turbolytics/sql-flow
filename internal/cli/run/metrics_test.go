package run

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sort"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	prom "github.com/prometheus/client_golang/prometheus"
	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/core"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/turbolytics/sql-flow/internal/sinks"
	"github.com/turbolytics/sql-flow/internal/turbostats"
	"github.com/turbolytics/sql-flow/internal/webhook"
	"github.com/zeebo/assert"
	"go.opentelemetry.io/otel/exporters/prometheus"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.uber.org/zap"
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

	mux := newHTTPMux(nil, func() (*core.StateStats, error) { return want, nil }, nil, nil, nil, 30*time.Second, time.Now)

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
	mux := newHTTPMux(nil, func() (*core.StateStats, error) { return nil, nil }, nil, nil, nil, 30*time.Second, time.Now)

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
	}, nil, nil, nil, 30*time.Second, time.Now)

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/stats", nil))
	assert.Equal(t, http.StatusInternalServerError, rec.Code)
}

// With no stats provider at all -- metrics enabled but state unwired -- the
// endpoint must not be registered as a half-working route.
func TestObservabilityMetrics_StatsHandler_AbsentWithoutAProvider(t *testing.T) {
	coverage.Covers(t, "observability.metrics")
	mux := newHTTPMux(nil, nil, nil, nil, nil, 30*time.Second, time.Now)

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
	m.PhaseDuration.Record(ctx, 1)
	m.ConsumerLag.Record(ctx, 1)
	m.StateCommitLatency.Record(ctx, 1)
	m.StateCommitCount.Add(ctx, 1)
	m.StateSizeBytes.Record(ctx, 1)
	m.StateTableRows.Record(ctx, 1)
	m.ReferenceTableRows.Record(ctx, 1)
	// The flat twins the TurboStats bundle reads. They export too, which is
	// the cost of one instrument feeding both readers.
	m.PipelineErrors.Add(ctx, 1)
	m.PipelineFlushes.Add(ctx, 1)
	m.PipelineCommits.Add(ctx, 1)
	m.PipelineRowsAccepted.Add(ctx, 1)
	m.PipelineRowsWritten.Add(ctx, 1)
	m.PipelineLastMessage.Record(ctx, 1)

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
		"phase_duration_seconds",
		"pipeline_commits_total",
		"pipeline_errors_total",
		"pipeline_flushes_total",
		"pipeline_last_message_timestamp_seconds",
		"pipeline_rows_accepted_total",
		"pipeline_rows_written_total",
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

// wantLatencyBuckets is the boundary set every latency histogram declares, in
// seconds.
//
// Written out here rather than imported from core on purpose: a test that read
// the production constant would agree with whatever that constant became, and
// the defect this guards against is a silent change of boundaries.
var wantLatencyBuckets = []float64{
	0.0001, 0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5,
	1, 2.5, 5, 10, 30, 60,
}

// exportedBuckets returns the bucket upper bounds each histogram exports.
func exportedBuckets(t *testing.T) map[string][]float64 {
	t.Helper()

	reg := prom.NewRegistry()
	exp, err := prometheus.New(prometheus.WithRegisterer(reg))
	assert.NoError(t, err)
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(exp))

	m, err := core.NewMetrics(mp)
	assert.NoError(t, err)

	ctx := context.Background()
	m.SourceReadLatency.Record(ctx, 1)
	m.SinkFlushLatency.Record(ctx, 1)
	m.BatchProcessingLatency.Record(ctx, 1)
	m.StateCommitLatency.Record(ctx, 1)
	m.PhaseDuration.Record(ctx, 1)

	families, err := reg.Gather()
	assert.NoError(t, err)

	got := map[string][]float64{}
	for _, f := range families {
		for _, mm := range f.GetMetric() {
			h := mm.GetHistogram()
			if h == nil {
				continue
			}
			var bounds []float64
			for _, b := range h.GetBucket() {
				bounds = append(bounds, b.GetUpperBound())
			}
			got[f.GetName()] = bounds
		}
	}
	return got
}

// TestLatencyHistogramBucketsAreSecondsShaped pins the boundaries of every
// latency histogram.
//
// The OTel SDK's default explicit boundaries are millisecond-shaped -- 0, 5,
// 10, 25 ... 10000 -- and every histogram here records seconds. Under the
// defaults a 200 microsecond flush and a 4 second flush land in the same
// bucket, so histogram_quantile over any of these returned a number between 0
// and 5 and meant nothing. Only _sum and _count worked.
//
// What breaks if this is wrong: an operator reads a p99 that is not a p99.
func TestLatencyHistogramBucketsAreSecondsShaped(t *testing.T) {
	coverage.Covers(t, "observability.metrics")
	got := exportedBuckets(t)

	for _, name := range []string{
		"batch_processing_latency_seconds",
		"phase_duration_seconds",
		"sink_flush_latency_seconds",
		"source_read_latency_seconds",
		"state_commit_latency_seconds",
	} {
		bounds, ok := got[name]
		assert.That(t, ok)
		assert.DeepEqual(t, wantLatencyBuckets, bounds)
	}
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

// --- TurboStats ------------------------------------------------------------

// /turbostats/v1 is present only when asked for, like /stats: a route that
// answers with nothing is worse than none.
func TestObservabilityTurbostats_RouteAbsentWithoutACollector(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	mux := newHTTPMux(nil, nil, nil, nil, nil, 30*time.Second, time.Now)

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/turbostats/v1", nil))
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestObservabilityTurbostats_RouteServesTheBundle(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	mux := newHTTPMux(nil, nil, func(context.Context) (turbostats.Bundle, error) {
		return turbostats.Bundle{V: turbostats.Version}, nil
	}, nil, nil, 30*time.Second, time.Now)

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/turbostats/v1", nil))
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, turbostats.MediaType, rec.Header().Get("Content-Type"))
}

// Attaching the manual reader must not change what Prometheus exports. The
// series list is what the README documents and dashboards depend on.
func TestObservabilityTurbostats_ManualReaderLeavesExportedNamesAlone(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	coverage.Covers(t, "observability.metrics")
	reg := prom.NewRegistry()
	exp, err := prometheus.New(prometheus.WithRegisterer(reg))
	assert.NoError(t, err)
	manual := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(manual), sdkmetric.WithReader(exp))

	m, err := core.NewMetrics(mp)
	assert.NoError(t, err)
	m.MessageCount.Add(context.Background(), 1)

	families, err := reg.Gather()
	assert.NoError(t, err)
	found := false
	for _, f := range families {
		if f.GetName() == "message_count_messages_total" {
			found = true
		}
	}
	assert.That(t, found)
}

// The provider exists even with no exporter, so the instruments record and a
// reporter can read them. It used to be nil, and every counter recorded into
// nothing unless Prometheus was on.
func TestObservabilityTurbostats_ProviderExistsWithoutAnExporter(t *testing.T) {
	coverage.Covers(t, "observability.turbostats")
	mp, err := newMeterProvider("", false, turbostats.Static{}, zap.NewNop(), nil, nil, nil, 30*time.Second)
	assert.NoError(t, err)
	assert.That(t, mp != nil)

	m, err := core.NewMetrics(mp)
	assert.NoError(t, err)
	m.MessageCount.Add(context.Background(), 1)
}

func TestObservabilityTurbostats_RejectsAnUnknownExporter(t *testing.T) {
	coverage.Covers(t, "observability.metrics")
	_, err := newMeterProvider("statsd", false, turbostats.Static{}, zap.NewNop(), nil, nil, nil, 30*time.Second)
	assert.Error(t, err)
}
