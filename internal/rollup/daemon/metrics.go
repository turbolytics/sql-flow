package daemon

import (
	"fmt"
	"strings"

	prom "github.com/prometheus/client_golang/prometheus"
	"github.com/turbolytics/sql-flow/internal/errs"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

// instruments are the daemon's OTel instruments, named as the spec's
// metrics table names them. The Prometheus exporter appends the unit and
// _total.
type instruments struct {
	backfillBuckets metric.Int64Counter
	backfillBusy    metric.Int64Counter
	chunkDuration   metric.Float64Histogram
	verifyBuckets   metric.Int64Counter
	driftBuckets    metric.Int64Counter
	verifyDuration  metric.Float64Histogram
	newestBucket    metric.Float64Gauge
	errors          metric.Int64Counter
	leaderAcquired  metric.Int64Counter
}

func newInstruments(mp metric.MeterProvider) (*instruments, error) {
	m := mp.Meter("sqlflow.rollup")
	var in instruments
	var err error
	if in.backfillBuckets, err = m.Int64Counter("rollup_backfill_buckets",
		metric.WithDescription("Buckets written by backfill")); err != nil {
		return nil, err
	}
	if in.backfillBusy, err = m.Int64Counter("rollup_backfill_busy",
		metric.WithDescription("Chunks that gave up because a write held one of their buckets on every try")); err != nil {
		return nil, err
	}
	if in.chunkDuration, err = m.Float64Histogram("rollup_backfill_chunk_duration",
		metric.WithDescription("One backfill chunk's transaction"), metric.WithUnit("s")); err != nil {
		return nil, err
	}
	if in.verifyBuckets, err = m.Int64Counter("rollup_verify_buckets",
		metric.WithDescription("Buckets recomputed and compared")); err != nil {
		return nil, err
	}
	if in.driftBuckets, err = m.Int64Counter("rollup_drift_buckets",
		metric.WithDescription("Buckets that differed from the table they are built from")); err != nil {
		return nil, err
	}
	if in.verifyDuration, err = m.Float64Histogram("rollup_verify_duration",
		metric.WithDescription("One verify pass"), metric.WithUnit("s")); err != nil {
		return nil, err
	}
	if in.newestBucket, err = m.Float64Gauge("rollup_newest_bucket_timestamp",
		metric.WithDescription("The start of the newest bucket of each table and of the source, in Unix seconds"),
		metric.WithUnit("s")); err != nil {
		return nil, err
	}
	if in.errors, err = m.Int64Counter("rollup_errors",
		metric.WithDescription("Errors by phase: install, backfill, verify, observe or lock")); err != nil {
		return nil, err
	}
	if in.leaderAcquired, err = m.Int64Counter("rollup_leader_acquired",
		metric.WithDescription("Times this process became the leader")); err != nil {
		return nil, err
	}
	return &in, nil
}

// phase labels an error by the phase that raised it.
func phase(name string) metric.AddOption {
	return metric.WithAttributes(attribute.String("phase", name))
}

// newProvider builds the meter provider, the manual reader the TurboStats
// bundle reads, and, for the prometheus exporter, the registry /metrics
// serves. The manual reader is always attached, as serve and run attach
// theirs, so a daemon that reports without serving /metrics has a reader.
func newProvider(exporter string) (metric.MeterProvider, *sdkmetric.ManualReader, *prom.Registry, error) {
	reader := sdkmetric.NewManualReader()
	opts := []sdkmetric.Option{sdkmetric.WithReader(reader)}
	switch strings.ToLower(strings.TrimSpace(exporter)) {
	case "":
		return sdkmetric.NewMeterProvider(opts...), reader, nil, nil
	case "prometheus":
		registry := prom.NewRegistry()
		exp, err := prometheus.New(prometheus.WithRegisterer(registry))
		if err != nil {
			return nil, nil, nil, fmt.Errorf("prometheus exporter: %w", err)
		}
		opts = append(opts, sdkmetric.WithReader(exp))
		return sdkmetric.NewMeterProvider(opts...), reader, registry, nil
	default:
		return nil, nil, nil, errs.New(errs.CodeConfigInvalid,
			"--metrics %q is not an exporter this version serves; use prometheus", exporter)
	}
}
