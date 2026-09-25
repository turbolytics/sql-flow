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
	chunkDuration   metric.Float64Histogram
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
	if in.chunkDuration, err = m.Float64Histogram("rollup_backfill_chunk_duration",
		metric.WithDescription("One backfill chunk's transaction"), metric.WithUnit("s")); err != nil {
		return nil, err
	}
	if in.errors, err = m.Int64Counter("rollup_errors",
		metric.WithDescription("Errors by phase: install, backfill or lock")); err != nil {
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

// newProvider builds the meter provider and, for the prometheus exporter,
// the registry /metrics serves. With no exporter the instruments record into
// a provider nothing reads, so the loop never checks for nil.
func newProvider(exporter string) (metric.MeterProvider, *prom.Registry, error) {
	switch strings.ToLower(strings.TrimSpace(exporter)) {
	case "":
		return sdkmetric.NewMeterProvider(), nil, nil
	case "prometheus":
		registry := prom.NewRegistry()
		exp, err := prometheus.New(prometheus.WithRegisterer(registry))
		if err != nil {
			return nil, nil, fmt.Errorf("prometheus exporter: %w", err)
		}
		return sdkmetric.NewMeterProvider(sdkmetric.WithReader(exp)), registry, nil
	default:
		return nil, nil, errs.New(errs.CodeConfigInvalid,
			"--metrics %q is not an exporter this version serves; use prometheus", exporter)
	}
}
