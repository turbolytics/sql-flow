package run

import (
	"fmt"
	"net/http"
	"strings"

	prom "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.uber.org/zap"
)

// metricsPort is where the Python engine exposes its Prometheus scrape
// endpoint, so dashboards and scrape configs carry over unchanged.
const metricsPort = ":8000"

// newMeterProvider starts the exporter named by --metrics. An empty name
// disables metrics entirely.
func newMeterProvider(name string, l *zap.Logger) (metric.MeterProvider, error) {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "":
		return nil, nil

	case "prometheus":
		registry := prom.NewRegistry()
		exporter, err := prometheus.New(prometheus.WithRegisterer(registry))
		if err != nil {
			return nil, fmt.Errorf("prometheus exporter: %w", err)
		}

		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))

		go func() {
			l.Info("serving prometheus metrics", zap.String("addr", metricsPort+"/metrics"))
			if err := http.ListenAndServe(metricsPort, mux); err != nil {
				l.Error("metrics server stopped", zap.Error(err))
			}
		}()

		return sdkmetric.NewMeterProvider(sdkmetric.WithReader(exporter)), nil

	default:
		return nil, fmt.Errorf("unsupported --metrics exporter: %q (supported: prometheus)", name)
	}
}
