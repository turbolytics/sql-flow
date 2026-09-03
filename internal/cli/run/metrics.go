package run

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	prom "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/turbolytics/sql-flow/internal/core"
	"go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.uber.org/zap"
)

// metricsPort is where the Python engine exposes its Prometheus scrape
// endpoint, so dashboards and scrape configs carry over unchanged.
const metricsPort = ":8000"

// statsFunc reports a snapshot of the pipeline's durable state. It returns a
// nil snapshot, and no error, for a pipeline that has no state database.
type statsFunc func() (*core.StateStats, error)

// newHTTPMux builds the server the pipeline exposes: Prometheus scraping, and
// a JSON view of durable state.
//
// The two are on one mux deliberately. DuckDB takes an exclusive lock on the
// state file, so no other process can read it while the pipeline runs -- not
// even read-only. A running pipeline is the only thing that can report its own
// state, which makes this endpoint the live half of observability rather than
// a convenience.
func newHTTPMux(registry *prom.Registry, stats statsFunc) *http.ServeMux {
	mux := http.NewServeMux()

	if registry != nil {
		mux.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
	}

	if stats != nil {
		mux.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
			state, err := stats()
			if err != nil {
				// A monitoring system must see the failure, not a
				// healthy-looking blank.
				http.Error(w, fmt.Sprintf("collecting state stats: %v", err),
					http.StatusInternalServerError)
				return
			}

			w.Header().Set("Content-Type", "application/json")
			// state is nil for a pipeline with no state database, which
			// encodes as null: absent state and empty state are different
			// facts and a dashboard should be able to tell them apart.
			if err := json.NewEncoder(w).Encode(map[string]any{"state": state}); err != nil {
				return
			}
		})
	}

	return mux
}

// newMeterProvider starts the exporter named by --metrics. An empty name
// disables metrics entirely.
func newMeterProvider(name string, l *zap.Logger, stats statsFunc) (metric.MeterProvider, error) {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "":
		return nil, nil

	case "prometheus":
		registry := prom.NewRegistry()
		exporter, err := prometheus.New(prometheus.WithRegisterer(registry))
		if err != nil {
			return nil, fmt.Errorf("prometheus exporter: %w", err)
		}

		mux := newHTTPMux(registry, stats)

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
