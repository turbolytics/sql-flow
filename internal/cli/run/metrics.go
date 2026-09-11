package run

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	prom "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/turbolytics/sql-flow/internal/core"
	"github.com/turbolytics/sql-flow/internal/turbostats"
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

// newHTTPMux builds the server the pipeline exposes: Prometheus scraping, a
// JSON view of durable state, and the TurboStats bundle.
//
// All on one mux deliberately. DuckDB takes an exclusive lock on the state
// file, so no other process can read it while the pipeline runs -- not even
// read-only. A running pipeline is the only thing that can report its own
// state, which makes these the live half of observability rather than a
// convenience.
//
// Each route is registered only when its provider is non-nil, so a route that
// would answer with nothing is absent rather than half-working.
func newHTTPMux(registry *prom.Registry, stats statsFunc,
	collect func(context.Context) (turbostats.Bundle, error)) *http.ServeMux {
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

	if collect != nil {
		mux.Handle("/turbostats/v1", turbostats.Handler(collect))
	}

	return mux
}

// newMeterProvider builds the provider every instrument records into, and
// starts the HTTP server when anything needs it.
//
// The manual reader is attached always: it is what /turbostats/v1 reads, and
// the outbound reporter after it. The Prometheus exporter is a second reader,
// attached only for --metrics=prometheus. Both read the same instruments,
// which is the property the design exists for. Before this, the provider
// existed only for Prometheus, and without it every counter recorded into
// nothing -- so there was nothing for a bundle to read.
func newMeterProvider(exporter string, serveTurbostats bool,
	static turbostats.Static, l *zap.Logger, stats statsFunc) (metric.MeterProvider, error) {

	reader := sdkmetric.NewManualReader()
	opts := []sdkmetric.Option{sdkmetric.WithReader(reader)}

	var registry *prom.Registry
	switch strings.ToLower(strings.TrimSpace(exporter)) {
	case "":
	case "prometheus":
		registry = prom.NewRegistry()
		exp, err := prometheus.New(prometheus.WithRegisterer(registry))
		if err != nil {
			return nil, fmt.Errorf("prometheus exporter: %w", err)
		}
		opts = append(opts, sdkmetric.WithReader(exp))
	default:
		return nil, fmt.Errorf("unsupported --metrics exporter: %q (supported: prometheus)", exporter)
	}

	mp := sdkmetric.NewMeterProvider(opts...)

	// Nothing to serve: the provider still exists, so the instruments record
	// and a later reporter can read them without an HTTP server.
	if registry == nil && !serveTurbostats {
		return mp, nil
	}

	var collect func(context.Context) (turbostats.Bundle, error)
	if serveTurbostats {
		collect = func(ctx context.Context) (turbostats.Bundle, error) {
			return turbostats.Collect(ctx, static, reader, stats)
		}
	}
	mux := newHTTPMux(registry, stats, collect)

	go func() {
		routes := []string{}
		if registry != nil {
			routes = append(routes, "/metrics")
		}
		if serveTurbostats {
			routes = append(routes, "/turbostats/v1")
		}
		l.Info("serving http", zap.String("addr", metricsPort), zap.Strings("routes", routes))
		if err := http.ListenAndServe(metricsPort, mux); err != nil {
			l.Error("http server stopped", zap.Error(err))
		}
	}()

	return mp, nil
}
