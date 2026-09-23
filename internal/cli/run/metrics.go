package run

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	prom "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/turbolytics/sql-flow/internal/config"
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

// collectFunc builds one bundle. The route and the reporter share it.
type collectFunc func(context.Context) (turbostats.Bundle, error)

// statsFunc reports a snapshot of the pipeline's durable state. It returns a
// nil snapshot, and no error, for a pipeline that has no state database.
type statsFunc func(context.Context) (*core.StateStats, error)

// progressFunc reports the pipeline's liveness snapshot: when the newest
// batch arrived, when state last committed, and how many messages have been
// consumed.
type progressFunc func() core.Progress

// lastErrorFunc is the code and time of the pipeline's last error, and
// false before the first one. The turbine is built after the meter
// provider, so the command passes a closure over a holder rather than the
// turbine itself.
type lastErrorFunc func() (code string, at time.Time, ok bool)

// eventBasisFunc is where the pipeline's event times come from, and empty
// for a source with none. A closure for the same reason lastErrorFunc is
// one: the source is built after the meter provider.
type eventBasisFunc func() string

// stuckIntervals is how many flush intervals may pass with no commit before
// /healthz calls the pipeline stuck. Three, so one slow sink flush cannot
// flap it.
const stuckIntervals = 3

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
	bundle http.Handler,
	progress progressFunc, health healthFunc, interval time.Duration,
	now func() time.Time) *http.ServeMux {
	mux := http.NewServeMux()
	started := now()

	// ages reports the snapshot with its two ages in seconds. An age of -1
	// means the clock has never moved, which for arrivals is an ordinary
	// fact about a stream that has not delivered yet.
	ages := func() (core.Progress, float64, float64) {
		p := progress()
		arrival := -1.0
		if !p.LastArrival.IsZero() {
			arrival = now().Sub(p.LastArrival).Seconds()
		}
		commit := now().Sub(started).Seconds()
		if !p.LastCommit.IsZero() {
			commit = now().Sub(p.LastCommit).Seconds()
		}
		return p, arrival, commit
	}

	if registry != nil {
		mux.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
	}

	if stats != nil || progress != nil {
		mux.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
			out := map[string]any{"state": nil}

			if stats != nil {
				state, err := stats(r.Context())
				if err != nil {
					// A monitoring system must see the failure, not a
					// healthy-looking blank.
					http.Error(w, fmt.Sprintf("collecting state stats: %v", err),
						http.StatusInternalServerError)
					return
				}
				// state is nil for a pipeline with no state database, which
				// encodes as null: absent state and empty state are different
				// facts and a dashboard should be able to tell them apart.
				out["state"] = state
			}

			if progress != nil {
				p, arrival, commit := ages()
				out["progress"] = map[string]any{
					"last_arrival":        p.LastArrival,
					"last_commit":         p.LastCommit,
					"last_error":          p.LastError,
					"messages":            p.Messages,
					"errors":              p.Errors,
					"arrival_age_seconds": arrival,
					"commit_age_seconds":  commit,
				}
			}

			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(out); err != nil {
				return
			}
		})
	}

	if progress != nil {
		mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
			p, _, commit := ages()
			var snap healthSnapshot
			if health != nil {
				snap = health()
			}
			status, reason, code := healthStatus(p, commit, snap, interval, now())

			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(code)
			body := map[string]any{
				"status":             status,
				"commit_age_seconds": commit,
				"interval_seconds":   interval.Seconds(),
			}
			if reason != "" {
				body["reason"] = reason
			}
			_ = json.NewEncoder(w).Encode(body)
		})
	}

	if bundle != nil {
		mux.Handle("/turbostats/v1", bundle)
	}

	return mux
}

// meterOption configures newMeterProvider.
//
// The provider is built from ten independent facts. A positional parameter
// for each made every call site a row of nils whose meaning depended on
// counting commas. Each option below names one fact and says what its
// absence means.
type meterOption func(*meterOptions)

// meterOptions is what newMeterProvider builds from. Every zero is a working
// default: no exporter, no HTTP routes, and a logger that drops what it is
// told.
type meterOptions struct {
	exporter        string
	serveTurbostats bool
	static          turbostats.Static
	logger          *zap.Logger
	stats           statsFunc
	progress        progressFunc
	health          healthFunc
	lastError       lastErrorFunc
	eventBasis      eventBasisFunc
	interval        time.Duration
}

// withExporter names the exporter --metrics asked for. Empty means none, and
// the manual reader the bundle reads is attached either way. An unsupported
// name fails the build rather than starting a pipeline that exports nothing.
func withExporter(name string) meterOption {
	return func(o *meterOptions) { o.exporter = name }
}

// withTurbostatsRoute serves the bundle at GET /turbostats/v1. The bundle is
// built either way, because the reporter reads the same one and has no HTTP
// server of its own.
func withTurbostatsRoute(serve bool) meterOption {
	return func(o *meterOptions) { o.serveTurbostats = serve }
}

// withStatic supplies what the bundle says this process is: name, version,
// commit, config hash and labels. Absent, the bundle reports only what the
// instruments carry.
func withStatic(s turbostats.Static) meterOption {
	return func(o *meterOptions) { o.static = s }
}

// withLogger supplies the logger the HTTP server and the bundle handler
// report failures through. Without one those failures are dropped.
func withLogger(l *zap.Logger) meterOption {
	return func(o *meterOptions) { o.logger = l }
}

// withStateStats supplies the pipeline's durable-state reader. It serves GET
// /stats and fills the bundle's state section. Absent, neither reports state,
// which is the right answer for a pipeline that has no state database.
func withStateStats(f statsFunc) meterOption {
	return func(o *meterOptions) { o.stats = f }
}

// withProgress supplies the pipeline's liveness snapshot. It registers GET
// /stats and GET /healthz. Without it neither route exists, because a health
// check that cannot read progress would answer healthy for a dead pipeline.
func withProgress(f progressFunc) meterOption {
	return func(o *meterOptions) { o.progress = f }
}

// withHealth supplies the process's health snapshot, which /healthz reads
// beside progress. It does nothing without withProgress, because /healthz is
// registered on progress.
func withHealth(f healthFunc) meterOption {
	return func(o *meterOptions) { o.health = f }
}

// withLastError supplies the code and time of the pipeline's last error.
// Absent, the bundle omits both: a pipeline whose last error is unknown is
// not a pipeline that has had none.
func withLastError(f lastErrorFunc) meterOption {
	return func(o *meterOptions) { o.lastError = f }
}

// withEventBasis supplies where the pipeline's event times come from. Absent,
// the bundle omits event lag, because a lag whose basis is unknown is a
// number no reader can compare.
func withEventBasis(f eventBasisFunc) meterOption {
	return func(o *meterOptions) { o.eventBasis = f }
}

// withFlushInterval tells /healthz how long the pipeline may go without a
// commit before it is stuck. It does nothing without withProgress.
func withFlushInterval(d time.Duration) meterOption {
	return func(o *meterOptions) { o.interval = d }
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
// It also returns the bundle builder, because the reporter needs the same one
// the route serves. One builder, two transports, is the property the contract
// exists for: a second one here would let what an operator curls and what a
// control plane stores drift apart.
func newMeterProvider(opts ...meterOption) (metric.MeterProvider, collectFunc, error) {
	o := meterOptions{logger: zap.NewNop()}
	for _, opt := range opts {
		opt(&o)
	}

	reader := sdkmetric.NewManualReader()
	providerOpts := []sdkmetric.Option{sdkmetric.WithReader(reader)}

	var registry *prom.Registry
	switch strings.ToLower(strings.TrimSpace(o.exporter)) {
	case "":
	case "prometheus":
		registry = prom.NewRegistry()
		exp, err := prometheus.New(prometheus.WithRegisterer(registry))
		if err != nil {
			return nil, nil, fmt.Errorf("prometheus exporter: %w", err)
		}
		providerOpts = append(providerOpts, sdkmetric.WithReader(exp))
	default:
		return nil, nil, fmt.Errorf("unsupported --metrics exporter: %q (supported: prometheus)", o.exporter)
	}

	mp := sdkmetric.NewMeterProvider(providerOpts...)

	// Built whether or not anything serves it: the reporter reads the same
	// one, and it has no HTTP server of its own.
	collect := func(ctx context.Context) (turbostats.Bundle, error) {
		return turbostats.Collect(ctx, turbostats.Source{
			Static: o.static,
			Reader: reader,
			Pipeline: &turbostats.PipelineSource{
				Stats: o.stats, LastError: o.lastError, EventBasis: basisOf(o.eventBasis),
			},
		})
	}

	// Nothing to serve: the provider still exists, so the instruments record
	// and a later reporter can read them without an HTTP server.
	if registry == nil && !o.serveTurbostats && o.progress == nil {
		return mp, collect, nil
	}

	// Built here rather than in newHTTPMux because a failure is logged, and
	// this is where the logger is. The response never carries the error: the
	// route is unauthenticated, and a state backend's error can name its
	// connection string.
	var bundle http.Handler
	if o.serveTurbostats {
		bundle = turbostats.Handler(collect, func(err error) {
			o.logger.Error("building turbostats bundle", zap.Error(err))
		})
	}
	mux := newHTTPMux(registry, o.stats, bundle, o.progress, o.health, o.interval, time.Now)

	go func() {
		routes := []string{}
		if registry != nil {
			routes = append(routes, "/metrics")
		}
		if o.serveTurbostats {
			routes = append(routes, "/turbostats/v1")
		}
		if o.progress != nil {
			routes = append(routes, "/healthz")
		}
		o.logger.Info("serving http", zap.String("addr", metricsPort), zap.Strings("routes", routes))
		if err := http.ListenAndServe(metricsPort, mux); err != nil {
			o.logger.Error("http server stopped", zap.Error(err))
		}
	}()

	return mp, collect, nil
}

// flushIntervalFor is the one place the flush interval is decided. Absent,
// zero and negative all mean the default: a ticker the pipeline cannot lose,
// because a batch that a low-traffic topic never fills would otherwise wait
// forever.
func flushIntervalFor(seconds int) time.Duration {
	if seconds > 0 {
		return time.Duration(seconds) * time.Second
	}
	return config.DefaultFlushIntervalSeconds * time.Second
}

// drainDeadlineFor is the one place the drain deadline is decided. Absent,
// zero and negative all mean the default, for the same reason as the flush
// interval: a shutdown the config forgot to bound is still bounded.
func drainDeadlineFor(seconds int) time.Duration {
	if seconds > 0 {
		return time.Duration(seconds) * time.Second
	}
	return core.DefaultDrainDeadline
}

// basisOf reads the basis, tolerating the nil a test passes.
func basisOf(f eventBasisFunc) string {
	if f == nil {
		return ""
	}
	return f()
}
