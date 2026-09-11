package run

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

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

// progressFunc reports the pipeline's liveness snapshot: when the newest
// batch arrived, when state last committed, and how many messages have been
// consumed.
type progressFunc func() core.Progress

// stuckIntervals is how many flush intervals may pass with no commit before
// /healthz calls the pipeline stuck. Three, so one slow sink flush cannot
// flap it.
const stuckIntervals = 3

// newHTTPMux builds the server the pipeline exposes: Prometheus scraping, a
// JSON view of durable state and progress, and a health check.
//
// The first two are on one mux deliberately. DuckDB takes an exclusive lock
// on the state file, so no other process can read it while the pipeline runs
// -- not even read-only. A running pipeline is the only thing that can report
// its own state, which makes this endpoint the live half of observability
// rather than a convenience.
//
// /healthz answers the one question a supervisor has, and it is not "are
// messages arriving": a pipeline on a low-traffic topic may go hours between
// them and be perfectly well. It is "is this pipeline still doing its work",
// which is the commit clock, because the idle tick commits whether or not
// anything arrived. Idle is healthy. Stuck is stuckIntervals without a
// commit, and the body carries the age so an operator need not guess.
//
// now is injectable for the test. started covers the window before the first
// commit: a pipeline that has just launched has no commit clock yet, and
// must not be called stuck for not having one, but must be called stuck if
// it never gets one.
func newHTTPMux(registry *prom.Registry, stats statsFunc, progress progressFunc, interval time.Duration, now func() time.Time) *http.ServeMux {
	mux := http.NewServeMux()
	started := now()

	if registry != nil {
		mux.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
	}

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

	if stats != nil || progress != nil {
		mux.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
			out := map[string]any{"state": nil}

			if stats != nil {
				state, err := stats()
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
					"messages":            p.Messages,
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
			_, _, commit := ages()
			w.Header().Set("Content-Type", "application/json")

			if commit > float64(stuckIntervals)*interval.Seconds() {
				w.WriteHeader(http.StatusServiceUnavailable)
				_ = json.NewEncoder(w).Encode(map[string]any{
					"status":             "stuck",
					"commit_age_seconds": commit,
					"interval_seconds":   interval.Seconds(),
				})
				return
			}

			_ = json.NewEncoder(w).Encode(map[string]any{
				"status":             "ok",
				"commit_age_seconds": commit,
			})
		})
	}

	return mux
}

// newMeterProvider starts the exporter named by --metrics. An empty name
// disables metrics entirely.
func newMeterProvider(name string, l *zap.Logger, stats statsFunc, progress progressFunc, interval time.Duration) (metric.MeterProvider, error) {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "":
		return nil, nil

	case "prometheus":
		registry := prom.NewRegistry()
		exporter, err := prometheus.New(prometheus.WithRegisterer(registry))
		if err != nil {
			return nil, fmt.Errorf("prometheus exporter: %w", err)
		}

		mux := newHTTPMux(registry, stats, progress, interval, time.Now)

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

// flushIntervalFor is the one place the flush interval is decided. Absent,
// zero and negative all mean the default: a ticker the pipeline cannot lose,
// because a batch that a low-traffic topic never fills would otherwise wait
// forever. The config schema's own floor is thirty seconds, so this only
// ever fires for a config that omits the key.
func flushIntervalFor(seconds int) time.Duration {
	if seconds > 0 {
		return time.Duration(seconds) * time.Second
	}
	return 30 * time.Second
}
