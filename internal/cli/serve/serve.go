// Package serve is the `sqlflow serve` command.
package serve

import (
	"context"
	"errors"
	"fmt"
	prom "github.com/prometheus/client_golang/prometheus"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/spf13/cobra"
	"github.com/turbolytics/sql-flow/internal/activity"
	"github.com/turbolytics/sql-flow/internal/buildinfo"
	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/core"
	"github.com/turbolytics/sql-flow/internal/duckdb"
	"github.com/turbolytics/sql-flow/internal/errs"
	"github.com/turbolytics/sql-flow/internal/logging"
	api "github.com/turbolytics/sql-flow/internal/serve"
	"github.com/turbolytics/sql-flow/internal/turbostats"
	"github.com/turbolytics/sql-flow/turbostats/wire"
	"go.uber.org/zap"
)

func NewCommand() *cobra.Command {
	var configPath string
	var enablePprof bool
	var serveTurbostats bool

	cmd := &cobra.Command{
		Use:   "serve [config]",
		Short: "Serve a config's datasets over HTTP",
		Long: "Serve the named SQL datasets a serve config declares. Each request binds " +
			"its params into a statement fixed at startup and answers with JSON rows.",
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			logger, levelErr := logging.New()
			defer logger.Sync()
			l := logger.Named("sqlflow.serve")
			if levelErr != nil {
				return levelErr
			}

			path, err := resolveConfigPath(configPath, args)
			if err != nil {
				cmd.SilenceUsage = false
				return err
			}

			if enablePprof {
				runtime.SetBlockProfileRate(1)
				runtime.SetMutexProfileFraction(1)
				go func() {
					l.Info("starting pprof server on :6060")
					if err := http.ListenAndServe(":6060", nil); err != nil {
						l.Error("failed to start pprof server", zap.Error(err))
					}
				}()
			}

			// A supervisor stops the server with SIGTERM. Without the handler
			// the process dies mid-request and the deferred close never runs.
			// The command's own context is the parent, for the reason run
			// gives: cobra supplies Background here, and an embedding caller
			// gets a command it can stop.
			parent := cmd.Context()
			if parent == nil {
				parent = context.Background()
			}
			ctx, stop := signal.NotifyContext(parent, syscall.SIGINT, syscall.SIGTERM)
			defer stop()

			return serveConfig(ctx, path, l, nil, serveTurbostats)
		},
	}

	cmd.Flags().StringVarP(&configPath, "config", "c", "", "Path to the serve config (or pass it positionally)")
	cmd.Flags().BoolVar(&enablePprof, "pprof", false, "Enable pprof profiling server on :6060")
	cmd.Flags().BoolVar(&serveTurbostats, "turbostats", false,
		"Serve GET /turbostats/v1 on the serve address: the process's own state as one document")
	return cmd
}

// serveConfig loads a serve config, attaches its data, and answers requests
// until ctx ends. onListen, when set, receives the bound address; tests bind
// port 0 and need to learn which port that was.
func serveConfig(ctx context.Context, path string, l *zap.Logger, onListen func(net.Addr), serveTurbostats bool) error {
	// Taken here, once. Its wall time is the bundle's started_at, and a
	// restart is a receiver noticing it changed. Uptime and idle come from
	// its monotonic reading.
	clock := activity.Start()

	conf, rendered, err := config.LoadServeRendered(path, nil)
	if err != nil {
		return err
	}
	// Before DuckDB opens: a config error needs no attached database to find.
	if err := conf.CheckError(); err != nil {
		return err
	}

	db, err := duckdb.OpenPath(ctx, "")
	if err != nil {
		return err
	}
	defer func() {
		if err := db.Close(); err != nil {
			l.Error("failed to close DuckDB database", zap.Error(err))
		}
	}()

	ex, err := api.NewDuckDBExecutor(ctx, db, conf.Serve.PoolSize(),
		func(ctx context.Context, conn adbc.Connection) error {
			// Uncoded, as in run: an ATTACH that fails because the database
			// is not up yet exits 1, which a supervisor retries. Redacted
			// because a failed ATTACH prints the connection string.
			if err := core.InitCommands(conn, &config.Conf{Commands: conf.Commands}); err != nil {
				return errors.New("failed to initialize commands: " + api.Redact(err.Error()))
			}
			return nil
		}, nil)
	if err != nil {
		return err
	}

	opts := []api.Option{api.WithLogger(l)}
	if conf.Serve.MetricsEnabled() {
		opts = append(opts, api.WithMetrics(prom.NewRegistry()))
	}
	ts := conf.Serve.TurboStats
	static := turbostats.Static{
		Name:       conf.Serve.Name,
		Version:    buildinfo.Version,
		Commit:     buildinfo.Commit,
		ConfigHash: turbostats.HashConfig(rendered),
		StartedAt:  clock.StartedAt(),
		Clock:      clock,
	}
	if ts.Enabled() {
		static.ID = ts.ID
		// The receiver needs the interval to tell a late heartbeat from a
		// normal one, and only the reporter knows it.
		static.IntervalSeconds = int(ts.Interval().Seconds())
	}
	// Always: the reporter reads the same builder, and a fleet instance
	// reports without serving anything.
	opts = append(opts, api.WithTurbostats(static, serveTurbostats))

	srv, err := api.New(ctx, conf, ex, opts...)
	if err != nil {
		ex.Close()
		return err
	}
	// Deferred after the database's close, so it runs first: a query still
	// holding a session finishes before the database closes under it.
	defer srv.Close()

	ln, err := net.Listen("tcp", conf.Serve.Addr())
	if err != nil {
		return fmt.Errorf("listening on %s: %w", conf.Serve.Addr(), err)
	}

	l.Info("serving", zap.String("name", conf.Serve.Name), zap.String("addr", ln.Addr().String()))
	for _, ds := range conf.Serve.Datasets {
		l.Info("dataset", zap.String("name", ds.Name), zap.Strings("grains", ds.GrainNames()))
	}
	if onListen != nil {
		onListen(ln.Addr())
	}

	// The reporter reads the same bundle builder the route serves, so a
	// server with the route off still reports. Started before Serve blocks,
	// so an instance appears on a fleet page as soon as it is listening.
	var serveErr error
	if ts.Enabled() {
		key, err := wire.ParseCredential(ts.Key)
		if err != nil {
			return errs.New(errs.CodeConfigInvalid, "turbostats.key is not a credential")
		}
		reporter, err := turbostats.NewReporter(turbostats.ReporterConfig{
			ReportTo: ts.ReportTo, Key: key, Interval: ts.Interval(),
			Collect: srv.CollectBundle, Log: l.Named("turbostats"),
		})
		if err != nil {
			return err
		}
		stopReporter := turbostats.StartReporter(ctx, reporter)

		// A defer, so the goroutine is stopped and waited for however this
		// function returns. It runs after Serve, so the last bundle counts
		// every request the server answered. A crash never reaches it, which
		// is how a receiver tells a clean stop from one.
		//
		// The reason is derived rather than hardcoded. It used to say
		// "stopped" whatever Serve returned, so a server that fell over
		// reported the exit code of a crash and the reason of a clean stop --
		// and the reason is the field whose whole job is telling those apart.
		defer func() {
			final, cancel := context.WithTimeout(
				context.WithoutCancel(ctx), shutdownGrace)
			defer cancel()
			stopReporter(final, turbostats.Exit{
				Reason: turbostats.ExitReason(serveErr, ctx.Err()),
				Code:   errs.ExitCode(serveErr),
			})
		}()
	}

	serveErr = srv.Serve(ctx, ln)
	return serveErr
}

// shutdownGrace bounds the last bundle. The reporter has its own timeout; this
// is the outer bound on the whole step, so a stop is never held open by a
// control plane that stopped answering.
const shutdownGrace = 15 * time.Second
