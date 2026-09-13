// Package serve is the `sqlflow serve` command.
package serve

import (
	"context"
	"fmt"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os/signal"
	"runtime"
	"syscall"

	"github.com/spf13/cobra"
	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/core"
	"github.com/turbolytics/sql-flow/internal/duckdb"
	"github.com/turbolytics/sql-flow/internal/logging"
	api "github.com/turbolytics/sql-flow/internal/serve"
	"go.uber.org/zap"
)

func NewCommand() *cobra.Command {
	var configPath string
	var enablePprof bool

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
			ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
			defer stop()

			return serveConfig(ctx, path, l, nil)
		},
	}

	cmd.Flags().StringVarP(&configPath, "config", "c", "", "Path to the serve config (or pass it positionally)")
	cmd.Flags().BoolVar(&enablePprof, "pprof", false, "Enable pprof profiling server on :6060")
	return cmd
}

// serveConfig loads a serve config, attaches its data, and answers requests
// until ctx ends. onListen, when set, receives the bound address; tests bind
// port 0 and need to learn which port that was.
func serveConfig(ctx context.Context, path string, l *zap.Logger, onListen func(net.Addr)) error {
	conf, _, err := config.LoadServeRendered(path, nil)
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

	conn, err := db.Connect(ctx)
	if err != nil {
		return err
	}
	defer func() {
		if err := conn.Close(); err != nil {
			l.Error("failed to close DuckDB connection", zap.Error(err))
		}
	}()

	// Uncoded, as in run: an ATTACH that fails because the database is not up
	// yet exits 1, which a supervisor retries.
	if err := core.InitCommands(conn, &config.Conf{Commands: conf.Commands}); err != nil {
		return fmt.Errorf("failed to initialize commands: %w", err)
	}

	srv, err := api.New(ctx, conf, conn, api.WithLogger(l))
	if err != nil {
		return err
	}
	// Deferred after conn's close, so it runs first: a query still holding
	// the lock finishes before the connection closes under it.
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

	return srv.Serve(ctx, ln)
}
