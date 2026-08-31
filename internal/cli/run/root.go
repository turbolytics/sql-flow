package run

import (
	"context"
	"fmt"
	"github.com/apache/arrow-adbc/go/adbc/drivermgr" // Import the driver manager
	"github.com/spf13/cobra"
	"github.com/turbolytics/turbine/internal/handlers"
	"github.com/turbolytics/turbine/internal/sinks"
	"github.com/turbolytics/turbine/internal/sources"
	"go.uber.org/zap"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/turbolytics/turbine/internal/config"
	"github.com/turbolytics/turbine/internal/core"
)

func NewCommand() *cobra.Command {
	var configPath string
	var maxMsgs int
	var enablePprof bool

	var cmd = &cobra.Command{
		Use:   "run",
		Short: "Run turbine against a stream of data",
		RunE: func(cmd *cobra.Command, args []string) error {
			logger, _ := zap.NewDevelopment()
			defer logger.Sync()
			l := logger.Named("turbine.run")

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

			conf, err := config.Load(configPath, map[string]string{})
			if err != nil {
				return fmt.Errorf("failed to load config: %w", err)
			}

			// Initialize ADBC connection using driver manager
			duckdbLib := os.Getenv("SQLFLOW_DUCKDB_LIB")
			if duckdbLib == "" {
				duckdbLib = "/opt/homebrew/lib/libduckdb.dylib"
			}
			var drv drivermgr.Driver
			db, err := drv.NewDatabase(map[string]string{
				"driver":     duckdbLib,
				"entrypoint": "duckdb_adbc_init",
			})
			if err != nil {
				return fmt.Errorf("failed to initialize DuckDB driver: %w", err)
			}

			conn, err := db.Open(context.Background())
			if err != nil {
				return fmt.Errorf("failed to open DuckDB connection: %w", err)
			}
			defer func() {
				if err := conn.Close(); err != nil {
					l.Error("failed to close DuckDB connection", zap.Error(err))
				}
			}()

			// Initialize commands
			if err := core.InitCommands(conn, conf); err != nil {
				return fmt.Errorf("failed to initialize commands: %w", err)
			}

			src, err := sources.New(
				conf.Pipeline.Source,
				logger,
			)
			if err != nil {
				return fmt.Errorf("failed to create source: %w", err)
			}

			sink, err := sinks.New(conf.Pipeline.Sink)
			if err != nil {
				return fmt.Errorf("failed to create sink: %w", err)
			}

			handler, err := handlers.New(
				conn,
				conf.Pipeline.Handler,
				logger,
			)
			if err != nil {
				return fmt.Errorf("failed to create handler: %w", err)
			}

			lock := &sync.Mutex{}
			turbine := core.NewTurbine(
				src,
				handler,
				sink,
				conf.Pipeline.BatchSize,
				time.Duration(conf.Pipeline.FlushIntervalSeconds)*time.Second,
				lock,
				core.PipelineErrorPolicies{
					// Source: conf.Pipeline.Source.Error.Policy,
				},
				core.WithTurbineLogger(l),
			)

			go func() {
				if err := turbine.StatusLoop(context.Background()); err != nil {
					l.Error("failed to start status loop", zap.Error(err))
				}
			}()

			_, err = turbine.ConsumeLoop(context.Background(), maxMsgs)
			if err != nil {
				l.Error("failed to consume loop", zap.Error(err))
				return err
			}
			return nil
		},
	}

	cmd.Flags().StringVarP(&configPath, "config", "c", "", "Path to turbine config file (required)")
	cmd.MarkFlagRequired("config")
	cmd.Flags().IntVar(&maxMsgs, "max-msgs", 0, "Maximum number of messages to consume (0 = unlimited)")
	cmd.Flags().BoolVar(&enablePprof, "pprof", false, "Enable pprof profiling server on :6060")

	return cmd
}
