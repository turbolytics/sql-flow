package run

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/spf13/cobra"
	"github.com/turbolytics/sql-flow/internal/duckdb"
	"github.com/turbolytics/sql-flow/internal/handlers"
	"github.com/turbolytics/sql-flow/internal/logging"
	"github.com/turbolytics/sql-flow/internal/managers"
	"github.com/turbolytics/sql-flow/internal/sinks"
	"github.com/turbolytics/sql-flow/internal/sources"
	"go.uber.org/zap"
	"io"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/core"
)

// newErrorPolicies resolves pipeline.on_error, building the DLQ sink when the
// policy calls for one.
func newErrorPolicies(conf *config.Conf, conn adbc.Connection) (core.PipelineErrorPolicies, error) {
	var policies core.PipelineErrorPolicies

	onError := conf.Pipeline.OnError
	if onError == nil {
		return policies, nil
	}

	policy, err := core.ParseErrorPolicy(onError.Policy)
	if err != nil {
		return policies, err
	}
	policies.Policy = policy

	if policy == core.PolicyDLQ {
		if onError.DLQ == nil {
			return policies, fmt.Errorf("pipeline.on_error: policy DLQ requires a dlq sink")
		}
		dlqSink, err := sinks.New(*onError.DLQ, conn)
		if err != nil {
			return policies, fmt.Errorf("pipeline.on_error dlq: %w", err)
		}
		policies.DLQSink = dlqSink
	}

	return policies, nil
}

func NewCommand() *cobra.Command {
	var configPath string
	var maxMsgs int
	var enablePprof bool
	var statsJSONPath string
	var metricsExporter string
	var withHTTPDebug bool

	var cmd = &cobra.Command{
		Use:   "run",
		Short: "Run sqlflow against a stream of data",
		RunE: func(cmd *cobra.Command, args []string) error {
			logger, levelErr := logging.New()
			defer logger.Sync()
			l := logger.Named("sqlflow.run")
			if levelErr != nil {
				return levelErr
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

			conf, err := config.Load(configPath, map[string]string{})
			if err != nil {
				return fmt.Errorf("failed to load config: %w", err)
			}

			// Initialize ADBC connection using driver manager
			conn, err := duckdb.Open(context.Background())
			if err != nil {
				return err
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

			if err := core.InitTables(conn, conf); err != nil {
				return fmt.Errorf("failed to initialize tables: %w", err)
			}

			if err := core.InitUDFs(conf); err != nil {
				return err
			}

			// Shared with everything that touches the connection: the pipeline,
			// the table managers and the debug API.
			lock := &sync.Mutex{}

			if withHTTPDebug {
				startDebugServer(conn, lock, l)
			}

			meterProvider, err := newMeterProvider(metricsExporter, l)
			if err != nil {
				return err
			}
			pipelineMetrics, err := core.NewMetrics(meterProvider)
			if err != nil {
				return fmt.Errorf("failed to create metrics: %w", err)
			}

			src, err := sources.New(
				conf.Pipeline.Source,
				logger,
				meterProvider,
			)
			if err != nil {
				return fmt.Errorf("failed to create source: %w", err)
			}

			sink, err := sinks.New(conf.Pipeline.Sink, conn)
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
			// Disk-backed handlers stage batch files under the results cache
			// dir and only remove them on close.
			if closer, ok := handler.(io.Closer); ok {
				defer func() {
					if err := closer.Close(); err != nil {
						l.Error("failed to close handler", zap.Error(err))
					}
				}()
			}

			errorPolicies, err := newErrorPolicies(conf, conn)
			if err != nil {
				return err
			}

			// Matches the Python engine's default when the key is absent.
			flushInterval := 30 * time.Second
			if conf.Pipeline.FlushIntervalSeconds > 0 {
				flushInterval = time.Duration(conf.Pipeline.FlushIntervalSeconds) * time.Second
			}

			turbine := core.NewTurbine(
				src,
				handler,
				sink,
				conf.Pipeline.BatchSize,
				flushInterval,
				lock,
				errorPolicies,
				core.WithTurbineLogger(l),
				core.WithMetrics(pipelineMetrics),
			)

			managedTables, err := buildManagedTables(conf, conn, lock, l)
			if err != nil {
				return err
			}

			// Managers run for the lifetime of the pipeline. Cancelling their
			// context makes each publish one final time before returning, so
			// windows that close during shutdown are not stranded.
			managerCtx, stopManagers := context.WithCancel(context.Background())
			var managerWG sync.WaitGroup
			for _, m := range managedTables {
				managerWG.Add(1)
				go func(m *managers.Tumbling) {
					defer managerWG.Done()
					if err := m.Start(managerCtx); err != nil {
						l.Error("table manager stopped", zap.Error(err))
					}
				}(m)
			}
			defer func() {
				stopManagers()
				managerWG.Wait()
			}()

			go func() {
				if err := turbine.StatusLoop(context.Background()); err != nil {
					l.Error("failed to start status loop", zap.Error(err))
				}
			}()

			stats, err := turbine.ConsumeLoop(context.Background(), maxMsgs)
			if err != nil {
				l.Error("failed to consume loop", zap.Error(err))
				return err
			}

			if statsJSONPath != "" {
				out, err := json.Marshal(map[string]int64{
					"messages_consumed": stats.MessagesConsumed(),
					"num_errors":        int64(stats.NumErrors),
				})
				if err != nil {
					return fmt.Errorf("failed to marshal stats: %w", err)
				}
				if err := os.WriteFile(statsJSONPath, out, 0o644); err != nil {
					return fmt.Errorf("failed to write stats file: %w", err)
				}
			}
			return nil
		},
	}

	cmd.Flags().StringVarP(&configPath, "config", "c", "", "Path to sqlflow config file (required)")
	cmd.MarkFlagRequired("config")
	cmd.Flags().IntVar(&maxMsgs, "max-msgs", 0, "Maximum number of messages to consume (0 = unlimited)")
	cmd.Flags().BoolVar(&enablePprof, "pprof", false, "Enable pprof profiling server on :6060")
	cmd.Flags().StringVar(&statsJSONPath, "stats-json", "", "Write final run stats as JSON to this path")
	cmd.Flags().StringVar(&metricsExporter, "metrics", "", "Metrics exporter to enable (prometheus); serves /metrics on :8000")
	cmd.Flags().BoolVar(&withHTTPDebug, "with-http-debug", false, "Serve GET /debug?sql=... against the live DuckDB connection on "+debugAddr)

	return cmd
}
