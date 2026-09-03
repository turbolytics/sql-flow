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
	"os/signal"
	"path/filepath"
	"runtime"
	"sync"
	"syscall"
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

	var maxMsgsToProcess int

	var cmd = &cobra.Command{
		Use:   "run [config]",
		Short: "Run sqlflow against a stream of data",
		// Zero args for the -c form, one for the Python engine's positional
		// form. See resolveConfigPath.
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			logger, levelErr := logging.New()
			defer logger.Sync()
			l := logger.Named("sqlflow.run")
			if levelErr != nil {
				return levelErr
			}

			configPath, err := resolveConfigPath(configPath, args)
			if err != nil {
				cmd.SilenceUsage = false
				return err
			}

			maxMsgs, err := resolveMaxMsgs(maxMsgs, maxMsgsToProcess)
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

			// A supervisor stops a pipeline with SIGTERM. Go's default handler
			// terminates the process without running any deferred function.
			// Three deferred steps below would therefore never run: the final
			// batch, the managers' last poll, and the state commit that makes
			// their deletes durable.
			ctx, stopSignals := signal.NotifyContext(
				context.Background(), syscall.SIGINT, syscall.SIGTERM)
			defer stopSignals()

			conf, err := config.Load(configPath, map[string]string{})
			if err != nil {
				return fmt.Errorf("failed to load config: %w", err)
			}

			// A pipeline that declares a state path gets a DuckDB backed by
			// that file, so window state and the offsets that produced it
			// survive a crash. Without one, state is in memory and is lost --
			// while its offsets have already been committed.
			statePath := ""
			if conf.Pipeline.State != nil {
				statePath = conf.Pipeline.State.Path
			}
			if statePath != "" {
				if err := os.MkdirAll(filepath.Dir(statePath), 0o755); err != nil {
					return fmt.Errorf("creating state directory for %q: %w", statePath, err)
				}
			}

			// The handle is kept, not just a connection: a second connection
			// is what lets /stats read committed state without contending
			// with the writer.
			db, err := duckdb.OpenPath(context.Background(), statePath)
			if err != nil {
				return err
			}
			defer func() {
				if err := db.Close(); err != nil {
					l.Error("failed to close DuckDB database", zap.Error(err))
				}
			}()

			conn, err := db.Connect(context.Background())
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

			// State wiring. Everything below is skipped for a pipeline with no
			// state path, which then behaves exactly as it did before.
			var (
				turbineOpts []core.TurbineOption
				statsFn     statsFunc
				storedMarks *core.Marks
			)
			if statePath != "" {
				offsets := core.NewOffsetStore(conn)
				if err := offsets.Init(context.Background()); err != nil {
					return fmt.Errorf("initializing offsets table: %w", err)
				}

				// Read the durable positions before autocommit is turned off,
				// so setup is committed and the read is straightforward.
				storedMarks, err = offsets.Load(context.Background())
				if err != nil {
					return fmt.Errorf("loading stored offsets: %w", err)
				}

				// Every batch from here on is one transaction: the handler's
				// writes and the offsets that produced them commit together.
				po, ok := conn.(adbc.PostInitOptions)
				if !ok {
					return fmt.Errorf("state requires a connection supporting transactions")
				}
				if err := po.SetOption(adbc.OptionKeyAutoCommit, adbc.OptionValueDisabled); err != nil {
					return fmt.Errorf("disabling autocommit for state: %w", err)
				}

				stateConn, ok := conn.(interface {
					Commit(context.Context) error
					Rollback(context.Context) error
				})
				if !ok {
					return fmt.Errorf("state requires a connection supporting commit and rollback")
				}
				turbineOpts = append(turbineOpts, core.WithStateStore(offsets, stateConn))

				// A connection of its own for reading: it sees committed state
				// only, so a scrape never blocks a batch and never reports
				// rows a rollback then erased.
				statsConn, err := db.Connect(context.Background())
				if err != nil {
					return fmt.Errorf("opening state reader connection: %w", err)
				}
				defer func() {
					if err := statsConn.Close(); err != nil {
						l.Error("failed to close state reader connection", zap.Error(err))
					}
				}()

				var statsMu sync.Mutex
				statsFn = func() (*core.StateStats, error) {
					// One ADBC connection is not safe for concurrent use, and
					// the status loop and any number of scrapes share this
					// one. The lock guards the reader, never the writer.
					statsMu.Lock()
					defer statsMu.Unlock()
					return core.CollectStateStats(context.Background(), statsConn, statePath)
				}
				turbineOpts = append(turbineOpts, core.WithStateStats(statsFn))

				l.Info("pipeline state is durable",
					zap.String("path", statePath),
					zap.Int("resuming_partitions", storedMarks.Len()))
			}

			if withHTTPDebug {
				startDebugServer(conn, lock, l)
			}

			meterProvider, err := newMeterProvider(metricsExporter, l, statsFn)
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

			// Resume where the durable state left off. The state database is
			// the source of truth; a disagreement with Kafka is resolved in
			// its favour, immediately.
			if storedMarks != nil && !storedMarks.Empty() {
				seeker, ok := src.(interface{ SeekTo(*core.Marks) error })
				if !ok {
					return fmt.Errorf("source %q cannot resume from stored offsets", conf.Pipeline.Source.Type)
				}
				if err := seeker.SeekTo(storedMarks); err != nil {
					return fmt.Errorf("resuming from stored offsets: %w", err)
				}
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
				append([]core.TurbineOption{
					core.WithTurbineLogger(l),
					core.WithMetrics(pipelineMetrics),
				}, turbineOpts...)...,
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
				// Close the open transaction first. The managers' final poll
				// runs on this connection, and its close predicate is
				// evaluated against the transaction's clock -- which is
				// frozen at the last commit until this runs.
				if err := turbine.SyncState(context.Background()); err != nil {
					l.Error("failed to sync state before final poll", zap.Error(err))
				}
				stopManagers()
				managerWG.Wait()
				// And again afterwards, so what that poll published is
				// actually deleted. Without this the delete is rolled back
				// when the connection closes, and every clean shutdown
				// guarantees a republished window on the next start.
				if err := turbine.SyncState(context.Background()); err != nil {
					l.Error("failed to sync state after final poll", zap.Error(err))
				}
			}()

			// Cancelled before the reader connection closes. Left running, a
			// gauge sample can be mid-query on statsConn while the deferred
			// Close runs, which is a use-after-free inside DuckDB rather than
			// anything the race detector can see.
			statusCtx, stopStatus := context.WithCancel(context.Background())
			var statusWG sync.WaitGroup
			statusWG.Add(1)
			go func() {
				defer statusWG.Done()
				if err := turbine.StatusLoop(statusCtx); err != nil {
					l.Error("failed to start status loop", zap.Error(err))
				}
			}()
			defer func() {
				stopStatus()
				statusWG.Wait()
			}()

			stats, err := turbine.ConsumeLoop(ctx, maxMsgs)
			// Restore default signal handling for the rest of the shutdown.
			// The deferred drain below still has to run. Leaving the handler
			// installed would swallow a second SIGTERM, so an operator could
			// not interrupt a drain that hangs.
			stopSignals()
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

	// Deliberately not MarkFlagRequired: the config may instead arrive as the
	// positional argument the Python engine takes.
	cmd.Flags().StringVarP(&configPath, "config", "c", "", "Path to sqlflow config file (or pass it positionally)")
	cmd.Flags().IntVar(&maxMsgs, "max-msgs", 0, "Maximum number of messages to consume (0 = unlimited)")
	cmd.Flags().IntVar(&maxMsgsToProcess, "max-msgs-to-process", 0, "Alias for --max-msgs, as spelled by the Python engine")
	cmd.Flags().BoolVar(&enablePprof, "pprof", false, "Enable pprof profiling server on :6060")
	cmd.Flags().StringVar(&statsJSONPath, "stats-json", "", "Write final run stats as JSON to this path")
	cmd.Flags().StringVar(&metricsExporter, "metrics", "", "Metrics exporter to enable (prometheus); serves /metrics on :8000")
	cmd.Flags().BoolVar(&withHTTPDebug, "with-http-debug", false, "Serve GET /debug?sql=... against the live DuckDB connection on "+debugAddr)

	return cmd
}
