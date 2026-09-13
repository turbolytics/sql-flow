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
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/turbolytics/sql-flow/internal/buildinfo"
	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/core"
	"github.com/turbolytics/sql-flow/internal/errs"
	"github.com/turbolytics/sql-flow/internal/turbostats"
	"go.opentelemetry.io/otel/metric"
)

// newErrorPolicies resolves pipeline.on_error, building the DLQ sink when the
// policy calls for one.
func newErrorPolicies(
	ctx context.Context,
	conf *config.Conf,
	conn adbc.Connection,
	mp metric.MeterProvider,
) (core.PipelineErrorPolicies, error) {
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
		// The DLQ carries its own role so its rows never sum into the
		// pipeline's delivered series. Without that, a pipeline looks
		// healthier the more records it rejects.
		dlqSink, err := sinks.New(ctx, *onError.DLQ, conn,
			sinks.WithMeterProvider(mp),
			sinks.WithSinkRole("dlq"))
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
	var serveTurbostats bool

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

			// Taken here, once. It is the TurboStats bundle's started_at, and
			// a restart is the control plane seeing that value change.
			startedAt := time.Now().UTC()

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

			conf, rendered, err := config.LoadRendered(configPath, map[string]string{})
			if err != nil {
				// Returned as-is: the error already names the file, the stage and the
				// code, so another prefix adds a word and no information.
				return err
			}

			// A pipeline that declares a state path gets a DuckDB backed by
			// that file, so window state and the offsets that produced it
			// survive a crash. Without one, state is in memory and is lost --
			// while its offsets have already been committed.
			statePath := ""
			if conf.Pipeline.State != nil {
				statePath = conf.Pipeline.State.Path
			}

			// The handle is kept, not just a connection: a second connection
			// is what lets /stats read committed state without contending
			// with the writer.
			//
			// A declared state path goes through OpenState, which separates a
			// first run from a damaged file. An empty path is in-memory and
			// has nothing to recover, so it opens directly.
			var db *duckdb.DB
			if statePath != "" {
				db, err = core.OpenState(context.Background(), statePath)
			} else {
				db, err = duckdb.OpenPath(context.Background(), "")
			}
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

			// Liveness, for every pipeline whether or not it has a state path.
			// Created before the state branch turns autocommit off, so the
			// CREATE TABLE commits on its own the way the offsets table's
			// does. With a state path its updates then ride each batch's
			// transaction; without one they autocommit.
			progressStore := core.NewProgressStore(conn)
			if err := progressStore.Init(context.Background()); err != nil {
				return err
			}

			// The metrics mux is built before the turbine exists, so the
			// health endpoint reads through a pointer the turbine fills in
			// below. Until then a zero snapshot means "no commit yet", which
			// /healthz measures from when the server started.
			var liveTurbine atomic.Pointer[core.Turbine]
			progressFn := func() core.Progress {
				if tb := liveTurbine.Load(); tb != nil {
					return tb.Progress()
				}
				return core.Progress{}
			}

			flushInterval := flushIntervalFor(conf.Pipeline.FlushIntervalSeconds)

			// State wiring. Everything below is skipped for a pipeline with no
			// state path, which then behaves exactly as it did before.
			var (
				turbineOpts = []core.TurbineOption{core.WithProgressStore(progressStore)}
				statsFn     statsFunc
				storedMarks *core.Marks
			)
			if statePath != "" {
				// Both calls are returned as-is. They already carry a code, and
				// the outermost code wins: re-wrapping either as
				// CodeStateInternal would turn a corrupt state file into a
				// retryable failure and hand a supervisor a crash loop.
				offsets := core.NewOffsetStore(conn)
				if err := offsets.Init(context.Background()); err != nil {
					return err
				}

				// Read the durable positions before autocommit is turned off,
				// so setup is committed and the read is straightforward.
				storedMarks, err = offsets.Load(context.Background())
				if err != nil {
					return err
				}

				// Every batch from here on is one transaction: the handler's
				// writes and the offsets that produced them commit together.
				po, ok := conn.(adbc.PostInitOptions)
				if !ok {
					return errs.New(errs.CodeStateInternal, "state requires a connection supporting transactions")
				}
				if err := po.SetOption(adbc.OptionKeyAutoCommit, adbc.OptionValueDisabled); err != nil {
					return errs.Wrap(errs.CodeStateInternal, err, "disabling autocommit for state")
				}

				stateConn, ok := conn.(interface {
					Commit(context.Context) error
					Rollback(context.Context) error
				})
				if !ok {
					return errs.New(errs.CodeStateInternal, "state requires a connection supporting commit and rollback")
				}
				turbineOpts = append(turbineOpts, core.WithStateStore(offsets, stateConn))

				// A connection of its own for reading: it sees committed state
				// only, so a scrape never blocks a batch and never reports
				// rows a rollback then erased.
				statsConn, err := db.Connect(context.Background())
				if err != nil {
					return errs.Wrap(errs.CodeStateInternal, err, "opening state reader connection")
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

			// What the bundle says this process is. ID stays empty until the
			// pipeline.turbostats config block lands with the reporter.
			static := turbostats.Static{
				Pipeline:   conf.Pipeline.Name,
				Version:    buildinfo.Version,
				Commit:     buildinfo.Commit,
				ConfigHash: turbostats.HashConfig(rendered),
				StartedAt:  startedAt,
			}

			meterProvider, err := newMeterProvider(metricsExporter, serveTurbostats, static, l,
				statsFn, progressFn, nil, flushInterval)
			if err != nil {
				return err
			}
			pipelineMetrics, err := core.NewMetrics(meterProvider)
			if err != nil {
				return fmt.Errorf("failed to create metrics: %w", err)
			}

			// A dimension table that did not load is the enrichment failure
			// nothing else makes visible. Reported before the pipeline
			// consumes anything, and on the signal context so a SIGTERM during
			// startup stops the count rather than waiting it out.
			//
			// It runs here rather than beside InitTables because it records a
			// gauge, and the metrics do not exist until now.
			if err := core.CheckReferenceTables(ctx, conn, conf, pipelineMetrics, l); err != nil {
				return err
			}

			src, err := sources.New(
				conf.Pipeline.Source,
				logger,
				meterProvider,
			)
			if err != nil {
				// Returned as-is: the code already names the subsystem.
				return err
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

			// The signal context, so a SIGTERM arriving while the sink dials
			// its destination stops the start instead of waiting it out.
			sink, err := sinks.New(ctx, conf.Pipeline.Sink, conn,
				sinks.WithMeterProvider(meterProvider),
				sinks.WithSinkRole(core.SinkRolePipeline))
			if err != nil {
				return err
			}

			handler, err := handlers.New(
				conn,
				conf.Pipeline.Handler,
				logger,
			)
			if err != nil {
				return err
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

			errorPolicies, err := newErrorPolicies(ctx, conf, conn, meterProvider)
			if err != nil {
				return err
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
			liveTurbine.Store(turbine)

			managedTables, err := buildManagedTables(ctx, conf, conn, lock, l, meterProvider)
			if err != nil {
				return err
			}

			// Managers run for the lifetime of the pipeline. Cancelling their
			// context makes each publish one final time before returning, so
			// windows that close during shutdown are not stranded.
			//
			// A manager that stops is a pipeline that has stopped publishing,
			// whatever the consume loop is still doing: a windowed pipeline's
			// entire output goes through its manager. So a manager failure
			// cancels the run, the loop drains as it would on SIGTERM, and the
			// manager's error is what the process exits with. Before #267 the
			// failure was logged and the loop kept consuming into a table
			// nothing would ever publish.
			runCtx, failRun := context.WithCancelCause(ctx)
			defer failRun(nil)
			managerCtx, stopManagers := context.WithCancel(context.Background())
			var managerWG sync.WaitGroup
			for _, m := range managedTables {
				managerWG.Add(1)
				go func(m *managers.Tumbling) {
					defer managerWG.Done()
					if err := m.Start(managerCtx); err != nil {
						l.Error("table manager stopped", zap.Error(err))
						failRun(err)
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

			stats, err := turbine.ConsumeLoop(runCtx, maxMsgs)
			// Restore default signal handling for the rest of the shutdown.
			// The deferred drain below still has to run. Leaving the handler
			// installed would swallow a second SIGTERM, so an operator could
			// not interrupt a drain that hangs.
			stopSignals()
			// A cause other than plain cancellation is a manager's error. It
			// outranks whatever the loop returned: the loop was stopped on
			// purpose, and the manager's error carries the code a supervisor
			// reads.
			if cause := context.Cause(runCtx); cause != nil && cause != context.Canceled {
				l.Error("table manager failed, pipeline stopped", zap.Error(cause))
				return cause
			}
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
	cmd.Flags().BoolVar(&serveTurbostats, "turbostats", false,
		"Serve GET /turbostats/v1 on "+metricsPort+": the process's own state as one document")

	return cmd
}
