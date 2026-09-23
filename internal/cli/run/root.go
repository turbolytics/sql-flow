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
	"github.com/turbolytics/sql-flow/turbostats/wire"
	"go.opentelemetry.io/otel/metric"
)

// newErrorPolicies resolves pipeline.on_error, building the DLQ sink when the
// policy calls for one.
func newErrorPolicies(
	ctx context.Context,
	conf *config.Conf,
	conn adbc.Connection,
	lock *sync.Mutex,
	mp metric.MeterProvider,
	events sinks.RetryEvents,
	logger *zap.Logger,
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
			sinks.WithSinkRole("dlq"),
			sinks.WithRetryEvents(events),
			sinks.WithConnLock(lock),
			sinks.WithLogger(logger))
		if err != nil {
			return policies, fmt.Errorf("pipeline.on_error dlq: %w", err)
		}
		policies.DLQSink = dlqSink
	}

	return policies, nil
}

// newPipelineSink builds the pipeline's own sink. A function of its own so the
// shared-connection test builds it exactly as the run command does.
func newPipelineSink(
	ctx context.Context,
	conf *config.Conf,
	conn adbc.Connection,
	lock *sync.Mutex,
	mp metric.MeterProvider,
	events sinks.RetryEvents,
	logger *zap.Logger,
) (core.Sink, error) {
	return sinks.New(ctx, conf.Pipeline.Sink, conn,
		sinks.WithMeterProvider(mp),
		sinks.WithSinkRole(core.SinkRolePipeline),
		sinks.WithRetryEvents(events),
		sinks.WithConnLock(lock),
		sinks.WithLogger(logger))
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
		RunE: func(cmd *cobra.Command, args []string) (runErr error) {
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
			// The command's own context is the parent, not Background. Under
			// `sqlflow run` cobra supplies Background and nothing changes,
			// but a caller that embeds this command -- a test, or another
			// binary -- could not stop it at all, and a run that cannot be
			// stopped is a run a test can only leak.
			parent := cmd.Context()
			if parent == nil {
				parent = context.Background()
			}
			ctx, stopSignals := signal.NotifyContext(
				parent, syscall.SIGINT, syscall.SIGTERM)
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

			// Shared with everything that touches the pipeline's connection:
			// the pipeline itself and the debug API. A window manager has a
			// connection of its own and never takes it.
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
			// The windows' watermarks, for the same reason and at the same
			// point: under autocommit, before the state branch turns it off.
			if err := initWindowStores(context.Background(), conf, conn); err != nil {
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

			// One deadline for the whole shutdown. Its clock starts when the
			// first step of the drain asks for it, and the turbine's final
			// batch, both state syncs and every manager's final poll spend
			// it. A supervisor gives a stop one grace period, not four.
			budget := core.NewDrainBudget(drainDeadlineFor(conf.Pipeline.DrainDeadlineSeconds))
			defer budget.Stop()

			// What /healthz knows that the progress snapshot does not: the
			// failure stopping the process, and the sinks whose retry ladders
			// are running.
			hs := newHealth()
			retryEvents := sinks.RetryEvents{Retry: hs.Retry, Settle: hs.Settle}

			// State wiring. Everything below is skipped for a pipeline with no
			// state path, which then behaves exactly as it did before.
			var (
				turbineOpts = progressOptions(conf, progressStore)
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
				statsFn = func(ctx context.Context) (*core.StateStats, error) {
					// One ADBC connection is not safe for concurrent use, and
					// the status loop and any number of scrapes share this
					// one. The lock guards the reader, never the writer.
					statsMu.Lock()
					defer statsMu.Unlock()
					return core.CollectStateStats(ctx, statsConn, statePath)
				}
				turbineOpts = append(turbineOpts, core.WithStateStats(statsFn))

				l.Info("pipeline state is durable",
					zap.String("path", statePath),
					zap.Int("resuming_partitions", storedMarks.Len()))
			}

			if withHTTPDebug {
				startDebugServer(conn, lock, l)
			}

			// What the bundle says this process is.
			ts := conf.Pipeline.TurboStats
			static := turbostats.Static{
				Name:       conf.Pipeline.Name,
				Version:    buildinfo.Version,
				Commit:     buildinfo.Commit,
				ConfigHash: turbostats.HashConfig(rendered),
				StartedAt:  startedAt,
			}
			if ts.Enabled() {
				static.ID = ts.ID
				// The receiver needs the interval to tell a late heartbeat
				// from a normal one, and only the reporter knows it.
				static.IntervalSeconds = int(ts.Interval().Seconds())
			}

			meterProvider, collectBundle, err := newMeterProvider(metricsExporter, serveTurbostats,
				static, l, statsFn, progressFn, hs.Snapshot, flushInterval)
			if err != nil {
				return err
			}

			// The reporter runs on its own goroutine with its own timeout, so
			// nothing it does can block the consume loop. It is started here
			// rather than later so an instance appears on a fleet page while
			// the pipeline is still connecting to its source.
			var stopReporter func(context.Context, turbostats.Exit)
			if ts.Enabled() {
				// serve refuses an invalid block at startup and run did not,
				// so a config `sqlflow validate` rejects started anyway. The
				// reporter signs every bundle; a plaintext report_to then puts
				// the document and its signature headers across a public
				// network in the clear, which is what reportToProblem exists
				// to refuse.
				if err := ts.CheckError([]string{"pipeline", "turbostats"}); err != nil {
					return err
				}
				key, err := wire.ParseCredential(ts.Key)
				if err != nil {
					return errs.New(errs.CodeConfigInvalid,
						"turbostats.key is not a credential")
				}
				reporter, err := turbostats.NewReporter(turbostats.ReporterConfig{
					ReportTo: ts.ReportTo, Key: key, Interval: ts.Interval(),
					Collect: collectBundle, Log: l.Named("turbostats"),
				})
				if err != nil {
					return err
				}
				// The command's context, not the pipeline's: a run that
				// failed is exactly when someone wants telemetry, and the
				// reporter should keep going until the process exits.
				stopReporter = turbostats.StartReporter(ctx, reporter)

				// Registered here rather than with the drain below, for two
				// reasons.
				//
				// Everything between this line and the drain can fail -- a
				// broker that will not connect, a dimension table that will
				// not load -- and those returns never reach the drain's
				// defer. The instance would appear on a fleet page and then
				// go silent, which this design defines as a crash.
				//
				// And this runs after the drain's defer but before the state
				// reader connection closes, so the reporter is stopped and
				// waited for while the connection it collects from is still
				// open. Left running, a collect can be mid-query on statsConn
				// while the deferred Close runs, which is a use-after-free
				// inside DuckDB rather than anything the race detector sees.
				// The status loop below already takes this shape.
				//
				// On an ordinary stop the drain has already sent the bundle,
				// and stopReporter sends one bundle however often it is
				// called, so this is then only the wait.
				defer func() {
					final, cancel := context.WithTimeout(
						context.WithoutCancel(ctx), reporterGrace)
					defer cancel()
					stopReporter(final, turbostats.Exit{
						Reason: turbostats.ExitReason(runErr, ctx.Err()),
						Code:   errs.ExitCode(runErr),
					})
				}()
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
			// A source whose partitions can move tells the lag table which it
			// holds, so a partition taken away in a rebalance stops being
			// reported here while another instance reports it too.
			if owner, ok := src.(core.PartitionOwner); ok {
				owner.OnPartitions(pipelineMetrics.Lag.Assigned,
					pipelineMetrics.Lag.Released, pipelineMetrics.Lag.Lost)
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
			sink, err := newPipelineSink(ctx, conf, conn, lock, meterProvider, retryEvents, logger)
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

			errorPolicies, err := newErrorPolicies(ctx, conf, conn, lock, meterProvider, retryEvents, logger)
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
					core.WithDrainBudget(budget),
				}, turbineOpts...)...,
			)
			liveTurbine.Store(turbine)

			managedTables, closeWindowConns, err := buildManagedTables(ctx, conf, db, l,
				meterProvider, budget, retryEvents)
			if err != nil {
				return err
			}
			// Registered before the managers' own deferred block, so it runs
			// after the final polls have returned: a connection closed under
			// a poll in flight is a use-after-free inside DuckDB.
			defer closeWindowConns()

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
			var group managerGroup
			for _, m := range managedTables {
				group.start(managerCtx, m, func(err error) {
					l.Error("table manager stopped", zap.Error(err))
					hs.Fail(err)
					failRun(err)
				})
			}
			defer func() {
				// Every step below spends the drain budget. On a stop that
				// was not a signal, --max-msgs or a closed source, this starts
				// the clock, and the steps finish in milliseconds.
				drainCtx := budget.Context()
				// Commit first. The managers' final poll reads the progress
				// row from a connection of its own, so it sees only what has
				// been committed, and it closes on idleness only for the quiet
				// that row confirms. This forces the write, so the poll sees
				// the quiet up to the signal rather than up to the last
				// interval write.
				if err := turbine.SyncState(drainCtx); err != nil {
					l.Error("failed to sync state before final poll", zap.Error(err))
				}
				stopManagers()
				managerErr := group.wait()
				// And again afterwards, so the batch transaction closes
				// with everything the drain wrote before the connection
				// does. The managers' final poll committed on connections
				// of its own; this is the engine's.
				if err := turbine.SyncState(drainCtx); err != nil {
					l.Error("failed to sync state after final poll", zap.Error(err))
				}
				// A manager whose final poll failed is a stop that did not
				// finish, whatever the loop reported: its windows are still
				// in the table, and the next start publishes them. The
				// manager says why, and a poll the drain deadline ended
				// carries the drain code. A failure during the run reached
				// here through failRun already, and the loop's own error
				// outranks this one.
				if managerErr != nil && runErr == nil {
					runErr = managerErr
				}

				// Last, after the managers stop and the final sync, so the
				// bundle reports state that is actually committed. It runs on
				// the drain budget, so a receiver that hangs cannot hold a
				// shutdown past the deadline a supervisor is waiting on.
				//
				// A process that crashes never reaches this line, which is
				// the whole signal: the receiver tells a clean stop from a
				// crash by whether this bundle arrived.
				if stopReporter != nil {
					stopReporter(drainCtx, turbostats.Exit{
						Reason: turbostats.ExitReason(runErr, ctx.Err()),
						Code:   errs.ExitCode(runErr),
					})
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
			// A manager that failed cancelled the run, and its error outranks
			// whatever the loop returned: the loop was stopped on purpose,
			// and the manager's error carries the code a supervisor reads.
			//
			// Asked of the group, not inferred from runCtx's cause. Go 1.26
			// made signal.NotifyContext cancel with a cause naming the signal
			// ("terminated signal received"), and that cause propagates into
			// runCtx. The old test -- a cause that is not context.Canceled --
			// then matched every SIGTERM, so a clean drain was reported as a
			// failed manager and exited 1, the unclassified internal code a
			// supervisor retries forever.
			if cause := group.err(); cause != nil {
				l.Error("table manager failed, pipeline stopped", zap.Error(cause))
				return cause
			}
			if err != nil {
				hs.Fail(err)
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

// reporterGrace bounds the final bundle when the drain did not send one,
// which happens only when a startup step failed before the drain existed.
const reporterGrace = 15 * time.Second
