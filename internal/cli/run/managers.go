package run

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/core"
	"github.com/turbolytics/sql-flow/internal/duckdb"
	"github.com/turbolytics/sql-flow/internal/errs"
	"github.com/turbolytics/sql-flow/internal/managers"
	"github.com/turbolytics/sql-flow/internal/sinks"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
)

// windowDeclaration turns a config block into what the manager is built
// from.
func windowDeclaration(table config.TableSQL) managers.Declaration {
	w := table.Window
	return managers.Declaration{
		Table:      table.Name,
		TimeColumn: w.TimeColumn,
		Size:       time.Duration(w.SizeSeconds) * time.Second,
		Grace:      time.Duration(w.GraceSeconds) * time.Second,
		IdleClose:  time.Duration(w.IdleCloseSeconds) * time.Second,
		Late:       managers.LatePolicy(w.LateRows),
		EmitSQL:    w.EmitSQL,
	}
}

// initWindowStores creates sqlflow_windows on the pipeline's connection,
// under autocommit, so the DDL commits on its own the way the offsets and
// progress tables' does. Nothing to do for a pipeline with no window.
func initWindowStores(ctx context.Context, conf *config.Conf, conn adbc.Connection) error {
	if conf.Tables == nil {
		return nil
	}
	for _, table := range conf.Tables.SQL {
		if table.Window != nil {
			return managers.NewStore(conn).Init(ctx)
		}
	}
	return nil
}

// buildManagedTables constructs a watermark manager per table that declares
// a window. Each manager gets two connections of its own to the pipeline's
// DuckDB: one with autocommit off for the close, so it reads committed rows
// only and its delete commits with its watermark, and one for its sink,
// because a transaction may write to one database only and a sink stages a
// batch table or writes into an attached database. The returned close func
// closes those connections; call it after the managers have returned.
func buildManagedTables(
	ctx context.Context,
	conf *config.Conf,
	db *duckdb.DB,
	l *zap.Logger,
	mp metric.MeterProvider,
	budget *core.DrainBudget,
	events sinks.RetryEvents,
) (built []*managers.Watermark, closeConns func(), err error) {
	var conns []adbc.Connection
	closeConns = func() {
		for _, c := range conns {
			if err := c.Close(); err != nil {
				l.Error("failed to close window connection", zap.Error(err))
			}
		}
	}
	defer func() {
		if err != nil {
			closeConns()
		}
	}()

	if conf.Tables == nil {
		return nil, closeConns, nil
	}

	for _, table := range conf.Tables.SQL {
		if table.Window == nil {
			continue
		}

		conn, err := db.Connect(ctx)
		if err != nil {
			return nil, closeConns, errs.Wrap(errs.CodeStateInternal, err,
				"table %q: opening the window's connection", table.Name)
		}
		conns = append(conns, conn)
		po, ok := conn.(adbc.PostInitOptions)
		if !ok {
			return nil, closeConns, errs.New(errs.CodeStateInternal,
				"table %q: the window's connection does not support transactions", table.Name)
		}
		if err := po.SetOption(adbc.OptionKeyAutoCommit, adbc.OptionValueDisabled); err != nil {
			return nil, closeConns, errs.Wrap(errs.CodeStateInternal, err,
				"table %q: disabling autocommit on the window's connection", table.Name)
		}

		// A windowed pipeline's entire output comes through this sink, so
		// its counters are what sink_rows_written reports for it.
		sinkConn, err := db.Connect(ctx)
		if err != nil {
			return nil, closeConns, errs.Wrap(errs.CodeStateInternal, err,
				"table %q: opening the window sink's connection", table.Name)
		}
		conns = append(conns, sinkConn)
		// The sink's connection is the window's alone, and the manager runs
		// the sink from one goroutine, so the lock a sqlcommand sink needs
		// has no other party. It is a lock of its own rather than the
		// pipeline's: taking the pipeline's here would stall the consume
		// loop for the length of every window write.
		sink, err := sinks.New(ctx, table.Window.Sink, sinkConn,
			sinks.WithMeterProvider(mp),
			sinks.WithSinkRole("manager"),
			sinks.WithRetryEvents(events),
			sinks.WithConnLock(&sync.Mutex{}),
			sinks.WithLogger(l))
		if err != nil {
			return nil, closeConns, fmt.Errorf("table %q window sink: %w", table.Name, err)
		}

		m, err := managers.NewWatermark(conn, windowDeclaration(table),
			time.Duration(table.Window.PollIntervalSecs)*time.Second, sink,
			managers.WithLogger(l),
			managers.WithDrainBudget(budget),
			managers.WithMeterProvider(mp))
		if err != nil {
			return nil, closeConns, fmt.Errorf("table %q window: %w", table.Name, err)
		}
		built = append(built, m)
	}

	return built, closeConns, nil
}

// managerGroup runs every table manager and keeps the first error any of
// them returned. run reads it after the shutdown, because a manager that
// fails while the loop is running cancels the run itself, and one that fails
// in its final poll has nobody left to tell: the loop has already returned.
// Before this, that second failure was logged and the process exited 0 with
// windows unpublished.
type managerGroup struct {
	wg    sync.WaitGroup
	mu    sync.Mutex
	first error
}

// start runs m until ctx ends. onFail is called with any error m returns,
// so the caller can cancel the run and mark the process failed.
func (g *managerGroup) start(ctx context.Context, m *managers.Watermark, onFail func(error)) {
	g.wg.Add(1)
	go func() {
		defer g.wg.Done()
		err := m.Start(ctx)
		if err == nil {
			return
		}
		g.mu.Lock()
		if g.first == nil {
			g.first = err
		}
		g.mu.Unlock()
		onFail(err)
	}()
}

// wait blocks until every manager has returned and reports the first error.
func (g *managerGroup) wait() error {
	g.wg.Wait()
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.first
}
