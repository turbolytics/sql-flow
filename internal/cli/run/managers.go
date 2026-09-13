package run

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/core"
	"github.com/turbolytics/sql-flow/internal/managers"
	"github.com/turbolytics/sql-flow/internal/sinks"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
)

// buildManagedTables constructs a manager per table that declares one. Each
// manager gets its own sink and shares the pipeline's DuckDB lock.
func buildManagedTables(
	ctx context.Context,
	conf *config.Conf,
	conn adbc.Connection,
	lock *sync.Mutex,
	l *zap.Logger,
	mp metric.MeterProvider,
	budget *core.DrainBudget,
	events sinks.RetryEvents,
) ([]*managers.Tumbling, error) {
	if conf.Tables == nil {
		return nil, nil
	}

	var built []*managers.Tumbling
	for _, table := range conf.Tables.SQL {
		if table.Manager == nil {
			continue
		}
		if table.Manager.TumblingWindow == nil {
			return nil, fmt.Errorf("table %q: only tumbling_window managers are supported", table.Name)
		}
		// A windowed pipeline's entire output comes through here. Without the
		// counters on this sink, sink_rows_written would report zero for it.
		sink, err := sinks.New(ctx, table.Manager.Sink, conn,
			sinks.WithMeterProvider(mp),
			sinks.WithSinkRole("manager"),
			sinks.WithRetryEvents(events))
		if err != nil {
			return nil, fmt.Errorf("table %q manager sink: %w", table.Name, err)
		}

		window := table.Manager.TumblingWindow
		built = append(built, managers.NewTumbling(
			conn,
			window.CollectSQL,
			window.DeleteSQL,
			time.Duration(window.PollIntervalSecs)*time.Second,
			sink,
			lock,
			managers.WithLogger(l),
			managers.WithDrainBudget(budget),
		))
	}

	return built, nil
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
func (g *managerGroup) start(ctx context.Context, m *managers.Tumbling, onFail func(error)) {
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
