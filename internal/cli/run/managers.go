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
