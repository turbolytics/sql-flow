package run

import (
	"fmt"
	"sync"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/managers"
	"github.com/turbolytics/sql-flow/internal/sinks"
	"go.uber.org/zap"
)

// buildManagedTables constructs a manager per table that declares one. Each
// manager gets its own sink and shares the pipeline's DuckDB lock.
func buildManagedTables(
	conf *config.Conf,
	conn adbc.Connection,
	lock *sync.Mutex,
	l *zap.Logger,
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
		sink, err := sinks.New(table.Manager.Sink, conn)
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
		))
	}

	return built, nil
}
