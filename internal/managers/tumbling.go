// Package managers holds the routines that manage tables across the lifetime
// of a pipeline, rather than per batch.
package managers

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/turbolytics/sql-flow/internal/core"
	"go.uber.org/zap"
)

// defaultPollInterval matches the Python manager's default.
const defaultPollInterval = 10 * time.Second

// Tumbling publishes and then removes rows whose window has closed. The two
// SQL statements come from the config and define what "closed" means, so the
// manager itself carries no notion of time.
type Tumbling struct {
	conn         adbc.Connection
	collectSQL   string
	deleteSQL    string
	pollInterval time.Duration
	sink         core.Sink

	// Shared with the pipeline: the window queries and the handler's batch
	// both run against the same DuckDB connection.
	lock   *sync.Mutex
	logger *zap.Logger
}

func NewTumbling(
	conn adbc.Connection,
	collectSQL, deleteSQL string,
	pollInterval time.Duration,
	sink core.Sink,
	lock *sync.Mutex,
	opts ...TumblingOption,
) *Tumbling {
	if pollInterval <= 0 {
		pollInterval = defaultPollInterval
	}

	m := &Tumbling{
		conn:         conn,
		collectSQL:   collectSQL,
		deleteSQL:    deleteSQL,
		pollInterval: pollInterval,
		sink:         sink,
		lock:         lock,
		logger:       zap.NewNop(),
	}

	for _, opt := range opts {
		opt(m)
	}
	return m
}

type TumblingOption func(*Tumbling)

func WithLogger(l *zap.Logger) TumblingOption {
	return func(m *Tumbling) {
		m.logger = l.Named("manager.tumbling")
	}
}

// Start polls until the context is cancelled, then polls once more so windows
// that closed during the final interval are not stranded in the table.
func (m *Tumbling) Start(ctx context.Context) error {
	m.logger.Info("starting tumbling window manager",
		zap.Duration("poll_interval", m.pollInterval))

	ticker := time.NewTicker(m.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := m.Poll(ctx); err != nil {
				// A failed poll must not kill the manager: the next tick
				// retries, and the rows are still in the table.
				m.logger.Error("poll failed", zap.Error(err))
			}
		case <-ctx.Done():
			if err := m.Poll(context.Background()); err != nil {
				m.logger.Error("final poll failed", zap.Error(err))
			}
			return nil
		}
	}
}

// Poll publishes any closed windows and removes them from the table.
func (m *Tumbling) Poll(ctx context.Context) error {
	m.lock.Lock()
	closed, err := m.collectClosed(ctx)
	m.lock.Unlock()
	if err != nil {
		return fmt.Errorf("collecting closed windows: %w", err)
	}
	if closed == nil {
		return nil
	}
	defer closed.Release()

	if closed.NumRows() == 0 {
		return nil
	}

	m.logger.Debug("publishing closed windows", zap.Int64("rows", closed.NumRows()))

	if err := m.sink.WriteTable(closed); err != nil {
		return fmt.Errorf("writing closed windows: %w", err)
	}
	// Flushed before deleting, so a failure leaves the rows in the table to be
	// retried rather than dropping them.
	if err := m.sink.Flush(); err != nil {
		return fmt.Errorf("flushing closed windows: %w", err)
	}

	m.lock.Lock()
	defer m.lock.Unlock()
	return m.deleteClosed(ctx)
}

func (m *Tumbling) collectClosed(ctx context.Context) (arrow.Table, error) {
	stmt, err := m.conn.NewStatement()
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	if err := stmt.SetSqlQuery(m.collectSQL); err != nil {
		return nil, err
	}

	reader, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		return nil, err
	}
	defer reader.Release()

	var records []arrow.Record
	for reader.Next() {
		rec := reader.Record()
		rec.Retain()
		records = append(records, rec)
	}
	if err := reader.Err(); err != nil {
		for _, rec := range records {
			rec.Release()
		}
		return nil, err
	}

	table := array.NewTableFromRecords(reader.Schema(), records)
	table.Retain()
	for _, rec := range records {
		rec.Release()
	}
	return table, nil
}

func (m *Tumbling) deleteClosed(ctx context.Context) error {
	stmt, err := m.conn.NewStatement()
	if err != nil {
		return err
	}
	defer stmt.Close()

	if err := stmt.SetSqlQuery(m.deleteSQL); err != nil {
		return err
	}
	if _, err := stmt.ExecuteUpdate(ctx); err != nil {
		return fmt.Errorf("deleting closed windows: %w", err)
	}
	return nil
}
