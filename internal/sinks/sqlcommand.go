package sinks

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/google/uuid"
	"github.com/turbolytics/sql-flow/internal/config"
)

// sinkBatchTable is the table the user's sink SQL selects from. The Python
// sink registers the Arrow table under this name; here it is materialized
// into a table of the same name.
const sinkBatchTable = "sqlflow_sink_batch"

// SQLCommandSink materializes the result batch as a table and runs arbitrary
// DuckDB SQL against it. This is how the Python engine writes parquet, S3,
// Postgres, DuckLake and MotherDuck outputs.
type SQLCommandSink struct {
	conn          adbc.Connection
	sql           string
	substitutions []config.SQLCommandSubstitution

	mu     sync.Mutex
	tables []arrow.Table
}

func NewSQLCommandSink(conn adbc.Connection, sql string, substitutions []config.SQLCommandSubstitution) (*SQLCommandSink, error) {
	if strings.TrimSpace(sql) == "" {
		return nil, fmt.Errorf("sqlcommand sink: sql is required")
	}
	return &SQLCommandSink{conn: conn, sql: sql, substitutions: substitutions}, nil
}

func (s *SQLCommandSink) WriteTable(ctx context.Context, batch arrow.Table) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	batch.Retain()
	s.tables = append(s.tables, batch)
	return nil
}

// Flush runs the sink SQL over the buffered rows, and keeps them buffered if
// it cannot.
//
// Every batch used to be released on the way out whatever the outcome, so a
// failed flush left nothing to retry. Nothing calls Flush twice on this sink
// today -- retriesHelp excludes it, and processBatch calls it once and lets
// the error stop the pipeline -- so the rows replayed from the source rather
// than vanishing. That made it correct by accident of the call pattern, and
// the accident ends the day anything retries a sqlcommand sink, or an error
// policy makes a failed flush non-fatal.
//
// Keeping the batch is the invariant. Whether a retry ladder is wrapped
// around this sink is a different question with a different answer, and
// retriesHelp answers only that one.
func (s *SQLCommandSink) Flush(ctx context.Context) error {
	s.mu.Lock()
	tables := s.tables
	s.tables = nil
	s.mu.Unlock()

	if len(tables) == 0 {
		return nil
	}

	if err := s.send(ctx, tables); err != nil {
		s.requeue(tables)
		return err
	}

	for _, t := range tables {
		t.Release()
	}
	return nil
}

// requeue returns undelivered batches to the head of the buffer, ahead of
// anything written while the failed flush was in flight, so a retry runs them
// in the order they arrived.
func (s *SQLCommandSink) requeue(tables []arrow.Table) {
	s.mu.Lock()
	s.tables = append(tables, s.tables...)
	s.mu.Unlock()
}

// send is one delivery attempt. It releases nothing: whether these batches can
// be dropped is the caller's decision, and it depends on this error.
func (s *SQLCommandSink) send(ctx context.Context, tables []arrow.Table) error {
	if err := s.materialize(ctx, tables); err != nil {
		return err
	}

	sql, err := s.applySubstitutions()
	if err != nil {
		return err
	}

	stmt, err := s.conn.NewStatement()
	if err != nil {
		return err
	}
	defer stmt.Close()

	if err := stmt.SetSqlQuery(sql); err != nil {
		return fmt.Errorf("sqlcommand sink: set sql: %w", err)
	}
	if _, err := stmt.ExecuteUpdate(ctx); err != nil {
		return fmt.Errorf("sqlcommand sink: execute: %w", err)
	}
	return nil
}

// materialize replaces the sink batch table with the accumulated rows.
func (s *SQLCommandSink) materialize(ctx context.Context, tables []arrow.Table) error {
	dropStmt, err := s.conn.NewStatement()
	if err != nil {
		return err
	}
	if err := dropStmt.SetSqlQuery("DROP TABLE IF EXISTS " + sinkBatchTable); err != nil {
		dropStmt.Close()
		return err
	}
	if _, err := dropStmt.ExecuteUpdate(ctx); err != nil {
		dropStmt.Close()
		return err
	}
	dropStmt.Close()

	ingest, err := s.conn.NewStatement()
	if err != nil {
		return err
	}
	defer ingest.Close()

	if err := ingest.SetOption(adbc.OptionKeyIngestMode, adbc.OptionValueIngestModeCreateAppend); err != nil {
		return err
	}
	if err := ingest.SetOption(adbc.OptionKeyIngestTargetTable, sinkBatchTable); err != nil {
		return err
	}

	for _, table := range tables {
		reader := array.NewTableReader(table, 0)
		for reader.Next() {
			rec := reader.Record()
			if rec.NumRows() == 0 {
				continue
			}
			if err := ingest.Bind(ctx, rec); err != nil {
				reader.Release()
				return fmt.Errorf("sqlcommand sink: bind: %w", err)
			}
			if _, err := ingest.ExecuteUpdate(ctx); err != nil {
				reader.Release()
				return fmt.Errorf("sqlcommand sink: ingest: %w", err)
			}
		}
		err := reader.Err()
		reader.Release()
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *SQLCommandSink) applySubstitutions() (string, error) {
	sql := s.sql
	for _, sub := range s.substitutions {
		switch sub.Type {
		case "uuid4":
			sql = strings.ReplaceAll(sql, sub.Var, uuid.New().String())
		default:
			return "", fmt.Errorf("unsupported substitution type: %q", sub.Type)
		}
	}
	return sql, nil
}

// BufferedRows reports the rows this sink is holding that no flush has
// delivered. The pipeline publishes it as sink_buffered_rows, so an operator
// can tell a sink retrying a destination from one that has stopped draining.
func (s *SQLCommandSink) BufferedRows() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	var rows int64
	for _, t := range s.tables {
		rows += t.NumRows()
	}
	return int(rows)
}
