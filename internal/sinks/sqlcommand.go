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

func (s *SQLCommandSink) Batch() (arrow.Table, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.tables) == 0 {
		return nil, nil
	}
	return s.tables[len(s.tables)-1], nil
}

func (s *SQLCommandSink) Flush(ctx context.Context) error {
	s.mu.Lock()
	tables := s.tables
	s.tables = nil
	s.mu.Unlock()

	if len(tables) == 0 {
		return nil
	}
	defer func() {
		for _, t := range tables {
			t.Release()
		}
	}()

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
