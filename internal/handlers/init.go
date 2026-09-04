package handlers

import (
	"context"
	"fmt"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/core"
	"github.com/turbolytics/sql-flow/internal/errs"
	"go.uber.org/zap"
)

func New(conn adbc.Connection, c config.Handler, l *zap.Logger) (core.Handler, error) {
	switch c.Type {
	case "handlers.StructuredBatch":
		// Derive the Arrow schema from the DuckDB table definition
		stmt, err := conn.NewStatement()
		if err != nil {
			return nil, errs.Wrap(errs.CodeSQLInvalid, err, "failed to create statement")
		}
		defer stmt.Close()

		if err := stmt.SetSqlQuery(fmt.Sprintf("SELECT * FROM %s LIMIT 0", c.Table)); err != nil {
			return nil, errs.Wrap(errs.CodeSQLInvalid, err, "failed to set SQL query")
		}

		reader, _, err := stmt.ExecuteQuery(context.Background())
		if err != nil {
			return nil, errs.Wrap(errs.CodeSQLInvalid, err, "failed to execute query")
		}
		defer reader.Release()

		h, err := NewStructuredBatchHandler(
			conn,
			c.SQL,
			c.Table,
			reader.Schema(),
			StructuredBatchWithLogger(l),
		)
		if err != nil {
			return nil, errs.Wrap(errs.CodeSQLInvalid, err, "failed to create StructuredBatchHandler")
		}
		return h, nil

	case "handlers.InferredMemBatch":
		h, err := NewInferredMemBatchHandler(
			conn,
			c.SQL,
			InferredMemBatchWithLogger(l),
		)
		if err != nil {
			return nil, errs.Wrap(errs.CodeSQLInvalid, err, "failed to create InferredMemBatchHandler")
		}
		return h, nil

	case "handlers.InferredDiskBatch":
		cacheDir := c.SQLResultsCacheDir
		if cacheDir == "" {
			cacheDir = config.SQLResultsCacheDir()
		}

		h, err := NewInferredDiskBatchHandler(
			conn,
			c.SQL,
			cacheDir,
			InferredDiskBatchWithLogger(l),
		)
		if err != nil {
			return nil, errs.Wrap(errs.CodeSQLInvalid, err, "failed to create InferredDiskBatchHandler")
		}
		return h, nil

	default:
		return nil, errs.New(errs.CodeSQLInvalid, "handler: %q not supported", c.Type)
	}
}
