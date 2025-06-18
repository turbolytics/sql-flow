package handlers

import (
	"context"
	"fmt"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/turbolytics/turbine/internal/config"
	"github.com/turbolytics/turbine/internal/core"
	"go.uber.org/zap"
)

func New(conn adbc.Connection, c config.Handler, l *zap.Logger) (core.Handler, error) {
	switch c.Type {
	case "handlers.StructuredBatch":
		// Create a statement to fetch the schema
		stmt, err := conn.NewStatement()
		if err != nil {
			return nil, fmt.Errorf("failed to create statement: %w", err)
		}
		defer stmt.Close()

		// Set the query to fetch the schema
		if err := stmt.SetSqlQuery(fmt.Sprintf("SELECT * FROM %s LIMIT 0", c.Table)); err != nil {
			return nil, fmt.Errorf("failed to set SQL query: %w", err)
		}

		// Execute the query to get the schema
		reader, _, err := stmt.ExecuteQuery(context.Background())
		if err != nil {
			return nil, fmt.Errorf("failed to execute query: %w", err)
		}
		defer reader.Release()

		// Create the StructuredBatchHandler
		h, err := NewStructuredBatchHandler(
			conn,
			c.SQL,
			c.Table,
			reader.Schema(),
			StructuredBatchWithLogger(l),
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create StructuredBatchHandler: %w", err)
		}
		return h, nil
	default:
		return nil, fmt.Errorf(`handler: %q not supported`, c.Type)
	}
}
