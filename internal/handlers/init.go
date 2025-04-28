package handlers

import (
	"context"
	"fmt"
	"github.com/marcboeker/go-duckdb"
	"github.com/turbolytics/turbine/internal/config"
	"github.com/turbolytics/turbine/internal/core"
)

func New(arrConn *duckdb.Arrow, c config.Handler) (core.Handler, error) {
	switch c.Type {
	case "handlers.StructuredBatch":
		rdr, err := arrConn.QueryContext(
			context.Background(),
			fmt.Sprintf("SELECT * FROM %s", c.Table),
		)
		if err != nil {
			return nil, err
		}

		h, err := NewStructuredBatchHandler(
			arrConn,
			c.SQL,
			c.Table,
			rdr.Schema(),
		)
		return h, err
	default:
		return nil, fmt.Errorf(`handler: %q not supported`, c.Type)
	}
}
