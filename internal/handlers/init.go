package handlers

import (
	"context"
	"fmt"
	"sort"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/core"
	"github.com/turbolytics/sql-flow/internal/errs"
	"go.uber.org/zap"
)

// configTypes maps the config's Python-era handler names to the registry's.
//
// The config keeps the long form because every shipped example carries it.
// integrations.yml uses the short one, which is what an id like
// handler.inferred_mem reads as.
var configTypes = map[string]string{
	"handlers.StructuredBatch":   "structured",
	"handlers.InferredMemBatch":  "inferred_mem",
	"handlers.InferredDiskBatch": "inferred_disk",
}

// builders constructs each handler type.
//
// A map rather than a switch so Kinds can list it. A registry test holds that
// list equal to integrations.yml, and a handler the engine can build but
// nothing declares has no invariant cells at all.
var builders = map[string]func(conn adbc.Connection, c config.Handler, l *zap.Logger) (core.Handler, error){
	"structured": func(conn adbc.Connection, c config.Handler, l *zap.Logger) (core.Handler, error) {
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
	},

	"inferred_mem": func(conn adbc.Connection, c config.Handler, l *zap.Logger) (core.Handler, error) {
		h, err := NewInferredMemBatchHandler(
			conn,
			c.SQL,
			InferredMemBatchWithLogger(l),
		)
		if err != nil {
			return nil, errs.Wrap(errs.CodeSQLInvalid, err, "failed to create InferredMemBatchHandler")
		}
		return h, nil
	},

	"inferred_disk": func(conn adbc.Connection, c config.Handler, l *zap.Logger) (core.Handler, error) {
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
	},
}

// Kinds lists every handler type the engine can build, sorted. The registry's
// short names, not the config's.
func Kinds() []string {
	out := make([]string, 0, len(builders))
	for kind := range builders {
		out = append(out, kind)
	}
	sort.Strings(out)
	return out
}

// ConfigTypes lists the handler names a config may write, sorted. Kinds
// returns the registry's short names; these are the long ones users type.
//
// The generated config schema builds its handler enum from this, so a handler
// the engine can build is always one the schema accepts.
func ConfigTypes() []string {
	out := make([]string, 0, len(configTypes))
	for name := range configTypes {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

func New(conn adbc.Connection, c config.Handler, l *zap.Logger) (core.Handler, error) {
	kind, ok := configTypes[c.Type]
	if !ok {
		return nil, errs.New(errs.CodeSQLInvalid, "handler: %q not supported", c.Type)
	}
	return builders[kind](conn, c, l)
}
