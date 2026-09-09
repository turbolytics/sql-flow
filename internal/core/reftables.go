package core

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/turbolytics/sql-flow/internal/config"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// batchTable is declared in stats.go: the transient table the handlers create
// per batch. It is the one name the handler SQL always reads and never a
// reference table.

// RefTable is a table the handler SQL reads that the pipeline did not create.
type RefTable struct {
	Catalog string
	Schema  string
	Name    string
}

// Qualified renders the name as SQL can address it.
func (r RefTable) Qualified() string {
	parts := make([]string, 0, 3)
	if r.Catalog != "" {
		parts = append(parts, r.Catalog)
	}
	if r.Schema != "" {
		parts = append(parts, r.Schema)
	}
	return strings.Join(append(parts, r.Name), ".")
}

// Attached reports whether the table lives in an ATTACHed catalog, where a
// full count crosses the wire to somebody else's server.
func (r RefTable) Attached() bool { return r.Catalog != "" }

// ReferenceTables reports the tables the handler SQL joins.
//
// Deriving beats declaring. The operator adds no config, and the set tracks
// the SQL automatically rather than drifting from it when a table is renamed.
//
// managed names the tables the `tables:` block created. Those legitimately
// start empty -- a tumbling window's aggregate table holds nothing until the
// first window closes -- so they are not reference tables.
func ReferenceTables(
	ctx context.Context,
	conn adbc.Connection,
	sql string,
	managed []string,
) ([]RefTable, error) {
	ast, err := serializeSQL(ctx, conn, sql)
	if err != nil {
		return nil, err
	}

	var root any
	if err := json.Unmarshal([]byte(ast), &root); err != nil {
		return nil, fmt.Errorf("parse serialized sql: %w", err)
	}

	// json_serialize_sql reports a parse failure in the JSON rather than as a
	// query error, so an unreadable query looks like a successful call
	// returning no tables unless this is checked.
	if m, ok := root.(map[string]any); ok {
		if bad, _ := m["error"].(bool); bad {
			return nil, fmt.Errorf("handler sql did not parse: %v", m["error_message"])
		}
	}

	skip := map[string]bool{batchTable: true}
	for _, name := range managed {
		skip[strings.ToLower(name)] = true
	}
	// DuckDB emits a CTE reference as a BASE_TABLE node, so a CTE name is
	// indistinguishable from a real table until the declarations are
	// subtracted. Counting one fails, and every pipeline using WITH would warn
	// at every start -- a check that cries wolf is worse than no check.
	for _, name := range collectCTENames(root) {
		skip[strings.ToLower(name)] = true
	}

	var (
		out  []RefTable
		seen = map[string]bool{}
	)
	for _, tbl := range collectBaseTables(root) {
		if skip[strings.ToLower(tbl.Name)] {
			continue
		}
		q := tbl.Qualified()
		if seen[q] {
			continue
		}
		seen[q] = true
		out = append(out, tbl)
	}
	return out, nil
}

// CheckReferenceTables counts the tables the handler SQL joins and reports
// what it found.
//
// This is a diagnostic, not a gate. A count that fails logs a warning and the
// pipeline starts: a table that does not exist fails later at prepare, which
// is static validation's job, and refusing to start on an empty dimension is a
// UX decision nobody has made yet. A warning is reversible; a refusal is not.
//
// m may be nil, for callers with no metrics.
func CheckReferenceTables(
	ctx context.Context,
	conn adbc.Connection,
	conf *config.Conf,
	m *Metrics,
	l *zap.Logger,
) error {
	sql := conf.Pipeline.Handler.SQL
	if strings.TrimSpace(sql) == "" {
		return nil
	}

	// The repo's development logger stacktraces every warning. Here that
	// buries the one fact the operator needs -- which table is empty, already
	// in the fields -- under a frame list pointing at this file, which tells
	// them nothing. Errors still carry theirs.
	l = l.WithOptions(zap.AddStacktrace(zapcore.ErrorLevel))

	var managed []string
	if conf.Tables != nil {
		for _, t := range conf.Tables.SQL {
			managed = append(managed, t.Name)
		}
	}

	tables, err := ReferenceTables(ctx, conn, sql, managed)
	if err != nil {
		// Unreadable SQL is the static validator's finding, not a reason to
		// stop a pipeline DuckDB may still accept.
		l.Warn("could not derive reference tables from handler sql", zap.Error(err))
		return nil
	}

	for _, tbl := range tables {
		start := time.Now()

		// A full count on an ATTACHed catalog is a sequential scan on somebody
		// else's server. The difference between some rows and none is the fact
		// worth having, and EXISTS gets it for the price of one row.
		if tbl.Attached() {
			has, err := refTableHasRows(ctx, conn, tbl)
			if err != nil {
				l.Warn("could not probe reference table",
					zap.String("table", tbl.Qualified()), zap.Error(err))
				continue
			}
			if !has {
				l.Warn("reference table is empty, joined by handler SQL",
					zap.String("table", tbl.Qualified()),
					zap.Duration("took", time.Since(start)))
				continue
			}
			l.Info("reference table has rows",
				zap.String("table", tbl.Qualified()),
				zap.Duration("took", time.Since(start)))
			continue
		}

		n, err := countRefTable(ctx, conn, tbl)
		if err != nil {
			l.Warn("could not count reference table",
				zap.String("table", tbl.Qualified()), zap.Error(err))
			continue
		}

		took := time.Since(start)
		if m != nil {
			m.ReferenceTableRows.Record(ctx, n,
				metric.WithAttributes(attribute.String("table", tbl.Qualified())))
		}

		if n == 0 {
			l.Warn("reference table has 0 rows, joined by handler SQL",
				zap.String("table", tbl.Qualified()),
				zap.Int64("rows", n),
				zap.Duration("took", took))
			continue
		}
		l.Info("reference table loaded",
			zap.String("table", tbl.Qualified()),
			zap.Int64("rows", n),
			zap.Duration("took", took))
	}
	return nil
}

// countRefTable returns the table's row count. Named apart from stats.go's
// countRows, which counts a managed state table by name.
func countRefTable(ctx context.Context, conn adbc.Connection, tbl RefTable) (int64, error) {
	var n int64
	err := scalar(ctx, conn, "SELECT COUNT(*) FROM "+tbl.Qualified(),
		func(rec arrow.Record) error {
			col, ok := rec.Column(0).(*array.Int64)
			if !ok {
				return fmt.Errorf("COUNT(*) returned %T", rec.Column(0))
			}
			n = col.Value(0)
			return nil
		})
	return n, err
}

// refTableHasRows reports whether the table holds anything, without counting it.
func refTableHasRows(ctx context.Context, conn adbc.Connection, tbl RefTable) (bool, error) {
	var has bool
	err := scalar(ctx, conn,
		"SELECT EXISTS(SELECT 1 FROM "+tbl.Qualified()+" LIMIT 1)",
		func(rec arrow.Record) error {
			col, ok := rec.Column(0).(*array.Boolean)
			if !ok {
				return fmt.Errorf("EXISTS returned %T", rec.Column(0))
			}
			has = col.Value(0)
			return nil
		})
	return has, err
}

// scalar runs a query and hands its first record to read.
func scalar(ctx context.Context, conn adbc.Connection, sql string, read func(arrow.Record) error) error {
	stmt, err := conn.NewStatement()
	if err != nil {
		return err
	}
	defer stmt.Close()

	if err := stmt.SetSqlQuery(sql); err != nil {
		return err
	}

	reader, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		return err
	}
	defer reader.Release()

	for reader.Next() {
		rec := reader.Record()
		if rec.NumRows() == 0 {
			continue
		}
		return read(rec)
	}
	return fmt.Errorf("query returned no rows: %s", sql)
}

// serializeSQL returns DuckDB's JSON AST for the query, without executing it.
func serializeSQL(ctx context.Context, conn adbc.Connection, sql string) (string, error) {
	stmt, err := conn.NewStatement()
	if err != nil {
		return "", err
	}
	defer stmt.Close()

	if err := stmt.SetSqlQuery(
		"SELECT json_serialize_sql(" + quoteLiteral(sql) + ")",
	); err != nil {
		return "", err
	}

	reader, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		return "", err
	}
	defer reader.Release()

	for reader.Next() {
		rec := reader.Record()
		if rec.NumRows() == 0 {
			continue
		}
		col, ok := rec.Column(0).(*array.String)
		if !ok {
			return "", fmt.Errorf("json_serialize_sql returned %T", rec.Column(0))
		}
		return col.Value(0), nil
	}
	return "", fmt.Errorf("json_serialize_sql returned no rows")
}

// quoteLiteral renders s as a single-quoted SQL string. Handler SQL is
// operator input and may contain any quote sequence.
func quoteLiteral(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

// collectBaseTables walks every BASE_TABLE node, at any depth. Joins nest
// inside subqueries, so a top-level scan is not enough.
func collectBaseTables(n any) []RefTable {
	var out []RefTable
	switch v := n.(type) {
	case map[string]any:
		if v["type"] == "BASE_TABLE" {
			if name, _ := v["table_name"].(string); name != "" {
				catalog, _ := v["catalog_name"].(string)
				schema, _ := v["schema_name"].(string)
				out = append(out, RefTable{
					Catalog: catalog,
					Schema:  schema,
					Name:    name,
				})
			}
		}
		for _, child := range v {
			out = append(out, collectBaseTables(child)...)
		}
	case []any:
		for _, child := range v {
			out = append(out, collectBaseTables(child)...)
		}
	}
	return out
}

// collectCTENames walks every cte_map, at any depth, returning the names a
// WITH clause declares.
func collectCTENames(n any) []string {
	var out []string
	switch v := n.(type) {
	case map[string]any:
		if cm, ok := v["cte_map"].(map[string]any); ok {
			if entries, ok := cm["map"].([]any); ok {
				for _, e := range entries {
					entry, ok := e.(map[string]any)
					if !ok {
						continue
					}
					if key, _ := entry["key"].(string); key != "" {
						out = append(out, key)
					}
				}
			}
		}
		for _, child := range v {
			out = append(out, collectCTENames(child)...)
		}
	case []any:
		for _, child := range v {
			out = append(out, collectCTENames(child)...)
		}
	}
	return out
}
