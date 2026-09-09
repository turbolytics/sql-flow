package core

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow/array"
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
