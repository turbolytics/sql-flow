package rollup

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/errs"
)

// checkSource reports what the triggers need from r's source and cannot
// find: the table, a column the file reads, or an index that leads with the
// time column. The trigger on the source reads it by a time range on every
// write, and without that index each write scans the whole history.
func checkSource(ctx context.Context, q querier, r config.Rollup, path []string) ([]config.Violation, error) {
	var out []config.Violation
	add := func(key, format string, args ...any) {
		out = append(out, config.Violation{
			Code: errs.CodeConfigRollup, Path: yamlPath(path, "source", key), Message: fmt.Sprintf(format, args...),
		})
	}

	src := quote(r.Source.Table)
	cols, err := columns(ctx, q, src)
	if err != nil {
		return nil, err
	}
	if len(cols) == 0 {
		add("table", "rollup %s: source table %s does not exist on the connection's search_path; the pipeline's migration creates it",
			r.Name, r.Source.Table)
		return out, nil
	}
	for _, c := range sourceColumns(r) {
		if _, ok := cols[c]; !ok {
			add("table", "rollup %s: source table %s has no column %s, which the rollups file reads", r.Name, r.Source.Table, c)
		}
	}

	var indexed bool
	if err := q.QueryRow(ctx, `SELECT EXISTS (
  SELECT 1 FROM pg_index i
  JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = i.indkey[0]
  WHERE i.indrelid = to_regclass($1) AND a.attname = $2)`, src, r.Source.TimeColumn).Scan(&indexed); err != nil {
		return nil, err
	}
	if !indexed {
		add("time_column", "rollup %s: no index on %s leads with %s, so the trigger on it would scan the whole table on every write; run CREATE INDEX ON %s (%s)",
			r.Name, r.Source.Table, r.Source.TimeColumn, src, quote(r.Source.TimeColumn))
	}
	return out, nil
}

// sourceColumns is every source column the triggers read, sorted.
func sourceColumns(r config.Rollup) []string {
	seen := map[string]bool{r.Source.TimeColumn: true}
	for _, d := range r.Source.Dimensions {
		seen[d] = true
	}
	for _, set := range r.DimensionSets {
		for _, m := range set.Measures {
			if m.Column != "" {
				seen[m.Column] = true
			}
		}
	}
	return sortedKeys(seen)
}

// columns maps each column of relation to its type as Postgres prints it.
// relation is a quoted name, optionally schema-qualified. A relation that
// does not exist has no columns.
func columns(ctx context.Context, q querier, relation string) (map[string]string, error) {
	rows, err := q.Query(ctx, `SELECT a.attname, format_type(a.atttypid, a.atttypmod)
FROM pg_attribute a
WHERE a.attrelid = to_regclass($1) AND a.attnum > 0 AND NOT a.attisdropped`, relation)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]string{}
	for rows.Next() {
		var name, typ string
		if err := rows.Scan(&name, &typ); err != nil {
			return nil, err
		}
		out[name] = typ
	}
	return out, rows.Err()
}

// compareTable reports a declared table that exists with other columns than
// the file declares for it: another declaration built it. The expected shape
// comes the way ddl gets it, from the generator's query WITH NO DATA, into a
// temporary table that the transaction drops, so it must run inside one.
func compareTable(ctx context.Context, q querier, r config.Rollup, e edge, relation string, path []string) ([]config.Violation, error) {
	tmp := quote("sqlflow_expect_" + e.Table)
	if _, err := q.Exec(ctx, "CREATE TEMP TABLE "+tmp+" ON COMMIT DROP AS\n"+tableQuery(r, e.Set, e.Grain)+"\nWITH NO DATA"); err != nil {
		return nil, err
	}
	want, err := columns(ctx, q, "pg_temp."+tmp)
	if err != nil {
		return nil, err
	}
	got, err := columns(ctx, q, relation)
	if err != nil {
		return nil, err
	}

	var diffs []string
	for _, c := range sortedKeys(want, got) {
		w, declared := want[c]
		g, stored := got[c]
		switch {
		case !stored:
			diffs = append(diffs, c+" is missing")
		case !declared:
			diffs = append(diffs, c+" is not declared")
		case w != g:
			diffs = append(diffs, fmt.Sprintf("%s is %s, not %s", c, g, w))
		}
	}
	if len(diffs) == 0 {
		return nil, nil
	}
	return []config.Violation{{
		Code: errs.CodeConfigRollupChange,
		Path: yamlPath(path, "dimension_sets", strconv.Itoa(e.SetIndex)),
		Message: fmt.Sprintf("rollup %s: table %s exists with other columns than the file declares (%s), so another declaration built it; %s, or drop the table by hand",
			r.Name, e.Table, strings.Join(diffs, "; "), changeRemedy),
	}}, nil
}
