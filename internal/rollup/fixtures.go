package rollup

import (
	"context"
	"database/sql/driver"
	"fmt"
	"maps"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/errs"
)

// CaseFailure is one way a fixture case's final state differs from what the
// case expects.
type CaseFailure struct {
	Table string
	// Key is the row's key, such as "bucket=2026-09-24T12:05:00Z, lang=en".
	Key string
	// Kind is "missing", an expected row the table lacks; "extra", a row
	// the table holds that the case does not list; "differs"; or
	// "invariant", where Expected is the invariant and Actual what breaks
	// it.
	Kind string
	// Expected and Actual are the two rows as column=value lists. A side
	// the row lacks is empty.
	Expected string
	Actual   string
}

// RunCase runs case tc, the index-th of its file, in the sandbox on the
// session's search_path: it empties every source and rollup table of conf,
// runs each write as one upsert in its own transaction, compares each
// listed table to its expected rows by key, and checks the invariants.
//
// A write that names a column the source lacks is refused at its YAML path
// before anything is emptied or written.
func RunCase(ctx context.Context, conn *pgx.Conn, conf *config.RollupsConf, index int, tc config.RollupTestCase) ([]CaseFailure, error) {
	path := []string{"tests", strconv.Itoa(index)}
	var r config.Rollup
	for _, cand := range conf.Rollups {
		if cand.Name == tc.Rollup {
			r = cand
		}
	}
	if r.Name == "" {
		return nil, caseRuleError([]config.Violation{{Code: errs.CodeConfigRollup, Path: yamlPath(path, "rollup"),
			Message: fmt.Sprintf("test case %q names rollup %q, which the rollups file does not declare", tc.Name, tc.Rollup)}})
	}
	srcTypes, err := columns(ctx, conn, quote(r.Source.Table))
	if err != nil {
		return nil, caseError(err, tc.Name, "read the columns of "+r.Source.Table)
	}
	var vs []config.Violation
	for j, w := range tc.Writes {
		for k, row := range w {
			for _, name := range slices.Sorted(maps.Keys(row)) {
				if _, ok := srcTypes[name]; !ok {
					vs = append(vs, config.Violation{Code: errs.CodeConfigRollup,
						Path:    yamlPath(path, "writes", strconv.Itoa(j), strconv.Itoa(k), name),
						Message: fmt.Sprintf("test case %q writes column %s, which source table %s does not have", tc.Name, name, r.Source.Table)})
				}
			}
		}
	}
	if len(vs) > 0 {
		return nil, caseRuleError(vs)
	}

	// An expected timestamp written without an offset reads as UTC.
	if _, err := conn.Exec(ctx, "SET TimeZone TO 'UTC'"); err != nil {
		return nil, caseError(err, tc.Name, "set TimeZone")
	}
	if _, err := conn.Exec(ctx, "TRUNCATE "+quoteList(caseTables(conf))); err != nil {
		return nil, caseError(err, tc.Name, "empty the tables")
	}
	key := append([]string{r.Source.TimeColumn}, r.Source.Dimensions...)
	for j, w := range tc.Writes {
		if err := writeCase(ctx, conn, r.Source.Table, srcTypes, key, w); err != nil {
			return nil, errs.Wrap(errs.CodeConfigRollup, err, "rollup test: %s", strings.Join(yamlPath(path, "writes", strconv.Itoa(j)), "."))
		}
	}

	var out []CaseFailure
	for _, table := range slices.Sorted(maps.Keys(tc.Expect)) {
		// Check refuses an undeclared table first; a direct caller gets the
		// same rule rather than a panic.
		if _, ok := findEdge(r, table); !ok {
			return nil, caseRuleError([]config.Violation{{Code: errs.CodeConfigRollup, Path: yamlPath(path, "expect", table),
				Message: fmt.Sprintf("test case %q expects table %s, which rollup %s does not declare", tc.Name, table, r.Name)}})
		}
		fs, err := compareCase(ctx, conn, r, table, tc.Expect[table], yamlPath(path, "expect", table))
		if err != nil {
			return nil, err
		}
		out = append(out, fs...)
	}
	invariants, err := CheckInvariants(ctx, conn, conf)
	if err != nil {
		return nil, err
	}
	for _, f := range invariants {
		cf := CaseFailure{Table: f.Table, Kind: "invariant", Expected: f.Invariant, Actual: fmt.Sprintf("%d buckets break it", f.Buckets)}
		if len(f.Sample) > 0 {
			s := f.Sample[0]
			cf.Key = r.Source.TimeColumn + "=" + s.Bucket.UTC().Format(time.RFC3339Nano)
			if s.Key != "" {
				cf.Key += ", " + s.Key
			}
			cf.Actual += fmt.Sprintf("; the first is %s %s", s.Kind, s.Measure)
		}
		out = append(out, cf)
	}
	return out, nil
}

// caseTables is every source and rollup table of conf, sources first, each
// once: two rollups may read one source.
func caseTables(conf *config.RollupsConf) []string {
	var out []string
	seen := map[string]bool{}
	for _, r := range conf.Rollups {
		if !seen[r.Source.Table] {
			seen[r.Source.Table] = true
			out = append(out, r.Source.Table)
		}
	}
	for _, r := range conf.Rollups {
		for _, e := range edges(r) {
			out = append(out, e.Table)
		}
	}
	return out
}

// writeCase writes one write's rows as one upsert. Its columns are every
// column a row names, and a row that omits one writes NULL there, as a
// sink batch does for a field a record lacks.
func writeCase(ctx context.Context, conn *pgx.Conn, table string, types map[string]string, key []string, rows []map[string]any) error {
	named := map[string]bool{}
	for _, row := range rows {
		for name := range row {
			named[name] = true
		}
	}
	var cols []sourceColumn
	for _, name := range slices.Sorted(maps.Keys(named)) {
		cols = append(cols, sourceColumn{Name: name, Type: types[name]})
	}
	var args []any
	for _, row := range rows {
		for _, c := range cols {
			args = append(args, yamlText(row[c.Name]))
		}
	}
	return writeBatch(ctx, conn, "UTC", upsertSQL(table, cols, key, len(rows)), args)
}

// caseRow is one row of a listed table: its values as Postgres returns them,
// and each rendered.
type caseRow struct {
	values   []any
	rendered []string
}

// compareCase compares table to the rows the case expects there. Both
// sides pass through the column's type, so "2026-09-24T12:05:00Z" and
// "2026-09-24 12:05:00+00" name one bucket. A double sum matches within
// verify's tolerance, and every other value exactly.
func compareCase(ctx context.Context, conn *pgx.Conn, r config.Rollup, table string, expect []map[string]any, path []string) ([]CaseFailure, error) {
	e, _ := findEdge(r, table)
	cols := config.RollupTableColumns(r)[table]
	nKey := len(keyColumns(r, e.Set))
	types, err := columns(ctx, conn, quote(table))
	if err != nil {
		return nil, caseError(err, table, "read its columns")
	}
	render := func(values []any) caseRow {
		row := caseRow{values: values}
		for _, v := range values {
			row.rendered = append(row.rendered, renderValue(v))
		}
		return row
	}
	keyOf := func(row caseRow) string {
		parts := make([]string, nKey)
		for i := range parts {
			parts[i] = cols[i] + "=" + row.rendered[i]
		}
		return strings.Join(parts, ", ")
	}
	full := func(row caseRow) string {
		parts := make([]string, len(cols))
		for i := range parts {
			parts[i] = cols[i] + "=" + row.rendered[i]
		}
		return strings.Join(parts, ", ")
	}

	casts := make([]string, len(cols))
	for i, c := range cols {
		casts[i] = "$" + strconv.Itoa(i+1) + "::" + types[c]
	}
	want := map[string]caseRow{}
	for j, row := range expect {
		args := make([]any, len(cols))
		for i, c := range cols {
			args[i] = yamlText(row[c])
		}
		rows, err := conn.Query(ctx, "SELECT "+strings.Join(casts, ", "), args...)
		if err != nil {
			return nil, errs.Wrap(errs.CodeConfigRollup, err, "rollup test: %s", strings.Join(yamlPath(path, strconv.Itoa(j)), "."))
		}
		values, err := oneRow(rows)
		if err != nil {
			return nil, errs.Wrap(errs.CodeConfigRollup, err, "rollup test: %s", strings.Join(yamlPath(path, strconv.Itoa(j)), "."))
		}
		w := render(values)
		k := keyOf(w)
		if _, dup := want[k]; dup {
			return nil, caseRuleError([]config.Violation{{Code: errs.CodeConfigRollup, Path: yamlPath(path, strconv.Itoa(j)),
				Message: fmt.Sprintf("a second expected %s row has key %s", table, k)}})
		}
		want[k] = w
	}

	rows, err := conn.Query(ctx, "SELECT "+quoteList(cols)+" FROM "+quote(table))
	if err != nil {
		return nil, caseError(err, table, "read it")
	}
	got := map[string]caseRow{}
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			rows.Close()
			return nil, caseError(err, table, "read it")
		}
		g := render(values)
		got[keyOf(g)] = g
	}
	if err := rows.Err(); err != nil {
		return nil, caseError(err, table, "read it")
	}

	var out []CaseFailure
	for _, k := range slices.Sorted(maps.Keys(mergeKeys(want, got))) {
		w, inWant := want[k]
		g, inGot := got[k]
		switch {
		case !inGot:
			out = append(out, CaseFailure{Table: table, Key: k, Kind: "missing", Expected: full(w)})
		case !inWant:
			out = append(out, CaseFailure{Table: table, Key: k, Kind: "extra", Actual: full(g)})
		default:
			for i := nKey; i < len(cols); i++ {
				if valueDiffers(e.Set.Measures[cols[i]], w.values[i], g.values[i], w.rendered[i], g.rendered[i]) {
					out = append(out, CaseFailure{Table: table, Key: k, Kind: "differs", Expected: full(w), Actual: full(g)})
					break
				}
			}
		}
	}
	return out, nil
}

// valueDiffers compares one measure: a double sum within verify's
// tolerance, as verify compares it, and any other by its text.
func valueDiffers(m config.RollupMeasure, w, g any, wText, gText string) bool {
	wf, wok := w.(float64)
	gf, gok := g.(float64)
	if m.Type == "sum" && m.Numeric == "double" && wok && gok {
		return math.Abs(wf-gf) > verifyTolerance*math.Max(math.Abs(wf), math.Abs(gf))
	}
	return wText != gText
}

func mergeKeys(a, b map[string]caseRow) map[string]bool {
	out := map[string]bool{}
	for k := range a {
		out[k] = true
	}
	for k := range b {
		out[k] = true
	}
	return out
}

func oneRow(rows pgx.Rows) ([]any, error) {
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return nil, err
		}
		return nil, pgx.ErrNoRows
	}
	values, err := rows.Values()
	if err != nil {
		return nil, err
	}
	rows.Close()
	return values, rows.Err()
}

// yamlText is a value from the test file as the text a statement casts to
// the column's type. YAML hands over a string, a number, a bool or null.
func yamlText(v any) any {
	switch v := v.(type) {
	case nil:
		return nil
	case string:
		return v
	case time.Time:
		return v.Format(time.RFC3339Nano)
	case float64:
		return strconv.FormatFloat(v, 'g', -1, 64)
	default:
		return fmt.Sprint(v)
	}
}

// renderValue is a value Postgres returned, as a failure prints it. An
// instant prints in UTC, as the test file writes one.
func renderValue(v any) string {
	switch v := v.(type) {
	case nil:
		return "NULL"
	case string:
		return v
	case time.Time:
		return v.UTC().Format(time.RFC3339Nano)
	case float64:
		return strconv.FormatFloat(v, 'g', -1, 64)
	case float32:
		return strconv.FormatFloat(float64(v), 'g', -1, 32)
	case driver.Valuer:
		dv, err := v.Value()
		if err != nil {
			return fmt.Sprint(v)
		}
		return renderValue(dv)
	default:
		return fmt.Sprint(v)
	}
}

// caseRuleError is violations of the test file's rules that only the
// database can check, as CheckError lists the static ones.
func caseRuleError(vs []config.Violation) error {
	var b strings.Builder
	b.WriteString("the rollups test file is invalid")
	for _, v := range vs {
		fmt.Fprintf(&b, "\n  %s: %s", strings.Join(v.Path, "."), v.Message)
	}
	return errs.New(vs[0].Code, "%s", b.String())
}

func caseError(err error, subject, step string) error {
	return errs.Wrap(errs.CodeRollupInternal, err, "rollup test: %s: %s", subject, step)
}
