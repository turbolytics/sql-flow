// Package rollup generates, from a rollups file, the database objects that
// keep rollup tables current and the serve datasets that read them, and
// checks committed copies of both against the file.
package rollup

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/turbolytics/sql-flow/internal/config"
)

// origin is the instant every bucket is binned from. Midnight UTC puts hours,
// 6-hour buckets and days on UTC boundaries whatever the session's zone.
const origin = "TIMESTAMPTZ '2000-01-01 00:00:00+00'"

// Table names the table holding a dimension set at a grain.
func Table(set config.RollupDimensionSet, grain string) string {
	return set.Name + "_" + grain
}

// quote double-quotes a Postgres identifier. Names already match
// ^[a-z][a-z0-9_]*$; quoting keeps a keyword such as user working.
func quote(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

func quoteList(names []string) string {
	quoted := make([]string, len(names))
	for i, n := range names {
		quoted[i] = quote(n)
	}
	return strings.Join(quoted, ", ")
}

// interval writes a width in hours, minutes or seconds, never days. Adding
// INTERVAL '1 day' to a timestamptz steps a calendar day in the session's
// zone, which is 23 or 25 hours across a daylight saving change.
func interval(d time.Duration) string {
	switch {
	case d%time.Hour == 0:
		return "INTERVAL '" + strconv.FormatInt(int64(d/time.Hour), 10) + " hours'"
	case d%time.Minute == 0:
		return "INTERVAL '" + strconv.FormatInt(int64(d/time.Minute), 10) + " minutes'"
	default:
		return "INTERVAL '" + strconv.FormatInt(int64(d/time.Second), 10) + " seconds'"
	}
}

// bin puts expr in its bucket of the given width.
func bin(width time.Duration, expr string) string {
	return "date_bin(" + interval(width) + ", " + expr + ", " + origin + ")"
}

// keyColumns are a table's key: the time column, then the set's dimensions in
// declared order.
func keyColumns(r config.Rollup, set config.RollupDimensionSet) []string {
	return append([]string{r.Source.TimeColumn}, set.Dimensions...)
}

// sumCast is the type a sum is stored as. The tables are created from the
// query that fills them, so the cast is also the column's type.
func sumCast(m config.RollupMeasure) string {
	if m.Numeric == "double" {
		return "double precision"
	}
	return "bigint"
}

// lastExpr is the value of column in the latest of the finer rows aliased f.
// Nulls sort after every value, so a bucket whose latest row is null keeps
// the one before it, and a bucket of nulls stores null. Check admits last
// only in a set that keeps every source dimension, so a group holds one row
// per finer bucket and the order has no ties.
func lastExpr(r config.Rollup, column string) string {
	c := "f." + quote(column)
	return "(array_agg(" + c + " ORDER BY (" + c + " IS NULL), f." + quote(r.Source.TimeColumn) + " DESC))[1]"
}

// sourceExpr builds a measure's stored form from source rows aliased f.
func sourceExpr(r config.Rollup, m config.RollupMeasure) string {
	switch m.Type {
	case "sum":
		return "sum(f." + quote(m.Column) + ")::" + sumCast(m)
	case "min":
		return "min(f." + quote(m.Column) + ")"
	case "max":
		return "max(f." + quote(m.Column) + ")"
	case "last":
		return lastExpr(r, m.Column)
	case "count_buckets":
		return "count(DISTINCT f." + quote(r.Source.TimeColumn) + ")"
	}
	panic(fmt.Sprintf("rollup: no source expression for measure type %q; Check admits only generated types", m.Type))
}

// mergeExpr merges a measure's stored form from finer rollup rows aliased f.
// count_buckets sums, because source buckets never overlap in time. Every
// table of a set names its time column as the source does, so last orders by
// the same column at every grain.
func mergeExpr(r config.Rollup, name string, m config.RollupMeasure) string {
	switch m.Type {
	case "sum":
		return "sum(f." + quote(name) + ")::" + sumCast(m)
	case "count_buckets":
		return "sum(f." + quote(name) + ")::bigint"
	case "min":
		return "min(f." + quote(name) + ")"
	case "max":
		return "max(f." + quote(name) + ")"
	case "last":
		return lastExpr(r, name)
	}
	panic(fmt.Sprintf("rollup: no merge expression for measure type %q; Check admits only generated types", m.Type))
}

// selectList is the SELECT list that fills a table of set at grain: the
// bucket, the dimensions, then each measure, built from the source when the
// grain is built from the source grain and merged otherwise.
func selectList(r config.Rollup, set config.RollupDimensionSet, grain config.RollupLevel) []string {
	cols := []string{bin(grain.Width, "f."+quote(r.Source.TimeColumn))}
	for _, d := range set.Dimensions {
		cols = append(cols, "f."+quote(d))
	}
	for _, name := range set.MeasureNames() {
		m := set.Measures[name]
		if grain.From == r.Source.Grain {
			cols = append(cols, sourceExpr(r, m))
		} else {
			cols = append(cols, mergeExpr(r, name, m))
		}
	}
	return cols
}

// groupBy is "1, 2, …, n".
func groupBy(n int) string {
	pos := make([]string, n)
	for i := range pos {
		pos[i] = strconv.Itoa(i + 1)
	}
	return strings.Join(pos, ", ")
}

// lockSQL takes a transaction advisory lock on every bucket of table that the
// changed rows touch, in bucket order, before the re-merge reads. Without it
// two overlapping writers each re-merge from a snapshot missing the other's
// row, and the later commit overwrites the earlier one's count. The key is
// the bucket's epoch: a bucket's text renders in the session's TimeZone, and
// writers in two zones would lock two keys for one bucket. The migration's
// backfill sets sqlflow.rollup_backfill: it holds the source table's lock, and
// a lock per bucket of a long history would exhaust the lock table.
func lockSQL(table string, width time.Duration, timeColumn string) string {
	return "  IF current_setting('sqlflow.rollup_backfill', true) IS DISTINCT FROM 'on' THEN\n" +
		"    PERFORM pg_advisory_xact_lock(hashtextextended('" + table + ":' || extract(epoch FROM touched.b)::bigint, 0))\n" +
		"    FROM (SELECT DISTINCT " + bin(width, quote(timeColumn)) + " AS b FROM changed ORDER BY 1) AS touched;\n" +
		"  END IF;\n"
}
