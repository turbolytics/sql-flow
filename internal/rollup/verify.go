package rollup

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/errs"
)

// verifyTolerance is how far apart two double sums may be, relative to the
// larger, and still match. Postgres sums floats in no fixed order, so a
// trigger's re-merge and verify's recompute can differ in the last bits of
// a correct sum.
const verifyTolerance = 1e-9

// maxDriftSample bounds the drifted rows one check returns, and so the
// daemon's log to 10 lines per table per pass.
const maxDriftSample = 10

// Target is one table verify checks against the table it is built from.
type Target struct {
	Rollup    config.Rollup
	Table     string
	BuiltFrom string
	// Retained is a table a later declaration removed. Its triggers still
	// run, so it is still checked.
	Retained bool
	// Skip says why verify leaves the table alone. Empty means check it.
	Skip string
	e    edge
}

func newTarget(r config.Rollup, e edge) Target {
	return Target{Rollup: r, Table: e.Table, BuiltFrom: e.From, e: e}
}

// DriftRow is one row where a table differs from what its from table makes.
type DriftRow struct {
	Bucket time.Time `json:"bucket_at"`
	// Key is the row's dimension values, such as "lang=en", and empty for a
	// set with none.
	Key string `json:"key"`
	// Kind is "missing", a row the from table makes that the table lacks;
	// "extra", a stored row nothing makes; or "differs".
	Kind string `json:"kind"`
	// Measure is the first measure that differs, and Stored and Recomputed
	// are its two values as text. A side the row lacks is empty.
	Measure    string `json:"measure"`
	Stored     string `json:"stored"`
	Recomputed string `json:"recomputed"`
}

// Verified is one table's check over the buckets in [From, To).
type Verified struct {
	Rollup    string
	Table     string
	BuiltFrom string
	From, To  time.Time
	// Buckets is how many buckets either side holds, and DriftBuckets how
	// many of them differ.
	Buckets      int64
	DriftBuckets int64
	// Sample is the first drifted rows, oldest first, at most
	// maxDriftSample.
	Sample []DriftRow
}

// measureDiffers is true when a measure's recomputed value w and stored
// value g differ.
func measureDiffers(m config.RollupMeasure, w, g string) string {
	if m.Type == "sum" && m.Numeric == "double" {
		return fmt.Sprintf("((%[1]s IS NULL) <> (%[2]s IS NULL) OR coalesce(abs(%[1]s - %[2]s) > %[3]g * greatest(abs(%[1]s), abs(%[2]s)), false))",
			w, g, verifyTolerance)
	}
	return w + " IS DISTINCT FROM " + g
}

// verifySQL is one statement that recomputes e's rows over the buckets in
// [$1, $2) from the table e is built from, pairs them with the stored rows
// on the table's key, and returns the buckets either side holds, the
// buckets that differ, and the first differing rows as JSON.
//
// One statement reads one snapshot, and a writer writes the source and
// every grain in one transaction, so a correct table never differs, even in
// its open bucket. Rows pair through GROUP BY rather than a full join:
// Postgres refuses a full join on IS NOT DISTINCT FROM, and grouping treats
// NULL dimension values as equal, as the unique index does.
func verifySQL(r config.Rollup, e edge) string {
	t := quote(r.Source.TimeColumn)
	keys := keyColumns(r, e.Set)
	measures := e.Set.MeasureNames()
	names := append(append([]string{}, keys...), measures...)

	cols := selectList(r, e.Set, e.Grain)
	for i := range cols {
		cols[i] += " AS " + quote(names[i])
	}
	pairs := []string{quoteList(keys), "bool_or(side = 'w') AS in_want", "bool_or(side = 'g') AS in_got"}
	var differs, measureArms, storedArms, recomputedArms []string
	for _, m := range measures {
		w, g := quote("w:"+m), quote("g:"+m)
		pairs = append(pairs,
			fmt.Sprintf("(array_agg(%s) FILTER (WHERE side = 'w'))[1] AS %s", quote(m), w),
			fmt.Sprintf("(array_agg(%s) FILTER (WHERE side = 'g'))[1] AS %s", quote(m), g))
		d := measureDiffers(e.Set.Measures[m], w, g)
		differs = append(differs, d)
		measureArms = append(measureArms, fmt.Sprintf("WHEN %s THEN '%s'", d, m))
		storedArms = append(storedArms, fmt.Sprintf("WHEN %s THEN %s::text", d, g))
		recomputedArms = append(recomputedArms, fmt.Sprintf("WHEN %s THEN %s::text", d, w))
	}
	key := "''"
	if len(e.Set.Dimensions) > 0 {
		parts := make([]string, len(e.Set.Dimensions))
		for i, d := range e.Set.Dimensions {
			parts[i] = fmt.Sprintf("'%s=' || coalesce(%s::text, 'null')", d, quote(d))
		}
		key = "concat_ws(', ', " + strings.Join(parts, ", ") + ")"
	}

	return fmt.Sprintf(`WITH want AS (
  SELECT %[1]s
  FROM %[2]s AS f
  WHERE f.%[3]s >= $1 AND f.%[3]s < $2
  GROUP BY %[4]s
), got AS (
  SELECT %[5]s FROM %[6]s WHERE %[3]s >= $1 AND %[3]s < $2
), pairs AS (
  SELECT %[7]s
  FROM (SELECT 'w' AS side, %[5]s FROM want UNION ALL SELECT 'g', %[5]s FROM got) AS u
  GROUP BY %[8]s
), diff AS (
  SELECT %[3]s AS bucket_at, %[9]s AS key,
         CASE WHEN NOT in_got THEN 'missing' WHEN NOT in_want THEN 'extra' ELSE 'differs' END AS kind,
         CASE %[10]s END AS measure,
         CASE %[11]s END AS stored,
         CASE %[12]s END AS recomputed
  FROM pairs
  WHERE NOT in_want OR NOT in_got OR %[13]s
)
SELECT (SELECT count(DISTINCT %[3]s) FROM pairs),
       (SELECT count(DISTINCT bucket_at) FROM diff),
       coalesce((SELECT jsonb_agg(s ORDER BY s.bucket_at, s.key)
                 FROM (SELECT * FROM diff ORDER BY bucket_at, key LIMIT %[14]d) AS s), '[]'::jsonb)
`,
		strings.Join(cols, ", "), quote(e.From), t, groupBy(len(keys)),
		quoteList(names), quote(e.Table),
		strings.Join(pairs, ", "), quoteList(keys), key,
		strings.Join(measureArms, " "), strings.Join(storedArms, " "), strings.Join(recomputedArms, " "),
		strings.Join(differs, " OR "), maxDriftSample)
}

// VerifyRange checks tg's table over the buckets in [lo, hi). lo and hi lie
// on the table's grain boundaries, so every bucket is whole.
func VerifyRange(ctx context.Context, conn *pgx.Conn, tg Target, lo, hi time.Time) (Verified, error) {
	v := Verified{Rollup: tg.Rollup.Name, Table: tg.Table, BuiltFrom: tg.BuiltFrom, From: lo, To: hi}
	var sample []byte
	if err := conn.QueryRow(ctx, verifySQL(tg.Rollup, tg.e), lo, hi).Scan(&v.Buckets, &v.DriftBuckets, &sample); err != nil {
		return v, verifyError(err, tg.Table)
	}
	if err := json.Unmarshal(sample, &v.Sample); err != nil {
		return v, verifyError(err, tg.Table)
	}
	return v, nil
}

func verifyError(err error, table string) error {
	return errs.Wrap(errs.CodeRollupInternal, err, "rollup verify %s", table)
}
