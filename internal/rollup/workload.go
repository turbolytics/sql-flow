package rollup

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"hash"
	"math/rand"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/errs"
)

// The generated workload's shape, per rollup.
const (
	workloadBatches = 20
	workloadMaxRows = 50
)

var (
	// The source buckets span 2026-03-08, when New York moves its clocks
	// forward. A trigger that bins in the session's zone puts rows in the
	// wrong day there.
	workloadFrom = time.Date(2026, 3, 7, 0, 0, 0, 0, time.UTC)
	workloadTo   = time.Date(2026, 3, 10, 0, 0, 0, 0, time.UTC)
	// Each batch runs in one of these session zones: UTC, a zone whose
	// offset changes inside the span, and a zone at a half-hour offset.
	workloadZones = []string{"UTC", "America/New_York", "Asia/Kolkata"}
	// textPool is every value a text column gets. Five values make keys
	// repeat, so rows share buckets. One character fits any varchar.
	textPool = []string{"a", "b", "c", "d", "e"}
)

// WorkloadReport is what the generated workload wrote to one rollup's
// source.
type WorkloadReport struct {
	Rollup  string
	Batches int
	Rows    int
	// Digest is a sha256 over every statement's zone and arguments in order.
	// One seed gives one digest.
	Digest string
	// FedBy names the rollup whose table is this rollup's source. Its rows
	// arrive through that rollup's triggers, so the workload writes none.
	FedBy string
}

// sourceColumn is one source column the workload writes.
type sourceColumn struct {
	Name string
	// The type as format_type prints it, and the cast its value binds with.
	Type    string
	NotNull bool
}

// RunWorkload writes a seeded workload to each rollup's source on the
// session's search_path, through the triggers: 20 batches of 1 to 50 rows,
// each one upsert in its own transaction and session zone, one row in ten
// rewriting a key an earlier batch wrote. A rollup whose source another
// rollup makes gets its rows through that rollup's triggers instead.
func RunWorkload(ctx context.Context, conn *pgx.Conn, conf *config.RollupsConf, seed int64) ([]WorkloadReport, error) {
	rng := rand.New(rand.NewSource(seed))
	made := madeBy(conf)
	var out []WorkloadReport
	for _, r := range conf.Rollups {
		if owner, ok := made[r.Source.Table]; ok {
			out = append(out, WorkloadReport{Rollup: r.Name, FedBy: owner})
			continue
		}
		rep, err := runWorkload(ctx, conn, r, rng)
		if err != nil {
			return nil, err
		}
		out = append(out, rep)
	}
	return out, nil
}

func runWorkload(ctx context.Context, conn *pgx.Conn, r config.Rollup, rng *rand.Rand) (WorkloadReport, error) {
	cols, err := writtenColumns(ctx, conn, r)
	if err != nil {
		return WorkloadReport{}, workloadError(err, r.Source.Table)
	}
	// Every type is checked before the first write, so a run that cannot
	// finish writes nothing.
	for _, c := range cols {
		if c.Name == r.Source.TimeColumn {
			continue
		}
		if err := writable(c); err != nil {
			return WorkloadReport{}, err
		}
	}
	width, err := config.ParseServeDuration(r.Source.Grain)
	if err != nil {
		return WorkloadReport{}, workloadError(err, r.Source.Table)
	}
	key := append([]string{r.Source.TimeColumn}, r.Source.Dimensions...)
	byName := map[string]sourceColumn{}
	for _, c := range cols {
		byName[c.Name] = c
	}
	first := floorTo(workloadFrom, width)
	if first.Before(workloadFrom) {
		first = first.Add(width)
	}
	buckets := max(int64(workloadTo.Sub(first)/width), 1)

	// earlier holds each key a committed batch wrote, in the order written,
	// so a seed picks the same rewrites every run.
	var earlier [][]any
	written := map[string]bool{}
	h := sha256.New()
	rep := WorkloadReport{Rollup: r.Name}
	for b := 0; b < workloadBatches; b++ {
		zone := workloadZones[rng.Intn(len(workloadZones))]
		loc, err := time.LoadLocation(zone)
		if err != nil {
			return WorkloadReport{}, workloadError(err, r.Source.Table)
		}
		// ON CONFLICT DO UPDATE refuses to touch a row twice in one
		// statement, so a key drawn twice in a batch is written once.
		inBatch := map[string]bool{}
		var keys [][]any
		var rows []map[string]any
		for n := rng.Intn(workloadMaxRows) + 1; n > 0; n-- {
			var k []any
			if len(earlier) > 0 && rng.Intn(10) == 0 {
				k = earlier[rng.Intn(len(earlier))]
			} else {
				k = []any{first.Add(time.Duration(rng.Int63n(buckets)) * width)}
				for _, d := range r.Source.Dimensions {
					c := byName[d]
					if !c.NotNull && rng.Intn(10) == 0 {
						k = append(k, nil)
						continue
					}
					v, err := generate(c, k[0].(time.Time), width, rng)
					if err != nil {
						return WorkloadReport{}, err
					}
					k = append(k, v)
				}
			}
			row := map[string]any{}
			for i, name := range key {
				row[name] = k[i]
			}
			for _, c := range cols {
				if _, ok := row[c.Name]; ok {
					continue
				}
				v, err := generate(c, k[0].(time.Time), width, rng)
				if err != nil {
					return WorkloadReport{}, err
				}
				row[c.Name] = v
			}
			if enc := encodeKey(k); !inBatch[enc] {
				inBatch[enc] = true
				keys = append(keys, k)
				rows = append(rows, row)
			}
		}

		var args []any
		for _, row := range rows {
			for _, c := range cols {
				args = append(args, bindText(row[c.Name], c, loc))
			}
		}
		digest(h, zone, args)
		if err := writeBatch(ctx, conn, zone, upsertSQL(r.Source.Table, cols, key, len(rows)), args); err != nil {
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && pgErr.Code == "42P10" {
				return WorkloadReport{}, errs.Wrap(errs.CodeConfigRollup, err,
					"rollup test: rollup %s: the sink's upsert needs a unique index on (%s) of %s, and the source has none",
					r.Name, strings.Join(key, ", "), r.Source.Table)
			}
			return WorkloadReport{}, workloadError(err, r.Source.Table)
		}
		for _, k := range keys {
			if enc := encodeKey(k); !written[enc] {
				written[enc] = true
				earlier = append(earlier, k)
			}
		}
		rep.Batches++
		rep.Rows += len(rows)
	}
	rep.Digest = hex.EncodeToString(h.Sum(nil))
	return rep, nil
}

// writtenColumns is the source's columns the workload writes, in table
// order: every column the triggers read, and every other NOT NULL column
// the database cannot fill itself with a default, an identity or a
// generation.
func writtenColumns(ctx context.Context, q querier, r config.Rollup) ([]sourceColumn, error) {
	read := map[string]bool{}
	for _, c := range sourceColumns(r) {
		read[c] = true
	}
	rows, err := q.Query(ctx, `SELECT a.attname, format_type(a.atttypid, a.atttypmod), a.attnotnull,
  a.atthasdef OR a.attidentity <> '' OR a.attgenerated <> ''
FROM pg_attribute a
WHERE a.attrelid = to_regclass($1) AND a.attnum > 0 AND NOT a.attisdropped
ORDER BY a.attnum`, quote(r.Source.Table))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []sourceColumn
	for rows.Next() {
		var c sourceColumn
		var filled bool
		if err := rows.Scan(&c.Name, &c.Type, &c.NotNull, &filled); err != nil {
			return nil, err
		}
		if read[c.Name] || (c.NotNull && !filled) {
			out = append(out, c)
		}
	}
	return out, rows.Err()
}

// writable reports a column type the workload cannot write.
func writable(c sourceColumn) error {
	switch {
	case c.Type == "smallint", c.Type == "integer", c.Type == "bigint",
		c.Type == "double precision",
		c.Type == "text", strings.HasPrefix(c.Type, "character varying"),
		c.Type == "timestamp with time zone", c.Type == "timestamp without time zone":
		return nil
	}
	return errs.New(errs.CodeConfigRollup,
		"rollup test: source column %s is %s, which the generated workload cannot write; it writes smallint, integer, bigint, double precision, text, character varying and timestamps",
		c.Name, c.Type)
}

// generate draws a value for c in a row whose source bucket is width wide
// from start: an integer from 0 to 1,000, a double from 0 to 1,000 with a
// fraction, text from the pool, or a timestamp inside the bucket.
// A timestamp stays a time.Time until bindText formats it in the batch's
// zone.
func generate(c sourceColumn, start time.Time, width time.Duration, rng *rand.Rand) (any, error) {
	if err := writable(c); err != nil {
		return nil, err
	}
	switch {
	case c.Type == "smallint", c.Type == "integer", c.Type == "bigint":
		return strconv.Itoa(rng.Intn(1001)), nil
	case c.Type == "double precision":
		// Never negative. Verify's tolerance is relative, and a sum of mixed
		// signs that lands near zero differs between a merge of finer sums
		// and a sum from scratch by more than 1e-9 of itself.
		return strconv.FormatFloat(rng.Float64()*1000, 'g', -1, 64), nil
	case c.Type == "text", strings.HasPrefix(c.Type, "character varying"):
		return textPool[rng.Intn(len(textPool))], nil
	}
	return start.Add(time.Duration(rng.Int63n(int64(width/time.Microsecond))) * time.Microsecond), nil
}

// bindText is v as the text the statement casts to c's type. A timestamptz
// is written with the batch zone's offset, so the instant is exact and the
// offset differs on either side of the change.
func bindText(v any, c sourceColumn, loc *time.Location) any {
	t, ok := v.(time.Time)
	if !ok {
		return v
	}
	if c.Type == "timestamp with time zone" {
		return t.In(loc).Format("2006-01-02T15:04:05.999999Z07:00")
	}
	return t.UTC().Format("2006-01-02 15:04:05.999999")
}

// upsertSQL is the sink's upsert of rows rows: every column binds as text
// cast to its type, and the columns outside key update from EXCLUDED.
func upsertSQL(table string, cols []sourceColumn, key []string, rows int) string {
	var b strings.Builder
	b.WriteString("INSERT INTO " + quote(table) + " (")
	for i, c := range cols {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(quote(c.Name))
	}
	b.WriteString(") VALUES ")
	n := 0
	for i := 0; i < rows; i++ {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString("(")
		for j, c := range cols {
			if j > 0 {
				b.WriteString(", ")
			}
			n++
			b.WriteString("$" + strconv.Itoa(n) + "::" + c.Type)
		}
		b.WriteString(")")
	}
	b.WriteString(" ON CONFLICT (" + quoteList(key) + ") ")
	var sets []string
	for _, c := range cols {
		if !slices.Contains(key, c.Name) {
			sets = append(sets, quote(c.Name)+" = EXCLUDED."+quote(c.Name))
		}
	}
	if len(sets) == 0 {
		b.WriteString("DO NOTHING")
	} else {
		b.WriteString("DO UPDATE SET " + strings.Join(sets, ", "))
	}
	return b.String()
}

// writeBatch runs one statement in its own transaction, in zone.
func writeBatch(ctx context.Context, conn *pgx.Conn, zone, sql string, args []any) error {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, "SELECT set_config('TimeZone', $1, true)", zone); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, sql, args...); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// encodeKey is k as a map key. NULL is its own value, as a NULLS NOT
// DISTINCT index treats it.
func encodeKey(k []any) string {
	var b strings.Builder
	for _, v := range k {
		switch v := v.(type) {
		case nil:
			b.WriteString("\x00N")
		case time.Time:
			b.WriteString("\x00T" + strconv.FormatInt(v.UnixMicro(), 10))
		case string:
			b.WriteString("\x00S" + v)
		}
	}
	return b.String()
}

// digest adds one statement's zone and arguments to h, each with its
// length, so no two sequences hash alike.
func digest(h hash.Hash, zone string, args []any) {
	write := func(s string) {
		_ = binary.Write(h, binary.BigEndian, int64(len(s)))
		h.Write([]byte(s))
	}
	write(zone)
	for _, a := range args {
		if a == nil {
			_ = binary.Write(h, binary.BigEndian, int64(-1))
			continue
		}
		write(a.(string))
	}
}

func workloadError(err error, table string) error {
	return errs.Wrap(errs.CodeRollupInternal, err, "rollup test: write the workload to %s", table)
}
