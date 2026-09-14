package sinks

import (
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/turbolytics/sql-flow/internal/errs"
)

// The Postgres sink's SQL. Pure functions of the config and the batch's
// columns, so the statements are tested without a server.

const (
	PostgresModeUpsert = "upsert"
	PostgresModeAppend = "append"

	// postgresStaging is the session temp table every flush copies into.
	// Temp tables live in a per-session schema, so two sinks on two
	// connections never collide, and the table dies with the connection.
	postgresStaging = "sqlflow_staging"
	// postgresSeq is the row's position in the batch, which is what "last
	// row wins" orders by.
	postgresSeq = "__seq"
)

// parsePostgresTable reads "table" or "schema.table" into a quoted
// identifier. Anything else is refused: a name with three parts or a quote
// in it is a config error, not something to pass to the server and see.
func parsePostgresTable(name string) (pgx.Identifier, error) {
	parts := strings.Split(name, ".")
	if len(parts) > 2 {
		return nil, errs.New(errs.CodeSinkInvalid, "postgres sink: table %q has more than one dot; use table or schema.table", name)
	}
	for _, p := range parts {
		if p == "" || strings.ContainsAny(p, `"`) {
			return nil, errs.New(errs.CodeSinkInvalid, "postgres sink: table %q is not a table name; use table or schema.table, unquoted", name)
		}
	}
	return pgx.Identifier(parts), nil
}

// quoteColumns renders a column list for a statement.
func quoteColumns(cols []string) string {
	quoted := make([]string, len(cols))
	for i, c := range cols {
		quoted[i] = pgx.Identifier{c}.Sanitize()
	}
	return strings.Join(quoted, ", ")
}

// postgresStagingSQL creates the staging table for one column set. It has
// exactly the batch's columns with the target's types and none of the
// target's constraints or defaults, so the merge is the only place a row is
// judged and the error names the target. ON COMMIT DELETE ROWS empties it at
// every commit.
func postgresStagingSQL(target pgx.Identifier, cols []string) []string {
	return []string{
		"DROP TABLE IF EXISTS " + postgresStaging,
		fmt.Sprintf("CREATE TEMP TABLE %s ON COMMIT DELETE ROWS AS SELECT %s FROM %s WITH NO DATA",
			postgresStaging, quoteColumns(cols), target.Sanitize()),
		fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s bigint", postgresStaging, postgresSeq),
	}
}

// postgresMergeSQL moves the staging table into the target.
//
// upsert resolves two rows with one key to the last one in the batch before
// the INSERT sees them, because ON CONFLICT DO UPDATE refuses to touch a row
// twice in one statement (21000), and a CDC batch carrying two updates for
// one id is ordinary. The INSERT names the batch's columns and no others, so
// a column the batch omits takes the table's default on insert and keeps its
// value on update.
func postgresMergeSQL(target pgx.Identifier, mode string, key, cols []string) string {
	list := quoteColumns(cols)
	if mode == PostgresModeAppend {
		return fmt.Sprintf("INSERT INTO %s (%s) SELECT %s FROM %s ORDER BY %s",
			target.Sanitize(), list, list, postgresStaging, postgresSeq)
	}
	keyList := quoteColumns(key)
	var sets []string
	for _, c := range cols {
		if containsString(key, c) {
			continue
		}
		q := pgx.Identifier{c}.Sanitize()
		sets = append(sets, q+" = EXCLUDED."+q)
	}
	action := "DO NOTHING"
	if len(sets) > 0 {
		action = "DO UPDATE SET " + strings.Join(sets, ", ")
	}
	return fmt.Sprintf("INSERT INTO %s (%s) SELECT DISTINCT ON (%s) %s FROM %s ORDER BY %s, %s DESC ON CONFLICT (%s) %s",
		target.Sanitize(), list, keyList, list, postgresStaging, keyList, postgresSeq, keyList, action)
}

func containsString(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

// The catalog queries the probe runs. $1 is the target as regclass text,
// e.g. "public"."t".
const (
	postgresTableExistsSQL = `SELECT to_regclass($1::text) IS NOT NULL`

	postgresColumnsSQL = `SELECT attname::text FROM pg_attribute
WHERE attrelid = $1::text::regclass AND attnum > 0 AND NOT attisdropped ORDER BY attnum`

	// A unique index or constraint whose columns are exactly $2, in any
	// order. A partial index (indpred) or an expression index (indexprs) is
	// not a conflict target for ON CONFLICT (<columns>), so neither counts.
	postgresUniqueIndexSQL = `SELECT EXISTS (
  SELECT 1 FROM pg_index i
  WHERE i.indrelid = $1::text::regclass AND i.indisunique
    AND i.indpred IS NULL AND i.indexprs IS NULL
    AND (SELECT array_agg(a.attname::text ORDER BY a.attname)
         FROM unnest(i.indkey) WITH ORDINALITY AS k(attnum, ord)
         JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum) = $2::text[])`

	postgresAnyUniqueIndexSQL = `SELECT EXISTS (
  SELECT 1 FROM pg_index WHERE indrelid = $1::text::regclass AND indisunique)`
)
