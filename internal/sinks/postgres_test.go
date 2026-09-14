package sinks

import (
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

func TestSinkPostgres_TableNameParses(t *testing.T) {
	coverage.Covers(t, "sink.postgres")
	id, err := parsePostgresTable("rollups")
	assert.NoError(t, err)
	assert.DeepEqual(t, pgx.Identifier{"rollups"}, id)

	id, err = parsePostgresTable("analytics.rollups")
	assert.NoError(t, err)
	assert.DeepEqual(t, pgx.Identifier{"analytics", "rollups"}, id)

	for _, bad := range []string{"", "a.b.c", ".t", "t.", `pub"lic.t`} {
		_, err := parsePostgresTable(bad)
		assert.Error(t, err)
	}
}

// The staging table has the batch's columns with the target's types and
// nothing else, plus the sequence the merge orders by.
func TestSinkPostgres_StagingDDLTakesTheTargetsTypes(t *testing.T) {
	coverage.Covers(t, "sink.postgres")
	got := postgresStagingSQL(pgx.Identifier{"public", "t"}, []string{"bucket", "lang", "posts"})
	assert.DeepEqual(t, []string{
		`DROP TABLE IF EXISTS sqlflow_staging`,
		`CREATE TEMP TABLE sqlflow_staging ON COMMIT DELETE ROWS AS SELECT "bucket", "lang", "posts" FROM "public"."t" WITH NO DATA`,
		`ALTER TABLE sqlflow_staging ADD COLUMN __seq bigint`,
	}, got)
}

// upsert names the batch's columns and no others, resolves two rows with one
// key to the last one in the batch, and updates every non-key column.
func TestSinkPostgres_UpsertMergeIsLastRowWins(t *testing.T) {
	coverage.Covers(t, "sink.postgres")
	got := postgresMergeSQL(pgx.Identifier{"t"}, PostgresModeUpsert,
		[]string{"bucket", "lang"}, []string{"bucket", "lang", "posts", "updated_at"})
	assert.Equal(t, `INSERT INTO "t" ("bucket", "lang", "posts", "updated_at") `+
		`SELECT DISTINCT ON ("bucket", "lang") "bucket", "lang", "posts", "updated_at" FROM sqlflow_staging `+
		`ORDER BY "bucket", "lang", __seq DESC `+
		`ON CONFLICT ("bucket", "lang") DO UPDATE SET "posts" = EXCLUDED."posts", "updated_at" = EXCLUDED."updated_at"`, got)
}

// A batch whose only columns are the key has nothing to update.
func TestSinkPostgres_UpsertOfKeyOnlyDoesNothingOnConflict(t *testing.T) {
	coverage.Covers(t, "sink.postgres")
	got := postgresMergeSQL(pgx.Identifier{"t"}, PostgresModeUpsert, []string{"id"}, []string{"id"})
	assert.That(t, strings.HasSuffix(got, `ON CONFLICT ("id") DO NOTHING`))
}

func TestSinkPostgres_AppendMergeKeepsBatchOrder(t *testing.T) {
	coverage.Covers(t, "sink.postgres")
	got := postgresMergeSQL(pgx.Identifier{"s", "t"}, PostgresModeAppend, nil, []string{"a", "b"})
	assert.Equal(t, `INSERT INTO "s"."t" ("a", "b") SELECT "a", "b" FROM sqlflow_staging ORDER BY __seq`, got)
}
