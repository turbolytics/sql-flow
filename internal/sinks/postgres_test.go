package sinks

import (
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/turbolytics/sql-flow/internal/errs"
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

// Every scalar the lattice names has a pgx value, nulls are nil, and a type
// with no conversion fails with the code the operator can act on.
func TestSinkPostgres_ScalarsConvert(t *testing.T) {
	coverage.Covers(t, "sink.postgres")
	mem := memory.NewGoAllocator()

	i8 := array.NewInt8Builder(mem)
	i8.Append(-128)
	i8.AppendNull()
	arr := i8.NewArray()
	defer arr.Release()
	v, err := postgresValue(arr, 0)
	assert.NoError(t, err)
	assert.Equal(t, int16(-128), v)
	v, err = postgresValue(arr, 1)
	assert.NoError(t, err)
	assert.Nil(t, v)

	u64 := array.NewUint64Builder(mem)
	u64.Append(7)
	u64.Append(18446744073709551615)
	uarr := u64.NewArray()
	defer uarr.Release()
	v, err = postgresValue(uarr, 0)
	assert.NoError(t, err)
	assert.Equal(t, int64(7), v)
	v, err = postgresValue(uarr, 1)
	assert.NoError(t, err)
	n, ok := v.(pgtype.Numeric)
	assert.That(t, ok)
	text, err := n.Value()
	assert.NoError(t, err)
	assert.Equal(t, "18446744073709551615", text)

	ts := array.NewTimestampBuilder(mem, &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "Asia/Tokyo"})
	ts.Append(arrow.Timestamp(time.Date(2026, 9, 8, 12, 0, 0, 123456000, time.UTC).UnixMicro()))
	tarr := ts.NewArray()
	defer tarr.Release()
	v, err = postgresValue(tarr, 0)
	assert.NoError(t, err)
	assert.Equal(t, time.Date(2026, 9, 8, 12, 0, 0, 123456000, time.UTC), v.(time.Time).UTC())

	d := array.NewDecimal128Builder(mem, &arrow.Decimal128Type{Precision: 10, Scale: 2})
	d.Append(decimal128.FromI64(123456))
	darr := d.NewArray()
	defer darr.Release()
	v, err = postgresValue(darr, 0)
	assert.NoError(t, err)
	text, err = v.(pgtype.Numeric).Value()
	assert.NoError(t, err)
	assert.Equal(t, "1234.56", text)
}

func TestSinkPostgres_UnsupportedTypeCarriesACode(t *testing.T) {
	coverage.Covers(t, "sink.postgres")
	b := array.NewTime64Builder(memory.NewGoAllocator(), &arrow.Time64Type{Unit: arrow.Microsecond})
	b.AppendNull()
	arr := b.NewArray()
	defer arr.Release()
	// A null of an unsupported type still fails: an all-null column is the
	// ordinary shape of a field the producer stopped sending.
	_, err := postgresValue(arr, 0)
	assert.Error(t, err)
	assert.Equal(t, errs.CodeSinkTypeUnsupported, errs.CodeOf(err))
}

// Containers go as JSON text rendered from the Arrow array, and an element
// the sink cannot convert fails from inside the container as it would alone.
func TestSinkPostgres_ContainersRenderAsJSON(t *testing.T) {
	coverage.Covers(t, "sink.postgres")
	mem := memory.NewGoAllocator()
	lb := array.NewListBuilder(mem, arrow.PrimitiveTypes.Int64)
	lb.Append(true)
	vb := lb.ValueBuilder().(*array.Int64Builder)
	vb.Append(1)
	vb.AppendNull()
	vb.Append(3)
	arr := lb.NewArray()
	defer arr.Release()
	v, err := postgresValue(arr, 0)
	assert.NoError(t, err)
	assert.Equal(t, "[1,null,3]", v)

	sb := array.NewStructBuilder(mem, arrow.StructOf(arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Int32, Nullable: true}))
	sb.Append(true)
	sb.FieldBuilder(0).(*array.Int32Builder).Append(7)
	sarr := sb.NewArray()
	defer sarr.Release()
	v, err = postgresValue(sarr, 0)
	assert.NoError(t, err)
	assert.Equal(t, `{"a":7}`, v)

	db := array.NewListBuilder(mem, &arrow.Decimal128Type{Precision: 38, Scale: 0})
	db.Append(true)
	darr := db.NewArray()
	defer darr.Release()
	_, err = postgresValue(darr, 0)
	assert.Error(t, err)
	assert.Equal(t, errs.CodeSinkTypeUnsupported, errs.CodeOf(err))
}

// postgresRows appends the row's position in the batch, which the merge
// orders by, and names the column in a conversion failure.
func TestSinkPostgres_RowsCarryTheSequence(t *testing.T) {
	coverage.Covers(t, "sink.postgres")
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{{Name: "k", Type: arrow.PrimitiveTypes.Int64}}, nil)
	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()
	b.Field(0).(*array.Int64Builder).AppendValues([]int64{10, 20}, nil)
	rec := b.NewRecord()
	defer rec.Release()
	tbl := array.NewTableFromRecords(schema, []arrow.Record{rec})
	defer tbl.Release()

	rows, err := postgresRows(tbl)
	assert.NoError(t, err)
	assert.DeepEqual(t, [][]any{{int64(10), int64(0)}, {int64(20), int64(1)}}, rows)

	bad := arrow.NewSchema([]arrow.Field{{Name: "when", Type: &arrow.Time64Type{Unit: arrow.Microsecond}}}, nil)
	bb := array.NewRecordBuilder(mem, bad)
	defer bb.Release()
	bb.Field(0).(*array.Time64Builder).Append(1)
	brec := bb.NewRecord()
	defer brec.Release()
	btbl := array.NewTableFromRecords(bad, []arrow.Record{brec})
	defer btbl.Release()
	_, err = postgresRows(btbl)
	assert.Error(t, err)
	assert.That(t, strings.Contains(err.Error(), `column "when"`))
}
