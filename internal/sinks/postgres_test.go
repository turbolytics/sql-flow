package sinks

import (
	"context"
	"errors"
	"net"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/turbolytics/sql-flow/internal/config"
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

// The SQLSTATE class is the whole retry policy: connection, shutdown,
// resource and serialization classes retry as unreachable; data, constraint,
// syntax, auth and missing-object classes are the user's and never retry.
func TestSinkPostgres_SQLStateClassifies(t *testing.T) {
	coverage.Covers(t, "sink.postgres")
	cases := []struct {
		state string
		code  errs.Code
		exit  int
	}{
		{"08006", errs.CodeSinkUnreachable, errs.ExitSinkUnreachable},
		{"57P01", errs.CodeSinkUnreachable, errs.ExitSinkUnreachable},
		{"53300", errs.CodeSinkUnreachable, errs.ExitSinkUnreachable},
		{"40001", errs.CodeSinkUnreachable, errs.ExitSinkUnreachable},
		{"22003", errs.CodeSinkEncodeFailed, errs.ExitUserError},
		{"23502", errs.CodeSinkInvalid, errs.ExitUserError},
		{"21000", errs.CodeSinkInvalid, errs.ExitUserError},
		{"42703", errs.CodeSinkInvalid, errs.ExitUserError},
		{"28P01", errs.CodeSinkInvalid, errs.ExitUserError},
		{"3D000", errs.CodeSinkInvalid, errs.ExitUserError},
		{"XX000", errs.CodeSinkWriteFailed, errs.ExitInternal},
	}
	for _, c := range cases {
		err := postgresError(&pgconn.PgError{Code: c.state, Message: "m"}, "merge")
		assert.Equal(t, c.code, errs.CodeOf(err))
		assert.Equal(t, c.exit, errs.ExitCode(err))
		assert.That(t, strings.Contains(err.Error(), c.state))
	}

	// A dial failure is unreachable, and a context that ran out stays
	// visible through the wrap so the harness can see the deadline.
	err := postgresError(&net.OpError{Op: "dial", Err: syscall.ECONNREFUSED}, "connect")
	assert.Equal(t, errs.CodeSinkUnreachable, errs.CodeOf(err))
	err = postgresError(context.DeadlineExceeded, "copy")
	assert.Equal(t, errs.CodeSinkUnreachable, errs.CodeOf(err))
	assert.That(t, errors.Is(err, context.DeadlineExceeded))

	// pgx refusing to encode a Go value is the client's, and permanent.
	err = postgresCopyError(errors.New("unable to encode 1.5 into binary format for int8"))
	assert.Equal(t, errs.CodeSinkEncodeFailed, errs.CodeOf(err))
	assert.That(t, !retryable(err))

	// The same refusal after the COPY started reaches the sink as the
	// server's 57014. Measured against postgres:16 with 1<<40 into smallint.
	err = postgresCopyError(&pgconn.PgError{Code: "57014",
		Message: "COPY from stdin failed: unable to encode 1099511627776 into binary format for int2 (OID 21)"})
	assert.Equal(t, errs.CodeSinkEncodeFailed, errs.CodeOf(err))
	assert.Equal(t, errs.ExitUserError, errs.ExitCode(err))
	assert.That(t, !retryable(err))

	// A statement the server cancelled for any other reason keeps the class
	// rule: it may succeed on the next attempt.
	err = postgresCopyError(&pgconn.PgError{Code: "57014", Message: "canceling statement due to statement timeout"})
	assert.Equal(t, errs.CodeSinkUnreachable, errs.CodeOf(err))
}

func TestSinkPostgres_NewChecksTheBlock(t *testing.T) {
	coverage.Covers(t, "sink.postgres")
	good := config.PostgresSink{DSN: "postgres://u:p@localhost:5432/db", Table: "t", Mode: "upsert", Key: []string{"id"}}
	s, err := NewPostgresSink(good)
	assert.NoError(t, err)
	assert.DeepEqual(t, []string{"id"}, s.Key())

	for name, bad := range map[string]config.PostgresSink{
		"no dsn":           {Table: "t", Mode: "upsert", Key: []string{"id"}},
		"no table":         {DSN: good.DSN, Mode: "upsert", Key: []string{"id"}},
		"no mode":          {DSN: good.DSN, Table: "t", Key: []string{"id"}},
		"bad mode":         {DSN: good.DSN, Table: "t", Mode: "merge", Key: []string{"id"}},
		"upsert no key":    {DSN: good.DSN, Table: "t", Mode: "upsert"},
		"append with key":  {DSN: good.DSN, Table: "t", Mode: "append", Key: []string{"id"}},
		"duplicate key":    {DSN: good.DSN, Table: "t", Mode: "upsert", Key: []string{"id", "id"}},
		"bad dsn":          {DSN: "postgres://[::1", Table: "t", Mode: "append"},
		"three-part table": {DSN: good.DSN, Table: "a.b.c", Mode: "append"},
	} {
		_, err := NewPostgresSink(bad)
		if err == nil {
			t.Fatalf("%s: built", name)
		}
		assert.Equal(t, errs.CodeSinkInvalid, errs.CodeOf(err))
		// The examples test reads these substrings as a parity failure.
		assert.That(t, !strings.Contains(err.Error(), "not supported"))
		assert.That(t, !strings.Contains(err.Error(), "requires a"))
	}

	a, err := NewPostgresSink(config.PostgresSink{DSN: good.DSN, Table: "t", Mode: "append"})
	assert.NoError(t, err)
	assert.Equal(t, 0, len(a.Key()))
}

// New dials nothing: a sink pointed at a host that does not resolve builds,
// and Probe is where the pipeline learns the destination is not there.
func TestSinkPostgres_NewDoesNotDial(t *testing.T) {
	coverage.Covers(t, "sink.postgres")
	s, err := NewPostgresSink(config.PostgresSink{DSN: "postgres://u:p@no-such-host.invalid:5432/db", Table: "t", Mode: "append"})
	assert.NoError(t, err)
	assert.Equal(t, 0, s.BufferedRows())
	assert.NoError(t, s.Close())
	assert.NoError(t, s.Close())
}

// WriteTable buffers and Flush of nothing is a noop, without a server.
func TestSinkPostgres_WriteTableBuffersOnly(t *testing.T) {
	coverage.Covers(t, "sink.postgres")
	s, err := NewPostgresSink(config.PostgresSink{DSN: "postgres://u:p@no-such-host.invalid:5432/db", Table: "t", Mode: "append"})
	assert.NoError(t, err)
	assert.NoError(t, s.Flush(context.Background()))

	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)
	b := array.NewRecordBuilder(memory.NewGoAllocator(), schema)
	defer b.Release()
	b.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2}, nil)
	rec := b.NewRecord()
	defer rec.Release()
	tbl := array.NewTableFromRecords(schema, []arrow.Record{rec})
	defer tbl.Release()

	assert.NoError(t, s.WriteTable(context.Background(), tbl))
	assert.Equal(t, 2, s.BufferedRows())
}

// A dead-end host fails a flush with the retryable code, and the batch stays
// buffered for the retry.
func TestSinkPostgres_FlushAgainstNoServerIsUnreachableAndKeepsTheBatch(t *testing.T) {
	coverage.Covers(t, "sink.postgres")
	s, err := NewPostgresSink(config.PostgresSink{DSN: "postgres://u:p@127.0.0.1:1/db?connect_timeout=2", Table: "t", Mode: "append"})
	assert.NoError(t, err)
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)
	b := array.NewRecordBuilder(memory.NewGoAllocator(), schema)
	defer b.Release()
	b.Field(0).(*array.Int64Builder).Append(1)
	rec := b.NewRecord()
	defer rec.Release()
	tbl := array.NewTableFromRecords(schema, []arrow.Record{rec})
	defer tbl.Release()
	assert.NoError(t, s.WriteTable(context.Background(), tbl))

	err = s.Flush(context.Background())
	assert.Error(t, err)
	assert.Equal(t, errs.CodeSinkUnreachable, errs.CodeOf(err))
	assert.Equal(t, 1, s.BufferedRows())
}
