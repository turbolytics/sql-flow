package arrowfmt

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/zeebo/assert"
)

var instant = time.Date(2026, 9, 8, 12, 0, 0, 123456789, time.UTC)

func tsType(unit arrow.TimeUnit, zone string) *arrow.TimestampType {
	return &arrow.TimestampType{
		Unit:     unit,
		TimeZone: zone,
	}
}

func tsArray(t *testing.T, typ *arrow.TimestampType, at ...*time.Time) arrow.Array {
	t.Helper()
	b := array.NewTimestampBuilder(memory.DefaultAllocator, typ)
	defer b.Release()
	for _, v := range at {
		if v == nil {
			b.AppendNull()
			continue
		}
		ts, err := arrow.TimestampFromTime(*v, typ.Unit)
		assert.NoError(t, err)
		b.Append(ts)
	}
	return b.NewArray()
}

func marshal(t *testing.T, col arrow.Array, i int) string {
	t.Helper()
	b, err := Marshal(col, i)
	assert.NoError(t, err)
	return string(b)
}

func TestArrowfmtTimestamp_NaiveRendersAsUTCInstant(t *testing.T) {
	for unit, want := range map[arrow.TimeUnit]string{
		arrow.Second:      `"2026-09-08T12:00:00Z"`,
		arrow.Millisecond: `"2026-09-08T12:00:00.123Z"`,
		arrow.Microsecond: `"2026-09-08T12:00:00.123456Z"`,
		arrow.Nanosecond:  `"2026-09-08T12:00:00.123456789Z"`,
	} {
		col := tsArray(t, tsType(unit, ""), &instant)
		assert.Equal(t, want, marshal(t, col, 0))
	}
}

func TestArrowfmtTimestamp_ZonedRendersInItsOwnZone(t *testing.T) {
	naive := tsArray(t, tsType(arrow.Microsecond, ""), &instant)
	tokyo := tsArray(t, tsType(arrow.Microsecond, "Asia/Tokyo"), &instant)

	assert.Equal(t, `"2026-09-08T12:00:00.123456Z"`, marshal(t, naive, 0))
	assert.Equal(t, `"2026-09-08T21:00:00.123456+09:00"`, marshal(t, tokyo, 0))
}

func TestArrowfmtTimestamp_OutOfRangeYearDoesNotError(t *testing.T) {
	for _, year := range []int{10000, -1} {
		at := time.Date(year, 1, 1, 0, 0, 0, 0, time.UTC)
		col := tsArray(t, tsType(arrow.Second, ""), &at)
		assert.True(t, len(marshal(t, col, 0)) > 2)
	}
}

func TestArrowfmtText_ATimestampMatchesItsJSON(t *testing.T) {
	col := tsArray(t, tsType(arrow.Microsecond, ""), &instant, nil)

	assert.Equal(t, "2026-09-08T12:00:00.123456Z", Text(col, 0))
	assert.Equal(t, array.NullValueStr, Text(col, 1))
}

func TestArrowfmtNested_NullTimestampsRenderNull(t *testing.T) {
	typ := tsType(arrow.Millisecond, "")
	mem := memory.DefaultAllocator

	lb := array.NewListBuilder(mem, typ)
	lb.Append(true)
	lb.ValueBuilder().(*array.TimestampBuilder).AppendNull()
	lb.ValueBuilder().(*array.TimestampBuilder).Append(arrow.Timestamp(instant.UnixMilli()))
	list := lb.NewArray()
	assert.Equal(t, `[null,"2026-09-08T12:00:00.123Z"]`, marshal(t, list, 0))

	sb := array.NewStructBuilder(mem, arrow.StructOf(arrow.Field{Name: "ts", Type: typ, Nullable: true}))
	sb.Append(true)
	sb.FieldBuilder(0).(*array.TimestampBuilder).AppendNull()
	st := sb.NewArray()
	assert.Equal(t, `{"ts":null}`, marshal(t, st, 0))

	mb := array.NewMapBuilder(mem, arrow.BinaryTypes.String, typ, false)
	mb.Append(true)
	mb.KeyBuilder().(*array.StringBuilder).Append("k")
	mb.ItemBuilder().(*array.TimestampBuilder).AppendNull()
	m := mb.NewArray()
	assert.Equal(t, `[{"key":"k","value":null}]`, marshal(t, m, 0))
}

func TestArrowfmtNested_ListOfTimestampsRendersEach(t *testing.T) {
	lb := array.NewListBuilder(memory.DefaultAllocator, tsType(arrow.Millisecond, ""))
	lb.Append(true)
	for range 3 {
		lb.ValueBuilder().(*array.TimestampBuilder).Append(arrow.Timestamp(instant.UnixMilli()))
	}
	list := lb.NewArray()

	assert.Equal(t,
		`["2026-09-08T12:00:00.123Z","2026-09-08T12:00:00.123Z","2026-09-08T12:00:00.123Z"]`,
		marshal(t, list, 0))
}

func TestArrowfmtNested_RendersEveryDepth(t *testing.T) {
	elem := arrow.StructOf(arrow.Field{Name: "ts", Type: tsType(arrow.Millisecond, ""), Nullable: true})
	lb := array.NewListBuilder(memory.DefaultAllocator, elem)
	lb.Append(true)
	sb := lb.ValueBuilder().(*array.StructBuilder)
	for range 2 {
		sb.Append(true)
		sb.FieldBuilder(0).(*array.TimestampBuilder).Append(arrow.Timestamp(instant.UnixMilli()))
	}
	list := lb.NewArray()

	assert.Equal(t,
		`[{"ts":"2026-09-08T12:00:00.123Z"},{"ts":"2026-09-08T12:00:00.123Z"}]`,
		marshal(t, list, 0))
}

func TestArrowfmtNested_DictionaryRendersValuesNotIndexes(t *testing.T) {
	values := tsArray(t, tsType(arrow.Millisecond, ""), &instant)
	idx := array.NewInt8Builder(memory.DefaultAllocator)
	idx.AppendValues([]int8{0, 0}, nil)
	indices := idx.NewArray()
	dict := array.NewDictionaryArray(&arrow.DictionaryType{
		IndexType: arrow.PrimitiveTypes.Int8,
		ValueType: values.DataType(),
	}, indices, values)

	assert.Equal(t, `"2026-09-08T12:00:00.123Z"`, marshal(t, dict, 1))
}

func TestArrowfmtValue_TypesWithoutATimestampMatchArrow(t *testing.T) {
	mem := memory.DefaultAllocator
	build := func(dt arrow.DataType, fill func(b array.Builder)) arrow.Array {
		b := array.NewBuilder(mem, dt)
		defer b.Release()
		fill(b)
		return b.NewArray()
	}
	cols := map[string]arrow.Array{
		"int64": build(arrow.PrimitiveTypes.Int64, func(b array.Builder) { b.(*array.Int64Builder).Append(7) }),
		"float64": build(arrow.PrimitiveTypes.Float64, func(b array.Builder) {
			b.(*array.Float64Builder).Append(1.5)
		}),
		"string": build(arrow.BinaryTypes.String, func(b array.Builder) { b.(*array.StringBuilder).Append("a") }),
		"bool":   build(arrow.FixedWidthTypes.Boolean, func(b array.Builder) { b.(*array.BooleanBuilder).Append(true) }),
		"decimal": build(&arrow.Decimal128Type{Precision: 10, Scale: 2}, func(b array.Builder) {
			b.(*array.Decimal128Builder).Append(decimal128.FromI64(1234))
		}),
		"date32": build(arrow.FixedWidthTypes.Date32, func(b array.Builder) { b.(*array.Date32Builder).Append(20000) }),
		"time64": build(arrow.FixedWidthTypes.Time64us, func(b array.Builder) { b.(*array.Time64Builder).Append(1000) }),
		"binary": build(arrow.BinaryTypes.Binary, func(b array.Builder) { b.(*array.BinaryBuilder).Append([]byte("xy")) }),
		"list": build(arrow.ListOf(arrow.PrimitiveTypes.Int64), func(b array.Builder) {
			lb := b.(*array.ListBuilder)
			lb.Append(true)
			lb.ValueBuilder().(*array.Int64Builder).AppendValues([]int64{1, 2}, nil)
		}),
		"struct": build(arrow.StructOf(arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Int64}), func(b array.Builder) {
			sb := b.(*array.StructBuilder)
			sb.Append(true)
			sb.FieldBuilder(0).(*array.Int64Builder).Append(3)
		}),
		"map": build(arrow.MapOf(arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int64), func(b array.Builder) {
			mb := b.(*array.MapBuilder)
			mb.Append(true)
			mb.KeyBuilder().(*array.StringBuilder).Append("k")
			mb.ItemBuilder().(*array.Int64Builder).Append(1)
		}),
	}
	for name, col := range cols {
		t.Run(name, func(t *testing.T) {
			want, err := json.Marshal(col.GetOneForMarshal(0))
			assert.NoError(t, err)
			assert.Equal(t, string(want), marshal(t, col, 0))
		})
	}
}

func TestArrowfmtRecordRow_MatchesRecordToJSON(t *testing.T) {
	mem := memory.DefaultAllocator
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "zeta", Type: arrow.PrimitiveTypes.Int64},
		{Name: "alpha", Type: arrow.BinaryTypes.String},
		{Name: "mid", Type: arrow.FixedWidthTypes.Boolean},
	}, nil)
	rb := array.NewRecordBuilder(mem, schema)
	defer rb.Release()
	for i := range 3 {
		rb.Field(0).(*array.Int64Builder).Append(int64(i))
		rb.Field(1).(*array.StringBuilder).Append("s")
		rb.Field(2).(*array.BooleanBuilder).Append(i%2 == 0)
	}
	rec := rb.NewRecordBatch()

	var want bytes.Buffer
	assert.NoError(t, array.RecordToJSON(rec, &want))
	lines := strings.Split(strings.TrimRight(want.String(), "\n"), "\n")

	for i, line := range lines {
		got, err := RecordRow(rec, i)
		assert.NoError(t, err)
		assert.Equal(t, line, string(got))
	}
}
