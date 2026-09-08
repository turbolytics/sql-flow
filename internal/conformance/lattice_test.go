package conformance

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

// DuckDB's ADBC output names a list's child "l"; arrow.ListOf names it
// "item". Both are the same logical type, and a key that told them apart
// would match the constructor and miss the engine -- which is the only one
// whose types reach a sink.
func TestToolingConformanceCanonicalKey_IgnoresChildNamesAndNullability(t *testing.T) {
	coverage.Covers(t, "tooling.conformance")

	duckdbShape := arrow.ListOfField(
		arrow.Field{Name: "l", Type: arrow.PrimitiveTypes.Int32, Nullable: true})
	goShape := arrow.ListOf(arrow.PrimitiveTypes.Int32)

	assert.That(t, duckdbShape.String() != goShape.String())
	assert.Equal(t, CanonicalKey(duckdbShape), CanonicalKey(goShape))
	assert.Equal(t, CanonicalKey(goShape), "list<int32>")
}

// The zone is the DuckDB session's, not the type's. Pinning it into the key
// would make the declaration depend on the host that ran the test.
func TestToolingConformanceCanonicalKey_WildcardsTheTimestampZone(t *testing.T) {
	zoned := &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "Asia/Tokyo"}
	bare := &arrow.TimestampType{Unit: arrow.Microsecond}

	assert.Equal(t, CanonicalKey(zoned), "timestamp[us, tz=*]")
	assert.Equal(t, CanonicalKey(bare), "timestamp[us]")
}

// Precision and scale are the user's choice and do not change which branch of
// a sink's conversion runs, so one key covers the family.
func TestToolingConformanceCanonicalKey_CollapsesTheDecimalFamily(t *testing.T) {
	p38 := &arrow.Decimal128Type{Precision: 38, Scale: 0}
	p18 := &arrow.Decimal128Type{Precision: 18, Scale: 3}

	assert.Equal(t, CanonicalKey(p38), "decimal(*, *)")
	assert.Equal(t, CanonicalKey(p18), "decimal(*, *)")
}

func TestToolingConformanceCanonicalKey_NamesEachConstructor(t *testing.T) {
	cases := map[string]arrow.DataType{
		"int64":                     arrow.PrimitiveTypes.Int64,
		"utf8":                      arrow.BinaryTypes.String,
		"date32":                    arrow.FixedWidthTypes.Date32,
		"struct":                    arrow.StructOf(arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Int32, Nullable: true}),
		"map":                       arrow.MapOf(arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int32),
		"dictionary":                &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Uint8, ValueType: arrow.BinaryTypes.String},
		"list<list<int64>>":         arrow.ListOf(arrow.ListOf(arrow.PrimitiveTypes.Int64)),
		"fixed_size_list<int32>[3]": arrow.FixedSizeListOf(3, arrow.PrimitiveTypes.Int32),
	}
	for want, dt := range cases {
		if got := CanonicalKey(dt); got != want {
			t.Errorf("CanonicalKey(%s) = %q, want %q", dt, got, want)
		}
	}
}
