package conformance

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
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
	coverage.Covers(t, "tooling.conformance")

	zoned := &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "Asia/Tokyo"}
	bare := &arrow.TimestampType{Unit: arrow.Microsecond}

	assert.Equal(t, CanonicalKey(zoned), "timestamp[us, tz=*]")
	assert.Equal(t, CanonicalKey(bare), "timestamp[us]")
}

// Precision and scale are the user's choice and do not change which branch of
// a sink's conversion runs, so one key covers the family.
func TestToolingConformanceCanonicalKey_CollapsesTheDecimalFamily(t *testing.T) {
	coverage.Covers(t, "tooling.conformance")

	p38 := &arrow.Decimal128Type{Precision: 38, Scale: 0}
	p18 := &arrow.Decimal128Type{Precision: 18, Scale: 3}

	assert.Equal(t, CanonicalKey(p38), "decimal(*, *)")
	assert.Equal(t, CanonicalKey(p18), "decimal(*, *)")
}

func TestToolingConformanceCanonicalKey_NamesEachConstructor(t *testing.T) {
	coverage.Covers(t, "tooling.conformance")

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

// Two statements that must agree: lattice.yml says which types exist, and the
// value table says how to build one. Either alone leaves a type declared and
// never exercised, or exercised and never counted.
func TestToolingConformanceLattice_EveryDeclaredKeyHasAValue(t *testing.T) {
	coverage.Covers(t, "tooling.conformance")

	declared, err := coverage.Lattice()
	assert.NoError(t, err)

	built := map[string]bool{}
	for _, k := range LatticeKeys() {
		built[k] = true
	}
	inLattice := map[string]bool{}
	for _, e := range declared {
		inLattice[e.Key] = true
		if !built[e.Key] {
			t.Errorf("lattice.yml declares %q; the value table builds no array for it", e.Key)
		}
	}
	for k := range built {
		if !inLattice[k] {
			t.Errorf("the value table builds %q; lattice.yml does not declare it", k)
		}
	}
}

// The array the table builds must carry the type its key names, or a row is
// judged against a different type than the one it claims.
func TestToolingConformanceLattice_EveryValueCarriesItsOwnKey(t *testing.T) {
	coverage.Covers(t, "tooling.conformance")

	for _, k := range LatticeKeys() {
		arr, err := LatticeArray(k, false)
		if err != nil {
			t.Errorf("LatticeArray(%q): %v", k, err)
			continue
		}
		if got := CanonicalKey(arr.DataType()); got != k {
			t.Errorf("LatticeArray(%q) built a %s", k, got)
		}
		assert.Equal(t, arr.Len(), 1)
		assert.Equal(t, arr.IsNull(0), false)
		arr.Release()
	}
}

// type.null asks the same question of every type, so every key builds a null.
func TestToolingConformanceLattice_EveryKeyBuildsANull(t *testing.T) {
	coverage.Covers(t, "tooling.conformance")

	for _, k := range LatticeKeys() {
		arr, err := LatticeArray(k, true)
		if err != nil {
			t.Errorf("LatticeArray(%q, null): %v", k, err)
			continue
		}
		if arr.Len() != 1 {
			t.Errorf("LatticeArray(%q, null) has %d rows, want 1", k, arr.Len())
		}
		// A union carries no validity bitmap: the Arrow format puts a union's
		// nullness in the child the type code selects, so the union array
		// itself always reports not-null. Asserting otherwise would be
		// asserting against the format.
		if _, isUnion := arr.(array.Union); !isUnion && !arr.IsNull(0) {
			t.Errorf("LatticeArray(%q, null) does not report its row as null", k)
		}
		arr.Release()
	}
}

// The third null position. A sink that drops it, or silently substitutes a
// value for it, does so without an error, so only a direct test sees it.
func TestToolingConformanceLattice_BuildsAListWithANullElement(t *testing.T) {
	coverage.Covers(t, "tooling.conformance")

	arr, err := LatticeListWithNullElement("list<int64>")
	assert.NoError(t, err)
	defer arr.Release()

	assert.Equal(t, arr.Len(), 1)
	assert.Equal(t, arr.IsNull(0), false)

	values := arr.(*array.List).ListValues()
	assert.Equal(t, values.Len(), 3)
	assert.Equal(t, values.IsNull(0), false)
	assert.Equal(t, values.IsNull(1), true)
	assert.Equal(t, values.IsNull(2), false)
}

// A key that is not a list has no element to null, and asking for one is a
// mistake worth naming rather than a nil to dereference later.
func TestToolingConformanceLattice_RejectsANullElementOnANonList(t *testing.T) {
	coverage.Covers(t, "tooling.conformance")

	_, err := LatticeListWithNullElement("int64")
	assert.Error(t, err)
}

// The union's null lives one level down, so it is checked there rather than
// left unchecked. A sink that reads the child sees a null; one that reads the
// union sees a value.
func TestToolingConformanceLattice_AUnionNullLivesInItsChild(t *testing.T) {
	coverage.Covers(t, "tooling.conformance")

	arr, err := LatticeArray("sparse_union", true)
	assert.NoError(t, err)
	defer arr.Release()

	u, ok := arr.(array.Union)
	assert.That(t, ok)
	assert.Equal(t, u.Len(), 1)

	child := u.(*array.SparseUnion).Field(int(u.ChildID(0)))
	assert.That(t, child.IsNull(0))
}
