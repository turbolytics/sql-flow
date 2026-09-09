package conformance

// The canonical form of an Arrow type.
//
// The key is canonical rather than raw because DataType.String() is not one
// name per type. DuckDB's ADBC output calls a list's child "l" and
// arrow.ListOf calls it "item", so the same logical type has two spellings,
// decided by whoever built it. A registry keyed on the raw string would match
// the constructor and silently miss the engine.

import (
	"fmt"
	"sort"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// CanonicalKey is the docs/coverage/lattice.yml key for an Arrow type.
//
// It drops what a declaration must not depend on: a child field's name, its
// nullability, a decimal's precision and scale, and a timestamp's zone. It
// keeps what changes a sink's behaviour: the width, the time unit, and the
// element type of a container.
func CanonicalKey(dt arrow.DataType) string {
	switch t := dt.(type) {
	case *arrow.ListType:
		return "list<" + CanonicalKey(t.Elem()) + ">"
	case *arrow.LargeListType:
		return "large_list<" + CanonicalKey(t.Elem()) + ">"
	case *arrow.FixedSizeListType:
		return fmt.Sprintf("fixed_size_list<%s>[%d]", CanonicalKey(t.Elem()), t.Len())
	case *arrow.StructType:
		return "struct"
	case *arrow.MapType:
		return "map"
	case *arrow.SparseUnionType:
		return "sparse_union"
	case *arrow.DenseUnionType:
		return "dense_union"
	case *arrow.DictionaryType:
		return "dictionary"
	case *arrow.Decimal128Type, *arrow.Decimal256Type:
		// Precision and scale are the user's choice and do not change which
		// branch of a sink's conversion runs.
		return "decimal(*, *)"
	case *arrow.TimestampType:
		// The zone comes from the DuckDB session, so it is a property of the
		// host that ran the query rather than of the type.
		if t.TimeZone != "" {
			return "timestamp[" + t.Unit.String() + ", tz=*]"
		}
		return "timestamp[" + t.Unit.String() + "]"
	default:
		return dt.String()
	}
}

// latticeType is the concrete Arrow type each key builds.
//
// A test holds these keys equal to lattice.yml in both directions, so a type
// declared with no builder fails in seconds rather than going unexercised.
var latticeType = map[string]arrow.DataType{
	"bool":    arrow.FixedWidthTypes.Boolean,
	"int8":    arrow.PrimitiveTypes.Int8,
	"int16":   arrow.PrimitiveTypes.Int16,
	"int32":   arrow.PrimitiveTypes.Int32,
	"int64":   arrow.PrimitiveTypes.Int64,
	"uint8":   arrow.PrimitiveTypes.Uint8,
	"uint16":  arrow.PrimitiveTypes.Uint16,
	"uint32":  arrow.PrimitiveTypes.Uint32,
	"uint64":  arrow.PrimitiveTypes.Uint64,
	"float32": arrow.PrimitiveTypes.Float32,
	"float64": arrow.PrimitiveTypes.Float64,

	"decimal(*, *)": &arrow.Decimal128Type{Precision: 38, Scale: 0},
	"utf8":          arrow.BinaryTypes.String,
	"binary":        arrow.BinaryTypes.Binary,
	"date32":        arrow.FixedWidthTypes.Date32,
	"time64[us]":    arrow.FixedWidthTypes.Time64us,
	"time64[ns]":    arrow.FixedWidthTypes.Time64ns,
	"timestamp[s]":  &arrow.TimestampType{Unit: arrow.Second},
	"timestamp[ms]": &arrow.TimestampType{Unit: arrow.Millisecond},
	"timestamp[us]": &arrow.TimestampType{Unit: arrow.Microsecond},
	"timestamp[ns]": &arrow.TimestampType{Unit: arrow.Nanosecond},

	// The zone is deliberately not UTC. A runner on a UTC host proves nothing
	// about zone leakage when the value it writes is already UTC, which is how
	// #153 survived until someone ran the pipeline from a UTC-4 laptop.
	"timestamp[us, tz=*]":     &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "Asia/Tokyo"},
	"month_day_nano_interval": arrow.FixedWidthTypes.MonthDayNanoInterval,

	"list<bool>":                arrow.ListOf(arrow.FixedWidthTypes.Boolean),
	"list<int8>":                arrow.ListOf(arrow.PrimitiveTypes.Int8),
	"list<int16>":               arrow.ListOf(arrow.PrimitiveTypes.Int16),
	"list<int32>":               arrow.ListOf(arrow.PrimitiveTypes.Int32),
	"list<int64>":               arrow.ListOf(arrow.PrimitiveTypes.Int64),
	"list<uint8>":               arrow.ListOf(arrow.PrimitiveTypes.Uint8),
	"list<uint16>":              arrow.ListOf(arrow.PrimitiveTypes.Uint16),
	"list<uint32>":              arrow.ListOf(arrow.PrimitiveTypes.Uint32),
	"list<uint64>":              arrow.ListOf(arrow.PrimitiveTypes.Uint64),
	"list<float32>":             arrow.ListOf(arrow.PrimitiveTypes.Float32),
	"list<float64>":             arrow.ListOf(arrow.PrimitiveTypes.Float64),
	"list<utf8>":                arrow.ListOf(arrow.BinaryTypes.String),
	"list<binary>":              arrow.ListOf(arrow.BinaryTypes.Binary),
	"list<date32>":              arrow.ListOf(arrow.FixedWidthTypes.Date32),
	"list<time64[us]>":          arrow.ListOf(arrow.FixedWidthTypes.Time64us),
	"list<timestamp[s]>":        arrow.ListOf(&arrow.TimestampType{Unit: arrow.Second}),
	"list<timestamp[ms]>":       arrow.ListOf(&arrow.TimestampType{Unit: arrow.Millisecond}),
	"list<timestamp[us]>":       arrow.ListOf(&arrow.TimestampType{Unit: arrow.Microsecond}),
	"list<timestamp[ns]>":       arrow.ListOf(&arrow.TimestampType{Unit: arrow.Nanosecond}),
	"list<timestamp[us, tz=*]>": arrow.ListOf(&arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "Asia/Tokyo"}),
	"list<decimal(*, *)>":       arrow.ListOf(&arrow.Decimal128Type{Precision: 38, Scale: 0}),
	"list<list<int64>>":         arrow.ListOf(arrow.ListOf(arrow.PrimitiveTypes.Int64)),
	"list<struct>": arrow.ListOf(arrow.StructOf(
		arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Int32, Nullable: true})),
	"fixed_size_list<int32>[3]": arrow.FixedSizeListOf(3, arrow.PrimitiveTypes.Int32),
	"struct":                    arrow.StructOf(arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Int32, Nullable: true}),
	"map":                       arrow.MapOf(arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int32),
	"sparse_union": arrow.SparseUnionOf(
		[]arrow.Field{{Name: "n", Type: arrow.PrimitiveTypes.Int32, Nullable: true}},
		[]arrow.UnionTypeCode{0}),
	"dictionary": &arrow.DictionaryType{
		IndexType: arrow.PrimitiveTypes.Uint8, ValueType: arrow.BinaryTypes.String},
}

// LatticeKeys returns every key the value table can build, sorted.
func LatticeKeys() []string {
	out := make([]string, 0, len(latticeType))
	for k := range latticeType {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// LatticeArray builds a one-row array of the key's type. The caller releases
// it.
//
// A null row and a valued row are the same call, because type.null asks the
// same question of the same type: what the destination holds afterwards.
func LatticeArray(key string, null bool) (arrow.Array, error) {
	dt, ok := latticeType[key]
	if !ok {
		return nil, fmt.Errorf("conformance: no lattice value for %q", key)
	}

	b := array.NewBuilder(memory.NewGoAllocator(), dt)
	defer b.Release()

	if null {
		b.AppendNull()
		return b.NewArray(), nil
	}
	if err := appendLatticeValue(b, key); err != nil {
		return nil, err
	}
	return b.NewArray(), nil
}

// LatticeString builds a one-row utf8 array holding v. The caller releases it.
//
// It exists for the column entries that declare their own value. A UUID, a
// Decimal or an Enum column reparses text, so the pair needs content the key's
// canonical value cannot supply: the canonical utf8 value is the hostile
// string type.string.fidelity needs, and no UUID column will take it.
func LatticeString(v string) arrow.Array {
	b := array.NewStringBuilder(memory.NewGoAllocator())
	defer b.Release()
	b.Append(v)
	return b.NewArray()
}

// LatticeListWithNullElement builds a one-row list holding [value, null,
// value]. The caller releases it.
//
// This is the third null position, and the one no published table mentions. A
// null column and a null list are both visible to anyone who looks; a null
// inside a list is not, and ClickHouse's Array(T) has no way to hold one.
func LatticeListWithNullElement(key string) (arrow.Array, error) {
	dt, ok := latticeType[key]
	if !ok {
		return nil, fmt.Errorf("conformance: no lattice value for %q", key)
	}
	lt, ok := dt.(*arrow.ListType)
	if !ok {
		return nil, fmt.Errorf("conformance: %q is not a list, so it has no element to null", key)
	}

	b := array.NewBuilder(memory.NewGoAllocator(), lt).(*array.ListBuilder)
	defer b.Release()

	b.Append(true)
	elem := b.ValueBuilder()
	if err := appendLatticeValue(elem, key); err != nil {
		return nil, err
	}
	elem.AppendNull()
	if err := appendLatticeValue(elem, key); err != nil {
		return nil, err
	}
	return b.NewArray(), nil
}

// latticeInstant is the one moment every temporal key writes, so a shifted
// value names the unit that shifted it. The sub-second digits are distinct at
// every precision, so a truncation is visible rather than a rounding.
var latticeInstant = time.Date(2026, 9, 8, 12, 0, 0, 123456789, time.UTC)

// appendLatticeValue writes the one canonical value for a key.
//
// Values are chosen to catch a width or sign error rather than to be tidy:
// each integer is its type's bound, so a value stored one width too narrow
// wraps rather than surviving.
func appendLatticeValue(b array.Builder, key string) error {
	switch bldr := b.(type) {
	case *array.BooleanBuilder:
		bldr.Append(true)
	case *array.Int8Builder:
		bldr.Append(-128)
	case *array.Int16Builder:
		bldr.Append(-32768)
	case *array.Int32Builder:
		bldr.Append(-2147483648)
	case *array.Int64Builder:
		bldr.Append(-9223372036854775808)
	case *array.Uint8Builder:
		bldr.Append(255)
	case *array.Uint16Builder:
		bldr.Append(65535)
	case *array.Uint32Builder:
		bldr.Append(4294967295)
	case *array.Uint64Builder:
		bldr.Append(18446744073709551615)
	case *array.Float32Builder:
		bldr.Append(3.4028235e38)
	case *array.Float64Builder:
		bldr.Append(1.7976931348623157e308)
	case *array.Decimal128Builder:
		bldr.Append(decimal128.FromI64(170141183460469231))
	case *array.StringBuilder:
		// #149's defect: escapes reached the destination undecoded. The quote
		// and the backslash are the two that were corrupted.
		bldr.Append("L'Œil 👁 \"quoted\" back\\slash\ttab")
	case *array.BinaryBuilder:
		bldr.Append([]byte{0x00, 0xff, 0x7f, 0x80})
	case *array.Date32Builder:
		bldr.Append(arrow.Date32FromTime(latticeInstant))
	case *array.Time64Builder:
		bldr.Append(arrow.Time64(12*3600*1e6 + 123456))
	case *array.TimestampBuilder:
		ts, err := arrow.TimestampFromTime(latticeInstant, bldr.Type().(*arrow.TimestampType).Unit)
		if err != nil {
			return fmt.Errorf("conformance: %s: %w", key, err)
		}
		bldr.Append(ts)
	case *array.MonthDayNanoIntervalBuilder:
		bldr.Append(arrow.MonthDayNanoInterval{Months: 1, Days: 2, Nanoseconds: 3})
	case *array.ListBuilder:
		bldr.Append(true)
		// Three elements, filled through the same switch that fills a bare
		// column, so a list of T carries the same value as a T. A divergence
		// between the two is then a real difference in the sink rather than in
		// the fixture. Nested lists and structs recurse through here.
		elem := bldr.ValueBuilder()
		for i := 0; i < 3; i++ {
			if err := appendLatticeValue(elem, key); err != nil {
				return err
			}
		}
	case *array.FixedSizeListBuilder:
		bldr.Append(true)
		bldr.ValueBuilder().(*array.Int32Builder).AppendValues([]int32{1, 2, 3}, nil)
	case *array.StructBuilder:
		bldr.Append(true)
		bldr.FieldBuilder(0).(*array.Int32Builder).Append(7)
	case *array.MapBuilder:
		bldr.Append(true)
		bldr.KeyBuilder().(*array.StringBuilder).Append("k")
		bldr.ItemBuilder().(*array.Int32Builder).Append(9)
	case *array.SparseUnionBuilder:
		bldr.Append(0)
		bldr.Child(0).(*array.Int32Builder).Append(5)
	case *array.BinaryDictionaryBuilder:
		if err := bldr.AppendString("a"); err != nil {
			return fmt.Errorf("conformance: %s: %w", key, err)
		}
	default:
		return fmt.Errorf("conformance: no value appender for %q (%T)", key, b)
	}
	return nil
}
