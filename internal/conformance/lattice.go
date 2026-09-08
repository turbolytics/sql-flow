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

	"github.com/apache/arrow-go/v18/arrow"
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
