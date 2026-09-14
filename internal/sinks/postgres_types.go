package sinks

import (
	"encoding/json"
	"math"
	"strconv"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/turbolytics/sql-flow/internal/errs"
)

// Arrow to pgx. pgx encodes each Go value for the staging column's own type,
// which it reads from the server, so a BIGINT column takes an int32 value and
// a TIMESTAMPTZ column takes a time.Time as the instant. The sink's job is to
// hand it a Go value it can encode, and to refuse a type it cannot, with a
// code the operator can act on.

// postgresRows converts a whole batch before anything is sent. Memory is the
// batch, and a value the sink cannot convert fails before a transaction is
// opened. Each row ends with its position in the batch.
func postgresRows(tbl arrow.Table) ([][]any, error) {
	reader := array.NewTableReader(tbl, 0)
	defer reader.Release()

	rows := make([][]any, 0, tbl.NumRows())
	seq := int64(0)
	for reader.Next() {
		rec := reader.Record()
		for i := 0; i < int(rec.NumRows()); i++ {
			row := make([]any, rec.NumCols()+1)
			for c := 0; c < int(rec.NumCols()); c++ {
				v, err := postgresValue(rec.Column(c), i)
				if err != nil {
					return nil, errs.Wrap(errs.CodeOf(err), err, "postgres sink: column %q", rec.ColumnName(c))
				}
				row[c] = v
			}
			row[rec.NumCols()] = seq
			seq++
			rows = append(rows, row)
		}
	}
	return rows, reader.Err()
}

// postgresValue converts one cell. A null is nil, and Postgres stores it as
// NULL or refuses it under the column's own constraint. Convertibility is
// checked before absence: a null of a type the sink cannot convert fails,
// because an all-null column is the ordinary shape of a field the producer
// stopped sending, and it must fail the same way as the column with a value.
func postgresValue(arr arrow.Array, i int) (any, error) {
	switch arr.(type) {
	case *array.List, *array.LargeList, *array.FixedSizeList, *array.Struct, *array.Map:
		if err := jsonRenderable(arr.DataType()); err != nil {
			return nil, err
		}
		if arr.IsNull(i) {
			return nil, nil
		}
		// Rendered from the Arrow array itself: GetOneForMarshal is what
		// the array's own MarshalJSON uses for each element.
		b, err := json.Marshal(arr.(interface{ GetOneForMarshal(int) any }).GetOneForMarshal(i))
		if err != nil {
			return nil, errs.Wrap(errs.CodeSinkEncodeFailed, err, "rendering %s as JSON", arr.DataType())
		}
		return string(b), nil
	}

	v, err := postgresScalar(arr, i)
	if err != nil {
		return nil, err
	}
	if arr.IsNull(i) {
		return nil, nil
	}
	return v, nil
}

// jsonRenderable refuses a container holding a type the sink has no
// conversion for. A list does not launder its element.
func jsonRenderable(dt arrow.DataType) error {
	switch t := dt.(type) {
	case *arrow.ListType:
		return jsonRenderable(t.Elem())
	case *arrow.LargeListType:
		return jsonRenderable(t.Elem())
	case *arrow.FixedSizeListType:
		return jsonRenderable(t.Elem())
	case *arrow.MapType:
		if err := jsonRenderable(t.KeyType()); err != nil {
			return err
		}
		return jsonRenderable(t.ItemType())
	case *arrow.StructType:
		for _, f := range t.Fields() {
			if err := jsonRenderable(f.Type); err != nil {
				return err
			}
		}
		return nil
	case *arrow.BooleanType, *arrow.Int8Type, *arrow.Int16Type, *arrow.Int32Type, *arrow.Int64Type,
		*arrow.Uint8Type, *arrow.Uint16Type, *arrow.Uint32Type, *arrow.Uint64Type,
		*arrow.Float32Type, *arrow.Float64Type, *arrow.StringType, *arrow.LargeStringType,
		*arrow.BinaryType, *arrow.LargeBinaryType, *arrow.Date32Type, *arrow.Date64Type, *arrow.TimestampType:
		return nil
	default:
		return errs.New(errs.CodeSinkTypeUnsupported, "no conversion for arrow type %s inside a container", dt)
	}
}

func postgresScalar(arr arrow.Array, i int) (any, error) {
	switch a := arr.(type) {
	case *array.Boolean:
		return a.Value(i), nil
	case *array.Int8:
		return int16(a.Value(i)), nil
	case *array.Int16:
		return a.Value(i), nil
	case *array.Int32:
		return a.Value(i), nil
	case *array.Int64:
		return a.Value(i), nil
	case *array.Uint8:
		return int16(a.Value(i)), nil
	case *array.Uint16:
		return int32(a.Value(i)), nil
	case *array.Uint32:
		return int64(a.Value(i)), nil
	case *array.Uint64:
		v := a.Value(i)
		if v <= math.MaxInt64 {
			return int64(v), nil
		}
		return numericFromString(strconv.FormatUint(v, 10))
	case *array.Float32:
		return a.Value(i), nil
	case *array.Float64:
		return a.Value(i), nil
	case *array.String:
		return a.Value(i), nil
	case *array.LargeString:
		return a.Value(i), nil
	case *array.Binary:
		return a.Value(i), nil
	case *array.LargeBinary:
		return a.Value(i), nil
	case *array.Date32:
		return a.Value(i).ToTime(), nil
	case *array.Date64:
		return a.Value(i).ToTime(), nil
	case *array.Timestamp:
		// ToTime returns the instant in UTC for a zoned type, and the wall
		// clock read as UTC for a naive one. pgx encodes the instant into
		// timestamptz and the UTC wall clock into timestamp, which is what
		// the spec's type table promises for both.
		return a.Value(i).ToTime(a.DataType().(*arrow.TimestampType).Unit), nil
	case *array.Decimal128:
		return numericFromString(a.Value(i).ToString(a.DataType().(*arrow.Decimal128Type).Scale))
	case *array.Decimal256:
		return numericFromString(a.Value(i).ToString(a.DataType().(*arrow.Decimal256Type).Scale))
	default:
		return nil, errs.New(errs.CodeSinkTypeUnsupported, "no conversion for arrow type %s", arr.DataType())
	}
}

func numericFromString(s string) (any, error) {
	var n pgtype.Numeric
	if err := n.Scan(s); err != nil {
		return nil, errs.Wrap(errs.CodeSinkEncodeFailed, err, "%q as numeric", s)
	}
	return n, nil
}
