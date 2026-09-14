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

// postgresCopySource streams a batch into pgx's COPY one row at a time.
//
// It used to convert the whole batch into [][]any before the COPY started,
// which boxed every cell: a 1M-row, 4-column batch held 177 MiB of Go heap
// on top of its Arrow buffers while it was sent (#290 review). pgx encodes
// each row as soon as Values returns it, so one reused row is all this holds.
// Each row ends with its position in the batch, which the merge orders by.
type postgresCopySource struct {
	reader *array.TableReader
	rec    arrow.Record
	row    int
	seq    int64
	values []any
	err    error
}

func newPostgresCopySource(tbl arrow.Table) *postgresCopySource {
	return &postgresCopySource{
		reader: array.NewTableReader(tbl, 0),
		values: make([]any, tbl.NumCols()+1),
	}
}

func (s *postgresCopySource) Next() bool {
	if s.err != nil {
		return false
	}
	for s.rec == nil || s.row >= int(s.rec.NumRows()) {
		if !s.reader.Next() {
			s.err = s.reader.Err()
			return false
		}
		s.rec, s.row = s.reader.Record(), 0
	}
	for c := 0; c < int(s.rec.NumCols()); c++ {
		v, err := postgresValue(s.rec.Column(c), s.row)
		if err != nil {
			s.err = errs.Wrap(errs.CodeOf(err), err, "postgres sink: column %q", s.rec.ColumnName(c))
			return false
		}
		s.values[c] = v
	}
	s.values[len(s.values)-1] = s.seq
	s.seq++
	s.row++
	return true
}

func (s *postgresCopySource) Values() ([]any, error) { return s.values, nil }

func (s *postgresCopySource) Err() error { return s.err }

func (s *postgresCopySource) Release() { s.reader.Release() }

// postgresCheckSchema refuses a batch with a column the sink has no
// conversion for, before a transaction is opened. Streaming converts a cell
// only when pgx asks for its row, so without this an unsupported column
// would fail mid-COPY, where the server reports the abandoned COPY as 57014
// and the column's own code is lost.
func postgresCheckSchema(schema *arrow.Schema) error {
	for _, f := range schema.Fields() {
		if err := postgresConvertible(f.Type); err != nil {
			return errs.Wrap(errs.CodeOf(err), err, "postgres sink: column %q", f.Name)
		}
	}
	return nil
}

// postgresConvertible reports whether a column of dt converts. It must hold
// the same type set as postgresValue: a type one accepts and the other does
// not is a column that passes the check and fails mid-COPY, or the reverse.
func postgresConvertible(dt arrow.DataType) error {
	switch dt.(type) {
	case *arrow.ListType, *arrow.LargeListType, *arrow.FixedSizeListType, *arrow.StructType, *arrow.MapType:
		return jsonRenderable(dt)
	case *arrow.BooleanType, *arrow.Int8Type, *arrow.Int16Type, *arrow.Int32Type, *arrow.Int64Type,
		*arrow.Uint8Type, *arrow.Uint16Type, *arrow.Uint32Type, *arrow.Uint64Type,
		*arrow.Float32Type, *arrow.Float64Type, *arrow.StringType, *arrow.LargeStringType,
		*arrow.BinaryType, *arrow.LargeBinaryType, *arrow.Date32Type, *arrow.Date64Type,
		*arrow.TimestampType, *arrow.Decimal128Type, *arrow.Decimal256Type,
		*arrow.Time32Type, *arrow.Time64Type, *arrow.MonthDayNanoIntervalType, *arrow.DurationType:
		return nil
	case *arrow.DictionaryType:
		// DuckDB's ENUM. The column holds the decoded value, so it converts
		// when its values do.
		return postgresConvertible(dt.(*arrow.DictionaryType).ValueType)
	default:
		return errs.New(errs.CodeSinkTypeUnsupported, "no conversion for arrow type %s", dt)
	}
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

	if err := postgresConvertible(arr.DataType()); err != nil {
		return nil, err
	}
	if arr.IsNull(i) {
		return nil, nil
	}
	return postgresScalar(arr, i)
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
		*arrow.BinaryType, *arrow.LargeBinaryType, *arrow.Date32Type, *arrow.Date64Type, *arrow.TimestampType,
		*arrow.Time32Type, *arrow.Time64Type, *arrow.MonthDayNanoIntervalType, *arrow.DurationType:
		return nil
	case *arrow.DictionaryType:
		return jsonRenderable(t.ValueType)
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
	case *array.Time32:
		return pgtype.Time{Microseconds: int64(a.Value(i)) * microsPer(a.DataType().(*arrow.Time32Type).Unit), Valid: true}, nil
	case *array.Time64:
		return pgtype.Time{Microseconds: toMicros(int64(a.Value(i)), a.DataType().(*arrow.Time64Type).Unit), Valid: true}, nil
	case *array.MonthDayNanoInterval:
		// Postgres keeps microseconds; nanoseconds below one are truncated.
		v := a.Value(i)
		return pgtype.Interval{Months: v.Months, Days: v.Days, Microseconds: v.Nanoseconds / 1000, Valid: true}, nil
	case *array.Duration:
		return pgtype.Interval{Microseconds: toMicros(int64(a.Value(i)), a.DataType().(*arrow.DurationType).Unit), Valid: true}, nil
	case *array.Dictionary:
		return postgresScalar(a.Dictionary(), a.GetValueIndex(i))
	default:
		return nil, errs.New(errs.CodeSinkTypeUnsupported, "no conversion for arrow type %s", arr.DataType())
	}
}

// microsPer is how many microseconds one tick of unit is, for units of a
// microsecond or coarser.
func microsPer(unit arrow.TimeUnit) int64 {
	switch unit {
	case arrow.Second:
		return 1_000_000
	case arrow.Millisecond:
		return 1_000
	default:
		return 1
	}
}

// toMicros converts ticks of unit to microseconds, truncating nanoseconds:
// Postgres time and interval keep microseconds.
func toMicros(v int64, unit arrow.TimeUnit) int64 {
	if unit == arrow.Nanosecond {
		return v / 1000
	}
	return v * microsPer(unit)
}

func numericFromString(s string) (any, error) {
	var n pgtype.Numeric
	if err := n.Scan(s); err != nil {
		return nil, errs.Wrap(errs.CodeSinkEncodeFailed, err, "%q as numeric", s)
	}
	return n, nil
}
