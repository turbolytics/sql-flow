package serve

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// column describes one result column in a response.
type column struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// result is a query's rows, already encoded. Nothing in it points into an
// Arrow buffer, so it outlives the reader that produced it.
type result struct {
	Columns []column
	// Rows is a JSON array of objects keyed by column name.
	Rows      json.RawMessage
	RowCount  int
	Truncated bool
}

// naiveTimestampLayout renders a timestamp without a zone. It carries no
// offset, because the value names a wall-clock time, not an instant.
const naiveTimestampLayout = "2006-01-02T15:04:05.999999999"

// readRows encodes at most maxRows rows from rdr.
//
// Every value is encoded inside the loop, while its record is current. A
// string column's Go value aliases an Arrow buffer DuckDB owns, and the next
// call to Next frees it.
//
// It stops reading at the first row past maxRows. DuckDB streams results, so
// the caller releasing the reader then stops the query.
//
// It also stops at the first batch boundary after ctx ends. ADBC's Go API has
// no Cancel, and documents releasing a reader without consuming it as
// equivalent to AdbcStatementCancel, so stopping here and letting the caller
// release is the only way a request that gave up stops the work it started. A
// session held by a query nobody is waiting for is a session the next request
// cannot have.
func readRows(ctx context.Context, rdr array.RecordReader, maxRows int) (result, error) {
	fields := rdr.Schema().Fields()
	res := result{Columns: make([]column, len(fields))}
	keys := make([][]byte, len(fields))
	for i, f := range fields {
		res.Columns[i] = column{Name: f.Name, Type: duckTypeName(f.Type)}
		key, err := json.Marshal(f.Name)
		if err != nil {
			return result{}, err
		}
		keys[i] = key
	}

	var buf bytes.Buffer
	buf.WriteByte('[')
	for !res.Truncated && rdr.Next() {
		if err := ctx.Err(); err != nil {
			return result{}, err
		}
		rec := rdr.RecordBatch()
		for r := 0; r < int(rec.NumRows()); r++ {
			if res.RowCount == maxRows {
				res.Truncated = true
				break
			}
			if res.RowCount > 0 {
				buf.WriteByte(',')
			}
			buf.WriteByte('{')
			for c := 0; c < int(rec.NumCols()); c++ {
				if c > 0 {
					buf.WriteByte(',')
				}
				buf.Write(keys[c])
				buf.WriteByte(':')
				if err := encodeValue(&buf, rec.Column(c), r); err != nil {
					return result{}, fmt.Errorf("encoding column %s: %w", fields[c].Name, err)
				}
			}
			buf.WriteByte('}')
			res.RowCount++
		}
	}
	if err := rdr.Err(); err != nil {
		return result{}, err
	}
	buf.WriteByte(']')

	res.Rows = buf.Bytes()
	return res, nil
}

// encodeValue writes one value as JSON.
//
// Four types leave Arrow's own rendering. A zoned timestamp renders in UTC
// rather than the session zone. A naive timestamp carries no offset rather
// than a Z. A decimal is a string of its exact digits, because Arrow's
// rendering goes through a float and prints HUGEINT as 1.7e+38. A float that
// is NaN or infinite is a string, because JSON has no literal for either.
func encodeValue(buf *bytes.Buffer, col arrow.Array, i int) error {
	if col.IsNull(i) {
		buf.WriteString("null")
		return nil
	}

	switch a := col.(type) {
	case *array.Timestamp:
		typ := a.DataType().(*arrow.TimestampType)
		t := a.Value(i).ToTime(typ.Unit)
		if typ.TimeZone != "" {
			return marshalInto(buf, t.UTC().Format(time.RFC3339Nano))
		}
		return marshalInto(buf, t.Format(naiveTimestampLayout))
	case *array.Decimal128:
		typ := a.DataType().(*arrow.Decimal128Type)
		return marshalInto(buf, a.Value(i).ToString(typ.Scale))
	case *array.Float32:
		return writeFloat(buf, float64(a.Value(i)), a.Value(i))
	case *array.Float64:
		return writeFloat(buf, a.Value(i), a.Value(i))
	}
	return marshalInto(buf, col.GetOneForMarshal(i))
}

// writeFloat takes the value twice: as a float64 to test, and as its own
// type to marshal, so a float32 prints as 1.1 rather than 1.100000023841858.
func writeFloat(buf *bytes.Buffer, f float64, v any) error {
	switch {
	case math.IsNaN(f):
		return marshalInto(buf, "NaN")
	case math.IsInf(f, 1):
		return marshalInto(buf, "Infinity")
	case math.IsInf(f, -1):
		return marshalInto(buf, "-Infinity")
	}
	return marshalInto(buf, v)
}

func marshalInto(buf *bytes.Buffer, v any) error {
	encoded, err := json.Marshal(v)
	if err != nil {
		return err
	}
	buf.Write(encoded)
	return nil
}

// duckTypeName reports a column's type as DuckDB names it. A caller wrote
// the SQL in DuckDB's terms and never sees an Arrow type.
func duckTypeName(dt arrow.DataType) string {
	switch t := dt.(type) {
	case *arrow.TimestampType:
		if t.TimeZone != "" {
			return "TIMESTAMP WITH TIME ZONE"
		}
		return "TIMESTAMP"
	case *arrow.Decimal128Type:
		return fmt.Sprintf("DECIMAL(%d,%d)", t.Precision, t.Scale)
	case *arrow.DictionaryType:
		// A DuckDB ENUM arrives as a dictionary of strings.
		return duckTypeName(t.ValueType)
	}

	switch dt.ID() {
	case arrow.STRING, arrow.LARGE_STRING:
		return "VARCHAR"
	case arrow.BOOL:
		return "BOOLEAN"
	case arrow.INT8:
		return "TINYINT"
	case arrow.INT16:
		return "SMALLINT"
	case arrow.INT32:
		return "INTEGER"
	case arrow.INT64:
		return "BIGINT"
	case arrow.UINT8:
		return "UTINYINT"
	case arrow.UINT16:
		return "USMALLINT"
	case arrow.UINT32:
		return "UINTEGER"
	case arrow.UINT64:
		return "UBIGINT"
	case arrow.FLOAT32:
		return "FLOAT"
	case arrow.FLOAT64:
		return "DOUBLE"
	case arrow.DATE32:
		return "DATE"
	case arrow.TIME64:
		return "TIME"
	case arrow.BINARY, arrow.LARGE_BINARY:
		return "BLOB"
	case arrow.INTERVAL_MONTH_DAY_NANO:
		return "INTERVAL"
	case arrow.LIST, arrow.LARGE_LIST:
		return "LIST"
	case arrow.STRUCT:
		return "STRUCT"
	case arrow.MAP:
		return "MAP"
	}
	return dt.Name()
}
