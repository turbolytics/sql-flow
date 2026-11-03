// Package arrowfmt renders Arrow values as text.
//
// arrow-go chooses its own timestamp layout, and a release changed it: 18.7.0
// ended a zone-less timestamp in Z and 18.8.0 did not. Every place that turns
// an Arrow value into text calls this package, so an upgrade cannot change
// what a sink writes.
package arrowfmt

import (
	"encoding/json"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// Marshal returns the JSON for element i of col.
func Marshal(col arrow.Array, i int) ([]byte, error) {
	return json.Marshal(Value(col, i))
}

// Value returns what json.Marshal renders for element i of col. It has the
// shape of Arrow's GetOneForMarshal, with every timestamp rendered by this
// package.
func Value(col arrow.Array, i int) any {
	if !containsTimestamp(col.DataType()) {
		return col.GetOneForMarshal(i)
	}
	if col.IsNull(i) {
		return nil
	}

	switch a := col.(type) {
	case *array.Timestamp:
		return timestampText(a, i)
	case *array.Dictionary:
		return Value(a.Dictionary(), a.GetValueIndex(i))
	case *array.Struct:
		fields := a.DataType().(*arrow.StructType).Fields()
		out := make(map[string]any, len(fields))
		for j := range fields {
			out[fields[j].Name] = Value(a.Field(j), i)
		}
		return out
	case listLike:
		start, end := a.ValueOffsets(i)
		values := a.ListValues()
		out := make([]any, 0, end-start)
		for j := start; j < end; j++ {
			out = append(out, Value(values, int(j)))
		}
		return out
	}
	return col.GetOneForMarshal(i)
}

// Text returns element i of col as ValueStr would, with a timestamp rendered
// by this package.
func Text(col arrow.Array, i int) string {
	if a, ok := col.(*array.Timestamp); ok && !a.IsNull(i) {
		return timestampText(a, i)
	}
	return col.ValueStr(i)
}

// RecordRow returns row i of rec as one JSON object, keyed by column name.
func RecordRow(rec arrow.RecordBatch, i int) ([]byte, error) {
	fields := rec.Schema().Fields()
	row := make(map[string]any, len(fields))
	for c, col := range rec.Columns() {
		row[fields[c].Name] = Value(col, i)
	}
	return json.Marshal(row)
}

type listLike interface {
	arrow.Array
	ListValues() arrow.Array
	ValueOffsets(i int) (start, end int64)
}

// timestampText renders the instant in the column's zone, or in UTC when the
// column has none, or when Go cannot load it. This is arrow-go 18.7.0's rendering, kept so that a sink
// writes the same string it wrote before the layout moved out of our hands.
func timestampText(a *array.Timestamp, i int) string {
	typ := a.DataType().(*arrow.TimestampType)
	toTime, err := typ.GetToTimeFunc()
	if err != nil {
		// A zone Go cannot load, such as a runtime image without zoneinfo,
		// still names a known instant. UTC renders it without a panic.
		return a.Value(i).ToTime(typ.Unit).UTC().Format(time.RFC3339Nano)
	}
	return toTime(a.Value(i)).Format(time.RFC3339Nano)
}

func containsTimestamp(dt arrow.DataType) bool {
	switch t := dt.(type) {
	case *arrow.TimestampType:
		return true
	case *arrow.DictionaryType:
		return containsTimestamp(t.ValueType)
	case *arrow.StructType:
		for _, f := range t.Fields() {
			if containsTimestamp(f.Type) {
				return true
			}
		}
	case arrow.ListLikeType:
		return containsTimestamp(t.Elem())
	}
	return false
}
