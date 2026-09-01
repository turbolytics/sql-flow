package sinks

import (
	"encoding/json"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/zeebo/assert"
)

// newTestTable builds a two-column table shaped like a typical aggregation
// result: a string key and an integer count.
func newTestTable(t *testing.T, cities []string, counts []int64) arrow.Table {
	t.Helper()

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "city", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "count", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	}, nil)

	b := array.NewRecordBuilder(memory.NewGoAllocator(), schema)
	defer b.Release()

	b.Field(0).(*array.StringBuilder).AppendValues(cities, nil)
	b.Field(1).(*array.Int64Builder).AppendValues(counts, nil)

	rec := b.NewRecord()
	defer rec.Release()

	return array.NewTableFromRecords(schema, []arrow.Record{rec})
}

func TestTableRowsAsJSON_OneObjectPerRow(t *testing.T) {
	table := newTestTable(t, []string{"NYC", "SF"}, []int64{3, 1})
	defer table.Release()

	rows, err := tableRowsAsJSON(table)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(rows))

	var first map[string]any
	assert.NoError(t, json.Unmarshal(rows[0], &first))
	assert.Equal(t, "NYC", first["city"])
	assert.Equal(t, float64(3), first["count"])

	var second map[string]any
	assert.NoError(t, json.Unmarshal(rows[1], &second))
	assert.Equal(t, "SF", second["city"])
	assert.Equal(t, float64(1), second["count"])
}

func TestTableRowsAsJSON_EmptyTable(t *testing.T) {
	table := newTestTable(t, []string{}, []int64{})
	defer table.Release()

	rows, err := tableRowsAsJSON(table)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(rows))
}
