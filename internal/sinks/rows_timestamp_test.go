package sinks

import (
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/zeebo/assert"
)

func naiveTimestampTable(t *testing.T) arrow.Table {
	t.Helper()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "at", Type: &arrow.TimestampType{Unit: arrow.Microsecond}},
	}, nil)
	rb := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer rb.Release()
	at := time.Date(2026, 9, 8, 12, 0, 0, 123456000, time.UTC)
	rb.Field(0).(*array.TimestampBuilder).Append(arrow.Timestamp(at.UnixMicro()))
	rec := rb.NewRecordBatch()
	return array.NewTableFromRecords(schema, []arrow.RecordBatch{rec})
}

// arrow-go 18.8.0 dropped the Z from a zone-less timestamp. A consumer that
// parses the row misreads the string if the Z goes missing.
func TestSinkRows_ANaiveTimestampKeepsTheZ(t *testing.T) {
	rows, err := tableRowsAsJSON(naiveTimestampTable(t))
	assert.NoError(t, err)
	assert.Equal(t, 1, len(rows))
	assert.Equal(t, `{"at":"2026-09-08T12:00:00.123456Z"}`, string(rows[0]))
}

func TestSinkRows_AKeyColumnOfTimestampsKeepsTheZ(t *testing.T) {
	keys, err := tableRowKeys(naiveTimestampTable(t), "at")
	assert.NoError(t, err)
	assert.Equal(t, 1, len(keys))
	assert.Equal(t, "2026-09-08T12:00:00.123456Z", string(keys[0]))
}
