package sinks

import (
	"bytes"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/turbolytics/sql-flow/internal/errs"
)

// tableRowsAsJSON renders each row of the table as a JSON object, matching
// the Python sinks, which serialize pyarrow's to_pylist() rows individually.
func tableRowsAsJSON(table arrow.Table) ([][]byte, error) {
	var rows [][]byte

	// An empty batch yields no table. Every sink funnels through here, so
	// one check keeps a nil out of arrow's reader, which dereferences it.
	if table == nil {
		return nil, nil
	}

	reader := array.NewTableReader(table, 0)
	defer reader.Release()

	for reader.Next() {
		rec := reader.Record()
		if rec.NumRows() == 0 {
			continue
		}

		// RecordToJSON writes one JSON object per line.
		var buf bytes.Buffer
		if err := array.RecordToJSON(rec, &buf); err != nil {
			return nil, err
		}

		for _, line := range bytes.Split(bytes.TrimRight(buf.Bytes(), "\n"), []byte("\n")) {
			if len(bytes.TrimSpace(line)) == 0 {
				continue
			}
			row := make([]byte, len(line))
			copy(row, line)
			rows = append(rows, row)
		}
	}

	return rows, reader.Err()
}

// tableRowKeys returns column's value for every row of table, as text, in
// the order tableRowsAsJSON returns the rows. A null is an error: a keyed
// sink exists so one key always lands on one partition, and a null has no
// partition to land on.
func tableRowKeys(table arrow.Table, column string) ([][]byte, error) {
	if table == nil {
		return nil, nil
	}
	idx := table.Schema().FieldIndices(column)
	if len(idx) == 0 {
		return nil, errs.New(errs.CodeSinkEncodeFailed,
			"kafka sink: key column %q is not in the handler's output", column)
	}
	reader := array.NewTableReader(table, 0)
	defer reader.Release()
	var keys [][]byte
	for reader.Next() {
		rec := reader.Record()
		col := rec.Column(idx[0])
		for i := 0; i < int(rec.NumRows()); i++ {
			if col.IsNull(i) {
				return nil, errs.New(errs.CodeSinkEncodeFailed,
					"kafka sink: key column %q is null in row %d", column, len(keys))
			}
			keys = append(keys, []byte(col.ValueStr(i)))
		}
	}
	return keys, nil
}
