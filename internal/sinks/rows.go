package sinks

import (
	"bytes"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
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
