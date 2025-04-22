package handlers

import (
	"bytes"
	"context"
	"fmt"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/marcboeker/go-duckdb"
	"sync"
)

type StructuredBatchHandler struct {
	mu    sync.Mutex
	batch []arrow.Record

	alloc  *memory.GoAllocator
	arr    *duckdb.Arrow
	schema *arrow.Schema
	sql    string

	tableName string
}

func (h *StructuredBatchHandler) Init(ctx context.Context) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.batch = nil
	rdr, err := h.arr.QueryContext(
		ctx,
		fmt.Sprintf(
			"TRUNCATE TABLE %s;",
			h.tableName,
		),
	)
	if err != nil {
		return err
	}
	rdr.Release()
	return nil
}

func (h *StructuredBatchHandler) Write(r []byte) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	record, _, err := array.RecordFromJSON(
		h.alloc,
		h.schema,
		bytes.NewReader(r),
		array.WithMultipleDocs(),
	)
	if err != nil {
		return err
	}

	h.batch = append(h.batch, record)
	return nil
}

func (h *StructuredBatchHandler) Invoke(ctx context.Context) (arrow.Table, error) {
	recordReader, err := array.NewRecordReader(
		h.schema,
		h.batch,
	)
	if err != nil {
		return nil, err
	}

	release, err := h.arr.RegisterView(
		recordReader,
		"batch",
	)
	if err != nil {
		return nil, err
	}
	defer release()

	copyRes, err := h.arr.QueryContext(
		ctx,
		fmt.Sprintf(
			"INSERT INTO %s (SELECT * FROM batch)",
			h.tableName,
		),
	)
	if err != nil {
		return nil, err
	}
	copyRes.Release()

	rdr, err := h.arr.QueryContext(
		ctx,
		h.sql,
	)
	if err != nil {
		return nil, err
	}

	defer rdr.Release()

	var records []arrow.Record
	for rdr.Next() {
		records = append(records, rdr.Record())
	}

	table := array.NewTableFromRecords(
		rdr.Schema(),
		records,
	)

	return table, nil
}

func NewStructuredBatchHandler(arr *duckdb.Arrow, sql string, tableName string, schema *arrow.Schema) (*StructuredBatchHandler, error) {
	pool := memory.NewGoAllocator()

	s := &StructuredBatchHandler{
		alloc:     pool,
		arr:       arr,
		schema:    schema,
		sql:       sql,
		tableName: tableName,
	}
	return s, nil
}
