package handlers

import (
	"bytes"
	"context"
	"fmt"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/marcboeker/go-duckdb"
	"go.uber.org/zap"
	"runtime/debug"
	"sync"
)

type StructuredBatchHandler struct {
	mu    sync.Mutex
	batch []arrow.Record

	alloc  *memory.GoAllocator
	arr    *duckdb.Arrow
	logger *zap.Logger
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
	var records []arrow.Record
	defer func() {
		if r := recover(); r != nil {
			// Log the panic details for debugging
			h.logger.Error(
				"Panic occurred in Invoke: %v\n",
				zap.Error(r.(error)),
			)

			for _, rec := range records {
				bs, _ := rec.MarshalJSON()
				h.logger.Debug("record", zap.String("record", string(bs)))
			}

			for _, rec := range h.batch {
				bs, _ := rec.MarshalJSON()
				h.logger.Debug("batch_record", zap.String("record", string(bs)))
			}
			debug.PrintStack() // Print the stack trace for more context
		}
	}()
	recordReader, err := array.NewRecordReader(
		h.schema,
		h.batch,
	)
	if err != nil {
		return nil, err
	}
	defer recordReader.Release()

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

	for rdr.Next() {
		rec := rdr.Record()
		rec.Retain()
		records = append(records, rec)
	}

	table := array.NewTableFromRecords(
		rdr.Schema(),
		records,
	)
	table.Retain()

	for _, rec := range records {
		rec.Release()
	}

	return table, nil
}

type StructuredBatchHandlerOption func(*StructuredBatchHandler)

func StructuredBatchWithLogger(l *zap.Logger) StructuredBatchHandlerOption {
	return func(h *StructuredBatchHandler) {
		h.logger = l
	}
}

func NewStructuredBatchHandler(
	arr *duckdb.Arrow,
	sql string,
	tableName string,
	schema *arrow.Schema,
	opts ...StructuredBatchHandlerOption,
) (*StructuredBatchHandler, error) {

	pool := memory.NewGoAllocator()

	s := &StructuredBatchHandler{
		alloc:     pool,
		arr:       arr,
		schema:    schema,
		sql:       sql,
		tableName: tableName,
		logger:    zap.NewNop(),
	}

	for _, opt := range opts {
		opt(s)
	}

	return s, nil
}
