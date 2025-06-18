package handlers

import (
	"bytes"
	"context"
	"fmt"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"go.uber.org/zap"
	"runtime/debug"
	"sync"
)

type StructuredBatchHandler struct {
	mu    sync.Mutex
	batch []arrow.Record

	alloc  *memory.GoAllocator
	conn   adbc.Connection
	stmt   adbc.Statement
	logger *zap.Logger
	schema *arrow.Schema
	sql    string

	tableName string
}

func (h *StructuredBatchHandler) Init(ctx context.Context) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.batch = nil

	// Execute truncate table statement
	if err := h.stmt.SetSqlQuery(fmt.Sprintf("TRUNCATE TABLE %s;", h.tableName)); err != nil {
		return err
	}
	if _, err := h.stmt.ExecuteUpdate(ctx); err != nil {
		return err
	}
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
	// Merge records into one for ingestion
	recordReader, err := array.NewRecordReader(h.schema, h.batch)
	if err != nil {
		return nil, err
	}
	defer recordReader.Release()

	var allRecords []arrow.Record
	for recordReader.Next() {
		rec := recordReader.Record()
		rec.Retain() // prevent premature GC
		allRecords = append(allRecords, rec)
	}

	var allColumns []arrow.Array
	var totalRows int64

	// Collect columns and count total rows
	for _, rec := range allRecords {
		for i := 0; i < int(rec.NumCols()); i++ {
			if len(allColumns) <= i {
				allColumns = append(allColumns, rec.Column(i))
			} else {
				allColumns[i], _ = array.Concatenate([]arrow.Array{allColumns[i], rec.Column(i)}, h.alloc)
			}
		}
		totalRows += rec.NumRows()
	}

	// Build the combined record
	combined := array.NewRecord(h.schema, allColumns, totalRows)
	defer combined.Release()

	stmt, err := h.conn.NewStatement()
	if err != nil {
		return nil, fmt.Errorf("new statement error: %v", err)
	}
	defer stmt.Close()

	// Use append mode if schema is known, otherwise create
	if h.schema == nil {
		if err := stmt.SetOption(adbc.OptionKeyIngestMode, adbc.OptionValueIngestModeCreate); err != nil {
			return nil, fmt.Errorf("set option ingest mode create error: %v", err)
		}
	} else {
		if err := stmt.SetOption(adbc.OptionKeyIngestMode, adbc.OptionValueIngestModeAppend); err != nil {
			return nil, fmt.Errorf("set option ingest mode append error: %v", err)
		}
	}

	if err := stmt.SetOption(adbc.OptionKeyIngestTargetTable, h.tableName); err != nil {
		return nil, fmt.Errorf("set option target table error: %v", err)
	}

	if err := stmt.Bind(ctx, combined); err != nil {
		return nil, fmt.Errorf("statement binding arrow record error: %v", err)
	}

	if _, err := stmt.ExecuteUpdate(ctx); err != nil {
		return nil, fmt.Errorf("execute update error: %w", err)
	}

	// Query results back using your stored SQL
	queryStmt, err := h.conn.NewStatement()
	if err != nil {
		return nil, fmt.Errorf("new query statement error: %v", err)
	}
	defer queryStmt.Close()

	if err := queryStmt.SetSqlQuery(h.sql); err != nil {
		return nil, fmt.Errorf("set query error: %v", err)
	}

	reader, _, err := queryStmt.ExecuteQuery(ctx)
	if err != nil {
		return nil, fmt.Errorf("query execution error: %v", err)
	}
	defer reader.Release()

	for reader.Next() {
		rec := reader.Record()
		rec.Retain()
		records = append(records, rec)
	}

	// Clean up batch
	for _, rec := range h.batch {
		rec.Release()
	}
	h.batch = nil

	result := array.NewTableFromRecords(reader.Schema(), records)
	result.Retain()

	for _, rec := range records {
		rec.Release()
	}

	return result, nil
}

type StructuredBatchHandlerOption func(*StructuredBatchHandler)

func StructuredBatchWithLogger(l *zap.Logger) StructuredBatchHandlerOption {
	return func(h *StructuredBatchHandler) {
		h.logger = l
	}
}

func NewStructuredBatchHandler(
	conn adbc.Connection,
	sql string,
	tableName string,
	schema *arrow.Schema,
	opts ...StructuredBatchHandlerOption,
) (*StructuredBatchHandler, error) {

	pool := memory.NewGoAllocator()

	stmt, err := conn.NewStatement()
	if err != nil {
		return nil, err
	}

	s := &StructuredBatchHandler{
		alloc:     pool,
		conn:      conn,
		stmt:      stmt,
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
