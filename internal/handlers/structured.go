package handlers

import (
	"context"
	"fmt"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/buger/jsonparser"
	"go.uber.org/zap"
	"strconv"
	"time"
	"unsafe"
)

type StructuredBatchHandler struct {
	rawBatch [][]byte

	alloc      *memory.GoAllocator
	conn       adbc.Connection
	truncStmt  adbc.Statement
	ingestStmt adbc.Statement
	queryStmt  adbc.Statement
	logger     *zap.Logger
	schema     *arrow.Schema
	sql        string

	// Pre-computed field names for jsonparser extraction
	fieldNames []string
	tableName  string
}

func (h *StructuredBatchHandler) Init(ctx context.Context) error {
	h.rawBatch = h.rawBatch[:0]

	if _, err := h.truncStmt.ExecuteUpdate(ctx); err != nil {
		return err
	}
	return nil
}

func (h *StructuredBatchHandler) Write(r []byte) error {
	h.rawBatch = append(h.rawBatch, r)
	return nil
}

// unsafeString converts bytes to string without allocation.
func unsafeString(b []byte) string {
	return unsafe.String(unsafe.SliceData(b), len(b))
}

// appendJSONValue extracts a JSON value and appends it to the corresponding Arrow builder.
func appendJSONValue(builder array.Builder, fieldType arrow.DataType, data []byte, key string) error {
	val, dataType, _, err := jsonparser.Get(data, key)
	if err != nil || dataType == jsonparser.NotExist {
		builder.AppendNull()
		return nil
	}

	switch fieldType.(type) {
	case *arrow.StringType:
		builder.(*array.StringBuilder).Append(unsafeString(val))

	case *arrow.Int32Type:
		n, err := strconv.ParseInt(unsafeString(val), 10, 32)
		if err != nil {
			builder.AppendNull()
			return nil
		}
		builder.(*array.Int32Builder).Append(int32(n))

	case *arrow.Int64Type:
		n, err := strconv.ParseInt(unsafeString(val), 10, 64)
		if err != nil {
			builder.AppendNull()
			return nil
		}
		builder.(*array.Int64Builder).Append(n)

	case *arrow.Float64Type:
		f, err := strconv.ParseFloat(unsafeString(val), 64)
		if err != nil {
			builder.AppendNull()
			return nil
		}
		builder.(*array.Float64Builder).Append(f)

	case *arrow.StructType:
		st := fieldType.(*arrow.StructType)
		sb := builder.(*array.StructBuilder)
		sb.Append(true)
		for i, subField := range st.Fields() {
			if err := appendJSONValue(sb.FieldBuilder(i), subField.Type, val, subField.Name); err != nil {
				return err
			}
		}

	default:
		return fmt.Errorf("unsupported arrow type: %s", fieldType)
	}
	return nil
}

func (h *StructuredBatchHandler) Invoke(ctx context.Context) (arrow.Table, error) {
	raw := h.rawBatch
	h.rawBatch = h.rawBatch[:0]

	if len(raw) == 0 {
		return nil, fmt.Errorf("no records to invoke")
	}

	t0 := time.Now()

	// Create builders for each column
	builders := make([]array.Builder, h.schema.NumFields())
	for i, f := range h.schema.Fields() {
		builders[i] = array.NewBuilder(h.alloc, f.Type)
		builders[i].Reserve(len(raw))
	}

	// Schema-aware JSON extraction: only extract fields matching the schema
	fields := h.schema.Fields()
	for _, msg := range raw {
		for i, f := range fields {
			if err := appendJSONValue(builders[i], f.Type, msg, f.Name); err != nil {
				for _, b := range builders {
					b.Release()
				}
				return nil, fmt.Errorf("json extract error for field %q: %w", f.Name, err)
			}
		}
	}

	// Build arrays and create record
	arrays := make([]arrow.Array, len(builders))
	for i, b := range builders {
		arrays[i] = b.NewArray()
	}

	combined := array.NewRecord(h.schema, arrays, int64(len(raw)))
	for _, a := range arrays {
		a.Release()
	}

	t1 := time.Now()

	// Zero-copy Arrow ingest into DuckDB via ADBC
	if err := h.ingestStmt.Bind(ctx, combined); err != nil {
		combined.Release()
		return nil, fmt.Errorf("statement binding arrow record error: %v", err)
	}
	if _, err := h.ingestStmt.ExecuteUpdate(ctx); err != nil {
		combined.Release()
		return nil, fmt.Errorf("execute update error: %w", err)
	}
	combined.Release()

	t2 := time.Now()

	// Query results back using the user's SQL
	reader, _, err := h.queryStmt.ExecuteQuery(ctx)

	t3 := time.Now()
	h.logger.Debug("invoke timing",
		zap.Duration("json_parse", t1.Sub(t0)),
		zap.Duration("ingest", t2.Sub(t1)),
		zap.Duration("query", t3.Sub(t2)),
		zap.Duration("total", t3.Sub(t0)),
		zap.Int("batch_size", len(raw)),
	)
	if err != nil {
		return nil, fmt.Errorf("query execution error: %v", err)
	}
	defer reader.Release()

	var records []arrow.Record
	for reader.Next() {
		rec := reader.Record()
		rec.Retain()
		records = append(records, rec)
	}

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

	// Pre-create truncate statement
	truncStmt, err := conn.NewStatement()
	if err != nil {
		return nil, fmt.Errorf("new truncate statement: %w", err)
	}
	if err := truncStmt.SetSqlQuery(fmt.Sprintf("TRUNCATE TABLE %s;", tableName)); err != nil {
		return nil, fmt.Errorf("set truncate query: %w", err)
	}

	// Pre-create ingest statement with options set once
	ingestStmt, err := conn.NewStatement()
	if err != nil {
		return nil, fmt.Errorf("new ingest statement: %w", err)
	}
	if err := ingestStmt.SetOption(adbc.OptionKeyIngestMode, adbc.OptionValueIngestModeAppend); err != nil {
		return nil, fmt.Errorf("set ingest mode: %w", err)
	}
	if err := ingestStmt.SetOption(adbc.OptionKeyIngestTargetTable, tableName); err != nil {
		return nil, fmt.Errorf("set ingest target table: %w", err)
	}

	// Pre-create query statement with SQL set once
	queryStmt, err := conn.NewStatement()
	if err != nil {
		return nil, fmt.Errorf("new query statement: %w", err)
	}
	if err := queryStmt.SetSqlQuery(sql); err != nil {
		return nil, fmt.Errorf("set query sql: %w", err)
	}

	// Pre-compute field names
	fieldNames := make([]string, schema.NumFields())
	for i, f := range schema.Fields() {
		fieldNames[i] = f.Name
	}

	s := &StructuredBatchHandler{
		alloc:      pool,
		conn:       conn,
		truncStmt:  truncStmt,
		ingestStmt: ingestStmt,
		queryStmt:  queryStmt,
		schema:     schema,
		sql:        sql,
		tableName:  tableName,
		fieldNames: fieldNames,
		logger:     zap.NewNop(),
	}

	for _, opt := range opts {
		opt(s)
	}

	return s, nil
}
