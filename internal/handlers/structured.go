package handlers

import (
	"bytes"
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

// jsonString decodes a JSON string value. jsonparser returns the raw bytes
// between the quotes with escape sequences intact, so "a\nb" arrives as a
// backslash and an n; storing that verbatim silently corrupts the value
// rather than failing. The Python engine decodes with json.loads.
//
// A string containing no backslash -- the overwhelming majority -- needs no
// decoding and keeps the zero-copy path.
func jsonString(b []byte) string {
	if bytes.IndexByte(b, '\\') < 0 {
		return unsafeString(b)
	}
	s, err := jsonparser.ParseString(b)
	if err != nil {
		// Malformed escapes are not worth failing the batch over; the raw
		// bytes are still closer to the source than an empty string.
		return unsafeString(b)
	}
	return s
}

// appendJSONValue extracts a JSON value and appends it to the corresponding Arrow builder.
func appendJSONValue(builder array.Builder, fieldType arrow.DataType, data []byte, key string) error {
	val, dataType, _, err := jsonparser.Get(data, key)
	if err != nil || dataType == jsonparser.NotExist {
		builder.AppendNull()
		return nil
	}
	return appendValue(builder, fieldType, val, dataType)
}

// appendValue appends an already-extracted JSON value. List elements arrive
// this way: ArrayEach hands over the item itself, with no key to look up.
func appendValue(builder array.Builder, fieldType arrow.DataType, val []byte, dataType jsonparser.ValueType) error {
	switch fieldType.(type) {
	case *arrow.StringType:
		builder.(*array.StringBuilder).Append(jsonString(val))

	case *arrow.BooleanType:
		b, err := strconv.ParseBool(unsafeString(val))
		if err != nil {
			builder.AppendNull()
			return nil
		}
		builder.(*array.BooleanBuilder).Append(b)

	case *arrow.NullType:
		builder.AppendNull()

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

	case *arrow.ListType:
		lt := fieldType.(*arrow.ListType)
		lb := builder.(*array.ListBuilder)
		if dataType != jsonparser.Array {
			lb.AppendNull()
			return nil
		}
		// Append(true) opens the list; every value appended to the child
		// builder until the next Append belongs to this row's list.
		lb.Append(true)
		elem := lt.Elem()
		vb := lb.ValueBuilder()
		var cbErr error
		if _, err := jsonparser.ArrayEach(val, func(item []byte, ivt jsonparser.ValueType, _ int, _ error) {
			if cbErr != nil {
				return
			}
			cbErr = appendValue(vb, elem, item, ivt)
		}); err != nil {
			return err
		}
		if cbErr != nil {
			return cbErr
		}

	default:
		return fmt.Errorf("unsupported arrow type: %s", fieldType)
	}
	return nil
}

func (h *StructuredBatchHandler) Invoke(ctx context.Context) (arrow.Table, error) {
	raw := h.rawBatch
	h.rawBatch = h.rawBatch[:0]

	// An empty batch is a no-op, not an error. See the note on
	// InferredMemBatchHandler.Invoke.
	if len(raw) == 0 {
		return nil, nil
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

	// Re-prepared per batch, after the ingest. DuckDB's ADBC layer plans the
	// statement when the SQL is set and keeps that plan until the catalog
	// changes, which TRUNCATE and append never do. A plan built against the
	// empty table folds its statistics in: a WHERE over a column becomes
	// always-false and an expression over a list element becomes NULL, so
	// every batch afterwards returns nothing or one group.
	if err := h.queryStmt.SetSqlQuery(h.sql); err != nil {
		return nil, fmt.Errorf("set query sql: %w", err)
	}

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
