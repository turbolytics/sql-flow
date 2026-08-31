package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"go.uber.org/zap"
)

// batchTableName is the table user SQL selects from. The Python engine
// exposes the batch under this name via a DuckDB replacement scan of the
// local `batch` variable; Go has no such mechanism, so the batch is
// materialized into a table of the same name.
const batchTableName = "batch"

// InferredMemBatchHandler buffers a batch of JSON messages in memory and
// infers the Arrow schema from the messages themselves, rather than
// requiring a pre-declared table.
type InferredMemBatchHandler struct {
	rows []map[string]any

	alloc      *memory.GoAllocator
	conn       adbc.Connection
	ingestStmt adbc.Statement
	logger     *zap.Logger
	sql        string
}

func NewInferredMemBatchHandler(
	conn adbc.Connection,
	sql string,
	opts ...InferredMemBatchHandlerOption,
) (*InferredMemBatchHandler, error) {
	ingestStmt, err := conn.NewStatement()
	if err != nil {
		return nil, fmt.Errorf("new ingest statement: %w", err)
	}
	if err := ingestStmt.SetOption(adbc.OptionKeyIngestMode, adbc.OptionValueIngestModeCreate); err != nil {
		return nil, fmt.Errorf("set ingest mode: %w", err)
	}
	if err := ingestStmt.SetOption(adbc.OptionKeyIngestTargetTable, batchTableName); err != nil {
		return nil, fmt.Errorf("set ingest target table: %w", err)
	}

	h := &InferredMemBatchHandler{
		alloc:      memory.NewGoAllocator(),
		conn:       conn,
		ingestStmt: ingestStmt,
		sql:        sql,
		logger:     zap.NewNop(),
	}

	for _, opt := range opts {
		opt(h)
	}

	return h, nil
}

type InferredMemBatchHandlerOption func(*InferredMemBatchHandler)

func InferredMemBatchWithLogger(l *zap.Logger) InferredMemBatchHandlerOption {
	return func(h *InferredMemBatchHandler) {
		h.logger = l
	}
}

func (h *InferredMemBatchHandler) Init(ctx context.Context) error {
	h.rows = h.rows[:0]
	return h.dropBatchTable(ctx)
}

func (h *InferredMemBatchHandler) dropBatchTable(ctx context.Context) error {
	stmt, err := h.conn.NewStatement()
	if err != nil {
		return err
	}
	defer stmt.Close()

	if err := stmt.SetSqlQuery("DROP TABLE IF EXISTS " + batchTableName); err != nil {
		return err
	}
	_, err = stmt.ExecuteUpdate(ctx)
	return err
}

func (h *InferredMemBatchHandler) Write(r []byte) error {
	// Decoded here rather than at Invoke so malformed JSON surfaces as a
	// per-message write error, which is what the error policies key off.
	// The decoded row is kept so Invoke does not parse the batch a second
	// time.
	var row map[string]any
	if err := decodeJSON(r, &row); err != nil {
		return fmt.Errorf("invalid json: %w", err)
	}
	h.rows = append(h.rows, row)
	return nil
}

// decodeJSON decodes with UseNumber so integers stay distinguishable from
// floats during schema inference.
func decodeJSON(data []byte, v any) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	return dec.Decode(v)
}

func (h *InferredMemBatchHandler) Invoke(ctx context.Context) (arrow.Table, error) {
	rows := h.rows
	h.rows = h.rows[:0]

	if len(rows) == 0 {
		return nil, fmt.Errorf("no records to invoke")
	}

	schema, err := inferSchema(rows)
	if err != nil {
		return nil, fmt.Errorf("schema inference: %w", err)
	}

	record, err := buildRecord(h.alloc, schema, rows)
	if err != nil {
		return nil, fmt.Errorf("build record: %w", err)
	}

	if err := h.ingestStmt.Bind(ctx, record); err != nil {
		record.Release()
		return nil, fmt.Errorf("bind arrow record: %w", err)
	}
	if _, err := h.ingestStmt.ExecuteUpdate(ctx); err != nil {
		record.Release()
		return nil, fmt.Errorf("ingest batch: %w", err)
	}
	record.Release()

	// The statement is created per invoke: ADBC prepares the SQL when it is
	// set, which requires the batch table to already exist.
	queryStmt, err := h.conn.NewStatement()
	if err != nil {
		return nil, fmt.Errorf("new query statement: %w", err)
	}
	defer queryStmt.Close()

	if err := queryStmt.SetSqlQuery(h.sql); err != nil {
		return nil, fmt.Errorf("set query sql: %w", err)
	}

	reader, _, err := queryStmt.ExecuteQuery(ctx)
	if err != nil {
		return nil, fmt.Errorf("query execution error: %w", err)
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

// sortedKeys gives field ordering a deterministic basis. Go maps lose the
// document's key order, so field order is alphabetical rather than
// first-seen-in-JSON as in the Python engine; SQL referencing columns by
// name is unaffected.
func sortedKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// inferSchema derives an Arrow schema from decoded JSON rows, mirroring
// pyarrow.Table.from_pylist: fields appear in first-seen order and a field's
// type comes from the first non-null value seen for it.
func inferSchema(rows []map[string]any) (*arrow.Schema, error) {
	var names []string
	types := map[string]arrow.DataType{}

	for _, row := range rows {
		for _, k := range sortedKeys(row) {
			if _, seen := types[k]; !seen {
				names = append(names, k)
				types[k] = nil
			}
			if types[k] == nil {
				dt, err := inferType(row[k])
				if err != nil {
					return nil, fmt.Errorf("field %q: %w", k, err)
				}
				types[k] = dt
			}
		}
	}

	fields := make([]arrow.Field, 0, len(names))
	for _, name := range names {
		dt := types[name]
		if dt == nil {
			// Every value was null; pyarrow yields a null column.
			dt = arrow.Null
		}
		fields = append(fields, arrow.Field{Name: name, Type: dt, Nullable: true})
	}

	return arrow.NewSchema(fields, nil), nil
}

// inferType maps a decoded JSON value to an Arrow type. A nil value yields a
// nil type, meaning "not yet known" — a later row may supply one.
func inferType(v any) (arrow.DataType, error) {
	switch t := v.(type) {
	case nil:
		return nil, nil
	case bool:
		return arrow.FixedWidthTypes.Boolean, nil
	case string:
		return arrow.BinaryTypes.String, nil
	case json.Number:
		if _, err := t.Int64(); err == nil {
			return arrow.PrimitiveTypes.Int64, nil
		}
		return arrow.PrimitiveTypes.Float64, nil
	case map[string]any:
		var subFields []arrow.Field
		for _, k := range sortedKeys(t) {
			dt, err := inferType(t[k])
			if err != nil {
				return nil, err
			}
			if dt == nil {
				dt = arrow.Null
			}
			subFields = append(subFields, arrow.Field{Name: k, Type: dt, Nullable: true})
		}
		return arrow.StructOf(subFields...), nil
	default:
		return nil, fmt.Errorf("unsupported json value of type %T", v)
	}
}

func buildRecord(alloc memory.Allocator, schema *arrow.Schema, rows []map[string]any) (arrow.Record, error) {
	builder := array.NewRecordBuilder(alloc, schema)
	defer builder.Release()

	for _, row := range rows {
		for i, f := range schema.Fields() {
			if err := appendValue(builder.Field(i), f.Type, row[f.Name]); err != nil {
				return nil, fmt.Errorf("field %q: %w", f.Name, err)
			}
		}
	}

	return builder.NewRecord(), nil
}

func appendValue(b array.Builder, dt arrow.DataType, v any) error {
	if v == nil {
		b.AppendNull()
		return nil
	}

	switch dt.(type) {
	case *arrow.BooleanType:
		val, ok := v.(bool)
		if !ok {
			b.AppendNull()
			return nil
		}
		b.(*array.BooleanBuilder).Append(val)

	case *arrow.StringType:
		switch val := v.(type) {
		case string:
			b.(*array.StringBuilder).Append(val)
		default:
			// A field typed as string by an earlier row: keep the row by
			// rendering the value rather than failing the whole batch.
			b.(*array.StringBuilder).Append(fmt.Sprintf("%v", val))
		}

	case *arrow.Int64Type:
		num, ok := v.(json.Number)
		if !ok {
			b.AppendNull()
			return nil
		}
		n, err := num.Int64()
		if err != nil {
			b.AppendNull()
			return nil
		}
		b.(*array.Int64Builder).Append(n)

	case *arrow.Float64Type:
		num, ok := v.(json.Number)
		if !ok {
			b.AppendNull()
			return nil
		}
		f, err := num.Float64()
		if err != nil {
			b.AppendNull()
			return nil
		}
		b.(*array.Float64Builder).Append(f)

	case *arrow.StructType:
		st := dt.(*arrow.StructType)
		sb := b.(*array.StructBuilder)
		obj, ok := v.(map[string]any)
		if !ok {
			sb.AppendNull()
			return nil
		}
		sb.Append(true)
		for i, sf := range st.Fields() {
			if err := appendValue(sb.FieldBuilder(i), sf.Type, obj[sf.Name]); err != nil {
				return err
			}
		}

	case *arrow.NullType:
		b.AppendNull()

	default:
		return fmt.Errorf("unsupported arrow type: %s", dt)
	}
	return nil
}
