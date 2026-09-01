package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/buger/jsonparser"
	"github.com/turbolytics/sql-flow/internal/core"
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
//
// Schema inference follows pyarrow.Table.from_pylist, which the Python engine
// uses: the column set and their order come from the first message, types are
// promoted across the batch, and a value that cannot be promoted fails the
// batch rather than being silently nulled.
type InferredMemBatchHandler struct {
	rawBatch [][]byte
	// Parallel to rawBatch, populated only when the source supplies
	// provenance; empty for sources that do not.
	metadata []core.Message

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
	h.rawBatch = h.rawBatch[:0]
	h.metadata = h.metadata[:0]
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
	// Validated here rather than at Invoke so malformed JSON surfaces as a
	// per-message write error, which is what the error policies key off.
	if !json.Valid(r) {
		return fmt.Errorf("invalid json: %q", truncate(r, 64))
	}
	h.rawBatch = append(h.rawBatch, r)
	return nil
}

// WriteMessage buffers a message along with its source metadata, which Invoke
// exposes as kafka_topic / kafka_partition / kafka_offset columns.
func (h *InferredMemBatchHandler) WriteMessage(msg core.Message) error {
	if err := h.Write(msg.Value); err != nil {
		return err
	}
	if msg.HasMetadata() {
		// Kept positionally against rawBatch, which Write just appended to.
		for len(h.metadata) < len(h.rawBatch)-1 {
			h.metadata = append(h.metadata, core.Message{})
		}
		h.metadata = append(h.metadata, msg)
	}
	return nil
}

func truncate(b []byte, n int) string {
	if len(b) <= n {
		return string(b)
	}
	return string(b[:n]) + "..."
}

func (h *InferredMemBatchHandler) Invoke(ctx context.Context) (arrow.Table, error) {
	raw := h.rawBatch
	h.rawBatch = h.rawBatch[:0]
	meta := h.metadata
	h.metadata = h.metadata[:0]

	// An empty batch is a no-op, not an error. The pipeline counts a message
	// it consumed even when the handler rejected it, so a batch whose
	// messages were all malformed arrives here with nothing buffered; there
	// is simply no table to produce. A nil table is what the caller already
	// handles for a failed invoke.
	if len(raw) == 0 {
		return nil, nil
	}

	schema, err := inferSchema(raw)
	if err != nil {
		return nil, fmt.Errorf("schema inference: %w", err)
	}
	schema = withMetadataFields(schema, len(meta) > 0)

	record, err := buildRecord(h.alloc, schema, raw, meta)
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

// inferSchema derives the batch schema from raw JSON messages. Column names
// and order come from the first message; types are promoted over the rest of
// the batch.
func inferSchema(msgs [][]byte) (*arrow.Schema, error) {
	fields, err := inferFields(msgs[0])
	if err != nil {
		return nil, err
	}

	for _, msg := range msgs[1:] {
		if err := promoteFields(fields, msg); err != nil {
			return nil, err
		}
	}

	arrowFields := make([]arrow.Field, len(fields))
	for i, f := range fields {
		arrowFields[i] = f.arrowField()
	}
	return arrow.NewSchema(arrowFields, nil), nil
}

// inferredField is a column discovered in the first message. Struct columns
// carry their own children, discovered the same way.
type inferredField struct {
	name     string
	dataType arrow.DataType
	children []*inferredField
}

func (f *inferredField) arrowField() arrow.Field {
	if len(f.children) > 0 {
		subFields := make([]arrow.Field, len(f.children))
		for i, c := range f.children {
			subFields[i] = c.arrowField()
		}
		return arrow.Field{Name: f.name, Type: arrow.StructOf(subFields...), Nullable: true}
	}
	return arrow.Field{Name: f.name, Type: f.dataType, Nullable: true}
}

func inferFields(msg []byte) ([]*inferredField, error) {
	var fields []*inferredField
	err := jsonparser.ObjectEach(msg, func(key, value []byte, vt jsonparser.ValueType, _ int) error {
		f := &inferredField{name: string(key)}
		switch vt {
		case jsonparser.Object:
			children, err := inferFields(value)
			if err != nil {
				return err
			}
			if len(children) == 0 {
				// An empty object has no inferable struct type.
				f.dataType = arrow.Null
			}
			f.children = children
		default:
			dt, err := jsonValueType(value, vt)
			if err != nil {
				return fmt.Errorf("field %q: %w", key, err)
			}
			f.dataType = dt
		}
		fields = append(fields, f)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return fields, nil
}

// promoteFields widens the inferred types to accommodate one more message.
func promoteFields(fields []*inferredField, msg []byte) error {
	for _, f := range fields {
		value, vt, _, err := jsonparser.Get(msg, f.name)
		if err != nil || vt == jsonparser.NotExist || vt == jsonparser.Null {
			continue
		}

		if len(f.children) > 0 {
			if vt != jsonparser.Object {
				return fmt.Errorf("field %q: cannot mix struct and non-struct values", f.name)
			}
			if err := promoteFields(f.children, value); err != nil {
				return err
			}
			continue
		}

		dt, err := jsonValueType(value, vt)
		if err != nil {
			return fmt.Errorf("field %q: %w", f.name, err)
		}

		promoted, err := promoteType(f.dataType, dt)
		if err != nil {
			return fmt.Errorf("field %q: %w", f.name, err)
		}
		f.dataType = promoted
	}
	return nil
}

func jsonValueType(value []byte, vt jsonparser.ValueType) (arrow.DataType, error) {
	switch vt {
	case jsonparser.String:
		return arrow.BinaryTypes.String, nil
	case jsonparser.Boolean:
		return arrow.FixedWidthTypes.Boolean, nil
	case jsonparser.Null:
		return arrow.Null, nil
	case jsonparser.Number:
		if bytes.ContainsAny(value, ".eE") {
			return arrow.PrimitiveTypes.Float64, nil
		}
		return arrow.PrimitiveTypes.Int64, nil
	case jsonparser.Object:
		return nil, fmt.Errorf("cannot mix struct and non-struct values")
	case jsonparser.Array:
		return nil, fmt.Errorf("unsupported json array value")
	default:
		return nil, fmt.Errorf("unsupported json value")
	}
}

// promoteType widens two observed types to one that holds both, mirroring
// pyarrow: ints widen to floats, a null-typed column takes the other type,
// and anything else conflicting is an error.
func promoteType(current, next arrow.DataType) (arrow.DataType, error) {
	if current == nil || arrow.TypeEqual(current, arrow.Null) {
		return next, nil
	}
	if arrow.TypeEqual(next, arrow.Null) || arrow.TypeEqual(current, next) {
		return current, nil
	}

	isInt := func(dt arrow.DataType) bool { return arrow.TypeEqual(dt, arrow.PrimitiveTypes.Int64) }
	isFloat := func(dt arrow.DataType) bool { return arrow.TypeEqual(dt, arrow.PrimitiveTypes.Float64) }

	if (isInt(current) && isFloat(next)) || (isFloat(current) && isInt(next)) {
		return arrow.PrimitiveTypes.Float64, nil
	}

	return nil, fmt.Errorf("cannot convert %s value to %s", next, current)
}

// buildRecord extracts each message directly into Arrow builders with
// jsonparser, the same zero-copy path StructuredBatch uses.
// withMetadataFields appends the Kafka provenance columns the Python engine
// injects into each row, so handler SQL can reference them.
func withMetadataFields(schema *arrow.Schema, withMetadata bool) *arrow.Schema {
	if !withMetadata {
		return schema
	}
	fields := append([]arrow.Field{}, schema.Fields()...)
	fields = append(fields,
		arrow.Field{Name: "kafka_topic", Type: arrow.BinaryTypes.String, Nullable: true},
		arrow.Field{Name: "kafka_partition", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		arrow.Field{Name: "kafka_offset", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	)
	return arrow.NewSchema(fields, nil)
}

func isMetadataField(name string) bool {
	return name == "kafka_topic" || name == "kafka_partition" || name == "kafka_offset"
}

func buildRecord(alloc memory.Allocator, schema *arrow.Schema, msgs [][]byte, meta []core.Message) (arrow.Record, error) {
	builders := make([]array.Builder, schema.NumFields())
	for i, f := range schema.Fields() {
		builders[i] = array.NewBuilder(alloc, f.Type)
		builders[i].Reserve(len(msgs))
	}
	defer func() {
		for _, b := range builders {
			b.Release()
		}
	}()

	fields := schema.Fields()
	for row, msg := range msgs {
		for i, f := range fields {
			if isMetadataField(f.Name) {
				appendMetadataValue(builders[i], f.Name, meta, row)
				continue
			}
			if err := appendJSONValue(builders[i], f.Type, msg, f.Name); err != nil {
				return nil, fmt.Errorf("field %q: %w", f.Name, err)
			}
		}
	}

	arrays := make([]arrow.Array, len(builders))
	for i, b := range builders {
		arrays[i] = b.NewArray()
	}
	defer func() {
		for _, a := range arrays {
			a.Release()
		}
	}()

	return array.NewRecord(schema, arrays, int64(len(msgs))), nil
}

// appendMetadataValue fills a provenance column, leaving it null for any row
// whose source did not supply metadata.
func appendMetadataValue(b array.Builder, name string, meta []core.Message, row int) {
	if row >= len(meta) || !meta[row].HasMetadata() {
		b.AppendNull()
		return
	}

	switch name {
	case "kafka_topic":
		b.(*array.StringBuilder).Append(meta[row].Topic)
	case "kafka_partition":
		b.(*array.Int32Builder).Append(meta[row].Partition)
	case "kafka_offset":
		b.(*array.Int64Builder).Append(meta[row].Offset)
	}
}
