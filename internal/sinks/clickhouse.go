package sinks

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/errs"
)

// ClickhouseSink inserts result batches into a ClickHouse table.
//
// The Python sink hands the Arrow table to clickhouse_connect's insert_arrow,
// which maps Arrow columns to table columns by name. clickhouse-go has no
// Arrow entry point, so the batch is unpacked into rows against an explicit
// column list taken from the Arrow schema, which preserves that name-based
// mapping.
type ClickhouseSink struct {
	conn  driver.Conn
	table string

	mu     sync.Mutex
	tables []arrow.Table
}

func NewClickhouseSink(conf config.ClickhouseSink) (*ClickhouseSink, error) {
	if strings.TrimSpace(conf.Table) == "" {
		return nil, errs.New(errs.CodeSinkInvalid, "clickhouse sink: table is required")
	}

	opts, err := clickhouseOptions(conf.DSN)
	if err != nil {
		return nil, err
	}

	conn, err := clickhouse.Open(opts)
	if err != nil {
		return nil, errs.Wrap(errs.CodeSinkUnreachable, err, "clickhouse sink: open")
	}

	return &ClickhouseSink{conn: conn, table: conf.Table}, nil
}

// clickhouseOptions translates a Python-style dsn, e.g.
// clickhouse://user:pass@host:8123/db, into driver options. The Python engine
// reaches ClickHouse over clickhouse_connect, which speaks the HTTP interface
// exclusively, so a dsn port is an HTTP port unless the scheme asks for the
// native protocol.
func clickhouseOptions(dsn string) (*clickhouse.Options, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return nil, errs.Wrap(errs.CodeSinkInvalid, err, "clickhouse sink: parse dsn")
	}

	var (
		protocol clickhouse.Protocol
		secure   bool
	)
	switch u.Scheme {
	case "clickhouse", "http":
		protocol = clickhouse.HTTP
	case "clickhouses", "https":
		protocol, secure = clickhouse.HTTP, true
	case "tcp", "native":
		protocol = clickhouse.Native
	case "natives":
		protocol, secure = clickhouse.Native, true
	default:
		return nil, errs.New(errs.CodeSinkInvalid, "clickhouse sink: unsupported dsn scheme %q", u.Scheme)
	}

	host := u.Hostname()
	if host == "" {
		return nil, errs.New(errs.CodeSinkInvalid, "clickhouse sink: dsn has no host: %q", dsn)
	}

	port := u.Port()
	if port == "" {
		port = defaultClickhousePort(protocol, secure)
	}

	username := u.User.Username()
	if username == "" {
		username = "default"
	}
	password, _ := u.User.Password()

	database := strings.TrimPrefix(u.Path, "/")
	if database == "" {
		database = "default"
	}

	opts := &clickhouse.Options{
		Addr:     []string{net.JoinHostPort(host, port)},
		Protocol: protocol,
		Auth: clickhouse.Auth{
			Database: database,
			Username: username,
			Password: password,
		},
	}
	if secure {
		opts.TLS = &tls.Config{}
	}
	return opts, nil
}

func defaultClickhousePort(protocol clickhouse.Protocol, secure bool) string {
	switch {
	case protocol == clickhouse.HTTP && secure:
		return "8443"
	case protocol == clickhouse.HTTP:
		return "8123"
	case secure:
		return "9440"
	default:
		return "9000"
	}
}

func (s *ClickhouseSink) WriteTable(batch arrow.Table) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	batch.Retain()
	s.tables = append(s.tables, batch)
	return nil
}

// Batch returns nothing. The Python ClickhouseSink alone among the sinks
// reports no batch: rows go straight to ClickHouse and are not held for a
// downstream reader.
func (s *ClickhouseSink) Batch() (arrow.Table, error) {
	return nil, nil
}

func (s *ClickhouseSink) Flush() error {
	s.mu.Lock()
	tables := s.tables
	s.tables = nil
	s.mu.Unlock()

	if len(tables) == 0 {
		return nil
	}
	defer func() {
		for _, t := range tables {
			t.Release()
		}
	}()

	// A handler whose query matched nothing yields an empty, column-less
	// table; there is no INSERT to build from it.
	tables = withRows(tables)
	if len(tables) == 0 {
		return nil
	}

	schema := tables[0].Schema()
	for _, t := range tables[1:] {
		if !t.Schema().Equal(schema) {
			return fmt.Errorf("clickhouse sink: buffered batches have differing schemas")
		}
	}

	ctx := context.Background()
	batch, err := s.conn.PrepareBatch(ctx, s.insertStatement(schema))
	if err != nil {
		return errs.Wrap(errs.CodeSinkWriteFailed, err, "clickhouse sink: prepare batch")
	}

	// The prepared batch knows the target column types, which the Arrow
	// schema does not; appendTables needs them to spot temporal columns.
	types := make([]column.Type, len(batch.Columns()))
	for i, c := range batch.Columns() {
		types[i] = c.Type()
	}

	if err := appendTables(batch, types, tables); err != nil {
		batch.Abort()
		return err
	}

	if err := batch.Send(); err != nil {
		return errs.Wrap(errs.CodeSinkWriteFailed, err, "clickhouse sink: send batch")
	}
	return nil
}

func (s *ClickhouseSink) Close() error {
	return s.conn.Close()
}

// insertStatement names the columns explicitly so the Arrow schema, not the
// table's column order, decides where each value lands.
func (s *ClickhouseSink) insertStatement(schema *arrow.Schema) string {
	cols := make([]string, schema.NumFields())
	for i, f := range schema.Fields() {
		cols[i] = "`" + strings.ReplaceAll(f.Name, "`", "``") + "`"
	}
	return fmt.Sprintf("INSERT INTO %s (%s)", s.table, strings.Join(cols, ", "))
}

// withRows drops the tables that would contribute nothing to the insert. The
// returned tables are still owned by the caller.
func withRows(tables []arrow.Table) []arrow.Table {
	kept := tables[:0:0]
	for _, t := range tables {
		if t.NumRows() > 0 && t.NumCols() > 0 {
			kept = append(kept, t)
		}
	}
	return kept
}

func appendTables(batch driver.Batch, types []column.Type, tables []arrow.Table) error {
	for _, table := range tables {
		reader := array.NewTableReader(table, 0)

		for reader.Next() {
			rec := reader.Record()
			row := make([]any, rec.NumCols())

			for i := int64(0); i < rec.NumRows(); i++ {
				for c := range row {
					v, err := arrowValue(rec.Column(c), int(i))
					if err != nil {
						reader.Release()
						return fmt.Errorf("clickhouse sink: column %q: %w", rec.ColumnName(c), err)
					}
					if s, ok := v.(string); ok && c < len(types) {
						if t, ok := temporalFromString(types[c], s); ok {
							v = t
						}
					}
					row[c] = v
				}
				if err := batch.Append(row...); err != nil {
					reader.Release()
					return fmt.Errorf("clickhouse sink: append row: %w", err)
				}
			}
		}

		err := reader.Err()
		reader.Release()
		if err != nil {
			return err
		}
	}
	return nil
}

// temporalFromString parses a string bound for a Date/DateTime column, with a
// zone-less value read as UTC.
//
// Left to the driver, a zone-less string is parsed in time.Local
// (lib/column/datetime.go, date.go), so a JSON timestamp the handler passed
// through without a CAST was stored shifted by the SQLFlow host's UTC offset --
// "12:00:00" from a UTC-4 laptop landed as 16:00:00. The Python engine sends
// the string to the server, which parses it as UTC; this matches that. A
// string carrying an explicit offset is honoured as written.
//
// The layouts are the driver's own, so anything it accepted before is still
// accepted; a string that matches none is handed to the driver unchanged to
// report as it always has.
func temporalFromString(colType column.Type, s string) (time.Time, bool) {
	var withZone, noZone string
	switch base := baseColumnType(colType); {
	case strings.HasPrefix(base, "DateTime64"):
		withZone, noZone = "2006-01-02 15:04:05.999999999 -07:00", "2006-01-02 15:04:05.999999999"
	case strings.HasPrefix(base, "DateTime"):
		withZone, noZone = "2006-01-02 15:04:05 -07:00", "2006-01-02 15:04:05"
	case base == "Date" || base == "Date32":
		withZone, noZone = "2006-01-02 -07:00", "2006-01-02"
	default:
		return time.Time{}, false
	}

	if t, err := time.Parse(withZone, s); err == nil {
		return t, true
	}
	if t, err := time.ParseInLocation(noZone, s, time.UTC); err == nil {
		return t, true
	}
	return time.Time{}, false
}

// baseColumnType strips the Nullable(...) and LowCardinality(...) wrappers so
// the underlying type can be matched.
func baseColumnType(t column.Type) string {
	s := string(t)
	for {
		switch {
		case strings.HasPrefix(s, "Nullable(") && strings.HasSuffix(s, ")"):
			s = s[len("Nullable(") : len(s)-1]
		case strings.HasPrefix(s, "LowCardinality(") && strings.HasSuffix(s, ")"):
			s = s[len("LowCardinality(") : len(s)-1]
		default:
			return s
		}
	}
}

// arrowValue converts one Arrow cell to the Go value the driver's column
// appenders accept. A null becomes nil, which the driver stores as NULL in a
// Nullable column and as the column's zero value otherwise.
func arrowValue(arr arrow.Array, i int) (any, error) {
	// Lists are handled ahead of the null check: ClickHouse's Array(T) is not
	// nullable and defaults to the empty array, so a null list must append an
	// empty slice rather than nil.
	if l, ok := arr.(*array.List); ok {
		return arrowListValue(l, i)
	}

	if arr.IsNull(i) {
		return nil, nil
	}

	switch a := arr.(type) {
	case *array.Boolean:
		return a.Value(i), nil
	case *array.Int8:
		return a.Value(i), nil
	case *array.Int16:
		return a.Value(i), nil
	case *array.Int32:
		return a.Value(i), nil
	case *array.Int64:
		return a.Value(i), nil
	case *array.Uint8:
		return a.Value(i), nil
	case *array.Uint16:
		return a.Value(i), nil
	case *array.Uint32:
		return a.Value(i), nil
	case *array.Uint64:
		return a.Value(i), nil
	case *array.Float32:
		return a.Value(i), nil
	case *array.Float64:
		return a.Value(i), nil
	case *array.String:
		return a.Value(i), nil
	case *array.LargeString:
		return a.Value(i), nil
	case *array.Binary:
		return a.Value(i), nil
	case *array.LargeBinary:
		return a.Value(i), nil
	case *array.Timestamp:
		return a.Value(i).ToTime(a.DataType().(*arrow.TimestampType).Unit), nil
	case *array.Date32:
		return a.Value(i).ToTime(), nil
	case *array.Date64:
		return a.Value(i).ToTime(), nil
	default:
		return nil, fmt.Errorf("unsupported arrow type %s", arr.DataType())
	}
}

// arrowListValue converts one list cell into a Go slice for an Array(T)
// column. The slice is typed from the Arrow element type rather than built as
// []any, because the driver matches an Array column's element type against
// the slice's own element type; an []any is rejected, and an empty list has
// no element to infer a type from.
func arrowListValue(l *array.List, row int) (any, error) {
	values := l.ListValues()

	var start, end int64
	if !l.IsNull(row) {
		start, end = l.ValueOffsets(row)
	}

	out := reflect.MakeSlice(goSliceType(l.DataType().(*arrow.ListType).Elem()), 0, int(end-start))
	for i := start; i < end; i++ {
		v, err := arrowValue(values, int(i))
		if err != nil {
			return nil, err
		}
		if v == nil {
			out = reflect.Append(out, reflect.Zero(out.Type().Elem()))
			continue
		}
		rv := reflect.ValueOf(v)
		if !rv.Type().AssignableTo(out.Type().Elem()) {
			return nil, fmt.Errorf("list element %s is not assignable to %s", rv.Type(), out.Type().Elem())
		}
		out = reflect.Append(out, rv)
	}
	return out.Interface(), nil
}

// goSliceType maps an Arrow element type to the Go slice type the driver
// expects for Array(T), recursing so a nested list becomes [][]T.
func goSliceType(elem arrow.DataType) reflect.Type {
	return reflect.SliceOf(goElemType(elem))
}

func goElemType(dt arrow.DataType) reflect.Type {
	switch t := dt.(type) {
	case *arrow.ListType:
		return goSliceType(t.Elem())
	case *arrow.BooleanType:
		return reflect.TypeOf(false)
	case *arrow.Int8Type:
		return reflect.TypeOf(int8(0))
	case *arrow.Int16Type:
		return reflect.TypeOf(int16(0))
	case *arrow.Int32Type:
		return reflect.TypeOf(int32(0))
	case *arrow.Int64Type:
		return reflect.TypeOf(int64(0))
	case *arrow.Uint8Type:
		return reflect.TypeOf(uint8(0))
	case *arrow.Uint16Type:
		return reflect.TypeOf(uint16(0))
	case *arrow.Uint32Type:
		return reflect.TypeOf(uint32(0))
	case *arrow.Uint64Type:
		return reflect.TypeOf(uint64(0))
	case *arrow.Float32Type:
		return reflect.TypeOf(float32(0))
	case *arrow.Float64Type:
		return reflect.TypeOf(float64(0))
	case *arrow.TimestampType, *arrow.Date32Type, *arrow.Date64Type:
		return reflect.TypeOf(time.Time{})
	case *arrow.BinaryType, *arrow.LargeBinaryType:
		return reflect.TypeOf([]byte(nil))
	default:
		// String and anything else the element switch renders as a string.
		return reflect.TypeOf("")
	}
}
