package sinks

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/url"
	"strings"
	"sync"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/turbolytics/sql-flow/internal/config"
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
		return nil, fmt.Errorf("clickhouse sink: table is required")
	}

	opts, err := clickhouseOptions(conf.DSN)
	if err != nil {
		return nil, err
	}

	conn, err := clickhouse.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("clickhouse sink: open: %w", err)
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
		return nil, fmt.Errorf("clickhouse sink: parse dsn: %w", err)
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
		return nil, fmt.Errorf("clickhouse sink: unsupported dsn scheme %q", u.Scheme)
	}

	host := u.Hostname()
	if host == "" {
		return nil, fmt.Errorf("clickhouse sink: dsn has no host: %q", dsn)
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
		return fmt.Errorf("clickhouse sink: prepare batch: %w", err)
	}

	if err := appendTables(batch, tables); err != nil {
		batch.Abort()
		return err
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("clickhouse sink: send batch: %w", err)
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

func appendTables(batch driver.Batch, tables []arrow.Table) error {
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

// arrowValue converts one Arrow cell to the Go value the driver's column
// appenders accept. A null becomes nil, which the driver stores as NULL in a
// Nullable column and as the column's zero value otherwise.
func arrowValue(arr arrow.Array, i int) (any, error) {
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
