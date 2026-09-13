package serve

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/errs"
	"github.com/turbolytics/sql-flow/internal/sqlparams"
)

// statement is one dataset statement, numbered and checked against DuckDB at
// startup.
type statement struct {
	dataset string
	// grain is empty for a dataset without grains.
	grain string
	// sql is the statement as the config wrote it.
	sql string
	// rewritten is sql with $name replaced by $N.
	rewritten string
	// schema has one field per placeholder, in number order, typed by the
	// declared param. A request's values bind into a record of this shape.
	schema *arrow.Schema
}

// where names the statement in an error.
func (s *statement) where() string {
	if s.grain == "" {
		return "dataset " + s.dataset
	}
	return "dataset " + s.dataset + " grain " + s.grain
}

// paramTypes maps a declared param type to the Arrow type it binds as. The
// type comes from the config, never from the value, so an absent param is a
// typed null and coalesce resolves against the right type.
var paramTypes = map[string]arrow.DataType{
	"string":    arrow.BinaryTypes.String,
	"integer":   arrow.PrimitiveTypes.Int64,
	"timestamp": &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"},
}

// prepare numbers a statement's placeholders and has DuckDB check it.
//
// DuckDB binds a statement when its SQL is set, so a syntax error, a missing
// table and a missing column all fail here, at startup, rather than on the
// first request. The statement is closed afterwards: each request plans its
// own, because a held plan can fold table statistics into constants.
func prepare(ctx context.Context, conn adbc.Connection, dataset, grain, sql string, params []config.ServeParam) (*statement, error) {
	st := &statement{dataset: dataset, grain: grain, sql: sql}

	rw, err := sqlparams.Rewrite(sql)
	if err != nil {
		return nil, errs.Wrap(errs.CodeConfigServeDataset, err, "%s", st.where())
	}
	st.rewritten = rw.SQL

	declared := map[string]string{}
	for _, p := range params {
		declared[p.Name] = p.Type
	}
	fields := make([]arrow.Field, len(rw.Names))
	for i, name := range rw.Names {
		typ, ok := paramTypes[declared[name]]
		if !ok {
			return nil, errs.New(errs.CodeConfigServeDataset,
				"%s: $%s is not a declared param", st.where(), name)
		}
		fields[i] = arrow.Field{Name: name, Type: typ, Nullable: true}
	}
	st.schema = arrow.NewSchema(fields, nil)

	stmt, err := conn.NewStatement()
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	if err := stmt.SetSqlQuery(rw.SQL); err != nil {
		return nil, errs.Wrap(errs.CodeSQLInvalid, err, "%s: the SQL does not prepare", st.where())
	}
	if err := stmt.Prepare(ctx); err != nil {
		return nil, errs.Wrap(errs.CodeSQLInvalid, err, "%s: the SQL does not prepare", st.where())
	}

	// The scanner and DuckDB must agree on the count. If they do not, the
	// scanner misread the SQL, and binding would put values on the wrong
	// placeholders and answer with wrong rows.
	ps, err := stmt.GetParameterSchema()
	if err != nil {
		return nil, errs.Wrap(errs.CodeSQLInvalid, err, "%s: reading the parameter count", st.where())
	}
	if ps.NumFields() != len(rw.Names) {
		return nil, errs.New(errs.CodeSQLInvalid,
			"%s: DuckDB counts %d parameters and sqlflow counts %d (%s); "+
				"a quote or comment form the scanner does not know is hiding or inventing one",
			st.where(), ps.NumFields(), len(rw.Names), strings.Join(rw.Names, ", "))
	}

	return st, nil
}

// query runs the statement with one request's values and encodes at most
// maxRows rows. values maps a param name to a string, int64 or time.Time; an
// absent name binds NULL.
func (s *statement) query(ctx context.Context, conn adbc.Connection, values map[string]any, maxRows int) (result, error) {
	stmt, err := conn.NewStatement()
	if err != nil {
		return result{}, err
	}
	defer stmt.Close()

	if err := stmt.SetSqlQuery(s.rewritten); err != nil {
		return result{}, err
	}

	// DuckDB wants exactly one field per placeholder, so a statement with
	// none binds nothing.
	if s.schema.NumFields() > 0 {
		rec := s.record(values)
		defer rec.Release()
		if err := stmt.Bind(ctx, rec); err != nil {
			return result{}, err
		}
	}

	rdr, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		return result{}, err
	}
	defer rdr.Release()

	return readRows(rdr, maxRows)
}

// record builds the one-row record a request binds.
func (s *statement) record(values map[string]any) arrow.RecordBatch {
	cols := make([]arrow.Array, s.schema.NumFields())
	for i, f := range s.schema.Fields() {
		v, present := values[f.Name]
		switch typ := f.Type.(type) {
		case *arrow.StringType:
			b := array.NewStringBuilder(memory.DefaultAllocator)
			if present {
				b.Append(v.(string))
			} else {
				b.AppendNull()
			}
			cols[i] = b.NewArray()
			b.Release()
		case *arrow.Int64Type:
			b := array.NewInt64Builder(memory.DefaultAllocator)
			if present {
				b.Append(v.(int64))
			} else {
				b.AppendNull()
			}
			cols[i] = b.NewArray()
			b.Release()
		case *arrow.TimestampType:
			b := array.NewTimestampBuilder(memory.DefaultAllocator, typ)
			if present {
				b.Append(arrow.Timestamp(v.(time.Time).UnixMicro()))
			} else {
				b.AppendNull()
			}
			cols[i] = b.NewArray()
			b.Release()
		default:
			panic(fmt.Sprintf("serve: no builder for param type %s", f.Type))
		}
	}

	rec := array.NewRecordBatch(s.schema, cols, 1)
	for _, c := range cols {
		c.Release()
	}
	return rec
}

// errClosed is returned for a query that arrives after Close.
var errClosed = errors.New("the server is shutting down")

// executor serializes every query on one connection.
//
// An ADBC connection is not safe for concurrent use, so one mutex guards it,
// the same shape as run's. DuckDB cannot cancel a query through the Go driver
// manager, so a deadline bounds the caller's wait, not the query: at the
// deadline the caller gets context.DeadlineExceeded, and the query keeps the
// lock until it finishes.
type executor struct {
	mu     sync.Mutex
	conn   adbc.Connection
	closed bool
}

type outcome struct {
	res result
	err error
}

// run calls fn with the connection under the lock, waiting at most timeout.
//
// fn runs on its own goroutine and returns finished bytes over a buffered
// channel. Nothing it produces touches the response, so a caller that has
// already given up cannot race it.
func (e *executor) run(ctx context.Context, timeout time.Duration, fn func(context.Context, adbc.Connection) (result, error)) (result, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	done := make(chan outcome, 1)
	go func() {
		e.mu.Lock()
		defer e.mu.Unlock()

		// The caller may have given up while this waited for the lock.
		// Running the query anyway would hold the connection for nobody.
		if err := ctx.Err(); err != nil {
			done <- outcome{err: err}
			return
		}
		if e.closed {
			done <- outcome{err: errClosed}
			return
		}
		res, err := fn(ctx, e.conn)
		done <- outcome{res: res, err: err}
	}()

	select {
	case o := <-done:
		return o.res, o.err
	case <-ctx.Done():
		return result{}, ctx.Err()
	}
}

// close waits for a running query, then refuses every later one. The caller
// closes the connection after this returns.
func (e *executor) close() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.closed = true
}
