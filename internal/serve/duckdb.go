package serve

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/turbolytics/sql-flow/internal/duckdb"
	"github.com/turbolytics/sql-flow/internal/errs"
	"github.com/turbolytics/sql-flow/internal/sqlparams"
)

// duckdbExecutor runs datasets on DuckDB over ADBC. It is the only type in
// this package that names either.
type duckdbExecutor struct {
	*pool
	db *duckdb.DB
	// setup is the connection the config's commands ran on, kept because
	// Prepare needs a connection and every other one is in the pool. It is
	// idle afterwards, which costs nothing measurable.
	setup adbc.Connection
	// closeOnce keeps a second Close from closing setup twice: the server
	// owns the executor, and a test may close both.
	closeOnce sync.Once
}

// NewDuckDBExecutor opens size sessions on db and returns an Executor over
// them.
//
// init runs once, on a connection of its own, before any session is used. The
// caller passes the config's commands: ATTACH is database-wide and attaching
// the same alias twice is an error, so they must not run per session.
//
// Every session is then pinned to UTC. That is serve's own contract rather
// than the config's: SET TimeZone is session-scoped, so a session that missed
// it evaluates date_trunc and naive casts in the host's zone and returns
// wrong buckets from a correct config.
//
// onWait receives how long each Acquire waited; it may be nil.
func NewDuckDBExecutor(ctx context.Context, db *duckdb.DB, size int,
	init func(context.Context, adbc.Connection) error, onWait func(time.Duration)) (Executor, error) {
	setup, err := db.Connect(ctx)
	if err != nil {
		return nil, err
	}
	if init != nil {
		if err := init(ctx, setup); err != nil {
			_ = setup.Close()
			return nil, err
		}
	}

	backends := make([]backendSession, 0, size)
	for i := 0; i < size; i++ {
		conn, err := db.Connect(ctx)
		if err != nil {
			closeBackends(backends)
			_ = setup.Close()
			return nil, err
		}
		if err := execOn(ctx, conn, "SET TimeZone='UTC'"); err != nil {
			_ = conn.Close()
			closeBackends(backends)
			_ = setup.Close()
			return nil, fmt.Errorf("pinning the session timezone: %w", err)
		}
		backends = append(backends, &duckdbSession{conn: conn})
	}

	return &duckdbExecutor{pool: newPool(backends, onWait), db: db, setup: setup}, nil
}

// closeBackends closes the sessions opened before one failed, so a failed
// start leaves no connection behind.
func closeBackends(backends []backendSession) {
	for _, b := range backends {
		_ = b.close()
	}
}

// execOn runs one statement for its effect.
func execOn(ctx context.Context, conn adbc.Connection, sql string) error {
	stmt, err := conn.NewStatement()
	if err != nil {
		return err
	}
	defer stmt.Close()
	if err := stmt.SetSqlQuery(sql); err != nil {
		return err
	}
	_, err = stmt.ExecuteUpdate(ctx)
	return err
}

func (e *duckdbExecutor) Close() {
	e.closeOnce.Do(func() {
		e.pool.Close()
		_ = e.setup.Close()
	})
}

// duckdbSession is one ADBC connection.
type duckdbSession struct{ conn adbc.Connection }

func (d *duckdbSession) close() error { return d.conn.Close() }

func (d *duckdbSession) run(ctx context.Context, st Statement, values map[string]any) (array.RecordReader, error) {
	s, ok := st.(*duckdbStatement)
	if !ok {
		return nil, fmt.Errorf("serve: statement %T did not come from this executor", st)
	}

	stmt, err := d.conn.NewStatement()
	if err != nil {
		return nil, err
	}
	// The statement outlives this call through the reader, so it closes when
	// the reader is released rather than here.
	if err := stmt.SetSqlQuery(s.rewritten); err != nil {
		_ = stmt.Close()
		return nil, err
	}
	// DuckDB wants exactly one field per placeholder, so a statement with
	// none binds nothing.
	if s.schema.NumFields() > 0 {
		rec := s.record(values)
		err := stmt.Bind(ctx, rec)
		rec.Release()
		if err != nil {
			_ = stmt.Close()
			return nil, err
		}
	}
	rdr, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		_ = stmt.Close()
		return nil, err
	}
	return &closingReader{RecordReader: rdr, stmt: stmt}, nil
}

// closingReader closes the statement when the reader is released, so a
// caller that only knows about the reader leaks neither.
type closingReader struct {
	array.RecordReader
	stmt adbc.Statement
}

func (c *closingReader) Release() {
	c.RecordReader.Release()
	_ = c.stmt.Close()
}

// duckdbStatement is one dataset statement, numbered and checked against
// DuckDB at startup.
type duckdbStatement struct {
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

// Where names the statement in an error.
func (s *duckdbStatement) Where() string {
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

// Prepare checks one statement against DuckDB at startup. DuckDB binds a
// statement when its SQL is set, so a syntax error, a missing table and a
// missing column all fail here rather than on the first request.
//
// The statement is closed afterwards: each request plans its own, because a
// held plan can fold table statistics into constants.
func (e *duckdbExecutor) Prepare(ctx context.Context, spec StatementSpec) (Statement, error) {
	st := &duckdbStatement{dataset: spec.Dataset, grain: spec.Grain, sql: spec.SQL}

	rw, err := sqlparams.Rewrite(spec.SQL)
	if err != nil {
		return nil, errs.Wrap(errs.CodeConfigServeDataset, err, "%s", st.Where())
	}
	st.rewritten = rw.SQL

	declared := map[string]string{}
	for _, p := range spec.Params {
		declared[p.Name] = p.Type
	}
	fields := make([]arrow.Field, len(rw.Names))
	for i, name := range rw.Names {
		typ, ok := paramTypes[declared[name]]
		if !ok {
			return nil, errs.New(errs.CodeConfigServeDataset,
				"%s: $%s is not a declared param", st.Where(), name)
		}
		fields[i] = arrow.Field{Name: name, Type: typ, Nullable: true}
	}
	st.schema = arrow.NewSchema(fields, nil)

	stmt, err := e.setup.NewStatement()
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	if err := stmt.SetSqlQuery(rw.SQL); err != nil {
		return nil, errs.Wrap(errs.CodeSQLInvalid, err, "%s: the SQL does not prepare", st.Where())
	}
	if err := stmt.Prepare(ctx); err != nil {
		return nil, errs.Wrap(errs.CodeSQLInvalid, err, "%s: the SQL does not prepare", st.Where())
	}

	// The scanner and DuckDB must agree on the count. If they do not, the
	// scanner misread the SQL, and binding would put values on the wrong
	// placeholders and answer with wrong rows.
	ps, err := stmt.GetParameterSchema()
	if err != nil {
		return nil, errs.Wrap(errs.CodeSQLInvalid, err, "%s: reading the parameter count", st.Where())
	}
	if ps.NumFields() != len(rw.Names) {
		return nil, errs.New(errs.CodeSQLInvalid,
			"%s: DuckDB counts %d parameters and sqlflow counts %d (%s); "+
				"a quote or comment form the scanner does not know is hiding or inventing one",
			st.Where(), ps.NumFields(), len(rw.Names), strings.Join(rw.Names, ", "))
	}

	return st, nil
}

// record builds the one-row record a request binds.
func (s *duckdbStatement) record(values map[string]any) arrow.RecordBatch {
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
