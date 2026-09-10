package sinks

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/turbolytics/sql-flow/internal/core"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

// The premises behind every sink exemption in the registry.
//
// An exemption says an invariant cannot apply. Left as prose that is an
// excuse, and two such excuses hid live batch-loss bugs: sqlcommand and
// console were both exempted from sink.flush.keeps_batch on the grounds that
// nothing crosses a network. That is the argument for skipping a retry
// ladder, not for skipping the invariant a ladder depends on. Each exemption
// now names one of these tests, and the generator rejects one that does not.

func TestSinkConsole_ImplementsNoProber(t *testing.T) {
	coverage.Covers(t, "sink.console")
	var s core.Sink = NewConsoleSink()
	_, ok := s.(Prober)
	assert.That(t, !ok)
}

func TestSinkSqlcommand_ImplementsNoProber(t *testing.T) {
	coverage.Covers(t, "sink.sqlcommand")
	conn := newSinkTestConn(t)
	built, err := NewSQLCommandSink(conn, "SELECT 1", nil)
	assert.NoError(t, err)

	var s core.Sink = built
	_, ok := s.(Prober)
	assert.That(t, !ok)
}

func TestSinkNoop_ImplementsNoProber(t *testing.T) {
	coverage.Covers(t, "sink.noop")
	var s core.Sink = &NoopSink{}
	_, ok := s.(Prober)
	assert.That(t, !ok)
}

// the registry claimed sink.iceberg implements Prober and it never has.
// NewIcebergSink loads the catalog and the table, so an absent table fails the
// start -- but through the constructor, not through the interface sinks.New
// probes.
func TestSinkIceberg_ImplementsNoProber(t *testing.T) {
	coverage.Covers(t, "sink.iceberg")
	catalogName, tableName := newLocalIcebergTable(t)

	built, err := NewIcebergSink(context.Background(), catalogName, tableName)
	assert.NoError(t, err)

	var s core.Sink = built
	_, ok := s.(Prober)
	assert.That(t, !ok)
}

// The four sinks that hold nothing to release. lifecycle.close.idempotent is
// exempt for each, and these prove the premise: there is no Close to call
// twice. ClickHouse and Kafka own a client and do implement it.

func TestSinkConsole_ImplementsNoCloser(t *testing.T) {
	coverage.Covers(t, "sink.console")
	var s core.Sink = NewConsoleSink()
	_, ok := s.(io.Closer)
	assert.That(t, !ok)
}

func TestSinkSqlcommand_ImplementsNoCloser(t *testing.T) {
	coverage.Covers(t, "sink.sqlcommand")
	conn := newSinkTestConn(t)
	built, err := NewSQLCommandSink(conn, "SELECT 1", nil)
	assert.NoError(t, err)

	var s core.Sink = built
	_, ok := s.(io.Closer)
	assert.That(t, !ok)
}

func TestSinkNoop_ImplementsNoCloser(t *testing.T) {
	coverage.Covers(t, "sink.noop")
	var s core.Sink = &NoopSink{}
	_, ok := s.(io.Closer)
	assert.That(t, !ok)
}

func TestSinkIceberg_ImplementsNoCloser(t *testing.T) {
	coverage.Covers(t, "sink.iceberg")
	catalogName, tableName := newLocalIcebergTable(t)

	built, err := NewIcebergSink(context.Background(), catalogName, tableName)
	assert.NoError(t, err)

	var s core.Sink = built
	_, ok := s.(io.Closer)
	assert.That(t, !ok)
}

// The noop sink's contract, stated once rather than argued eight times.
//
// It exists so a benchmark can measure the engine without a sink, so every
// invariant about delivering rows is vacuous for it. That is only a
// legitimate exemption if discarding is deliberate and total, which is what
// this asserts.
func TestSinkNoop_DeliversNothingAndSaysSo(t *testing.T) {
	coverage.Covers(t, "sink.noop")
	s := &NoopSink{}
	ctx := context.Background()

	table := oneRowTable(t, 1)
	defer table.Release()

	assert.NoError(t, s.WriteTable(ctx, table))
	assert.NoError(t, s.Flush(ctx))

	// And it cannot fail, so there is never a batch to keep.
	assert.NoError(t, s.Flush(ctx))
}

// The three sinks below are exempt from sink.flush.honours_context, and these
// tests prove the premise rather than asserting it in prose.
//
// The claim is that Flush returns ctx.Err() when the context ends. A sink whose
// failing write returns immediately never reaches a deadline, so there is no
// context to honour. The conformance harness skips the claim for them, and
// the registry names these tests.
//
// Each one breaks the destination, flushes under a generous deadline, and holds
// that the flush failed while the context was still live.

func TestSinkConsole_FlushCannotOutliveItsContext(t *testing.T) {
	coverage.Covers(t, "sink.console")
	w := &breakableWriter{}
	s := NewConsoleSinkTo(w)

	table := oneRowTable(t, 1)
	defer table.Release()
	assert.NoError(t, s.WriteTable(context.Background(), table))
	w.set(true)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	assert.Error(t, s.Flush(ctx))
	assert.NoError(t, ctx.Err())
}

func TestSinkSqlcommand_FlushCannotOutliveItsContext(t *testing.T) {
	coverage.Covers(t, "sink.sqlcommand")
	conn := newSinkTestConn(t)
	target := fmt.Sprintf("outlive_target_%d", time.Now().UnixNano())
	exec(t, conn, "CREATE TABLE "+target+" (id BIGINT)")

	// The SQL names a table that does not exist, so the statement fails as soon
	// as DuckDB parses it.
	s, err := NewSQLCommandSink(conn,
		"INSERT INTO "+target+" SELECT b.id FROM "+sinkBatchTable+" b, missing_table", nil)
	assert.NoError(t, err)

	table := oneRowTable(t, 1)
	defer table.Release()
	assert.NoError(t, s.WriteTable(context.Background(), table))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	assert.Error(t, s.Flush(ctx))
	assert.NoError(t, ctx.Err())
}

func TestSinkIceberg_FlushCannotOutliveItsContext(t *testing.T) {
	coverage.Covers(t, "sink.iceberg")
	catalogName, tableName := newLocalIcebergTable(t)

	s, err := NewIcebergSink(context.Background(), catalogName, tableName)
	assert.NoError(t, err)

	// The table declares city and count. A batch of one int64 id does not
	// match it, so the append fails locally without reaching any storage.
	table := oneRowTable(t, 1)
	defer table.Release()
	assert.NoError(t, s.WriteTable(context.Background(), table))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	assert.Error(t, s.Flush(ctx))
	assert.NoError(t, ctx.Err())
}

func oneRowTable(t *testing.T, id int64) arrow.Table {
	t.Helper()

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	b := array.NewRecordBuilder(memory.NewGoAllocator(), schema)
	defer b.Release()
	b.Field(0).(*array.Int64Builder).Append(id)

	rec := b.NewRecord()
	defer rec.Release()
	return array.NewTableFromRecords(schema, []arrow.Record{rec})
}
