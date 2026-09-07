package sinks

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/turbolytics/sql-flow/internal/core"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

// The premises behind every sink exemption in integrations.yml.
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

	// Nothing buffered, nothing reported, nothing to lose.
	batch, err := s.Batch()
	assert.NoError(t, err)
	assert.Nil(t, batch)

	// And it cannot fail, so there is never a batch to keep.
	assert.NoError(t, s.Flush(ctx))
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
