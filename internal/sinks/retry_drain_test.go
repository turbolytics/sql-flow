package sinks

// A sink that discards its buffer when the write fails cannot be retried: the
// ladder calls Flush again, the second call finds nothing to send and returns
// nil, and the caller is told a flush happened that delivered no rows. The
// pipeline then commits those offsets and the batch is gone with no error.
//
// The ladder cannot compensate for this -- only the sink knows what it failed
// to deliver -- so the invariant is tested here, on the sinks themselves:
//
//	a failed Flush leaves the batch buffered for the next attempt.

import (
	"context"
	"errors"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/zeebo/assert"
)

// mismatchedTable carries a column the target table does not have, which the
// server rejects. Any reliable flush failure would do.
func mismatchedTable(t *testing.T) arrow.Table {
	t.Helper()

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "no_such_column", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	b := array.NewRecordBuilder(memory.NewGoAllocator(), schema)
	defer b.Release()
	b.Field(0).(*array.Int64Builder).Append(1)

	rec := b.NewRecord()
	defer rec.Release()
	return array.NewTableFromRecords(schema, []arrow.Record{rec})
}

func TestSinkClickhouse_FailedFlushKeepsTheBatchBuffered(t *testing.T) {
	s := newLiveClickhouseSink(t, `CREATE TABLE %s (id UInt64) ENGINE = MergeTree() ORDER BY id`)

	ctx := context.Background()
	table := mismatchedTable(t)
	defer table.Release()

	assert.NoError(t, s.WriteTable(ctx, table))
	assert.Error(t, s.Flush(ctx))

	// Still buffered, so the next attempt sends these rows rather than
	// finding an empty buffer and reporting a success that delivered nothing.
	assert.Equal(t, 1, len(s.tables))

	// And the retry genuinely re-attempts them: it fails the same way rather
	// than returning nil.
	assert.Error(t, s.Flush(ctx))
	assert.Equal(t, 1, len(s.tables))
}

func TestSinkClickhouse_SuccessfulFlushClearsTheBuffer(t *testing.T) {
	s := newLiveClickhouseSink(t, `CREATE TABLE %s (
		timestamp DateTime,
		user_id   Int64,
		action    String,
		browser   String,
		score     Float64,
		active    Bool
	) ENGINE = MergeTree() ORDER BY user_id`)

	ctx := context.Background()
	table := clickhouseFixtureTable(t)
	defer table.Release()

	assert.NoError(t, s.WriteTable(ctx, table))
	assert.NoError(t, s.Flush(ctx))
	assert.Equal(t, 0, len(s.tables))
}

// The ladder's half of the contract: given a sink that keeps what it could not
// deliver, a retried flush delivers it rather than reporting a hollow success.
func TestSinkRetry_RetriedFlushDeliversTheBatch(t *testing.T) {
	sink := &flakySink{failures: 1, err: errors.New("connection reset by peer")}
	r := newRetrying(sink, testPolicy())

	ctx := context.Background()
	assert.NoError(t, r.WriteTable(ctx, nil))
	assert.NoError(t, r.Flush(ctx))

	assert.Equal(t, 2, sink.attempts)
	assert.Equal(t, 1, sink.delivered)
}

// A flush that gives up must not claim the rows were delivered.
func TestSinkRetry_ExhaustedLadderReportsTheFailure(t *testing.T) {
	sink := &flakySink{failures: 99, err: errors.New("connection reset by peer")}
	r := newRetrying(sink, testPolicy())

	ctx := context.Background()
	assert.NoError(t, r.WriteTable(ctx, nil))
	assert.Error(t, r.Flush(ctx))
	assert.Equal(t, 0, sink.delivered)
}
