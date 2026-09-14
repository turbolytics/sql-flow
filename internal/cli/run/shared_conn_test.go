package run

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/core"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/turbolytics/sql-flow/internal/duckdb"
	"github.com/turbolytics/sql-flow/internal/handlers"
	"github.com/turbolytics/sql-flow/internal/sinks"
	"github.com/zeebo/assert"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.uber.org/zap"
)

// sliceSource delivers fixed batches and then closes its stream.
type sliceSource struct {
	batches [][]core.Message
}

func (s *sliceSource) Start() error  { return nil }
func (s *sliceSource) Commit() error { return nil }
func (s *sliceSource) Close() error  { return nil }

func (s *sliceSource) Stream() <-chan []core.Message {
	ch := make(chan []core.Message, len(s.batches))
	for _, b := range s.batches {
		ch <- b
	}
	close(ch)
	return ch
}

func sharedConnCount(t *testing.T, conn adbc.Connection, table string) int64 {
	t.Helper()
	stmt, err := conn.NewStatement()
	assert.NoError(t, err)
	defer stmt.Close()
	assert.NoError(t, stmt.SetSqlQuery("SELECT count(*) FROM "+table))
	reader, _, err := stmt.ExecuteQuery(context.Background())
	assert.NoError(t, err)
	defer reader.Release()
	assert.That(t, reader.Next())
	return reader.Record().Column(0).(*array.Int64).Value(0)
}

// Every party that runs statements on the pipeline's DuckDB connection holds
// the shared lock while it does: the handler, the pipeline sink, the DLQ sink,
// the progress store, and each table manager with its own sink.
//
// DuckDB closes a pending result the moment another statement runs on its
// connection. A party that skips the lock fails whichever query another party
// has in flight, with "Attempting to execute an unsuccessful or closed pending
// query result". #280: the handler reset and the progress write ran outside
// the lock, the Bluesky demo failed a window poll at batch 500, and at batch 1
// the failure recurred every few minutes. On main a failed poll stops the
// process.
//
// The race has no deterministic reproduction, so this checks the invariant on
// every statement instead. The components are built by the same functions the
// run command calls, over a connection that records any execution finding the
// lock free. Nothing else touches the connection while the loop and the poll
// run, so a free lock means a party let go of it.
//
// It covers the two places that build a sqlcommand sink on the shared
// connection, the pipeline's and the DLQ's. sinks.New refuses to build one
// without the lock; this test is what shows the sink then uses it. The window
// manager is built beside them and polls, but it runs on connections of its
// own since #281, so it is not a party to the shared one: the lock checker
// never sees it, and that is the point.
func TestPipelineSharedConnection_EveryPartyHoldsTheLock(t *testing.T) {
	coverage.Covers(t, "manager.window", "sink.sqlcommand", "error.dlq", "handler.inferred_mem")
	ctx := context.Background()

	db, raw := rowsTestDB(t)
	rowsTestExec(t, raw, "CREATE TABLE win (bucket TIMESTAMPTZ, n BIGINT)")
	rowsTestExec(t, raw, "CREATE TABLE published (bucket TIMESTAMPTZ, n BIGINT)")
	rowsTestExec(t, raw, "CREATE TABLE dead (error VARCHAR, message VARCHAR, phase VARCHAR, timestamp VARCHAR)")

	lock := &sync.Mutex{}
	conn := duckdb.NewLockChecked(raw, lock)
	mp := sdkmetric.NewMeterProvider()

	conf := &config.Conf{
		Tables: &config.Tables{
			SQL: []config.TableSQL{{
				Name: "win",
				// One-second buckets with no grace: a bucket closes once a
				// later one exists.
				Window: &config.Window{
					TimeColumn:       "bucket",
					SizeSeconds:      1,
					LateRows:         "drop",
					PollIntervalSecs: 3600,
					EmitSQL:          "SELECT bucket, sum(n)::BIGINT AS n FROM closed GROUP BY ALL",
					Sink: config.Sink{Type: "sqlcommand", SQLCommand: &config.SQLCommandSink{
						SQL: "INSERT INTO published SELECT bucket, n FROM sqlflow_sink_batch",
					}},
				},
			}},
		},
	}
	conf.Pipeline.Handler = config.Handler{
		Type: "handlers.InferredMemBatch",
		SQL:  "SELECT to_timestamp(a) AS bucket, count(*)::BIGINT AS n FROM batch GROUP BY ALL",
	}
	conf.Pipeline.Sink = config.Sink{Type: "sqlcommand", SQLCommand: &config.SQLCommandSink{
		SQL: "INSERT INTO win SELECT bucket, n FROM sqlflow_sink_batch",
	}}
	conf.Pipeline.OnError = &config.OnError{
		Policy: "DLQ",
		DLQ: &config.Sink{Type: "sqlcommand", SQLCommand: &config.SQLCommandSink{
			SQL: "INSERT INTO dead SELECT * FROM sqlflow_sink_batch",
		}},
	}

	// Startup runs before any second goroutine exists, and the run command
	// does it without the lock. Holding it here keeps the check about the
	// running pipeline.
	progress := core.NewProgressStore(conn)
	lock.Lock()
	assert.NoError(t, progress.Init(ctx))
	assert.NoError(t, initWindowStores(ctx, conf, conn))
	lock.Unlock()

	sink, err := newPipelineSink(ctx, conf, conn, lock, mp, sinks.RetryEvents{}, zap.NewNop())
	assert.NoError(t, err)
	policies, err := newErrorPolicies(ctx, conf, conn, lock, mp, sinks.RetryEvents{}, zap.NewNop())
	assert.NoError(t, err)
	handler, err := handlers.New(conn, conf.Pipeline.Handler, zap.NewNop())
	assert.NoError(t, err)
	managed, closeConns, err := buildManagedTables(ctx, conf, db, zap.NewNop(), mp,
		nil, sinks.RetryEvents{})
	assert.NoError(t, err)
	defer closeConns()
	assert.Equal(t, 1, len(managed))

	msg := func(v string) core.Message { return core.Message{Value: []byte(v)} }
	src := &sliceSource{batches: [][]core.Message{
		{msg(`{"a": 1}`), msg(`{"a": 1}`)},
		{msg(`not json`), msg(`{"a": 2}`)},
		{msg(`{"a": 2}`), msg(`{"a": 3}`)},
	}}
	tb := core.NewTurbine(src, handler, sink, 2, time.Hour, lock, policies,
		core.WithProgressStore(progress))

	_, err = tb.ConsumeLoop(ctx, 6)
	assert.NoError(t, err)
	assert.NoError(t, managed[0].Poll(ctx))

	// Every party ran at least once, or a clean result proves nothing. The
	// window closed the two buckets a later one exists for, and the newest
	// stays open.
	assert.Equal(t, int64(2), sharedConnCount(t, raw, "published"))
	assert.Equal(t, int64(1), sharedConnCount(t, raw, "dead"))

	if v := conn.Violations(); len(v) > 0 {
		t.Fatalf("%d statements ran on the shared connection without the lock:\n%s",
			len(v), strings.Join(v, "\n"))
	}
}
