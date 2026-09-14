package core

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/turbolytics/sql-flow/internal/duckdb"
	"github.com/zeebo/assert"
)

// connHandler is a fakeHandler that touches the connection the way the real
// handlers do: Init resets a batch table and Invoke queries one. The pipeline
// decides whether either runs under the lock, and the fake handler that
// touches nothing hides that decision.
type connHandler struct {
	fakeHandler
	conn adbc.Connection
}

func (h *connHandler) exec(ctx context.Context, sql string) error {
	stmt, err := h.conn.NewStatement()
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

func (h *connHandler) Init(ctx context.Context) error {
	if err := h.fakeHandler.Init(ctx); err != nil {
		return err
	}
	return h.exec(ctx, "DROP TABLE IF EXISTS batch")
}

func (h *connHandler) Invoke(ctx context.Context) (arrow.Table, error) {
	if err := h.exec(ctx, "CREATE OR REPLACE TABLE batch AS SELECT 1 AS n"); err != nil {
		return nil, err
	}
	return h.fakeHandler.Invoke(ctx)
}

// Every statement the pipeline runs on the shared connection runs under the
// shared lock. The table managers collect on the same connection under that
// lock, and DuckDB closes a pending result the moment another statement runs
// on its connection, so a statement outside the lock fails whichever poll is
// in flight. #280: the handler reset after every batch and the progress write
// both ran outside the lock, and the Bluesky demo logged "closed pending
// query result" once at batch 500 and seven times in ten minutes at batch 1.
//
// The race cannot be reproduced on demand. The invariant can be checked on
// every statement: the wrapper records any execution that finds the lock
// free. The loop runs with no other goroutine touching the connection, so a
// free lock means the pipeline itself let go of it.
func TestCoreConsumeLoop_EveryStatementOnTheSharedConnectionHoldsTheLock(t *testing.T) {
	coverage.Covers(t, "state.durability")
	ctx := context.Background()
	db, err := duckdb.OpenPath(ctx, "")
	assert.NoError(t, err)
	defer db.Close()
	raw, err := db.Connect(ctx)
	assert.NoError(t, err)
	defer raw.Close()

	lock := &sync.Mutex{}
	conn := duckdb.NewLockChecked(raw, lock)

	// Startup runs before the managers exist, and root.go does it without
	// the lock. Holding it here keeps the check about the running pipeline.
	progress := NewProgressStore(conn)
	lock.Lock()
	assert.NoError(t, progress.Init(ctx))
	lock.Unlock()

	src := &fakeSource{batches: [][]Message{messages(4), messages(4), messages(4)}}
	tb := NewTurbine(src, &connHandler{conn: conn}, &fakeSink{}, 4, time.Second,
		lock, PipelineErrorPolicies{}, WithProgressStore(progress))
	stats, err := tb.ConsumeLoop(ctx, 12)
	assert.NoError(t, err)
	assert.Equal(t, int64(12), stats.MessagesConsumed())

	if v := conn.Violations(); len(v) > 0 {
		t.Fatalf("%d statements ran on the shared connection without the lock:\n%s",
			len(v), strings.Join(v, "\n"))
	}
}
