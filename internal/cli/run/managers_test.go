package run

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/turbolytics/sql-flow/internal/core"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/turbolytics/sql-flow/internal/duckdb"
	"github.com/turbolytics/sql-flow/internal/errs"
	"github.com/turbolytics/sql-flow/internal/managers"
	"github.com/zeebo/assert"
)

// hangingSink blocks in Flush until its context ends.
type hangingSink struct{}

func (hangingSink) WriteTable(context.Context, arrow.Table) error { return nil }
func (hangingSink) Flush(ctx context.Context) error {
	<-ctx.Done()
	return ctx.Err()
}

// A manager whose final poll runs out of drain deadline reports it through
// the group, with the drain code, so run exits 15 rather than 0 with the
// window still in the table. The failure also reaches onFail, which is how
// /healthz learns of it.
func TestLifecycleDrain_AFailedFinalPollReachesTheRun(t *testing.T) {
	coverage.Covers(t, "lifecycle.drain")
	db, conn := rowsTestDB(t)
	rowsTestExec(t, conn, "CREATE TABLE agg (bucket TIMESTAMPTZ, id BIGINT)")
	rowsTestExec(t, conn, `INSERT INTO agg VALUES
		(TIMESTAMPTZ '2026-09-13 10:00:00+00', 1),
		(TIMESTAMPTZ '2026-09-13 11:00:00+00', 2)`)
	assert.NoError(t, managers.NewStore(conn).Init(context.Background()))

	budget := core.NewDrainBudget(200 * time.Millisecond)
	defer budget.Stop()
	m := newTestWindow(t, db, hangingSink{}, managers.WithDrainBudget(budget))

	var (
		group  managerGroup
		mu     sync.Mutex
		failed []error
	)
	ctx, cancel := context.WithCancel(context.Background())
	group.start(ctx, m, func(err error) {
		mu.Lock()
		failed = append(failed, err)
		mu.Unlock()
	})
	cancel()
	err := group.wait()

	assert.Equal(t, errs.CodeDrainIncomplete, errs.CodeOf(err))
	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, 1, len(failed))
	assert.That(t, errors.Is(failed[0], err))
}

// A clean final poll reports nothing, so a clean stop stays exit 0.
func TestLifecycleDrain_ACleanFinalPollReportsNothing(t *testing.T) {
	coverage.Covers(t, "lifecycle.drain")
	db, conn := rowsTestDB(t)
	rowsTestExec(t, conn, "CREATE TABLE agg (bucket TIMESTAMPTZ, id BIGINT)")
	assert.NoError(t, managers.NewStore(conn).Init(context.Background()))

	m := newTestWindow(t, db, hangingSink{})

	var group managerGroup
	ctx, cancel := context.WithCancel(context.Background())
	group.start(ctx, m, func(err error) { t.Errorf("onFail called: %v", err) })
	cancel()
	assert.NoError(t, group.wait())
}

// A stop that no manager caused reports no manager error, whatever cancelled
// the run.
//
// run used to infer the failure from the run context's cause: any cause that
// was not context.Canceled meant a manager had failed. Go 1.26 made
// signal.NotifyContext cancel with a cause naming the signal instead, and
// that cause propagates into the run context -- so every SIGTERM matched the
// inference, a clean drain was logged as a failed manager, and the process
// exited 1 where it had exited 0. This is the shape run builds, with the
// signal arriving and no manager having failed.
func TestLifecycleDrain_ASignalStopIsNotAManagerFailure(t *testing.T) {
	coverage.Covers(t, "lifecycle.drain")
	db, conn := rowsTestDB(t)
	rowsTestExec(t, conn, "CREATE TABLE agg (bucket TIMESTAMPTZ, id BIGINT)")
	assert.NoError(t, managers.NewStore(conn).Init(context.Background()))

	m := newTestWindow(t, db, hangingSink{})

	signalCtx, signalled := context.WithCancelCause(context.Background())
	runCtx, failRun := context.WithCancelCause(signalCtx)
	defer failRun(nil)

	var group managerGroup
	managerCtx, stopManagers := context.WithCancel(context.Background())
	group.start(managerCtx, m, func(err error) { failRun(err) })

	// What the stdlib now does on SIGTERM, and what it does to runCtx.
	signalled(errors.New("terminated signal received"))
	<-runCtx.Done()
	assert.That(t, context.Cause(runCtx) != context.Canceled)

	// No manager failed, so the run has nothing to exit non-zero with.
	assert.NoError(t, group.err())

	stopManagers()
	assert.NoError(t, group.wait())
}

// newTestWindow builds a one-minute window over agg on a connection of its
// own, the way buildManagedTables does, with the engine's watermark asserted
// past every bucket the caller seeded: what a poll does is the subject here,
// so it is given something to do.
func newTestWindow(t *testing.T, db *duckdb.DB, sink core.Sink, opts ...managers.Option) *managers.Watermark {
	t.Helper()
	ctx := context.Background()
	pipeline, err := db.Connect(ctx)
	assert.NoError(t, err)
	t.Cleanup(func() { pipeline.Close() })
	store := core.NewWatermarkStore(pipeline)
	assert.NoError(t, store.Init(ctx))
	assert.NoError(t, store.Save(ctx, "agg", time.Date(2026, 9, 13, 12, 0, 0, 0, time.UTC)))

	conn, err := db.Connect(context.Background())
	assert.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	po, ok := conn.(adbc.PostInitOptions)
	assert.That(t, ok)
	assert.NoError(t, po.SetOption(adbc.OptionKeyAutoCommit, adbc.OptionValueDisabled))

	m, err := managers.NewWatermark(conn, managers.Declaration{
		Table: "agg", TimeColumn: "bucket", Size: time.Minute, Late: managers.LateDrop,
	}, time.Hour, sink, opts...)
	assert.NoError(t, err)
	return m
}
