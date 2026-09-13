package run

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/turbolytics/sql-flow/internal/core"
	"github.com/turbolytics/sql-flow/internal/coverage"
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
	conn := rowsTestConn(t)
	rowsTestExec(t, conn, "CREATE TABLE agg (id BIGINT)")
	rowsTestExec(t, conn, "INSERT INTO agg VALUES (1), (2), (3)")

	budget := core.NewDrainBudget(200 * time.Millisecond)
	defer budget.Stop()
	m := managers.NewTumbling(conn, "SELECT id FROM agg", "DELETE FROM agg",
		time.Hour, hangingSink{}, &sync.Mutex{}, managers.WithDrainBudget(budget))

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
	conn := rowsTestConn(t)
	rowsTestExec(t, conn, "CREATE TABLE agg (id BIGINT)")

	m := managers.NewTumbling(conn, "SELECT id FROM agg", "DELETE FROM agg",
		time.Hour, hangingSink{}, &sync.Mutex{})

	var group managerGroup
	ctx, cancel := context.WithCancel(context.Background())
	group.start(ctx, m, func(err error) { t.Errorf("onFail called: %v", err) })
	cancel()
	assert.NoError(t, group.wait())
}
