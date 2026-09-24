package core

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/turbolytics/sql-flow/internal/duckdb"
	"github.com/zeebo/assert"
)

// wakeObserver reports what the progress row said at the instant the
// handler ran. A windowed pipeline's handler writes the batch's rows into the
// window table inside Invoke, and with no state path that INSERT autocommits
// as it runs, so this is the first instant a window manager -- reading on a
// connection of its own -- can see them.
type wakeObserver struct {
	fakeHandler
	tb     testing.TB
	reader adbc.Connection

	ran   bool
	quiet time.Duration
}

func (h *wakeObserver) Invoke(ctx context.Context) (arrow.Table, error) {
	h.ran, h.quiet = true, quietInRow(h.tb, h.reader)
	return h.fakeHandler.Invoke(ctx)
}

// quietInRow is the subtraction the window manager makes: how much silence
// the row confirms, read on a connection that sees committed state only.
func quietInRow(tb testing.TB, conn adbc.Connection) time.Duration {
	tb.Helper()
	stmt, err := conn.NewStatement()
	assert.NoError(tb, err)
	defer stmt.Close()
	assert.NoError(tb, stmt.SetSqlQuery(
		`SELECT coalesce(epoch_us(last_commit) - epoch_us(last_arrival), 0) FROM sqlflow_progress`))
	reader, _, err := stmt.ExecuteQuery(context.Background())
	assert.NoError(tb, err)
	defer reader.Release()
	assert.That(tb, reader.Next())
	return time.Duration(reader.Record().Column(0).(*array.Int64).Value(0)) * time.Microsecond
}

// The row has to say the stream woke before the rows it woke with can be
// seen.
//
// A window manager reads the window table and the progress row together, on
// its own connection. Without a state path the handler's INSERT autocommits
// the moment it runs, while the arrival clock is stamped after the sink has
// flushed and the write itself is throttled to once a second. So there is a
// stretch -- up to that whole second -- in which the manager sees a burst's
// first rows beside the silence that preceded them. It closes the bucket on
// an idle bound the burst has already ended, and every later row of that
// burst arrives after the watermark and is dropped as late.
//
// Nothing here needs the manager to be running: the defect is what the
// engine leaves visible, and this pins it at the one instant that matters.
func TestStateDurability_TheRowSaysTheStreamWokeBeforeItsRowsAppear(t *testing.T) {
	coverage.Covers(t, "state.durability")
	ctx := context.Background()

	db, err := duckdb.OpenPath(ctx, "")
	assert.NoError(t, err)
	defer db.Close()
	// The pipeline's connection, and the window manager's.
	conn, err := db.Connect(ctx)
	assert.NoError(t, err)
	defer conn.Close()
	reader, err := db.Connect(ctx)
	assert.NoError(t, err)
	defer reader.Close()

	store := NewProgressStore(conn)
	assert.NoError(t, store.Init(ctx))

	h := &wakeObserver{tb: t, reader: reader}
	tb := NewTurbine(newIdleSource(), h, &fakeSink{}, 1, time.Hour,
		&sync.Mutex{}, PipelineErrorPolicies{}, WithProgressStore(store))

	// Thirty-four seconds of silence, written by the idle ticks that ran
	// through it. The row is fresh and it is true: past any idle bound a
	// window would close on.
	tb.quietSince = time.Now().Add(-34 * time.Second)
	assert.NoError(t, tb.recordProgress(ctx, progressForced))
	assert.That(t, quietInRow(t, reader) >= 34*time.Second)

	// The first batch of the burst that ends the silence.
	assert.NoError(t, tb.processBatch(ctx, 1))

	assert.That(t, h.ran)
	// Not zero: stamping the quiet clock and writing the row are two
	// instants, so the row always confirms the microseconds between them.
	// A second is the bound that matters -- idle_close_seconds is whole
	// seconds, so quiet under one can confirm no bound anyone can configure.
	if h.quiet >= time.Second {
		t.Fatalf("the handler's rows became visible while the row still confirmed %s of silence: "+
			"a window manager polling here closes the bucket on an idle bound this batch has "+
			"already ended, and drops the rest of the burst as late", h.quiet)
	}
}
