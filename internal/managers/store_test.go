package managers

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/turbolytics/sql-flow/internal/core"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/turbolytics/sql-flow/internal/duckdb"
	"github.com/zeebo/assert"
)

// The watermark survives the process: saved on one connection, committed,
// and read back from a reopened file at the same instant.
func TestManagerWindow_WatermarkPersistsAcrossReopen(t *testing.T) {
	coverage.Covers(t, "manager.window")
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state.db")
	at := time.Date(2026, 9, 13, 10, 5, 0, 123456000, time.UTC)

	d := newTestDB(t, path)
	store := NewStore(d.manager)
	_, ok, err := store.Load(ctx, "w")
	assert.NoError(t, err)
	assert.That(t, !ok)

	assert.NoError(t, store.Save(ctx, "w", at, at.Add(time.Second)))
	assert.NoError(t, d.manager.(transaction).Commit(ctx))
	// Saved again in place: still one row.
	assert.NoError(t, store.Save(ctx, "w", at.Add(time.Minute), at.Add(2*time.Second)))
	assert.NoError(t, d.manager.(transaction).Commit(ctx))
	assert.Equal(t, int64(1), countRows(t, d.pipeline, windowsTable))

	assert.NoError(t, d.manager.Close())
	assert.NoError(t, d.pipeline.Close())
	assert.NoError(t, d.db.Close())

	db, err := duckdb.OpenPath(ctx, path)
	assert.NoError(t, err)
	defer db.Close()
	conn, err := db.Connect(ctx)
	assert.NoError(t, err)
	defer conn.Close()

	got, ok, err := NewStore(conn).Load(ctx, "w")
	assert.NoError(t, err)
	assert.That(t, ok)
	assert.That(t, got.Equal(at.Add(time.Minute)))
}

// Init twice is safe, the table carries no index, and it is an engine table
// the stats endpoint leaves out of the user's tables.
func TestManagerWindow_StoreIsIndexFreeAndHidden(t *testing.T) {
	coverage.Covers(t, "manager.window")
	ctx := context.Background()
	d := newTestDB(t, "")
	assert.NoError(t, NewStore(d.pipeline).Init(ctx))

	n, _, err := queryInt64(ctx, d.pipeline,
		`SELECT count(*)::BIGINT FROM duckdb_indexes() WHERE table_name = 'sqlflow_windows'`)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), n)

	assert.That(t, core.IsEngineTable(windowsTable))
	assert.NoError(t, core.NewOffsetStore(d.pipeline).Init(ctx))
	stats, err := core.CollectStateStats(ctx, d.pipeline, "")
	assert.NoError(t, err)
	for _, tbl := range stats.Tables {
		assert.That(t, tbl.Name != windowsTable)
	}
}

// A window name with a quote in it round-trips.
func TestManagerWindow_StoreEscapesTheName(t *testing.T) {
	coverage.Covers(t, "manager.window")
	ctx := context.Background()
	d := newTestDB(t, "")
	store := NewStore(d.manager)
	at := time.Date(2026, 9, 13, 10, 0, 0, 0, time.UTC)
	assert.NoError(t, store.Save(ctx, "it's", at, at))
	got, ok, err := store.Load(ctx, "it's")
	assert.NoError(t, err)
	assert.That(t, ok)
	assert.That(t, got.Equal(at))
}
