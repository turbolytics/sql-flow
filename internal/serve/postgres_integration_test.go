package serve

// What a pool costs an attached Postgres. One DuckDB scan opens up to
// pg_connection_limit connections, so with a pool the worst case may be
// size x that, against a database that also carries the pipeline's writer.
// The spec would not assume it; this measures it.

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/jackc/pgx/v5"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/turbolytics/sql-flow/internal/duckdb"
	"github.com/zeebo/assert"
)

const servePostgresImage = "postgres:18"

func TestIntegrationServePool_PostgresBackendsStayBounded(t *testing.T) {
	coverage.Covers(t, "cli.serve")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	ctx := context.Background()

	pg, err := tcpostgres.Run(ctx, servePostgresImage,
		tcpostgres.WithDatabase("serve"),
		tcpostgres.WithUsername("serve"),
		tcpostgres.WithPassword("serve"),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	t.Cleanup(func() { _ = pg.Terminate(context.Background()) })

	dsn, err := pg.ConnectionString(ctx, "sslmode=disable")
	assert.NoError(t, err)

	direct, err := pgx.Connect(ctx, dsn)
	assert.NoError(t, err)
	t.Cleanup(func() { _ = direct.Close(context.Background()) })

	// Enough rows that a scan takes long enough to overlap with the others.
	_, err = direct.Exec(ctx, `CREATE TABLE wide AS
		SELECT i AS id, 'lang_' || (i % 170) AS lang, i::bigint AS n
		FROM generate_series(1, 400000) AS s(i)`)
	assert.NoError(t, err)

	const poolSize, connLimit = 8, 4
	db, err := duckdb.OpenPath(ctx, "")
	assert.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	ex, err := NewDuckDBExecutor(ctx, db, poolSize,
		func(ctx context.Context, conn adbc.Connection) error {
			for _, sql := range []string{
				"INSTALL postgres; LOAD postgres;",
				fmt.Sprintf("SET pg_connection_limit = %d", connLimit),
				fmt.Sprintf("ATTACH '%s' AS pg (TYPE POSTGRES, READ_ONLY)", dsn),
			} {
				if err := execOn(ctx, conn, sql); err != nil {
					return err
				}
			}
			return nil
		}, nil)
	assert.NoError(t, err)
	t.Cleanup(ex.Close)

	st, err := ex.Prepare(ctx, StatementSpec{
		Dataset: "wide",
		SQL:     "SELECT lang, sum(n)::BIGINT AS total FROM pg.wide GROUP BY lang ORDER BY lang",
	})
	assert.NoError(t, err)

	// Sample while the scans run: the peak is what matters, and it is gone
	// by the time they finish.
	done := make(chan struct{})
	peak := make(chan int64, 1)
	go func() {
		var seen int64
		for {
			select {
			case <-done:
				peak <- seen
				return
			case <-time.After(20 * time.Millisecond):
				var n int64
				if err := direct.QueryRow(context.Background(),
					`SELECT count(*) FROM pg_stat_activity
					 WHERE datname = current_database() AND pid <> pg_backend_pid()`).Scan(&n); err == nil && n > seen {
					seen = n
				}
			}
		}
	}()

	var wg sync.WaitGroup
	for i := 0; i < poolSize; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, _, err := query(ctx, ex, st, nil, 1000)
			assert.NoError(t, err)
			assert.That(t, res.RowCount > 0)
		}()
	}
	wg.Wait()
	close(done)

	got := <-peak
	t.Logf("peak Postgres backends: %d, with pool.size=%d and pg_connection_limit=%d", got, poolSize, connLimit)
	// Measured 2026-09-16: the peak was 4 with eight sessions scanning at
	// once, so the attachment's connections are shared across sessions rather
	// than opened per session. pg_connection_limit caps the attachment, and
	// the pool does not multiply it. The assertion is that bound, so this
	// fails if a DuckDB release changes it and the README goes stale.
	//
	// Sampling every 20 ms could in principle miss a spike shorter than that,
	// which would understate the peak; a scan of 400k rows is far longer.
	if got > connLimit {
		t.Fatalf("peak %d backends exceeds pg_connection_limit = %d, so the pool now multiplies connections; update the README",
			got, connLimit)
	}
}
