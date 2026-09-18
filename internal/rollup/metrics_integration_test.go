package rollup

// A double sum and a last through the generated triggers: the fraction
// survives every grain, and last follows the latest minute through a
// rewrite, a delete and a null.

import (
	"context"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/zeebo/assert"
)

// metricsSourceDDL is the Deploy to Render template's minute table, without
// the columns no rollup reads.
const metricsSourceDDL = `CREATE TABLE metrics_1m (
  bucket         TIMESTAMPTZ      NOT NULL,
  name           TEXT             NOT NULL,
  type           TEXT             NOT NULL,
  dimensions_key TEXT             NOT NULL,
  value_sum      DOUBLE PRECISION NOT NULL,
  value_count    BIGINT           NOT NULL,
  value_min      DOUBLE PRECISION,
  value_max      DOUBLE PRECISION,
  value_last     DOUBLE PRECISION,
  PRIMARY KEY (bucket, name, type, dimensions_key)
)`

var metricsGrains = []string{"5m", "15m", "1h", "6h", "1d"}

func startMetricsPostgres(t *testing.T) *rollupServer {
	t.Helper()
	srv := startRollupPostgres(t)
	execSQL(t, srv.conn, metricsSourceDDL)
	script, err := PostgresDDL(loadMetrics(t))
	assert.NoError(t, err)
	applyDDL(t, srv.conn, script)
	return srv
}

// putGauge upserts one minute of the series cpu, as the pipeline's sink does:
// a second write of a minute replaces the first. A nil value stores null.
// The casts are not decoration: beside the literal 0, Postgres infers $2 as
// an integer, and 0.25 arrives as 0.
func putGauge(t *testing.T, conn *pgx.Conn, minute string, value *float64) {
	t.Helper()
	_, err := conn.Exec(context.Background(), `
INSERT INTO metrics_1m (bucket, name, type, dimensions_key, value_sum, value_count, value_min, value_max, value_last)
VALUES ($1::timestamptz, 'cpu', 'gauge', '{}', coalesce($2::double precision, 0), 1, $2::double precision, $2::double precision, $2::double precision)
ON CONFLICT (bucket, name, type, dimensions_key) DO UPDATE SET
  value_sum = excluded.value_sum, value_count = excluded.value_count,
  value_min = excluded.value_min, value_max = excluded.value_max, value_last = excluded.value_last`,
		"2026-09-15 "+minute+":00+00", value)
	assert.NoError(t, err)
}

func f(v float64) *float64 { return &v }

// column reads one measure of the one row each grain holds for the day.
func column(t *testing.T, conn *pgx.Conn, grain, col string) *float64 {
	t.Helper()
	var v *float64
	q := fmt.Sprintf("SELECT %s::double precision FROM metrics_%s WHERE name = 'cpu' AND dimensions_key = '{}'", col, grain)
	assert.NoError(t, conn.QueryRow(context.Background(), q).Scan(&v))
	return v
}

func assertEveryGrain(t *testing.T, conn *pgx.Conn, col string, want float64) {
	t.Helper()
	for _, g := range metricsGrains {
		got := column(t, conn, g, col)
		if got == nil {
			t.Fatalf("metrics_%s.%s is null, want %v", g, col, want)
		}
		if *got != want {
			t.Fatalf("metrics_%s.%s is %v, want %v", g, col, *got, want)
		}
	}
}

// 0.25 and 0.5 are exact in binary, so equality is exact. Under the bigint
// cast the sum is 0.
func TestIntegrationRollup_ADoubleSumKeepsItsFraction(t *testing.T) {
	coverage.Covers(t, "cli.rollup")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	srv := startMetricsPostgres(t)

	putGauge(t, srv.conn, "10:00", f(0.25))
	putGauge(t, srv.conn, "10:01", f(0.5))

	assertEveryGrain(t, srv.conn, "value_sum", 0.75)
	assertEveryGrain(t, srv.conn, "value_count", 2)
	assertEveryGrain(t, srv.conn, "value_min", 0.25)
	assertEveryGrain(t, srv.conn, "value_max", 0.5)

	var typ string
	assert.NoError(t, srv.conn.QueryRow(context.Background(),
		"SELECT data_type FROM information_schema.columns WHERE table_name = 'metrics_1d' AND column_name = 'value_sum'").Scan(&typ))
	assert.Equal(t, "double precision", typ)
	assert.NoError(t, srv.conn.QueryRow(context.Background(),
		"SELECT data_type FROM information_schema.columns WHERE table_name = 'metrics_1d' AND column_name = 'value_count'").Scan(&typ))
	assert.Equal(t, "bigint", typ)
}

// Two series of one name sum into metrics_total at every grain, fraction kept.
func TestIntegrationRollup_ATotalSumsAcrossDimensions(t *testing.T) {
	coverage.Covers(t, "cli.rollup")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	srv := startMetricsPostgres(t)

	putGauge(t, srv.conn, "10:00", f(0.25))
	execSQL(t, srv.conn, `INSERT INTO metrics_1m (bucket, name, type, dimensions_key, value_sum, value_count, value_min, value_max, value_last)
VALUES ('2026-09-15 10:00:00+00', 'cpu', 'gauge', '{"host":"b"}', 0.5, 3, 0.125, 0.25, 0.125)`)

	for _, g := range metricsGrains {
		var sum, min, max float64
		var n int64
		q := fmt.Sprintf("SELECT value_sum, value_count, value_min, value_max FROM metrics_total_%s WHERE name = 'cpu'", g)
		assert.NoError(t, srv.conn.QueryRow(context.Background(), q).Scan(&sum, &n, &min, &max))
		assert.Equal(t, 0.75, sum)
		assert.Equal(t, int64(4), n)
		assert.Equal(t, 0.125, min)
		assert.Equal(t, 0.25, max)
	}
}

// The minutes are written out of order, so last cannot be the last written.
func TestIntegrationRollup_LastIsTheLatestMinute(t *testing.T) {
	coverage.Covers(t, "cli.rollup")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	srv := startMetricsPostgres(t)

	putGauge(t, srv.conn, "10:04", f(4))
	putGauge(t, srv.conn, "10:00", f(3))
	putGauge(t, srv.conn, "10:01", f(9))
	assertEveryGrain(t, srv.conn, "value_last", 4)

	// A later bucket of a coarser grain: 10:20 is another 5m and 15m bucket,
	// the same hour. The hour and up follow it; 10:00's 5m bucket does not.
	putGauge(t, srv.conn, "10:20", f(6))
	for _, g := range []string{"1h", "6h", "1d"} {
		assert.Equal(t, 6.0, *column(t, srv.conn, g, "value_last"))
	}
	var first float64
	assert.NoError(t, srv.conn.QueryRow(context.Background(),
		"SELECT value_last FROM metrics_5m WHERE bucket = '2026-09-15 10:00:00+00'").Scan(&first))
	assert.Equal(t, 4.0, first)
}

func TestIntegrationRollup_LastFollowsARewriteOfTheLatestMinuteOnly(t *testing.T) {
	coverage.Covers(t, "cli.rollup")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	srv := startMetricsPostgres(t)

	putGauge(t, srv.conn, "10:00", f(3))
	putGauge(t, srv.conn, "10:01", f(9))
	putGauge(t, srv.conn, "10:04", f(4))

	putGauge(t, srv.conn, "10:04", f(7))
	assertEveryGrain(t, srv.conn, "value_last", 7)

	putGauge(t, srv.conn, "10:01", f(100))
	assertEveryGrain(t, srv.conn, "value_last", 7)
	assertEveryGrain(t, srv.conn, "value_max", 100)
}

// As for every measure: retention on the minute table leaves history alone.
func TestIntegrationRollup_LastOutlivesADelete(t *testing.T) {
	coverage.Covers(t, "cli.rollup")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	srv := startMetricsPostgres(t)

	putGauge(t, srv.conn, "10:01", f(9))
	putGauge(t, srv.conn, "10:04", f(4))
	execSQL(t, srv.conn, "DELETE FROM metrics_1m WHERE bucket = '2026-09-15 10:04:00+00'")

	assertEveryGrain(t, srv.conn, "value_last", 4)
}

func TestIntegrationRollup_LastSkipsANullAndStoresOneWhenNothingElseExists(t *testing.T) {
	coverage.Covers(t, "cli.rollup")
	if testing.Short() {
		t.Skip("integration: starts a Postgres container")
	}
	srv := startMetricsPostgres(t)

	putGauge(t, srv.conn, "10:04", nil)
	for _, g := range metricsGrains {
		assert.Nil(t, column(t, srv.conn, g, "value_last"))
	}

	putGauge(t, srv.conn, "10:01", f(9))
	assertEveryGrain(t, srv.conn, "value_last", 9)
}
