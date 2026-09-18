package rollup

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/turbolytics/sql-flow/internal/duckdb"
	api "github.com/turbolytics/sql-flow/internal/serve"
	"github.com/zeebo/assert"
)

func TestCliRollup_ServeYAMLMatchesTheGolden(t *testing.T) {
	coverage.Covers(t, "cli.rollup")

	datasets, err := ServeDatasets(loadExample(t))
	assert.NoError(t, err)
	out, err := ServeYAML(datasets)
	assert.NoError(t, err)
	golden(t, "testdata/bluesky.serve.yml", string(out))
}

// A generated dataset is only useful if serve accepts it.
func TestCliRollup_ServeDatasetsPassServesRules(t *testing.T) {
	coverage.Covers(t, "cli.rollup")

	datasets, err := ServeDatasets(withTotals(t))
	assert.NoError(t, err)
	conf := config.ServeConf{Serve: config.Serve{
		Clients:  []config.ServeClient{{Name: "page", ID: "page-id"}},
		Datasets: datasets,
	}}
	assert.Equal(t, 0, len(conf.Check()))
}

// withTotals is the example with posts_total served too, so the dataset
// without a dimension is generated and exercised.
func withTotals(t *testing.T) *config.RollupsConf {
	t.Helper()
	conf := loadExample(t)
	serve := conf.Rollups[0].Serve
	serve.Datasets = append(serve.Datasets, config.RollupDataset{
		Name:         "posts_total",
		DimensionSet: "posts_total",
		DefaultRange: "30d",
		MaxRange:     map[string]string{"1h": "14d", "1d": "365d"},
	})
	return conf
}

func execDuck(t *testing.T, conn adbc.Connection, sql string) {
	t.Helper()
	stmt, err := conn.NewStatement()
	assert.NoError(t, err)
	defer stmt.Close()
	assert.NoError(t, stmt.SetSqlQuery(sql))
	_, err = stmt.ExecuteUpdate(context.Background())
	assert.NoError(t, err)
}

// The generated SQL prepares in DuckDB and folds correctly. The catalog is an
// attached in-memory database standing in for Postgres: serve reads it by
// the same pg.<table> names.
func TestCliRollup_ServeDatasetsAnswerFromTheirTables(t *testing.T) {
	coverage.Covers(t, "cli.rollup")

	// The setup runs on the executor's own connection: ATTACH is
	// database-wide, and serve opens its sessions from the same database.
	setup := []string{
		"SET TimeZone='UTC'",
		"ATTACH ':memory:' AS pg",
		"CREATE TABLE pg.posts_per_minute_by_lang (bucket TIMESTAMPTZ, lang VARCHAR, posts INTEGER, updated_at TIMESTAMPTZ)",
	}
	for _, g := range []string{"5m", "15m", "1h", "6h", "1d"} {
		setup = append(setup,
			"CREATE TABLE pg.posts_by_lang_"+g+" (bucket TIMESTAMPTZ, lang VARCHAR, posts BIGINT)",
			"CREATE TABLE pg.posts_total_"+g+" (bucket TIMESTAMPTZ, minutes BIGINT, posts BIGINT)")
	}
	setup = append(setup,
		`INSERT INTO pg.posts_per_minute_by_lang VALUES
		('2026-09-13 18:01:00+00', 'en', 5, now()), ('2026-09-13 18:02:00+00', 'en', 7, now()),
		('2026-09-13 18:01:00+00', 'ja', 3, now())`,
		`INSERT INTO pg.posts_by_lang_15m VALUES
		('2026-09-12 00:15:00+00', 'en', 10), ('2026-09-12 00:15:00+00', 'ja', 4)`,
		`INSERT INTO pg.posts_total_1d VALUES ('2026-09-12 00:00:00+00', 1440, 250000)`)

	db, err := duckdb.OpenPath(context.Background(), "")
	assert.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	ex, err := api.NewDuckDBExecutor(context.Background(), db, 1,
		func(ctx context.Context, conn adbc.Connection) error {
			for _, sql := range setup {
				execDuck(t, conn, sql)
			}
			return nil
		}, nil)
	assert.NoError(t, err)

	datasets, err := ServeDatasets(withTotals(t))
	assert.NoError(t, err)
	conf := &config.ServeConf{Serve: config.Serve{
		Clients:  []config.ServeClient{{Name: "page", ID: "page-id"}},
		Datasets: datasets,
	}}
	srv, err := api.New(context.Background(), conf, ex)
	assert.NoError(t, err)
	t.Cleanup(srv.Close)
	handler := srv.Handler()

	get := func(target string) (int, map[string]any) {
		req := httptest.NewRequest(http.MethodGet, target+"&client_id=page-id", nil)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)
		var body map[string]any
		assert.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
		return w.Code, body
	}
	rowsOf := func(body map[string]any) string {
		var parts []string
		for _, r := range body["rows"].([]any) {
			row := r.(map[string]any)
			parts = append(parts, row["bucket"].(string)+" "+row["lang"].(string)+" "+jsonNumber(row["posts"]))
		}
		return strings.Join(parts, ", ")
	}

	// Two hours: the source grain, top 1 keeps en and folds ja into other.
	status, body := get("/v1/datasets/posts_by_lang?since=2026-09-13T18:00:00Z&until=2026-09-13T20:00:00Z&top=1")
	assert.Equal(t, http.StatusOK, status)
	assert.Equal(t, "1m", body["grain"])
	assert.Equal(t, "2026-09-13T18:01:00Z en 5, 2026-09-13T18:01:00Z other 3, 2026-09-13T18:02:00Z en 7", rowsOf(body))

	// A filter returns its value unfolded.
	_, body = get("/v1/datasets/posts_by_lang?since=2026-09-13T18:00:00Z&until=2026-09-13T20:00:00Z&top=1&lang=ja")
	assert.Equal(t, "2026-09-13T18:01:00Z ja 3", rowsOf(body))

	// Two days picks 15m.
	_, body = get("/v1/datasets/posts_by_lang?since=2026-09-11T00:00:00Z&until=2026-09-13T00:00:00Z&top=1")
	assert.Equal(t, "15m", body["grain"])
	assert.Equal(t, "2026-09-12T00:15:00Z en 10, 2026-09-12T00:15:00Z other 4", rowsOf(body))

	// Seven days at 5m is refused, and so is a top past its bound.
	status, _ = get("/v1/datasets/posts_by_lang?grain=5m&since=2026-09-06T00:00:00Z&until=2026-09-13T00:00:00Z")
	assert.Equal(t, http.StatusBadRequest, status)
	status, _ = get("/v1/datasets/posts_by_lang?top=21")
	assert.Equal(t, http.StatusBadRequest, status)

	// The dataset without a dimension selects its measures as they are.
	status, body = get("/v1/datasets/posts_total?since=2026-09-01T00:00:00Z&until=2026-09-14T00:00:00Z&grain=1d")
	assert.Equal(t, http.StatusOK, status)
	row := body["rows"].([]any)[0].(map[string]any)
	assert.Equal(t, "1440", jsonNumber(row["minutes"]))
	assert.Equal(t, "250000", jsonNumber(row["posts"]))
}

func jsonNumber(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}

// Every generated grain carries its bucket, which is its name: a rollup
// grain is named for its width. The cache block appears only when the
// declaration asks.
func TestCliRollup_ServeDatasetsCarryBucketsAndTheCacheOptIn(t *testing.T) {
	coverage.Covers(t, "cli.rollup")

	conf := withTotals(t)
	for i := range conf.Rollups[0].Serve.Datasets {
		conf.Rollups[0].Serve.Datasets[i].CacheTTLSeconds = 0
	}
	conf.Rollups[0].Serve.Datasets[0].CacheTTLSeconds = 30
	conf.Rollups[0].Serve.Datasets[0].CacheTTLByGrain = map[string]int{"1d": 600}

	datasets, err := ServeDatasets(conf)
	assert.NoError(t, err)
	assert.That(t, len(datasets) >= 2)
	for _, ds := range datasets {
		for name, g := range ds.Grains {
			assert.Equal(t, name, g.Bucket)
		}
	}
	assert.Equal(t, 30, datasets[0].Cache.TTLSeconds)
	// Only the grain the declaration names carries a block of its own.
	for name, g := range datasets[0].Grains {
		if name == "1d" {
			assert.Equal(t, 600, g.Cache.TTLSeconds)
		} else {
			assert.That(t, g.Cache == nil)
		}
	}
	for _, ds := range datasets[1:] {
		assert.That(t, ds.Cache == nil)
	}
}

// A folded dataset sums a measure across the values folded into other. A
// double sum cast to BIGINT there would lose the fraction the tables kept.
func TestCliRollup_ServeKeepsADoubleSumsFraction(t *testing.T) {
	coverage.Covers(t, "cli.rollup")

	conf := loadExample(t)
	set := conf.Rollups[0].DimensionSets[0]
	m := set.Measures["posts"]
	m.Numeric = "double"
	set.Measures["posts"] = m

	datasets, err := ServeDatasets(conf)
	assert.NoError(t, err)
	for grain, g := range datasets[0].Grains {
		if !strings.Contains(g.SQL, "sum(posts)::DOUBLE AS posts") {
			t.Fatalf("grain %s still casts the sum to an integer:\n%s", grain, g.SQL)
		}
	}
}
