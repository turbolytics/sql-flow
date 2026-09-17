package rollup

import (
	"strconv"
	"strings"
	"testing"

	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/turbolytics/sql-flow/internal/errs"
	"github.com/zeebo/assert"
)

// committed is what a repository adopting the example commits: the
// generated migration, and a serve file holding the generated dataset beside
// one written by hand.
func committed(t *testing.T) (*config.RollupsConf, []byte, *config.ServeConf) {
	t.Helper()
	conf := loadExample(t)
	script, err := PostgresDDL(conf)
	assert.NoError(t, err)
	datasets, err := ServeDatasets(conf)
	assert.NoError(t, err)

	serve := &config.ServeConf{Serve: config.Serve{
		Clients: []config.ServeClient{{Name: "page", ID: "page-id"}},
		Datasets: append([]config.ServeDataset{{
			Name: "pipeline_status",
			SQL:  "SELECT * FROM pg.pipeline_status",
		}}, datasets...),
	}}
	return conf, []byte(script), serve
}

func onlyViolation(t *testing.T, violations []config.Violation) config.Violation {
	t.Helper()
	if len(violations) != 1 {
		t.Fatalf("want 1 violation, got %d: %+v", len(violations), violations)
	}
	return violations[0]
}

func TestCliRollup_CheckPassesWhatTheGeneratorsWrote(t *testing.T) {
	coverage.Covers(t, "cli.rollup")
	conf, migration, serve := committed(t)
	assert.Equal(t, 0, len(Check(conf, migration, serve)))
}

func TestCliRollup_CheckReportsAMigrationEditedByHand(t *testing.T) {
	coverage.Covers(t, "cli.rollup")
	conf, migration, serve := committed(t)

	edited := strings.Replace(string(migration), "NULLS NOT DISTINCT", "", 1)
	v := onlyViolation(t, Check(conf, []byte(edited), serve))
	assert.Equal(t, errs.CodeConfigRollupDrift, v.Code)
	assert.Equal(t, "migration", strings.Join(v.Path, "."))
	wantLine := strings.Count(string(migration)[:strings.Index(string(migration), "NULLS NOT DISTINCT")], "\n") + 1
	assert.That(t, strings.Contains(v.Message, "from line "+strconv.Itoa(wantLine)+";"))
}

func TestCliRollup_CheckReportsEachServeFieldEditedByHand(t *testing.T) {
	coverage.Covers(t, "cli.rollup")

	// The generated dataset is index 1: pipeline_status comes first.
	for _, tt := range []struct {
		name string
		edit func(ds *config.ServeDataset)
		path string
	}{
		{"a bound", func(ds *config.ServeDataset) { max := int64(50); ds.Params[3].Max = &max }, "serve.datasets.1.params"},
		{"a param removed", func(ds *config.ServeDataset) { ds.Params = ds.Params[:3] }, "serve.datasets.1.params"},
		{"the default range", func(ds *config.ServeDataset) { ds.Range.Default = "12h" }, "serve.datasets.1.range"},
		{"a max_range", func(ds *config.ServeDataset) {
			g := ds.Grains["1h"]
			g.MaxRange = "30d"
			ds.Grains["1h"] = g
		}, "serve.datasets.1.grains.1h.max_range"},
		{"a grain's SQL", func(ds *config.ServeDataset) {
			g := ds.Grains["1h"]
			g.SQL = strings.Replace(g.SQL, "'other'", "'rest'", 1)
			ds.Grains["1h"] = g
		}, "serve.datasets.1.grains.1h.sql"},
		{"a grain removed", func(ds *config.ServeDataset) { delete(ds.Grains, "6h") }, "serve.datasets.1.grains"},
		// A hand-edited bucket or cache block is drift, like a hand-edited
		// max_range: the cache's key is only exact for the bucket the rollup
		// really has.
		{"a bucket", func(ds *config.ServeDataset) {
			g := ds.Grains["1h"]
			g.Bucket = "30m"
			ds.Grains["1h"] = g
		}, "serve.datasets.1.grains.1h.bucket"},
		{"the cache ttl", func(ds *config.ServeDataset) {
			ds.Cache = &config.ServeDatasetCache{TTLSeconds: 5}
		}, "serve.datasets.1.cache"},
		{"a grain's ttl", func(ds *config.ServeDataset) {
			g := ds.Grains["1d"]
			g.Cache = &config.ServeDatasetCache{TTLSeconds: 5}
			ds.Grains["1d"] = g
		}, "serve.datasets.1.grains.1d.cache"},
		{"the cache block removed", func(ds *config.ServeDataset) { ds.Cache = nil }, "serve.datasets.1.cache"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			conf, migration, serve := committed(t)
			tt.edit(&serve.Serve.Datasets[1])
			v := onlyViolation(t, Check(conf, migration, serve))
			assert.Equal(t, errs.CodeConfigRollupDrift, v.Code)
			assert.Equal(t, tt.path, strings.Join(v.Path, "."))
		})
	}

	t.Run("the dataset missing", func(t *testing.T) {
		conf, migration, serve := committed(t)
		serve.Serve.Datasets = serve.Serve.Datasets[:1]
		v := onlyViolation(t, Check(conf, migration, serve))
		assert.Equal(t, "serve.datasets", strings.Join(v.Path, "."))
		assert.That(t, strings.Contains(v.Message, "posts_by_lang"))
	})

	// A serve file's param order is the author's.
	t.Run("params reordered", func(t *testing.T) {
		conf, migration, serve := committed(t)
		p := serve.Serve.Datasets[1].Params
		p[0], p[3] = p[3], p[0]
		assert.Equal(t, 0, len(Check(conf, migration, serve)))
	})

	// Reindenting SQL for a reader is not drift.
	t.Run("whitespace only", func(t *testing.T) {
		conf, migration, serve := committed(t)
		g := serve.Serve.Datasets[1].Grains["1h"]
		g.SQL = "  " + strings.ReplaceAll(g.SQL, "\n", "\n    ") + "\n\n"
		serve.Serve.Datasets[1].Grains["1h"] = g
		assert.Equal(t, 0, len(Check(conf, migration, serve)))
	})
}

func TestCliRollup_CheckProvesTheRowBoundAgainstTheServeFile(t *testing.T) {
	coverage.Covers(t, "cli.rollup")
	conf, migration, serve := committed(t)

	serve.Serve.Limits = &config.ServeLimits{MaxRows: 5000}
	v := onlyViolation(t, Check(conf, migration, serve))
	assert.Equal(t, errs.CodeConfigRollup, v.Code)
	assert.Equal(t, "serve.datasets.1", strings.Join(v.Path, "."))
	assert.That(t, strings.Contains(v.Message, "7665 rows, 365 buckets × 21 series, more than its max_rows 5000"))
}

// A file that breaks a rule generates nothing worth comparing, so Check
// returns the rules alone.
func TestCliRollup_CheckReturnsTheRulesFirst(t *testing.T) {
	coverage.Covers(t, "cli.rollup")
	conf, _, serve := committed(t)
	conf.Rollups[0].Serve.MaxBuckets = 0

	v := onlyViolation(t, Check(conf, []byte("anything"), serve))
	assert.Equal(t, errs.CodeConfigRollup, v.Code)
	assert.Equal(t, "rollups.0.serve.max_buckets", strings.Join(v.Path, "."))
}
