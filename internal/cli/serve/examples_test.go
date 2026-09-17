package serve

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/core"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/turbolytics/sql-flow/internal/duckdb"
	api "github.com/turbolytics/sql-flow/internal/serve"
	"github.com/turbolytics/sql-flow/internal/validate"
	"github.com/zeebo/assert"
)

// exampleOverrides supplies every variable the shipped serve examples read
// without a default, the way a deploy's environment would.
var exampleOverrides = map[string]string{
	"SQLFLOW_SERVE_CLIENT_ID":              "example-client",
	"SQLFLOW_SERVE_CLIENT_ID_BLUESKY_DEMO": "example-client",
	"SQLFLOW_POSTGRES_URI":                 "postgresql://sqlflow@127.0.0.1:1/unreachable",
}

// denyExternalAccess keeps the sweep off the network: INSTALL postgres and
// ATTACH then fail in microseconds rather than dialing out.
func denyExternalAccess(t *testing.T, conn adbc.Connection) {
	t.Helper()
	stmt, err := conn.NewStatement()
	assert.NoError(t, err)
	defer stmt.Close()
	assert.NoError(t, stmt.SetSqlQuery("SET enable_external_access=false"))
	_, _, err = stmt.ExecuteQuery(context.Background())
	assert.NoError(t, err)
}

// Every shipped serve example validates with no variables set, and builds a
// real server when its commands can run here. An example that needs Postgres
// skips at its ATTACH, as every Postgres pipeline example does.
func TestCliServe_ShippedExamplesValidateAndBuild(t *testing.T) {
	coverage.Covers(t, "cli.serve", "config.validation")

	paths, err := filepath.Glob("../../../dev/config/serve/*.yml")
	assert.NoError(t, err)
	assert.That(t, len(paths) > 0)

	for _, path := range paths {
		t.Run(filepath.Base(path), func(t *testing.T) {
			raw, err := os.ReadFile(path)
			assert.NoError(t, err)
			rep, err := validate.Validate(context.Background(), validate.Request{Path: path, Config: string(raw)})
			assert.NoError(t, err)
			if !rep.OK {
				t.Fatalf("validate rejects a shipped example: %+v", rep.Diagnostics)
			}

			conf, _, err := config.LoadServeRendered(path, exampleOverrides)
			assert.NoError(t, err)
			assert.NoError(t, conf.CheckError())

			db, err := duckdb.OpenPath(context.Background(), "")
			assert.NoError(t, err)
			defer db.Close()

			// The init connection is the one the commands run on, so it is
			// the one that must be kept off the network.
			var initErr error
			ex, err := api.NewDuckDBExecutor(context.Background(), db, conf.Serve.PoolSize(),
				func(ctx context.Context, conn adbc.Connection) error {
					denyExternalAccess(t, conn)
					initErr = core.InitCommands(conn, &config.Conf{Commands: conf.Commands})
					return initErr
				}, nil)
			if initErr != nil {
				t.Skipf("commands need an external system: %v", initErr)
			}
			assert.NoError(t, err)

			srv, err := api.New(context.Background(), conf, ex)
			assert.NoError(t, err)
			srv.Close()
		})
	}
}
