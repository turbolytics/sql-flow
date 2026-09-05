package cli

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/core"
	"github.com/turbolytics/sql-flow/internal/duckdb"
	"github.com/turbolytics/sql-flow/internal/handlers"
	"github.com/turbolytics/sql-flow/internal/sinks"
	"go.uber.org/zap"
)

// exampleConfigs walks every shipped example, including the nested ones.
func exampleConfigs(t *testing.T) []string {
	t.Helper()

	var paths []string
	root := filepath.Join("..", "..", "dev", "config", "examples")
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && strings.HasSuffix(path, ".yml") {
			paths = append(paths, path)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) == 0 {
		t.Fatal("no example configs found")
	}
	return paths
}

// probeTimeout bounds the sink probe. sinks.New probes with a background
// context, so a config naming a host that drops packets rather than refusing
// them would hang this test until the kernel gave up.
const probeTimeout = 2 * time.Second

// denyExternalAccess stops DuckDB reaching the network for the rest of the
// connection's life.
//
// The examples run INSTALL httpfs, LOAD postgres and ATTACH 'md:my_db'. With
// external access on, each one downloads an extension or dials a service --
// on a cold CI runner that cost 187 seconds, and it produced no signal,
// because a command that needs a live system is skipped either way. With it
// off, the same commands fail in microseconds and deterministically, so this
// test behaves the same on a warm laptop and a cold runner.
func denyExternalAccess(conn adbc.Connection) error {
	stmt, err := conn.NewStatement()
	if err != nil {
		return err
	}
	defer stmt.Close()

	if err := stmt.SetSqlQuery("SET enable_external_access=false"); err != nil {
		return err
	}
	_, _, err = stmt.ExecuteQuery(context.Background())
	return err
}

// The guard on the guard. denyExternalAccess is what keeps the example sweep
// off the network, and a setting that stopped taking effect would not fail
// anything -- the sweep would quietly go back to downloading extensions, and
// the only symptom would be a CI job three minutes slower than it should be.
// So assert the block directly.
func TestConfigValidation_ExternalAccessIsDenied(t *testing.T) {
	conn, err := duckdb.Open(context.Background())
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer conn.Close()

	if err := denyExternalAccess(conn); err != nil {
		t.Fatalf("deny external access: %v", err)
	}

	stmt, err := conn.NewStatement()
	if err != nil {
		t.Fatalf("new statement: %v", err)
	}
	defer stmt.Close()

	if err := stmt.SetSqlQuery("INSTALL httpfs"); err != nil {
		t.Fatalf("set query: %v", err)
	}
	if _, _, err := stmt.ExecuteQuery(context.Background()); err == nil {
		t.Fatal("INSTALL httpfs succeeded, so DuckDB still reaches the network")
	}
}

// Rendering a config proves the template is valid; it does not prove sqlflow
// can build a pipeline from it. This constructs the real handler, sink and
// error policy for every shipped example, which is where an unimplemented
// type or a missing config block actually surfaces.
//
// Sources are deliberately not constructed: a kafka source dials a broker and
// a webhook source binds a port, neither of which belongs in a unit test.
//
// Nothing here reaches the network, and that is load-bearing rather than
// tidiness. This test is in the job whose contract is "unit tests only".
func TestConfigValidation_ExampleConfigsBuildRealComponents(t *testing.T) {
	// Values the release tests supply. Every example now names its variables
	// with the SQLFLOW_ prefix, which is the only form an environment can
	// reach -- these are passed as overrides here only because this test never
	// runs a process.
	overrides := map[string]string{
		"SQLFLOW_CATALOG_NAME": "test_catalog",
		"SQLFLOW_TABLE_NAME":   "default.test_table",
	}

	for _, path := range exampleConfigs(t) {
		t.Run(filepath.Base(path), func(t *testing.T) {
			conf, err := config.Load(path, overrides)
			if err != nil {
				t.Fatalf("load: %v", err)
			}

			conn, err := duckdb.Open(context.Background())
			if err != nil {
				t.Fatalf("open duckdb: %v", err)
			}
			defer conn.Close()

			if err := denyExternalAccess(conn); err != nil {
				t.Fatalf("deny external access: %v", err)
			}

			// Commands and managed tables routinely need a system this test
			// has no business reaching: an ATTACHed postgres, an httpfs read,
			// MotherDuck. Their failure is recorded, not fatal. Aborting here
			// is how four examples' sinks went unchecked -- the subtest
			// skipped before it ever reached the components it exists to
			// build.
			if err := core.InitCommands(conn, conf); err != nil {
				t.Logf("commands need an external system: %v", err)
			}
			if err := core.InitTables(conn, conf); err != nil {
				t.Logf("managed tables need an external system: %v", err)
			}

			// udfs are unsupported by design, and those configs are expected
			// to be rejected.
			if err := core.InitUDFs(conf); err != nil {
				t.Skipf("config declares udfs, which sqlflow rejects: %v", err)
			}

			ctx, cancel := context.WithTimeout(context.Background(), probeTimeout)
			defer cancel()

			// The sink is built before the handler because it depends on
			// nothing the commands above may have failed to create. Building
			// it second meant a handler that skipped for a missing ATTACHed
			// table took the sink check down with it.
			_, err = sinks.NewWithContext(ctx, conf.Pipeline.Sink, conn)
			checkBuildError(t, "sink", err)

			if conf.Tables != nil {
				for _, table := range conf.Tables.SQL {
					if table.Manager == nil {
						continue
					}
					_, err := sinks.NewWithContext(ctx, table.Manager.Sink, conn)
					checkBuildError(t, "manager sink for "+table.Name, err)
				}
			}

			handler, err := handlers.New(conn, conf.Pipeline.Handler, zap.NewNop())
			checkBuildError(t, "handler", err)
			if closer, ok := handler.(io.Closer); ok {
				defer closer.Close()
			}
		})
	}
}

// checkBuildError separates the two reasons a component fails to build. A
// type turbine does not implement is a parity failure and must fail the test.
// A configured resource that is simply absent here -- an attached database, a
// TLS certificate, an iceberg catalog -- is not something a unit test can
// supply, and says nothing about turbine's coverage.
func checkBuildError(t *testing.T, what string, err error) {
	t.Helper()
	if err == nil {
		return
	}

	msg := err.Error()
	if strings.Contains(msg, "not supported") || strings.Contains(msg, "requires a") {
		t.Fatalf("build %s: %v", what, err)
	}
	t.Skipf("build %s needs a resource this test cannot provide: %v", what, err)
}
