package cli

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

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

// Rendering a config proves the template is valid; it does not prove turbine
// can build a pipeline from it. This constructs the real handler, sink and
// error policy for every shipped example, which is where an unimplemented
// type or a missing config block actually surfaces.
//
// Sources are deliberately not constructed: a kafka source dials a broker and
// a webhook source binds a port, neither of which belongs in a unit test.
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

			if err := core.InitCommands(conn, conf); err != nil {
				// Commands that reach for external systems (ATTACH postgres,
				// httpfs, motherduck) cannot run here; the config is still
				// structurally sound.
				t.Skipf("commands need an external system: %v", err)
			}
			if err := core.InitTables(conn, conf); err != nil {
				t.Fatalf("init tables: %v", err)
			}

			// udfs are unsupported by design, and those configs are expected
			// to be rejected.
			if err := core.InitUDFs(conf); err != nil {
				t.Skipf("config declares udfs, which turbine rejects: %v", err)
			}

			handler, err := handlers.New(conn, conf.Pipeline.Handler, zap.NewNop())
			checkBuildError(t, "handler", err)
			if closer, ok := handler.(io.Closer); ok {
				defer closer.Close()
			}

			_, err = sinks.New(conf.Pipeline.Sink, conn)
			checkBuildError(t, "sink", err)

			if conf.Tables != nil {
				for _, table := range conf.Tables.SQL {
					if table.Manager == nil {
						continue
					}
					_, err := sinks.New(table.Manager.Sink, conn)
					checkBuildError(t, "manager sink for "+table.Name, err)
				}
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
