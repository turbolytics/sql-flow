package duckdb

import (
	"context"
	"fmt"
	"os"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/drivermgr"
)

// DefaultLibPath is where a Homebrew install puts libduckdb; SQLFLOW_DUCKDB_LIB
// overrides it.
const DefaultLibPath = "/opt/homebrew/lib/libduckdb.dylib"

// LibPath resolves the DuckDB shared library the ADBC driver manager loads.
func LibPath() string {
	if lib := os.Getenv("SQLFLOW_DUCKDB_LIB"); lib != "" {
		return lib
	}
	return DefaultLibPath
}

// Open returns an in-memory DuckDB connection over ADBC.
func Open(ctx context.Context) (adbc.Connection, error) {
	var drv drivermgr.Driver
	db, err := drv.NewDatabase(map[string]string{
		"driver":     LibPath(),
		"entrypoint": "duckdb_adbc_init",
	})
	if err != nil {
		return nil, fmt.Errorf("failed to initialize DuckDB driver: %w", err)
	}

	conn, err := db.Open(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to open DuckDB connection: %w", err)
	}

	return conn, nil
}
