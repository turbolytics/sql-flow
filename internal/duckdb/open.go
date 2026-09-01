package duckdb

import (
	"context"
	"fmt"
	"os"
	"runtime"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/drivermgr"
)

// DefaultLibPath is where a Homebrew install puts libduckdb on macOS, the usual
// development setup; SQLFLOW_DUCKDB_LIB overrides it.
const DefaultLibPath = "/opt/homebrew/lib/libduckdb.dylib"

// DefaultLinuxLibPath is where scripts/install-libduckdb.sh and the container
// image put the library. The pinned version lives in the DUCKDB_VERSION file,
// not here: nothing in the Go build cares which release is installed, only
// where it landed.
const DefaultLinuxLibPath = "/usr/local/lib/libduckdb.so"

// LibPath resolves the DuckDB shared library the ADBC driver manager loads.
func LibPath() string {
	if lib := os.Getenv("SQLFLOW_DUCKDB_LIB"); lib != "" {
		return lib
	}
	if runtime.GOOS == "darwin" {
		return DefaultLibPath
	}
	return DefaultLinuxLibPath
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
