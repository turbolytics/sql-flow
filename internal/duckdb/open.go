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

// DB is an open DuckDB database. It exists to hold the ADBC database handle,
// which is what allows more than one connection against the same file.
//
// That matters twice over. A file-backed database is how pipeline state
// survives a crash, and a second connection is how that state can be read --
// by the stats endpoint -- without disturbing the writer: connections have
// independent transaction state, so a reader sees committed rows only and
// never blocks the pipeline.
type DB struct {
	db   adbc.Database
	path string
}

// OpenPath opens a DuckDB database over ADBC. A non-empty path is a file, so
// tables created on it outlive the process; an empty path is in-memory.
func OpenPath(ctx context.Context, path string) (*DB, error) {
	opts := map[string]string{
		"driver":     LibPath(),
		"entrypoint": "duckdb_adbc_init",
	}
	if path != "" {
		opts["path"] = path
	}

	var drv drivermgr.Driver
	database, err := drv.NewDatabase(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize DuckDB driver: %w", err)
	}

	return &DB{db: database, path: path}, nil
}

// Connect opens one connection to the database. Each connection carries its
// own transaction state.
func (d *DB) Connect(ctx context.Context) (adbc.Connection, error) {
	conn, err := d.db.Open(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to open DuckDB connection: %w", err)
	}
	return conn, nil
}

// Path is the file backing the database, empty when it is in-memory.
func (d *DB) Path() string { return d.path }

func (d *DB) Close() error { return d.db.Close() }

// Open returns a single in-memory DuckDB connection, for callers that need no
// handle of their own.
func Open(ctx context.Context) (adbc.Connection, error) {
	db, err := OpenPath(ctx, "")
	if err != nil {
		return nil, err
	}
	return db.Connect(ctx)
}
