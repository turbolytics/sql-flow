package core

import (
	"context"
	"os"
	"path/filepath"

	"github.com/turbolytics/sql-flow/internal/duckdb"
	"github.com/turbolytics/sql-flow/internal/errs"
)

// OpenState opens the DuckDB database backing a pipeline's durable state.
//
// It exists to separate two cases that duckdb.OpenPath cannot tell apart. A
// path with no file is the first run, and creating it is correct. A path that
// holds something DuckDB refuses to open is a damaged state file, and starting
// over would replay from the beginning while reporting healthy.
//
// A damaged file is never repaired, moved, or truncated here. It holds the only
// copy of the pipeline's positions, so it is evidence an operator needs.
func OpenState(ctx context.Context, path string) (*duckdb.DB, error) {
	info, err := os.Stat(path)
	switch {
	case err == nil && info.IsDir():
		// A directory is a config mistake, not corruption. Saying so sends the
		// operator to the config rather than to the state file.
		return nil, errs.New(errs.CodeConfigInvalid,
			"pipeline.state.path %q is a directory, not a file", path)

	case os.IsNotExist(err):
		// First run. DuckDB creates the file, but not the directories above it.
		if mkErr := os.MkdirAll(filepath.Dir(path), 0o755); mkErr != nil {
			return nil, errs.Wrap(errs.CodeStateInternal, mkErr,
				"creating state directory for %q", path)
		}

	case err != nil:
		return nil, errs.Wrap(errs.CodeStateInternal, err, "reading state path %q", path)
	}

	db, err := duckdb.OpenPath(ctx, path)
	if err != nil {
		// The file exists and DuckDB will not open it. A restart reads the
		// same bytes, so this is terminal rather than retryable.
		return nil, errs.Wrap(errs.CodeStateCorrupt, err,
			"state file %q exists but cannot be opened; it has been left untouched", path)
	}
	return db, nil
}
