package run

import (
	"context"
	"strings"
	"testing"

	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/turbolytics/sql-flow/internal/errs"
	"github.com/turbolytics/sql-flow/internal/sinks"
	"github.com/zeebo/assert"
	"go.uber.org/zap"
)

// A config that skipped validate must not run the pairing validate refuses:
// a postgres sink that upserts by key, handed a reemit of the late rows
// alone, replaces a published count with theirs. run refuses it before it
// dials anything, with the exit code a config error gets.
func TestWindowUpsertWithReemitIsRefusedAtStartup(t *testing.T) {
	coverage.Covers(t, "manager.window")
	db, _ := rowsTestDB(t)
	conf := &config.Conf{Tables: &config.Tables{SQL: []config.TableSQL{{
		Name: "agg",
		Window: &config.Window{
			TimeColumn: "bucket", SizeSeconds: 60, LateRows: "reemit",
			Sink: config.Sink{Type: "postgres", Postgres: &config.PostgresSink{
				DSN: "postgres://u:p@127.0.0.1:1/db", Table: "agg", Mode: "upsert", Key: []string{"bucket"},
			}},
		},
	}}}}

	_, closeConns, err := buildManagedTables(context.Background(), conf, db, zap.NewNop(), nil, nil, sinks.RetryEvents{})
	closeConns()
	assert.Error(t, err)
	assert.Equal(t, errs.CodeConfigInvalid, errs.CodeOf(err))
	assert.Equal(t, errs.ExitUserError, errs.ExitCode(err))
	assert.That(t, strings.Contains(err.Error(), "late_rows is reemit and the postgres sink upserts"))
}
