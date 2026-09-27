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
// lateness above zero with a sink that appends, which holds a republished
// bucket twice. run refuses it before it dials anything, with the exit code
// a config error gets.
func TestWindowLatenessWithAnAppendingSinkIsRefusedAtStartup(t *testing.T) {
	coverage.Covers(t, "manager.window")
	db, _ := rowsTestDB(t)
	conf := &config.Conf{Tables: &config.Tables{SQL: []config.TableSQL{{
		Name: "agg",
		Window: &config.Window{
			TimeColumn: "bucket", SizeSeconds: 60, AllowedLatenessSecs: 300,
			Sink: config.Sink{Type: "postgres", Postgres: &config.PostgresSink{
				DSN: "postgres://u:p@127.0.0.1:1/db", Table: "agg", Mode: "append",
			}},
		},
	}}}}

	_, closeConns, err := buildManagedTables(context.Background(), conf, db, nil, zap.NewNop(), nil, nil, sinks.RetryEvents{})
	closeConns()
	assert.Error(t, err)
	assert.Equal(t, errs.CodeConfigInvalid, errs.CodeOf(err))
	assert.Equal(t, errs.ExitUserError, errs.ExitCode(err))
	assert.That(t, strings.Contains(err.Error(), "allowed_lateness_seconds"))
	assert.That(t, strings.Contains(err.Error(), "postgres sink appends"))
}

// The same lateness with a sink that replaces by key is accepted: a
// republished bucket overwrites its earlier value there.
func TestWindowLatenessWithAReplacingSinkIsAccepted(t *testing.T) {
	coverage.Covers(t, "manager.window")
	db, _ := rowsTestDB(t)
	conf := &config.Conf{Tables: &config.Tables{SQL: []config.TableSQL{{
		Name: "agg",
		Window: &config.Window{
			TimeColumn: "bucket", SizeSeconds: 60, AllowedLatenessSecs: 300,
			Sink: config.Sink{Type: "console"},
		},
	}}}}

	built, closeConns, err := buildManagedTables(context.Background(), conf, db, nil, zap.NewNop(), nil, nil, sinks.RetryEvents{})
	defer closeConns()
	assert.NoError(t, err)
	assert.Equal(t, 1, len(built))
}
