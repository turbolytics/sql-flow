package core

import (
	"context"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/zeebo/assert"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

// refExec runs a statement and fails the test if it does not.
func refExec(t *testing.T, conn adbc.Connection, sql string) {
	t.Helper()
	stmt, err := conn.NewStatement()
	assert.NoError(t, err)
	defer stmt.Close()
	assert.NoError(t, stmt.SetSqlQuery(sql))
	_, err = stmt.ExecuteUpdate(context.Background())
	assert.NoError(t, err)
}

// observedLogger returns a logger and the entries written through it.
func observedLogger(t *testing.T) (*observer.ObservedLogs, *zap.Logger) {
	t.Helper()
	core, logs := observer.New(zapcore.DebugLevel)
	return logs, zap.New(core)
}

// confWithHandlerSQL builds the smallest config the check reads.
func confWithHandlerSQL(sql string) *config.Conf {
	c := &config.Conf{}
	c.Pipeline.Handler.SQL = sql
	return c
}

// TestCheckReferenceTablesWarnsOnEmpty is the catastrophic case: a dimension
// table that loaded nothing, so every joined row takes the unmatched path.
func TestCheckReferenceTablesWarnsOnEmpty(t *testing.T) {
	conn := refConn(t)
	refExec(t, conn, "CREATE TABLE dim (id INTEGER)")

	logs, logger := observedLogger(t)
	conf := confWithHandlerSQL("SELECT * FROM batch JOIN dim ON dim.id = batch.id")

	assert.NoError(t, CheckReferenceTables(context.Background(), conn, conf, nil, logger))

	warns := logs.FilterLevelExact(zapcore.WarnLevel).All()
	assert.Equal(t, 1, len(warns))
	assert.Equal(t, "dim", warns[0].ContextMap()["table"])
}

// TestCheckReferenceTablesLogsCount is the healthy case, and the number is the
// point: an operator reading "loaded 14203 rows" knows the dimension arrived.
func TestCheckReferenceTablesLogsCount(t *testing.T) {
	conn := refConn(t)
	refExec(t, conn, "CREATE TABLE dim AS SELECT * FROM range(14203) t(id)")

	logs, logger := observedLogger(t)
	conf := confWithHandlerSQL("SELECT * FROM batch JOIN dim ON dim.id = batch.id")

	assert.NoError(t, CheckReferenceTables(context.Background(), conn, conf, nil, logger))

	assert.Equal(t, 0, logs.FilterLevelExact(zapcore.WarnLevel).Len())
	infos := logs.FilterLevelExact(zapcore.InfoLevel).All()
	assert.Equal(t, 1, len(infos))
	assert.Equal(t, int64(14203), infos[0].ContextMap()["rows"])
}

// TestCheckReferenceTablesSurvivesACountError keeps a startup diagnostic from
// becoming a startup failure. A table that does not exist fails later at
// prepare, which is static validation's job.
func TestCheckReferenceTablesSurvivesACountError(t *testing.T) {
	conn := refConn(t)

	logs, logger := observedLogger(t)
	conf := confWithHandlerSQL("SELECT * FROM batch JOIN nonexistent n ON n.id = batch.id")

	assert.NoError(t, CheckReferenceTables(context.Background(), conn, conf, nil, logger))
	assert.That(t, logs.FilterLevelExact(zapcore.WarnLevel).Len() >= 1)
}

// TestCheckReferenceTablesExcludesManagedTables keeps a tumbling window's
// aggregate table from warning on every start. It is empty by design until the
// first window closes.
func TestCheckReferenceTablesExcludesManagedTables(t *testing.T) {
	conn := refConn(t)
	refExec(t, conn, "CREATE TABLE agg_city_count (city VARCHAR)")

	logs, logger := observedLogger(t)
	conf := confWithHandlerSQL(
		"SELECT * FROM batch JOIN agg_city_count a ON a.city = batch.city")
	conf.Tables = &config.Tables{
		SQL: []config.TableSQL{{Name: "agg_city_count"}},
	}

	assert.NoError(t, CheckReferenceTables(context.Background(), conn, conf, nil, logger))
	assert.Equal(t, 0, logs.Len())
}

// TestCheckReferenceTablesNoHandlerSQL is the pipeline with nothing to check.
func TestCheckReferenceTablesNoHandlerSQL(t *testing.T) {
	conn := refConn(t)
	logs, logger := observedLogger(t)

	assert.NoError(t, CheckReferenceTables(
		context.Background(), conn, confWithHandlerSQL("  "), nil, logger))
	assert.Equal(t, 0, logs.Len())
}
