package core

import (
	"context"
	"fmt"
	"strings"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/turbolytics/turbine/internal/config"
	"github.com/turbolytics/turbine/internal/logging"
	"go.uber.org/zap"
)

var logger *zap.Logger

func init() {
	// A bad SQLFLOW_LOG_LEVEL is reported by the command that builds its own
	// logger; here it only falls back to the default level.
	logger, _ = logging.New()
}

// InitUDFs reports that a config declares UDFs, which turbine does not
// support. The Python engine registers arbitrary Python callables through
// conn.create_function; Go has no equivalent, and rather than inventing a
// plugin mechanism, user-defined functions are left to DuckDB itself (a
// macro, an extension, or an ATTACHed database that defines them).
//
// This is an error rather than a silent skip: ignoring the block would defer
// the failure to an unhelpful binder error when the handler SQL calls the
// function.
func InitUDFs(c *config.Conf) error {
	if len(c.UDFs) == 0 {
		return nil
	}

	names := make([]string, 0, len(c.UDFs))
	for _, udf := range c.UDFs {
		names = append(names, udf.FunctionName)
	}

	return fmt.Errorf(
		"udfs are not supported: %s. Define them in DuckDB instead "+
			"(a macro or extension, or ATTACH a database that provides them)",
		strings.Join(names, ", "),
	)
}

// InitTables creates the tables that live across the pipeline's lifetime,
// such as the aggregate tables a tumbling window manager maintains.
func InitTables(conn adbc.Connection, c *config.Conf) error {
	if c.Tables == nil {
		return nil
	}

	for _, table := range c.Tables.SQL {
		logger.Info("Creating managed table", zap.String("name", table.Name))

		stmt, err := conn.NewStatement()
		if err != nil {
			return err
		}

		if err := stmt.SetSqlQuery(table.SQL); err != nil {
			stmt.Close()
			return err
		}
		if _, _, err := stmt.ExecuteQuery(context.Background()); err != nil {
			stmt.Close()
			return err
		}
		stmt.Close()
	}
	return nil
}

func InitCommands(conn adbc.Connection, c *config.Conf) error {
	for _, command := range c.Commands {
		logger.Info("Executing command step", zap.String("name", command.Name))

		stmt, err := conn.NewStatement()
		if err != nil {
			return err
		}

		if err := stmt.SetSqlQuery(command.SQL); err != nil {
			return err
		}

		_, _, err = stmt.ExecuteQuery(context.Background())
		if err != nil {
			return err
		}
		stmt.Close()
	}
	return nil
}
