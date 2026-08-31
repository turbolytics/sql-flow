package core

import (
	"context"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/turbolytics/turbine/internal/config"
	"go.uber.org/zap"
)

var logger *zap.Logger

func init() {
	logger, _ = zap.NewDevelopment()
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
