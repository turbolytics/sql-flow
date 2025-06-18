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
