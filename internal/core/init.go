package core

import (
	"context"
	"github.com/marcboeker/go-duckdb"
	"github.com/turbolytics/turbine/internal/config"
	"go.uber.org/zap"
)

var logger *zap.Logger

func init() {
	logger, _ = zap.NewDevelopment()
}

func InitCommands(arrConn *duckdb.Arrow, c *config.Conf) error {
	for _, command := range c.Commands {
		logger.Info("Executing command step", zap.String("name", command.Name))
		_, err := arrConn.QueryContext(
			context.Background(),
			command.SQL,
		)
		if err != nil {
			return err
		}
	}

	return nil
}
