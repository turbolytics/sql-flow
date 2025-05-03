package run

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/marcboeker/go-duckdb"
	"github.com/spf13/cobra"
	"github.com/turbolytics/turbine/internal/handlers"
	"github.com/turbolytics/turbine/internal/sinks"
	"github.com/turbolytics/turbine/internal/sources"
	"go.uber.org/zap"
	"sync"
	"time"

	"github.com/turbolytics/turbine/internal/config"
	"github.com/turbolytics/turbine/internal/core"
)

func NewCommand() *cobra.Command {
	var configPath string

	var cmd = &cobra.Command{
		Use:   "run",
		Short: "Run turbine against a stream of data",
		RunE: func(cmd *cobra.Command, args []string) error {
			logger, _ := zap.NewDevelopment()
			defer logger.Sync()
			l := logger.Named("turbine.run")

			conf, err := config.Load(configPath, map[string]string{})
			if err != nil {
				return fmt.Errorf("failed to load config: %w", err)
			}

			connector, err := duckdb.NewConnector(":memory:", nil)
			if err != nil {
				return fmt.Errorf("failed to create connector: %w", err)
			}
			defer func() {
				if err := connector.Close(); err != nil {
					l.Error("failed to close connector", zap.Error(err))
				}
			}()

			db := sql.OpenDB(connector)
			defer func() {
				if err := db.Close(); err != nil {
					l.Error("failed to close db", zap.Error(err))
				}
			}()

			conn, err := connector.Connect(context.Background())
			if err != nil {
				return fmt.Errorf("failed to connect: %w", err)
			}
			defer func() {
				if err := conn.Close(); err != nil {
					l.Error("failed to close connection", zap.Error(err))
				}
			}()

			arr, err := duckdb.NewArrowFromConn(conn)
			if err != nil {
				return fmt.Errorf("failed to create arrow from conn: %w", err)
			}

			if err := core.InitCommands(arr, conf); err != nil {
				return fmt.Errorf("failed to init commands: %w", err)
			}

			src, err := sources.New(
				conf.Pipeline.Source,
				logger,
			)
			if err != nil {
				return fmt.Errorf("failed to create source: %w", err)
			}

			sink, err := sinks.New(conf.Pipeline.Sink)
			if err != nil {
				return fmt.Errorf("failed to create sink: %w", err)
			}

			handler, err := handlers.New(
				arr,
				conf.Pipeline.Handler,
			)
			if err != nil {
				return fmt.Errorf("failed to create handler: %w", err)
			}

			lock := &sync.Mutex{}
			turbine := core.NewTurbine(
				src,
				handler,
				sink,
				conf.Pipeline.BatchSize,
				time.Duration(conf.Pipeline.FlushIntervalSeconds)*time.Second,
				lock,
				core.PipelineErrorPolicies{
					// Source: conf.Pipeline.Source.Error.Policy,
				},
				core.WithTurbineLogger(l),
			)

			_, err = turbine.ConsumeLoop(context.Background(), 0)
			if err != nil {
				l.Error("failed to consume loop", zap.Error(err))
				return err
			}
			return nil
		},
	}

	cmd.Flags().StringVarP(&configPath, "config", "c", "", "Path to turbine config file (required)")
	cmd.MarkFlagRequired("config")

	return cmd
}
