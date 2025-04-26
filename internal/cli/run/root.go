package run

import (
	"fmt"
	"github.com/spf13/cobra"
	"github.com/turbolytics/turbine/internal/handlers"
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
			ctx := cmd.Context()
			logger, _ := zap.NewDevelopment()
			defer logger.Sync()
			l := logger.Named("turbine")

			conf, err := config.Load(configPath, map[string]string{})
			if err != nil {
				return fmt.Errorf("failed to load config: %w", err)
			}

			src, err := sources.New(conf.Pipeline.Source)
			if err != nil {
				return fmt.Errorf("failed to create source: %w", err)
			}

			/*
				sink, err := sinks.New(conf.Pipeline.Sink)
				if err != nil {
					return fmt.Errorf("failed to create sink: %w", err)
				}

				handler, err := handlers.New(conf.Pipeline.Handler)
				if err != nil {
					return fmt.Errorf("failed to create handler: %w", err)
				}
			*/

			lock := &sync.Mutex{}
			turbine := core.NewTurbine(
				src,
				handlers.Noop{},
				nil,
				conf.Pipeline.BatchSize,
				time.Duration(conf.Pipeline.FlushIntervalSeconds)*time.Second,
				lock,
				core.PipelineErrorPolicies{
					// Source: conf.Pipeline.Source.Error.Policy,
				},
				core.WithTurbineLogger(l),
			)

			_, err = turbine.ConsumeLoop(ctx, 0)
			return err
		},
	}

	cmd.Flags().StringVarP(&configPath, "config", "c", "", "Path to turbine config file (required)")
	cmd.MarkFlagRequired("config")

	return cmd
}
