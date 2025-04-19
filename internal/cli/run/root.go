package run

import (
	"context"
	"errors"
	"fmt"
	"github.com/spf13/cobra"
	"github.com/turbolytics/turbine/internal/sinks"
	"github.com/turbolytics/turbine/internal/sources"
	"sync"
	"time"

	"github.com/turbolytics/turbine/internal/config"
	"github.com/turbolytics/turbine/internal/core"
	"github.com/turbolytics/turbine/internal/handlers"
)

func NewCommand() *cobra.Command {
	var configPath string

	var cmd = &cobra.Command{
		Use:   "run",
		Short: "Run turbine against a stream of data",
		RunE: func(cmd *cobra.Command, args []string) error {
			if configPath == "" {
				return errors.New("missing required --config flag")
			}

			conf, err := config.Load(configPath, map[string]string{})
			if err != nil {
				return fmt.Errorf("failed to load config: %w", err)
			}

			src, err := sources.New(conf.Pipeline.Source)
			if err != nil {
				return fmt.Errorf("failed to create source: %w", err)
			}

			sink, err := sinks.New(conf.Pipeline.Sink)
			if err != nil {
				return fmt.Errorf("failed to create sink: %w", err)
			}

			handler, err := handlers.New(conf.Pipeline.Handler)
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
				nil, // metrics meter - can be wired later
			)

			ctx := context.Background()
			_, err = turbine.ConsumeLoop(ctx, 0)
			return err
		},
	}

	cmd.Flags().StringVarP(&configPath, "config", "c", "", "Path to turbine config file (required)")
	cmd.MarkFlagRequired("config")

	return cmd
}
