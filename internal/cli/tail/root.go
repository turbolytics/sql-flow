package tail

import (
	"fmt"
	"github.com/spf13/cobra"
	"github.com/turbolytics/turbine/internal/config"
	"github.com/turbolytics/turbine/internal/sources"
	"go.uber.org/zap"
	"time"
)

func NewCommand() *cobra.Command {
	var configPath string

	var cmd = &cobra.Command{
		Use:   "tail",
		Short: "Tail a stream of data",
		RunE: func(cmd *cobra.Command, args []string) error {
			logger, _ := zap.NewDevelopment()
			defer logger.Sync()
			l := logger.Named("turbine.tail")

			conf, err := config.Load(configPath, map[string]string{})
			if err != nil {
				return fmt.Errorf("failed to load config: %w", err)
			}

			src, err := sources.New(conf.Pipeline.Source, logger)
			if err != nil {
				return fmt.Errorf("failed to create source: %w", err)
			}

			if err := src.Start(); err != nil {
				return fmt.Errorf("failed to start source: %w", err)
			}

			stream := src.Stream()
			defer func() {
				if err := src.Close(); err != nil {
					l.Error("failed to close source", zap.Error(err))
				}
			}()

			// Status loop to display total messages processed every 5 seconds
			totalMessages := 0
			done := make(chan struct{})
			go func() {
				ticker := time.NewTicker(5 * time.Second)
				defer ticker.Stop()
				for {
					select {
					case <-ticker.C:
						l.Info("status update", zap.Int("totalMessages", totalMessages))
					case <-done:
						return
					}
				}
			}()

			// Read all messages from the source
			for range stream {
				totalMessages++
				// fmt.Printf("Message: %s\n", string(msg.Value()))
				if err := src.Commit(); err != nil {
					l.Error("failed to commit message", zap.Error(err))
					return err
				}
			}

			// Signal the status loop to stop
			close(done)
			return nil
		},
	}

	cmd.Flags().StringVarP(&configPath, "config", "c", "", "Path to turbine config file (required)")
	cmd.MarkFlagRequired("config")
	return cmd

}
