package rollup

import (
	"context"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
	"github.com/turbolytics/sql-flow/internal/buildinfo"
	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/logging"
	"github.com/turbolytics/sql-flow/internal/rollup/daemon"
	"github.com/turbolytics/sql-flow/internal/turbostats"
)

func newRunCommand() *cobra.Command {
	var configPath, metrics string
	cmd := &cobra.Command{
		Use:   "run",
		Short: "Keep the rollup tables a rollups file declares installed and filled",
		Long: "Install what the rollups file declares, then lead its rollups and fill every " +
			"table install marks for backfill, one chunk at a time, beside a live pipeline. " +
			"Several instances may run: one leads and the rest stand by. --metrics prometheus " +
			"serves /metrics and /healthz on :8000.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			logger, levelErr := logging.New()
			defer func() { _ = logger.Sync() }()
			if levelErr != nil {
				return levelErr
			}
			// Rendered once and parsed, so the bundle's config hash is of
			// the text the daemon runs.
			rendered, err := config.RenderTemplate(configPath, nil)
			if err != nil {
				return err
			}
			conf, err := config.ParseRollups(rendered)
			if err != nil {
				return err
			}
			if err := conf.CheckError(); err != nil {
				return err
			}
			dsn, err := conf.PostgresDSN()
			if err != nil {
				return err
			}
			d, err := daemon.New(conf, dsn, daemon.Options{
				Version: buildinfo.Version, Commit: buildinfo.Commit, Logger: logger.Named("sqlflow.rollup"),
				Metrics: metrics, TurboStats: conf.TurboStats, ConfigHash: turbostats.HashConfig(rendered),
			})
			if err != nil {
				return err
			}

			// A supervisor stops the daemon with SIGTERM. The command's
			// context is the parent, so a test or an embedding caller can
			// stop it too.
			parent := cmd.Context()
			if parent == nil {
				parent = context.Background()
			}
			ctx, stop := signal.NotifyContext(parent, syscall.SIGINT, syscall.SIGTERM)
			defer stop()
			return d.Run(ctx)
		},
	}
	cmd.Flags().StringVarP(&configPath, "config", "c", "", "Path to the rollups file")
	cmd.Flags().StringVar(&metrics, "metrics", "", "Metrics exporter to enable (prometheus); serves /metrics and /healthz on :8000")
	_ = cmd.MarkFlagRequired("config")
	return cmd
}
