package rollup

import (
	"context"
	"fmt"
	"io"

	"github.com/spf13/cobra"
	"github.com/turbolytics/sql-flow/internal/buildinfo"
	"github.com/turbolytics/sql-flow/internal/config"
	gen "github.com/turbolytics/sql-flow/internal/rollup"
)

func newInstallCommand() *cobra.Command {
	var configPath string
	cmd := &cobra.Command{
		Use:   "install",
		Short: "Create the rollup tables and triggers a rollups file declares, then exit",
		Long: "Create every rollup table, function and trigger the rollups file declares, in one " +
			"transaction, and record what was applied in sqlflow_rollup_state. A change that would " +
			"corrupt stored rows changes nothing and exits non-zero. install fills no table: " +
			"`sqlflow rollup run` backfills the tables it marks.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			conf, err := config.LoadRollups(configPath)
			if err != nil {
				return err
			}
			// The file's rules before its database, so a typo is reported as
			// a typo and not as a connection failure.
			if err := conf.CheckError(); err != nil {
				return err
			}
			dsn, err := conf.PostgresDSN()
			if err != nil {
				return err
			}
			ctx := cmd.Context()
			if ctx == nil {
				ctx = context.Background()
			}
			conn, err := gen.Connect(ctx, dsn)
			if err != nil {
				return err
			}
			defer conn.Close(context.Background())

			report, err := gen.Install(ctx, conn, conf, buildinfo.Version)
			if err != nil {
				return err
			}
			printInstall(cmd.OutOrStdout(), report)
			return nil
		},
	}
	cmd.Flags().StringVarP(&configPath, "config", "c", "", "Path to the rollups file")
	_ = cmd.MarkFlagRequired("config")
	return cmd
}

// printInstall writes one line per thing the install did, so a deploy log
// shows what changed and what still needs `sqlflow rollup run`.
func printInstall(w io.Writer, rep *gen.InstallReport) {
	for _, r := range rep.Rollups {
		if r.Adopted {
			fmt.Fprintf(w, "rollup %s: adopted the tables a migration created\n", r.Name)
		}
		for _, t := range r.Created {
			fmt.Fprintf(w, "rollup %s: created %s\n", r.Name, t)
		}
		for _, t := range r.Dropped {
			fmt.Fprintf(w, "rollup %s: retained table %s no longer exists; its record is removed\n", r.Name, t)
		}
		for _, t := range r.Plan.Retain {
			fmt.Fprintf(w, "rollup %s: %s is no longer declared; its table and triggers stay\n", r.Name, t.Table)
		}
		for _, t := range r.Plan.Restore {
			fmt.Fprintf(w, "rollup %s: %s is declared again\n", r.Name, t)
		}
		for _, t := range r.Plan.Backfill {
			fmt.Fprintf(w, "rollup %s: %s awaits a backfill\n", r.Name, t)
		}
		fmt.Fprintf(w, "rollup %s: functions and triggers are current\n", r.Name)
	}
	for _, name := range rep.Undeclared {
		fmt.Fprintf(w, "rollup %s is no longer declared; its tables and triggers stay\n", name)
	}
}
