// Package rollup is the `sqlflow rollup` command.
package rollup

import (
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/errs"
	gen "github.com/turbolytics/sql-flow/internal/rollup"
)

func NewCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "rollup",
		Short: "Declare rollup tables, keep them filled, and generate the serve datasets that read them",
		Long: "Generate, from a rollups file, the migration that creates rollup tables and the " +
			"triggers that keep them current, and the serve datasets that read them. install " +
			"applies the tables and triggers itself, and run keeps them installed and fills " +
			"their history. verify checks every table against the table it is built from. " +
			"check fails when a committed copy of a generated file has drifted.",
	}
	cmd.AddCommand(newDDLCommand(), newServeCommand(), newCheckCommand(), newInstallCommand(), newRunCommand(), newVerifyCommand())
	return cmd
}

func newDDLCommand() *cobra.Command {
	var configPath, backend string
	cmd := &cobra.Command{
		Use:   "ddl",
		Short: "Print the migration that creates the rollup tables and keeps them current",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if backend != "postgres" {
				return errs.New(errs.CodeConfigInvalid, "--backend %q is not supported; the only backend is postgres", backend)
			}
			conf, err := config.LoadRollups(configPath)
			if err != nil {
				return err
			}
			script, err := gen.PostgresDDL(conf)
			if err != nil {
				return err
			}
			_, err = io.WriteString(cmd.OutOrStdout(), script)
			return err
		},
	}
	cmd.Flags().StringVarP(&configPath, "config", "c", "", "Path to the rollups file")
	cmd.Flags().StringVar(&backend, "backend", "postgres", "The database the migration is written for")
	_ = cmd.MarkFlagRequired("config")
	return cmd
}

func newServeCommand() *cobra.Command {
	var configPath, dataset string
	cmd := &cobra.Command{
		Use:   "serve",
		Short: "Print the serve datasets that read the rollup tables",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			conf, err := config.LoadRollups(configPath)
			if err != nil {
				return err
			}
			datasets, err := gen.ServeDatasets(conf)
			if err != nil {
				return err
			}
			if dataset != "" {
				var picked []config.ServeDataset
				for _, ds := range datasets {
					if ds.Name == dataset {
						picked = append(picked, ds)
					}
				}
				if len(picked) == 0 {
					return errs.New(errs.CodeConfigInvalid, "the rollups file declares no dataset %s", dataset)
				}
				datasets = picked
			}
			out, err := gen.ServeYAML(datasets)
			if err != nil {
				return err
			}
			_, err = cmd.OutOrStdout().Write(out)
			return err
		},
	}
	cmd.Flags().StringVarP(&configPath, "config", "c", "", "Path to the rollups file")
	cmd.Flags().StringVar(&dataset, "dataset", "", "Print only this dataset")
	_ = cmd.MarkFlagRequired("config")
	return cmd
}

func newCheckCommand() *cobra.Command {
	var configPath, migrationPath, servePath string
	cmd := &cobra.Command{
		Use:   "check",
		Short: "Fail when a committed migration or serve file has drifted from the rollups file",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			conf, err := config.LoadRollups(configPath)
			if err != nil {
				return err
			}
			migration, err := os.ReadFile(migrationPath)
			if err != nil {
				return errs.New(errs.CodeConfigNotFound, "migration not found: %s", migrationPath)
			}
			serve, _, err := config.LoadServeRendered(servePath, nil)
			if err != nil {
				return err
			}

			violations := gen.Check(conf, migration, serve)
			if len(violations) == 0 {
				fmt.Fprintf(cmd.OutOrStdout(), "%s, %s and %s agree\n", configPath, migrationPath, servePath)
				return nil
			}

			files := map[string]string{"rollups": configPath, "migration": migrationPath, "serve": servePath}
			for _, v := range violations {
				fmt.Fprintf(cmd.ErrOrStderr(), "%s: %s: [%s] %s\n", files[v.Path[0]], strings.Join(v.Path, "."), v.Code, v.Message)
			}
			// Each line above is the message. Cobra printing a summary again
			// would bury them.
			cmd.SilenceErrors = true
			return errs.New(violations[0].Code, "%d problems between %s, %s and %s",
				len(violations), configPath, migrationPath, servePath)
		},
	}
	cmd.Flags().StringVarP(&configPath, "config", "c", "", "Path to the rollups file")
	cmd.Flags().StringVar(&migrationPath, "migration", "", "Path to the committed migration")
	cmd.Flags().StringVar(&servePath, "serve", "", "Path to the committed serve file")
	for _, f := range []string{"config", "migration", "serve"} {
		_ = cmd.MarkFlagRequired(f)
	}
	return cmd
}
