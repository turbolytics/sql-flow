package cli

import (
	"fmt"
	"github.com/spf13/cobra"
	"github.com/turbolytics/sql-flow/internal/cli/run"
	"github.com/turbolytics/sql-flow/internal/cli/tail"
	"github.com/turbolytics/sql-flow/internal/errs"
	"os"
)

func NewRootCommand() *cobra.Command {
	var cmd = &cobra.Command{
		Use:   "sqlflow",
		Short: "",
		Long:  ``,
		// The run function is called when the command is executed
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println("Welcome to sqlflow!")
		},
		// Cobra prints the whole flag list for any error a command returns,
		// which buries the one line that says what failed. Flags have already
		// parsed by the time this runs, so a usage error still gets usage and
		// a runtime failure does not.
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			cmd.SilenceUsage = true
			return nil
		},
	}

	// Setting Version gives cobra a --version flag for free; the template makes
	// it print exactly what the version subcommand does.
	cmd.Version = Version
	cmd.SetVersionTemplate(versionString())

	cmd.AddCommand(run.NewCommand())
	cmd.AddCommand(tail.NewCommand())
	cmd.AddCommand(newConfigCommand())
	cmd.AddCommand(newDevCommand())
	cmd.AddCommand(newVersionCommand())

	return cmd
}

func Execute() {
	os.Exit(execute(NewRootCommand()))
}

// execute runs the command and returns the code the process should exit with.
//
// The code is the part of the error taxonomy a supervisor actually reads, so
// it has to come from the error rather than being a constant. Exiting 1 for
// every failure marks a bad config and a corrupt state file as retryable, and
// a supervisor then restarts both forever.
//
// Nothing is printed here: cobra has already reported the error on stderr.
func execute(cmd *cobra.Command) int {
	if err := cmd.Execute(); err != nil {
		return errs.ExitCode(err)
	}
	return errs.ExitOK
}
