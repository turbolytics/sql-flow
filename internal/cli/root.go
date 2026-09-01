package cli

import (
	"fmt"
	"github.com/spf13/cobra"
	"github.com/turbolytics/sql-flow/internal/cli/run"
	"github.com/turbolytics/sql-flow/internal/cli/tail"
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
	cmd := NewRootCommand()
	// cobra has already reported the error on stderr by this point; printing
	// it again here would duplicate every message.
	if err := cmd.Execute(); err != nil {
		os.Exit(1)
	}
}
