package cli

import (
	"fmt"
	"github.com/spf13/cobra"
	"github.com/turbolytics/turbine/internal/cli/run"
	"github.com/turbolytics/turbine/internal/cli/tail"
	"os"
)

func NewRootCommand() *cobra.Command {
	var cmd = &cobra.Command{
		Use:   "turbine",
		Short: "",
		Long:  ``,
		// The run function is called when the command is executed
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println("Welcome to turbine!")
		},
	}

	cmd.AddCommand(run.NewCommand())
	cmd.AddCommand(tail.NewCommand())
	cmd.AddCommand(newConfigCommand())
	cmd.AddCommand(newDevCommand())

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
