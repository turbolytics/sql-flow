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

	return cmd
}

func Execute() {
	cmd := NewRootCommand()
	if err := cmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
