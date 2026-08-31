package cli

import (
	"fmt"
	"runtime"

	"github.com/spf13/cobra"
)

// Version and Commit are stamped at link time by the Makefile and the container
// build:
//
//	go build -ldflags "-X github.com/turbolytics/turbine/internal/cli.Version=v1.0.0"
//
// A plain `go build ./cmd/turbine/` leaves the defaults below, which is how an
// unreleased local binary identifies itself.
var (
	Version = "dev"
	Commit  = "unknown"
)

// versionString is what both `turbine version` and `turbine --version` print.
func versionString() string {
	return fmt.Sprintf(
		"turbine %s\ncommit: %s\ngo:     %s\n",
		Version, Commit, runtime.Version(),
	)
}

func newVersionCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print the turbine version",
		Args:  cobra.NoArgs,
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Fprint(cmd.OutOrStdout(), versionString())
		},
	}
}
