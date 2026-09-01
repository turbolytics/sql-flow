package cli

import (
	"fmt"
	"runtime"

	"github.com/spf13/cobra"
)

// Version and Commit are stamped at link time by the Makefile and the container
// build:
//
//	go build -ldflags "-X github.com/turbolytics/sql-flow/internal/cli.Version=v1.0.0"
//
// A plain `go build ./cmd/sqlflow/` leaves the defaults below, which is how an
// unreleased local binary identifies itself.
var (
	Version = "dev"
	Commit  = "unknown"
)

// versionString is what both `sqlflow version` and `sqlflow --version` print.
func versionString() string {
	return fmt.Sprintf(
		"sqlflow %s\ncommit: %s\ngo:     %s\n",
		Version, Commit, runtime.Version(),
	)
}

func newVersionCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print the sqlflow version",
		Args:  cobra.NoArgs,
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Fprint(cmd.OutOrStdout(), versionString())
		},
	}
}
