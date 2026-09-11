package cli

import (
	"fmt"
	"runtime"

	"github.com/spf13/cobra"
	"github.com/turbolytics/sql-flow/internal/buildinfo"
)

// Version and Commit live in internal/buildinfo, so the run command can
// report them in the TurboStats bundle without importing this package, which
// would be a cycle. These names stay here because root.go sets cmd.Version
// from one of them.
var (
	Version = buildinfo.Version
	Commit  = buildinfo.Commit
)

// versionString is what both `sqlflow version` and `sqlflow --version` print.
//
// It reads buildinfo rather than the copies above. -X sets buildinfo.Version
// at link time and the copies are initialized from it, so the two agree; one
// source is still one fewer thing to reason about.
func versionString() string {
	return fmt.Sprintf(
		"sqlflow %s\ncommit: %s\ngo:     %s\n",
		buildinfo.Version, buildinfo.Commit, runtime.Version(),
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
