package cli

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/turbolytics/sql-flow/internal/errs"
	"github.com/turbolytics/sql-flow/internal/validate"
)

func newValidateCommand() *cobra.Command {
	var asJSON bool

	cmd := &cobra.Command{
		Use:   "validate <config>",
		Short: "Check a pipeline without running it",
		Long: "Check a pipeline offline. validate reaches no broker and no sink, " +
			"executes nothing from the commands block, and reports every fault it " +
			"finds in one pass.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			// The config parsed as a command line fine. Usage is noise.
			cmd.SilenceUsage = true

			src, err := os.ReadFile(args[0])
			if err != nil {
				return errs.New(errs.CodeConfigNotFound, "config file not found: %s", args[0])
			}

			rep, err := validate.Validate(cmd.Context(), validate.Request{
				Path:   args[0],
				Config: string(src),
			})
			if err != nil {
				return err
			}

			if asJSON {
				enc := json.NewEncoder(cmd.OutOrStdout())
				enc.SetIndent("", "  ")
				if err := enc.Encode(rep); err != nil {
					return err
				}
			} else {
				writeText(cmd, rep)
			}

			if !rep.OK {
				// The report is the message. Rendering the same fault a
				// second time as an error string helps nobody.
				cmd.SilenceErrors = true
				return errs.New(errs.CodeConfigInvalid, "%s is invalid", args[0])
			}
			return nil
		},
	}

	cmd.Flags().BoolVar(&asJSON, "json", false, "Emit the report as JSON")
	return cmd
}

// writeText renders the report for a human. It says what was skipped as
// plainly as what failed: an unqualified "valid" over checks that never ran is
// the one output this tool must not produce.
func writeText(cmd *cobra.Command, rep validate.Report) {
	out := cmd.OutOrStdout()

	for _, d := range rep.Diagnostics {
		where := ""
		if d.Position != nil {
			where = fmt.Sprintf("%s:%d:%d: ", rep.Config, d.Position.Line, d.Position.Column)
		}
		fmt.Fprintf(out, "%s%s: [%s] %s\n", where, d.Severity, d.Code, d.Message)
		if len(d.DidYouMean) > 0 {
			fmt.Fprintf(out, "    did you mean: %v\n", d.DidYouMean)
		}
		if d.Action != "" {
			fmt.Fprintf(out, "    %s\n", d.Action)
		}
	}

	for _, c := range rep.Checks {
		if c.Status == validate.StatusSkipped {
			fmt.Fprintf(out, "skipped %s: %s\n", c.ID, c.Reason)
		}
	}

	if vars := rep.Vars(); vars != nil && len(vars.Unused) > 0 {
		fmt.Fprintf(out, "supplied but never read: %v\n", vars.Unused)
	}

	if rep.OK {
		fmt.Fprintf(out, "%s: valid\n", rep.Config)
	}
}
