package rollup

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/errs"
	gen "github.com/turbolytics/sql-flow/internal/rollup"
)

func newVerifyCommand() *cobra.Command {
	var configPath, since string
	cmd := &cobra.Command{
		Use:   "verify",
		Short: "Check every rollup table against the table it is built from, then exit",
		Long: "Recompute every rollup table's buckets from the table it is built from and compare " +
			"them with the stored rows. Exits non-zero with system.rollup.drift when a bucket " +
			"differs. verify only reads: it takes no lock and changes nothing, so it runs beside " +
			"`sqlflow rollup run` or on a database no daemon manages. A table still filling is " +
			"skipped. --since bounds the buckets checked, because a retention job makes old " +
			"buckets differ from their source by design.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			conf, err := config.LoadRollups(configPath)
			if err != nil {
				return err
			}
			if err := conf.CheckError(); err != nil {
				return err
			}
			from, err := parseSince(since, time.Now())
			if err != nil {
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

			targets, err := gen.VerifyTargets(ctx, conn, conf)
			if err != nil {
				return err
			}
			out := cmd.OutOrStdout()
			var drifted []string
			var buckets int64
			for _, tg := range targets {
				if tg.Skip != "" {
					fmt.Fprintf(out, "%s: skipped, %s\n", tg.Table, tg.Skip)
					continue
				}
				v, err := gen.VerifySince(ctx, conn, tg, from)
				if err != nil {
					return err
				}
				printVerified(out, v)
				if v.DriftBuckets > 0 {
					drifted = append(drifted, v.Table)
					buckets += v.DriftBuckets
				}
			}
			if len(drifted) > 0 {
				return errs.New(errs.CodeRollupDrift, "%d buckets differ from the tables they are built from, in %s",
					buckets, strings.Join(drifted, ", "))
			}
			return nil
		},
	}
	cmd.Flags().StringVarP(&configPath, "config", "c", "", "Path to the rollups file")
	cmd.Flags().StringVar(&since, "since", "", "Check buckets from this RFC 3339 time, or this long ago, such as 36h or 7d; default all")
	_ = cmd.MarkFlagRequired("config")
	return cmd
}

// parseSince reads --since: an RFC 3339 time, or a duration back from now.
// Empty checks every bucket.
func parseSince(s string, now time.Time) (*time.Time, error) {
	if s == "" {
		return nil, nil
	}
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return &t, nil
	}
	if d, err := config.ParseServeDuration(s); err == nil {
		t := now.Add(-d)
		return &t, nil
	}
	return nil, errs.New(errs.CodeConfigInvalid, "--since %q is neither an RFC 3339 time nor a duration such as 36h or 7d", s)
}

func printVerified(w io.Writer, v gen.Verified) {
	fmt.Fprintf(w, "%s: %d buckets checked against %s, %d differ\n", v.Table, v.Buckets, v.BuiltFrom, v.DriftBuckets)
	for _, row := range v.Sample {
		line := fmt.Sprintf("  %s %s", row.Bucket.UTC().Format(time.RFC3339), row.Kind)
		if row.Key != "" {
			line += " " + row.Key
		}
		if row.Measure != "" {
			line += fmt.Sprintf(": %s stored %s, recomputed %s", row.Measure, orNone(row.Stored), orNone(row.Recomputed))
		}
		fmt.Fprintln(w, line)
	}
}

func orNone(s string) string {
	if s == "" {
		return "none"
	}
	return s
}
