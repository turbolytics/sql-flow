package rollup

import (
	"context"
	"fmt"
	"io"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/spf13/cobra"
	"github.com/turbolytics/sql-flow/internal/buildinfo"
	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/errs"
	gen "github.com/turbolytics/sql-flow/internal/rollup"
)

func newTestCommand() *cobra.Command {
	var configPath, dsn, testsPath string
	var seed int64
	var keep bool
	cmd := &cobra.Command{
		Use:   "test",
		Short: "Check a rollups file on a real Postgres, in a schema the command creates and drops",
		Long: "Clone each source table into a new schema on --dsn, install the rollups there, and " +
			"write a seeded workload through the triggers: 20 batches per rollup, across a daylight " +
			"saving change, in three session zones. Then run each case of --tests from empty tables. " +
			"Every table must equal a from-scratch GROUP BY of its source, and every bucket must lie " +
			"on its grain's UTC boundary. A failed check exits non-zero with " +
			"user.config.rollup_test_failed. The command connects only to --dsn, never to the " +
			"file's store, and writes nothing outside its schema. --seed repeats a run; --keep " +
			"leaves the schema for inspection.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			conf, err := config.LoadRollups(configPath)
			if err != nil {
				return err
			}
			// The files' rules before the database, so a typo is reported
			// as a typo and not as a connection failure.
			if err := conf.CheckError(); err != nil {
				return err
			}
			var tests config.RollupTests
			if testsPath != "" {
				loaded, err := config.LoadRollupTests(testsPath)
				if err != nil {
					return err
				}
				if err := loaded.CheckError(conf); err != nil {
					return err
				}
				tests = *loaded
			}
			// pgx's parse error quotes the DSN, password and all.
			if _, err := pgx.ParseConfig(dsn); err != nil {
				return errs.New(errs.CodeConfigInvalid, "--dsn is not a Postgres connection string or URL")
			}
			if !cmd.Flags().Changed("seed") {
				seed = time.Now().UnixNano()
			}
			// Ctrl-C and a CI runner's SIGTERM end the run through its
			// context, so the deferred cleanup still drops the schema.
			parent := cmd.Context()
			if parent == nil {
				parent = context.Background()
			}
			ctx, stop := signal.NotifyContext(parent, syscall.SIGINT, syscall.SIGTERM)
			defer stop()
			conn, err := gen.ConnectDSN(ctx, dsn)
			if err != nil {
				return err
			}
			defer conn.Close(context.Background())

			out := cmd.OutOrStdout()
			fmt.Fprintf(out, "seed %d\n", seed)
			sb, err := gen.OpenSandbox(ctx, conn, conf, buildinfo.Version)
			if err != nil {
				return err
			}
			// The schema goes even when a check fails or the context ends.
			defer func() {
				if cerr := sb.Release(context.Background(), conn, keep); cerr != nil && err == nil {
					err = cerr
				}
				if keep {
					fmt.Fprintf(out, "kept schema %s\n", sb.Schema)
				}
			}()

			// Each printed verdict is one check: a rollup's workload, or a
			// case.
			checks, failed := 0, 0
			reports, err := gen.RunWorkload(ctx, conn, conf, seed)
			if err != nil {
				return err
			}
			invariants, err := gen.CheckInvariants(ctx, conn, conf)
			if err != nil {
				return err
			}
			broken := map[string][]gen.InvariantFailure{}
			for _, f := range invariants {
				broken[f.Rollup] = append(broken[f.Rollup], f)
			}
			for _, rep := range reports {
				checks++
				fs := broken[rep.Rollup]
				fmt.Fprintf(out, "workload %s: %d batches, %d rows: %s\n", rep.Rollup, rep.Batches, rep.Rows, verdict(len(fs) == 0))
				if len(fs) > 0 {
					failed++
					printInvariants(out, fs)
				}
			}

			for i, tc := range tests.Tests {
				checks++
				failures, err := gen.RunCase(ctx, conn, conf, i, tc)
				if err != nil {
					return err
				}
				fmt.Fprintf(out, "case %s: %s\n", tc.Name, verdict(len(failures) == 0))
				if len(failures) > 0 {
					failed++
					printCaseFailures(out, failures)
				}
			}
			if failed > 0 {
				return errs.New(errs.CodeConfigRollupTestFailed, "%d of %d checks failed; rerun with --seed %d to repeat the workload", failed, checks, seed)
			}
			return nil
		},
	}
	cmd.Flags().StringVarP(&configPath, "config", "c", "", "Path to the rollups file")
	cmd.Flags().StringVar(&dsn, "dsn", "", "The Postgres to test on, with the team's migrations applied; never the file's store")
	cmd.Flags().StringVar(&testsPath, "tests", "", "Path to a file of fixture cases, run after the workload")
	cmd.Flags().Int64Var(&seed, "seed", 0, "Seed for the generated workload; default a new one, printed")
	cmd.Flags().BoolVar(&keep, "keep", false, "Keep the schema the command creates, for inspection")
	_ = cmd.MarkFlagRequired("config")
	_ = cmd.MarkFlagRequired("dsn")
	return cmd
}

func verdict(ok bool) string {
	if ok {
		return "ok"
	}
	return "failed"
}

func printInvariants(w io.Writer, fs []gen.InvariantFailure) {
	for _, f := range fs {
		fmt.Fprintf(w, "  %s: %s: %d buckets break it\n", f.Table, f.Invariant, f.Buckets)
		for _, row := range f.Sample {
			line := fmt.Sprintf("    %s %s", row.Bucket.UTC().Format(time.RFC3339), row.Kind)
			if row.Key != "" {
				line += " " + row.Key
			}
			if row.Measure != "" {
				line += fmt.Sprintf(": %s stored %s, from scratch %s", row.Measure, orNone(row.Stored), orNone(row.Recomputed))
			}
			fmt.Fprintln(w, line)
		}
	}
}

func printCaseFailures(w io.Writer, fs []gen.CaseFailure) {
	for _, f := range fs {
		fmt.Fprintf(w, "  %s %s %s\n", f.Table, f.Kind, f.Key)
		if f.Expected != "" {
			fmt.Fprintf(w, "    expected %s\n", f.Expected)
		}
		if f.Actual != "" {
			fmt.Fprintf(w, "    actual   %s\n", f.Actual)
		}
	}
}
