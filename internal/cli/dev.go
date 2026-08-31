package cli

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"os"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/spf13/cobra"
	"github.com/turbolytics/turbine/internal/config"
	"github.com/turbolytics/turbine/internal/core"
	"github.com/turbolytics/turbine/internal/duckdb"
	"github.com/turbolytics/turbine/internal/handlers"
	"github.com/turbolytics/turbine/internal/sinks"
	"go.uber.org/zap"
)

func newDevCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "dev",
		Short: "Development commands",
	}

	cmd.AddCommand(newDevInvokeCommand())

	return cmd
}

func newDevInvokeCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "invoke <config> <fixture>",
		Short: "Execute a pipeline against a static file. Use for verifying pipeline logic.",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			cmd.SilenceUsage = true

			ctx := cmd.Context()
			if ctx == nil {
				ctx = context.Background()
			}

			conn, err := duckdb.Open(ctx)
			if err != nil {
				return err
			}
			defer conn.Close()

			table, err := devInvoke(ctx, conn, args[0], args[1], cmd.OutOrStdout())
			if err != nil {
				return err
			}
			defer table.Release()

			return nil
		},
	}
}

// devInvoke runs the configured pipeline's handler over a JSONL fixture rather
// than a live source, and writes the resulting rows to out. It mirrors the
// Python lifecycle.invoke: config commands run first, the handler is built and
// initialized, every non-empty fixture line is written to it, and the batch is
// invoked once. The sink is deliberately not exercised, matching the Python
// default of invoke_sink=False.
//
// The returned table is owned by the caller, who must Release it.
func devInvoke(
	ctx context.Context,
	conn adbc.Connection,
	configPath string,
	fixturePath string,
	out io.Writer,
) (arrow.Table, error) {
	conf, err := config.Load(configPath, map[string]string{})
	if err != nil {
		return nil, fmt.Errorf("failed to load config: %w", err)
	}

	// Commands and tables are created before the handler is built: a
	// StructuredBatch handler derives its schema from a table they create,
	// and handler SQL may insert into a managed table.
	if err := core.InitCommands(conn, conf); err != nil {
		return nil, fmt.Errorf("failed to initialize commands: %w", err)
	}

	if err := core.InitTables(conn, conf); err != nil {
		return nil, fmt.Errorf("failed to initialize tables: %w", err)
	}

	if err := core.InitUDFs(conf); err != nil {
		return nil, err
	}

	handler, err := handlers.New(conn, conf.Pipeline.Handler, zap.NewNop())
	if err != nil {
		return nil, fmt.Errorf("failed to create handler: %w", err)
	}
	// Disk-backed handlers stage batch files under the results cache dir and
	// only remove them on close.
	if closer, ok := handler.(io.Closer); ok {
		defer closer.Close()
	}

	if err := handler.Init(ctx); err != nil {
		return nil, fmt.Errorf("failed to initialize handler: %w", err)
	}

	if err := writeFixture(handler, fixturePath); err != nil {
		return nil, err
	}

	table, err := handler.Invoke(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to invoke handler: %w", err)
	}

	sink := sinks.NewConsoleSinkTo(out)
	if err := sink.WriteTable(table); err != nil {
		table.Release()
		return nil, fmt.Errorf("failed to write results: %w", err)
	}
	if err := sink.Flush(); err != nil {
		table.Release()
		return nil, fmt.Errorf("failed to flush results: %w", err)
	}

	return table, nil
}

// writeFixture feeds each non-empty line of a JSONL fixture to the handler.
func writeFixture(handler core.Handler, path string) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("failed to open fixture: %w", err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	// Fixture rows can exceed the scanner's default 64KB line budget.
	scanner.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)

	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		// Handlers retain what they are given until the batch is invoked,
		// while the scanner reuses its buffer on every Scan, so the line has
		// to be copied out.
		msg := make([]byte, len(line))
		copy(msg, line)

		if err := handler.Write(msg); err != nil {
			return fmt.Errorf("failed to write fixture line: %w", err)
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("failed to read fixture: %w", err)
	}

	return nil
}
