//go:build leakloop

package sinks

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/turbolytics/sql-flow/internal/config"
)

// The postgres sink under the sqlcommand loop's load: one closed minute of
// 40 rows per flush into a table that grows by that much every flush. Beside
// TestSinkSQLCommand__PostgresUpsertPerFlush, which does the same write
// through the DuckDB extension, and TestSinkSQLCommand__PostgresInsertPerFlush,
// which is a plain insert through it. Run with dev/bench/leakloops.sh.

func runPostgresSinkLoop(t *testing.T, name, table, mode string, key []string) {
	dsn := os.Getenv("SQLFLOW_LEAK_POSTGRES")
	if dsn == "" {
		t.Fatal("SQLFLOW_LEAK_POSTGRES is required: a Postgres connection string as this process sees it")
	}
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	for _, sql := range []string{
		fmt.Sprintf("DROP TABLE IF EXISTS %s", table),
		fmt.Sprintf(`CREATE TABLE %s (bucket TIMESTAMPTZ NOT NULL, lang TEXT NOT NULL, posts INTEGER NOT NULL,
			updated_at TIMESTAMPTZ NOT NULL DEFAULT now(), PRIMARY KEY (bucket, lang))`, table),
	} {
		if _, err := conn.Exec(ctx, sql); err != nil {
			t.Fatal(err)
		}
	}
	conn.Close(ctx)

	s, err := NewPostgresSink(config.PostgresSink{DSN: dsn, Table: table, Mode: mode, Key: key})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	runSinkLoop(t, name, s, nil, newMinute, nil)
}

// TestSinkPostgres__UpsertPerFlush is the demo's write through the keyed
// sink. The expectation is the plain insert's rate.
func TestSinkPostgres__UpsertPerFlush(t *testing.T) {
	runPostgresSinkLoop(t, "postgres sink, upsert, growing table", "leakloop_pgsink_upsert",
		PostgresModeUpsert, []string{"bucket", "lang"})
}

func TestSinkPostgres__AppendPerFlush(t *testing.T) {
	runPostgresSinkLoop(t, "postgres sink, append, growing table", "leakloop_pgsink_append",
		PostgresModeAppend, nil)
}
