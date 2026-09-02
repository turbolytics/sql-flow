package core

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// batchTable is the transient table the inferred handlers create per batch.
// Like the offsets table it is engine machinery, not user state, and its row
// count changes on every batch -- reporting it would be noise.
const batchTable = "batch"

// TableStat is one managed table and how much is in it.
type TableStat struct {
	Name string `json:"name"`
	Rows int64  `json:"rows"`
}

// OffsetStat is one partition's durable position.
type OffsetStat struct {
	Topic       string `json:"topic"`
	Partition   int32  `json:"partition"`
	Offset      int64  `json:"offset"`
	LeaderEpoch int32  `json:"leader_epoch"`
}

// StateStats is a snapshot of a pipeline's durable state: how large it is,
// what is in it, and how far through the stream it has committed.
//
// It is produced once and rendered by two consumers -- the /stats endpoint of
// a running pipeline and the CLI reading a stopped one -- so the two can never
// disagree about what a pipeline's state contains.
type StateStats struct {
	Path      string       `json:"path"`
	SizeBytes int64        `json:"size_bytes"`
	Tables    []TableStat  `json:"tables"`
	Offsets   []OffsetStat `json:"offsets"`
}

// CollectStateStats reads a snapshot of the state database.
//
// The connection should be one dedicated to reading, not the connection the
// pipeline writes on. Connections to a DuckDB database have independent
// transaction state, so a reader sees committed rows only: the numbers here
// describe what would survive a crash at this moment, and collecting them
// cannot block the writer or be blocked by it.
func CollectStateStats(ctx context.Context, conn adbc.Connection, path string) (*StateStats, error) {
	stats := &StateStats{
		Path:    path,
		Tables:  []TableStat{},
		Offsets: []OffsetStat{},
	}

	if path != "" {
		// A database that has never been flushed to disk has no file yet;
		// that is a size of zero, not an error.
		if info, err := os.Stat(path); err == nil {
			stats.SizeBytes = info.Size()
		}
	}

	names, err := userTables(ctx, conn)
	if err != nil {
		return nil, err
	}
	for _, name := range names {
		rows, err := countRows(ctx, conn, name)
		if err != nil {
			return nil, err
		}
		stats.Tables = append(stats.Tables, TableStat{Name: name, Rows: rows})
	}

	// Read through the store rather than a second query, so there is one
	// definition of the offsets schema.
	marks, err := NewOffsetStore(conn).Load(ctx)
	if err != nil {
		return nil, fmt.Errorf("loading offsets: %w", err)
	}
	// Each iterates sorted, so the result is ordered without sorting again.
	marks.Each(func(topic string, partition int32, m Mark) {
		stats.Offsets = append(stats.Offsets, OffsetStat{
			Topic:       topic,
			Partition:   partition,
			Offset:      m.Offset,
			LeaderEpoch: m.LeaderEpoch,
		})
	})

	return stats, nil
}

// userTables lists the tables that hold pipeline state, excluding the engine's
// own. The result is sorted so repeated calls render identically.
func userTables(ctx context.Context, conn adbc.Connection) ([]string, error) {
	stmt, err := conn.NewStatement()
	if err != nil {
		return nil, fmt.Errorf("creating statement: %w", err)
	}
	defer stmt.Close()

	if err := stmt.SetSqlQuery(
		`SELECT table_name FROM duckdb_tables() WHERE schema_name = 'main' ORDER BY table_name`,
	); err != nil {
		return nil, fmt.Errorf("setting table-list query: %w", err)
	}

	reader, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing tables: %w", err)
	}
	defer reader.Release()

	var names []string
	for reader.Next() {
		rec := reader.Record()
		col, ok := rec.Column(0).(*array.String)
		if !ok {
			return nil, fmt.Errorf("unexpected type for table_name column: %T", rec.Column(0))
		}
		for i := 0; i < int(rec.NumRows()); i++ {
			name := col.Value(i)
			if name == offsetsTable || name == batchTable {
				continue
			}
			// Clone: the value aliases the record's backing buffer, which is
			// released -- and can be freed or reused -- when this returns.
			names = append(names, strings.Clone(name))
		}
	}
	if err := reader.Err(); err != nil {
		return nil, fmt.Errorf("reading table list: %w", err)
	}

	sort.Strings(names)
	return names, nil
}

func countRows(ctx context.Context, conn adbc.Connection, table string) (int64, error) {
	stmt, err := conn.NewStatement()
	if err != nil {
		return 0, fmt.Errorf("creating statement: %w", err)
	}
	defer stmt.Close()

	// Quoted: a managed table may be named anything the user's DDL allows.
	query := fmt.Sprintf(`SELECT count(*) FROM "%s"`, strings.ReplaceAll(table, `"`, `""`))
	if err := stmt.SetSqlQuery(query); err != nil {
		return 0, fmt.Errorf("setting count query for %q: %w", table, err)
	}

	reader, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		return 0, fmt.Errorf("counting rows in %q: %w", table, err)
	}
	defer reader.Release()

	var count int64
	for reader.Next() {
		rec := reader.Record()
		if rec.NumRows() == 0 {
			continue
		}
		col, ok := rec.Column(0).(*array.Int64)
		if !ok {
			return 0, fmt.Errorf("unexpected type for count column: %T", rec.Column(0))
		}
		count = col.Value(0)
	}
	if err := reader.Err(); err != nil {
		return 0, fmt.Errorf("reading count for %q: %w", table, err)
	}
	return count, nil
}
