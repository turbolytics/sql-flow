package managers

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/turbolytics/sql-flow/internal/duckdb"
	"github.com/turbolytics/sql-flow/internal/handlers"
	"github.com/turbolytics/sql-flow/internal/turbostats"
	"go.uber.org/zap"
)

// The window leak loop.
//
// The Bluesky demo grew from 100 MB to 380 MB in fourteen hours on Render.
// Five eight-hour soaks localized it to the window table, and a Python loop
// against DuckDB reproduced it in seconds: once a checkpoint has written them,
// rows deleted from a table that carries a UNIQUE INDEX are never freed, in
// memory or on disk, and the CHECKPOINT StructuredBatch runs after every
// truncate (#247) doubles the rate. Every shipped tumbling example uses that
// pattern. See #268.
//
// A leak that is linear in window operations needs operations, not wall
// clock. This loop builds the real handler and the real manager once, over one
// DuckDB connection the way the pipeline does, and drives them for
// SQLFLOW_LEAK_BATCHES batches (default 1000, so the -short pass runs it in
// seconds; set it to 100000 to watch a component hold on to resources over a
// long run). Every scenario reports the same three numbers, so a change to
// DuckDB, the handler, or the manager shows up here before it shows up on a
// dashboard.

// leakBatches is how many batches each scenario runs after warm-up.
func leakBatches(tb testing.TB) int {
	tb.Helper()
	if v := os.Getenv("SQLFLOW_LEAK_BATCHES"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			tb.Fatalf("SQLFLOW_LEAK_BATCHES=%q is not a positive integer", v)
		}
		return n
	}
	return 1000
}

const (
	leakMessagesPerBatch = 500
	leakLangs            = 40
	leakBatchesPerMinute = 4
	// closeAfter is how many minutes behind max(bucket) a window closes. The
	// demo uses one minute plus a one-minute grace.
	leakCloseAfter = 2
)

// leakScenario is one shape of window table under the same load.
type leakScenario struct {
	name string
	// path is the DuckDB file, empty for in-memory.
	path string
	// index puts the demo's UNIQUE INDEX on the window table.
	index bool
	// upsert uses INSERT ... ON CONFLICT DO UPDATE, which needs the index.
	// Without it the handler inserts one row per batch and the collect SQL
	// sums them, which is the workaround #268 proposes.
	upsert bool
}

func (s leakScenario) tableDDL() string {
	ddl := "CREATE TABLE IF NOT EXISTS w (bucket BIGINT, lang VARCHAR, posts INTEGER);"
	if s.index {
		ddl += " CREATE UNIQUE INDEX IF NOT EXISTS w_idx ON w (bucket, lang);"
	}
	return ddl
}

func (s leakScenario) handlerSQL() string {
	sql := `INSERT INTO w
		SELECT time_us // 60000000 AS bucket, lang, count(*) AS posts
		FROM posts GROUP BY bucket, lang`
	if s.upsert {
		sql += ` ON CONFLICT (bucket, lang) DO UPDATE SET posts = posts + EXCLUDED.posts`
	}
	return sql
}

func (s leakScenario) collectSQL() string {
	if s.upsert {
		return fmt.Sprintf(`SELECT bucket, lang, posts FROM w
			WHERE bucket < (SELECT max(bucket) FROM w) - %d`, leakCloseAfter)
	}
	return fmt.Sprintf(`SELECT bucket, lang, sum(posts) AS posts FROM w
		WHERE bucket < (SELECT max(bucket) FROM w) - %d GROUP BY bucket, lang`, leakCloseAfter)
}

func (s leakScenario) deleteSQL() string {
	return fmt.Sprintf(`DELETE FROM w WHERE bucket < (SELECT max(bucket) FROM w) - %d`, leakCloseAfter)
}

// leakSample is what the loop measures. All three come from the process, not
// from a profiler: a Go heap profile cannot see a DuckDB row group.
type leakSample struct {
	// tableBytes is duckdb_memory() for the two table-storage tags that grew
	// in the soaks.
	tableBytes int64
	// rowGroups and storedRows are the window table's physical footprint,
	// from pragma_storage_info. liveRows is what a SELECT sees.
	rowGroups, storedRows, liveRows int64
	// resident is the process's anonymous resident memory.
	resident int64
}

func (s leakSample) String() string {
	return fmt.Sprintf("tables %d MiB, %d row groups holding %d rows for %d live, resident %d MiB",
		s.tableBytes>>20, s.rowGroups, s.storedRows, s.liveRows, s.resident>>20)
}

func queryInt64s(tb testing.TB, conn adbc.Connection, sql string, n int) []int64 {
	tb.Helper()
	stmt, err := conn.NewStatement()
	if err != nil {
		tb.Fatal(err)
	}
	defer stmt.Close()
	if err := stmt.SetSqlQuery(sql); err != nil {
		tb.Fatal(err)
	}
	reader, _, err := stmt.ExecuteQuery(context.Background())
	if err != nil {
		tb.Fatalf("%s: %v", sql, err)
	}
	defer reader.Release()

	out := make([]int64, n)
	for reader.Next() {
		rec := reader.Record()
		if rec.NumRows() == 0 {
			continue
		}
		for i := 0; i < n; i++ {
			out[i] = rec.Column(i).(*array.Int64).Value(0)
		}
		return out
	}
	tb.Fatalf("%s: no rows", sql)
	return nil
}

func takeLeakSample(tb testing.TB, conn adbc.Connection) leakSample {
	tb.Helper()
	runtime.GC()
	debug.FreeOSMemory()

	mem := queryInt64s(tb, conn, `SELECT coalesce(sum(memory_usage_bytes), 0)::BIGINT
		FROM duckdb_memory() WHERE tag IN ('IN_MEMORY_TABLE', 'BASE_TABLE')`, 1)
	st := queryInt64s(tb, conn, `SELECT count(DISTINCT row_group_id)::BIGINT, coalesce(sum(count), 0)::BIGINT
		FROM pragma_storage_info('w') WHERE column_id = 0`, 2)
	live := queryInt64s(tb, conn, `SELECT count(*)::BIGINT FROM w`, 1)

	resident, err := turbostats.ResidentAnonBytes()
	if err != nil {
		tb.Fatal(err)
	}
	return leakSample{
		tableBytes: mem[0],
		rowGroups:  st[0],
		storedRows: st[1],
		liveRows:   live[0],
		resident:   resident,
	}
}

// leakLoop builds the real components and runs them. It returns the sample
// after warm-up and the sample at the end, so the caller judges the growth
// between the two rather than the absolute size, which includes the
// database's own fixed cost.
func leakLoop(tb testing.TB, sc leakScenario, batches int) (before, after leakSample) {
	tb.Helper()
	ctx := context.Background()

	if sc.path != "" {
		_ = os.Remove(sc.path)
		_ = os.Remove(sc.path + ".wal")
	}
	db, err := duckdb.OpenPath(ctx, sc.path)
	if err != nil {
		tb.Fatal(err)
	}
	defer db.Close()
	conn, err := db.Connect(ctx)
	if err != nil {
		tb.Fatal(err)
	}
	defer conn.Close()

	// The batch table StructuredBatch loads, and the window table the
	// handler SQL writes. Both come from the config's commands and tables
	// blocks in a real pipeline.
	exec(tb, conn, "CREATE TABLE posts (time_us BIGINT, lang VARCHAR)")
	exec(tb, conn, sc.tableDDL())

	h, err := handlers.New(conn, config.Handler{
		Type:  "handlers.StructuredBatch",
		Table: "posts",
		SQL:   sc.handlerSQL(),
	}, zap.NewNop())
	if err != nil {
		tb.Fatal(err)
	}
	if err := h.Init(ctx); err != nil {
		tb.Fatal(err)
	}

	// A lock shared with nobody: the pipeline serializes the handler and
	// the manager on one mutex, and this loop is the only caller of both.
	sink := &recordingSink{}
	m := NewTumbling(conn, sc.collectSQL(), sc.deleteSQL(), time.Hour, sink, &sync.Mutex{})

	// Messages for one batch, regenerated per minute so the bucket moves.
	// Forty languages per minute is the demo's cardinality.
	msgs := make([][]byte, leakMessagesPerBatch)
	minute := 0
	fill := func() {
		for i := range msgs {
			msgs[i] = []byte(fmt.Sprintf(`{"time_us": %d, "lang": "l%d"}`,
				int64(minute)*60_000_000+int64(i), i%leakLangs))
		}
	}

	run := func(n int) {
		for b := 0; b < n; b++ {
			if b%leakBatchesPerMinute == 0 {
				fill()
				minute++
			}
			for _, msg := range msgs {
				if err := h.Write(msg); err != nil {
					tb.Fatal(err)
				}
			}
			tbl, err := h.Invoke(ctx)
			if err != nil {
				tb.Fatal(err)
			}
			if tbl != nil {
				tbl.Release()
			}
			// The pipeline polls on a timer, about six times a minute at the
			// demo's rate. Once per batch keeps the ratio close enough.
			if err := m.Poll(ctx); err != nil {
				tb.Fatal(err)
			}
			// Init is where StructuredBatch truncates and checkpoints, after
			// the batch has been committed, which is the order the consume
			// loop runs it in.
			if err := h.Init(ctx); err != nil {
				tb.Fatal(err)
			}
		}
	}

	// Warm-up gets past the first few windows closing and DuckDB's initial
	// allocations, which is where a run that is not leaking still grows.
	warmup := leakBatchesPerMinute * (leakCloseAfter + 3)
	run(warmup)
	before = takeLeakSample(tb, conn)
	run(batches)
	after = takeLeakSample(tb, conn)

	rows, _ := sink.counts()
	tb.Logf("%s: %d batches, %d messages, %d window rows published\n  after warm-up: %s\n  at the end:    %s",
		sc.name, batches, batches*leakMessagesPerBatch, rows, before, after)
	return before, after
}

// The shipped pattern: UNIQUE INDEX, ON CONFLICT upsert, in memory. This is
// tumbling.window.yml, both Bluesky windowed examples, and the README. It
// leaks, and this test says so on purpose: if it starts passing the flat
// check, DuckDB changed and the docs in #268 are out of date.
func TestManagerTumblingWindow__IndexedTableInMemoryRetainsDeletedRows(t *testing.T) {
	coverage.Covers(t, "manager.tumbling_window")
	before, after := leakLoop(t, leakScenario{name: "indexed, upsert, in memory", index: true, upsert: true}, leakBatches(t))
	assertLeaks(t, before, after)
}

// The workaround: no index, one row per batch, the collect SQL sums. Storage
// stays at one row group however long it runs: measured at 15 million
// messages and 30,000 manager polls.
func TestManagerTumblingWindow__PlainInsertInMemoryStaysFlat(t *testing.T) {
	coverage.Covers(t, "manager.tumbling_window")
	before, after := leakLoop(t, leakScenario{name: "no index, plain insert, in memory"}, leakBatches(t))
	assertLeakFlat(t, before, after)
}

// The same indexed pattern on a file-backed database leaks the same way, so
// pipeline.state.path is not a way out. A Python loop against DuckDB suggested
// otherwise, and was wrong for a reason worth keeping: it never checkpointed,
// so every deleted row was still in the write-ahead log and vanished with it.
// The rows become permanent once a checkpoint writes them. StructuredBatch
// checkpoints every batch (#247), and DuckDB checkpoints on its own once the
// log reaches checkpoint_threshold, 16 MiB by default, so a real pipeline
// always gets there.
func TestManagerTumblingWindow__IndexedTableOnDiskRetainsDeletedRows(t *testing.T) {
	coverage.Covers(t, "manager.tumbling_window")
	path := filepath.Join(t.TempDir(), "window.duckdb")
	before, after := leakLoop(t, leakScenario{name: "indexed, upsert, on disk", path: path, index: true, upsert: true}, leakBatches(t))
	assertLeaks(t, before, after)
}

// assertLeaks is the signature of the footgun in #268: the table holds many
// times the rows a SELECT sees, in row groups that keep accumulating. If a
// DuckDB upgrade makes this fail, the leak is fixed upstream, and the
// examples, the README, and the validate lint in #268 are out of date.
func assertLeaks(t *testing.T, before, after leakSample) {
	t.Helper()
	if after.storedRows <= after.liveRows*4 {
		t.Fatalf("expected the window table to hold far more rows than are live; stored %d, live %d. "+
			"If DuckDB now reclaims deleted rows from an indexed table, update #268 and the examples.",
			after.storedRows, after.liveRows)
	}
	if after.rowGroups <= before.rowGroups {
		t.Fatalf("expected row groups to accumulate: %d after warm-up, %d at the end", before.rowGroups, after.rowGroups)
	}
}

// assertLeakFlat is the bound a healthy run holds, in DuckDB's own
// accounting. Two row groups is the steady state for a table rewritten every
// batch: the one being written and the one being vacuumed.
//
// Resident memory is reported and deliberately not asserted. On macOS
// ResidentAnonBytes falls back to getrusage, which is peak RSS: it only rises,
// and on the no-index run it rose 48, 97, then 117 MiB at 1k, 10k, and 30k
// batches while the table stayed at one row group. That is an allocator
// warming to a ceiling, not a leak linear in batches, and a threshold on it
// fails a healthy run. The memory soak on Linux, which reads RssAnon, is the
// gate for native growth.
func assertLeakFlat(t *testing.T, before, after leakSample) {
	t.Helper()
	const (
		maxRowGroupGrowth = 2
		maxTableGrowth    = 4 << 20
	)
	if g := after.rowGroups - before.rowGroups; g > maxRowGroupGrowth {
		t.Errorf("row groups grew by %d (%d -> %d); deleted rows are not being reclaimed",
			g, before.rowGroups, after.rowGroups)
	}
	if g := after.tableBytes - before.tableBytes; g > maxTableGrowth {
		t.Errorf("DuckDB table memory grew by %d MiB (%d -> %d MiB)",
			g>>20, before.tableBytes>>20, after.tableBytes>>20)
	}
}
