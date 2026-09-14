//go:build leakloop

package sinks

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/turbolytics/sql-flow/internal/leakloop"
)

// The sqlcommand leak loops. Every flush drops and recreates
// sqlflow_sink_batch, ingests the batch into it, and runs the user's SQL. The
// Bluesky demo flushes once per closed minute, an upsert into an attached
// Postgres, so these report growth per flush with and without Postgres behind
// it.

var windowRowSchema = arrow.NewSchema([]arrow.Field{
	{Name: "bucket", Type: arrow.FixedWidthTypes.Timestamp_us},
	{Name: "lang", Type: arrow.BinaryTypes.String},
	{Name: "posts", Type: arrow.PrimitiveTypes.Int32},
}, nil)

var windowLangs = []string{"en", "ja", "pt", "es", "de", "fr", "ko", "zh", "it", "nl",
	"tr", "pl", "ru", "id", "th", "uk", "ar", "fa", "hi", "sv",
	"fi", "no", "da", "cs", "el", "he", "hu", "ro", "vi", "ca",
	"eu", "gl", "tl", "ms", "bg", "hr", "sk", "sl", "et", "unknown"}

// closedMinute is one closed window as the manager hands it to the sink: one
// row per language for minute i.
func closedMinute(i int) arrow.Table {
	b := array.NewRecordBuilder(memory.DefaultAllocator, windowRowSchema)
	defer b.Release()
	bucket := arrow.Timestamp(time.Date(2026, 9, 13, 0, 0, 0, 0, time.UTC).Add(time.Duration(i) * time.Minute).UnixMicro())
	for j, lang := range windowLangs {
		b.Field(0).(*array.TimestampBuilder).Append(bucket)
		b.Field(1).(*array.StringBuilder).Append(lang)
		b.Field(2).(*array.Int32Builder).Append(int32(j + i%100))
	}
	rec := b.NewRecord()
	defer rec.Release()
	return array.NewTableFromRecords(windowRowSchema, []arrow.Record{rec})
}

// runSinkLoop flushes one closed minute per flush. minute picks which minute
// flush i writes, so a loop can write new keys every time or the same ones.
func runSinkLoop(t *testing.T, name string, s *SQLCommandSink, minute func(i int) int, after func()) {
	flushes := leakloop.Count(t, 300)
	step := flushes / 20
	if step == 0 {
		step = 1
	}
	ctx := context.Background()
	loop := leakloop.New(t, name, "flush", s.conn)
	loop.Sample(0)
	for i := 1; i <= flushes; i++ {
		tbl := closedMinute(minute(i))
		if err := s.WriteTable(ctx, tbl); err != nil {
			t.Fatal(err)
		}
		tbl.Release()
		if err := s.Flush(ctx); err != nil {
			t.Fatal(err)
		}
		if after != nil {
			after()
		}
		if i%step == 0 {
			loop.Sample(int64(i))
		}
	}
	loop.Report()
}

// TestSinkSQLCommand__LocalTablePerFlush is the sink's own work with no
// Postgres: the drop, the ingest, and an insert into a DuckDB table that is
// emptied after every flush.
func TestSinkSQLCommand__LocalTablePerFlush(t *testing.T) {
	conn := newSinkTestConn(t)
	exec(t, conn, `CREATE TABLE w_local (bucket TIMESTAMPTZ, lang TEXT, posts INTEGER)`)
	s, err := NewSQLCommandSink(conn, `INSERT INTO w_local SELECT bucket, lang, posts FROM sqlflow_sink_batch`, nil)
	if err != nil {
		t.Fatal(err)
	}
	runSinkLoop(t, "sqlcommand, local table", s, newMinute, func() { exec(t, conn, `DELETE FROM w_local`) })
}

func newMinute(i int) int { return i }
func sameMinute(int) int  { return 1 }

// pgSinkLoop is one Postgres sink scenario.
type pgSinkLoop struct {
	name string
	// table is the Postgres table, one per scenario so scenarios can run in
	// parallel against the same database.
	table string
	// sql is the sink statement; %[1]s is the table.
	sql    string
	minute func(i int) int
	// truncate empties the Postgres table after every flush, so its size
	// stays at one minute's rows.
	truncate bool
}

const demoUpsert = `INSERT INTO pg.%[1]s (bucket, lang, posts, updated_at)
SELECT bucket, lang, posts, now() FROM sqlflow_sink_batch
ON CONFLICT (bucket, lang) DO UPDATE
  SET posts = EXCLUDED.posts, updated_at = EXCLUDED.updated_at`

// run attaches SQLFLOW_LEAK_POSTGRES as pg, creates the demo's rollup table
// there, and flushes into it.
func (sc pgSinkLoop) run(t *testing.T) {
	dsn := os.Getenv("SQLFLOW_LEAK_POSTGRES")
	if dsn == "" {
		t.Fatal("SQLFLOW_LEAK_POSTGRES is required: a Postgres connection string as this process sees it")
	}
	conn := newSinkTestConn(t)
	exec(t, conn, `INSTALL postgres`)
	exec(t, conn, `LOAD postgres`)
	exec(t, conn, `ATTACH '`+dsn+`' AS pg (TYPE POSTGRES)`)
	exec(t, conn, fmt.Sprintf(`CALL postgres_execute('pg', 'DROP TABLE IF EXISTS %s')`, sc.table))
	exec(t, conn, fmt.Sprintf(`CALL postgres_execute('pg', 'CREATE TABLE %s (
		bucket TIMESTAMPTZ NOT NULL, lang TEXT NOT NULL, posts INTEGER NOT NULL,
		updated_at TIMESTAMPTZ NOT NULL DEFAULT now(), PRIMARY KEY (bucket, lang))')`, sc.table))
	exec(t, conn, `CALL pg_clear_cache()`)

	s, err := NewSQLCommandSink(conn, fmt.Sprintf(sc.sql, sc.table), nil)
	if err != nil {
		t.Fatal(err)
	}
	var after func()
	if sc.truncate {
		after = func() { exec(t, conn, fmt.Sprintf(`CALL postgres_execute('pg', 'TRUNCATE %s')`, sc.table)) }
	}
	runSinkLoop(t, sc.name, s, sc.minute, after)
}

// TestSinkSQLCommand__PostgresUpsertPerFlush is the demo's sink statement,
// verbatim but for the table name, against SQLFLOW_LEAK_POSTGRES. Every flush
// writes a new minute, so the table grows by one minute's rows per flush, as
// the demo's does.
func TestSinkSQLCommand__PostgresUpsertPerFlush(t *testing.T) {
	pgSinkLoop{name: "sqlcommand, postgres upsert, growing table", table: "leakloop_upsert",
		sql: demoUpsert, minute: newMinute}.run(t)
}

// TestSinkSQLCommand__PostgresUpsertSameKeysPerFlush writes the same minute
// every flush: the table holds one minute's rows and every row conflicts.
func TestSinkSQLCommand__PostgresUpsertSameKeysPerFlush(t *testing.T) {
	pgSinkLoop{name: "sqlcommand, postgres upsert, same keys", table: "leakloop_upsert_same",
		sql: demoUpsert, minute: sameMinute}.run(t)
}

// TestSinkSQLCommand__PostgresUpsertTruncatedPerFlush writes a new minute
// every flush and empties the table after it: nothing conflicts, and the
// table never holds more than one minute.
func TestSinkSQLCommand__PostgresUpsertTruncatedPerFlush(t *testing.T) {
	pgSinkLoop{name: "sqlcommand, postgres upsert, truncated table", table: "leakloop_upsert_trunc",
		sql: demoUpsert, minute: newMinute, truncate: true}.run(t)
}

// TestSinkSQLCommand__PostgresInsertPerFlush drops the ON CONFLICT. Every
// flush writes new keys, so the insert never conflicts.
func TestSinkSQLCommand__PostgresInsertPerFlush(t *testing.T) {
	pgSinkLoop{name: "sqlcommand, postgres plain insert, growing table", table: "leakloop_insert",
		sql: `INSERT INTO pg.%[1]s (bucket, lang, posts, updated_at)
SELECT bucket, lang, posts, now() FROM sqlflow_sink_batch`, minute: newMinute}.run(t)
}

// TestSinkSQLCommand__PostgresReadPerFlush writes nothing to Postgres. Each
// flush reads one row from the attached table into DuckDB: a round trip
// through the extension without a COPY.
func TestSinkSQLCommand__PostgresReadPerFlush(t *testing.T) {
	pgSinkLoop{name: "sqlcommand, postgres read", table: "leakloop_read",
		sql: `CREATE OR REPLACE TABLE leakloop_read AS
SELECT count(*) AS n, (SELECT count(*) FROM sqlflow_sink_batch) AS batch FROM pg.%[1]s`, minute: newMinute}.run(t)
}
