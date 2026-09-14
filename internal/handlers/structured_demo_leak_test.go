package handlers

import (
	"context"
	"os"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/turbolytics/sql-flow/internal/leakloop"
)

// The StructuredBatch leak loops, in the shape of the Bluesky demo: its
// nested post schema, its handler SQL, the progress row the pipeline updates
// after every batch, and the window SQL the tumbling manager runs between
// batches. TestStructuredInvoke_DoesNotLeakNativeMemory covers a flat
// three-column schema with an 8 MiB bound over half a million messages, which
// is about 17 bytes a message; these report the rate itself.

const demoPostsDDL = `CREATE TABLE posts (
	time_us BIGINT,
	commit STRUCT(operation TEXT, record STRUCT(langs TEXT[]))
)`

const demoSelect = `SELECT
	time_bucket(INTERVAL '1 minute', to_timestamp(time_us / 1000000)) AS bucket,
	coalesce(commit.record.langs[1], 'unknown') AS lang,
	count(*) AS posts
FROM posts
WHERE commit.operation = 'create'
GROUP BY bucket, lang`

// demoCollect and demoDelete are the demo's tumbling window predicates,
// including the idle branch that reads sqlflow_progress.
const demoCollect = `SELECT bucket, lang, sum(posts)::INTEGER AS posts
FROM posts_per_minute_by_lang
WHERE bucket + INTERVAL '1 minute' < (SELECT max(bucket) FROM posts_per_minute_by_lang) - INTERVAL '60 seconds'
   OR (SELECT now() - last_arrival FROM sqlflow_progress) > INTERVAL '1 minute'
GROUP BY bucket, lang`

const demoDelete = `DELETE FROM posts_per_minute_by_lang
WHERE bucket + INTERVAL '1 minute' < (SELECT max(bucket) FROM posts_per_minute_by_lang) - INTERVAL '60 seconds'
   OR (SELECT now() - last_arrival FROM sqlflow_progress) > INTERVAL '1 minute'`

// demoBatchesPerPoll is how many batches the demo writes between window
// polls on Render: a batch of 500 every 10 to 17 seconds, a poll every 10.
const demoBatchesPerPoll = 1

func execSQL(tb testing.TB, conn adbc.Connection, sql string) {
	tb.Helper()
	stmt, err := conn.NewStatement()
	if err != nil {
		tb.Fatal(err)
	}
	defer stmt.Close()
	if err := stmt.SetSqlQuery(sql); err != nil {
		tb.Fatalf("%s: %v", sql, err)
	}
	if _, err := stmt.ExecuteUpdate(context.Background()); err != nil {
		tb.Fatalf("%s: %v", sql, err)
	}
}

// drainQuery runs a query and releases every record it returns, the way the
// tumbling manager collects closed windows.
func drainQuery(tb testing.TB, conn adbc.Connection, sql string) {
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
	for reader.Next() {
	}
	if err := reader.Err(); err != nil {
		tb.Fatal(err)
	}
}

type demoScenario struct {
	name string
	// insert writes the handler's rows into the window table, and window runs
	// the demo's collect and delete after every demoBatchesPerPoll batches.
	insert bool
	// attach loads the postgres extension and attaches SQLFLOW_LEAK_POSTGRES
	// the way the demo's commands do. Nothing is written to it.
	attach bool
}

func runDemoHandler(t *testing.T, sc demoScenario) {
	const batchSize = 500
	batches := leakloop.Count(t, 100)
	posts := leakloop.Posts(t, 20_000)

	conn, cleanup := newTestADBCConn(t)
	t.Cleanup(cleanup)

	execSQL(t, conn, `SET TimeZone='UTC'`)
	if sc.attach {
		dsn := os.Getenv("SQLFLOW_LEAK_POSTGRES")
		if dsn == "" {
			t.Skip("SQLFLOW_LEAK_POSTGRES is not set")
		}
		execSQL(t, conn, `INSTALL postgres`)
		execSQL(t, conn, `LOAD postgres`)
		execSQL(t, conn, `ATTACH '`+dsn+`' AS pg (TYPE POSTGRES)`)
	}
	execSQL(t, conn, demoPostsDDL)
	execSQL(t, conn, `CREATE TABLE posts_per_minute_by_lang (bucket TIMESTAMPTZ, lang TEXT, posts INTEGER)`)
	execSQL(t, conn, `CREATE TABLE sqlflow_progress (last_arrival TIMESTAMPTZ, last_commit TIMESTAMPTZ, messages BIGINT)`)
	execSQL(t, conn, `INSERT INTO sqlflow_progress VALUES (now(), now(), 0)`)

	sql := demoSelect
	if sc.insert {
		sql = "INSERT INTO posts_per_minute_by_lang " + demoSelect
	}

	stmt, err := conn.NewStatement()
	if err != nil {
		t.Fatal(err)
	}
	if err := stmt.SetSqlQuery("SELECT * FROM posts LIMIT 0"); err != nil {
		t.Fatal(err)
	}
	reader, _, err := stmt.ExecuteQuery(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	schema := reader.Schema()
	reader.Release()
	stmt.Close()

	h, err := NewStructuredBatchHandler(conn, sql, "posts", schema)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := h.Init(ctx); err != nil {
		t.Fatal(err)
	}

	loop := leakloop.New(t, "structured batch, demo schema, "+sc.name, "message", conn)
	step := batches / 20
	if step == 0 {
		step = 1
	}
	loop.Sample(0)
	k := 0
	for b := 1; b <= batches; b++ {
		for j := 0; j < batchSize; j++ {
			if err := h.Write(posts[k%len(posts)]); err != nil {
				t.Fatal(err)
			}
			k++
		}
		tbl, err := h.Invoke(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if tbl != nil {
			tbl.Release()
		}
		// The pipeline's commit: the progress row the window's idle branch reads.
		execSQL(t, conn, `UPDATE sqlflow_progress SET last_arrival = now(), last_commit = now(), messages = messages + 500`)
		if err := h.Init(ctx); err != nil {
			t.Fatal(err)
		}
		if sc.insert && b%demoBatchesPerPoll == 0 {
			drainQuery(t, conn, demoCollect)
			execSQL(t, conn, demoDelete)
		}
		if b%step == 0 {
			loop.Sample(int64(b * batchSize))
		}
	}
	loop.Report()
}

func TestStructuredDemo__SelectPerMessage(t *testing.T) {
	coverage.Covers(t, "handler.structured")
	runDemoHandler(t, demoScenario{name: "select only"})
}

func TestStructuredDemo__InsertAndWindowPerMessage(t *testing.T) {
	coverage.Covers(t, "handler.structured")
	runDemoHandler(t, demoScenario{name: "insert, window collect and delete", insert: true})
}

func TestStructuredDemo__PostgresAttachedPerMessage(t *testing.T) {
	coverage.Covers(t, "handler.structured")
	runDemoHandler(t, demoScenario{name: "insert, window, postgres attached", insert: true, attach: true})
}
