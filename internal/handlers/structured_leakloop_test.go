//go:build leakloop

package handlers

import (
	"context"
	"testing"

	"github.com/turbolytics/sql-flow/internal/leakloop"
)

// The StructuredBatch leak loop, in the shape of the Bluesky demo: its nested
// post schema and its handler's SELECT, with nothing downstream. The window
// the demo writes into is internal/managers' loop.
// TestStructuredInvoke_DoesNotLeakNativeMemory bounds a flat three-column
// schema at 8 MiB over half a million messages, about 17 bytes a message;
// this reports the rate itself. Run it with dev/bench/leakloops.sh.

func TestStructuredDemo__SelectPerMessage(t *testing.T) {
	const batchSize = 500
	batches := leakloop.Count(t, 100)
	replay := leakloop.NewReplay(t, leakloop.Posts(t, 20_000))

	conn, cleanup := newTestADBCConn(t)
	t.Cleanup(cleanup)
	ctx := context.Background()

	for _, sql := range []string{
		`SET TimeZone='UTC'`,
		`CREATE TABLE posts (
			time_us BIGINT,
			commit STRUCT(operation TEXT, record STRUCT(langs TEXT[]))
		)`,
	} {
		stmt, err := conn.NewStatement()
		if err != nil {
			t.Fatal(err)
		}
		if err := stmt.SetSqlQuery(sql); err != nil {
			t.Fatal(err)
		}
		if _, err := stmt.ExecuteUpdate(ctx); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
		stmt.Close()
	}

	stmt, err := conn.NewStatement()
	if err != nil {
		t.Fatal(err)
	}
	if err := stmt.SetSqlQuery("SELECT * FROM posts LIMIT 0"); err != nil {
		t.Fatal(err)
	}
	reader, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		t.Fatal(err)
	}
	schema := reader.Schema()
	reader.Release()
	stmt.Close()

	h, err := NewStructuredBatchHandler(conn, `SELECT
		time_bucket(INTERVAL '1 minute', to_timestamp(time_us / 1000000)) AS bucket,
		coalesce(commit.record.langs[1], 'unknown') AS lang,
		count(*) AS posts
	FROM posts
	WHERE commit.operation = 'create'
	GROUP BY bucket, lang`, "posts", schema)
	if err != nil {
		t.Fatal(err)
	}
	if err := h.Init(ctx); err != nil {
		t.Fatal(err)
	}

	loop := leakloop.New(t, "structured batch, demo schema, select", "message", conn)
	step := max(batches/20, 1)
	loop.Sample(0)
	for b := 1; b <= batches; b++ {
		for j := 0; j < batchSize; j++ {
			if err := h.Write(replay.Next()); err != nil {
				t.Fatal(err)
			}
		}
		tbl, err := h.Invoke(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if tbl != nil {
			tbl.Release()
		}
		if err := h.Init(ctx); err != nil {
			t.Fatal(err)
		}
		if b%step == 0 {
			loop.Sample(int64(b * batchSize))
		}
	}
	loop.Report()
}
