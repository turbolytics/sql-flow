//go:build leakloop

package managers

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/core"
	"github.com/turbolytics/sql-flow/internal/handlers"
	"github.com/turbolytics/sql-flow/internal/leakloop"
	"github.com/turbolytics/sql-flow/internal/sinks"
	"go.uber.org/zap"
)

// The window leak loop in the shape of the Bluesky demo: the real
// StructuredBatch handler writing the demo's rows, the pipeline's progress
// row, and the real watermark manager closing minutes on its own connection
// after every batch, the way run wires them. It reports growth per message;
// see internal/leakloop for what the numbers mean, and dev/bench/leakloops.sh
// to run it. window_leak_test.go is the asserting test for the #268 shape.

// countingSink accepts what the manager publishes and keeps nothing, so
// the sample sees the engine, not the test.
type countingSink struct{ rows int64 }

func (s *countingSink) WriteTable(ctx context.Context, batch arrow.Table) error {
	s.rows += batch.NumRows()
	return nil
}

func (s *countingSink) Flush(ctx context.Context) error { return nil }

func runDemoWindow(t *testing.T, name string, sink core.Sink, setup func(*testDB)) {
	const batchSize = 500
	batches := leakloop.Count(t, 100)
	replay := leakloop.NewReplay(t, leakloop.Posts(t, 20_000))
	ctx := context.Background()

	d := newTestDB(t, "")
	exec(t, d.pipeline, `SET TimeZone='UTC'`)
	exec(t, d.pipeline, `CREATE TABLE posts (
		time_us BIGINT,
		commit STRUCT(operation TEXT, record STRUCT(langs TEXT[]))
	)`)
	exec(t, d.pipeline, `CREATE TABLE posts_per_minute_by_lang (bucket TIMESTAMPTZ, lang TEXT, posts INTEGER)`)
	if setup != nil {
		setup(d)
	}
	progress := core.NewProgressStore(d.pipeline)
	if err := progress.Init(ctx); err != nil {
		t.Fatal(err)
	}

	h, err := handlers.New(d.pipeline, config.Handler{
		Type:  "handlers.StructuredBatch",
		Table: "posts",
		SQL: `INSERT INTO posts_per_minute_by_lang
			SELECT
			  time_bucket(INTERVAL '1 minute', to_timestamp(time_us / 1000000)) AS bucket,
			  coalesce(commit.record.langs[1], 'unknown') AS lang,
			  count(*) AS posts
			FROM posts
			WHERE commit.operation = 'create'
			GROUP BY bucket, lang`,
	}, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	if err := h.Init(ctx); err != nil {
		t.Fatal(err)
	}

	// The demo's window declaration.
	m, err := NewWatermark(d.manager, Declaration{
		Table:      "posts_per_minute_by_lang",
		TimeColumn: "bucket",
		Size:       time.Minute,
		Grace:      time.Minute,
		IdleClose:  time.Minute,
		Late:       LateDrop,
		EmitSQL:    "SELECT bucket, lang, sum(posts)::INTEGER AS posts FROM closed GROUP BY bucket, lang",
	}, time.Hour, sink)
	if err != nil {
		t.Fatal(err)
	}

	loop := leakloop.New(t, name, "message", d.pipeline)
	step := max(batches/20, 1)
	loop.Sample(0)
	var consumed int64
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
		consumed += batchSize
		// The pipeline's commit, which moves the arrival clock the idle
		// close reads.
		now := time.Now().UTC()
		if err := progress.Record(ctx, core.Progress{LastArrival: now, LastCommit: now, Messages: consumed}); err != nil {
			t.Fatal(err)
		}
		// One poll per batch: on Render a batch of 500 lands every 10 to 17
		// seconds and the manager polls every 10.
		if err := m.Poll(ctx); err != nil {
			t.Fatal(err)
		}
		if err := h.Init(ctx); err != nil {
			t.Fatal(err)
		}
		if b%step == 0 {
			loop.Sample(consumed)
		}
	}
	loop.Report()
}

// TestWindowDemo__CountingSinkPerMessage keeps every published minute in
// process: the handler, the window, and nothing written anywhere.
func TestWindowDemo__CountingSinkPerMessage(t *testing.T) {
	runDemoWindow(t, "demo window, counting sink", &countingSink{}, nil)
}

// TestWindowDemo__PostgresUpsertPerMessage publishes every closed minute
// through the demo's sqlcommand upsert into SQLFLOW_LEAK_POSTGRES, on a
// connection of its own, as run wires a window sink.
func TestWindowDemo__PostgresUpsertPerMessage(t *testing.T) {
	dsn := os.Getenv("SQLFLOW_LEAK_POSTGRES")
	if dsn == "" {
		t.Fatal("SQLFLOW_LEAK_POSTGRES is required: a Postgres connection string as this process sees it")
	}
	var sink core.Sink
	runDemoWindow(t, "demo window, postgres upsert", sinkProxy{&sink}, func(d *testDB) {
		ctx := context.Background()
		sconn, err := d.db.Connect(ctx)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { sconn.Close() })
		for _, sql := range []string{
			`INSTALL postgres`, `LOAD postgres`,
			`ATTACH '` + dsn + `' AS pg (TYPE POSTGRES)`,
			`CALL postgres_execute('pg', 'DROP TABLE IF EXISTS leakloop_window')`,
			`CALL postgres_execute('pg', 'CREATE TABLE leakloop_window (
				bucket TIMESTAMPTZ NOT NULL, lang TEXT NOT NULL, posts INTEGER NOT NULL,
				updated_at TIMESTAMPTZ NOT NULL DEFAULT now(), PRIMARY KEY (bucket, lang))')`,
			`CALL pg_clear_cache()`,
		} {
			exec(t, sconn, sql)
		}
		s, err := sinks.NewSQLCommandSink(sconn, `INSERT INTO pg.leakloop_window (bucket, lang, posts, updated_at)
			SELECT bucket, lang, posts, now() FROM sqlflow_sink_batch
			ON CONFLICT (bucket, lang) DO UPDATE
			  SET posts = EXCLUDED.posts, updated_at = EXCLUDED.updated_at`, nil)
		if err != nil {
			t.Fatal(err)
		}
		sink = s
	})
}

// sinkProxy lets the sink be built inside setup, after the database exists.
type sinkProxy struct{ s *core.Sink }

func (p sinkProxy) WriteTable(ctx context.Context, b arrow.Table) error {
	return (*p.s).WriteTable(ctx, b)
}
func (p sinkProxy) Flush(ctx context.Context) error { return (*p.s).Flush(ctx) }
