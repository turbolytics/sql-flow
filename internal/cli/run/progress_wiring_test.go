package run

import (
	"context"
	"github.com/apache/arrow-go/v18/arrow/array"
	"sync"
	"testing"
	"time"

	"github.com/turbolytics/sql-flow/internal/config"
	"github.com/turbolytics/sql-flow/internal/core"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/turbolytics/sql-flow/internal/duckdb"
	"github.com/zeebo/assert"
)

// countingProgress counts the progress writes it is handed.
type countingProgress struct {
	mu sync.Mutex
	n  int
}

func (c *countingProgress) Record(context.Context, core.Progress) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.n++
	return nil
}

func (c *countingProgress) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.n
}

func windowedConf(idleCloseSeconds int) *config.Conf {
	return &config.Conf{Tables: &config.Tables{SQL: []config.TableSQL{{
		Name: "t",
		Window: &config.Window{
			TimeColumn: "bucket", SizeSeconds: 60, GraceSeconds: 5,
			IdleCloseSeconds: idleCloseSeconds,
		},
	}}}}
}

// run's wiring decides which pipelines assert a watermark. Wired to always,
// a pipeline with no window would carry a tracker for nothing; wired to
// never, a window would have no watermark and never close. The specs the
// tracker is built from are the config's durations, one per window.
func TestManagerWindow_OnlyAWindowingPipelineAssertsWatermarks(t *testing.T) {
	coverage.Covers(t, "manager.window")
	ctx := context.Background()
	db, err := duckdb.OpenPath(ctx, "")
	assert.NoError(t, err)
	defer db.Close()
	conn, err := db.Connect(ctx)
	assert.NoError(t, err)
	defer conn.Close()

	w, opts := windowOptions(&config.Conf{}, conn)
	assert.That(t, w == nil)
	assert.Equal(t, 0, len(opts))

	w, opts = windowOptions(windowedConf(10), conn)
	assert.That(t, w != nil)
	assert.Equal(t, 1, len(opts))

	specs := windowSpecs(windowedConf(10))
	assert.Equal(t, 1, len(specs))
	assert.Equal(t, core.WindowSpec{Name: "t", Size: time.Minute, Grace: 5 * time.Second, IdleClose: 10 * time.Second}, specs[0])
}

// Idle ticks write no progress row on any pipeline: nothing reads it
// between batches, and on a box that runs its state from an SD card a
// statement, a WAL append and an fsync every flush interval is the
// difference between a quiet pipeline and a busy one. This ties the config
// to the turbine through the options run builds, on a source that never
// delivers so every commit is an idle tick.
func TestManagerWindow_IdleTicksWriteNoProgressRow(t *testing.T) {
	coverage.Covers(t, "manager.window")
	ctx := context.Background()
	db, err := duckdb.OpenPath(ctx, "")
	assert.NoError(t, err)
	defer db.Close()
	conn, err := db.Connect(ctx)
	assert.NoError(t, err)
	defer conn.Close()
	assert.NoError(t, initWindowStores(ctx, windowedConf(10), conn))

	for _, tc := range []struct {
		name string
		conf *config.Conf
	}{
		{"a window with idle_close_seconds", windowedConf(10)},
		{"a window without it", windowedConf(0)},
		{"a pipeline with no window", &config.Conf{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := &countingProgress{}
			src := &tickingSource{every: time.Hour, release: make(chan struct{})}
			_, windowOpts := windowOptions(tc.conf, conn)
			opts := append([]core.TurbineOption{core.WithProgressStore(store)}, windowOpts...)
			tb := core.NewTurbine(src, nilHandler{}, noopSink{}, 1, 20*time.Millisecond,
				&sync.Mutex{}, core.PipelineErrorPolicies{}, opts...)
			done := make(chan struct{})
			go func() { _, _ = tb.ConsumeLoop(context.Background(), 0); close(done) }()

			// Ten ticks' worth.
			time.Sleep(200 * time.Millisecond)
			close(src.release)
			<-done

			assert.Equal(t, 0, store.count())
		})
	}
}

// Only a pipeline that windows over Kafka holds its commit at the low
// watermark. Wired onto a pipeline with no window it would do nothing;
// left off a windowed Kafka pipeline, a worker that lost its disk would
// start past rows only its window held. The store exists only with a state
// path, and is created here, under autocommit.
func TestStateDurability_OnlyAWindowedKafkaPipelineTracksWindowOffsets(t *testing.T) {
	coverage.Covers(t, "state.durability")
	ctx := context.Background()
	db, err := duckdb.OpenPath(ctx, "")
	assert.NoError(t, err)
	defer db.Close()
	conn, err := db.Connect(ctx)
	assert.NoError(t, err)
	defer conn.Close()

	kafkaWindowed := windowedConf(10)
	kafkaWindowed.Pipeline.Source.Type = "kafka"
	webhookWindowed := windowedConf(10)
	webhookWindowed.Pipeline.Source.Type = "webhook"
	kafkaFlat := &config.Conf{}
	kafkaFlat.Pipeline.Source.Type = "kafka"

	for _, conf := range []*config.Conf{webhookWindowed, kafkaFlat} {
		opts, err := windowOffsetOptions(ctx, conf, conn, true)
		assert.NoError(t, err)
		assert.Equal(t, 0, len(opts))
	}

	opts, err := windowOffsetOptions(ctx, kafkaWindowed, conn, false)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(opts))
	_, err = core.NewWindowOffsetStore(conn, nil).Load(ctx)
	assert.Error(t, err) // no state path, no table

	opts, err = windowOffsetOptions(ctx, kafkaWindowed, conn, true)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(opts))
	_, err = core.NewWindowOffsetStore(conn, nil).Load(ctx)
	assert.NoError(t, err)
}

// A partition_owned pipeline starts with nothing it held before: its window
// rows, stored positions and offset records all go, so each partition it is
// assigned is recounted from the group's committed position. Kept, they
// would be stale for any partition another member consumed while this
// process was down, and the start pass would publish them.
func TestStateDurability_APartitionOwnedPipelineStartsEmpty(t *testing.T) {
	coverage.Covers(t, "state.durability")
	ctx := context.Background()
	db, err := duckdb.OpenPath(ctx, "")
	assert.NoError(t, err)
	defer db.Close()
	conn, err := db.Connect(ctx)
	assert.NoError(t, err)
	defer conn.Close()
	exec := func(q string) {
		t.Helper()
		stmt, err := conn.NewStatement()
		assert.NoError(t, err)
		defer stmt.Close()
		assert.NoError(t, stmt.SetSqlQuery(q))
		_, err = stmt.ExecuteUpdate(ctx)
		assert.NoError(t, err)
	}

	// No owned window: nothing is wired and nothing is touched.
	opts, err := partitionOwnedOptions(ctx, windowedConf(10), conn, true)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(opts))

	conf := windowedConf(10)
	conf.Pipeline.Source = config.Source{Type: "kafka", Kafka: &config.KafkaSource{Topics: []string{"topic"}}}
	conf.Tables.SQL[0].Window.PartitionOwned = true
	conf.Tables.SQL[0].Window.Sink = config.Sink{Type: "postgres", Postgres: &config.PostgresSink{
		Mode: "upsert", Key: []string{"bucket", "kafka_partition"}}}
	specs := windowSpecs(conf)
	assert.That(t, specs[0].PartitionOwned)

	exec(`CREATE TABLE t (bucket TIMESTAMPTZ, kafka_partition INTEGER)`)
	exec(`INSERT INTO t VALUES (now(), 0)`)
	offsets := core.NewOffsetStore(conn)
	assert.NoError(t, offsets.Init(ctx))
	marks := core.NewMarks()
	marks.Advance("topic", 0, core.Mark{Offset: 9})
	assert.NoError(t, offsets.Save(ctx, marks))
	wo := core.NewWindowOffsetStore(conn, specs)
	assert.NoError(t, wo.Init(ctx))
	assert.NoError(t, wo.Save(ctx, core.WindowOffsetsDelta{Added: []core.OffsetRecord{{
		Window: "t", Bucket: time.Now(), Topic: "topic", Partition: 0, Mark: core.Mark{Offset: 3}}}}))

	opts, err = partitionOwnedOptions(ctx, conf, conn, true)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(opts))

	loaded, err := offsets.Load(ctx)
	assert.NoError(t, err)
	assert.That(t, loaded.Empty())
	recs, err := wo.Load(ctx)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(recs))
	stmt, err := conn.NewStatement()
	assert.NoError(t, err)
	defer stmt.Close()
	assert.NoError(t, stmt.SetSqlQuery(`SELECT count(*)::BIGINT FROM t`))
	rdr, _, err := stmt.ExecuteQuery(ctx)
	assert.NoError(t, err)
	defer rdr.Release()
	assert.That(t, rdr.Next())
	assert.Equal(t, int64(0), rdr.Record().Column(0).(*array.Int64).Value(0))
}
