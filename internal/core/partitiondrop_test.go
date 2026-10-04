package core

import (
	"context"
	"testing"

	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/turbolytics/sql-flow/internal/coverage"
	"github.com/turbolytics/sql-flow/internal/duckdb"
	"github.com/zeebo/assert"
)

func TestDuckDBPartitionDropper_DeletesOnlyThePartition(t *testing.T) {
	coverage.Covers(t, "source.kafka")
	ctx := context.Background()
	conn := memConn(t)
	for _, q := range []string{
		`CREATE TABLE w (minute TIMESTAMPTZ, kafka_partition INTEGER, n INTEGER)`,
		`INSERT INTO w VALUES (TIMESTAMPTZ '2026-09-30 12:00:00+00', 0, 1), (TIMESTAMPTZ '2026-09-30 12:00:00+00', 1, 2)`,
	} {
		stmt, err := conn.NewStatement()
		assert.NoError(t, err)
		assert.NoError(t, stmt.SetSqlQuery(q))
		_, err = stmt.ExecuteUpdate(ctx)
		assert.NoError(t, err)
		stmt.Close()
	}
	assert.NoError(t, NewDuckDBPartitionDropper(conn, []string{"w"}).DropPartitions(ctx, "t", []int32{1}))

	offsets := NewOffsetStore(conn)
	assert.NoError(t, offsets.Init(ctx))
	marks := NewMarks()
	marks.Advance("t", 0, Mark{Offset: 1})
	marks.Advance("t", 1, Mark{Offset: 2})
	assert.NoError(t, offsets.Save(ctx, marks))
	assert.NoError(t, offsets.Delete(ctx, "t", []int32{1}))
	loaded, err := offsets.Load(ctx)
	assert.NoError(t, err)
	_, has1 := loaded.Get("t", 1)
	_, has0 := loaded.Get("t", 0)
	assert.That(t, !has1 && has0)

	stmt, err := conn.NewStatement()
	assert.NoError(t, err)
	defer stmt.Close()
	assert.NoError(t, stmt.SetSqlQuery(`SELECT count(*)::BIGINT FROM w WHERE kafka_partition = 1`))
	rdr, _, err := stmt.ExecuteQuery(ctx)
	assert.NoError(t, err)
	defer rdr.Release()
	assert.That(t, rdr.Next())
	assert.Equal(t, int64(0), rdr.Record().Column(0).(*array.Int64).Value(0))
}

func TestDuckDBPartitionDropper_DropAllEmptiesEveryOwnedTable(t *testing.T) {
	coverage.Covers(t, "source.kafka")
	ctx := context.Background()
	conn := memConn(t)
	for _, q := range []string{
		`CREATE TABLE a (kafka_partition INTEGER)`,
		`CREATE TABLE b (kafka_partition INTEGER)`,
		`CREATE TABLE other (kafka_partition INTEGER)`,
		`INSERT INTO a VALUES (0), (1)`,
		`INSERT INTO b VALUES (2)`,
		`INSERT INTO other VALUES (3)`,
	} {
		stmt, err := conn.NewStatement()
		assert.NoError(t, err)
		assert.NoError(t, stmt.SetSqlQuery(q))
		_, err = stmt.ExecuteUpdate(ctx)
		assert.NoError(t, err)
		stmt.Close()
	}
	assert.NoError(t, NewDuckDBPartitionDropper(conn, []string{"a", "b"}).DropAll(ctx))

	stmt, err := conn.NewStatement()
	assert.NoError(t, err)
	defer stmt.Close()
	assert.NoError(t, stmt.SetSqlQuery(
		`SELECT (SELECT count(*) FROM a)::BIGINT + (SELECT count(*) FROM b), (SELECT count(*) FROM other)::BIGINT`))
	rdr, _, err := stmt.ExecuteQuery(ctx)
	assert.NoError(t, err)
	defer rdr.Release()
	assert.That(t, rdr.Next())
	assert.Equal(t, int64(0), rdr.Record().Column(0).(*array.Int64).Value(0))
	assert.Equal(t, int64(1), rdr.Record().Column(1).(*array.Int64).Value(0))
}

// The drop runs on the pipeline's connection, whose transaction may have
// been open since before the manager's pass deleted the same rows on its
// own connection and committed. DuckDB then refuses the drop's delete with
// "Conflict on tuple deletion", and the pipeline stopped over it (#437).
// The drop must succeed whatever the pipeline's transaction has seen.
//
//	pipeline conn (autocommit off)    manager conn
//	----------------------------------------------------------
//	INSERT ... ; COMMIT
//	UPDATE progress (opens the next tx)
//	                                  DELETE closed rows; COMMIT
//	lose partitions -> drop  <-- conflict, unless the tx ends first
func TestTurbine_PartitionDropSucceedsAfterAPassDeletedTheRows(t *testing.T) {
	coverage.Covers(t, "source.kafka")
	ctx := context.Background()
	db, err := duckdb.OpenPath(ctx, "")
	assert.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	pipeline, err := db.Connect(ctx)
	assert.NoError(t, err)
	t.Cleanup(func() { pipeline.Close() })
	manager, err := db.Connect(ctx)
	assert.NoError(t, err)
	t.Cleanup(func() { manager.Close() })

	exec(t, pipeline, `CREATE TABLE w (minute TIMESTAMPTZ, kafka_partition INTEGER, n INTEGER)`)
	exec(t, pipeline, `CREATE TABLE progress (seen TIMESTAMPTZ)`)
	exec(t, pipeline, `INSERT INTO progress VALUES (now())`)
	assert.NoError(t, pipeline.(adbc.PostInitOptions).SetOption(adbc.OptionKeyAutoCommit, adbc.OptionValueDisabled))
	tx := pipeline.(stateTx)

	// A batch: rows for partitions 3 and 4, committed.
	exec(t, pipeline, `INSERT INTO w VALUES (TIMESTAMPTZ '2026-10-04 19:33:00+00', 3, 1), (TIMESTAMPTZ '2026-10-04 19:33:00+00', 4, 1)`)
	assert.NoError(t, tx.Commit(ctx))
	// An idle tick's progress write: opens the pipeline's next transaction.
	exec(t, pipeline, `UPDATE progress SET seen = now()`)
	// The manager's pass closes the minute: deletes its rows, commits.
	exec(t, manager, `DELETE FROM w WHERE minute = TIMESTAMPTZ '2026-10-04 19:33:00+00'`)

	// The session is lost: the turbine drops the partitions.
	src := newOwnerSource()
	w := NewWatermarks([]WindowSpec{ownedSpec}, nil)
	tb := newWindowedTurbine(src, &fakeHandler{}, &fakeSink{}, 100, w,
		WithPartitionDropper(NewDuckDBPartitionDropper(pipeline, []string{"w"})),
		WithStateStore(&nopSaver{}, tx))
	loopCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := tb.ConsumeLoop(loopCtx, 0); done <- err }()
	src.assigned(map[string][]int32{"t": {3, 4, 5}})
	src.lost(map[string][]int32{"t": {3, 4, 5}}) // blocks until dropped
	select {
	case err := <-done:
		t.Fatalf("the run stopped: %v", err)
	case <-time.After(200 * time.Millisecond):
	}
}

type nopSaver struct{}

func (nopSaver) Save(context.Context, *Marks) error { return nil }
