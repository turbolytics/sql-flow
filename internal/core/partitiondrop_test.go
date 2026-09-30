package core

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/turbolytics/sql-flow/internal/coverage"
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
